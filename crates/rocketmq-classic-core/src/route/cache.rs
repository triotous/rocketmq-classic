use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_protocol::route::TopicRouteData;

use crate::runtime::RuntimeError;

use super::table::TopicRouteDataHandle;

#[derive(Clone, Debug)]
pub struct TopicSnapshot {
    pub route: Arc<TopicRouteData>,
    pub publish_queues: Vec<MessageQueue>,
    pub subscribe_queues: BTreeSet<MessageQueue>,
    pub endpoints: BTreeMap<MessageQueue, String>,
    pub order_topic: bool,
}

#[derive(Clone, Debug, Default)]
pub struct RouteSnapshot {
    /// Source raw topic table; explicit map writes do not rebuild `topics`.
    pub raw_routes: BTreeMap<String, TopicRouteDataHandle>,
    /// Last converted publication, independent of later raw map mutations.
    pub topics: BTreeMap<String, Arc<TopicSnapshot>>,
    pub broker_addresses: BTreeMap<String, BTreeMap<i64, Option<String>>>,
    pub(crate) broker_versions: BTreeMap<(String, String), i32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerAddress {
    pub address: String,
    pub slave: bool,
    pub version: i32,
}

impl RouteSnapshot {
    /// Java selects a random BrokerData from this topic's raw cached route,
    /// then its master or a random available slave. Global broker metadata may
    /// have since been overwritten by another topic and must not be used here.
    pub fn broker_address_by_topic(&self, topic: &str) -> Result<Option<String>, RuntimeError> {
        self.select_topic_broker(topic, |length| rand::random_range(0..length))
    }

    fn select_topic_broker(
        &self,
        topic: &str,
        mut select: impl FnMut(usize) -> usize,
    ) -> Result<Option<String>, RuntimeError> {
        let Some(route) = self.raw_routes.get(topic) else {
            return Ok(None);
        };
        let route = route.snapshot()?;
        let brokers = route.broker_datas.as_deref().unwrap_or(&[]);
        if brokers.is_empty() {
            return Ok(None);
        }
        let broker = brokers[select(brokers.len())]
            .as_ref()
            .ok_or(RuntimeError::InvalidRoute("selected broker is null"))?;
        let addresses = broker
            .broker_addrs
            .as_ref()
            .ok_or(RuntimeError::InvalidRoute(
                "selected broker has null address map",
            ))?;
        if let Some(master) = addresses.get(&0).and_then(Option::as_deref) {
            return Ok(Some(master.to_owned()));
        }
        if addresses.is_empty() {
            return Err(RuntimeError::InvalidRoute(
                "selected broker has no addresses",
            ));
        }
        Ok(addresses
            .values()
            .nth(select(addresses.len()))
            .and_then(Option::clone))
    }

    pub fn publish_address(&self, broker: &str) -> Option<&str> {
        self.broker_addresses
            .get(broker)?
            .get(&0)
            .and_then(Option::as_deref)
    }

    pub fn subscribe_address(
        &self,
        broker: &str,
        id: i64,
        only_this_broker: bool,
    ) -> Option<BrokerAddress> {
        let addresses = self.broker_addresses.get(broker)?;
        let exact_or_adjacent = addresses
            .get_key_value(&id)
            .filter(|(_, address)| address.is_some())
            .or_else(|| {
                (id != 0)
                    .then(|| addresses.get_key_value(&id.wrapping_add(1)))
                    .flatten()
                    .filter(|(_, address)| address.is_some())
            });
        let selected = exact_or_adjacent.or_else(|| {
            (!only_this_broker)
                .then(|| addresses.first_key_value())
                .flatten()
        })?;
        let address = selected.1.as_ref()?;
        Some(BrokerAddress {
            address: address.clone(),
            // Java changes the slave flag only for the arbitrary-entry fallback,
            // not for brokerId + 1 (including signed overflow/negative IDs).
            slave: if exact_or_adjacent.is_some() {
                id != 0
            } else {
                *selected.0 != 0
            },
            version: self
                .broker_versions
                .get(&(broker.to_owned(), address.clone()))
                .copied()
                .unwrap_or(0),
        })
    }

    pub fn admin_address(&self, broker: &str) -> Option<BrokerAddress> {
        // Java iterates until it finds a nonnull value. HashMap order is not
        // observable when several valid entries exist.
        let (id, address) = self
            .broker_addresses
            .get(broker)?
            .iter()
            .find_map(|(id, address)| address.as_ref().map(|address| (id, address)))?;
        Some(BrokerAddress {
            address: address.clone(),
            slave: *id != 0,
            version: self
                .broker_versions
                .get(&(broker.to_owned(), address.clone()))
                .copied()
                .unwrap_or(0),
        })
    }

    pub fn broker_name<'a>(&'a self, queue: &'a MessageQueue) -> Option<&'a str> {
        let endpoints = queue
            .topic()
            .and_then(|name| self.topics.get(name))
            .map(|topic| &topic.endpoints);
        match endpoints.filter(|endpoints| !endpoints.is_empty()) {
            Some(endpoints) => endpoints.get(queue).map(String::as_str),
            None => queue.broker_name(),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct RouteCache {
    snapshot: Arc<RwLock<Arc<RouteSnapshot>>>,
}

impl RouteCache {
    pub(super) fn update_raw<R>(
        &self,
        update: impl FnOnce(&mut BTreeMap<String, TopicRouteDataHandle>) -> (bool, R),
    ) -> Result<R, RuntimeError> {
        let mut current = self
            .snapshot
            .write()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let mut next = current.as_ref().clone();
        let (changed, result) = update(&mut next.raw_routes);
        if changed {
            *current = Arc::new(next);
        }
        Ok(result)
    }

    pub(crate) fn record_version(
        &self,
        broker: &str,
        address: &str,
        version: i32,
    ) -> Result<(), RuntimeError> {
        let mut current = self
            .snapshot
            .write()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let mut next = current.as_ref().clone();
        next.broker_versions
            .insert((broker.to_owned(), address.to_owned()), version);
        *current = Arc::new(next);
        Ok(())
    }

    pub(crate) fn clean_offline(&self) -> Result<(), RuntimeError> {
        let routes = self
            .snapshot()?
            .raw_routes
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut live = BTreeSet::new();
        for route in routes {
            let route = route.snapshot()?;
            let broker_datas = route
                .broker_datas
                .as_deref()
                .ok_or(RuntimeError::InvalidRoute(
                    "null broker data list during cleanup",
                ))?;
            for broker in broker_datas {
                let broker = broker.as_ref().ok_or(RuntimeError::InvalidRoute(
                    "null broker data during cleanup",
                ))?;
                if let Some(addresses) = broker.broker_addrs.as_ref() {
                    live.extend(addresses.values().cloned());
                }
            }
        }
        let mut current = self
            .snapshot
            .write()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let mut next = current.as_ref().clone();
        next.broker_addresses.retain(|_, addresses| {
            addresses.retain(|_, address| live.contains(address));
            !addresses.is_empty()
        });
        *current = Arc::new(next);
        Ok(())
    }
    pub(crate) fn snapshot(&self) -> Result<Arc<RouteSnapshot>, RuntimeError> {
        self.snapshot
            .read()
            .map(|value| value.clone())
            .map_err(|_| RuntimeError::RegistryPoisoned)
    }

    pub(crate) fn replace(&self, topic: &str, route: TopicRouteData) -> Result<bool, RuntimeError> {
        self.replace_for_owner_update(topic, route, false)
    }

    pub(crate) fn route_changed(
        &self,
        topic: &str,
        route: &TopicRouteData,
    ) -> Result<bool, RuntimeError> {
        let old = self.snapshot()?.raw_routes.get(topic).cloned();
        match old {
            Some(old) => Ok(!routes_equal(&old.snapshot()?, route)?),
            None => Ok(true),
        }
    }

    pub(crate) fn replace_for_owner_update(
        &self,
        topic: &str,
        route: TopicRouteData,
        force: bool,
    ) -> Result<bool, RuntimeError> {
        // A raw map write can insert data that normal refresh cannot convert.
        // Java compares cached raw data before entering conversion/publication.
        if !force && !self.route_changed(topic, &route)? {
            return Ok(false);
        }
        // Java's brokerAddrTable is a ConcurrentHashMap: publishing a null
        // address map fails, it is not a valid empty Broker. Validate before
        // any conversion/publication to preserve the coherent native snapshot.
        if route.broker_datas.is_none() {
            return Err(RuntimeError::InvalidRoute("null broker data list"));
        }
        if route
            .broker_datas
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .any(Option::is_none)
        {
            return Err(RuntimeError::InvalidRoute("null broker data"));
        }
        if route
            .broker_datas
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .flatten()
            .any(|broker| broker.broker_name.is_none())
        {
            return Err(RuntimeError::InvalidRoute("null broker name"));
        }
        let ordered = route
            .order_topic_conf
            .as_deref()
            .is_some_and(|value| !value.is_empty());
        let static_route = route.order_topic_conf.is_none()
            && route
                .topic_queue_mapping_by_broker
                .as_ref()
                .is_some_and(|mappings| !mappings.is_empty());
        if route.queue_datas.is_none() && !ordered && !static_route {
            return Err(RuntimeError::InvalidRoute("null queue data list"));
        }
        if !static_route
            && route
                .queue_datas
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .any(Option::is_none)
        {
            return Err(RuntimeError::InvalidRoute("null queue data"));
        }
        if !ordered
            && !static_route
            && route.queue_datas.as_deref().unwrap_or(&[]).len() > 1
            && route
                .queue_datas
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .flatten()
                .any(|queue| queue.broker_name.is_none())
        {
            return Err(RuntimeError::InvalidRoute(
                "null queue broker name during sort",
            ));
        }
        if let Some(mappings) = &route.topic_queue_mapping_by_broker {
            if mappings.values().any(Option::is_none) {
                return Err(RuntimeError::InvalidRoute("null static mapping"));
            }
            for mapping in mappings
                .values()
                .flatten()
                .filter(|mapping| mapping.scope.is_some())
            {
                let ids = mapping
                    .curr_id_map
                    .as_ref()
                    .ok_or(RuntimeError::InvalidRoute("null static mapping id map"))?;
                if mapping.bname.is_none()
                    && ids.keys().any(|id| *id >= 0 && *id < mapping.total_queues)
                {
                    return Err(RuntimeError::InvalidRoute(
                        "null static mapping broker name",
                    ));
                }
            }
        }
        if route
            .broker_datas
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .flatten()
            .any(|broker| broker.broker_addrs.is_none())
        {
            return Err(RuntimeError::InvalidRoute("null broker address map"));
        }
        // All conversion occurs before the writer lock. Invalid ordered data
        // must not make Phase 1's tolerant helper publish a partial snapshot.
        let mut conversion_route = route.clone();
        if let Some(ordered) = route
            .order_topic_conf
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            let mut normalized = Vec::new();
            for entry in ordered
                .trim_end_matches(';')
                .split(';')
                .filter(|_| !ordered.trim_end_matches(';').is_empty())
            {
                let count = entry.split(':').nth(1).and_then(java_integer);
                let count =
                    count.ok_or(RuntimeError::InvalidRoute("invalid ordered queue count"))?;
                normalized.push(format!(
                    "{}:{count}",
                    entry.split(':').next().unwrap_or_default()
                ));
            }
            conversion_route.order_topic_conf = Some(normalized.join(";"));
        }
        // Raw order presence controls branch selection, even when a separator-
        // only Java split normalizes to an empty conversion string.
        validate_conversion_size(topic, &route)?;
        let mut topic_snapshot =
            TopicSnapshot {
                publish_queues: if route.order_topic_conf.as_deref().is_some_and(|value| {
                    !value.is_empty() && value.trim_end_matches(';').is_empty()
                }) {
                    Vec::new()
                } else {
                    conversion_route.publish_queues(topic)
                },
                subscribe_queues: route.subscribe_queues(topic).into_iter().collect(),
                endpoints: route.static_queue_endpoints(topic).unwrap_or_default(),
                order_topic: route
                    .order_topic_conf
                    .as_ref()
                    .is_some_and(|value| !value.is_empty()),
                // Java publishes a TopicRouteData copy whose constructor
                // normalizes null collections to empty containers.
                route: Arc::new(route.java_copy_constructed()),
            };
        loop {
            let observed = self.snapshot()?.raw_routes.get(topic).cloned();
            let unchanged = match &observed {
                Some(old) => routes_equal(&old.snapshot()?, &topic_snapshot.route)?,
                None => false,
            };
            if unchanged && !force {
                return Ok(false);
            }
            let mut current = self
                .snapshot
                .write()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            let still_same = match (current.raw_routes.get(topic), observed.as_ref()) {
                (None, None) => true,
                (Some(current), Some(observed)) => current.ptr_eq(observed),
                _ => false,
            };
            if !still_same {
                drop(current);
                continue;
            }
            // Java retains existing endpoints when a new conversion is empty.
            if topic_snapshot.endpoints.is_empty() {
                if let Some(old) = current.topics.get(topic) {
                    topic_snapshot.endpoints.clone_from(&old.endpoints);
                }
            }
            let mut next = current.as_ref().clone();
            next.raw_routes.insert(
                topic.to_owned(),
                TopicRouteDataHandle::new((*topic_snapshot.route).clone()),
            );
            for broker in topic_snapshot
                .route
                .broker_datas
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .flatten()
            {
                let Some(name) = broker.broker_name.clone() else {
                    continue;
                };
                next.broker_addresses
                    .insert(name, broker.broker_addrs.clone().unwrap_or_default());
            }
            next.topics
                .insert(topic.to_owned(), Arc::new(topic_snapshot));
            *current = Arc::new(next);
            return Ok(!unchanged);
        }
    }

    pub(crate) fn replace_raw_if_same(
        &self,
        key: &str,
        expected: &TopicRouteDataHandle,
        value: TopicRouteDataHandle,
    ) -> Result<bool, RuntimeError> {
        self.update_raw(|map| {
            let matched = map.get(key).is_some_and(|current| current.ptr_eq(expected));
            if matched {
                map.insert(key.to_owned(), value);
            }
            (matched, matched)
        })
    }

    pub(crate) fn remove_raw_if_same(
        &self,
        key: &str,
        expected: &TopicRouteDataHandle,
    ) -> Result<bool, RuntimeError> {
        self.update_raw(|map| {
            let matched = map.get(key).is_some_and(|current| current.ptr_eq(expected));
            if matched {
                map.remove(key);
            }
            (matched, matched)
        })
    }
}

// Bound expansion of compact routes before invoking collection converters.
fn validate_conversion_size(topic: &str, route: &TopicRouteData) -> Result<(), RuntimeError> {
    let mut estimated = 0usize;
    let mut add = |count: i32, broker: &str| -> Result<(), RuntimeError> {
        let size = std::mem::size_of::<MessageQueue>()
            .checked_add(topic.len())
            .and_then(|size| size.checked_add(broker.len()))
            .and_then(|size| size.checked_add(64))
            .and_then(|size| size.checked_mul(count.max(0) as usize));
        estimated = size
            .and_then(|size| estimated.checked_add(size))
            .filter(|size| *size <= rocketmq_classic_remoting::codec::frame::DEFAULT_MAX_FRAME_SIZE)
            .ok_or(RuntimeError::InvalidRoute(
                "route queue expansion exceeds bounded memory budget",
            ))?;
        Ok(())
    };
    let ordered = route
        .order_topic_conf
        .as_deref()
        .filter(|value| !value.is_empty());
    let mappings = route
        .topic_queue_mapping_by_broker
        .as_ref()
        .filter(|value| !value.is_empty());
    if let Some(ordered) = ordered {
        for item in ordered.split(';') {
            let mut parts = item.split(':');
            let broker = parts.next().unwrap_or_default();
            let count = parts.next().and_then(java_integer).unwrap_or(0);
            add(count, broker)?;
        }
    }
    // Static metadata suppresses ordinary subscription conversion; it also
    // suppresses ordinary publish conversion only for null orderTopicConf.
    if let Some(mappings) = mappings {
        let mut totals = BTreeMap::<String, i32>::new();
        for mapping in mappings.values().flatten() {
            if let Some(scope) = mapping.scope.as_deref().filter(|_| mapping.epoch >= -1) {
                let broker = format!(
                    "__syslo__{}",
                    if scope == "__global__" {
                        &scope[2..]
                    } else {
                        scope
                    }
                );
                let total = totals.entry(broker).or_default();
                *total = (*total).max(mapping.total_queues);
            }
        }
        for (broker, total) in totals {
            add(total, &broker)?; // subscribe queues
            add(total, &broker)?; // endpoints
            if route.order_topic_conf.is_none() {
                add(total, &broker)?;
            } // publish queues
        }
    }
    let mut read_counts = BTreeMap::<Option<&str>, i32>::new();
    for queue in route.queue_datas.as_deref().unwrap_or(&[]).iter().flatten() {
        if mappings.is_none() && queue.perm & 4 != 0 {
            let count = read_counts.entry(queue.broker_name.as_deref()).or_default();
            *count = (*count).max(queue.read_queue_nums);
        }
        if ordered.is_none()
            && !(route.order_topic_conf.is_none() && mappings.is_some())
            && queue.perm & 2 != 0
        {
            let master = route
                .broker_datas
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .flatten()
                .find(|broker| broker.broker_name == queue.broker_name)
                .and_then(|broker| broker.broker_addrs.as_ref())
                .is_some_and(|addresses| addresses.contains_key(&0));
            if master {
                add(
                    queue.write_queue_nums,
                    queue.broker_name.as_deref().unwrap_or_default(),
                )?;
            }
        }
    }
    for (broker, count) in read_counts {
        add(count, broker.unwrap_or_default())?;
    }
    Ok(())
}

// Integer.parseInt iterates Java char/UTF-16 units and Character.digit(10),
// accepting BMP decimal scripts but not supplementary mathematical digits.
// Accumulate negatively so i32::MIN is representable without widening/range loss.
fn java_integer(value: &str) -> Option<i32> {
    crate::java_number::parse_long(value)?.try_into().ok()
}

#[cfg(test)]
fn java_decimal_digit(unit: u16) -> Option<u16> {
    crate::java_number::decimal_digit(unit)
}

fn routes_equal(left: &TopicRouteData, right: &TopicRouteData) -> Result<bool, RuntimeError> {
    let changed = left
        .java_topic_route_data_changed(Some(right))
        .map_err(|error| {
            use rocketmq_classic_protocol::route::TopicRouteDataChangedError;
            RuntimeError::InvalidRoute(match error {
                TopicRouteDataChangedError::NullQueueData => "null queue data during sort",
                TopicRouteDataChangedError::NullQueueBrokerName => {
                    "null queue broker name during sort"
                }
                TopicRouteDataChangedError::NullBrokerData => "null broker data during sort",
                TopicRouteDataChangedError::NullBrokerName => "null broker name during sort",
            })
        })?;
    Ok(!changed)
}

// TopicRouteData.equals delegates BrokerData.equals, which compares only name
// and addresses. BrokerData::PartialEq carries that source behavior.
pub(super) fn route_values_equal(left: &TopicRouteData, right: &TopicRouteData) -> bool {
    left.order_topic_conf == right.order_topic_conf
        && left.queue_datas == right.queue_datas
        && left.filter_server_table == right.filter_server_table
        && left.topic_queue_mapping_by_broker == right.topic_queue_mapping_by_broker
        && left.broker_datas == right.broker_datas
}

/// Java MixAll.brokerVIPChannel's last-colon split and signed-int port offset.
pub fn broker_vip_channel(enabled: bool, address: &str) -> Result<String, RuntimeError> {
    if !enabled {
        return Ok(address.to_owned());
    }
    let (host, port) = address
        .rsplit_once(':')
        .ok_or(RuntimeError::InvalidBrokerAddress)?;
    let port = crate::java_number::parse_long(port)
        .and_then(|port| i32::try_from(port).ok())
        .ok_or(RuntimeError::InvalidBrokerPort)?;
    Ok(format!("{host}:{}", port.wrapping_sub(2)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nullable_route_collection_refresh_matches_invoked_unstarted_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["runtimeNullableCollections"].as_array().unwrap() {
            if case["registeredConsumer"] == true {
                continue; // The registered-consumer branch is exercised through ClientRuntime.
            }
            let cache = RouteCache::default();
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let result = cache.replace("topic", route);
            assert_eq!(result.is_ok(), case["outcome"] == "OK");
            let snapshot = cache.snapshot().unwrap();
            assert_eq!(snapshot.raw_routes.contains_key("topic"), case["cached"]);
            if let Some(cached) = snapshot.raw_routes.get("topic") {
                let cached = cached.snapshot().unwrap();
                assert_eq!(cached.queue_datas.is_none(), case["cachedQueueNull"]);
                assert_eq!(cached.broker_datas.is_none(), case["cachedBrokerNull"]);
                assert_eq!(
                    cached.filter_server_table.is_none(),
                    case["cachedFilterNull"]
                );
            }
        }
    }

    #[test]
    fn nullable_static_mapping_conversion_matches_invoked_java_failures() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullStaticMappingConversion"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let result = RouteCache::default().replace("topic", route);
            assert_eq!(result.is_err(), case["outcome"] == "NullPointerException");
        }
    }

    #[test]
    fn nullable_route_collections_preserve_raw_values_and_copy_comparison() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableRouteCollections"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            let copy = route.java_copy_constructed();
            assert_eq!(
                copy.queue_datas.as_ref().unwrap().is_empty(),
                case["copyQueueEmpty"]
            );
            assert_eq!(
                copy.broker_datas.as_ref().unwrap().is_empty(),
                case["copyBrokerEmpty"]
            );
            assert_eq!(
                copy.filter_server_table.as_ref().unwrap().is_empty(),
                case["copyFilterEmpty"]
            );
            assert!(routes_equal(&route, &copy).unwrap());
            if route != copy {
                assert!(!route_values_equal(&route, &copy));
            }
            let cache = RouteCache::default();
            if route.queue_datas.is_none() || route.broker_datas.is_none() {
                assert!(matches!(
                    cache.replace("topic", route),
                    Err(RuntimeError::InvalidRoute(_))
                ));
            } else {
                assert!(cache.replace("topic", route.clone()).unwrap());
                assert!(!cache.route_changed("topic", &copy).unwrap());
                assert_eq!(
                    cache.snapshot().unwrap().raw_routes["topic"]
                        .snapshot()
                        .unwrap(),
                    copy
                );
            }
        }
    }

    #[test]
    fn offline_cleanup_skips_null_raw_broker_maps_like_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["runtimeRawOfflineCleanup"].as_array().unwrap() {
            let cache = RouteCache::default();
            if case["priorBroker"].as_bool().unwrap() {
                cache
                    .replace(
                        "topic",
                        serde_json::from_value(case["initialInput"].clone()).unwrap(),
                    )
                    .unwrap();
            }
            assert_eq!(
                cache.snapshot().unwrap().publish_address("broker"),
                case["publishAddressBefore"].as_str()
            );
            let table = super::super::TopicRouteTable::new(cache.clone());
            table.clear().unwrap();
            let raw = TopicRouteDataHandle::new(
                serde_json::from_value(case["rawInput"].clone()).unwrap(),
            );
            table.put("raw", raw.clone()).unwrap();
            let before = cache.snapshot().unwrap();
            assert_eq!(case["outcome"], "OK");
            cache.clean_offline().unwrap();
            let after = cache.snapshot().unwrap();
            assert_eq!(
                after.publish_address("broker"),
                case["publishAddressAfter"].as_str()
            );
            assert_eq!(
                after.raw_routes["raw"].ptr_eq(&raw),
                case["rawObjectRetained"]
            );
            assert_eq!(
                after.raw_routes["raw"]
                    .snapshot()
                    .unwrap()
                    .broker_datas
                    .unwrap()[0]
                    .as_ref()
                    .unwrap()
                    .broker_addrs
                    .is_none(),
                case["rawNullMapRetained"]
            );
            assert_eq!(
                before.publish_address("broker"),
                case["publishAddressBefore"].as_str()
            );
        }
    }

    #[test]
    fn malformed_raw_route_cleanup_shapes_match_invoked_java_runtime() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let cases = fixture["runtimeRawCleanupShapes"].as_array().unwrap();
        assert_eq!(cases.len(), 8);
        for required in [
            "null-route-collections",
            "null-broker-row",
            "null-address-map",
            "empty-address-map",
            "null-address-only",
            "null-and-live-addresses",
            "null-row-and-live-address",
            "live-null-address-retained",
        ] {
            assert!(
                cases.iter().any(|case| case["id"] == required),
                "missing {required}"
            );
        }
        for case in cases {
            let cache = RouteCache::default();
            let initial: TopicRouteData =
                serde_json::from_value(case["initialInput"].clone()).unwrap();
            cache.replace("topic", initial).unwrap();
            let table = super::super::TopicRouteTable::new(cache.clone());
            table.clear().unwrap();
            let raw: TopicRouteData = serde_json::from_value(case["rawInput"].clone()).unwrap();
            let raw = TopicRouteDataHandle::new(raw);
            table.put("raw", raw.clone()).unwrap();

            assert_eq!(case["fetchCount"], 1, "{}", case["id"]);
            let before_cleanup = cache.snapshot().unwrap();
            let cleanup = cache.clean_offline();
            match case["outcome"].as_str().unwrap() {
                "OK" => assert!(cleanup.is_ok(), "{}: {cleanup:?}", case["id"]),
                "NullPointerException" => {
                    assert!(cleanup.is_err(), "{} unexpectedly succeeded", case["id"])
                }
                outcome => panic!("unexpected Java cleanup outcome {outcome}"),
            }
            let snapshot = cache.snapshot().unwrap();
            if case["outcome"] == "NullPointerException" {
                assert!(Arc::ptr_eq(&before_cleanup, &snapshot), "{}", case["id"]);
            }
            assert_eq!(
                serde_json::to_value(&snapshot.broker_addresses).unwrap(),
                case["brokerAddressTable"],
                "{}",
                case["id"]
            );
            assert!(snapshot.raw_routes["raw"].ptr_eq(&raw), "{}", case["id"]);
        }
    }

    #[test]
    fn raw_map_mutations_keep_derived_publication_and_offline_cleanup_matches_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let case = &fixture["runtimeTopicRouteTable"];
        let first: TopicRouteData = serde_json::from_value(case["initialInput"].clone()).unwrap();
        let second = TopicRouteDataHandle::new(
            serde_json::from_value(case["replacementInput"].clone()).unwrap(),
        );
        let cache = RouteCache::default();
        let table = super::super::TopicRouteTable::new(cache.clone());
        assert_eq!(
            cache.replace("topic", first.clone()).unwrap(),
            case["refreshChanged"]
        );
        let before = cache.snapshot().unwrap();
        assert!(
            table
                .get("topic")
                .unwrap()
                .unwrap()
                .ptr_eq(&before.raw_routes["topic"])
        );
        assert_eq!(
            before.publish_address("broker"),
            case["initialPublishAddress"].as_str()
        );
        assert!(
            table
                .put("topic", second)
                .unwrap()
                .unwrap()
                .ptr_eq(&before.raw_routes["topic"])
        );
        let direct = cache.snapshot().unwrap();
        assert_eq!(
            direct.broker_address_by_topic("topic").unwrap().as_deref(),
            case["topicAddressAfterDirectPut"].as_str()
        );
        assert_eq!(
            direct.publish_address("broker"),
            case["publishAddressAfterDirectPut"].as_str()
        );
        assert!(Arc::ptr_eq(
            &direct.topics["topic"],
            &before.topics["topic"]
        ));
        assert_eq!(
            before.broker_address_by_topic("topic").unwrap().as_deref(),
            Some("master:10911")
        );
        table.clear().unwrap();
        let cleared = cache.snapshot().unwrap();
        assert!(table.get("topic").unwrap().is_none());
        assert_eq!(
            cleared.broker_address_by_topic("topic").unwrap().as_deref(),
            case["topicAddressAfterClear"].as_str()
        );
        assert_eq!(
            cleared.publish_address("broker"),
            case["publishAddressAfterClear"].as_str()
        );
        cache.clean_offline().unwrap();
        assert_eq!(
            cache.snapshot().unwrap().publish_address("broker"),
            case["publishAddressAfterOfflineClean"].as_str()
        );
        // Raw absence must force publication even when converted data is equal.
        assert!(cache.route_changed("topic", &first).unwrap());
        assert!(cache.replace("topic", first).unwrap());
        assert!(table.contains_key("topic").unwrap());
        assert_eq!(before.publish_address("broker"), Some("master:10911"));
        let mut invalid = before.raw_routes["topic"].snapshot().unwrap();
        invalid.broker_datas.as_mut().unwrap()[0]
            .as_mut()
            .unwrap()
            .broker_addrs = None;
        let invalid = TopicRouteDataHandle::new(invalid);
        table.put("invalid", invalid.clone()).unwrap();
        assert_eq!(
            cache
                .replace("invalid", invalid.snapshot().unwrap())
                .unwrap(),
            case["unchangedInvalidRawRefreshChanged"]
        );
        assert_eq!(
            table.get("invalid").unwrap().unwrap().ptr_eq(&invalid),
            case["unchangedInvalidRawRefreshKeepsCachedObject"]
        );
    }

    #[test]
    fn metadata_only_broker_changes_match_invoked_java_route_equality_and_refresh() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let case = &fixture["runtimeTopicRouteTable"];
        let initial: TopicRouteData = serde_json::from_value(case["initialInput"].clone()).unwrap();
        let metadata: TopicRouteData =
            serde_json::from_value(case["metadataOnlyInput"].clone()).unwrap();
        assert_eq!(initial, metadata); // TopicRouteData delegates to Java BrokerData.equals.
        assert_eq!(
            routes_equal(&initial, &metadata).unwrap(),
            case["brokerMetadataIgnoredByRouteEquals"]
        );
        let cache = RouteCache::default();
        assert!(cache.replace("topic", initial.clone()).unwrap());
        let before = cache.snapshot().unwrap();
        assert_eq!(
            cache.route_changed("topic", &metadata).unwrap(),
            case["metadataOnlyRefreshChanged"]
        );
        assert_eq!(
            cache.replace("topic", metadata).unwrap(),
            case["metadataOnlyRefreshChanged"]
        );
        let after = cache.snapshot().unwrap();
        assert_eq!(
            Arc::ptr_eq(&before, &after),
            case["metadataOnlyRefreshKeepsCachedObject"]
        );
        assert_eq!(after.topics["topic"].route.as_ref(), &initial);
    }

    #[test]
    fn route_change_sort_matches_invoked_java_broker_comparison_errors() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["topicRouteDataChanged"].as_array().unwrap() {
            let current: TopicRouteData = serde_json::from_value(case["current"].clone()).unwrap();
            let cache = RouteCache::default();
            if !case["old"].is_null() {
                let old: TopicRouteData = serde_json::from_value(case["old"].clone()).unwrap();
                super::super::TopicRouteTable::new(cache.clone())
                    .put("topic", TopicRouteDataHandle::new(old))
                    .unwrap();
            }
            let result = cache.route_changed("topic", &current);
            if case["outcome"] == "OK" {
                assert_eq!(result.unwrap(), case["changed"]);
            } else {
                assert!(matches!(result, Err(RuntimeError::InvalidRoute(_))));
            }
        }
    }

    #[test]
    fn ordered_integer_parser_matches_invoked_java_digits_and_signed_boundaries() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let zeros = fixture["integerDigitZeroCodeUnits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_u64().unwrap() as u16)
            .collect::<Vec<_>>();
        for unit in 0..=u16::MAX {
            let expected = zeros
                .iter()
                .find_map(|zero| unit.checked_sub(*zero).filter(|digit| *digit < 10));
            assert_eq!(java_decimal_digit(unit), expected, "UTF-16 unit {unit:x}");
        }
        for case in fixture["integers"].as_array().unwrap() {
            let expected = case["value"].as_i64().map(|value| value as i32);
            assert_eq!(
                java_integer(case["input"].as_str().unwrap()),
                expected,
                "{case}"
            );
        }
    }

    #[test]
    fn nullable_broker_address_values_match_invoked_java_refresh_and_lookups() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["runtimeNullBrokerValues"].as_array().unwrap() {
            assert_eq!(case["outcome"], "OK");
            assert_eq!(case["decodedMasterKeyPresent"], true);
            assert_eq!(case["decodedMasterNull"], true);
            assert_eq!(case["cachedMasterKeyPresent"], true);
            assert_eq!(case["cachedMasterNull"], true);
            let route: TopicRouteData = serde_json::from_value(case["input"].clone())
                .expect("Java accepts null address values");
            let cache = RouteCache::default();
            assert_eq!(cache.replace("topic", route).unwrap(), case["changed"]);
            let snapshot = cache.snapshot().unwrap();
            let topic = &snapshot.topics["topic"];
            let raw = serde_json::to_value(&*topic.route).unwrap();
            assert!(raw["brokerDatas"][0]["brokerAddrs"].get("0").is_some());
            assert!(raw["brokerDatas"][0]["brokerAddrs"]["0"].is_null());
            assert_eq!(
                topic.publish_queues.len() as u64,
                case["publishQueueCount"].as_u64().unwrap()
            );
            assert_eq!(
                topic.subscribe_queues.len() as u64,
                case["subscribeQueueCount"].as_u64().unwrap()
            );
            assert_eq!(
                snapshot.publish_address("broker"),
                case["publishAddress"].as_str()
            );
            assert_eq!(
                snapshot.admin_address("broker").map(|peer| peer.address),
                case["adminAddress"].as_str().map(str::to_owned)
            );
            assert_eq!(
                snapshot
                    .subscribe_address("broker", 0, true)
                    .map(|peer| peer.address),
                case["subscribeExactMasterAddress"]
                    .as_str()
                    .map(str::to_owned)
            );
            assert_eq!(case["topicSelectionOnlyAllowedChoices"], true);
            assert_eq!(snapshot.select_topic_broker("topic", |_| 0).unwrap(), None);
            if case["adminAddress"].is_string() {
                assert_eq!(
                    snapshot
                        .select_topic_broker("topic", |length| usize::from(length > 1))
                        .unwrap(),
                    case["adminAddress"].as_str().map(str::to_owned)
                );
            }
            for _ in 0..128 {
                let selected = snapshot.broker_address_by_topic("topic").unwrap();
                assert!(selected.is_none() || selected.as_deref() == case["adminAddress"].as_str());
            }
        }
    }

    #[test]
    fn null_exact_subscribe_address_tries_adjacent_then_first_entry() {
        let mut snapshot = RouteSnapshot::default();
        snapshot.broker_addresses.insert(
            "broker".into(),
            BTreeMap::from([
                (0, Some("master:10911".into())),
                (1, None),
                (2, Some("slave:10911".into())),
            ]),
        );
        assert_eq!(
            snapshot
                .subscribe_address("broker", 1, true)
                .unwrap()
                .address,
            "slave:10911"
        );
        snapshot
            .broker_addresses
            .get_mut("broker")
            .unwrap()
            .remove(&2);
        assert!(snapshot.subscribe_address("broker", 1, true).is_none());
        let fallback = snapshot.subscribe_address("broker", 1, false).unwrap();
        assert_eq!(fallback.address, "master:10911");
        assert!(!fallback.slave);
        snapshot
            .broker_addresses
            .get_mut("broker")
            .unwrap()
            .insert(0, None);
        assert!(snapshot.subscribe_address("broker", 1, false).is_none());
        assert!(snapshot.admin_address("broker").is_none());
    }

    #[test]
    fn offline_cleanup_preserves_live_null_values_but_removes_stale_ones() {
        let cache = RouteCache::default();
        let mut route = route(1);
        route.broker_datas.as_mut().unwrap()[0]
            .as_mut()
            .unwrap()
            .broker_addrs = Some(BTreeMap::from([(0, None)]));
        cache.replace("topic", route).unwrap();
        cache.clean_offline().unwrap();
        assert_eq!(
            cache.snapshot().unwrap().broker_addresses["broker"][&0],
            None
        );
        cache.update_raw(|raw| (true, raw.clear())).unwrap();
        cache.clean_offline().unwrap();
        assert!(cache.snapshot().unwrap().broker_addresses.is_empty());
    }

    #[test]
    fn null_broker_address_map_fails_refresh_without_replacing_coherent_cache() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let case = &fixture["runtimeNullBrokerMap"];
        assert_eq!(case["outcome"], "NullPointerException");
        assert_eq!(case["initialChanged"], true);
        assert_eq!(case["cachedRouteUnchanged"], true);
        let cache = RouteCache::default();
        cache
            .replace(
                "topic",
                serde_json::from_value(case["initialInput"].clone()).unwrap(),
            )
            .unwrap();
        let before = cache.snapshot().unwrap();
        let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
        assert!(matches!(
            cache.replace("topic", route.clone()),
            Err(RuntimeError::InvalidRoute(_))
        ));
        assert!(Arc::ptr_eq(&before, &cache.snapshot().unwrap()));
        // A legitimate empty map remains distinct from null and is accepted.
        let mut empty = route;
        empty.broker_datas.as_mut().unwrap()[0]
            .as_mut()
            .unwrap()
            .broker_addrs = Some(BTreeMap::new());
        assert!(cache.replace("topic", empty).unwrap());
    }

    #[test]
    fn static_to_ordinary_refresh_retains_endpoints_like_invoked_java_runtime() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let case = &fixture["runtimeEmptyEndpointTransition"];
        assert_eq!(case["initialChanged"], true);
        assert_eq!(case["changed"], true);
        assert_eq!(case["retained"], true);
        assert_eq!(case["rawRouteHasStaticMappings"], false);
        let cache = RouteCache::default();
        cache
            .replace(
                "topic",
                serde_json::from_value(case["initialInput"].clone()).unwrap(),
            )
            .unwrap();
        let before = cache.snapshot().unwrap();
        assert!(!before.topics["topic"].endpoints.is_empty());
        let ordinary: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
        assert!(cache.replace("topic", ordinary.clone()).unwrap());
        let after = cache.snapshot().unwrap();
        assert!(!Arc::ptr_eq(&before, &after));
        assert_eq!(after.topics["topic"].route.as_ref(), &ordinary);
        assert_eq!(
            after.topics["topic"].endpoints,
            before.topics["topic"].endpoints
        );
        assert_eq!(after.topics["topic"].publish_queues.len(), 2);
        assert_eq!(after.topics["topic"].subscribe_queues.len(), 3);
        for (snapshot, rows) in [(&before, &case["before"]), (&after, &case["after"])] {
            assert_eq!(
                snapshot.topics["topic"].endpoints.len(),
                rows.as_array().unwrap().len()
            );
            for row in rows.as_array().unwrap() {
                let queue: MessageQueue = serde_json::from_value(row["queue"].clone()).unwrap();
                assert_eq!(snapshot.broker_name(&queue), row["broker"].as_str());
            }
        }
        assert!(
            before.topics["topic"]
                .route
                .topic_queue_mapping_by_broker
                .is_some()
        );
        assert!(
            after.topics["topic"]
                .route
                .topic_queue_mapping_by_broker
                .is_none()
        );
    }
    use rocketmq_classic_protocol::route::{BrokerData, QueueData};

    #[test]
    fn topic_selection_uses_raw_route_and_selected_empty_broker_is_not_skipped() {
        let cache = RouteCache::default();
        let mut original = route(1);
        original.broker_datas.as_mut().unwrap()[0]
            .as_mut()
            .unwrap()
            .broker_addrs = Some(BTreeMap::from([
            (2, Some("slave-a:10911".into())),
            (3, Some("slave-b:10911".into())),
        ]));
        original
            .broker_datas
            .as_mut()
            .unwrap()
            .push(route(2).broker_datas.unwrap().remove(0));
        cache.replace("original", original.clone()).unwrap();
        cache.replace("overwriter", route(9)).unwrap();
        let snapshot = cache.snapshot().unwrap();
        assert_eq!(snapshot.publish_address("broker"), Some("master-9:10911"));
        let mut choices = [0, 1].into_iter();
        assert_eq!(
            snapshot
                .select_topic_broker("original", |length| {
                    assert_eq!(length, 2);
                    choices.next().unwrap()
                })
                .unwrap(),
            Some("slave-b:10911".to_owned())
        );
        assert_eq!(
            snapshot.select_topic_broker("original", |_| 1).unwrap(),
            Some("master-2:10911".to_owned())
        );
        original.broker_datas.as_mut().unwrap()[0]
            .as_mut()
            .unwrap()
            .broker_addrs = Some(BTreeMap::new());
        cache.replace("original", original).unwrap();
        assert!(matches!(
            cache
                .snapshot()
                .unwrap()
                .select_topic_broker("original", |_| 0),
            Err(RuntimeError::InvalidRoute(_))
        ));
    }

    #[test]
    fn topic_broker_lookup_matches_invoked_java_candidates_and_failures() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/lookup.json")).unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let mut snapshot = RouteSnapshot::default();
            if !case["input"].is_null() {
                // The Java lookup oracle reflects directly into its raw route
                // table, including states normal refresh cannot publish. Match
                // that test setup without bypassing production cache validation.
                snapshot.raw_routes.insert(
                    "topic".into(),
                    TopicRouteDataHandle::new(
                        serde_json::from_value(case["input"].clone()).unwrap(),
                    ),
                );
            }
            for _ in 0..128 {
                match case["outcome"].as_str().unwrap() {
                    "NULL" => assert_eq!(snapshot.broker_address_by_topic("topic").unwrap(), None),
                    "OK" => {
                        let selected = snapshot.broker_address_by_topic("topic").unwrap().unwrap();
                        assert!(
                            case["candidates"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .any(|value| value.as_str() == Some(selected.as_str()))
                        );
                    }
                    "IllegalArgumentException" | "NullPointerException" => {
                        assert!(matches!(
                            snapshot.broker_address_by_topic("topic"),
                            Err(RuntimeError::InvalidRoute(_))
                        ));
                    }
                    outcome => panic!("unexpected Java outcome {outcome}"),
                }
            }
        }
    }

    fn route(count: i32) -> TopicRouteData {
        TopicRouteData {
            queue_datas: Some(vec![Some(QueueData {
                broker_name: Some("broker".into()),
                read_queue_nums: count,
                write_queue_nums: count,
                perm: 6,
                ..Default::default()
            })]),
            broker_datas: Some(vec![Some(BrokerData {
                broker_name: Some("broker".into()),
                broker_addrs: Some(BTreeMap::from([
                    (0, Some(format!("master-{count}:10911"))),
                    (2, Some("slave:10911".into())),
                ])),
                ..Default::default()
            })]),
            ..Default::default()
        }
    }

    #[test]
    fn admin_resolution_reports_selected_peer_version_and_subscription_preserves_adjacent_slave_flag()
     {
        let mut snapshot = RouteSnapshot::default();
        assert!(snapshot.admin_address("missing").is_none());
        snapshot
            .broker_addresses
            .insert("empty".into(), BTreeMap::new());
        assert!(snapshot.admin_address("empty").is_none());
        snapshot
            .broker_addresses
            .insert("broker".into(), BTreeMap::from([(2, Some("slave".into()))]));
        assert_eq!(
            snapshot.admin_address("broker"),
            Some(BrokerAddress {
                address: "slave".into(),
                slave: true,
                version: 0
            })
        );
        snapshot
            .broker_versions
            .insert(("broker".into(), "slave".into()), 551);
        assert_eq!(snapshot.admin_address("broker").unwrap().version, 551);
        snapshot
            .broker_addresses
            .get_mut("broker")
            .unwrap()
            .insert(0, Some("master".into()));
        // Java's admin method chooses an available map entry, not a portable
        // master-first contract. Assert metadata for the actually selected peer.
        let admin = snapshot.admin_address("broker").unwrap();
        assert_eq!(admin.slave, admin.address != "master");
        assert_eq!(admin.version, if admin.slave { 551 } else { 0 });
        let adjacent = snapshot.subscribe_address("broker", -1, true).unwrap();
        assert_eq!(adjacent.address, "master");
        assert!(adjacent.slave); // Java keeps the originally requested nonzero-ID flag.
        assert!(!snapshot.subscribe_address("broker", 0, true).unwrap().slave);
        assert!(snapshot.subscribe_address("broker", 7, true).is_none());
    }

    #[test]
    fn heartbeat_versions_are_coherent_and_offline_cleanup_checks_all_topics() {
        let cache = RouteCache::default();
        cache.replace("first", route(1)).unwrap();
        cache
            .record_version("broker", "master-1:10911", 551)
            .unwrap();
        let old = cache.snapshot().unwrap();
        assert_eq!(
            old.subscribe_address("broker", 0, true).unwrap().version,
            551
        );
        cache.replace("second", route(2)).unwrap();
        cache.replace("second", TopicRouteData::default()).unwrap();
        cache.clean_offline().unwrap();
        let next = cache.snapshot().unwrap();
        assert!(next.publish_address("broker").is_none());
        assert!(next.subscribe_address("broker", 2, true).is_some()); // first topic still references this slave
        assert_eq!(
            next.broker_versions
                .get(&("broker".into(), "master-1:10911".into())),
            Some(&551)
        );
        assert_eq!(old.publish_address("broker"), Some("master-1:10911"));
        cache.replace("first", TopicRouteData::default()).unwrap();
        cache.clean_offline().unwrap();
        assert!(cache.snapshot().unwrap().broker_addresses.is_empty());
    }

    #[test]
    fn unchanged_route_requested_by_owner_republishes_its_broker_table() {
        let cache = RouteCache::default();
        cache.replace("first", route(1)).unwrap();
        cache.replace("second", route(2)).unwrap();
        let before = cache.snapshot().unwrap();
        assert_eq!(before.publish_address("broker"), Some("master-2:10911"));
        assert!(!cache.replace("first", route(1)).unwrap());
        assert!(Arc::ptr_eq(&before, &cache.snapshot().unwrap()));
        assert!(
            !cache
                .replace_for_owner_update("first", route(1), true)
                .unwrap()
        );
        let after = cache.snapshot().unwrap();
        assert_eq!(after.publish_address("broker"), Some("master-1:10911"));
        assert_eq!(before.publish_address("broker"), Some("master-2:10911"));
        assert!(after.topics.contains_key("second"));
    }

    #[test]
    fn coherent_snapshot_keeps_raw_broker_publish_and_subscribe_state_together() {
        let cache = Arc::new(RouteCache::default());
        cache.replace("topic", route(1)).unwrap();
        let old = cache.snapshot().unwrap();
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        let snapshot = cache.snapshot().unwrap();
                        let topic = &snapshot.topics["topic"];
                        let count = topic.route.queue_datas.as_ref().unwrap()[0]
                            .as_ref()
                            .unwrap()
                            .read_queue_nums as usize;
                        assert_eq!(topic.publish_queues.len(), count);
                        assert_eq!(topic.subscribe_queues.len(), count);
                        assert_eq!(
                            snapshot.publish_address("broker"),
                            Some(format!("master-{count}:10911").as_str())
                        );
                    }
                })
            })
            .collect();
        for _ in 0..100 {
            cache.replace("topic", route(2)).unwrap();
            cache.replace("topic", route(1)).unwrap();
        }
        for reader in readers {
            reader.join().unwrap();
        }
        assert_eq!(old.topics["topic"].publish_queues.len(), 1);
        assert!(!cache.replace("topic", route(1)).unwrap());
        let mut malformed = route(3);
        malformed.order_topic_conf = Some("broker:invalid".into());
        assert!(cache.replace("topic", malformed).is_err());
        assert_eq!(
            cache.snapshot().unwrap().topics["topic"]
                .publish_queues
                .len(),
            1
        );
    }

    #[test]
    fn ordered_conversion_uses_java_first_count_and_ignores_trailing_fields() {
        let cache = RouteCache::default();
        let mut ordered = route(1);
        ordered.order_topic_conf = Some("broker:2:ignored;".into());
        cache.replace("topic", ordered).unwrap();
        let snapshot = cache.snapshot().unwrap();
        assert_eq!(snapshot.topics["topic"].publish_queues.len(), 2);
        assert!(snapshot.topics["topic"].order_topic);
        assert_eq!(
            snapshot.topics["topic"].route.order_topic_conf.as_deref(),
            Some("broker:2:ignored;")
        );
    }

    #[test]
    fn subscribe_resolves_exact_adjacent_slave_and_optional_fallback() {
        let cache = RouteCache::default();
        cache.replace("topic", route(1)).unwrap();
        let snapshot = cache.snapshot().unwrap();
        assert!(!snapshot.subscribe_address("broker", 0, true).unwrap().slave);
        assert_eq!(
            snapshot
                .subscribe_address("broker", 1, true)
                .unwrap()
                .address,
            "slave:10911"
        );
        assert!(snapshot.subscribe_address("broker", 3, true).is_none());
        assert_eq!(
            snapshot
                .subscribe_address("broker", 3, false)
                .unwrap()
                .address,
            "master-1:10911"
        );
        assert!(snapshot.publish_address("missing").is_none());
        assert_eq!(
            snapshot.broker_name(&MessageQueue::new("topic", "broker", 0)),
            Some("broker")
        );
    }

    #[test]
    fn hostile_queue_counts_are_rejected_before_expanding_collections() {
        let cache = RouteCache::default();
        cache.replace("topic", route(1)).unwrap();
        let before = cache.snapshot().unwrap();
        assert!(cache.replace("topic", route(i32::MAX)).is_err());
        assert!(Arc::ptr_eq(&before, &cache.snapshot().unwrap()));
    }

    #[test]
    fn budget_counts_effective_static_space_and_separator_only_order_not_unused_metadata() {
        use rocketmq_classic_protocol::route::TopicQueueMappingInfo;
        let cache = RouteCache::default();
        let mut ordered = route(0);
        ordered.queue_datas.as_mut().unwrap()[0]
            .as_mut()
            .unwrap()
            .write_queue_nums = i32::MAX;
        ordered.order_topic_conf = Some(";;".into());
        cache.replace("topic", ordered).unwrap();
        assert!(
            cache.snapshot().unwrap().topics["topic"]
                .publish_queues
                .is_empty()
        );
        let mut repeated = route(i32::MAX);
        repeated.topic_queue_mapping_by_broker = Some(
            (0..1_000)
                .map(|index| {
                    (
                        index.to_string(),
                        Some(TopicQueueMappingInfo {
                            total_queues: 1_000,
                            epoch: 0,
                            scope: Some("__global__".into()),
                            curr_id_map: Some(BTreeMap::new()),
                            ..Default::default()
                        }),
                    )
                })
                .collect(),
        );
        cache.replace("topic", repeated.clone()).unwrap();
        assert_eq!(
            cache.snapshot().unwrap().topics["topic"]
                .publish_queues
                .len(),
            1_000
        );
        assert_eq!(
            cache.snapshot().unwrap().topics["topic"].endpoints.len(),
            1_000
        );
        repeated
            .topic_queue_mapping_by_broker
            .as_mut()
            .unwrap()
            .get_mut("0")
            .unwrap()
            .as_mut()
            .unwrap()
            .total_queues = i32::MAX;
        let before = cache.snapshot().unwrap();
        assert!(cache.replace("topic", repeated).is_err());
        assert!(Arc::ptr_eq(&before, &cache.snapshot().unwrap()));
    }

    #[test]
    fn vip_transformation_preserves_valid_ipv6_and_rejects_java_parse_failures() {
        for (input, expected) in [
            ("broker:10911", "broker:10909"),
            ("[2001:db8::1]:10911", "[2001:db8::1]:10909"),
            ("2001:db8::1:10911", "2001:db8::1:10909"),
        ] {
            assert_eq!(broker_vip_channel(true, input).unwrap(), expected);
            assert_eq!(broker_vip_channel(false, input).unwrap(), input);
        }
        for input in ["missing", "broker: 2", "broker:", "broker:999999999999"] {
            assert!(broker_vip_channel(true, input).is_err(), "{input}");
        }
    }

    #[test]
    fn vip_helper_matches_invoked_java_int_and_split_semantics() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["vipCases"].as_array().unwrap() {
            let address = case["address"].as_str().unwrap();
            let actual = broker_vip_channel(true, address);
            let java_accepts = case["enabledOutcome"] == "OK";
            assert_eq!(actual.is_ok(), java_accepts, "{address}: {actual:?}");
            match actual {
                Ok(actual) => {
                    assert_eq!(actual, case["enabledValue"].as_str().unwrap(), "{address}");
                }
                Err(actual) => match case["enabledOutcome"].as_str().unwrap() {
                    "StringIndexOutOfBoundsException" => {
                        assert!(matches!(actual, RuntimeError::InvalidBrokerAddress));
                    }
                    "NumberFormatException" => {
                        assert!(matches!(actual, RuntimeError::InvalidBrokerPort));
                    }
                    other => panic!("unmapped Java VIP exception {other} for {address}"),
                },
            }
            assert_eq!(
                broker_vip_channel(false, address).unwrap(),
                case["disabled"].as_str().unwrap(),
                "disabled {address}"
            );
        }
    }

    #[test]
    fn snapshots_and_vip_compare_actual_pinned_java_conversion_results() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let cache = RouteCache::default();
            let route = serde_json::from_value(case["input"].clone()).unwrap();
            if case["outcome"] == "OK" {
                cache.replace("topic", route).unwrap();
                let snapshot = cache.snapshot().unwrap();
                let topic = &snapshot.topics["topic"];
                assert_eq!(
                    serde_json::to_value(&topic.publish_queues).unwrap(),
                    case["publish"]
                );
                assert_eq!(
                    serde_json::to_value(&topic.subscribe_queues).unwrap(),
                    case["subscribe"]
                );
                assert_eq!(topic.order_topic, case["ordered"].as_bool().unwrap());
                assert_eq!(
                    serde_json::to_value(
                        topic
                            .endpoints
                            .iter()
                            .map(
                                |(queue, broker)| serde_json::json!({"queue":queue,"broker":broker})
                            )
                            .collect::<Vec<_>>()
                    )
                    .unwrap(),
                    case["endpoints"]
                );
            } else {
                assert!(cache.replace("topic", route).is_err());
                assert!(cache.snapshot().unwrap().topics.is_empty());
            }
        }
        for (input, output) in fixture["vip"].as_object().unwrap() {
            assert_eq!(
                broker_vip_channel(true, input).unwrap(),
                output.as_str().unwrap()
            );
        }
    }
}

#[cfg(test)]
#[path = "cache_nullable_elements.rs"]
mod nullable_element_tests;
