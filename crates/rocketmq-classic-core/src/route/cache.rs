use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_protocol::route::TopicRouteData;

use crate::runtime::RuntimeError;

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
    pub topics: BTreeMap<String, Arc<TopicSnapshot>>,
    pub broker_addresses: BTreeMap<String, BTreeMap<i64, String>>,
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
    pub fn broker_address_by_topic(&self, topic: &str) -> Result<Option<&str>, RuntimeError> {
        self.select_topic_broker(topic, |length| rand::random_range(0..length))
    }

    fn select_topic_broker(
        &self,
        topic: &str,
        mut select: impl FnMut(usize) -> usize,
    ) -> Result<Option<&str>, RuntimeError> {
        let Some(route) = self.topics.get(topic) else {
            return Ok(None);
        };
        let brokers = &route.route.broker_datas;
        if brokers.is_empty() {
            return Ok(None);
        }
        let broker = &brokers[select(brokers.len())];
        let addresses = broker
            .broker_addrs
            .as_ref()
            .ok_or(RuntimeError::InvalidRoute(
                "selected broker has null address map",
            ))?;
        if let Some(master) = addresses.get(&0) {
            return Ok(Some(master));
        }
        if addresses.is_empty() {
            return Err(RuntimeError::InvalidRoute(
                "selected broker has no addresses",
            ));
        }
        Ok(addresses
            .values()
            .nth(select(addresses.len()))
            .map(String::as_str))
    }

    pub fn publish_address(&self, broker: &str) -> Option<&str> {
        self.broker_addresses
            .get(broker)?
            .get(&0)
            .map(String::as_str)
    }

    pub fn subscribe_address(
        &self,
        broker: &str,
        id: i64,
        only_this_broker: bool,
    ) -> Option<BrokerAddress> {
        let addresses = self.broker_addresses.get(broker)?;
        let exact_or_adjacent = addresses.get_key_value(&id).or_else(|| {
            (id != 0)
                .then(|| addresses.get_key_value(&id.wrapping_add(1)))
                .flatten()
        });
        let selected = exact_or_adjacent.or_else(|| {
            (!only_this_broker)
                .then(|| addresses.first_key_value())
                .flatten()
        })?;
        Some(BrokerAddress {
            address: selected.1.clone(),
            // Java changes the slave flag only for the arbitrary-entry fallback,
            // not for brokerId + 1 (including signed overflow/negative IDs).
            slave: if exact_or_adjacent.is_some() {
                id != 0
            } else {
                *selected.0 != 0
            },
            version: self
                .broker_versions
                .get(&(broker.to_owned(), selected.1.clone()))
                .copied()
                .unwrap_or(0),
        })
    }

    pub fn admin_address(&self, broker: &str) -> Option<BrokerAddress> {
        // Java iterates HashMap entries until it finds an address. String values
        // in this typed snapshot are non-null; entry order is not a contract.
        let (id, address) = self.broker_addresses.get(broker)?.first_key_value()?;
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
        let endpoints = self.topics.get(queue.topic()).map(|topic| &topic.endpoints);
        match endpoints.filter(|endpoints| !endpoints.is_empty()) {
            Some(endpoints) => endpoints.get(queue).map(String::as_str),
            None => Some(queue.broker_name()),
        }
    }
}

#[derive(Default)]
pub(crate) struct RouteCache {
    snapshot: RwLock<Arc<RouteSnapshot>>,
}

impl RouteCache {
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
        let mut current = self
            .snapshot
            .write()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let live = current
            .topics
            .values()
            .flat_map(|topic| topic.route.broker_datas.iter())
            .filter_map(|broker| broker.broker_addrs.as_ref())
            .flat_map(|addresses| addresses.values())
            .cloned()
            .collect::<BTreeSet<_>>();
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
        Ok(!self
            .snapshot()?
            .topics
            .get(topic)
            .is_some_and(|old| routes_equal(&old.route, route)))
    }

    pub(crate) fn replace_for_owner_update(
        &self,
        topic: &str,
        route: TopicRouteData,
        force: bool,
    ) -> Result<bool, RuntimeError> {
        // Java's brokerAddrTable is a ConcurrentHashMap: publishing a null
        // address map fails, it is not a valid empty Broker. Validate before
        // any conversion/publication to preserve the coherent native snapshot.
        if route
            .broker_datas
            .iter()
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
                route: Arc::new(route),
            };
        let mut current = self
            .snapshot
            .write()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let unchanged = current
            .topics
            .get(topic)
            .is_some_and(|old| routes_equal(&old.route, &topic_snapshot.route));
        if unchanged && !force {
            return Ok(false);
        }
        // Java retains existing endpoints when a new conversion is empty.
        if topic_snapshot.endpoints.is_empty() {
            if let Some(old) = current.topics.get(topic) {
                topic_snapshot.endpoints.clone_from(&old.endpoints);
            }
        }
        let mut next = current.as_ref().clone();
        for broker in &topic_snapshot.route.broker_datas {
            next.broker_addresses.insert(
                broker.broker_name.clone(),
                broker.broker_addrs.clone().unwrap_or_default(),
            );
        }
        next.topics
            .insert(topic.to_owned(), Arc::new(topic_snapshot));
        *current = Arc::new(next);
        Ok(!unchanged)
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
        for mapping in mappings.values() {
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
    let mut read_counts = BTreeMap::<&str, i32>::new();
    for queue in &route.queue_datas {
        if mappings.is_none() && queue.perm & 4 != 0 {
            let count = read_counts.entry(&queue.broker_name).or_default();
            *count = (*count).max(queue.read_queue_nums);
        }
        if ordered.is_none()
            && !(route.order_topic_conf.is_none() && mappings.is_some())
            && queue.perm & 2 != 0
        {
            let master = route
                .broker_datas
                .iter()
                .find(|broker| broker.broker_name == queue.broker_name)
                .and_then(|broker| broker.broker_addrs.as_ref())
                .is_some_and(|addresses| addresses.contains_key(&0));
            if master {
                add(queue.write_queue_nums, &queue.broker_name)?;
            }
        }
    }
    for (broker, count) in read_counts {
        add(count, broker)?;
    }
    Ok(())
}

// Integer.parseInt iterates Java char/UTF-16 units and Character.digit(10),
// accepting BMP decimal scripts but not supplementary mathematical digits.
// Accumulate negatively so i32::MIN is representable without widening/range loss.
fn java_integer(value: &str) -> Option<i32> {
    let mut units = value.encode_utf16().peekable();
    let negative = match units.peek().copied()? {
        0x2d => {
            units.next();
            true
        }
        0x2b => {
            units.next();
            false
        }
        _ => false,
    };
    units.peek()?;
    let mut result = 0i32;
    for unit in units {
        let digit = java_decimal_digit(unit)?;
        result = result.checked_mul(10)?.checked_sub(i32::from(digit))?;
    }
    if negative {
        Some(result)
    } else {
        result.checked_neg()
    }
}

fn java_decimal_digit(unit: u16) -> Option<u16> {
    // Audited exhaustively against actual Character.digit(char, 10) zero
    // ranges in the pinned Java harness, not char::is_numeric or to_digit.
    const ZEROS: &[u16] = &[
        0x0030, 0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66, 0x0be6, 0x0c66,
        0x0ce6, 0x0d66, 0x0de6, 0x0e50, 0x0ed0, 0x0f20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946,
        0x19d0, 0x1a80, 0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0,
        0xa9f0, 0xaa50, 0xabf0, 0xff10,
    ];
    ZEROS
        .iter()
        .find_map(|zero| unit.checked_sub(*zero).filter(|digit| *digit < 10))
}

fn routes_equal(left: &TopicRouteData, right: &TopicRouteData) -> bool {
    let (mut left, mut right) = (left.clone(), right.clone());
    for route in [&mut left, &mut right] {
        route
            .queue_datas
            .sort_by(|a, b| a.broker_name.cmp(&b.broker_name));
        route
            .broker_datas
            .sort_by(|a, b| a.broker_name.cmp(&b.broker_name));
    }
    left == right
}

/// Java MixAll.brokerVIPChannel, with checked socket ports and bracketed IPv6.
pub fn broker_vip_channel(enabled: bool, address: &str) -> Result<String, RuntimeError> {
    if !enabled {
        return Ok(address.to_owned());
    }
    let (host, port) = address
        .rsplit_once(':')
        .ok_or(RuntimeError::InvalidBrokerAddress)?;
    let port = port
        .parse::<u16>()
        .ok()
        .and_then(|port| port.checked_sub(2))
        .filter(|port| *port > 0)
        .ok_or(RuntimeError::InvalidBrokerAddress)?;
    if host.is_empty()
        || (host.contains(':')
            && host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<std::net::Ipv6Addr>()
                .is_err())
    {
        return Err(RuntimeError::InvalidBrokerAddress);
    }
    Ok(format!("{host}:{port}"))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        empty.broker_datas[0].broker_addrs = Some(BTreeMap::new());
        assert!(cache.replace("topic", empty).unwrap());
    }
    use rocketmq_classic_protocol::route::{BrokerData, QueueData};

    #[test]
    fn topic_selection_uses_raw_route_and_selected_empty_broker_is_not_skipped() {
        let cache = RouteCache::default();
        let mut original = route(1);
        original.broker_datas[0].broker_addrs = Some(BTreeMap::from([
            (2, "slave-a:10911".into()),
            (3, "slave-b:10911".into()),
        ]));
        original.broker_datas.push(route(2).broker_datas.remove(0));
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
            Some("slave-b:10911")
        );
        assert_eq!(
            snapshot.select_topic_broker("original", |_| 1).unwrap(),
            Some("master-2:10911")
        );
        original.broker_datas[0].broker_addrs = Some(BTreeMap::new());
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
                snapshot.topics.insert(
                    "topic".into(),
                    Arc::new(TopicSnapshot {
                        route: Arc::new(serde_json::from_value(case["input"].clone()).unwrap()),
                        publish_queues: Vec::new(),
                        subscribe_queues: BTreeSet::new(),
                        endpoints: BTreeMap::new(),
                        order_topic: false,
                    }),
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
                                .any(|value| value == selected)
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
            queue_datas: vec![QueueData {
                broker_name: "broker".into(),
                read_queue_nums: count,
                write_queue_nums: count,
                perm: 6,
                ..Default::default()
            }],
            broker_datas: vec![BrokerData {
                broker_name: "broker".into(),
                broker_addrs: Some(BTreeMap::from([
                    (0, format!("master-{count}:10911")),
                    (2, "slave:10911".into()),
                ])),
                ..Default::default()
            }],
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
            .insert("broker".into(), BTreeMap::from([(2, "slave".into())]));
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
            .insert(0, "master".into());
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
                        let count = topic.route.queue_datas[0].read_queue_nums as usize;
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
        ordered.queue_datas[0].write_queue_nums = i32::MAX;
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
                        TopicQueueMappingInfo {
                            total_queues: 1_000,
                            epoch: 0,
                            scope: Some("__global__".into()),
                            curr_id_map: Some(BTreeMap::new()),
                            ..Default::default()
                        },
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
            .total_queues = i32::MAX;
        let before = cache.snapshot().unwrap();
        assert!(cache.replace("topic", repeated).is_err());
        assert!(Arc::ptr_eq(&before, &cache.snapshot().unwrap()));
    }

    #[test]
    fn vip_transformation_checks_ports_and_preserves_ipv6() {
        for (input, expected) in [
            ("broker:10911", "broker:10909"),
            ("[2001:db8::1]:10911", "[2001:db8::1]:10909"),
            ("2001:db8::1:10911", "2001:db8::1:10909"),
        ] {
            assert_eq!(broker_vip_channel(true, input).unwrap(), expected);
            assert_eq!(broker_vip_channel(false, input).unwrap(), input);
        }
        for input in [
            "missing",
            ":10911",
            "broker:2",
            "broker:-1",
            "broker:65536",
            "not:ipv6:10911",
        ] {
            assert!(broker_vip_channel(true, input).is_err(), "{input}");
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
