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
        let selected = addresses
            .get_key_value(&id)
            .or_else(|| {
                (id != 0)
                    .then(|| addresses.get_key_value(&id.wrapping_add(1)))
                    .flatten()
            })
            .or_else(|| {
                (!only_this_broker)
                    .then(|| addresses.first_key_value())
                    .flatten()
            })?;
        Some(BrokerAddress {
            address: selected.1.clone(),
            slave: *selected.0 != 0,
            version: self
                .broker_versions
                .get(&(broker.to_owned(), selected.1.clone()))
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
    pub(crate) fn snapshot(&self) -> Result<Arc<RouteSnapshot>, RuntimeError> {
        self.snapshot
            .read()
            .map(|value| value.clone())
            .map_err(|_| RuntimeError::RegistryPoisoned)
    }

    pub(crate) fn replace(&self, topic: &str, route: TopicRouteData) -> Result<bool, RuntimeError> {
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
                let count = entry
                    .split(':')
                    .nth(1)
                    .and_then(|value| value.parse::<i32>().ok());
                let count =
                    count.ok_or(RuntimeError::InvalidRoute("invalid ordered queue count"))?;
                normalized.push(format!(
                    "{}:{count}",
                    entry.split(':').next().unwrap_or_default()
                ));
            }
            conversion_route.order_topic_conf = Some(normalized.join(";"));
        }
        validate_conversion_size(topic, &conversion_route)?;
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
        if unchanged {
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
        Ok(true)
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
    for queue in &route.queue_datas {
        add(queue.read_queue_nums, &queue.broker_name)?;
        add(queue.write_queue_nums, &queue.broker_name)?;
    }
    if let Some(ordered) = route
        .order_topic_conf
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        for item in ordered.split(';') {
            let mut parts = item.split(':');
            let broker = parts.next().unwrap_or_default();
            let count = parts
                .next()
                .and_then(|value| value.parse::<i32>().ok())
                .unwrap_or(0);
            add(count, broker)?;
        }
    }
    if let Some(mappings) = &route.topic_queue_mapping_by_broker {
        for mapping in mappings.values() {
            add(mapping.total_queues, &mapping.bname)?;
        }
    }
    Ok(())
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
    use rocketmq_classic_protocol::route::{BrokerData, QueueData};

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
