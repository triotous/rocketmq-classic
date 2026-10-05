use std::collections::BTreeMap;

use rocketmq_classic_common::message::MessageQueue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueueData {
    pub broker_name: String,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
    pub topic_sys_flag: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BrokerData {
    pub cluster: String,
    pub broker_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_addrs: Option<BTreeMap<i64, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone_name: Option<String>,
    #[serde(default)]
    pub enable_acting_master: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TopicQueueMappingInfo {
    pub topic: String,
    pub total_queues: i32,
    pub bname: String,
    pub epoch: i64,
    #[serde(default)]
    pub dirty: bool,
    #[serde(default = "default_mapping_scope")]
    pub scope: Option<String>,
    #[serde(default)]
    pub curr_id_map: Option<BTreeMap<i32, i32>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_topic_conf: Option<String>,
    pub queue_datas: Vec<QueueData>,
    pub broker_datas: Vec<BrokerData>,
    pub filter_server_table: BTreeMap<String, Vec<String>>,
    pub topic_queue_mapping_by_broker: Option<BTreeMap<String, TopicQueueMappingInfo>>,
}

fn default_mapping_scope() -> Option<String> {
    Some("__global__".to_owned())
}

impl TopicRouteData {
    pub fn publish_queues(&self, topic: &str) -> Vec<MessageQueue> {
        if let Some(order_conf) = self
            .order_topic_conf
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            return order_conf
                .split(';')
                .filter_map(|entry| {
                    let (broker, count) = entry.split_once(':')?;
                    let count = count.parse::<i32>().ok()?;
                    Some((0..count).map(move |queue_id| MessageQueue::new(topic, broker, queue_id)))
                })
                .flatten()
                .collect();
        }
        if self.order_topic_conf.is_none() {
            if let Some(endpoints) = self.static_queue_endpoints(topic) {
                let mut queues = endpoints.into_keys().collect::<Vec<_>>();
                queues.sort_by_key(MessageQueue::queue_id);
                return queues;
            }
        }

        let mut masters = BTreeMap::new();
        for broker in &self.broker_datas {
            masters
                .entry(broker.broker_name.as_str())
                .or_insert_with(|| {
                    broker
                        .broker_addrs
                        .as_ref()
                        .is_some_and(|addresses| addresses.contains_key(&0))
                });
        }
        let mut queue_data = self.queue_datas.iter().collect::<Vec<_>>();
        queue_data.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
        queue_data
            .into_iter()
            .filter(|data| {
                data.write_queue_nums > 0
                    && data.perm & 0x2 != 0
                    && masters
                        .get(data.broker_name.as_str())
                        .copied()
                        .unwrap_or(false)
            })
            .flat_map(|data| {
                (0..data.write_queue_nums)
                    .map(|queue_id| MessageQueue::new(topic, &data.broker_name, queue_id))
            })
            .collect()
    }

    pub fn subscribe_queues(&self, topic: &str) -> Vec<MessageQueue> {
        if let Some(endpoints) = self.static_queue_endpoints(topic) {
            return endpoints.into_keys().collect();
        }
        let mut counts = BTreeMap::<&str, i32>::new();
        for data in self
            .queue_datas
            .iter()
            .filter(|data| data.read_queue_nums > 0 && data.perm & 0x4 != 0)
        {
            let count = counts.entry(&data.broker_name).or_default();
            *count = (*count).max(data.read_queue_nums);
        }
        counts
            .into_iter()
            .flat_map(|(broker, count)| {
                (0..count).map(move |queue_id| MessageQueue::new(topic, broker, queue_id))
            })
            .collect()
    }

    pub fn static_queue_endpoints(&self, topic: &str) -> Option<BTreeMap<MessageQueue, String>> {
        if self
            .topic_queue_mapping_by_broker
            .as_ref()
            .is_none_or(BTreeMap::is_empty)
        {
            return None;
        }
        let mut owners = BTreeMap::<(String, i32), &TopicQueueMappingInfo>::new();
        for mapping in self.topic_queue_mapping_by_broker.as_ref()?.values() {
            if let (Some(scope), Some(ids)) = (&mapping.scope, &mapping.curr_id_map) {
                for id in ids.keys() {
                    let key = (logical_broker(scope), *id);
                    if owners
                        .get(&key)
                        .is_none_or(|old| old.epoch <= mapping.epoch)
                    {
                        owners.insert(key, mapping);
                    }
                }
            }
        }
        let mut endpoints = BTreeMap::new();
        for queue in self.static_topic_queues(topic)? {
            let broker = owners
                .get(&(queue.broker_name().to_owned(), queue.queue_id()))
                .map(|mapping| mapping.bname.clone())
                .unwrap_or_else(|| "__syslo__none__".to_owned());
            endpoints.insert(queue, broker);
        }
        Some(endpoints)
    }

    fn static_topic_queues(&self, topic: &str) -> Option<Vec<MessageQueue>> {
        if self
            .topic_queue_mapping_by_broker
            .as_ref()
            .is_none_or(BTreeMap::is_empty)
        {
            return None;
        }
        let mut totals = BTreeMap::<String, i32>::new();
        for mapping in self.topic_queue_mapping_by_broker.as_ref()?.values() {
            if let Some(scope) = mapping.scope.as_deref().filter(|_| mapping.epoch >= -1) {
                // Java never updates its -1 threshold. Its endpoint map also
                // deduplicates logical names that different scope strings alias.
                let total = totals.entry(logical_broker(scope)).or_default();
                *total = (*total).max(mapping.total_queues);
            }
        }
        Some(
            totals
                .into_iter()
                .flat_map(|(broker, count)| {
                    (0..count).map(move |queue_id| MessageQueue::new(topic, &broker, queue_id))
                })
                .collect(),
        )
    }
}

fn logical_broker(scope: &str) -> String {
    // TopicQueueMappingUtils only strips the prefix for the exact global scope.
    format!(
        "__syslo__{}",
        if scope == "__global__" {
            &scope[2..]
        } else {
            scope
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_conversion_matches_pinned_java_epoch_scope_and_endpoint_oracle() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        let mut checked = 0;
        for case in fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|case| case["input"]["topicQueueMappingByBroker"].is_object())
        {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone()).unwrap();
            assert_eq!(
                serde_json::to_value(route.publish_queues("topic")).unwrap(),
                case["publish"]
            );
            assert_eq!(
                serde_json::to_value(route.subscribe_queues("topic")).unwrap(),
                case["subscribe"]
            );
            let endpoints = route.static_queue_endpoints("topic").unwrap();
            assert_eq!(
                serde_json::to_value(
                    endpoints
                        .iter()
                        .map(|(queue, broker)| serde_json::json!({"queue":queue,"broker":broker}))
                        .collect::<Vec<_>>()
                )
                .unwrap(),
                case["endpoints"]
            );
            checked += 1;
        }
        assert_eq!(checked, 4);
    }

    #[test]
    fn ordinary_publish_uses_first_matching_broker_and_static_publish_orders_ids_across_scopes() {
        let mut route = route();
        let mut duplicate = route.broker_datas[0].clone();
        duplicate.broker_addrs = Some(BTreeMap::from([(0, "other-master".into())]));
        route.broker_datas[0].broker_addrs = Some(BTreeMap::from([(1, "slave".into())]));
        route.broker_datas.push(duplicate);
        assert!(route.publish_queues("topic").is_empty());
        route.topic_queue_mapping_by_broker = Some(BTreeMap::from([
            (
                "a".into(),
                TopicQueueMappingInfo {
                    total_queues: 2,
                    epoch: 0,
                    scope: Some("a".into()),
                    curr_id_map: Some(BTreeMap::new()),
                    ..Default::default()
                },
            ),
            (
                "b".into(),
                TopicQueueMappingInfo {
                    total_queues: 2,
                    epoch: 0,
                    scope: Some("b".into()),
                    curr_id_map: Some(BTreeMap::new()),
                    ..Default::default()
                },
            ),
        ]));
        assert_eq!(
            route
                .publish_queues("topic")
                .iter()
                .map(MessageQueue::queue_id)
                .collect::<Vec<_>>(),
            [0, 0, 1, 1]
        );
    }

    #[test]
    fn duplicate_queue_data_retains_java_stable_publish_order_and_set_subscription() {
        let mut route = route();
        route.broker_datas[0].broker_addrs = Some(BTreeMap::from([(0, "master".into())]));
        route.queue_datas.retain(|queue| queue.broker_name == "b1");
        let mut second = route.queue_datas[0].clone();
        second.read_queue_nums = 1;
        second.write_queue_nums = 1;
        route.queue_datas.push(second);
        assert_eq!(
            route
                .publish_queues("topic")
                .iter()
                .map(MessageQueue::queue_id)
                .collect::<Vec<_>>(),
            [0, 1, 2, 0]
        );
        assert_eq!(
            route
                .subscribe_queues("topic")
                .iter()
                .map(MessageQueue::queue_id)
                .collect::<Vec<_>>(),
            [0, 1]
        );
    }

    fn route() -> TopicRouteData {
        TopicRouteData {
            queue_datas: vec![
                QueueData {
                    broker_name: "b1".into(),
                    read_queue_nums: 2,
                    write_queue_nums: 3,
                    perm: 6,
                    topic_sys_flag: 0,
                },
                QueueData {
                    broker_name: "b2".into(),
                    read_queue_nums: 4,
                    write_queue_nums: 4,
                    perm: 4,
                    topic_sys_flag: 0,
                },
                QueueData {
                    broker_name: "missing".into(),
                    read_queue_nums: 9,
                    write_queue_nums: 9,
                    perm: 6,
                    topic_sys_flag: 0,
                },
            ],
            broker_datas: vec![
                BrokerData {
                    broker_name: "b1".into(),
                    ..BrokerData::default()
                },
                BrokerData {
                    broker_name: "b2".into(),
                    ..BrokerData::default()
                },
            ],
            ..TopicRouteData::default()
        }
    }

    #[test]
    fn converts_writable_and_readable_queues_without_absent_masters() {
        let mut route = route();
        route.broker_datas[0].broker_addrs = Some(BTreeMap::from([(0, "b1-master".into())]));
        assert_eq!(route.publish_queues("t").len(), 3);
        assert_eq!(route.subscribe_queues("t").len(), 15);
    }

    #[test]
    fn order_topic_configuration_takes_precedence_over_route_data() {
        let mut route = route();
        route.order_topic_conf = Some("ordered:2;other:1".into());
        let queues = route.publish_queues("t");
        assert_eq!(queues.len(), 3);
        assert_eq!(queues[0].broker_name(), "ordered");
        assert_eq!(queues[2].queue_id(), 0);
    }

    #[test]
    fn static_routes_allocate_logical_queue_space_per_scope() {
        let mut route = route();
        route.topic_queue_mapping_by_broker = Some(BTreeMap::from([(
            "b1".into(),
            TopicQueueMappingInfo {
                topic: "t".into(),
                total_queues: 8,
                bname: "b1".into(),
                epoch: 0,
                dirty: false,
                scope: Some("__global__".into()),
                curr_id_map: Some(BTreeMap::from([(0, 0)])),
            },
        )]));
        let queues = route.publish_queues("t");
        assert_eq!(queues.len(), 8);
        assert_eq!(queues[0].broker_name(), "__syslo__global__");
        assert_eq!(queues[1].broker_name(), "__syslo__global__");
        let endpoints = route.static_queue_endpoints("t").unwrap();
        assert_eq!(endpoints.get(&queues[0]).unwrap(), "b1");
        assert_eq!(endpoints.get(&queues[1]).unwrap(), "__syslo__none__");
    }

    #[test]
    fn preserves_static_topic_mapping_and_json_field_names() {
        let mut route = route();
        route.topic_queue_mapping_by_broker = Some(BTreeMap::from([(
            "b1".into(),
            TopicQueueMappingInfo {
                topic: "t".into(),
                total_queues: 8,
                bname: "b1".into(),
                epoch: i64::MAX,
                dirty: true,
                scope: Some("global".into()),
                curr_id_map: Some(BTreeMap::from([(0, 2)])),
            },
        )]));
        let json = serde_json::to_value(&route).unwrap();
        assert_eq!(json["topicQueueMappingByBroker"]["b1"]["totalQueues"], 8);
        assert_eq!(json["topicQueueMappingByBroker"]["b1"]["epoch"], i64::MAX);
    }
}
