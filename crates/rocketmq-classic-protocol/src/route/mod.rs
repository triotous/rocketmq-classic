use std::collections::{BTreeMap, HashMap};

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
            if let Some(queues) = self.static_topic_queues(topic) {
                return queues;
            }
        }

        let masters: BTreeMap<&str, bool> = self
            .broker_datas
            .iter()
            .map(|broker| {
                (
                    broker.broker_name.as_str(),
                    broker
                        .broker_addrs
                        .as_ref()
                        .is_some_and(|addresses| addresses.contains_key(&0)),
                )
            })
            .collect();
        let mut queue_data = self.queue_datas.iter().collect::<Vec<_>>();
        queue_data.sort_by(|left, right| {
            left.broker_name
                .cmp(&right.broker_name)
                .then_with(|| left.read_queue_nums.cmp(&right.read_queue_nums))
                .then_with(|| left.write_queue_nums.cmp(&right.write_queue_nums))
                .then_with(|| left.perm.cmp(&right.perm))
                .then_with(|| left.topic_sys_flag.cmp(&right.topic_sys_flag))
        });
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
        if let Some(queues) = self.static_topic_queues(topic) {
            return queues;
        }
        self.queue_datas
            .iter()
            .filter(|data| data.read_queue_nums > 0 && data.perm & 0x4 != 0)
            .flat_map(|data| {
                (0..data.read_queue_nums)
                    .map(|queue_id| MessageQueue::new(topic, &data.broker_name, queue_id))
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
        let mut endpoints = BTreeMap::new();
        for queue in self.static_topic_queues(topic)? {
            let broker = self
                .topic_queue_mapping_by_broker
                .as_ref()?
                .values()
                .filter(|mapping| {
                    mapping.scope.as_ref().is_some_and(|scope| {
                        let logical_broker = scope.strip_prefix("__").map_or_else(
                            || format!("__syslo__{scope}"),
                            |suffix| format!("__syslo__{suffix}"),
                        );
                        logical_broker == queue.broker_name()
                    })
                })
                .filter(|mapping| {
                    mapping
                        .curr_id_map
                        .as_ref()
                        .is_some_and(|id_map| id_map.contains_key(&queue.queue_id()))
                })
                .max_by_key(|mapping| mapping.epoch)
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
        let mappings = self.topic_queue_mapping_by_broker.as_ref()?.values();
        let mut mappings_by_scope = HashMap::<String, Vec<&TopicQueueMappingInfo>>::new();
        for mapping in mappings {
            if let Some(scope) = &mapping.scope {
                mappings_by_scope
                    .entry(scope.clone())
                    .or_default()
                    .push(mapping);
            }
        }

        let mut scopes = mappings_by_scope.keys().cloned().collect::<Vec<_>>();
        scopes.sort();
        let mut queues = Vec::new();
        for scope in scopes {
            let mut mappings = mappings_by_scope
                .remove(&scope)
                .expect("scope was collected from the map");
            mappings.sort_by(|left, right| right.epoch.cmp(&left.epoch));
            let mut max_total_queues = 0;
            let mut max_total_epoch = -1;
            for mapping in mappings {
                if mapping.epoch >= max_total_epoch && mapping.total_queues > max_total_queues {
                    max_total_queues = mapping.total_queues;
                    max_total_epoch = mapping.epoch;
                }
            }

            let logical_broker = match scope.strip_prefix("__") {
                Some("") => return Some(Vec::new()),
                Some(suffix) => format!("__syslo__{suffix}"),
                None => format!("__syslo__{scope}"),
            };
            queues.extend(
                (0..max_total_queues)
                    .map(|queue_id| MessageQueue::new(topic, &logical_broker, queue_id)),
            );
        }
        Some(queues)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
