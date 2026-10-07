use std::collections::BTreeMap;

use rocketmq_classic_common::message::MessageQueue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueueData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_name: Option<String>,
    #[serde(default)]
    pub read_queue_nums: i32,
    #[serde(default)]
    pub write_queue_nums: i32,
    #[serde(default)]
    pub perm: i32,
    #[serde(default)]
    pub topic_sys_flag: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BrokerData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_addrs: Option<BTreeMap<i64, Option<String>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone_name: Option<String>,
    #[serde(default)]
    pub enable_acting_master: bool,
}

#[cfg(test)]
mod nullable_broker_address_tests {
    use super::BrokerData;

    #[test]
    fn null_map_value_is_distinct_from_missing_key_and_null_map() {
        let value = serde_json::json!({"cluster":"c","brokerName":"b","brokerAddrs":{"0":null,"1":"slave:10911"}});
        let broker: BrokerData = serde_json::from_value(value.clone()).unwrap();
        let addresses = broker.broker_addrs.as_ref().unwrap();
        assert_eq!(addresses.get(&0), Some(&None));
        assert!(!addresses.contains_key(&2));
        assert_eq!(
            addresses.get(&1).and_then(Option::as_deref),
            Some("slave:10911")
        );
        assert_eq!(
            serde_json::to_value(&broker).unwrap()["brokerAddrs"],
            value["brokerAddrs"]
        );
        let null_map: BrokerData = serde_json::from_value(
            serde_json::json!({"cluster":"c","brokerName":"b","brokerAddrs":null}),
        )
        .unwrap();
        assert!(null_map.broker_addrs.is_none());
    }
}

#[cfg(test)]
mod nullable_nested_route_tests {
    use super::TopicRouteData;

    #[test]
    fn nested_broker_and_queue_names_follow_invoked_java_defaults_and_json() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableNestedRouteFields"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone())
                .expect("pinned Java accepts this nested route input");
            let queue = &route.queue_datas.as_ref().unwrap()[0];
            let encoded = serde_json::to_value(&route).unwrap();
            assert_eq!(
                encoded["queueDatas"][0].get("brokerName").is_none(),
                case["queueNameNull"]
            );
            assert_eq!(
                encoded["brokerDatas"][0].get("brokerName").is_none(),
                case["brokerNameNull"]
            );
            assert_eq!(
                encoded["brokerDatas"][0].get("cluster").is_none(),
                case["clusterNull"]
            );
            assert_eq!(
                encoded["queueDatas"][0].get("brokerName").is_some(),
                case["encodedHasQueueName"]
            );
            assert_eq!(
                encoded["brokerDatas"][0].get("brokerName").is_some(),
                case["encodedHasBrokerName"]
            );
            assert_eq!(
                encoded["brokerDatas"][0].get("cluster").is_some(),
                case["encodedHasCluster"]
            );
            assert_eq!(queue.read_queue_nums, case["queueRead"]);
            assert_eq!(queue.write_queue_nums, case["queueWrite"]);
            assert_eq!(queue.perm, case["queuePerm"]);
            assert_eq!(queue.topic_sys_flag, case["queueFlag"]);
            let subscriptions = route.subscribe_queues("topic");
            assert_eq!(
                subscriptions.len(),
                case["subscribeQueueCount"].as_u64().unwrap() as usize
            );
            assert_eq!(
                subscriptions
                    .iter()
                    .any(|queue| queue.broker_name().is_none()),
                case["subscribeQueueBrokerNull"]
            );
            assert_eq!(
                subscriptions.iter().any(|queue| serde_json::to_value(queue)
                    .unwrap()
                    .get("brokerName")
                    .is_none()),
                case["subscribeJsonBrokerOmitted"]
            );
        }
    }
}

#[cfg(test)]
mod nullable_message_queue_tests {
    use rocketmq_classic_common::message::MessageQueue;

    #[test]
    fn nullable_message_queue_fields_match_invoked_java_defaults_and_json() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableMessageQueueFields"].as_array().unwrap() {
            let queue: MessageQueue = serde_json::from_value(case["input"].clone())
                .expect("pinned Java decodes this MessageQueue");
            let encoded = serde_json::to_value(&queue).unwrap();
            assert_eq!(encoded.get("topic").is_none(), case["topicNull"]);
            assert_eq!(encoded.get("brokerName").is_none(), case["brokerNull"]);
            assert_eq!(encoded.get("topic").is_some(), case["encodedHasTopic"]);
            assert_eq!(
                encoded.get("brokerName").is_some(),
                case["encodedHasBroker"]
            );
            assert_eq!(queue.queue_id(), case["queueId"].as_i64().unwrap() as i32);
        }
    }
}

#[cfg(test)]
mod nullable_route_collection_tests {
    use super::TopicRouteData;

    #[test]
    fn source_explicit_null_route_collections_decode_without_becoming_empty() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableRouteCollections"].as_array().unwrap() {
            let route: TopicRouteData = serde_json::from_value(case["input"].clone())
                .expect("Java decodes explicit null route collections");
            let encoded = serde_json::to_value(route).unwrap();
            for (field, decoded_key, encoded_key) in [
                ("queueDatas", "decodedQueueNull", "encodedHasQueue"),
                ("brokerDatas", "decodedBrokerNull", "encodedHasBroker"),
                ("filterServerTable", "decodedFilterNull", "encodedHasFilter"),
            ] {
                assert_eq!(encoded.get(field).is_none(), case[decoded_key]);
                assert_eq!(encoded.get(field).is_some(), case[encoded_key]);
            }
            assert_eq!(
                encoded.get("topicQueueMappingByBroker").is_some(),
                case["encodedHasMapping"]
            );
        }
    }
}

#[cfg(test)]
mod nullable_static_mapping_tests {
    use super::TopicQueueMappingInfo;

    #[test]
    fn static_mapping_optional_fields_match_invoked_java_decode_and_json() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/route.json")).unwrap();
        for case in fixture["nullableStaticMappingFields"].as_array().unwrap() {
            let mapping: TopicQueueMappingInfo = serde_json::from_value(case["input"].clone())
                .expect("pinned Java accepts this mapping input");
            let encoded = serde_json::to_value(&mapping).unwrap();
            for (field, null_key, encoded_key) in [
                ("topic", "topicNull", "encodedHasTopic"),
                ("bname", "bnameNull", "encodedHasBname"),
                ("scope", "scopeNull", "encodedHasScope"),
                ("currIdMap", "currIdMapNull", "encodedHasCurrIdMap"),
            ] {
                assert_eq!(encoded.get(field).is_none(), case[null_key]);
                assert_eq!(encoded.get(field).is_some(), case[encoded_key]);
            }
            assert_eq!(
                mapping
                    .curr_id_map
                    .as_ref()
                    .is_some_and(|map| map.is_empty()),
                case["currIdMapEmpty"]
            );
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicQueueMappingInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default)]
    pub total_queues: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bname: Option<String>,
    #[serde(default)]
    pub epoch: i64,
    #[serde(default)]
    pub dirty: bool,
    #[serde(
        default = "default_mapping_scope",
        skip_serializing_if = "Option::is_none"
    )]
    pub scope: Option<String>,
    #[serde(
        default = "default_mapping_ids",
        skip_serializing_if = "Option::is_none"
    )]
    pub curr_id_map: Option<BTreeMap<i32, i32>>,
}

fn default_mapping_ids() -> Option<BTreeMap<i32, i32>> {
    Some(BTreeMap::new())
}

impl Default for TopicQueueMappingInfo {
    fn default() -> Self {
        Self {
            topic: None,
            total_queues: 0,
            bname: None,
            epoch: 0,
            dirty: false,
            scope: default_mapping_scope(),
            curr_id_map: default_mapping_ids(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicRouteData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_topic_conf: Option<String>,
    #[serde(default = "default_route_vec", skip_serializing_if = "Option::is_none")]
    pub queue_datas: Option<Vec<QueueData>>,
    #[serde(default = "default_route_vec", skip_serializing_if = "Option::is_none")]
    pub broker_datas: Option<Vec<BrokerData>>,
    #[serde(
        default = "default_filter_table",
        skip_serializing_if = "Option::is_none"
    )]
    pub filter_server_table: Option<BTreeMap<String, Vec<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_queue_mapping_by_broker: Option<BTreeMap<String, TopicQueueMappingInfo>>,
}

fn default_route_vec<T>() -> Option<Vec<T>> {
    Some(Vec::new())
}

fn default_filter_table() -> Option<BTreeMap<String, Vec<String>>> {
    Some(BTreeMap::new())
}

impl Default for TopicRouteData {
    fn default() -> Self {
        Self {
            order_topic_conf: None,
            queue_datas: default_route_vec(),
            broker_datas: default_route_vec(),
            filter_server_table: default_filter_table(),
            topic_queue_mapping_by_broker: None,
        }
    }
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
        for broker in self.broker_datas.as_deref().unwrap_or(&[]) {
            masters
                .entry(broker.broker_name.as_deref())
                .or_insert_with(|| {
                    broker
                        .broker_addrs
                        .as_ref()
                        .is_some_and(|addresses| addresses.contains_key(&0))
                });
        }
        let mut queue_data = self
            .queue_datas
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .collect::<Vec<_>>();
        queue_data.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
        queue_data
            .into_iter()
            .filter(|data| {
                data.write_queue_nums > 0
                    && data.perm & 0x2 != 0
                    && masters
                        .get(&data.broker_name.as_deref())
                        .copied()
                        .unwrap_or(false)
            })
            .flat_map(|data| {
                (0..data.write_queue_nums).map(|queue_id| {
                    MessageQueue::new_nullable_broker(topic, data.broker_name.clone(), queue_id)
                })
            })
            .collect()
    }

    pub fn subscribe_queues(&self, topic: &str) -> Vec<MessageQueue> {
        if let Some(endpoints) = self.static_queue_endpoints(topic) {
            return endpoints.into_keys().collect();
        }
        let mut counts = BTreeMap::<Option<&str>, i32>::new();
        for data in self
            .queue_datas
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter(|data| data.read_queue_nums > 0 && data.perm & 0x4 != 0)
        {
            let count = counts.entry(data.broker_name.as_deref()).or_default();
            *count = (*count).max(data.read_queue_nums);
        }
        counts
            .into_iter()
            .flat_map(|(broker, count)| {
                (0..count).map(move |queue_id| {
                    MessageQueue::new_nullable_broker(topic, broker.map(str::to_owned), queue_id)
                })
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
                .get(&(
                    queue.broker_name().unwrap_or_default().to_owned(),
                    queue.queue_id(),
                ))
                .and_then(|mapping| mapping.bname.clone())
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
        let mut duplicate = route.broker_datas.as_ref().unwrap()[0].clone();
        duplicate.broker_addrs = Some(BTreeMap::from([(0, Some("other-master".into()))]));
        route.broker_datas.as_mut().unwrap()[0].broker_addrs =
            Some(BTreeMap::from([(1, Some("slave".into()))]));
        route.broker_datas.as_mut().unwrap().push(duplicate);
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
        route.broker_datas.as_mut().unwrap()[0].broker_addrs =
            Some(BTreeMap::from([(0, Some("master".into()))]));
        route
            .queue_datas
            .as_mut()
            .unwrap()
            .retain(|queue| queue.broker_name.as_deref() == Some("b1"));
        let mut second = route.queue_datas.as_ref().unwrap()[0].clone();
        second.read_queue_nums = 1;
        second.write_queue_nums = 1;
        route.queue_datas.as_mut().unwrap().push(second);
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
            queue_datas: Some(vec![
                QueueData {
                    broker_name: Some("b1".into()),
                    read_queue_nums: 2,
                    write_queue_nums: 3,
                    perm: 6,
                    topic_sys_flag: 0,
                },
                QueueData {
                    broker_name: Some("b2".into()),
                    read_queue_nums: 4,
                    write_queue_nums: 4,
                    perm: 4,
                    topic_sys_flag: 0,
                },
                QueueData {
                    broker_name: Some("missing".into()),
                    read_queue_nums: 9,
                    write_queue_nums: 9,
                    perm: 6,
                    topic_sys_flag: 0,
                },
            ]),
            broker_datas: Some(vec![
                BrokerData {
                    broker_name: Some("b1".into()),
                    ..BrokerData::default()
                },
                BrokerData {
                    broker_name: Some("b2".into()),
                    ..BrokerData::default()
                },
            ]),
            ..TopicRouteData::default()
        }
    }

    #[test]
    fn converts_writable_and_readable_queues_without_absent_masters() {
        let mut route = route();
        route.broker_datas.as_mut().unwrap()[0].broker_addrs =
            Some(BTreeMap::from([(0, Some("b1-master".into()))]));
        assert_eq!(route.publish_queues("t").len(), 3);
        assert_eq!(route.subscribe_queues("t").len(), 15);
    }

    #[test]
    fn order_topic_configuration_takes_precedence_over_route_data() {
        let mut route = route();
        route.order_topic_conf = Some("ordered:2;other:1".into());
        let queues = route.publish_queues("t");
        assert_eq!(queues.len(), 3);
        assert_eq!(queues[0].broker_name(), Some("ordered"));
        assert_eq!(queues[2].queue_id(), 0);
    }

    #[test]
    fn static_routes_allocate_logical_queue_space_per_scope() {
        let mut route = route();
        route.topic_queue_mapping_by_broker = Some(BTreeMap::from([(
            "b1".into(),
            TopicQueueMappingInfo {
                topic: Some("t".into()),
                total_queues: 8,
                bname: Some("b1".into()),
                epoch: 0,
                dirty: false,
                scope: Some("__global__".into()),
                curr_id_map: Some(BTreeMap::from([(0, 0)])),
            },
        )]));
        let queues = route.publish_queues("t");
        assert_eq!(queues.len(), 8);
        assert_eq!(queues[0].broker_name(), Some("__syslo__global__"));
        assert_eq!(queues[1].broker_name(), Some("__syslo__global__"));
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
                topic: Some("t".into()),
                total_queues: 8,
                bname: Some("b1".into()),
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
