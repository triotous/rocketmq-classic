use rocketmq_classic_protocol::route::{
    BrokerData, QueueData, SharedBrokerAddressMap, SharedBrokerDataHandle, SharedFilterServerList,
    SharedFilterServerTable, SharedQueueDataHandle, SharedRouteBrokerDataList,
    SharedRouteQueueDataList, SharedTopicQueueMappingIds, SharedTopicQueueMappingInfoHandle,
    SharedTopicQueueMappingTable, TopicQueueMappingInfo, TopicRouteData,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn route_fixture() -> Value {
    serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap()
}

#[test]
fn route_dto_public_fields_match_invoked_java_getter_setter_values() {
    let fixture = route_fixture();
    let java = &fixture["routeDtoAccessors"];

    let broker: BrokerData = serde_json::from_value(java["brokerData"].clone()).unwrap();
    let broker_addrs = broker.broker_addrs.as_ref().map(|values| {
        values.with(|values| {
            values
                .iter()
                .map(|(key, value)| {
                    (
                        key.map_or_else(|| "null".to_owned(), |id| id.to_string()),
                        value.clone(),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        })
    });
    assert_eq!(
        json!({
            "cluster": broker.cluster.as_deref(),
            "brokerName": broker.broker_name.as_deref(),
            "brokerAddrs": broker_addrs,
            "zoneName": broker.zone_name.as_deref(),
            "enableActingMaster": broker.enable_acting_master,
        }),
        java["brokerData"]
    );

    let broker_nulls: BrokerData = serde_json::from_value(java["brokerDataNulls"].clone()).unwrap();
    assert_eq!(
        json!({
            "cluster": broker_nulls.cluster.as_deref(),
            "brokerName": broker_nulls.broker_name.as_deref(),
            "brokerAddrs": broker_nulls.broker_addrs,
            "zoneName": broker_nulls.zone_name.as_deref(),
            "enableActingMaster": broker_nulls.enable_acting_master,
        }),
        java["brokerDataNulls"]
    );

    let queue: QueueData = serde_json::from_value(java["queueData"].clone()).unwrap();
    assert_eq!(
        json!({
            "brokerName": queue.broker_name.as_deref(),
            "readQueueNums": queue.read_queue_nums,
            "writeQueueNums": queue.write_queue_nums,
            "perm": queue.perm,
            "topicSysFlag": queue.topic_sys_flag,
        }),
        java["queueData"]
    );
    let queue_nulls: QueueData = serde_json::from_value(java["queueDataNulls"].clone()).unwrap();
    assert_eq!(
        json!({
            "brokerName": queue_nulls.broker_name.as_deref(),
            "readQueueNums": queue_nulls.read_queue_nums,
            "writeQueueNums": queue_nulls.write_queue_nums,
            "perm": queue_nulls.perm,
            "topicSysFlag": queue_nulls.topic_sys_flag,
        }),
        java["queueDataNulls"]
    );

    let mapping: TopicQueueMappingInfo =
        serde_json::from_value(java["topicQueueMappingInfo"].clone()).unwrap();
    let mapping_ids = mapping.curr_id_map.as_ref().map(|values| {
        values.with(|values| {
            values
                .iter()
                .map(|(key, value)| (key.to_string(), *value))
                .collect::<BTreeMap<_, _>>()
        })
    });
    assert_eq!(
        json!({
            "topic": mapping.topic.as_deref(),
            "scope": mapping.scope.as_deref(),
            "totalQueues": mapping.total_queues,
            "bname": mapping.bname.as_deref(),
            "epoch": mapping.epoch,
            "dirty": mapping.dirty,
            "currIdMap": mapping_ids,
        }),
        java["topicQueueMappingInfo"]
    );
    let mapping_nulls: TopicQueueMappingInfo =
        serde_json::from_value(java["topicQueueMappingInfoNulls"].clone()).unwrap();
    assert_eq!(
        json!({
            "topic": mapping_nulls.topic.as_deref(),
            "scope": mapping_nulls.scope.as_deref(),
            "totalQueues": mapping_nulls.total_queues,
            "bname": mapping_nulls.bname.as_deref(),
            "epoch": mapping_nulls.epoch,
            "dirty": mapping_nulls.dirty,
            "currIdMap": mapping_nulls.curr_id_map,
        }),
        java["topicQueueMappingInfoNulls"]
    );

    let route: TopicRouteData = TopicRouteData {
        order_topic_conf: Some(
            java["topicRouteData"]["orderTopicConf"]
                .as_str()
                .unwrap()
                .to_owned(),
        ),
        queue_datas: Some(
            vec![Some(
                serde_json::from_value::<QueueData>(java["queueData"].clone()).unwrap(),
            )]
            .into(),
        ),
        broker_datas: Some(
            vec![Some(BrokerData {
                broker_name: Some(
                    java["topicRouteData"]["brokerDatas"][0]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                ),
                ..Default::default()
            })]
            .into(),
        ),
        filter_server_table: Some(
            serde_json::from_value(java["topicRouteData"]["filterServerTable"].clone()).unwrap(),
        ),
        topic_queue_mapping_by_broker: Some(
            BTreeMap::<String, Option<TopicQueueMappingInfo>>::from([(
                java["topicRouteData"]["topicQueueMappingByBroker"][0]
                    .as_str()
                    .unwrap()
                    .to_owned(),
                None,
            )])
            .into(),
        ),
    };
    let queue_names = route.queue_datas_snapshot().map(|values| {
        values
            .iter()
            .map(|value| value.as_ref().and_then(|queue| queue.broker_name.clone()))
            .collect::<Vec<_>>()
    });
    let broker_names = route.broker_datas_snapshot().map(|values| {
        values
            .iter()
            .map(|value| value.as_ref().and_then(|broker| broker.broker_name.clone()))
            .collect::<Vec<_>>()
    });
    let mapping_brokers = route
        .topic_queue_mapping_by_broker_snapshot()
        .as_ref()
        .map(|values| values.keys().cloned().collect::<Vec<_>>());
    assert_eq!(
        json!({
            "orderTopicConf": route.order_topic_conf.as_deref(),
            "queueDatas": queue_names,
            "brokerDatas": broker_names,
            "filterServerTable": route.filter_server_table_snapshot(),
            "topicQueueMappingByBroker": mapping_brokers,
        }),
        java["topicRouteData"]
    );
    let route_nulls = TopicRouteData {
        order_topic_conf: None,
        queue_datas: None,
        broker_datas: None,
        filter_server_table: None,
        topic_queue_mapping_by_broker: None,
    };
    assert_eq!(
        json!({
            "orderTopicConf": route_nulls.order_topic_conf.as_deref(),
            "queueDatas": route_nulls.queue_datas_snapshot(),
            "brokerDatas": route_nulls.broker_datas_snapshot(),
            "filterServerTable": route_nulls.filter_server_table_snapshot(),
            "topicQueueMappingByBroker": route_nulls.topic_queue_mapping_by_broker_snapshot(),
        }),
        java["topicRouteDataNulls"]
    );
}

#[test]
fn route_dto_mutable_map_accessors_preserve_java_aliases() {
    let fixture = route_fixture();
    let java = &fixture["routeDtoReferenceAliases"];

    let mut broker = BrokerData::default();
    let caller_addresses: SharedBrokerAddressMap =
        BTreeMap::from([(Some(0), Some("master:10911".to_owned()))]).into();
    broker.set_broker_addrs(Some(caller_addresses.clone()));
    let getter_addresses = broker.get_broker_addrs().unwrap();
    assert_eq!(
        caller_addresses.ptr_eq(&getter_addresses),
        java["brokerSetterRetainsMap"]
    );
    assert_eq!(
        getter_addresses.ptr_eq(&broker.get_broker_addrs().unwrap()),
        java["brokerGetterIsStable"]
    );
    caller_addresses.with_mut(|addresses| {
        addresses.insert(Some(2), Some("caller-slave:10911".to_owned()));
    });
    assert_eq!(
        getter_addresses.with(|addresses| addresses.get(&Some(2)).cloned().flatten()),
        Some("caller-slave:10911".to_owned())
    );
    assert_eq!(java["brokerCallerMutationVisible"], true);
    getter_addresses.with_mut(|addresses| {
        addresses.insert(Some(3), Some("getter-slave:10911".to_owned()));
    });
    assert_eq!(
        caller_addresses.with(|addresses| addresses.get(&Some(3)).cloned().flatten()),
        Some("getter-slave:10911".to_owned())
    );
    assert_eq!(java["brokerGetterMutationVisible"], true);

    let mut mapping = TopicQueueMappingInfo::default();
    let caller_ids: SharedTopicQueueMappingIds = BTreeMap::from([(0, 4)]).into();
    mapping.set_curr_id_map(Some(caller_ids.clone()));
    let getter_ids = mapping.get_curr_id_map().unwrap();
    assert_eq!(
        caller_ids.ptr_eq(&getter_ids),
        java["mappingSetterRetainsMap"]
    );
    assert_eq!(
        getter_ids.ptr_eq(&mapping.get_curr_id_map().unwrap()),
        java["mappingGetterIsStable"]
    );
    caller_ids.with_mut(|ids| {
        ids.insert(1, 7);
    });
    assert_eq!(getter_ids.with(|ids| ids.get(&1).copied()), Some(7));
    assert_eq!(java["mappingCallerMutationVisible"], true);
    getter_ids.with_mut(|ids| {
        ids.insert(2, 9);
    });
    assert_eq!(caller_ids.with(|ids| ids.get(&2).copied()), Some(9));
    assert_eq!(java["mappingGetterMutationVisible"], true);
}

#[test]
fn topic_route_data_collection_accessors_preserve_java_aliases() {
    let fixture = route_fixture();
    let java = &fixture["routeDtoReferenceAliases"];
    let mut route = TopicRouteData::default();

    let caller_queue = SharedQueueDataHandle::new(QueueData {
        broker_name: Some("queue-broker".to_owned()),
        read_queue_nums: 1,
        write_queue_nums: 2,
        perm: 6,
        topic_sys_flag: 0,
    });
    let caller_queues: SharedRouteQueueDataList = vec![Some(caller_queue.clone())].into();
    route.set_queue_datas(Some(caller_queues.clone()));
    let getter_queues = route.get_queue_datas().unwrap();
    assert_eq!(
        caller_queues.ptr_eq(&getter_queues),
        java["routeQueueSetterRetainsList"]
    );
    assert_eq!(
        getter_queues.ptr_eq(&route.get_queue_datas().unwrap()),
        java["routeQueueGetterIsStable"]
    );
    assert_eq!(
        caller_queue.ptr_eq(&getter_queues.with(|queues| queues[0].as_ref().unwrap().clone())),
        java["routeQueueElementIdentityRetained"]
    );
    caller_queues.with_mut(|queues| {
        queues.push(Some(SharedQueueDataHandle::new(QueueData::default())));
    });
    assert_eq!(getter_queues.with(|queues| queues.len()), 2);
    assert_eq!(java["routeQueueCallerMutationVisible"], true);
    getter_queues.with(|queues| {
        queues[0].as_ref().unwrap().with_mut(|queue| queue.perm = 5);
    });
    assert_eq!(caller_queue.with(|queue| queue.perm), 5);
    assert_eq!(java["routeQueueGetterElementMutationVisible"], true);
    getter_queues.with_mut(|queues| {
        queues.push(Some(SharedQueueDataHandle::new(QueueData::default())));
    });
    assert_eq!(caller_queues.with(|queues| queues.len()), 3);
    assert_eq!(java["routeQueueGetterMutationVisible"], true);

    let caller_broker = SharedBrokerDataHandle::new(BrokerData {
        broker_name: Some("route-broker".to_owned()),
        ..Default::default()
    });
    let caller_brokers: SharedRouteBrokerDataList = vec![Some(caller_broker.clone())].into();
    route.set_broker_datas(Some(caller_brokers.clone()));
    let getter_brokers = route.get_broker_datas().unwrap();
    assert_eq!(
        caller_brokers.ptr_eq(&getter_brokers),
        java["routeBrokerSetterRetainsList"]
    );
    assert_eq!(
        getter_brokers.ptr_eq(&route.get_broker_datas().unwrap()),
        java["routeBrokerGetterIsStable"]
    );
    assert_eq!(
        caller_broker.ptr_eq(&getter_brokers.with(|brokers| brokers[0].as_ref().unwrap().clone())),
        java["routeBrokerElementIdentityRetained"]
    );
    caller_brokers.with_mut(|brokers| {
        brokers.push(Some(SharedBrokerDataHandle::new(BrokerData::default())));
    });
    assert_eq!(getter_brokers.with(|brokers| brokers.len()), 2);
    assert_eq!(java["routeBrokerCallerMutationVisible"], true);
    getter_brokers.with(|brokers| {
        brokers[0]
            .as_ref()
            .unwrap()
            .with_mut(|broker| broker.broker_name = Some("getter-broker".to_owned()));
    });
    assert_eq!(
        caller_broker.with(|broker| broker.broker_name.clone()),
        Some("getter-broker".to_owned())
    );
    assert_eq!(java["routeBrokerGetterElementMutationVisible"], true);
    getter_brokers.with_mut(|brokers| {
        brokers.push(Some(SharedBrokerDataHandle::new(BrokerData::default())));
    });
    assert_eq!(caller_brokers.with(|brokers| brokers.len()), 3);
    assert_eq!(java["routeBrokerGetterMutationVisible"], true);

    let caller_filter_list: SharedFilterServerList = vec![Some("filter-a".to_owned())].into();
    let caller_filters: SharedFilterServerTable =
        BTreeMap::from([("filter-broker".to_owned(), Some(caller_filter_list.clone()))]).into();
    route.set_filter_server_table(Some(caller_filters.clone()));
    let getter_filters = route.get_filter_server_table().unwrap();
    assert_eq!(
        caller_filters.ptr_eq(&getter_filters),
        java["routeFilterSetterRetainsMap"]
    );
    assert_eq!(
        getter_filters.ptr_eq(&route.get_filter_server_table().unwrap()),
        java["routeFilterGetterIsStable"]
    );
    assert_eq!(
        caller_filter_list.ptr_eq(
            &getter_filters.with(|filters| filters["filter-broker"].as_ref().unwrap().clone())
        ),
        java["routeFilterNestedListIdentityRetained"]
    );
    caller_filter_list.with_mut(|filters| filters.push(Some("caller-filter".to_owned())));
    assert_eq!(
        getter_filters.with(|filters| filters["filter-broker"]
            .as_ref()
            .unwrap()
            .with(|filters| filters.len())),
        2
    );
    assert_eq!(java["routeFilterCallerMutationVisible"], true);
    getter_filters.with_mut(|filters| {
        filters.insert(
            "getter-broker".to_owned(),
            Some(vec![Some("getter-filter".to_owned())].into()),
        );
    });
    assert!(caller_filters.with(|filters| filters.contains_key("getter-broker")));
    assert_eq!(java["routeFilterGetterMutationVisible"], true);
    getter_filters.with(|filters| {
        filters["filter-broker"]
            .as_ref()
            .unwrap()
            .with_mut(|filters| filters.push(Some("getter-filter".to_owned())));
    });
    assert_eq!(caller_filter_list.with(|filters| filters.len()), 3);
    assert_eq!(java["routeFilterGetterNestedMutationVisible"], true);

    let caller_mapping = SharedTopicQueueMappingInfoHandle::new(TopicQueueMappingInfo {
        topic: Some("route-topic".to_owned()),
        total_queues: 2,
        bname: Some("mapping-broker".to_owned()),
        epoch: 4,
        ..Default::default()
    });
    let caller_mappings: SharedTopicQueueMappingTable =
        BTreeMap::from([("mapping-broker".to_owned(), Some(caller_mapping.clone()))]).into();
    route.set_topic_queue_mapping_by_broker(Some(caller_mappings.clone()));
    let getter_mappings = route.get_topic_queue_mapping_by_broker().unwrap();
    assert_eq!(
        caller_mappings.ptr_eq(&getter_mappings),
        java["routeMappingSetterRetainsMap"]
    );
    assert_eq!(
        getter_mappings.ptr_eq(&route.get_topic_queue_mapping_by_broker().unwrap()),
        java["routeMappingGetterIsStable"]
    );
    assert_eq!(
        caller_mapping.ptr_eq(
            &getter_mappings.with(|mappings| mappings["mapping-broker"].as_ref().unwrap().clone())
        ),
        java["routeMappingElementIdentityRetained"]
    );
    caller_mappings.with_mut(|mappings| {
        mappings.insert("caller-broker".to_owned(), None);
    });
    assert_eq!(getter_mappings.with(|mappings| mappings.len()), 2);
    assert_eq!(java["routeMappingCallerMutationVisible"], true);
    getter_mappings.with(|mappings| {
        mappings["mapping-broker"]
            .as_ref()
            .unwrap()
            .with_mut(|mapping| mapping.dirty = true);
    });
    assert!(caller_mapping.with(|mapping| mapping.dirty));
    assert_eq!(java["routeMappingGetterElementMutationVisible"], true);
    getter_mappings.with_mut(|mappings| {
        mappings.insert(
            "getter-broker".to_owned(),
            Some(SharedTopicQueueMappingInfoHandle::new(
                TopicQueueMappingInfo::default(),
            )),
        );
    });
    assert_eq!(caller_mappings.with(|mappings| mappings.len()), 3);
    assert_eq!(java["routeMappingGetterMutationVisible"], true);
}
