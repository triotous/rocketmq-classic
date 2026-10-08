use rocketmq_classic_core::{config::ClientConfig, runtime::ClientRuntimeManager};
use rocketmq_classic_protocol::route::TopicRouteData;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[tokio::test]
async fn raw_clear_query_refetches_once_and_retained_table_observes_wire_refresh() {
    use bytes::BytesMut;
    use rocketmq_classic_remoting::{
        RemotingCommand,
        codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::{Duration, Instant},
    };
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let case = &fixture["runtimeTopicRouteTable"];
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("raw-table-wire");
    config.namesrv_addr = Some(listener.local_addr().unwrap().to_string());
    config.enable_heartbeat_channel_event_listener = false;
    let body = serde_json::to_vec(&case["initialInput"]).unwrap();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        for _ in 0..2 {
            let mut length = [0; 4];
            socket.read_exact(&mut length).await.unwrap();
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            socket.read_exact(&mut bytes[4..]).await.unwrap();
            let request = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .unwrap();
            assert_eq!(request.code, 105);
            assert_eq!(request.ext_fields.as_ref().unwrap()["topic"], "topic");
            let mut response = RemotingCommand {
                opaque: request.opaque,
                body: Some(body.clone()),
                ..Default::default()
            };
            response.mark_response();
            socket
                .write_all(&frame::encode(&response, DEFAULT_MAX_FRAME_SIZE).unwrap())
                .await
                .unwrap();
        }
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0); // No third cached-query RPC.
    });
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    // Start only lower transport: no runtime periodic tasks/Broker RPCs exist.
    runtime.remoting().start().unwrap();
    let table = runtime.topic_route_table();
    assert!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
    );
    assert!(table.contains_key("topic").unwrap());
    let replacement: Arc<TopicRouteData> =
        Arc::new(serde_json::from_value(case["replacementInput"].clone()).unwrap());
    table.put("topic", replacement.clone()).unwrap();
    assert!(Arc::ptr_eq(
        &runtime.query_topic_route("topic").await.unwrap().unwrap(),
        &replacement
    ));
    table.clear().unwrap();
    assert!(runtime.get_existing_topic_route("topic").unwrap().is_none());
    assert_eq!(
        runtime.route_snapshot().unwrap().publish_address("broker"),
        case["publishAddressAfterClear"].as_str()
    );
    let refreshed =
        tokio::time::timeout(Duration::from_secs(5), runtime.query_topic_route("topic"))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    assert!(Arc::ptr_eq(
        &table.get("topic").unwrap().unwrap(),
        &refreshed
    ));
    assert_eq!(
        refreshed.broker_datas.as_ref().unwrap()[0]
            .broker_addrs
            .as_ref()
            .unwrap()[&0],
        Some("master:10911".to_owned())
    );
    assert!(Arc::ptr_eq(
        &runtime.query_topic_route("topic").await.unwrap().unwrap(),
        &refreshed
    ));
    runtime.remoting().shutdown().await;
    tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .unwrap()
        .unwrap();
    assert!(table.contains_key("topic").unwrap()); // Transport shutdown does not erase raw data.
}

#[tokio::test]
async fn raw_map_put_if_absent_is_atomic_and_values_are_owned_without_runtime_retention() {
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("raw-table-atomic"))
        .await
        .unwrap();
    let table = runtime.topic_route_table();
    let owner = Arc::new(TopicRouteData::default());
    let installed = std::thread::scope(|scope| {
        let mut calls = Vec::new();
        for _ in 0..64 {
            let table = table.clone();
            let owner = owner.clone();
            calls.push(scope.spawn(move || table.put_if_absent("topic", owner).unwrap().is_none()));
        }
        calls
            .into_iter()
            .map(|call| usize::from(call.join().unwrap()))
            .sum::<usize>()
    });
    assert_eq!(installed, 1);
    let before = runtime.route_snapshot().unwrap();
    assert!(table.replace("absent", owner.clone()).unwrap().is_none());
    assert!(
        table
            .put_if_absent("topic", owner.clone())
            .unwrap()
            .is_some()
    );
    assert!(Arc::ptr_eq(&before, &runtime.route_snapshot().unwrap()));
    let weak_owner = Arc::downgrade(&owner);
    drop(owner);
    assert!(weak_owner.upgrade().is_some());
    let detached = table.snapshot().unwrap();
    table.clear().unwrap();
    assert!(table.is_empty().unwrap());
    assert!(weak_owner.upgrade().is_some());
    drop(detached);
    drop(before);
    assert!(weak_owner.upgrade().is_none());
}

#[tokio::test]
async fn retained_raw_topic_table_matches_invoked_java_primitives_and_cached_queries() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let case = &fixture["runtimeTopicRouteTable"];
    let first: Arc<TopicRouteData> =
        Arc::new(serde_json::from_value(case["initialInput"].clone()).unwrap());
    let second: Arc<TopicRouteData> =
        Arc::new(serde_json::from_value(case["replacementInput"].clone()).unwrap());
    let equal_second = Arc::new(second.as_ref().clone());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("raw-table"))
        .await
        .unwrap();
    let table = runtime.topic_route_table();
    let retained = table.clone();
    let same = |value: Option<Arc<TopicRouteData>>, expected: &Arc<TopicRouteData>| {
        value.is_some_and(|value| Arc::ptr_eq(&value, expected))
    };
    let mut outcomes = BTreeMap::new();
    outcomes.insert("initialEmpty", table.is_empty().unwrap());
    outcomes.insert(
        "firstPutAbsent",
        table.put("topic", first.clone()).unwrap().is_none(),
    );
    outcomes.insert(
        "getOrDefaultPresent",
        Arc::ptr_eq(
            &table.get_or_default("topic", second.clone()).unwrap(),
            &first,
        ),
    );
    outcomes.insert(
        "getOrDefaultAbsent",
        Arc::ptr_eq(
            &table.get_or_default("missing", second.clone()).unwrap(),
            &second,
        ),
    );
    outcomes.insert(
        "getterSeesFirst",
        same(runtime.get_existing_topic_route("topic").unwrap(), &first),
    );
    outcomes.insert(
        "putReturnsFirst",
        same(table.put("topic", second.clone()).unwrap(), &first),
    );
    outcomes.insert(
        "retainedGetterSeesReplacement",
        same(retained.get("topic").unwrap(), &second)
            && same(runtime.get_existing_topic_route("topic").unwrap(), &second),
    );
    outcomes.insert(
        "putIfAbsentKeepsSecond",
        same(
            table.put_if_absent("topic", first.clone()).unwrap(),
            &second,
        ),
    );
    outcomes.insert(
        "unequalReplaceRejected",
        !table
            .replace_if_equal("topic", &first, second.clone())
            .unwrap(),
    );
    outcomes.insert(
        "equalCopyReplaceAccepted",
        table
            .replace_if_equal("topic", &equal_second, first.clone())
            .unwrap(),
    );
    outcomes.insert(
        "replaceReturnsFirst",
        same(table.replace("topic", second.clone()).unwrap(), &first),
    );
    outcomes.insert(
        "unequalRemoveRejected",
        !table.remove_if_equal("topic", &first).unwrap(),
    );
    outcomes.insert(
        "equalCopyRemoveAccepted",
        table.remove_if_equal("topic", &equal_second).unwrap(),
    );
    outcomes.insert(
        "absentReplaceNull",
        table.replace("topic", first.clone()).unwrap().is_none(),
    );
    outcomes.insert("absentRemoveNull", table.remove("topic").unwrap().is_none());
    outcomes.insert(
        "putIfAbsentAbsentNull",
        table
            .put_if_absent("topic", first.clone())
            .unwrap()
            .is_none(),
    );
    outcomes.insert(
        "emptyKeyAccepted",
        table.put("", second.clone()).unwrap().is_none(),
    );
    outcomes.insert(
        "sizeTwo",
        table.len().unwrap() == 2 && table.contains_key("").unwrap(),
    );
    let detached = table.snapshot().unwrap();
    table.clear().unwrap();
    outcomes.insert("retainedViewSeesClear", retained.is_empty().unwrap());
    outcomes.insert(
        "getterSeesClear",
        runtime.get_existing_topic_route("topic").unwrap().is_none(),
    );
    outcomes.insert(
        "putAfterClearAccepted",
        table.put("topic", second.clone()).unwrap().is_none()
            && same(runtime.get_existing_topic_route("topic").unwrap(), &second),
    );
    let bulk_values = BTreeMap::from([
        ("bulk-a".to_owned(), first.clone()),
        ("bulk-b".to_owned(), second.clone()),
    ]);
    table.put_all(bulk_values).unwrap();
    outcomes.insert(
        "putAllAdds",
        same(table.get("bulk-a").unwrap(), &first)
            && same(table.get("bulk-b").unwrap(), &second)
            && table.len().unwrap() == 3,
    );
    outcomes.insert(
        "containsValueEqualCopy",
        table.contains_value(&equal_second).unwrap(),
    );
    let mut absent_route = second.as_ref().clone();
    absent_route.broker_datas.as_mut().unwrap()[0].broker_name = Some("absent-broker".into());
    outcomes.insert(
        "containsValueAbsent",
        !table.contains_value(&absent_route).unwrap(),
    );
    let expected_routes = BTreeMap::from([
        ("topic".to_owned(), second.clone()),
        ("bulk-a".to_owned(), first.clone()),
        ("bulk-b".to_owned(), second.clone()),
    ]);
    let mut visited_routes = BTreeMap::new();
    table
        .for_each(|key, value| {
            let reentered_len = table.len().unwrap();
            visited_routes.insert(
                key.to_owned(),
                reentered_len == expected_routes.len()
                    && expected_routes
                        .get(key)
                        .is_some_and(|expected| Arc::ptr_eq(value, expected)),
            );
        })
        .unwrap();
    outcomes.insert(
        "forEachVisitsCurrentMappings",
        visited_routes.len() == expected_routes.len()
            && visited_routes.values().all(|matches| *matches),
    );
    outcomes.insert(
        "computeIfAbsentMaps",
        same(
            table
                .compute_if_absent("compute-a", |_| Some(first.clone()))
                .unwrap(),
            &first,
        ),
    );
    outcomes.insert(
        "computeIfAbsentKeepsCurrent",
        same(
            table
                .compute_if_absent("compute-a", |_| Some(second.clone()))
                .unwrap(),
            &first,
        ),
    );
    outcomes.insert(
        "computeIfPresentReplaces",
        same(
            table
                .compute_if_present("compute-a", |_, _| Some(second.clone()))
                .unwrap(),
            &second,
        ) && same(table.get("compute-a").unwrap(), &second),
    );
    outcomes.insert(
        "computeReplaces",
        same(
            table
                .compute("compute-a", |_, _| Some(first.clone()))
                .unwrap(),
            &first,
        ) && same(table.get("compute-a").unwrap(), &first),
    );
    outcomes.insert(
        "computeAbsentInserts",
        same(
            table
                .compute("compute-b", |_, _| Some(second.clone()))
                .unwrap(),
            &second,
        ),
    );
    outcomes.insert(
        "computeNullRemoves",
        table.compute("compute-b", |_, _| None).unwrap().is_none()
            && !table.contains_key("compute-b").unwrap(),
    );
    outcomes.insert(
        "mergeCombines",
        same(
            table
                .merge("compute-a", second.clone(), |_, added| Some(added.clone()))
                .unwrap(),
            &second,
        ),
    );
    outcomes.insert(
        "mergeAbsentInserts",
        same(
            table
                .merge("merge-new", first.clone(), |_, _| None)
                .unwrap(),
            &first,
        ),
    );
    table
        .replace_all(|key, _| {
            if key == "topic" {
                second.clone()
            } else {
                first.clone()
            }
        })
        .unwrap();
    outcomes.insert(
        "replaceAllUpdatesMappings",
        table.len().unwrap() == 5
            && same(table.get("topic").unwrap(), &second)
            && same(table.get("bulk-a").unwrap(), &first)
            && same(table.get("compute-a").unwrap(), &first)
            && same(table.get("merge-new").unwrap(), &first),
    );

    table.clear().unwrap();
    let key_view = table.key_set_view();
    table.put("key-remove", first.clone()).unwrap();
    table.put("key-keep", second.clone()).unwrap();
    table.put("key-retain", first.clone()).unwrap();
    outcomes.insert("keyViewContains", key_view.contains("key-keep").unwrap());
    outcomes.insert(
        "keyViewRemove",
        key_view.remove("key-remove").unwrap() && !table.contains_key("key-remove").unwrap(),
    );
    outcomes.insert(
        "keyViewRemoveIf",
        key_view.remove_if(|key| key == "key-keep").unwrap() > 0
            && !table.contains_key("key-keep").unwrap(),
    );
    table.put("key-all", second.clone()).unwrap();
    table.put("key-retain-all", first.clone()).unwrap();
    outcomes.insert(
        "keyViewRemoveAll",
        key_view
            .remove_all(&BTreeSet::from(["key-all".to_owned()]))
            .unwrap()
            > 0
            && !table.contains_key("key-all").unwrap(),
    );
    outcomes.insert(
        "keyViewRetainAll",
        key_view
            .retain_all(&BTreeSet::from(["key-retain-all".to_owned()]))
            .unwrap()
            > 0
            && table.len().unwrap() == 1
            && table.contains_key("key-retain-all").unwrap(),
    );

    table.clear().unwrap();
    let values_view = table.values_view();
    table.put("value-a", first.clone()).unwrap();
    table.put("value-b", second.clone()).unwrap();
    table.put("value-c", second.clone()).unwrap();
    outcomes.insert(
        "valuesViewContainsEqualCopy",
        values_view.contains(&equal_second).unwrap(),
    );
    outcomes.insert(
        "valuesViewRemoveEqualCopy",
        values_view.remove(&equal_second).unwrap()
            && table
                .snapshot()
                .unwrap()
                .values()
                .filter(|value| value.as_ref() == equal_second.as_ref())
                .count()
                == 1,
    );
    outcomes.insert(
        "valuesViewRemoveIf",
        values_view
            .remove_if(|value| value == first.as_ref())
            .unwrap()
            > 0
            && !table.contains_key("value-a").unwrap(),
    );
    table.clear().unwrap();
    table.put("value-all", second.clone()).unwrap();
    table.put("value-retain-drop", second.clone()).unwrap();
    outcomes.insert(
        "valuesViewRemoveAllEqual",
        values_view.remove_all(&[equal_second.clone()]).unwrap() > 0
            && !table.contains_key("value-all").unwrap()
            && !table.contains_key("value-retain-drop").unwrap(),
    );
    table.put("value-retain-drop", second.clone()).unwrap();
    table.put("value-retain-keep", first.clone()).unwrap();
    outcomes.insert(
        "valuesViewRetainAllEqual",
        values_view.retain_all(&[first.clone()]).unwrap() > 0
            && table.len().unwrap() == 1
            && table.contains_key("value-retain-keep").unwrap(),
    );

    table.clear().unwrap();
    let entry_view = table.entry_set_view();
    table.put("entry-remove", first.clone()).unwrap();
    table.put("entry-keep", second.clone()).unwrap();
    outcomes.insert(
        "entryViewContainsEqualCopy",
        entry_view.contains("entry-keep", &equal_second).unwrap(),
    );
    outcomes.insert(
        "entryViewRemoveEqualCopy",
        entry_view.remove("entry-remove", &first).unwrap()
            && !table.contains_key("entry-remove").unwrap(),
    );
    outcomes.insert(
        "entryViewRemoveIf",
        entry_view.remove_if(|key, _| key == "entry-keep").unwrap() > 0
            && !table.contains_key("entry-keep").unwrap(),
    );
    table.put("entry-all", second.clone()).unwrap();
    table.put("entry-retain-drop", second.clone()).unwrap();
    table.put("entry-retain-keep", first.clone()).unwrap();
    outcomes.insert(
        "entryViewRemoveAllEqual",
        entry_view
            .remove_all(&BTreeMap::from([(
                "entry-all".to_owned(),
                equal_second.clone(),
            )]))
            .unwrap()
            > 0
            && !table.contains_key("entry-all").unwrap(),
    );
    outcomes.insert(
        "entryViewRetainAllEqual",
        entry_view
            .retain_all(&BTreeMap::from([(
                "entry-retain-keep".to_owned(),
                first.clone(),
            )]))
            .unwrap()
            > 0
            && table.len().unwrap() == 1
            && table.contains_key("entry-retain-keep").unwrap(),
    );

    table.clear().unwrap();
    table.put("iterator-key", first.clone()).unwrap();
    outcomes.insert(
        "keyIteratorRemove",
        key_view.remove("iterator-key").unwrap() && table.is_empty().unwrap(),
    );
    table.put("iterator-value", first.clone()).unwrap();
    outcomes.insert(
        "valuesIteratorRemove",
        values_view.remove(&first).unwrap() && table.is_empty().unwrap(),
    );
    table.put("iterator-entry", first.clone()).unwrap();
    outcomes.insert(
        "entryIteratorRemove",
        entry_view.remove("iterator-entry", &first).unwrap() && table.is_empty().unwrap(),
    );
    // Rust uses explicit live-view removals in place of Java Iterator.remove; snapshot traversal
    // stays detached and does not claim Java's weakly consistent iteration contract.
    table.put("topic", second.clone()).unwrap();
    table.remove("bulk-a").unwrap();
    table.remove("bulk-b").unwrap();
    table.remove("compute-a").unwrap();
    table.remove("merge-new").unwrap();
    let actual_outcomes = serde_json::to_value(outcomes).unwrap();
    for (name, outcome) in actual_outcomes.as_object().unwrap() {
        assert_eq!(
            outcome, &case["primitives"][name],
            "Java route table outcome {name}"
        );
    }
    assert_eq!(actual_outcomes, case["primitives"]);
    assert_eq!(detached.len(), 2);
    assert!(same(
        runtime.query_topic_route("topic").await.unwrap(),
        &second
    ));
    assert_eq!(
        runtime
            .find_broker_address_by_topic("topic")
            .unwrap()
            .as_deref(),
        case["topicAddressAfterDirectPut"].as_str()
    );
    assert!(runtime.route_snapshot().unwrap().topics.is_empty()); // No converted publication.
    let metadata = Arc::new(serde_json::from_value(case["metadataOnlyInput"].clone()).unwrap());
    table.put("metadata", first.clone()).unwrap();
    assert_eq!(
        table.remove_if_equal("metadata", &metadata).unwrap(),
        case["metadataEqualConditionalRemoveAccepted"]
    );
    let mut invalid = first.as_ref().clone();
    invalid.broker_datas.as_mut().unwrap()[0].broker_addrs = None;
    let invalid = Arc::new(invalid);
    table.put("invalid", invalid.clone()).unwrap();
    assert!(same(
        runtime.query_topic_route("invalid").await.unwrap(),
        &invalid
    ));
    assert!(runtime.find_broker_address_by_topic("invalid").is_err());
    let weak_runtime = Arc::downgrade(&runtime);
    drop(runtime);
    assert!(weak_runtime.upgrade().is_none());
    assert!(same(retained.get("invalid").unwrap(), &invalid));
    table.clear().unwrap();
    assert!(retained.is_empty().unwrap());
}
