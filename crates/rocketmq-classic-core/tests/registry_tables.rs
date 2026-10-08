use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, ConsumerRuntime, ProducerRuntime, RuntimeError},
};
use rocketmq_classic_protocol::heartbeat::ConsumerData;
use std::{collections::BTreeMap, sync::Arc};

struct Producer;
impl ProducerRuntime for Producer {}
struct Consumer;
impl ConsumerRuntime for Consumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData::default()
    }
}

fn same<T: ?Sized>(value: Option<Arc<T>>, expected: &Arc<T>) -> bool {
    value.is_some_and(|value| Arc::ptr_eq(&value, expected))
}

macro_rules! check_table {
    ($runtime:ident, $table:ident, $first:ident, $second:ident, $select:ident, $expected:expr) => {{
        let mut outcomes = BTreeMap::new();
        let retained = $table.clone();
        outcomes.insert("initialEmpty", $table.is_empty().unwrap());
        outcomes.insert(
            "firstPutAbsent",
            $table.put("group", $first.clone()).await.unwrap().is_none(),
        );
        outcomes.insert(
            "selectorSeesFirst",
            same($runtime.$select("group").unwrap(), &$first),
        );
        outcomes.insert(
            "putReturnsFirst",
            same($table.put("group", $second.clone()).await.unwrap(), &$first),
        );
        outcomes.insert(
            "selectorSeesReplacement",
            same($runtime.$select("group").unwrap(), &$second)
                && same(retained.get("group").unwrap(), &$second),
        );
        outcomes.insert(
            "putIfAbsentKeepsSecond",
            same(
                $table.put_if_absent("group", $first.clone()).await.unwrap(),
                &$second,
            ),
        );
        outcomes.insert(
            "staleReplaceRejected",
            !$table
                .replace_if_same("group", &$first, $second.clone())
                .await
                .unwrap(),
        );
        outcomes.insert(
            "conditionalReplaceAccepted",
            $table
                .replace_if_same("group", &$second, $first.clone())
                .await
                .unwrap(),
        );
        outcomes.insert(
            "replaceReturnsFirst",
            same(
                $table.replace("group", $second.clone()).await.unwrap(),
                &$first,
            ),
        );
        outcomes.insert(
            "staleRemoveRejected",
            !$table.remove_if_same("group", &$first).await.unwrap(),
        );
        outcomes.insert(
            "conditionalRemoveAccepted",
            $table.remove_if_same("group", &$second).await.unwrap(),
        );
        outcomes.insert(
            "absentReplaceNull",
            $table
                .replace("group", $first.clone())
                .await
                .unwrap()
                .is_none(),
        );
        outcomes.insert(
            "absentRemoveNull",
            $table.remove("group").await.unwrap().is_none(),
        );
        outcomes.insert(
            "putIfAbsentAbsentNull",
            $table
                .put_if_absent("group", $first.clone())
                .await
                .unwrap()
                .is_none(),
        );
        outcomes.insert(
            "emptyKeyAccepted",
            $table.put("", $second.clone()).await.unwrap().is_none(),
        );
        outcomes.insert(
            "sizeTwo",
            $table.len().unwrap() == 2
                && $table.snapshot().unwrap().len() == 2
                && $table.contains_key("").unwrap(),
        );
        $table.clear().await.unwrap();
        outcomes.insert("retainedViewSeesClear", retained.is_empty().unwrap());
        outcomes.insert(
            "selectorSeesClear",
            $runtime.$select("group").unwrap().is_none(),
        );
        outcomes.insert(
            "putAfterClearAccepted",
            $table
                .put("group", $second.clone())
                .await
                .unwrap()
                .is_none()
                && same($runtime.$select("group").unwrap(), &$second),
        );
        $table.clear().await.unwrap();
        assert_eq!(serde_json::to_value(outcomes).unwrap(), $expected);
    }};
}

#[tokio::test]
async fn retained_registry_tables_match_every_invoked_java_primitive_outcome() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("registry-tables"))
        .await
        .unwrap();
    let producer = runtime.producer_table();
    let first: Arc<dyn ProducerRuntime> = Arc::new(Producer);
    let second: Arc<dyn ProducerRuntime> = Arc::new(Producer);
    check_table!(
        runtime,
        producer,
        first,
        second,
        select_producer,
        fixture["registryTables"]["producer"]
    );
    let consumer = runtime.consumer_table();
    let first: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let second: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    check_table!(
        runtime,
        consumer,
        first,
        second,
        select_consumer,
        fixture["registryTables"]["consumer"]
    );
}

#[tokio::test]
async fn conditional_registry_mutations_match_java_custom_value_equality() {
    struct EqualProducer(&'static str);
    impl ProducerRuntime for EqualProducer {
        fn map_value_equality_key(&self) -> Option<String> {
            Some(self.0.to_owned())
        }
    }
    struct EqualConsumer(&'static str);
    impl ConsumerRuntime for EqualConsumer {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData::default()
        }
        fn map_value_equality_key(&self) -> Option<String> {
            Some(self.0.to_owned())
        }
    }
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let expected = &fixture["registryTables"]["customEquality"];
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("registry-custom-equality"))
        .await
        .unwrap();
    let producers = runtime.producer_table();
    let producer_a: Arc<dyn ProducerRuntime> = Arc::new(EqualProducer("producer-equal"));
    let producer_b: Arc<dyn ProducerRuntime> = Arc::new(EqualProducer("producer-equal"));
    let producer_c: Arc<dyn ProducerRuntime> = Arc::new(EqualProducer("producer-replacement"));
    producers.put("replace", producer_a.clone()).await.unwrap();
    assert!(
        producers
            .replace_if_equal("replace", &producer_b, producer_c)
            .await
            .unwrap()
    );
    producers.put("remove", producer_a).await.unwrap();
    assert!(
        producers
            .remove_if_equal("remove", &producer_b)
            .await
            .unwrap()
    );

    let consumers = runtime.consumer_table();
    let consumer_a: Arc<dyn ConsumerRuntime> = Arc::new(EqualConsumer("consumer-equal"));
    let consumer_b: Arc<dyn ConsumerRuntime> = Arc::new(EqualConsumer("consumer-equal"));
    let consumer_c: Arc<dyn ConsumerRuntime> = Arc::new(EqualConsumer("consumer-replacement"));
    consumers.put("replace", consumer_a.clone()).await.unwrap();
    assert!(
        consumers
            .replace_if_equal("replace", &consumer_b, consumer_c)
            .await
            .unwrap()
    );
    consumers.put("remove", consumer_a).await.unwrap();
    assert!(
        consumers
            .remove_if_equal("remove", &consumer_b)
            .await
            .unwrap()
    );
    assert_eq!(
        serde_json::json!({
            "producerReplaceByEqualValue": true,
            "producerRemoveByEqualValue": true,
            "consumerReplaceByEqualValue": true,
            "consumerRemoveByEqualValue": true
        }),
        *expected
    );
}

#[tokio::test]
async fn registry_default_map_operations_match_invoked_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json"))
            .unwrap();
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("registry-default-operations"))
        .await
        .unwrap();
    macro_rules! check_operations {
        ($table:expr, $first:expr, $second:expr, $expected:expr) => {{
            let table = $table;
            let first = $first;
            let second = $second;
            let mut calls = 0;
            let compute_if_absent = table
                .compute_if_absent("a", |_| {
                    calls += 1;
                    Some(first.clone())
                })
                .await
                .unwrap();
            let compute_if_absent_again = table
                .compute_if_absent("a", |_| {
                    calls += 1;
                    Some(second.clone())
                })
                .await
                .unwrap();
            let compute_if_present = table
                .compute_if_present("a", |_, _| Some(second.clone()))
                .await
                .unwrap();
            let compute_if_present_absent = table
                .compute_if_present("missing", |_, _| Some(second.clone()))
                .await
                .unwrap();
            let compute_insert = table
                .compute("b", |_, _| Some(first.clone()))
                .await
                .unwrap();
            let compute_remove = table.compute("b", |_, _| None).await.unwrap();
            let merge_insert = table
                .merge("c", first.clone(), |_, _| Some(second.clone()))
                .await
                .unwrap();
            let merge_update = table
                .merge("c", second.clone(), |_, _| Some(second.clone()))
                .await
                .unwrap();
            table
                .replace_all(|_, _| first.clone())
                .await
                .unwrap();
            let mut for_each_count = 0;
            table
                .for_each(|_, _| for_each_count += 1)
                .unwrap();
            let actual = serde_json::json!({
                "computeIfAbsentInsert": same(compute_if_absent, &first),
                "computeIfAbsentNoRemap": same(compute_if_absent_again, &first) && calls == 1,
                "computeIfPresentUpdate": same(compute_if_present, &second),
                "computeIfPresentAbsent": compute_if_present_absent.is_none(),
                "computeInsert": same(compute_insert, &first),
                "computeRemove": compute_remove.is_none() && !table.contains_key("b").unwrap(),
                "mergeInsert": same(merge_insert, &first),
                "mergeUpdate": same(merge_update, &second),
                "replaceAll": same(table.get("a").unwrap(), &first)
                    && same(table.get("c").unwrap(), &first),
                "forEachCount": for_each_count,
                "keys": table.keys().unwrap(),
                "valuesCount": table.values().unwrap().len()
            });
            assert_eq!(actual, $expected);
            table.clear().await.unwrap();
        }};
    }
    check_operations!(
        runtime.producer_table(),
        Arc::new(Producer) as Arc<dyn ProducerRuntime>,
        Arc::new(Producer) as Arc<dyn ProducerRuntime>,
        fixture["registryTables"]["producerDefaultOperations"].clone()
    );
    check_operations!(
        runtime.consumer_table(),
        Arc::new(Consumer) as Arc<dyn ConsumerRuntime>,
        Arc::new(Consumer) as Arc<dyn ConsumerRuntime>,
        fixture["registryTables"]["consumerDefaultOperations"].clone()
    );
}

#[tokio::test]
async fn group_unregister_matches_invoked_java_consumer_methods_and_removes_current_binding() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("group-unregister"))
        .await
        .unwrap();
    let table = runtime.consumer_table();
    let first: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let second: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let lease = runtime
        .register_consumer("group", first.clone())
        .await
        .unwrap();
    let mut outcomes = BTreeMap::new();
    outcomes.insert("consumerRegisterAccepted", true);
    outcomes.insert(
        "consumerDirectPutReturnsRegistered",
        same(table.put("group", second.clone()).await.unwrap(), &first),
    );
    outcomes.insert(
        "consumerRegisterAfterDirectPutRejected",
        matches!(
            runtime.register_consumer("group", first).await,
            Err(RuntimeError::DuplicateGroup { .. })
        ),
    );
    assert!(
        !runtime
            .unregister_consumer(&lease, tokio::time::Instant::now())
            .await
            .unwrap()
    );
    assert!(
        runtime
            .unregister_consumer_by_group("group", tokio::time::Instant::now())
            .await
            .unwrap()
    );
    outcomes.insert(
        "consumerUnregisterRemovesCurrentMapping",
        table.get("group").unwrap().is_none(),
    );
    for (name, outcome) in outcomes {
        assert_eq!(
            serde_json::json!(outcome),
            fixture["registryTables"][name],
            "{name}"
        );
    }
    assert!(
        !runtime
            .unregister_consumer_by_group("group", tokio::time::Instant::now())
            .await
            .unwrap()
    );
    let producer: Arc<dyn ProducerRuntime> = Arc::new(Producer);
    runtime
        .producer_table()
        .put("", producer.clone())
        .await
        .unwrap();
    assert!(
        runtime
            .unregister_producer_by_group("", tokio::time::Instant::now())
            .await
            .unwrap()
    );
    assert!(
        !runtime
            .unregister_producer_by_group("", tokio::time::Instant::now())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn live_table_replacement_invalidates_old_lease_without_retaining_runtime_or_owners() {
    let manager = ClientRuntimeManager::new();
    let mut config = ClientConfig::new("registry-lease");
    config.namesrv_addr = Some(String::new());
    let runtime = manager.get_or_create(&config).await.unwrap();
    runtime.start().await.unwrap();
    let table = runtime.consumer_table();
    let first: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let second: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let lease = runtime
        .register_consumer("registered", first.clone())
        .await
        .unwrap();
    assert!(same(
        table.put("registered", second.clone()).await.unwrap(),
        &first
    ));
    assert!(matches!(
        runtime.register_consumer("registered", first.clone()).await,
        Err(RuntimeError::DuplicateGroup { .. })
    ));
    assert!(
        !runtime
            .unregister_consumer(&lease, tokio::time::Instant::now())
            .await
            .unwrap()
    );
    assert!(same(table.get("registered").unwrap(), &second));
    assert!(table.remove_if_same("registered", &second).await.unwrap());
    assert!(runtime.shutdown().await.unwrap());
    let weak_runtime = Arc::downgrade(&runtime);
    drop(runtime);
    drop(manager);
    assert!(weak_runtime.upgrade().is_none());
    assert!(table.put("after", second.clone()).await.unwrap().is_none());
    assert!(same(table.get("after").unwrap(), &second));
    let weak_owner = Arc::downgrade(&second);
    drop(second);
    assert!(weak_owner.upgrade().is_none());
    assert!(table.get("after").unwrap().is_none());
    assert!(table.contains_key("after").unwrap()); // Binding does not disappear on owner drop.
    table.clear().await.unwrap();
    assert!(table.is_empty().unwrap());
}

#[tokio::test]
async fn running_table_writes_wait_for_unregister_and_cancelled_queue_writes_do_not_commit() {
    use bytes::BytesMut;
    use rocketmq_classic_remoting::{
        RemotingCommand,
        codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::oneshot,
        time::{Duration, Instant},
    };
    async fn read(socket: &mut TcpStream) -> RemotingCommand {
        let mut length = [0; 4];
        socket.read_exact(&mut length).await.unwrap();
        let mut bytes = BytesMut::from(&length[..]);
        bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
        socket.read_exact(&mut bytes[4..]).await.unwrap();
        frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap()
    }
    async fn reply(socket: &mut TcpStream, request: &RemotingCommand, body: Option<Vec<u8>>) {
        let mut response = RemotingCommand {
            opaque: request.opaque,
            body,
            ..Default::default()
        };
        response.mark_response();
        socket
            .write_all(&frame::encode(&response, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
    }
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker.local_addr().unwrap().to_string();
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nameserver_address = nameserver.local_addr().unwrap().to_string();
    let name_peer = tokio::spawn(async move {
        let (mut socket, _) = nameserver.accept().await.unwrap();
        let request = read(&mut socket).await;
        assert_eq!(request.code, 105);
        let route = serde_json::to_vec(&serde_json::json!({"queueDatas":[],"brokerDatas":[{"cluster":"cluster","brokerName":"broker","brokerAddrs":{"0":broker_address}}],"filterServerTable":{}})).unwrap();
        reply(&mut socket, &request, Some(route)).await;
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });
    let (seen, received) = oneshot::channel();
    let (release, proceed) = oneshot::channel();
    let broker_peer = tokio::spawn(async move {
        let (mut socket, _) = broker.accept().await.unwrap();
        let request = read(&mut socket).await;
        assert_eq!(request.code, 35);
        assert_eq!(
            request.ext_fields.as_ref().unwrap()["consumerGroup"],
            "registered"
        );
        seen.send(()).unwrap();
        proceed.await.unwrap();
        reply(&mut socket, &request, None).await;
        for (field, group) in [
            ("consumerGroup", "absent"),
            ("producerGroup", "producer"),
            ("consumerGroup", "registered"),
        ] {
            let request = read(&mut socket).await;
            assert_eq!(request.code, 35);
            let fields = request.ext_fields.as_ref().unwrap();
            assert_eq!(fields[field], group);
            assert_eq!(fields["clientID"], "registry-rpc-order@DEFAULT");
            let other = if field == "consumerGroup" {
                "producerGroup"
            } else {
                "consumerGroup"
            };
            assert!(!fields.contains_key(other));
            if group == "absent" {
                let mut response = RemotingCommand {
                    code: 1,
                    opaque: request.opaque,
                    ..Default::default()
                };
                response.mark_response();
                socket
                    .write_all(&frame::encode(&response, DEFAULT_MAX_FRAME_SIZE).unwrap())
                    .await
                    .unwrap();
            } else {
                reply(&mut socket, &request, None).await;
            }
        }
        let internal = read(&mut socket).await;
        assert_eq!(internal.code, 35);
        assert_eq!(
            internal.ext_fields.as_ref().unwrap()["producerGroup"],
            "CLIENT_INNER_PRODUCER"
        );
        reply(&mut socket, &internal, None).await;
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });
    let mut config = ClientConfig::new("registry-rpc-order");
    config.namesrv_addr = Some(nameserver_address);
    config.enable_heartbeat_channel_event_listener = false;
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.start().await.unwrap();
    runtime
        .update_topic_route("topic", Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    let table = runtime.consumer_table();
    let first: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let second: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let lease = runtime
        .register_consumer("registered", first.clone())
        .await
        .unwrap();
    table
        .put("cancelled-unregister", second.clone())
        .await
        .unwrap();
    let unregister = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .unregister_consumer_by_group("registered", Instant::now() + Duration::from_secs(3))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), received)
        .await
        .unwrap()
        .unwrap();
    let write = {
        let table = table.clone();
        let second = second.clone();
        tokio::spawn(async move { table.put("registered", second).await })
    };
    let cancelled = {
        let table = table.clone();
        let second = second.clone();
        tokio::spawn(async move { table.put("cancelled", second).await })
    };
    let cancelled_unregister = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .unregister_consumer_by_group(
                    "cancelled-unregister",
                    Instant::now() + Duration::from_secs(3),
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!write.is_finished());
    assert!(!cancelled.is_finished());
    assert!(!cancelled_unregister.is_finished());
    assert!(table.get("registered").unwrap().is_none()); // Reads are not blocked by the actor's RPC.
    cancelled.abort();
    assert!(matches!(cancelled.await, Err(error) if error.is_cancelled()));
    cancelled_unregister.abort();
    assert!(matches!(cancelled_unregister.await, Err(error) if error.is_cancelled()));
    release.send(()).unwrap();
    assert!(unregister.await.unwrap().unwrap());
    assert!(write.await.unwrap().unwrap().is_none());
    assert!(same(table.get("registered").unwrap(), &second));
    assert!(!table.contains_key("cancelled").unwrap());
    assert!(same(table.get("cancelled-unregister").unwrap(), &second));
    assert!(
        !runtime
            .unregister_consumer(&lease, Instant::now())
            .await
            .unwrap()
    );
    // Absent keys still issue RPCs and Broker rejection remains best-effort.
    assert!(
        !runtime
            .unregister_consumer_by_group("absent", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
    );
    let producer: Arc<dyn ProducerRuntime> = Arc::new(Producer);
    runtime
        .producer_table()
        .put("producer", producer)
        .await
        .unwrap();
    // Weak-owner expiry does not make the binding absent.
    assert!(
        runtime
            .unregister_producer_by_group("producer", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
    );
    assert!(
        runtime
            .unregister_consumer_by_group("registered", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
    );
    table.clear().await.unwrap();
    assert!(runtime.shutdown().await.unwrap());
    // Terminal maps can change without reviving transport or its control worker.
    table.put("after-stop", second.clone()).await.unwrap();
    assert!(
        runtime
            .unregister_consumer_by_group("after-stop", Instant::now())
            .await
            .unwrap()
    );
    assert!(
        !runtime
            .unregister_consumer_by_group("after-stop", Instant::now())
            .await
            .unwrap()
    );
    tokio::time::timeout(Duration::from_secs(5), broker_peer)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), name_peer)
        .await
        .unwrap()
        .unwrap();
}
