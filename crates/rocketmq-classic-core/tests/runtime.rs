use bytes::BytesMut;
use rocketmq_classic_protocol::codes::request;
use rocketmq_classic_remoting::{
    RemotingCommand,
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Duration, Instant},
};

use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{
        ClientRuntime, ClientRuntimeManager, ConsumerRuntime, ProducerRuntime, RuntimeError,
    },
};

struct RouteObserver {
    runtime: Weak<ClientRuntime>,
    updates: AtomicUsize,
    needed: AtomicBool,
    need_checks: AtomicUsize,
}

impl ProducerRuntime for RouteObserver {
    fn update_topic_publish_info(
        &self,
        topic: &str,
        info: Arc<rocketmq_classic_core::route::TopicSnapshot>,
    ) {
        let runtime = self.runtime.upgrade().unwrap();
        assert!(runtime.select_producer("producer").unwrap().is_some());
        assert_eq!(
            runtime.route_snapshot().unwrap().topics[topic].publish_queues,
            info.publish_queues
        );
        self.updates.fetch_add(1, Ordering::SeqCst);
    }
}

impl ConsumerRuntime for RouteObserver {
    fn heartbeat_data(&self) -> rocketmq_classic_protocol::heartbeat::ConsumerData {
        rocketmq_classic_protocol::heartbeat::ConsumerData {
            group_name: "consumer".into(),
            ..Default::default()
        }
    }
    fn is_subscribe_topic_need_update(&self, _: &str) -> bool {
        self.need_checks.fetch_add(1, Ordering::SeqCst);
        self.needed.load(Ordering::SeqCst)
    }
    fn update_topic_subscribe_info(
        &self,
        topic: &str,
        queues: std::collections::BTreeSet<rocketmq_classic_common::message::MessageQueue>,
    ) {
        let runtime = self.runtime.upgrade().unwrap();
        assert!(runtime.select_consumer("consumer").unwrap().is_some());
        assert_eq!(
            runtime.route_snapshot().unwrap().topics[topic].subscribe_queues,
            queues
        );
        self.updates.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn all_topic_refresh_unions_registered_topics_and_continues_after_rejection() {
    use rocketmq_classic_protocol::heartbeat::{ConsumerData, SubscriptionData};
    use std::collections::BTreeSet;
    struct Topics(Weak<ClientRuntime>);
    impl ProducerRuntime for Topics {
        fn publish_topics(&self) -> BTreeSet<String> {
            assert!(
                self.0
                    .upgrade()
                    .unwrap()
                    .select_producer("producer")
                    .unwrap()
                    .is_some()
            );
            BTreeSet::from(["common".into(), "producer-only".into()])
        }
    }
    impl ConsumerRuntime for Topics {
        fn heartbeat_data(&self) -> ConsumerData {
            assert!(
                self.0
                    .upgrade()
                    .unwrap()
                    .select_consumer("consumer")
                    .unwrap()
                    .is_some()
            );
            ConsumerData {
                subscription_data_set: ["common", "consumer-only"]
                    .into_iter()
                    .map(|topic| SubscriptionData {
                        topic: topic.into(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("all-topic-refresh");
    config.namesrv_addr = Some(listener.local_addr().unwrap().to_string());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    // Empty registration round opens no NameServer connection.
    runtime.update_all_topic_routes().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    let owners = Arc::new(Topics(Arc::downgrade(&runtime)));
    runtime
        .register_producer("producer", owners.clone())
        .await
        .unwrap();
    runtime
        .register_consumer("consumer", owners.clone())
        .await
        .unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let body = serde_json::to_vec(&fixture["cases"][0]["input"]).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut seen = BTreeSet::new();
        for _ in 0..3 {
            let mut length = [0; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            let command = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .unwrap();
            assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
            let topic = command.ext_fields.as_ref().unwrap()["topic"].clone();
            assert!(seen.insert(topic.clone()), "duplicate route request");
            let mut reply = RemotingCommand {
                code: if topic == "common" { 987654 } else { 0 },
                opaque: command.opaque,
                body: Some(body.clone()),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
                .await
                .unwrap();
        }
        assert_eq!(
            seen,
            BTreeSet::from([
                "common".into(),
                "consumer-only".into(),
                "producer-only".into()
            ])
        );
    });
    tokio::time::timeout(Duration::from_secs(5), runtime.update_all_topic_routes())
        .await
        .unwrap()
        .unwrap();
    peer.await.unwrap();
    let snapshot = runtime.route_snapshot().unwrap();
    assert!(!snapshot.topics.contains_key("common"));
    assert!(snapshot.topics.contains_key("consumer-only"));
    assert!(snapshot.topics.contains_key("producer-only"));
    runtime.remoting().shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p6_v01_many_first_lookups_share_one_unstarted_runtime_and_transport_owner() {
    let manager = ClientRuntimeManager::new();
    let config = ClientConfig::new("192.0.2.6");
    let mut calls = Vec::new();
    for _ in 0..128 {
        let manager = manager.clone();
        let config = config.clone();
        calls.push(tokio::spawn(async move {
            manager.get_or_create(&config).await.unwrap()
        }));
    }
    let first = calls.remove(0).await.unwrap();
    for call in calls {
        assert!(Arc::ptr_eq(&first, &call.await.unwrap()));
    }
    assert_eq!(first.client_id(), config.build_client_id());
}

#[tokio::test]
async fn p6_v02_incompatible_transport_configuration_is_not_silently_shared() {
    let manager = ClientRuntimeManager::new();
    let mut config = ClientConfig::new("192.0.2.6");
    let first = manager.get_or_create(&config).await.unwrap();
    config.namesrv_addr = Some("127.0.0.1:9876".into());
    assert!(matches!(
        manager.get_or_create(&config).await,
        Err(RuntimeError::ConfigurationConflict { .. })
    ));
    config.namesrv_addr = None;
    config.decode_read_body = false; // Consumer decoder policy, not transport ownership.
    assert!(Arc::ptr_eq(
        &first,
        &manager.get_or_create(&config).await.unwrap()
    ));
}

#[tokio::test]
async fn p6_v02_failed_construction_can_retry_and_stale_removal_preserves_replacement() {
    let manager = ClientRuntimeManager::new();
    let mut config = ClientConfig::new("192.0.2.6");
    config.enable_concurrent_heartbeat = true;
    config.concurrent_heartbeat_thread_pool_size = 0;
    assert!(manager.get_or_create(&config).await.is_err());
    config.concurrent_heartbeat_thread_pool_size = 1;
    let first = manager.get_or_create(&config).await.unwrap();
    assert!(
        manager
            .remove_client_factory(first.client_id(), &first)
            .unwrap()
    );
    let replacement = manager.get_or_create(&config).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &replacement));
    assert!(
        !manager
            .remove_client_factory(first.client_id(), &first)
            .unwrap()
    );
    assert!(Arc::ptr_eq(
        &replacement,
        &manager.get_or_create(&config).await.unwrap()
    ));
}

#[tokio::test]
async fn route_refresh_failure_preserves_last_coherent_snapshot() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("192.0.2.6");
    config.namesrv_addr = Some(listener.local_addr().unwrap().to_string());
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let body = serde_json::to_vec(&fixture["cases"][0]["input"]).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for code in [0, 0, 0, 987654] {
            let mut length = [0; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            let command = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .unwrap();
            assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
            let mut reply = RemotingCommand {
                code,
                opaque: command.opaque,
                body: Some(body.clone()),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
                .await
                .unwrap();
        }
    });
    let manager = ClientRuntimeManager::new();
    let runtime = manager.get_or_create(&config).await.unwrap();
    let producer = Arc::new(RouteObserver {
        runtime: Arc::downgrade(&runtime),
        updates: AtomicUsize::new(0),
        needed: AtomicBool::new(false),
        need_checks: AtomicUsize::new(0),
    });
    let consumer = Arc::new(RouteObserver {
        runtime: Arc::downgrade(&runtime),
        updates: AtomicUsize::new(0),
        needed: AtomicBool::new(false),
        need_checks: AtomicUsize::new(0),
    });
    runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    // Exercise route refresh in isolation, without scheduled refresh races.
    runtime.remoting().start().unwrap();
    assert!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    assert_eq!(producer.updates.load(Ordering::SeqCst), 1);
    assert_eq!(consumer.updates.load(Ordering::SeqCst), 1);
    assert_eq!(consumer.need_checks.load(Ordering::SeqCst), 0);
    assert!(
        !runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    assert_eq!(producer.updates.load(Ordering::SeqCst), 1);
    consumer.needed.store(true, Ordering::SeqCst);
    assert_eq!(consumer.need_checks.load(Ordering::SeqCst), 1);
    assert!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    assert_eq!(producer.updates.load(Ordering::SeqCst), 2);
    assert_eq!(consumer.updates.load(Ordering::SeqCst), 2);
    assert_eq!(consumer.need_checks.load(Ordering::SeqCst), 2);
    let before = runtime.route_snapshot().unwrap();
    assert!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await
            .is_ok_and(|changed| !changed)
    );
    assert!(Arc::ptr_eq(&before, &runtime.route_snapshot().unwrap()));
    assert_eq!(before.topics["topic"].publish_queues.len(), 2);
    assert_eq!(consumer.need_checks.load(Ordering::SeqCst), 2);
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn different_topic_refreshes_serialize_wire_fetches_without_blocking_snapshot_readers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("192.0.2.6");
    config.namesrv_addr = Some(listener.local_addr().unwrap().to_string());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    let (seen, first_seen) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut seen = Some(seen);
        let mut released = Some(released);
        for topic in ["first", "second"] {
            let mut length = [0; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            let command = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .unwrap();
            assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
            assert_eq!(command.ext_fields.as_ref().unwrap()["topic"], topic);
            if topic == "first" {
                seen.take().unwrap().send(()).unwrap();
                // An ungated second refresh would put its frame on this socket
                // before the first reply. read_exact is cancellation-safe here
                // because no bytes are expected on the correct path.
                let mut unexpected = [0; 1];
                assert!(
                    tokio::time::timeout(
                        Duration::from_millis(30),
                        stream.read_exact(&mut unexpected)
                    )
                    .await
                    .is_err()
                );
                released.take().unwrap().await.unwrap();
            }
            let mut reply = RemotingCommand {
                opaque: command.opaque,
                body: Some(
                    br#"{"queueDatas":[],"brokerDatas":[],"filterServerTable":{}}"#.to_vec(),
                ),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
                .await
                .unwrap();
        }
    });
    let first = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .update_topic_route("first", Instant::now() + Duration::from_secs(5))
                .await
        })
    };
    first_seen.await.unwrap();
    let before = runtime.route_snapshot().unwrap();
    let second = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .update_topic_route("second", Instant::now() + Duration::from_secs(5))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!first.is_finished());
    assert!(!second.is_finished());
    assert!(Arc::ptr_eq(&before, &runtime.route_snapshot().unwrap()));
    release.send(()).unwrap();
    assert!(first.await.unwrap().unwrap());
    assert!(second.await.unwrap().unwrap());
    assert_eq!(runtime.route_snapshot().unwrap().topics.len(), 2);
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn default_route_fetch_uses_auto_create_topic_and_clamps_both_counts_from_read_count() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("192.0.2.6");
    config.namesrv_addr = Some(listener.local_addr().unwrap().to_string());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for (topic, code) in [
            ("TBW102", 0),
            ("TBW102", 0),
            ("TBW102", 17),
            ("missing", 17),
            ("missing-body", 0),
            ("invalid-json", 0),
        ] {
            let mut length = [0; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            let command = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .unwrap();
            assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
            assert_eq!(command.ext_fields.as_ref().unwrap()["topic"], topic);
            let mut reply = RemotingCommand {
                code,
                opaque: command.opaque,
                body: Some(br#"{"queueDatas":[{"brokerName":"a","readQueueNums":8,"writeQueueNums":1,"perm":6,"topicSysFlag":0},{"brokerName":"b","readQueueNums":2,"writeQueueNums":9,"perm":6,"topicSysFlag":0}],"brokerDatas":[{"cluster":"cluster","brokerName":"a","brokerAddrs":{"0":"a:10911"}},{"cluster":"cluster","brokerName":"b","brokerAddrs":{"0":"b:10911"}}],"filterServerTable":{}}"#.to_vec()),
                ..Default::default()
            };
            if topic == "missing-body" {
                reply.body = None;
            } else if topic == "invalid-json" {
                reply.body = Some(b"{".to_vec());
            }
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
                .await
                .unwrap();
        }
    });
    assert!(
        runtime
            .update_topic_route_from_default("created", 4, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap()
    );
    let before = runtime.route_snapshot().unwrap();
    let info = &before.topics["created"];
    assert_eq!(
        info.route
            .queue_datas
            .iter()
            .map(|data| (data.read_queue_nums, data.write_queue_nums))
            .collect::<Vec<_>>(),
        [(4, 4), (2, 2)]
    );
    assert_eq!(info.publish_queues.len(), 6);
    assert_eq!(info.subscribe_queues.len(), 6);
    assert!(
        info.publish_queues
            .iter()
            .all(|queue| queue.topic() == "created")
    );
    assert!(!before.topics.contains_key("TBW102"));
    assert!(
        runtime
            .update_topic_route_from_default(
                "negative",
                -2,
                Instant::now() + Duration::from_secs(5)
            )
            .await
            .unwrap()
    );
    let negative = runtime.route_snapshot().unwrap();
    assert!(negative.topics["negative"].publish_queues.is_empty());
    assert!(
        negative.topics["negative"]
            .route
            .queue_datas
            .iter()
            .all(|data| data.read_queue_nums == -2 && data.write_queue_nums == -2)
    );
    assert!(
        !runtime
            .update_topic_route_from_default("created", 4, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap()
    );
    assert!(Arc::ptr_eq(&negative, &runtime.route_snapshot().unwrap()));
    assert!(
        !runtime
            .update_topic_route("missing", Instant::now() + Duration::from_secs(5))
            .await
            .unwrap()
    );
    assert!(Arc::ptr_eq(&negative, &runtime.route_snapshot().unwrap()));
    assert!(
        !runtime
            .update_topic_route("missing-body", Instant::now() + Duration::from_secs(5))
            .await
            .unwrap()
    );
    assert!(Arc::ptr_eq(&negative, &runtime.route_snapshot().unwrap()));
    assert!(
        runtime
            .update_topic_route("invalid-json", Instant::now() + Duration::from_secs(5))
            .await
            .is_err()
    );
    assert!(Arc::ptr_eq(&negative, &runtime.route_snapshot().unwrap()));
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
    assert!(
        runtime
            .update_topic_route_from_default("created", 4, Instant::now() + Duration::from_secs(1))
            .await
            .is_err()
    );
    assert!(Arc::ptr_eq(&negative, &runtime.route_snapshot().unwrap()));
}
