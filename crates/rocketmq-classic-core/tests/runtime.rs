use bytes::BytesMut;
use rocketmq_classic_protocol::codes::request;
use rocketmq_classic_remoting::client::RpcHook;
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
        ClientRuntime, ClientRuntimeManager, ConsumerRuntime, PULL_MESSAGE_SERVICE_NAME,
        ProducerRuntime, REBALANCE_SERVICE_NAME, RuntimeError, SERVICE_THREAD_JOIN_TIME_MILLIS,
    },
};

struct IdentityHook;

impl RpcHook for IdentityHook {}

#[test]
fn service_names_match_invoked_java_public_methods() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let cases = fixture["serviceNameCases"].as_array().unwrap();
    assert_eq!(cases.len(), 2);
    for case in cases {
        let expected = match case["class"].as_str().unwrap() {
            "RebalanceService" => REBALANCE_SERVICE_NAME,
            "PullMessageService" => PULL_MESSAGE_SERVICE_NAME,
            name => panic!("unknown Java service name case: {name}"),
        };
        assert_eq!(case["serviceName"], expected);
    }
}

#[test]
fn inherited_service_thread_oracle_records_java_thread_controls() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    for case in fixture["serviceThreadCases"].as_array().unwrap() {
        assert_eq!(
            case["joinTimeMillis"].as_u64().unwrap(),
            SERVICE_THREAD_JOIN_TIME_MILLIS,
            "{}",
            case["class"]
        );
    }
    assert_eq!(
        fixture["serviceThreadCases"],
        serde_json::json!([
            {
                "class": "RebalanceService",
                "joinTimeMillis": 90000,
                "initiallyDaemon": false,
                "initiallyStopped": false,
                "daemonAfterSet": true,
                "stoppedAfterWakeup": false,
                "stoppedAfterMakeStopBeforeStart": false,
                "stoppedAfterShutdownBeforeStart": false
            },
            {
                "class": "PullMessageService",
                "joinTimeMillis": 90000,
                "initiallyDaemon": false,
                "initiallyStopped": false,
                "daemonAfterSet": true,
                "stoppedAfterWakeup": false,
                "stoppedAfterMakeStopBeforeStart": false,
                "stoppedAfterShutdownBeforeStart": false,
                "scheduledExecutorShutdown": true
            }
        ])
    );
}

#[test]
fn inherited_service_thread_oracle_records_java_start_stop_contract() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    assert_eq!(
        fixture["serviceLifecycleCases"],
        serde_json::json!([
            {
                "class": "RebalanceService",
                "runsAfterTwoStarts": 1,
                "stoppedAfterStart": false,
                "stoppedAfterMakeStop": true,
                "threadAliveAfterMakeStop": true,
                "stoppedAfterShutdown": true,
                "threadAliveAfterShutdown": false
            },
            {
                "class": "PullMessageService",
                "runsAfterTwoStarts": 1,
                "stoppedAfterStart": false,
                "stoppedAfterMakeStop": true,
                "threadAliveAfterMakeStop": true,
                "stoppedAfterShutdown": true,
                "threadAliveAfterShutdown": false,
                "scheduledExecutorShutdown": true
            }
        ])
    );
}

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
            group_name: Some("consumer".into()),
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
async fn registered_consumer_rejects_null_ordered_route_queue_list_like_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let case = &fixture["runtimeNullableCollections"][5];
    assert_eq!(case["registeredConsumer"], true);
    assert_eq!(case["outcome"], "NullPointerException");
    let body = serde_json::to_vec(&case["input"]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("null-ordered-consumer");
    config.namesrv_addr = Some(listener.local_addr().unwrap().to_string());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let consumer = Arc::new(RouteObserver {
        runtime: Arc::downgrade(&runtime),
        updates: AtomicUsize::new(0),
        needed: AtomicBool::new(false),
        need_checks: AtomicUsize::new(0),
    });
    runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut length = [0; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut bytes = BytesMut::from(&length[..]);
        bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let request = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(request.code, request::GET_ROUTEINFO_BY_TOPIC);
        let mut reply = RemotingCommand {
            code: 0,
            opaque: request.opaque,
            body: Some(body),
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
    });
    assert!(matches!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await,
        Err(RuntimeError::InvalidRoute("null queue data list"))
    ));
    assert!(runtime.route_snapshot().unwrap().raw_routes.is_empty());
    assert_eq!(consumer.updates.load(Ordering::SeqCst), 0);
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn registered_consumer_accepts_null_static_route_queue_list_like_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let case = &fixture["runtimeNullableCollections"][10];
    assert_eq!(case["registeredConsumer"], true);
    assert_eq!(case["outcome"], "OK");
    assert_eq!(case["changed"], true);
    assert_eq!(case["cached"], true);
    assert_eq!(case["cachedQueueNull"], false);
    let body = serde_json::to_vec(&case["input"]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("null-static-consumer");
    config.namesrv_addr = Some(listener.local_addr().unwrap().to_string());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let consumer = Arc::new(RouteObserver {
        runtime: Arc::downgrade(&runtime),
        updates: AtomicUsize::new(0),
        needed: AtomicBool::new(false),
        need_checks: AtomicUsize::new(0),
    });
    runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut length = [0; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut bytes = BytesMut::from(&length[..]);
        bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let request = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(request.code, request::GET_ROUTEINFO_BY_TOPIC);
        let mut reply = RemotingCommand {
            code: 0,
            opaque: request.opaque,
            body: Some(body),
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
    });
    assert!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    let snapshot = runtime.route_snapshot().unwrap();
    let route = snapshot.raw_routes["topic"].snapshot().unwrap();
    assert!(route.queue_datas.as_ref().unwrap().is_empty());
    assert_eq!(consumer.updates.load(Ordering::SeqCst), 1);
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
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
                subscription_data_set: Some(rocketmq_classic_protocol::SharedValue::from(
                    [Some("common"), Some("consumer-only"), None]
                        .into_iter()
                        .map(|topic| {
                            Some(rocketmq_classic_protocol::SharedValue::from(
                                SubscriptionData {
                                    topic: topic.map(str::to_owned),
                                    ..Default::default()
                                },
                            ))
                        })
                        .collect::<std::collections::HashSet<_>>(),
                )),
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
    assert_eq!(fixture["nullSubscriptionTopicRefresh"]["requests"], 2);
    assert_eq!(
        fixture["nullSubscriptionTopicRefresh"]["requestedNull"],
        true
    );
    assert_eq!(
        fixture["nullSubscriptionTopicRefresh"]["requestedNamed"],
        true
    );
    let body = serde_json::to_vec(&fixture["cases"][0]["input"]).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut seen = BTreeSet::new();
        for _ in 0..4 {
            let mut length = [0; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut bytes = BytesMut::from(&length[..]);
            bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            let command = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .unwrap();
            assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
            let topic = command.ext_fields.as_ref().unwrap().get("topic").cloned();
            assert!(seen.insert(topic.clone()), "duplicate route request");
            let mut reply = RemotingCommand {
                code: if topic.as_deref() == Some("common") {
                    987654
                } else {
                    0
                },
                opaque: command.opaque,
                body: Some(if topic.is_none() {
                    b"null".to_vec()
                } else {
                    body.clone()
                }),
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
                Some("common".into()),
                Some("consumer-only".into()),
                Some("producer-only".into()),
                None
            ])
        );
    });
    tokio::time::timeout(Duration::from_secs(5), runtime.update_all_topic_routes())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .expect("all route topics, including null, must be requested")
        .unwrap();
    let snapshot = runtime.route_snapshot().unwrap();
    assert!(!snapshot.topics.contains_key("common"));
    assert!(snapshot.topics.contains_key("consumer-only"));
    assert!(snapshot.topics.contains_key("producer-only"));
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn null_subscription_topic_success_route_fails_before_cache_publication() {
    use rocketmq_classic_protocol::heartbeat::{ConsumerData, SubscriptionData};
    struct NullTopic;
    impl ConsumerRuntime for NullTopic {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData {
                subscription_data_set: Some(rocketmq_classic_protocol::SharedValue::from(
                    std::collections::HashSet::from([Some(
                        rocketmq_classic_protocol::SharedValue::from(SubscriptionData::default()),
                    )]),
                )),
                ..Default::default()
            }
        }
    }
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    assert_eq!(
        fixture["nullSubscriptionTopicRefresh"]["successRouteOutcome"],
        "NullPointerException"
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("null-topic-success");
    config.namesrv_addr = Some(listener.local_addr().unwrap().to_string());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let owner = Arc::new(NullTopic);
    runtime
        .register_consumer("consumer", owner.clone())
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut length = [0; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut bytes = BytesMut::from(&length[..]);
        bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let command = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
        assert!(!command.ext_fields.as_ref().unwrap().contains_key("topic"));
        let mut reply = RemotingCommand {
            code: 0,
            opaque: command.opaque,
            body: Some(b"{}".to_vec()),
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
    });
    assert!(matches!(
        runtime.update_all_topic_routes().await,
        Err(RuntimeError::InvalidRoute("null topic route key"))
    ));
    peer.await.unwrap();
    assert!(runtime.route_snapshot().unwrap().raw_routes.is_empty());
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
async fn p6_v02_same_client_id_reuses_first_java_manager_runtime() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerSharing"];
    let manager = ClientRuntimeManager::new();
    let mut first_config = ClientConfig::new("198.51.100.223");
    first_config.instance_name = Some("phase6-manager-sharing".into());
    first_config.namesrv_addr = Some(String::new());
    let mut later_config = first_config.clone();
    later_config.namesrv_addr = Some("different-nameserver:9876".into());
    later_config.namespace_v2 = Some("different-namespace".into());
    later_config.use_tls = true;
    later_config.mq_client_api_timeout = 1234;

    let first_hook: Arc<dyn RpcHook> = Arc::new(IdentityHook);
    let later_hook: Arc<dyn RpcHook> = Arc::new(IdentityHook);
    let first = manager
        .get_or_create_with_hook(&first_config, Some(first_hook.clone()))
        .await
        .unwrap();
    let later = manager
        .get_or_create_with_hook(&later_config, Some(later_hook.clone()))
        .await
        .unwrap();
    assert_eq!(first.client_id(), later.client_id());
    assert_eq!(first.client_id() == later.client_id(), java["sameClientId"]);
    assert_eq!(Arc::ptr_eq(&first, &later), java["sameRuntimeIdentity"]);
    assert_eq!(
        first.config().read().unwrap().namesrv_addr.as_deref(),
        java["retainedNameserver"].as_str()
    );
    assert_eq!(
        first.config().read().unwrap().namespace_v2,
        java["retainedNamespaceV2"].as_str().map(str::to_owned)
    );
    assert_eq!(first.config().read().unwrap().use_tls, java["retainedTls"]);
    assert_eq!(
        first.config().read().unwrap().mq_client_api_timeout,
        java["retainedTimeout"]
    );
    assert_eq!(
        later_config.namesrv_addr.as_deref(),
        Some("different-nameserver:9876")
    );
    assert_eq!(
        later_config.namespace_v2.as_deref(),
        Some("different-namespace")
    );
    assert!(later_config.use_tls);
    assert_eq!(later_config.mq_client_api_timeout, 1234);
    let registered_hooks = first.remoting().get_rpc_hooks();
    assert_eq!(java["firstHookRetained"], true);
    assert_eq!(java["laterHookIgnored"], true);
    assert!(
        registered_hooks
            .iter()
            .any(|registered| Arc::ptr_eq(registered, &first_hook))
    );
    assert!(
        !registered_hooks
            .iter()
            .any(|registered| Arc::ptr_eq(registered, &later_hook))
    );
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
async fn json_null_route_response_returns_false_and_preserves_cached_snapshot() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("192.0.2.6");
    config.namesrv_addr = Some(listener.local_addr().unwrap().to_string());
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for body in [
            br#"{"queueDatas":[],"brokerDatas":[],"filterServerTable":{}}"#.to_vec(),
            b"null".to_vec(),
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
            let mut reply = RemotingCommand {
                code: 0,
                opaque: command.opaque,
                body: Some(body),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
                .await
                .unwrap();
        }
    });
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    assert!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    let before_null = runtime.route_snapshot().unwrap();
    assert!(before_null.topics.contains_key("topic"));
    assert!(
        !runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    assert!(Arc::ptr_eq(
        &before_null,
        &runtime.route_snapshot().unwrap()
    ));
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
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
async fn accepted_empty_route_keeps_route_presence_separate_from_publish_queue_ok() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let java = &fixture["topicPublishInfoPublication"];
    assert_eq!(java["producerInserted"], true);
    assert_eq!(java["acceptedRouteChanged"], true);
    assert_eq!(java["callbackDelivered"], true);
    assert_eq!(java["haveTopicRouterInfo"], true);
    assert_eq!(java["ok"], false);
    assert_eq!(java["publishQueueCount"], 0);
    assert_eq!(java["topicRouteDataPresent"], true);
    assert_eq!(java["missingRouteChanged"], false);
    assert_eq!(java["missingRouteCallbackDelivered"], false);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("192.0.2.6");
    config.namesrv_addr = Some(listener.local_addr().unwrap().to_string());
    let body = serde_json::to_vec(&java["input"]).unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for (topic, body) in [("topic", body), ("missing", b"null".to_vec())] {
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
                opaque: command.opaque,
                body: Some(body),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
                .await
                .unwrap();
        }
    });

    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let producer = Arc::new(RouteObserver {
        runtime: Arc::downgrade(&runtime),
        updates: AtomicUsize::new(0),
        needed: AtomicBool::new(false),
        need_checks: AtomicUsize::new(0),
    });
    runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    assert!(
        !runtime
            .route_snapshot()
            .unwrap()
            .topics
            .contains_key("topic")
    );
    runtime.remoting().start().unwrap();
    assert!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    assert!(
        !runtime
            .update_topic_route("missing", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );

    let snapshot = runtime.route_snapshot().unwrap();
    let topic = snapshot
        .topics
        .get("topic")
        .expect("published route snapshot");
    assert!(topic.publish_queues.is_empty());
    assert_eq!(topic.route.queue_datas.as_ref().unwrap().len(), 1);
    assert!(!snapshot.topics.contains_key("missing"));
    assert_eq!(producer.updates.load(Ordering::SeqCst), 1);
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
            .as_ref()
            .unwrap()
            .iter()
            .map(|data| {
                let data = data.as_ref().unwrap();
                (data.read_queue_nums, data.write_queue_nums)
            })
            .collect::<Vec<_>>(),
        [(4, 4), (2, 2)]
    );
    assert_eq!(info.publish_queues.len(), 6);
    assert_eq!(info.subscribe_queues.len(), 6);
    assert!(
        info.publish_queues
            .iter()
            .all(|queue| queue.topic() == Some("created"))
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
            .as_ref()
            .unwrap()
            .iter()
            .all(|data| {
                data.as_ref()
                    .is_some_and(|data| data.read_queue_nums == -2 && data.write_queue_nums == -2)
            })
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
