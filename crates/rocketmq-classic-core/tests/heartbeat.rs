use bytes::BytesMut;
use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntime, ClientRuntimeManager, ConsumerRuntime, ProducerRuntime},
};
use rocketmq_classic_protocol::{
    codes::request,
    heartbeat::{ConsumerData, HeartbeatData, SubscriptionData},
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};

struct Producer;
impl ProducerRuntime for Producer {}
struct Consumer(Mutex<SubscriptionData>);
impl ConsumerRuntime for Consumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData {
            group_name: "consumer".into(),
            subscription_data_set: HashSet::from([self.0.lock().unwrap().clone()]),
            ..Default::default()
        }
    }
}

async fn read(stream: &mut TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
        .unwrap()
        .unwrap()
}

async fn respond(
    stream: &mut TcpStream,
    command: &RemotingCommand,
    body: Option<Vec<u8>>,
    support: bool,
    sub_change: bool,
) {
    let mut reply = RemotingCommand {
        opaque: command.opaque,
        version: 551,
        body,
        ext_fields: Some(std::collections::BTreeMap::from([
            ("IS_SUPPORT_HEART_BEAT_V2".into(), support.to_string()),
            ("IS_SUB_CHANGE".into(), sub_change.to_string()),
        ])),
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
        .await
        .unwrap();
}

async fn runtime(mut config: ClientConfig, addresses: &[String]) -> Arc<ClientRuntime> {
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.namesrv_addr = Some(nameserver.local_addr().unwrap().to_string());
    let broker_addrs = addresses
        .iter()
        .enumerate()
        .map(|(id, address)| (id.to_string(), address))
        .collect::<HashMap<_, _>>();
    let route = serde_json::to_vec(&serde_json::json!({
        "queueDatas": [{"brokerName":"broker", "readQueueNums":1,"writeQueueNums":1,"perm":6,"topicSysFlag":0}],
        "brokerDatas": [{"cluster":"cluster", "brokerName":"broker", "brokerAddrs":broker_addrs}], "filterServerTable":{}
    })).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = nameserver.accept().await.unwrap();
        let command = read(&mut stream).await;
        assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
        respond(&mut stream, &command, Some(route), false, false).await;
    });
    let manager = ClientRuntimeManager::new();
    let runtime = manager.get_or_create(&config).await.unwrap();
    runtime.remoting().start().unwrap();
    runtime
        .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    server.await.unwrap();
    runtime
}

#[tokio::test]
async fn single_peer_sends_to_slave_for_producer_only_and_reports_wire_failure() {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        let command = read(&mut stream).await;
        assert_eq!(command.code, request::HEART_BEAT);
        let body: HeartbeatData = serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
        assert!(body.consumer_data_set.is_empty());
        respond(&mut stream, &command, None, false, false).await;
        let command = read(&mut stream).await;
        let mut response = RemotingCommand {
            code: 1,
            opaque: command.opaque,
            ..Default::default()
        };
        response.mark_response();
        stream
            .write_all(&frame::encode(&response, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
    });
    let mut config = ClientConfig::new("single-peer");
    config.enable_heartbeat_channel_event_listener = false;
    let runtime = runtime(config, std::slice::from_ref(&address)).await;
    assert!(
        !runtime
            .send_heartbeat_to_broker(1, "broker", &address)
            .await
            .unwrap()
    );
    let producer = Arc::new(Producer);
    let lease = runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    assert!(
        runtime
            .send_heartbeat_to_broker(1, "broker", &address)
            .await
            .unwrap()
    );
    assert!(
        !runtime
            .send_heartbeat_to_broker(1, "broker", &address)
            .await
            .unwrap()
    );
    assert_eq!(
        runtime
            .route_snapshot()
            .unwrap()
            .subscribe_address("broker", 0, true)
            .unwrap()
            .version,
        551
    );
    peer.await.unwrap();
    runtime
        .unregister_producer(&lease, Instant::now())
        .await
        .unwrap();
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn active_broker_connection_sends_heartbeat_and_wakes_runtime_wide_rebalance() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Rebalancer(AtomicUsize);
    impl ConsumerRuntime for Rebalancer {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData {
                group_name: "rebalance-owner".into(),
                ..Default::default()
            }
        }
        fn try_rebalance(
            &self,
        ) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>>>
        {
            Some(Box::pin(async {
                self.0.fetch_add(1, Ordering::SeqCst);
                true
            }))
        }
    }
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap().to_string();
    let heartbeats = Arc::new(AtomicUsize::new(0));
    let observed = heartbeats.clone();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        loop {
            let mut prefix = [0; 4];
            if stream.read_exact(&mut prefix).await.is_err() {
                break;
            }
            let mut bytes = BytesMut::from(&prefix[..]);
            bytes.resize(u32::from_be_bytes(prefix) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            let command = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .unwrap();
            if command.code == request::HEART_BEAT {
                observed.fetch_add(1, Ordering::SeqCst);
            }
            respond(&mut stream, &command, None, false, false).await;
        }
    });
    let runtime = runtime(
        ClientConfig::new("active-heartbeat"),
        std::slice::from_ref(&address),
    )
    .await;
    assert!(runtime.remoting().get_channel_event_listener().is_some());
    let first = Arc::new(Rebalancer(AtomicUsize::new(0)));
    let second = Arc::new(Rebalancer(AtomicUsize::new(0)));
    let a = runtime
        .register_consumer("first", first.clone())
        .await
        .unwrap();
    let b = runtime
        .register_consumer("second", second.clone())
        .await
        .unwrap();
    runtime.start().await.unwrap();
    runtime
        .remoting()
        .invoke_sync(
            &address,
            RemotingCommand {
                code: 9999,
                ..Default::default()
            },
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while first.0.load(Ordering::SeqCst) == 0 || second.0.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(heartbeats.load(Ordering::SeqCst) > 0);
    assert_eq!(
        runtime
            .route_snapshot()
            .unwrap()
            .subscribe_address("broker", 0, true)
            .unwrap()
            .version,
        551
    );
    runtime
        .unregister_consumer(&a, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    runtime
        .unregister_consumer(&b, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    runtime.shutdown().await.unwrap();
    peer.await.unwrap();
    let listener = runtime.remoting().get_channel_event_listener().unwrap();
    let weak = Arc::downgrade(&runtime);
    drop(runtime);
    assert!(weak.upgrade().is_none(), "listener retained its runtime");
    listener.on_active_async(&address).await;
}

#[tokio::test]
async fn disabled_or_unknown_channel_does_not_send_unsolicited_heartbeat() {
    for enabled in [false, true] {
        let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = broker.local_addr().unwrap().to_string();
        let mut config = ClientConfig::new(format!("no-active-heartbeat-{enabled}"));
        config.enable_heartbeat_channel_event_listener = enabled;
        let runtime = runtime(
            config,
            if enabled {
                &[]
            } else {
                std::slice::from_ref(&address)
            },
        )
        .await;
        let producer = Arc::new(Producer);
        let lease = runtime
            .register_producer("producer", producer.clone())
            .await
            .unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = broker.accept().await.unwrap();
            let command = read(&mut stream).await;
            assert_eq!(command.code, 9999);
            respond(&mut stream, &command, None, false, false).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(50), read(&mut stream))
                    .await
                    .is_err()
            );
        });
        runtime
            .remoting()
            .invoke_sync(
                &address,
                RemotingCommand {
                    code: 9999,
                    ..Default::default()
                },
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        peer.await.unwrap();
        assert_eq!(
            runtime.remoting().get_channel_event_listener().is_some(),
            enabled
        );
        runtime
            .unregister_producer(&lease, Instant::now())
            .await
            .unwrap();
        runtime.remoting().shutdown().await;
    }
}

#[tokio::test]
async fn v2_negotiates_full_simple_subscription_change_version_and_rebalance() {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        let mut fingerprints = Vec::new();
        // No V2 support -> full retry; support -> reduced; sub change -> full.
        for (index, (simple, support, changed)) in [
            (false, false, false),
            (false, true, false),
            (true, true, true),
            (false, true, false),
            (true, true, false),
            (false, true, false),
            (false, true, false),
        ]
        .into_iter()
        .enumerate()
        {
            let command = read(&mut stream).await;
            assert_eq!(command.code, request::HEART_BEAT);
            let body: HeartbeatData =
                serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
            assert_eq!(body.client_id.as_deref(), Some("heartbeat@DEFAULT"));
            assert_eq!(body.is_without_sub, simple, "round {index}");
            assert_eq!(body.producer_data_set.len(), 1);
            let consumer = body.consumer_data_set.iter().next().unwrap();
            assert_eq!(consumer.subscription_data_set.is_empty(), simple);
            if !simple {
                assert_eq!(
                    consumer
                        .subscription_data_set
                        .iter()
                        .next()
                        .unwrap()
                        .sub_version,
                    if index < 4 { 12 } else { 99 }
                );
            }
            fingerprints.push(body.heartbeat_fingerprint);
            respond(&mut stream, &command, None, support, changed).await;
        }
        assert!(
            fingerprints[..6]
                .iter()
                .all(|fingerprint| *fingerprint == fingerprints[0])
        );
        assert_ne!(fingerprints[6], fingerprints[0]);
    });
    let mut config = ClientConfig::new("heartbeat");
    config.use_heartbeat_v2 = true;
    let runtime = runtime(config, std::slice::from_ref(&address)).await;
    let producer = Arc::new(Producer);
    let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData {
        topic: "topic".into(),
        sub_string: "*".into(),
        sub_version: 12,
        ..Default::default()
    })));
    runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    for index in 0..4 {
        if index % 2 == 0 {
            assert!(
                runtime
                    .send_heartbeat_to_broker(0, "broker", &address)
                    .await
                    .unwrap()
            );
        } else {
            assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
        }
    }
    consumer.0.lock().unwrap().sub_version = 99;
    assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
    assert!(
        runtime
            .send_heartbeat_to_all_brokers_with_rebalance(true)
            .await
            .unwrap()
    );
    consumer.0.lock().unwrap().sub_string = "changed".into();
    assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
    assert_eq!(
        runtime
            .route_snapshot()
            .unwrap()
            .subscribe_address("broker", 0, true)
            .unwrap()
            .version,
        551
    );
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn v1_producer_only_skips_slave_and_empty_registry_skips_all() {
    let master = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slave = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addresses = [
        master.local_addr().unwrap().to_string(),
        slave.local_addr().unwrap().to_string(),
    ];
    let peer = tokio::spawn(async move {
        let (mut stream, _) = master.accept().await.unwrap();
        let command = read(&mut stream).await;
        let body: HeartbeatData = serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
        assert_eq!(command.code, request::HEART_BEAT);
        assert!(body.consumer_data_set.is_empty());
        assert_eq!(body.producer_data_set.len(), 1);
        assert_eq!(body.heartbeat_fingerprint, 0);
        assert!(!body.is_without_sub);
        respond(&mut stream, &command, None, false, false).await;
    });
    let runtime = runtime(ClientConfig::new("heartbeat"), &addresses).await;
    assert!(!runtime.send_heartbeat_to_all_brokers().await.unwrap());
    assert!(
        !runtime
            .send_heartbeat_to_broker_with_lock_mode(0, "broker", &addresses[0], false)
            .await
            .unwrap()
    );
    let producer = Arc::new(Producer);
    runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
    peer.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), slave.accept())
            .await
            .is_err()
    );
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn failed_peer_does_not_prevent_heartbeat_to_other_brokers() {
    let master = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slave = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addresses = [
        master.local_addr().unwrap().to_string(),
        slave.local_addr().unwrap().to_string(),
    ];
    let failed = tokio::spawn(async move {
        let (mut stream, _) = master.accept().await.unwrap();
        let command = read(&mut stream).await;
        let mut reply = RemotingCommand {
            code: 1,
            opaque: command.opaque,
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
    });
    let successful = tokio::spawn(async move {
        let (mut stream, _) = slave.accept().await.unwrap();
        let command = read(&mut stream).await;
        respond(&mut stream, &command, None, false, false).await;
    });
    let runtime = runtime(ClientConfig::new("heartbeat"), &addresses).await;
    let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData::default())));
    runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap()); // Java outer return is not an all-peers-success flag.
    failed.await.unwrap();
    successful.await.unwrap();
    let snapshot = runtime.route_snapshot().unwrap();
    assert_eq!(
        snapshot
            .subscribe_address("broker", 0, true)
            .unwrap()
            .version,
        0
    );
    assert_eq!(
        snapshot
            .subscribe_address("broker", 1, true)
            .unwrap()
            .version,
        551
    );
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn concurrent_v1_uses_parallel_peers_and_busy_heartbeat_gate_skips_a_round() {
    let master = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slave = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addresses = [
        master.local_addr().unwrap().to_string(),
        slave.local_addr().unwrap().to_string(),
    ];
    let (ready, received) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let (second, second_received) = tokio::sync::oneshot::channel();
    let first = tokio::spawn(async move {
        let (mut stream, _) = master.accept().await.unwrap();
        let command = read(&mut stream).await;
        ready.send(()).unwrap();
        second_received.await.unwrap(); // Sequential execution would deadlock here.
        released.await.unwrap();
        respond(&mut stream, &command, None, false, false).await;
    });
    let second_peer = tokio::spawn(async move {
        let (mut stream, _) = slave.accept().await.unwrap();
        let command = read(&mut stream).await;
        let body: HeartbeatData = serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
        assert_eq!(body.consumer_data_set.len(), 1);
        second.send(()).unwrap();
        respond(&mut stream, &command, None, false, false).await;
    });
    let mut config = ClientConfig::new("heartbeat");
    config.enable_concurrent_heartbeat = true;
    config.concurrent_heartbeat_thread_pool_size = 2;
    let runtime = runtime(config, &addresses).await;
    let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData::default())));
    runtime
        .register_consumer("consumer", consumer.clone())
        .await
        .unwrap();
    let sending = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.send_heartbeat_to_all_brokers().await })
    };
    tokio::time::timeout(Duration::from_secs(2), received)
        .await
        .unwrap()
        .unwrap();
    assert!(!runtime.send_heartbeat_to_all_brokers().await.unwrap());
    assert!(
        !runtime
            .send_heartbeat_to_broker_with_lock_mode(0, "broker", &addresses[0], false)
            .await
            .unwrap()
    );
    release.send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), sending)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    );
    first.await.unwrap();
    second_peer.await.unwrap();
    let snapshot = runtime.route_snapshot().unwrap();
    assert_eq!(
        snapshot
            .subscribe_address("broker", 0, true)
            .unwrap()
            .version,
        551
    );
    assert_eq!(
        snapshot
            .subscribe_address("broker", 1, true)
            .unwrap()
            .version,
        551
    );
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn rebalance_wrapper_uses_serial_v1_even_when_concurrent_heartbeat_is_enabled() {
    for is_rebalance in [false, true] {
        let master = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let slave = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addresses = [
            master.local_addr().unwrap().to_string(),
            slave.local_addr().unwrap().to_string(),
        ];
        let (ready, received) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let first = tokio::spawn(async move {
            let (mut stream, _) = master.accept().await.unwrap();
            let command = read(&mut stream).await;
            ready.send(()).unwrap();
            released.await.unwrap();
            respond(&mut stream, &command, None, false, false).await;
        });
        let mut config = ClientConfig::new(format!("serial-v1-wrapper-{is_rebalance}"));
        config.enable_concurrent_heartbeat = true;
        config.concurrent_heartbeat_thread_pool_size = 2;
        let runtime = runtime(config, &addresses).await;
        let consumer = Arc::new(Consumer(Mutex::new(SubscriptionData::default())));
        runtime
            .register_consumer("consumer", consumer.clone())
            .await
            .unwrap();
        let sending = {
            let runtime = runtime.clone();
            tokio::spawn(async move {
                runtime
                    .send_heartbeat_to_all_brokers_with_rebalance(is_rebalance)
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), received)
            .await
            .unwrap()
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), slave.accept())
                .await
                .is_err(),
            "V2 wrapper's V1 fallback must not execute concurrently"
        );
        release.send(()).unwrap();
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(2), slave.accept())
            .await
            .unwrap()
            .unwrap();
        let command = read(&mut stream).await;
        let body: HeartbeatData = serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
        assert_eq!(body.heartbeat_fingerprint, 0);
        assert!(!body.is_without_sub);
        respond(&mut stream, &command, None, false, false).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(2), sending)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        );
        first.await.unwrap();
        runtime.remoting().shutdown().await;
    }
}

#[tokio::test]
async fn preparation_failure_returns_false_without_socket_and_releases_heartbeat_gate() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Failing(AtomicBool);
    impl ConsumerRuntime for Failing {
        fn heartbeat_data(&self) -> ConsumerData {
            assert!(
                !self.0.load(Ordering::SeqCst),
                "consumer preparation failure"
            );
            ConsumerData {
                group_name: "consumer".into(),
                ..Default::default()
            }
        }
    }
    for v2 in [false, true] {
        let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = broker.local_addr().unwrap().to_string();
        let mut config = ClientConfig::new(format!("failed-preparation-{v2}"));
        config.use_heartbeat_v2 = v2;
        config.enable_heartbeat_channel_event_listener = false;
        let runtime = runtime(config, std::slice::from_ref(&address)).await;
        let consumer = Arc::new(Failing(AtomicBool::new(true)));
        runtime
            .register_consumer("consumer", consumer.clone())
            .await
            .unwrap();
        assert!(!runtime.send_heartbeat_to_all_brokers().await.unwrap());
        for rebalance in [false, true] {
            assert!(
                !runtime
                    .send_heartbeat_to_all_brokers_with_rebalance(rebalance)
                    .await
                    .unwrap()
            );
        }
        assert!(
            !runtime
                .send_heartbeat_to_broker(0, "broker", &address)
                .await
                .unwrap()
        );
        assert!(
            !runtime
                .send_heartbeat_to_broker_with_lock_mode(0, "broker", &address, false)
                .await
                .unwrap()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), broker.accept())
                .await
                .is_err()
        );
        consumer.0.store(false, Ordering::SeqCst);
        let peer = tokio::spawn(async move {
            let (mut stream, _) = broker.accept().await.unwrap();
            let command = read(&mut stream).await;
            let body: HeartbeatData =
                serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
            assert_eq!(body.consumer_data_set.len(), 1);
            assert!(!body.is_without_sub);
            respond(&mut stream, &command, None, v2, false).await;
        });
        assert!(runtime.send_heartbeat_to_all_brokers().await.unwrap());
        peer.await.unwrap();
        runtime.remoting().shutdown().await;
    }
}
