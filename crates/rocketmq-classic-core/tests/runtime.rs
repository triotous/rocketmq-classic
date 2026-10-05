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
    config.client_callback_executor_threads = -1;
    assert!(manager.get_or_create(&config).await.is_err());
    config.client_callback_executor_threads = 1;
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
    });
    let consumer = Arc::new(RouteObserver {
        runtime: Arc::downgrade(&runtime),
        updates: AtomicUsize::new(0),
        needed: AtomicBool::new(false),
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
    assert!(
        !runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    assert_eq!(producer.updates.load(Ordering::SeqCst), 1);
    consumer.needed.store(true, Ordering::SeqCst);
    assert!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    assert_eq!(producer.updates.load(Ordering::SeqCst), 2);
    assert_eq!(consumer.updates.load(Ordering::SeqCst), 2);
    let before = runtime.route_snapshot().unwrap();
    assert!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await
            .is_err()
    );
    assert!(Arc::ptr_eq(&before, &runtime.route_snapshot().unwrap()));
    assert_eq!(before.topics["topic"].publish_queues.len(), 2);
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
}
