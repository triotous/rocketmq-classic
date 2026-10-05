use bytes::BytesMut;
use rocketmq_classic_protocol::codes::request;
use rocketmq_classic_remoting::{
    RemotingCommand,
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Duration, Instant},
};

use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, RuntimeError},
};

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
        for code in [0, 987654] {
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
    // Construction is separate from lifecycle; start is owned by the next step.
    runtime.remoting().start().unwrap();
    assert!(
        runtime
            .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
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
