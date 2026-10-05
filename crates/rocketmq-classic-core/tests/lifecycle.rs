use bytes::BytesMut;
use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, ProducerRuntime, RuntimeError, RuntimeState},
};
use rocketmq_classic_protocol::codes::request;
use rocketmq_classic_remoting::{
    RemotingCommand,
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
};
use std::sync::Arc;
use tokio::time::{Duration, Instant};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

struct Producer;
impl ProducerRuntime for Producer {}

#[tokio::test]
async fn shared_shutdown_uses_registration_identity_not_runtime_arc_counts() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("192.0.2.6"))
        .await
        .unwrap();
    let producer = Arc::new(Producer);
    let first = runtime
        .register_producer("first", producer.clone())
        .await
        .unwrap();
    let second = runtime
        .register_producer("second", producer.clone())
        .await
        .unwrap();
    runtime.start().await.unwrap();
    runtime.start().await.unwrap();
    assert_eq!(runtime.state(), RuntimeState::Running);
    assert!(!runtime.shutdown().await.unwrap());
    let deadline = Instant::now() + Duration::from_secs(1);
    runtime.unregister_producer(&first, deadline).await.unwrap();
    assert!(!runtime.shutdown().await.unwrap());
    assert_eq!(runtime.state(), RuntimeState::Running);
    runtime
        .unregister_producer(&second, deadline)
        .await
        .unwrap();
    let extra_owner = runtime.clone();
    assert!(runtime.shutdown().await.unwrap());
    assert_eq!(extra_owner.state(), RuntimeState::Stopped);
    assert!(!runtime.shutdown().await.unwrap());
    assert!(matches!(
        runtime.start().await,
        Err(RuntimeError::InvalidState(RuntimeState::Stopped))
    ));
    let replacement = manager
        .get_or_create(&ClientConfig::new("192.0.2.6"))
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&runtime, &replacement));
}

#[tokio::test]
async fn many_concurrent_start_calls_observe_one_serialized_transition() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("192.0.2.6"))
        .await
        .unwrap();
    let mut calls = Vec::new();
    for _ in 0..64 {
        let runtime = runtime.clone();
        calls.push(tokio::spawn(async move {
            runtime.start().await.unwrap();
        }));
    }
    for call in calls {
        call.await.unwrap();
    }
    assert_eq!(runtime.state(), RuntimeState::Running);
    assert!(runtime.shutdown().await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_shutdown_callers_join_the_same_terminal_control_task() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("shutdown-race"))
        .await
        .unwrap();
    runtime.start().await.unwrap();
    let mut calls = Vec::new();
    for _ in 0..64 {
        let runtime = runtime.clone();
        calls.push(tokio::spawn(
            async move { runtime.shutdown().await.unwrap() },
        ));
    }
    let mut actual_stops = 0;
    for call in calls {
        actual_stops += usize::from(call.await.unwrap());
    }
    assert_eq!(actual_stops, 1);
    assert_eq!(runtime.state(), RuntimeState::Stopped);
}

async fn read_command(stream: &mut TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
        .unwrap()
        .unwrap()
}

async fn respond(stream: &mut TcpStream, command: &RemotingCommand, body: Option<Vec<u8>>) {
    let mut response = RemotingCommand {
        opaque: command.opaque,
        body,
        ..Default::default()
    };
    response.mark_response();
    stream
        .write_all(&frame::encode(&response, DEFAULT_MAX_FRAME_SIZE).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn cancelled_unregister_finishes_before_replacement_registration_and_last_shutdown_closes_after_unregister()
 {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker.local_addr().unwrap().to_string();
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("192.0.2.6");
    config.namesrv_addr = Some(nameserver.local_addr().unwrap().to_string());
    let (observed, started) = tokio::sync::oneshot::channel();
    let (release, reply_allowed) = tokio::sync::oneshot::channel();
    let (last_observed, last_started) = tokio::sync::oneshot::channel();
    let (last_release, last_reply_allowed) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        let command = read_command(&mut stream).await;
        assert_eq!(command.code, request::UNREGISTER_CLIENT);
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["producerGroup"],
            "producer"
        );
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["clientID"],
            "192.0.2.6@DEFAULT"
        );
        assert!(
            !command
                .ext_fields
                .as_ref()
                .unwrap()
                .contains_key("consumerGroup")
        );
        observed.send(()).unwrap();
        reply_allowed.await.unwrap();
        respond(&mut stream, &command, None).await;
        let command = read_command(&mut stream).await;
        assert_eq!(command.code, request::UNREGISTER_CLIENT);
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["producerGroup"],
            "producer"
        );
        respond(&mut stream, &command, None).await;
        let command = read_command(&mut stream).await;
        assert_eq!(command.code, request::UNREGISTER_CLIENT);
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["producerGroup"],
            "CLIENT_INNER_PRODUCER"
        );
        last_observed.send(()).unwrap();
        last_reply_allowed.await.unwrap();
        respond(&mut stream, &command, None).await;
        let mut extra = [0; 1];
        assert_eq!(stream.read(&mut extra).await.unwrap(), 0);
    });
    let name_peer = tokio::spawn(async move {
        let (mut stream, _) = nameserver.accept().await.unwrap();
        let command = read_command(&mut stream).await;
        assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
        let route = serde_json::json!({"queueDatas":[],"brokerDatas":[{"brokerName":"broker","cluster":"cluster","brokerAddrs":{"0":broker_address}}],"filterServerTable":{}});
        respond(
            &mut stream,
            &command,
            Some(serde_json::to_vec(&route).unwrap()),
        )
        .await;
    });
    let manager = ClientRuntimeManager::new();
    let runtime = manager.get_or_create(&config).await.unwrap();
    let producer = Arc::new(Producer);
    let old = runtime
        .register_producer("producer", producer.clone())
        .await
        .unwrap();
    runtime.start().await.unwrap();
    runtime
        .update_topic_route("topic", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    name_peer.await.unwrap();
    let cleanup_runtime = runtime.clone();
    let old_token = old.clone();
    let cleanup = tokio::spawn(async move {
        cleanup_runtime
            .unregister_producer(&old_token, Instant::now() + Duration::from_secs(5))
            .await
    });
    started.await.unwrap();
    cleanup.abort();
    assert!(matches!(cleanup.await, Err(error) if error.is_cancelled()));
    let registering_runtime = runtime.clone();
    let registering_producer = producer.clone();
    let mut registering = tokio::spawn(async move {
        registering_runtime
            .register_producer("producer", registering_producer)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut registering)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    let replacement = registering.await.unwrap().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    assert!(!runtime.unregister_producer(&old, deadline).await.unwrap());
    assert!(
        runtime
            .unregister_producer(&replacement, deadline)
            .await
            .unwrap()
    );
    let stopping_runtime = runtime.clone();
    let stopping = tokio::spawn(async move { stopping_runtime.shutdown().await });
    last_started.await.unwrap();
    stopping.abort();
    assert!(matches!(stopping.await, Err(error) if error.is_cancelled()));
    last_release.send(()).unwrap();
    assert!(!runtime.shutdown().await.unwrap());
    peer.await.unwrap();
    assert_eq!(runtime.state(), RuntimeState::Stopped);
}
