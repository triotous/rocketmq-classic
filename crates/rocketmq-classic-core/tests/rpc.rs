use std::time::Duration;

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{
        MQClientApi,
        producer::{SendKind, SendMode},
    },
    error::RpcError,
};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::UpdateConsumerOffsetRequestHeader,
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RemotingClient, TransportMode},
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
    error::TransportError,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, timeout},
};

async fn read_command(stream: &mut tokio::net::TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
        .unwrap()
        .unwrap()
}

// A peer that consumes one request without responding, then watches for a retry.
async fn silent_peer() -> (String, tokio::task::JoinHandle<RemotingCommand>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let command = read_command(&mut stream).await;
        let mut extra = [0; 1];
        match timeout(Duration::from_millis(250), stream.read(&mut extra)).await {
            Ok(Ok(0)) | Err(_) => {}
            other => panic!("unexpected second attempt: {other:?}"),
        }
        command
    });
    (addr, task)
}

async fn route_reply(code: i32, body: Option<Vec<u8>>) -> (MQClientApi, RemotingClient) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut length = [0; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut bytes = BytesMut::from(&length[..]);
        let size = u32::from_be_bytes(length) as usize;
        bytes.resize(size + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let incoming = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
        assert_eq!(incoming.ext_fields.unwrap()["topic"], "test-topic");
        let mut reply = RemotingCommand {
            code,
            opaque: incoming.opaque,
            remark: Some("not found".into()),
            body,
            ..RemotingCommand::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.update_name_server_address_list(vec![address.to_string()]);
    client.start().unwrap();
    (MQClientApi::new(client.clone()), client)
}

#[tokio::test]
async fn route_success_unknown_status_and_malformed_success() {
    for (code, body) in [
        (response::SUCCESS, Some(br#"{"queueDatas":[],"brokerDatas":[],"filterServerTable":{},"topicQueueMappingByBroker":null}"#.to_vec())),
        (response::TOPIC_NOT_EXIST, None),
        (987654, None),
        (response::SUCCESS, None),
        (response::SUCCESS, Some(b"{".to_vec())),
    ] {
        let (api, client) = route_reply(code, body.clone()).await;
        let result = api
            .get_topic_route("test-topic", Instant::now() + Duration::from_secs(2))
            .await;
        match (code, body) {
            (response::SUCCESS, Some(body)) if body.starts_with(b"{\"queueDatas\"") => {
                assert!(result.unwrap().publish_queues("test-topic").is_empty());
            }
            (response::SUCCESS, _) => assert!(matches!(result, Err(RpcError::MalformedResponse { .. }))),
            (expected, _) => assert!(matches!(result, Err(RpcError::Response { code, peer, request_code: request::GET_ROUTEINFO_BY_TOPIC, .. }) if code == expected && peer == client.get_name_server_address_list().unwrap()[0])),
        }
        client.shutdown().await;
    }
}

#[tokio::test]
async fn route_fails_over_unavailable_nameserver_before_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let good = listener.local_addr().unwrap();
    let unavailable = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bad = unavailable.local_addr().unwrap();
    drop(unavailable);
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut len = [0; 4];
        stream.read_exact(&mut len).await.unwrap();
        let mut bytes = BytesMut::from(&len[..]);
        bytes.resize(u32::from_be_bytes(len) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let incoming = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        let mut reply = RemotingCommand {
            code: response::TOPIC_NOT_EXIST,
            opaque: incoming.opaque,
            ..RemotingCommand::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
    });
    let client = RemotingClient::new(TransportMode::Plain);
    // The selector starts at index 1; this forces the first connection to fail.
    client.update_name_server_address_list(vec![good.to_string(), bad.to_string()]);
    client.start().unwrap();
    let result = MQClientApi::new(client.clone())
        .get_topic_route("test-topic", Instant::now() + Duration::from_secs(2))
        .await;
    assert!(
        matches!(result, Err(RpcError::Response { peer, code: response::TOPIC_NOT_EXIST, .. }) if peer == good.to_string())
    );
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn route_without_nameserver_keeps_transport_error() {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let result = MQClientApi::new(client.clone())
        .get_topic_route("test-topic", Instant::now() + Duration::from_secs(1))
        .await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(TransportError::InvalidConfiguration(_)))
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn broker_send_modes_make_one_attempt_without_retry_on_timeout() {
    for mode in [SendMode::Sync, SendMode::Future] {
        let (addr, peer) = silent_peer().await;
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let result = MQClientApi::new(client.clone())
            .send_message(
                &addr,
                "broker",
                &Default::default(),
                b"payload".to_vec(),
                "unique",
                SendKind::V1,
                mode,
                Instant::now() + Duration::from_millis(100),
            )
            .await;
        assert!(matches!(result, Err(RpcError::Transport(_))), "{result:?}");
        let command = peer.await.unwrap();
        assert_eq!(command.code, request::SEND_MESSAGE);
        assert!(!command.is_oneway());
        client.shutdown().await;
    }
}

#[tokio::test]
async fn oneway_offset_returns_without_reply_and_expired_deadline_emits_nothing() {
    let (addr, peer) = silent_peer().await;
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    let api = MQClientApi::new(client.clone());
    let header = UpdateConsumerOffsetRequestHeader::default();
    api.update_consumer_offset(
        &addr,
        &header,
        true,
        Instant::now() + Duration::from_secs(2),
    )
    .await
    .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::UPDATE_CONSUMER_OFFSET);
    assert!(command.is_oneway());
    let result = api
        .update_consumer_offset(
            &addr,
            &header,
            true,
            Instant::now() - Duration::from_millis(1),
        )
        .await;
    assert!(matches!(
        result,
        Err(RpcError::Transport(TransportError::Timeout))
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn nameserver_deadline_does_not_fail_over_after_first_peer_stalls() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first = listener.local_addr().unwrap().to_string();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_addr = second.local_addr().unwrap().to_string();
    let stalled = [listener, second].map(|listener| {
        tokio::spawn(async move {
            let Ok(Ok((mut stream, _))) =
                timeout(Duration::from_millis(300), listener.accept()).await
            else {
                return 0;
            };
            let Ok(command) = timeout(Duration::from_millis(300), read_command(&mut stream)).await
            else {
                return 0;
            };
            assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
            tokio::time::sleep(Duration::from_millis(250)).await;
            1
        })
    });
    let client = RemotingClient::new(TransportMode::Plain);
    // Address order is shuffled, so both peers stall; only the selected one may see a request.
    client.update_name_server_address_list(vec![second_addr, first]);
    client.start().unwrap();
    let result = MQClientApi::new(client.clone())
        .get_topic_route("test-topic", Instant::now() + Duration::from_millis(100))
        .await;
    assert!(matches!(result, Err(RpcError::Transport(_))), "{result:?}");
    let mut attempts = 0;
    for peer in stalled {
        attempts += peer.await.unwrap();
    }
    assert_eq!(attempts, 1);
    client.shutdown().await;
}
