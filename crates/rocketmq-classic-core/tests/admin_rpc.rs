use std::time::Duration;

use base64::Engine;
use bytes::BytesMut;
use rocketmq_classic_common::message::{MessageExt, decode_many};
use rocketmq_classic_core::{api::MQClientApi, error::RpcError};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::{
        CommandHeader, CreateTopicRequestHeader, HeaderMap, QueryMessageRequestHeader,
        QueryMessageResponseHeader, ViewMessageRequestHeader,
    },
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RemotingClient, TransportMode},
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::Instant,
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

async fn scripted_peer(
    code: i32,
    header: Option<HeaderMap>,
    body: Option<Vec<u8>>,
) -> (String, tokio::task::JoinHandle<RemotingCommand>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut reply = RemotingCommand {
            code,
            opaque: request.opaque,
            ext_fields: header,
            body,
            ..RemotingCommand::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
        request
    });
    (addr, task)
}

fn java_message_record(name: &str) -> Vec<u8> {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == name)
        .unwrap();
    base64::engine::general_purpose::STANDARD
        .decode(case["recordBase64"].as_str().unwrap())
        .unwrap()
}

async fn api() -> (MQClientApi, RemotingClient) {
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    (MQClientApi::new(client.clone()), client)
}

#[tokio::test]
async fn create_topic_sends_java_request_code_and_header_fields() {
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let header = CreateTopicRequestHeader {
        topic: "orders".into(),
        default_topic: "TBW102".into(),
        read_queue_nums: 4,
        write_queue_nums: 4,
        perm: 6,
        topic_filter_type: "SINGLE_TAG".into(),
        topic_sys_flag: Some(3),
        order: Some(false),
        attributes: Some("+unit=true".into()),
        force: Some(false),
        ..Default::default()
    };

    api.create_topic(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::UPDATE_AND_CREATE_TOPIC);
    let mut fields = HeaderMap::new();
    header.encode_fields(&mut fields);
    assert_eq!(command.ext_fields, Some(fields));
    client.shutdown().await;
}

#[tokio::test]
async fn query_message_sets_unique_flag_and_decodes_java_message_records() {
    let mut fields = HeaderMap::new();
    QueryMessageResponseHeader {
        index_last_update_timestamp: 1_700_000_200_041,
        index_last_update_phyoffset: 1_700_000_000_041,
    }
    .encode_fields(&mut fields);
    let message_record = java_message_record("ipv4-v1");
    let (addr, peer) = scripted_peer(
        response::SUCCESS,
        Some(fields),
        Some(message_record.clone()),
    )
    .await;
    let (api, client) = api().await;
    let header = QueryMessageRequestHeader {
        topic: "orders".into(),
        key: "producer-unique-1".into(),
        max_num: 32,
        begin_timestamp: 1_700_000_000_000,
        end_timestamp: 1_700_000_300_000,
        index_type: Some("UNIQUE_KEY".into()),
        ..Default::default()
    };

    let result = api
        .query_message(
            &addr,
            &header,
            true,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_MESSAGE);
    let mut expected_fields = HeaderMap::new();
    header.encode_fields(&mut expected_fields);
    expected_fields.insert("_UNIQUE_KEY_QUERY".into(), "true".into());
    assert_eq!(command.ext_fields, Some(expected_fields));
    assert_eq!(result.index_last_update_timestamp, 1_700_000_200_041);
    let messages: Vec<MessageExt> =
        decode_many(&message_record, Default::default(), Default::default()).unwrap();
    assert_eq!(result.message_list, messages);
    client.shutdown().await;
}

#[tokio::test]
async fn query_message_preserves_broker_response_code_and_remark() {
    let (addr, peer) = scripted_peer(17, None, None).await;
    let (api, client) = api().await;
    let error = api
        .query_message(
            &addr,
            &QueryMessageRequestHeader::default(),
            false,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_MESSAGE);
    assert!(matches!(error, RpcError::Response { code: 17, .. }));
    client.shutdown().await;
}

#[tokio::test]
async fn query_message_rejects_success_without_response_header() {
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(vec![])).await;
    let (api, client) = api().await;
    let error = api
        .query_message(
            &addr,
            &QueryMessageRequestHeader::default(),
            false,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    peer.await.unwrap();
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::QUERY_MESSAGE,
            ..
        }
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn view_message_sends_physical_offset_and_decodes_one_java_message() {
    let message_record = java_message_record("ipv4-v1");
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(message_record)).await;
    let (api, client) = api().await;
    let header = ViewMessageRequestHeader {
        topic: Some("orders".into()),
        offset: 1_700_000_000_041,
    };

    let message = api
        .view_message(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::VIEW_MESSAGE_BY_ID);
    let mut expected_fields = HeaderMap::new();
    header.encode_fields(&mut expected_fields);
    assert_eq!(command.ext_fields, Some(expected_fields));
    assert_eq!(message.message().topic(), "orders");
    assert_eq!(message.message().body(), b"hello-v1");
    client.shutdown().await;
}

#[tokio::test]
async fn view_message_rejects_malformed_success_body() {
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(vec![0, 0, 0, 1])).await;
    let (api, client) = api().await;
    let error = api
        .view_message(
            &addr,
            &ViewMessageRequestHeader::default(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    peer.await.unwrap();
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::VIEW_MESSAGE_BY_ID,
            ..
        }
    ));
    client.shutdown().await;
}
