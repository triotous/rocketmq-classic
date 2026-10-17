use std::time::Duration;

use base64::Engine;
use bytes::BytesMut;
use rocketmq_classic_common::message::{MessageExt, decode_many};
use rocketmq_classic_core::{
    api::{MQClientApi, admin::AdminQueryRequest},
    error::RpcError,
};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::{
        BoundaryType, CommandHeader, CreateTopicRequestHeader,
        GetEarliestMsgStoretimeRequestHeader, GetMaxOffsetRequestHeader, GetMinOffsetRequestHeader,
        HeaderMap, QueryMessageRequestHeader, QueryMessageResponseHeader,
        SearchOffsetRequestHeader, ViewMessageRequestHeader,
    },
    route::TopicRouteData,
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RemotingClient, TransportMode},
    codec::frame,
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
    frame::decode_next(&mut bytes).unwrap().unwrap()
}

async fn scripted_peer(
    code: i32,
    header: Option<HeaderMap>,
    body: Option<Vec<u8>>,
) -> (String, tokio::task::JoinHandle<RemotingCommand>) {
    scripted_peer_after(code, header, body, Duration::ZERO).await
}

async fn scripted_peer_after(
    code: i32,
    header: Option<HeaderMap>,
    body: Option<Vec<u8>>,
    response_delay: Duration,
) -> (String, tokio::task::JoinHandle<RemotingCommand>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        tokio::time::sleep(response_delay).await;
        let mut reply = RemotingCommand {
            code,
            opaque: request.opaque,
            ext_fields: header.map(Into::into),
            body,
            ..RemotingCommand::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply).unwrap())
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

async fn invoke_offset_operation(
    api: &MQClientApi,
    addr: &str,
    code: i32,
) -> Result<i64, RpcError> {
    let deadline = Instant::now() + Duration::from_secs(2);
    match code {
        request::SEARCH_OFFSET_BY_TIMESTAMP => {
            api.search_offset(
                addr,
                &SearchOffsetRequestHeader {
                    topic: Some("orders".into()),
                    queue_id: Some(0),
                    timestamp: Some(1_700_000_000_000),
                    boundary_type: Some(BoundaryType::value_of_name(Some("LOWER")).unwrap()),
                    ..Default::default()
                },
                deadline,
            )
            .await
        }
        request::GET_MAX_OFFSET => {
            api.get_max_offset(
                addr,
                &GetMaxOffsetRequestHeader {
                    topic: Some("orders".into()),
                    queue_id: Some(0),
                    ..Default::default()
                },
                deadline,
            )
            .await
        }
        request::GET_MIN_OFFSET => {
            api.get_min_offset(
                addr,
                &GetMinOffsetRequestHeader {
                    topic: Some("orders".into()),
                    queue_id: Some(0),
                    ..Default::default()
                },
                deadline,
            )
            .await
        }
        request::GET_EARLIEST_MSG_STORETIME => {
            api.get_earliest_msg_storetime(
                addr,
                &GetEarliestMsgStoretimeRequestHeader {
                    topic: Some("orders".into()),
                    queue_id: Some(0),
                    ..Default::default()
                },
                deadline,
            )
            .await
        }
        _ => panic!("unexpected admin offset request code {code}"),
    }
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
    assert_eq!(command.ext_fields, Some(fields.into()));
    client.shutdown().await;
}

#[tokio::test]
async fn create_topic_preserves_broker_permission_error() {
    let (addr, peer) = scripted_peer(response::NO_PERMISSION, None, None).await;
    let (api, client) = api().await;
    let error = api
        .create_topic(
            &addr,
            &CreateTopicRequestHeader {
                topic: "orders".into(),
                default_topic: "TBW102".into(),
                ..Default::default()
            },
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::UPDATE_AND_CREATE_TOPIC,
            code: response::NO_PERMISSION,
            ..
        }
    ));
    assert_eq!(peer.await.unwrap().code, request::UPDATE_AND_CREATE_TOPIC);
    client.shutdown().await;
}

#[tokio::test]
async fn offset_admin_rpcs_preserve_broker_errors_and_reject_missing_response_fields() {
    for code in [
        request::SEARCH_OFFSET_BY_TIMESTAMP,
        request::GET_MAX_OFFSET,
        request::GET_MIN_OFFSET,
        request::GET_EARLIEST_MSG_STORETIME,
    ] {
        let (error_address, error_peer) = scripted_peer(response::NO_PERMISSION, None, None).await;
        let (api, client) = api().await;
        let error = invoke_offset_operation(&api, &error_address, code)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RpcError::Response {
                request_code,
                code: response::NO_PERMISSION,
                ..
            } if request_code == code
        ));
        assert_eq!(error_peer.await.unwrap().code, code);

        let (malformed_address, malformed_peer) =
            scripted_peer(response::SUCCESS, Some(HeaderMap::new()), None).await;
        let malformed = invoke_offset_operation(&api, &malformed_address, code)
            .await
            .unwrap_err();
        assert!(matches!(
            malformed,
            RpcError::MalformedResponse { request_code, .. } if request_code == code
        ));
        assert_eq!(malformed_peer.await.unwrap().code, code);
        client.shutdown().await;
    }
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
    assert_eq!(command.ext_fields, Some(expected_fields.into()));
    assert_eq!(result.get_index_last_update_timestamp(), 1_700_000_200_041);
    let mut messages: Vec<MessageExt> = decode_many(&message_record, Default::default()).unwrap();
    for message in &mut messages {
        let offset_id = message.message_id().map(str::to_owned);
        let client_id = message
            .properties()
            .get("UNIQ_KEY")
            .cloned()
            .or_else(|| offset_id.clone());
        message.set_offset_message_id(offset_id);
        message.set_message_id(client_id);
    }
    let returned_messages = result
        .get_message_list()
        .unwrap()
        .snapshot()
        .into_iter()
        .flatten()
        .map(|message| message.snapshot())
        .collect::<Vec<_>>();
    let returned_values = returned_messages
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected_values = messages
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(returned_values, expected_values);
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
async fn query_message_rejects_malformed_success_body() {
    let mut fields = HeaderMap::new();
    QueryMessageResponseHeader::default().encode_fields(&mut fields);
    let (addr, peer) = scripted_peer(response::SUCCESS, Some(fields), Some(vec![0, 0, 0, 1])).await;
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
async fn query_fanout_filters_unique_key_merges_watermarks_and_keeps_partial_successes() {
    let message_record = java_message_record("ipv4-v1");
    let decoded = decode_many(&message_record, Default::default()).unwrap();
    let offset_message_id = decoded[0].message_id().unwrap().to_owned();
    let unique_key = decoded[0].properties().get("UNIQ_KEY").unwrap().to_owned();
    assert_ne!(offset_message_id, unique_key);

    let mut first_header = HeaderMap::new();
    QueryMessageResponseHeader {
        index_last_update_timestamp: 100,
        index_last_update_phyoffset: 1,
    }
    .encode_fields(&mut first_header);
    let (first_address, first_peer) = scripted_peer(
        response::SUCCESS,
        Some(first_header),
        Some(message_record.clone()),
    )
    .await;

    let mut second_header = HeaderMap::new();
    QueryMessageResponseHeader {
        index_last_update_timestamp: 200,
        index_last_update_phyoffset: 2,
    }
    .encode_fields(&mut second_header);
    let (second_address, second_peer) = scripted_peer(
        response::SUCCESS,
        Some(second_header),
        Some(message_record.clone()),
    )
    .await;
    let (failed_address, failed_peer) = scripted_peer(response::NO_PERMISSION, None, None).await;

    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [
                {"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": first_address}},
                {"cluster": "cluster-a", "brokerName": "broker-b", "brokerAddrs": {"0": second_address}},
                {"cluster": "cluster-a", "brokerName": "broker-c", "brokerAddrs": {"0": failed_address}}
            ],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let query = AdminQueryRequest::by_unique_key("orders", unique_key.clone(), 32, 10, 99);
    let (api, client) = api().await;

    let result = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(result.get_index_last_update_timestamp(), 200);
    let messages = result.get_message_list().unwrap().snapshot();
    assert_eq!(messages.len(), 2);
    assert!(messages.iter().flatten().all(|message| {
        message.with(|message| {
            message.message_id() == Some(unique_key.as_str())
                && message.offset_message_id() == Some(offset_message_id.as_str())
        })
    }));
    for peer in [first_peer, second_peer] {
        let command = peer.await.unwrap();
        assert_eq!(command.code, request::QUERY_MESSAGE);
        let fields = command.ext_fields.unwrap();
        assert_eq!(fields.get("key"), Some(&unique_key));
        assert_eq!(fields.get("maxNum"), Some(&"32".to_owned()));
        assert_eq!(fields.get("beginTimestamp"), Some(&"10".to_owned()));
        assert_eq!(fields.get("endTimestamp"), Some(&"99".to_owned()));
        assert_eq!(fields.get("indexType"), Some(&"U".to_owned()));
        assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"true".to_owned()));
    }
    assert_eq!(failed_peer.await.unwrap().code, request::QUERY_MESSAGE);
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_preserves_java_response_completion_order_and_time_bounds() {
    let first_record = java_message_record("ipv4-v1");
    let mut second_record = first_record.clone();
    let body_marker = b"hello-v1";
    let body_offset = second_record
        .windows(body_marker.len())
        .position(|window| window == body_marker)
        .expect("the Java fixture contains a fixed-length message body");
    second_record[body_offset..body_offset + body_marker.len()].copy_from_slice(b"hello-v2");

    let mut first_header = HeaderMap::new();
    QueryMessageResponseHeader {
        index_last_update_timestamp: 200,
        index_last_update_phyoffset: 20,
    }
    .encode_fields(&mut first_header);
    let (first_address, first_peer) = scripted_peer_after(
        response::SUCCESS,
        Some(first_header),
        Some(first_record),
        Duration::from_millis(75),
    )
    .await;

    let mut second_header = HeaderMap::new();
    QueryMessageResponseHeader {
        index_last_update_timestamp: 100,
        index_last_update_phyoffset: 10,
    }
    .encode_fields(&mut second_header);
    let (second_address, second_peer) =
        scripted_peer(response::SUCCESS, Some(second_header), Some(second_record)).await;

    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [
                {"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": first_address}},
                {"cluster": "cluster-a", "brokerName": "broker-b", "brokerAddrs": {"0": second_address}}
            ],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let begin = 1_700_000_200_000;
    let end = 1_700_000_200_100;
    let query = AdminQueryRequest::by_unique_key("orders", "producer-unique-1", 32, begin, end);
    let (api, client) = api().await;

    let result = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(result.get_index_last_update_timestamp(), 200);
    let messages = result.get_message_list().unwrap().snapshot();
    assert_eq!(messages.len(), 2);
    assert!(
        messages[0]
            .as_ref()
            .unwrap()
            .with(|message| message.body().as_ref() == b"hello-v2")
    );
    assert!(
        messages[1]
            .as_ref()
            .unwrap()
            .with(|message| message.body().as_ref() == b"hello-v1")
    );
    for peer in [first_peer, second_peer] {
        let command = peer.await.unwrap();
        let fields = command.ext_fields.unwrap();
        assert_eq!(fields.get("beginTimestamp"), Some(&begin.to_string()));
        assert_eq!(fields.get("endTimestamp"), Some(&end.to_string()));
        assert_eq!(fields.get("maxNum"), Some(&"32".to_owned()));
    }
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_maps_only_terminal_broker_failures_to_java_no_message() {
    let (first_address, first_peer) = scripted_peer(response::NO_PERMISSION, None, None).await;
    let (second_address, second_peer) = scripted_peer(response::SYSTEM_ERROR, None, None).await;
    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [
                {"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": first_address}},
                {"cluster": "cluster-a", "brokerName": "broker-b", "brokerAddrs": {"0": second_address}}
            ],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let query = AdminQueryRequest::by_key("orders", "order-key", 32, 10, 99);
    let (api, client) = api().await;

    let error = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_MESSAGE,
            code: response::NO_MESSAGE,
            remark: Some(remark),
            ..
        } if remark == "query message by key finished, but no message."
    ));
    assert_eq!(first_peer.await.unwrap().code, request::QUERY_MESSAGE);
    assert_eq!(second_peer.await.unwrap().code, request::QUERY_MESSAGE);
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_returns_java_no_message_error_after_filtering() {
    let message_record = java_message_record("ipv4-v1");
    let mut header = HeaderMap::new();
    QueryMessageResponseHeader {
        index_last_update_timestamp: 100,
        index_last_update_phyoffset: 1,
    }
    .encode_fields(&mut header);
    let (address, peer) =
        scripted_peer(response::SUCCESS, Some(header), Some(message_record)).await;
    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [{"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": address}}],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let query = AdminQueryRequest::by_unique_key("orders", "not-the-message-id", 32, 10, 99);
    let (api, client) = api().await;

    let error = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_MESSAGE,
            code: response::NO_MESSAGE,
            remark: Some(remark),
            ..
        } if remark == "query message by key finished, but no message."
    ));
    assert_eq!(peer.await.unwrap().code, request::QUERY_MESSAGE);
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_matches_java_null_index_type_filter() {
    let message_record = java_message_record("ipv4-v1");
    let mut header = HeaderMap::new();
    QueryMessageResponseHeader {
        index_last_update_timestamp: 100,
        index_last_update_phyoffset: 1,
    }
    .encode_fields(&mut header);
    let (address, peer) =
        scripted_peer(response::SUCCESS, Some(header), Some(message_record)).await;
    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [{"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": address}}],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let query = AdminQueryRequest::by_key("orders", "order-key", 32, 10, 99)
        .with_index_type(None)
        .with_last_key(None);
    let (api, client) = api().await;

    let error = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_MESSAGE,
            code: response::NO_MESSAGE,
            remark: Some(remark),
            ..
        } if remark == "query message by key finished, but no message."
    ));
    let command = peer.await.unwrap();
    let fields = command.ext_fields.unwrap();
    assert!(!fields.contains_key("indexType"));
    assert!(!fields.contains_key("lastKey"));
    assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"false".to_owned()));
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_returns_java_no_message_error_for_empty_routes() {
    let route = TopicRouteData::java_decode(
        br#"{"queueDatas":[],"brokerDatas":[],"filterServerTable":{}}"#,
    )
    .unwrap();
    let query = AdminQueryRequest::by_key("orders", "order-key", 32, 10, 99);
    let (api, client) = api().await;

    let error = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_MESSAGE,
            code: response::NO_MESSAGE,
            remark: Some(remark),
            ..
        } if remark == "query message by key finished, but no message."
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_applies_java_cluster_filter_and_last_key() {
    let message_record = java_message_record("ipv4-v1");
    let mut header = HeaderMap::new();
    QueryMessageResponseHeader {
        index_last_update_timestamp: 100,
        index_last_update_phyoffset: 1,
    }
    .encode_fields(&mut header);
    let (selected_address, selected_peer) =
        scripted_peer(response::SUCCESS, Some(header), Some(message_record)).await;
    let (filtered_address, mut filtered_peer) = scripted_peer(response::SUCCESS, None, None).await;
    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [
                {"cluster": "cluster-a", "brokerName": "broker-a", "brokerAddrs": {"0": selected_address}},
                {"cluster": "cluster-b", "brokerName": "broker-b", "brokerAddrs": {"0": filtered_address}}
            ],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let query = AdminQueryRequest::by_unique_key("orders", "producer-unique-1", 32, 10, 99)
        .in_cluster("cluster-a")
        .with_last_key(Some("page-token".into()));
    let (api, client) = api().await;

    let result = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(result.get_message_list().unwrap().snapshot().len(), 1);
    let command = selected_peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_MESSAGE);
    let fields = command.ext_fields.unwrap();
    assert_eq!(fields.get("lastKey"), Some(&"page-token".to_owned()));
    assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"true".to_owned()));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut filtered_peer)
            .await
            .is_err()
    );
    filtered_peer.abort();
    client.shutdown().await;
}

#[tokio::test]
async fn query_fanout_uses_cluster_for_logical_topic_route_and_request() {
    let message_record = java_message_record("ipv4-v1");
    let mut header = HeaderMap::new();
    QueryMessageResponseHeader {
        index_last_update_timestamp: 100,
        index_last_update_phyoffset: 1,
    }
    .encode_fields(&mut header);
    let (address, peer) =
        scripted_peer(response::SUCCESS, Some(header), Some(message_record)).await;
    let route = TopicRouteData::java_decode(
        serde_json::json!({
            "queueDatas": [],
            "brokerDatas": [{"cluster": "other-cluster", "brokerName": "broker-a", "brokerAddrs": {"0": address}}],
            "filterServerTable": {}
        })
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let query = AdminQueryRequest::by_unique_key("%LMQ%child", "producer-unique-1", 32, 10, 99)
        .with_cluster_name(Some("parent".into()));
    let (api, client) = api().await;

    let result = api
        .query_message_across_brokers(&route, &query, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    assert_eq!(result.get_message_list().unwrap().snapshot().len(), 1);
    let command = peer.await.unwrap();
    let fields = command.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"parent".to_owned()));
    assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"true".to_owned()));
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
    assert_eq!(command.ext_fields, Some(expected_fields.into()));
    assert_eq!(message.topic(), "orders");
    assert_eq!(message.body().as_ref(), b"hello-v1");
    assert_eq!(
        message.message_id(),
        message.properties().get("UNIQ_KEY").map(String::as_str)
    );
    assert!(message.offset_message_id().is_some());
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

#[tokio::test]
async fn view_message_preserves_not_found_response() {
    let (addr, peer) = scripted_peer(response::NO_MESSAGE, None, None).await;
    let (api, client) = api().await;
    let error = api
        .view_message(
            &addr,
            &ViewMessageRequestHeader::default(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::VIEW_MESSAGE_BY_ID,
            code: response::NO_MESSAGE,
            ..
        }
    ));
    assert_eq!(peer.await.unwrap().code, request::VIEW_MESSAGE_BY_ID);
    client.shutdown().await;
}
