use std::{collections::HashMap, time::Duration};

use base64::Engine;
use bytes::BytesMut;
use rocketmq_classic_common::message::{MessageExt, decode_many};
use rocketmq_classic_core::{
    api::{MQClientApi, admin::AdminQueryRequest},
    error::RpcError,
};
use rocketmq_classic_protocol::{
    SharedValue,
    codes::{request, response},
    header::{
        BoundaryType, CommandHeader, ConsumeMessageDirectlyResultRequestHeader,
        CreateTopicRequestHeader, DeleteKvConfigRequestHeader,
        DeleteSubscriptionGroupRequestHeader, DeleteTopicFromNamesrvRequestHeader,
        DeleteTopicRequestHeader, GetConsumeStatsRequestHeader,
        GetConsumerConnectionListRequestHeader, GetConsumerRunningInfoRequestHeader,
        GetEarliestMsgStoretimeRequestHeader, GetMaxOffsetRequestHeader, GetMinOffsetRequestHeader,
        GetTopicStatsInfoRequestHeader, HeaderMap, QueryConsumeTimeSpanRequestHeader,
        QueryMessageRequestHeader, QueryMessageResponseHeader,
        QuerySubscriptionByConsumerRequestHeader, QueryTopicConsumeByWhoRequestHeader,
        ResetOffsetRequestHeader, SearchOffsetRequestHeader, ViewMessageRequestHeader,
    },
    heartbeat::SubscriptionData,
    route::TopicRouteData,
    subscription::{
        ExponentialRetryPolicy, GroupRetryPolicy, GroupRetryPolicyType, SimpleSubscriptionData,
        SubscriptionDataSet, SubscriptionGroupConfig,
    },
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RemotingClient, TransportMode},
    codec::frame,
};
use serde_json::{Value, json};
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
    scripted_peer_after_with_remark(code, None, header, body, response_delay).await
}

async fn scripted_peer_with_remark(
    code: i32,
    remark: Option<String>,
    header: Option<HeaderMap>,
    body: Option<Vec<u8>>,
) -> (String, tokio::task::JoinHandle<RemotingCommand>) {
    scripted_peer_after_with_remark(code, remark, header, body, Duration::ZERO).await
}

async fn scripted_peer_after_with_remark(
    code: i32,
    remark: Option<String>,
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
            remark,
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

fn java_query_message_response_header(
    timestamp: i64,
    phyoffset: i64,
) -> QueryMessageResponseHeader {
    let mut header = QueryMessageResponseHeader::default();
    header.set_index_last_update_timestamp(Some(timestamp));
    header.set_index_last_update_phyoffset(Some(phyoffset));
    header
}

fn java_reset_offset_header() -> ResetOffsetRequestHeader {
    {
        let mut __phase18_header_0 = ResetOffsetRequestHeader::default();
        __phase18_header_0.set_group("workers".into());
        __phase18_header_0.set_topic("orders".into());
        __phase18_header_0.set_queue_id(1);
        __phase18_header_0.set_offset(Some(9));
        __phase18_header_0.set_timestamp(1_712_345_678_901);
        __phase18_header_0.set_force(true);
        __phase18_header_0.set_namespace(Some("tenant".into()));
        __phase18_header_0.set_namespaced(Some(true));
        __phase18_header_0.set_broker_name(Some("broker-a".into()));
        __phase18_header_0.set_oneway(Some(false));
        __phase18_header_0
    }
}

async fn invoke_offset_operation(
    api: &MQClientApi,
    addr: &str,
    code: i32,
) -> Result<i64, RpcError> {
    let deadline = Instant::now() + Duration::from_secs(2);
    match code {
        request::SEARCH_OFFSET_BY_TIMESTAMP => {
            let mut header = SearchOffsetRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(0));
            header.set_timestamp(Some(1_700_000_000_000));
            header.set_boundary_type(Some(BoundaryType::value_of_name(Some("LOWER")).unwrap()));
            api.search_offset(addr, &header, deadline).await
        }
        request::GET_MAX_OFFSET => {
            let mut header = GetMaxOffsetRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(0));
            api.get_max_offset(addr, &header, deadline).await
        }
        request::GET_MIN_OFFSET => {
            let mut header = GetMinOffsetRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(0));
            api.get_min_offset(addr, &header, deadline).await
        }
        request::GET_EARLIEST_MSG_STORETIME => {
            let mut header = GetEarliestMsgStoretimeRequestHeader::default();
            header.set_topic(Some("orders".into()));
            header.set_queue_id(Some(0));
            api.get_earliest_msg_storetime(addr, &header, deadline)
                .await
        }
        _ => panic!("unexpected admin offset request code {code}"),
    }
}

#[tokio::test]
async fn create_topic_sends_java_request_code_and_header_fields() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["updateOrCreateTopic"]["success"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let header = {
        let mut __phase18_header_1 = CreateTopicRequestHeader::default();
        __phase18_header_1.set_topic(Some("orders".into()));
        __phase18_header_1.set_default_topic(Some("TBW102".into()));
        __phase18_header_1.set_read_queue_nums(Some(4));
        __phase18_header_1.set_write_queue_nums(Some(4));
        __phase18_header_1.set_perm(Some(6));
        __phase18_header_1.set_topic_filter_type(Some("SINGLE_TAG".into()));
        __phase18_header_1.set_topic_sys_flag(Some(3));
        __phase18_header_1.set_order(Some(false));
        __phase18_header_1.set_attributes(Some("+unit=true".into()));
        __phase18_header_1.set_force(Some(false));
        __phase18_header_1.set_namespace(Some("tenant".into()));
        __phase18_header_1.set_namespaced(Some(true));
        __phase18_header_1.set_broker_name(Some("broker-a".into()));
        __phase18_header_1.set_oneway(Some(false));
        __phase18_header_1
    };

    api.create_topic(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(
        command.code,
        expected["requestCode"].as_i64().unwrap() as i32
    );
    assert!(command.body.is_none());
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = expected["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn create_topic_rejects_invalid_permission_before_network_io() {
    let (api, client) = api().await;
    let mut header = CreateTopicRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_perm(Some(8));
    let error = api
        .create_topic(
            "not-an-address",
            &header,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "CODE: 16  DESC: topicPermission value: 8 is invalid.\nFor more information, please visit the url, https://rocketmq.apache.org/docs/faq/"
    );
    assert!(matches!(
        &error,
        RpcError::ClientValidation {
            request_code: request::UPDATE_AND_CREATE_TOPIC,
            response_code: response::NO_PERMISSION,
            message,
        } if message == "topicPermission value: 8 is invalid."
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn update_broker_config_sends_java_properties_body() {
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let properties = HashMap::from([
        ("brokerPermission".to_owned(), "7".to_owned()),
        ("brokerRole".to_owned(), "ASYNC_MASTER".to_owned()),
    ]);
    api.update_broker_config(&addr, &properties, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::UPDATE_BROKER_CONFIG);
    assert_eq!(
        command.language,
        Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
    );
    assert!(command.ext_fields.is_none());
    let lines = String::from_utf8(command.body.unwrap())
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        lines,
        std::collections::HashSet::from([
            "brokerPermission=7".to_owned(),
            "brokerRole=ASYNC_MASTER".to_owned(),
        ])
    );
    client.shutdown().await;
}

#[tokio::test]
async fn get_broker_config_matches_java_properties_decoding_cases() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    assert_eq!(
        fixture["getBrokerConfigApi"][0],
        "java.util.Properties org.apache.rocketmq.client.impl.MQClientAPIImpl.getBrokerConfig(java.lang.String,long)"
    );
    for case in fixture["brokerConfigProperties"].as_array().unwrap() {
        let input = case["input"].as_str().unwrap();
        if input.is_empty() {
            // An empty body is omitted from the RocketMQ frame and Java's
            // getBrokerConfig dereferences null before calling string2Properties.
            continue;
        }
        let (addr, peer) =
            scripted_peer(response::SUCCESS, None, Some(input.as_bytes().to_vec())).await;
        let (client_api, client) = api().await;
        let properties = client_api
            .get_broker_config(&addr, Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(properties.is_none(), case["isNull"], "input {input:?}");
        let expected = case["entries"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
            .collect::<HashMap<_, _>>();
        if let Some(properties) = properties {
            assert_eq!(properties, expected, "input {input:?}");
        }
        let request = peer.await.unwrap();
        assert_eq!(request.code, request::GET_BROKER_CONFIG);
        assert!(request.ext_fields.is_none());
        assert!(request.body.is_none());
        client.shutdown().await;
    }
}

#[tokio::test]
async fn get_broker_config_rejects_missing_body_and_preserves_broker_error() {
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (client_api, client) = api().await;
    assert!(matches!(
        client_api
            .get_broker_config(&addr, Instant::now() + Duration::from_secs(2))
            .await,
        Err(RpcError::MalformedResponse {
            request_code: request::GET_BROKER_CONFIG,
            peer,
            reason,
        }) if peer == addr && reason == "successful broker-config reply has no body"
    ));
    assert_eq!(peer.await.unwrap().code, request::GET_BROKER_CONFIG);
    client.shutdown().await;

    let (addr, peer) = scripted_peer_with_remark(
        response::SYSTEM_ERROR,
        Some("broker config unavailable".to_owned()),
        None,
        None,
    )
    .await;
    let (client_api, client) = api().await;
    assert!(matches!(
        client_api
            .get_broker_config(&addr, Instant::now() + Duration::from_secs(2))
            .await,
        Err(RpcError::Response {
            request_code: request::GET_BROKER_CONFIG,
            peer,
            code: response::SYSTEM_ERROR,
            remark: Some(remark),
        }) if peer == addr && remark == "broker config unavailable"
    ));
    assert_eq!(peer.await.unwrap().code, request::GET_BROKER_CONFIG);
    client.shutdown().await;
}

#[tokio::test]
async fn update_broker_config_empty_properties_is_a_noop() {
    let (api, client) = api().await;
    assert!(
        api.update_broker_config(
            "not-an-address",
            &HashMap::new(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .is_ok()
    );
    client.shutdown().await;
}

#[tokio::test]
async fn update_broker_config_rejects_invalid_and_malformed_permissions_locally() {
    let (api, client) = api().await;
    let invalid = HashMap::from([("brokerPermission".to_owned(), "8".to_owned())]);
    let error = api
        .update_broker_config(
            "not-an-address",
            &invalid,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "CODE: 16  DESC: brokerPermission value: 8 is invalid.\nFor more information, please visit the url, https://rocketmq.apache.org/docs/faq/"
    );
    assert!(matches!(
        &error,
        RpcError::ClientValidation {
            request_code: request::UPDATE_BROKER_CONFIG,
            response_code: response::NO_PERMISSION,
            message,
        } if message == "brokerPermission value: 8 is invalid."
    ));

    let malformed = HashMap::from([("brokerPermission".to_owned(), " 7".to_owned())]);
    assert!(matches!(
        api.update_broker_config(
            "not-an-address",
            &malformed,
            Instant::now() + Duration::from_secs(2),
        )
        .await,
        Err(RpcError::PermissionParse(source)) if source.to_string() == "For input string: \" 7\""
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn update_broker_config_preserves_broker_error() {
    let (addr, peer) = scripted_peer_with_remark(
        response::NO_PERMISSION,
        Some("broker denied update".to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let properties = HashMap::from([("brokerRole".to_owned(), "ASYNC_MASTER".to_owned())]);
    assert!(matches!(
        api.update_broker_config(&addr, &properties, Instant::now() + Duration::from_secs(2))
            .await,
        Err(RpcError::Response {
            request_code: request::UPDATE_BROKER_CONFIG,
            peer,
            code: response::NO_PERMISSION,
            remark: Some(remark),
        }) if peer == addr && remark == "broker denied update"
    ));
    assert_eq!(peer.await.unwrap().code, request::UPDATE_BROKER_CONFIG);
    client.shutdown().await;
}

#[tokio::test]
async fn create_topic_preserves_broker_permission_error() {
    let (addr, peer) = scripted_peer(response::NO_PERMISSION, None, None).await;
    let (api, client) = api().await;
    let error = api
        .create_topic(
            &addr,
            &{
                let mut __phase18_header_2 = CreateTopicRequestHeader::default();
                __phase18_header_2.set_topic(Some("orders".into()));
                __phase18_header_2.set_default_topic(Some("TBW102".into()));
                __phase18_header_2
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
async fn create_topic_matches_mq_client_admin_error_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["updateOrCreateTopic"]["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        expected["responseCode"].as_i64().unwrap() as i32,
        Some(expected["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let error = api
        .create_topic(
            &addr,
            &{
                let mut __phase18_header_3 = CreateTopicRequestHeader::default();
                __phase18_header_3.set_topic(Some("orders".into()));
                __phase18_header_3.set_default_topic(Some("TBW102".into()));
                __phase18_header_3.set_read_queue_nums(Some(4));
                __phase18_header_3.set_write_queue_nums(Some(4));
                __phase18_header_3.set_perm(Some(6));
                __phase18_header_3.set_topic_filter_type(Some("SINGLE_TAG".into()));
                __phase18_header_3.set_topic_sys_flag(Some(3));
                __phase18_header_3.set_order(Some(false));
                __phase18_header_3.set_attributes(Some("+unit=true".into()));
                __phase18_header_3.set_force(Some(false));
                __phase18_header_3.set_namespace(Some("tenant".into()));
                __phase18_header_3.set_namespaced(Some(true));
                __phase18_header_3.set_broker_name(Some("broker-a".into()));
                __phase18_header_3.set_oneway(Some(false));
                __phase18_header_3
            },
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::UPDATE_AND_CREATE_TOPIC,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == expected["responseCode"].as_i64().unwrap() as i32
            && remark == expected["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(peer.await.unwrap().code, request::UPDATE_AND_CREATE_TOPIC);
    client.shutdown().await;
}

#[tokio::test]
async fn delete_topic_in_broker_matches_mq_client_admin_success_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteTopicInBroker"]["success"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let mut header = DeleteTopicRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));

    api.delete_topic_in_broker(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(
        command.code,
        expected["requestCode"].as_i64().unwrap() as i32
    );
    assert!(command.body.is_none());
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = expected["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn delete_topic_in_broker_preserves_java_broker_error() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteTopicInBroker"]["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        expected["responseCode"].as_i64().unwrap() as i32,
        Some(expected["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let mut header = DeleteTopicRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));
    let error = api
        .delete_topic_in_broker(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::DELETE_TOPIC_IN_BROKER,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == expected["responseCode"].as_i64().unwrap() as i32
            && remark == expected["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(peer.await.unwrap().code, request::DELETE_TOPIC_IN_BROKER);
    client.shutdown().await;
}

#[tokio::test]
async fn delete_topic_in_nameserver_matches_mq_client_admin_success_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteTopicInNameserver"]["success"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let mut header = DeleteTopicFromNamesrvRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_cluster_name(Some("cluster-a".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));

    api.delete_topic_in_nameserver(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(
        command.code,
        expected["requestCode"].as_i64().unwrap() as i32
    );
    assert!(command.body.is_none());
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = expected["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn delete_topic_in_nameserver_preserves_java_broker_error() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteTopicInNameserver"]["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        expected["responseCode"].as_i64().unwrap() as i32,
        Some(expected["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let mut header = DeleteTopicFromNamesrvRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_cluster_name(Some("cluster-a".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));
    let error = api
        .delete_topic_in_nameserver(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::DELETE_TOPIC_IN_NAMESRV,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == expected["responseCode"].as_i64().unwrap() as i32
            && remark == expected["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(peer.await.unwrap().code, request::DELETE_TOPIC_IN_NAMESRV);
    client.shutdown().await;
}

#[tokio::test]
async fn delete_kv_config_matches_mq_client_admin_success_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteKvConfig"]["success"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let mut header = DeleteKvConfigRequestHeader::default();
    header.set_namespace(Some("config-namespace".into()));
    header.set_key(Some("config-key".into()));

    api.delete_kv_config(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(
        command.code,
        expected["requestCode"].as_i64().unwrap() as i32
    );
    assert!(command.body.is_none());
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = expected["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn delete_kv_config_preserves_java_nameserver_error() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteKvConfig"]["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        expected["responseCode"].as_i64().unwrap() as i32,
        Some(expected["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let mut header = DeleteKvConfigRequestHeader::default();
    header.set_namespace(Some("config-namespace".into()));
    header.set_key(Some("config-key".into()));
    let error = api
        .delete_kv_config(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::DELETE_KV_CONFIG,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == expected["responseCode"].as_i64().unwrap() as i32
            && remark == expected["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(peer.await.unwrap().code, request::DELETE_KV_CONFIG);
    client.shutdown().await;
}

#[tokio::test]
async fn delete_subscription_group_matches_mq_client_admin_success_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteSubscriptionGroup"]["success"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let mut header = DeleteSubscriptionGroupRequestHeader::default();
    header.set_group_name(Some("workers".into()));
    header.set_clean_offset(true);
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));

    api.delete_subscription_group(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(
        command.code,
        expected["requestCode"].as_i64().unwrap() as i32
    );
    assert!(command.body.is_none());
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = expected["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    client.shutdown().await;
}

#[tokio::test]
async fn delete_subscription_group_preserves_java_broker_error() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["deleteSubscriptionGroup"]["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        expected["responseCode"].as_i64().unwrap() as i32,
        Some(expected["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let mut header = DeleteSubscriptionGroupRequestHeader::default();
    header.set_group_name(Some("workers".into()));
    header.set_clean_offset(true);
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    let error = api
        .delete_subscription_group(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::DELETE_SUBSCRIPTIONGROUP,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == expected["responseCode"].as_i64().unwrap() as i32
            && remark == expected["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(peer.await.unwrap().code, request::DELETE_SUBSCRIPTIONGROUP);
    client.shutdown().await;
}

#[tokio::test]
async fn invoke_broker_to_reset_offset_matches_mq_client_admin_success_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["invokeBrokerToResetOffset"]["success"];
    let body = expected["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (api, client) = api().await;

    let result = api
        .invoke_broker_to_reset_offset(
            &addr,
            &java_reset_offset_header(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap()
        .unwrap()
        .snapshot();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::INVOKE_BROKER_TO_RESET_OFFSET);
    assert!(command.body.is_none());
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = expected["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    assert_eq!(result.len(), expected["result"].as_array().unwrap().len());
    for (queue, offset) in result {
        let expected_entry = expected["result"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| {
                entry["topic"] == queue.topic().unwrap()
                    && entry["brokerName"] == queue.broker_name().unwrap()
                    && entry["queueId"] == queue.queue_id()
            })
            .unwrap();
        assert_eq!(offset, Some(expected_entry["offset"].as_i64().unwrap()));
    }
    client.shutdown().await;
}

#[tokio::test]
async fn invoke_broker_to_reset_offset_preserves_java_broker_error() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["invokeBrokerToResetOffset"]["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        expected["responseCode"].as_i64().unwrap() as i32,
        Some(expected["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (api, client) = api().await;
    let error = api
        .invoke_broker_to_reset_offset(
            &addr,
            &java_reset_offset_header(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::INVOKE_BROKER_TO_RESET_OFFSET,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == expected["responseCode"].as_i64().unwrap() as i32
            && remark == expected["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(
        peer.await.unwrap().code,
        request::INVOKE_BROKER_TO_RESET_OFFSET
    );
    client.shutdown().await;
}

#[tokio::test]
async fn invoke_broker_to_reset_offset_maps_success_without_body_like_java() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let expected = &fixture["invokeBrokerToResetOffset"]["successWithoutBody"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (api, client) = api().await;
    let error = api
        .invoke_broker_to_reset_offset(
            &addr,
            &java_reset_offset_header(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::INVOKE_BROKER_TO_RESET_OFFSET,
            peer,
            code,
            remark: None,
        } if peer == addr && code == expected["responseCode"].as_i64().unwrap() as i32
    ));
    assert_eq!(
        peer.await.unwrap().code,
        request::INVOKE_BROKER_TO_RESET_OFFSET
    );
    client.shutdown().await;
}

#[tokio::test]
async fn query_subscription_by_consumer_sends_java_header_and_decodes_subscription() {
    let body = serde_json::json!({
        "subscriptionData": {
            "classFilterMode": false,
            "topic": "orders",
            "subString": "tag-a",
            "subVersion": 123,
            "expressionType": "TAG",
            "tagsSet": [],
            "codeSet": []
        },
        "group": "workers",
        "topic": "orders"
    });
    let (addr, peer) =
        scripted_peer(response::SUCCESS, None, Some(body.to_string().into_bytes())).await;
    let (api, client) = api().await;
    let mut header = QuerySubscriptionByConsumerRequestHeader::default();
    header.set_group(Some("workers".into()));
    header.set_topic(Some("orders".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));

    let subscription = api
        .query_subscription_by_consumer(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .expect("the Java response contains SubscriptionData");

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_SUBSCRIPTION_BY_CONSUMER);
    let mut expected_fields = HeaderMap::new();
    header.encode_fields(&mut expected_fields);
    assert_eq!(command.ext_fields, Some(expected_fields.into()));
    assert_eq!(
        subscription.with(|data: &SubscriptionData| {
            (
                data.get_topic().map(str::to_owned),
                data.get_sub_string().map(str::to_owned),
                data.get_sub_version(),
            )
        }),
        (Some("orders".into()), Some("tag-a".into()), 123)
    );
    client.shutdown().await;
}

#[tokio::test]
async fn query_subscription_by_consumer_preserves_nullable_subscription_data() {
    let (addr, peer) = scripted_peer(
        response::SUCCESS,
        None,
        Some(br#"{"subscriptionData":null,"group":"workers","topic":"orders"}"#.to_vec()),
    )
    .await;
    let (api, client) = api().await;
    let mut header = QuerySubscriptionByConsumerRequestHeader::default();
    header.set_group(Some("workers".into()));
    header.set_topic(Some("orders".into()));

    let result = api
        .query_subscription_by_consumer(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    assert!(result.is_none());
    assert_eq!(
        peer.await.unwrap().code,
        request::QUERY_SUBSCRIPTION_BY_CONSUMER
    );
    client.shutdown().await;
}

#[tokio::test]
async fn query_subscription_by_consumer_preserves_broker_error() {
    let (addr, peer) = scripted_peer(
        response::SUBSCRIPTION_NOT_EXIST,
        None,
        Some(b"ignored on error".to_vec()),
    )
    .await;
    let (api, client) = api().await;
    let mut header = QuerySubscriptionByConsumerRequestHeader::default();
    header.set_group(Some("workers".into()));
    header.set_topic(Some("orders".into()));

    let error = api
        .query_subscription_by_consumer(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_SUBSCRIPTION_BY_CONSUMER,
            peer,
            code: response::SUBSCRIPTION_NOT_EXIST,
            remark: None,
        } if peer == addr
    ));
    assert_eq!(
        peer.await.unwrap().code,
        request::QUERY_SUBSCRIPTION_BY_CONSUMER
    );
    client.shutdown().await;
}

#[tokio::test]
async fn get_topic_stats_info_sends_java_header_and_decodes_topic_stats_table() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/topic-stats-table.json"
    ))
    .unwrap();
    let body = fixture["populatedJson"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (api, client) = api().await;
    let mut header = GetTopicStatsInfoRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));

    let stats = api
        .get_topic_stats_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_TOPIC_STATS_INFO);
    let mut expected_fields = HeaderMap::new();
    header.encode_fields(&mut expected_fields);
    assert_eq!(command.ext_fields, Some(expected_fields.into()));
    assert_eq!(stats.get_topic_put_tps(), 1.25);
    let table = stats.get_offset_table().unwrap();
    assert_eq!(table.with(|table| table.len()), 1);
    assert_eq!(
        table.with(|table| {
            table[&rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 1)]
                .as_ref()
                .unwrap()
                .with(|offset| offset.get_max_offset())
        }),
        16
    );
    client.shutdown().await;
}

#[tokio::test]
async fn get_topic_stats_info_preserves_java_null_body_results() {
    for body in [None, Some(b"null".to_vec())] {
        let (addr, peer) = scripted_peer(response::SUCCESS, None, body).await;
        let (api, client) = api().await;
        let mut header = GetTopicStatsInfoRequestHeader::default();
        header.set_topic(Some("orders".into()));
        let result = api
            .get_topic_stats_info(&addr, &header, Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert!(result.is_none());
        assert_eq!(peer.await.unwrap().code, request::GET_TOPIC_STATS_INFO);
        client.shutdown().await;
    }
}

#[tokio::test]
async fn get_topic_stats_info_preserves_broker_errors_and_rejects_malformed_body() {
    let (addr, peer) = scripted_peer(
        response::TOPIC_NOT_EXIST,
        None,
        Some(b"ignored on error".to_vec()),
    )
    .await;
    let (client_api, client) = api().await;
    let mut header = GetTopicStatsInfoRequestHeader::default();
    header.set_topic(Some("orders".into()));
    let error = client_api
        .get_topic_stats_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::GET_TOPIC_STATS_INFO,
            peer,
            code: response::TOPIC_NOT_EXIST,
            remark: None,
        } if peer == addr
    ));
    assert_eq!(peer.await.unwrap().code, request::GET_TOPIC_STATS_INFO);
    client.shutdown().await;

    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(b"not-json".to_vec())).await;
    let (api, client) = api().await;
    let mut header = GetTopicStatsInfoRequestHeader::default();
    header.set_topic(Some("orders".into()));
    let error = api
        .get_topic_stats_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::GET_TOPIC_STATS_INFO,
            peer,
            ..
        } if peer == addr
    ));
    assert_eq!(peer.await.unwrap().code, request::GET_TOPIC_STATS_INFO);
    client.shutdown().await;
}

#[tokio::test]
async fn query_consume_time_span_matches_mq_client_admin_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let success_case = &fixture["success"];
    let body = success_case["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (client_api, client) = api().await;
    let mut header = QueryConsumeTimeSpanRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_group(Some("workers".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    let spans = client_api
        .query_consume_time_span(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(
        command.code,
        success_case["requestCode"].as_i64().unwrap() as i32
    );
    let actual_fields = command.ext_fields.as_ref().unwrap();
    let expected_fields = success_case["requestExtFields"].as_object().unwrap();
    assert_eq!(actual_fields.len(), expected_fields.len());
    for (key, value) in expected_fields {
        assert_eq!(
            actual_fields.get(key).map(String::as_str),
            value.as_str(),
            "field {key}"
        );
    }
    assert_eq!(
        spans.len(),
        success_case["result"].as_array().unwrap().len()
    );
    let span = spans[0].as_ref().unwrap();
    let expected = &success_case["result"][0];
    let queue = span.get_message_queue().unwrap().snapshot();
    assert_eq!(queue.topic(), expected["topic"].as_str());
    assert_eq!(queue.broker_name(), expected["brokerName"].as_str());
    assert_eq!(
        queue.queue_id(),
        expected["queueId"].as_i64().unwrap() as i32
    );
    assert_eq!(
        span.get_min_time_stamp(),
        expected["minTimeStamp"].as_i64().unwrap()
    );
    assert_eq!(
        span.get_max_time_stamp(),
        expected["maxTimeStamp"].as_i64().unwrap()
    );
    assert_eq!(
        span.get_consume_time_stamp(),
        expected["consumeTimeStamp"].as_i64().unwrap()
    );
    assert_eq!(
        span.get_delay_time(),
        expected["delayTime"].as_i64().unwrap()
    );
    client.shutdown().await;

    let error_case = &fixture["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().to_owned()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .query_consume_time_span(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_CONSUME_TIME_SPAN,
            peer,
            code,
            remark: Some(remark),
        } if peer == addr
            && code == error_case["responseCode"].as_i64().unwrap() as i32
            && remark == error_case["responseRemark"].as_str().unwrap()
    ));
    assert_eq!(peer.await.unwrap().code, request::QUERY_CONSUME_TIME_SPAN);
    client.shutdown().await;
}

#[tokio::test]
async fn query_topic_consume_by_who_sends_java_header_and_decodes_group_list() {
    let (addr, peer) = scripted_peer(
        response::SUCCESS,
        None,
        Some(br#"{"groupList":["workers",null]}"#.to_vec()),
    )
    .await;
    let (api, client) = api().await;
    let mut header = QueryTopicConsumeByWhoRequestHeader::default();
    header.set_topic(Some("orders".into()));
    header.set_namespace(Some("tenant".into()));
    header.set_namespaced(Some(true));
    header.set_broker_name(Some("broker-a".into()));
    header.set_oneway(Some(false));
    header.set_lo(Some(true));

    let groups = api
        .query_topic_consume_by_who(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_TOPIC_CONSUME_BY_WHO);
    let mut expected_fields = HeaderMap::new();
    header.encode_fields(&mut expected_fields);
    assert_eq!(command.ext_fields, Some(expected_fields.into()));
    let group_list = groups.get_group_list().unwrap();
    assert!(group_list.with(|values| values.contains(&Some("workers".into()))));
    assert!(group_list.with(|values| values.contains(&None)));
    client.shutdown().await;
}

#[tokio::test]
async fn query_topic_consume_by_who_preserves_nullable_group_list() {
    let (addr, peer) = scripted_peer(
        response::SUCCESS,
        None,
        Some(br#"{"groupList":null}"#.to_vec()),
    )
    .await;
    let (api, client) = api().await;
    let mut header = QueryTopicConsumeByWhoRequestHeader::default();
    header.set_topic(Some("orders".into()));

    let result = api
        .query_topic_consume_by_who(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    assert!(result.get_group_list().is_none());
    assert_eq!(
        peer.await.unwrap().code,
        request::QUERY_TOPIC_CONSUME_BY_WHO
    );
    client.shutdown().await;
}

#[tokio::test]
async fn query_topic_consume_by_who_preserves_broker_error() {
    let (addr, peer) = scripted_peer(
        response::TOPIC_NOT_EXIST,
        None,
        Some(b"ignored on error".to_vec()),
    )
    .await;
    let (api, client) = api().await;
    let mut header = QueryTopicConsumeByWhoRequestHeader::default();
    header.set_topic(Some("orders".into()));

    let error = api
        .query_topic_consume_by_who(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RpcError::Response {
            request_code: request::QUERY_TOPIC_CONSUME_BY_WHO,
            peer,
            code: response::TOPIC_NOT_EXIST,
            remark: None,
        } if peer == addr
    ));
    assert_eq!(
        peer.await.unwrap().code,
        request::QUERY_TOPIC_CONSUME_BY_WHO
    );
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
    java_query_message_response_header(1_700_000_200_041, 1_700_000_000_041)
        .encode_fields(&mut fields);
    let message_record = java_message_record("ipv4-v1");
    let (addr, peer) = scripted_peer(
        response::SUCCESS,
        Some(fields),
        Some(message_record.clone()),
    )
    .await;
    let (api, client) = api().await;
    let header = {
        let mut __phase18_header_4 = QueryMessageRequestHeader::default();
        __phase18_header_4.set_topic(Some("orders".into()));
        __phase18_header_4.set_key(Some("producer-unique-1".into()));
        __phase18_header_4.set_max_num(Some(32));
        __phase18_header_4.set_begin_timestamp(Some(1_700_000_000_000));
        __phase18_header_4.set_end_timestamp(Some(1_700_000_300_000));
        __phase18_header_4.set_index_type(Some("UNIQUE_KEY".into()));
        __phase18_header_4
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
async fn mq_client_admin_query_message_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let query = &fixture["queryMessage"];
    let response_body = base64::engine::general_purpose::STANDARD
        .decode(query["responseBodyBase64"].as_str().unwrap())
        .unwrap();

    for case_name in ["keyQueryDecompressed", "keyQueryCompressed", "uniqueQuery"] {
        let case = &query[case_name];
        let unique_key = case["uniqueKeyFlag"].as_bool().unwrap();
        let decompress_body = case["decompressBody"].as_bool().unwrap();
        let header = {
            let mut __phase18_header_5 = QueryMessageRequestHeader::default();
            __phase18_header_5.set_topic(Some("orders".into()));
            __phase18_header_5.set_key(Some(if unique_key { "unique-1" } else { "green" }.into()));
            __phase18_header_5.set_max_num(Some(3));
            __phase18_header_5.set_begin_timestamp(Some(1_700_000_000_000));
            __phase18_header_5.set_end_timestamp(Some(1_700_000_300_000));
            __phase18_header_5.set_index_type(Some("KEY".into()));
            __phase18_header_5
        };
        let (addr, peer) =
            scripted_peer(response::SUCCESS, None, Some(response_body.clone())).await;
        let (api, client) = api().await;
        let messages = api
            .mq_client_admin()
            .query_message(
                &addr,
                unique_key,
                decompress_body,
                &header,
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();

        let command = peer.await.unwrap();
        assert_eq!(command.code, request::QUERY_MESSAGE);
        assert_eq!(
            serde_json::to_value(command.ext_fields).unwrap(),
            case["requestExtFields"]
        );
        let actual = messages
            .iter()
            .map(|message| {
                serde_json::json!({
                    "topic": message.topic(),
                    "messageId": message.message_id(),
                    "offsetMessageId": message.offset_message_id(),
                    "bodyBase64": base64::engine::general_purpose::STANDARD.encode(message.body().as_ref()),
                    "sysFlag": message.sys_flag(),
                    "keys": message.keys(),
                    "properties": serde_json::to_value(message.properties()).unwrap(),
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(Value::Array(actual), case["result"]);
        client.shutdown().await;
    }

    let header = {
        let mut __phase18_header_6 = QueryMessageRequestHeader::default();
        __phase18_header_6.set_topic(Some("orders".into()));
        __phase18_header_6.set_key(Some("green".into()));
        __phase18_header_6.set_max_num(Some(3));
        __phase18_header_6.set_begin_timestamp(Some(1_700_000_000_000));
        __phase18_header_6.set_end_timestamp(Some(1_700_000_300_000));
        __phase18_header_6.set_index_type(Some("KEY".into()));
        __phase18_header_6
    };
    let (not_found_address, not_found_peer) = scripted_peer_with_remark(
        response::QUERY_NOT_FOUND,
        Some("query not found".into()),
        None,
        None,
    )
    .await;
    let (not_found_api, client) = api().await;
    let messages = not_found_api
        .mq_client_admin()
        .query_message(
            &not_found_address,
            false,
            true,
            &header,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(messages.is_empty());
    let command = not_found_peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_MESSAGE);
    client.shutdown().await;

    let (error_address, error_peer) = scripted_peer_with_remark(
        response::SYSTEM_ERROR,
        Some("query failed".into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .query_message(
            &error_address,
            false,
            true,
            &header,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    let command = error_peer.await.unwrap();
    assert_eq!(command.code, request::QUERY_MESSAGE);
    assert!(
        matches!(error, RpcError::Response { code: 1, remark: Some(remark), .. } if remark == "query failed")
    );
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_view_message_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let view = &fixture["viewMessage"];
    let success = &view["success"];
    let response_body = base64::engine::general_purpose::STANDARD
        .decode(success["responseBodyBase64"].as_str().unwrap())
        .unwrap();
    let header = {
        let mut __phase18_header_7 = ViewMessageRequestHeader::default();
        __phase18_header_7.set_topic(Some("orders".into()));
        __phase18_header_7.set_offset(Some(4));
        __phase18_header_7
    };
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(response_body)).await;
    let (view_api, client) = api().await;
    let message = view_api
        .mq_client_admin()
        .view_message(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::VIEW_MESSAGE_BY_ID);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        success["requestExtFields"]
    );
    assert_eq!(
        serde_json::json!({
            "topic": message.topic(),
            "messageId": message.message_id(),
            "offsetMessageId": message.offset_message_id(),
            "bodyBase64": base64::engine::general_purpose::STANDARD.encode(message.body().as_ref()),
            "sysFlag": message.sys_flag(),
            "keys": message.keys(),
            "properties": serde_json::to_value(message.properties()).unwrap(),
        }),
        success["result"]
    );
    client.shutdown().await;

    let error_case = &view["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .view_message(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::VIEW_MESSAGE_BY_ID);
    assert!(matches!(
        error,
        RpcError::Response { code: 17, remark: Some(remark), .. }
            if remark == "view topic not found"
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_consume_message_directly_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let consume = &fixture["consumeMessageDirectly"];
    let success = &consume["success"];
    let header = {
        let mut __phase18_header_8 = ConsumeMessageDirectlyResultRequestHeader::default();
        __phase18_header_8.set_consumer_group("workers".into());
        __phase18_header_8.set_client_id(Some("client-a".into()));
        __phase18_header_8.set_msg_id(Some("message-1".into()));
        __phase18_header_8.set_broker_name(Some("broker-a".into()));
        __phase18_header_8.set_topic(Some("orders".into()));
        __phase18_header_8.set_topic_sys_flag(Some(3));
        __phase18_header_8.set_group_sys_flag(Some(5));
        __phase18_header_8.set_namespace(Some("tenant".into()));
        __phase18_header_8.set_namespaced(Some(true));
        __phase18_header_8.set_bname(Some("rpc-broker-a".into()));
        __phase18_header_8.set_oneway(Some(false));
        __phase18_header_8
    };
    let body = success["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (admin_api, client) = api().await;
    let result = admin_api
        .mq_client_admin()
        .consume_message_directly(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::CONSUME_MESSAGE_DIRECTLY);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        success["requestExtFields"]
    );
    assert_eq!(serde_json::to_value(result).unwrap(), success["result"]);
    client.shutdown().await;

    let error_case = &consume["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .consume_message_directly(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::CONSUME_MESSAGE_DIRECTLY);
    assert!(matches!(
        error,
        RpcError::Response { code: 1, remark: Some(remark), .. }
            if remark == "direct consume failed"
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn consume_message_directly_rejects_malformed_success_body() {
    let header = {
        let mut __phase18_header_9 = ConsumeMessageDirectlyResultRequestHeader::default();
        __phase18_header_9.set_consumer_group("workers".into());
        __phase18_header_9
    };
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(b"{".to_vec())).await;
    let (api, client) = api().await;
    let error = api
        .mq_client_admin()
        .consume_message_directly(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::CONSUME_MESSAGE_DIRECTLY);
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::CONSUME_MESSAGE_DIRECTLY,
            ..
        }
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_get_broker_cluster_info_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let cluster = &fixture["getBrokerClusterInfo"];
    let success = &cluster["success"];
    let body = success["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (admin_api, client) = api().await;
    let result = admin_api
        .mq_client_admin()
        .get_broker_cluster_info(&addr, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_BROKER_CLUSTER_INFO);
    assert!(command.ext_fields.is_none());
    assert_eq!(serde_json::to_value(result).unwrap(), success["result"]);
    client.shutdown().await;

    let no_body_case = &cluster["successWithoutBody"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (no_body_api, client) = api().await;
    let result = no_body_api
        .mq_client_admin()
        .get_broker_cluster_info(&addr, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_BROKER_CLUSTER_INFO);
    assert!(command.ext_fields.is_none());
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        no_body_case["result"]
    );
    client.shutdown().await;

    let error_case = &cluster["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .get_broker_cluster_info(&addr, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_BROKER_CLUSTER_INFO);
    assert!(matches!(
        error,
        RpcError::Response { code: 1, remark: Some(remark), .. }
            if remark == "cluster info unavailable"
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_get_consumer_connection_list_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let connection_list = &fixture["getConsumerConnectionList"];
    let success = &connection_list["success"];
    let header = {
        let mut __phase18_header_10 = GetConsumerConnectionListRequestHeader::default();
        __phase18_header_10.set_consumer_group(Some("workers".into()));
        __phase18_header_10.set_namespace(Some("tenant".into()));
        __phase18_header_10.set_namespaced(Some(true));
        __phase18_header_10.set_broker_name(Some("broker-a".into()));
        __phase18_header_10.set_oneway(Some(false));
        __phase18_header_10
    };
    let body = success["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (admin_api, client) = api().await;
    let result = admin_api
        .mq_client_admin()
        .get_consumer_connection_list(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_CONNECTION_LIST);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        success["requestExtFields"]
    );

    let connections = result
        .get_connection_set()
        .unwrap()
        .snapshot()
        .into_iter()
        .map(|connection| {
            connection.unwrap().with(|connection| {
                json!({
                    "clientId": connection.get_client_id(),
                    "clientAddr": connection.get_client_addr(),
                    "language": connection.get_language().map(|language| language.name()),
                    "version": connection.get_version(),
                })
            })
        })
        .collect::<Vec<_>>();

    let subscription_table = result.get_subscription_table().unwrap().snapshot();
    let subscription = subscription_table["orders"]
        .as_ref()
        .unwrap()
        .with(|subscription| {
            let mut tags = subscription
                .get_tags_set()
                .unwrap()
                .snapshot()
                .into_iter()
                .collect::<Vec<_>>();
            tags.sort();
            let mut codes = subscription
                .get_code_set()
                .unwrap()
                .snapshot()
                .into_iter()
                .collect::<Vec<_>>();
            codes.sort();
            json!({
                "classFilterMode": subscription.is_class_filter_mode(),
                "topic": subscription.get_topic(),
                "subString": subscription.get_sub_string(),
                "tagsSet": tags,
                "codeSet": codes,
                "subVersion": subscription.get_sub_version(),
                "expressionType": subscription.get_expression_type(),
            })
        });
    let actual = json!({
        "connectionSet": connections,
        "subscriptionTable": {"orders": subscription},
        "consumeType": result.get_consume_type().map(|value| value.name()),
        "messageModel": result.get_message_model().map(|value| value.name()),
        "consumeFromWhere": result.get_consume_from_where().map(|value| value.name()),
    });
    assert_eq!(actual, success["result"]);
    client.shutdown().await;

    let no_body_case = &connection_list["successWithoutBody"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (no_body_api, client) = api().await;
    let result = no_body_api
        .mq_client_admin()
        .get_consumer_connection_list(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_CONNECTION_LIST);
    assert_eq!(result.is_none(), no_body_case["result"].is_null());
    client.shutdown().await;

    let error_case = &connection_list["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .get_consumer_connection_list(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_CONNECTION_LIST);
    assert!(matches!(
        error,
        RpcError::Response { code: 1, remark: Some(remark), .. }
            if remark == "consumer connections unavailable"
    ));
    client.shutdown().await;

    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(b"{".to_vec())).await;
    let (malformed_api, client) = api().await;
    let error = malformed_api
        .mq_client_admin()
        .get_consumer_connection_list(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_CONNECTION_LIST);
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::GET_CONSUMER_CONNECTION_LIST,
            ..
        }
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_get_consume_stats_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let consume_stats = &fixture["getConsumeStats"];
    assert_eq!(
        consume_stats["method"],
        "public abstract java.util.concurrent.CompletableFuture<org.apache.rocketmq.remoting.protocol.admin.ConsumeStats> org.apache.rocketmq.client.MqClientAdmin.getConsumeStats(java.lang.String,org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader,long)"
    );
    let success = &consume_stats["success"];
    let header = {
        let mut __phase18_header_11 = GetConsumeStatsRequestHeader::default();
        __phase18_header_11.set_consumer_group(Some("workers".into()));
        __phase18_header_11.set_topic(Some("ignored-topic".into()));
        __phase18_header_11.set_topic_list(Some("orders;payments;".into()));
        __phase18_header_11.set_namespace(Some("tenant".into()));
        __phase18_header_11.set_namespaced(Some(true));
        __phase18_header_11.set_broker_name(Some("broker-a".into()));
        __phase18_header_11.set_oneway(Some(false));
        __phase18_header_11
    };
    let body = success["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (admin_api, client) = api().await;
    let result = admin_api
        .mq_client_admin()
        .get_consume_stats(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();

    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUME_STATS);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        success["requestExtFields"]
    );
    assert_eq!(result.get_consume_tps(), 1.25);
    assert_eq!(result.compute_total_diff().unwrap(), 13);
    assert_eq!(result.compute_inflight_total_diff().unwrap(), 8);
    let offsets = result.get_offset_table().unwrap().snapshot();
    assert_eq!(offsets.len(), 2);
    let orders = offsets
        [&rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 1)]
        .as_ref()
        .unwrap()
        .clone();
    assert_eq!(orders.with(|offset| offset.get_last_timestamp()), 1000);
    client.shutdown().await;

    let no_body_case = &consume_stats["successWithoutBody"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (no_body_api, client) = api().await;
    let result = no_body_api
        .mq_client_admin()
        .get_consume_stats(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUME_STATS);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        no_body_case["requestExtFields"]
    );
    assert!(result.is_none());
    client.shutdown().await;

    let error_case = &consume_stats["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .get_consume_stats(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUME_STATS);
    assert!(matches!(
        error,
        RpcError::Response { code: 1, remark: Some(remark), .. }
            if remark == "consume stats unavailable"
    ));
    client.shutdown().await;

    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(b"{".to_vec())).await;
    let (malformed_api, client) = api().await;
    let error = malformed_api
        .mq_client_admin()
        .get_consume_stats(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUME_STATS);
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::GET_CONSUME_STATS,
            ..
        }
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_get_consumer_running_info_matches_invoked_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let operation = &fixture["getConsumerRunningInfo"];
    assert_eq!(
        operation["method"],
        "public abstract java.util.concurrent.CompletableFuture<org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo> org.apache.rocketmq.client.MqClientAdmin.getConsumerRunningInfo(java.lang.String,org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader,long)"
    );
    let header = {
        let mut __phase18_header_12 = GetConsumerRunningInfoRequestHeader::default();
        __phase18_header_12.set_consumer_group("workers".into());
        __phase18_header_12.set_client_id("client-a".into());
        __phase18_header_12.set_jstack_enable(true);
        __phase18_header_12.set_namespace(Some("tenant".into()));
        __phase18_header_12.set_namespaced(Some(true));
        __phase18_header_12.set_broker_name(Some("broker-a".into()));
        __phase18_header_12.set_oneway(Some(false));
        __phase18_header_12
    };
    let success = &operation["success"];
    let body = success["responseBody"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(body)).await;
    let (admin_api, client) = api().await;
    let result = admin_api
        .mq_client_admin()
        .get_consumer_running_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_RUNNING_INFO);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        success["requestExtFields"]
    );
    let properties = result.get_properties().unwrap().snapshot();
    let subscription_set = result.get_subscription_set().unwrap().snapshot();
    let mq_table = result.get_mq_table().unwrap().snapshot();
    let mq_pop_table = result.get_mq_pop_table().unwrap().snapshot();
    let status_table = result.get_status_table().unwrap().snapshot();
    let user_consumer_info = result.get_user_consumer_info().unwrap().snapshot();
    assert_eq!(
        properties.get("PROP_CONSUME_TYPE").map(String::as_str),
        Some("CONSUME_PASSIVELY")
    );
    assert_eq!(subscription_set.len(), 1);
    assert_eq!(subscription_set[0].topic.as_deref(), Some("orders"));
    assert_eq!(mq_table.len(), 1);
    assert_eq!(mq_table[0].0.queue_id(), 1);
    assert_eq!(mq_table[0].1.get_commit_offset(), 17);
    assert_eq!(mq_pop_table[0].1.get_wait_ack_count(), 2);
    assert_eq!(status_table["orders"].get_consume_failed_msgs(), 6);
    assert_eq!(
        user_consumer_info.get("fixture").map(String::as_str),
        Some("value")
    );
    assert_eq!(result.get_jstack(), Some("jstack-fixture"));
    assert_eq!(
        success["result"]["subscriptionSetSize"].as_u64(),
        Some(subscription_set.len() as u64)
    );
    assert_eq!(success["result"]["mqTableSize"], mq_table.len());
    assert_eq!(success["result"]["mqPopTableSize"], mq_pop_table.len());
    client.shutdown().await;

    let no_body_case = &operation["successWithoutBody"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (no_body_api, client) = api().await;
    let result = no_body_api
        .mq_client_admin()
        .get_consumer_running_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_RUNNING_INFO);
    assert_eq!(
        serde_json::to_value(command.ext_fields).unwrap(),
        no_body_case["requestExtFields"]
    );
    assert!(result.is_none());
    assert_eq!(no_body_case["resultIsNull"], result.is_none());
    client.shutdown().await;

    let error_case = &operation["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .get_consumer_running_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_RUNNING_INFO);
    assert!(matches!(
        error,
        RpcError::Response { code: 1, remark: Some(remark), .. }
            if remark == "consumer running info unavailable"
    ));
    client.shutdown().await;

    let (addr, peer) = scripted_peer(response::SUCCESS, None, Some(b"{".to_vec())).await;
    let (malformed_api, client) = api().await;
    let error = malformed_api
        .mq_client_admin()
        .get_consumer_running_info(&addr, &header, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::GET_CONSUMER_RUNNING_INFO);
    assert!(matches!(
        error,
        RpcError::MalformedResponse {
            request_code: request::GET_CONSUMER_RUNNING_INFO,
            ..
        }
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn mq_client_admin_update_or_create_subscription_group_matches_java_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/admin/query-consume-time-span.json"
    ))
    .unwrap();
    let operation = &fixture["updateOrCreateSubscriptionGroup"];
    assert_eq!(
        operation["method"],
        "public abstract java.util.concurrent.CompletableFuture<java.lang.Void> org.apache.rocketmq.client.MqClientAdmin.updateOrCreateSubscriptionGroup(java.lang.String,org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig,long)"
    );

    let mut config = SubscriptionGroupConfig::default();
    config.set_group_name(Some("workers".into()));
    config.set_consume_enable(false);
    config.set_consume_from_min_enable(false);
    config.set_consume_broadcast_enable(false);
    config.set_consume_message_orderly(true);
    config.set_retry_queue_nums(4);
    config.set_retry_max_times(8);
    let mut retry = GroupRetryPolicy::default();
    retry.set_type(Some(GroupRetryPolicyType::Exponential));
    retry.set_exponential_retry_policy(Some(SharedValue::new(ExponentialRetryPolicy::new(
        1_000, 60_000, 3,
    ))));
    config.set_group_retry_policy(Some(SharedValue::new(retry)));
    config.set_broker_id(7);
    config.set_which_broker_when_consume_slowly(9);
    config.set_notify_consumer_ids_changed_enable(false);
    config.set_group_sys_flag(3);
    config.set_consume_timeout_minute(12);
    let subscriptions: SubscriptionDataSet = [
        Some(SharedValue::new(SimpleSubscriptionData::new(
            Some("orders".into()),
            Some("TAG".into()),
            Some("tag-a || tag-b".into()),
            10,
        ))),
        Some(SharedValue::new(SimpleSubscriptionData::new(
            Some("payments".into()),
            Some("SQL92".into()),
            Some("amount > 0".into()),
            20,
        ))),
    ]
    .into_iter()
    .collect();
    config.set_subscription_data_set(Some(SharedValue::new(subscriptions)));
    config.get_attributes().unwrap().with_mut(|attributes| {
        attributes.insert("tenant".into(), Some("north".into()));
        attributes.insert("stage".into(), Some("blue".into()));
    });

    let success = &operation["success"];
    let (addr, peer) = scripted_peer(response::SUCCESS, None, None).await;
    let (admin_api, client) = api().await;
    admin_api
        .mq_client_admin()
        .update_or_create_subscription_group(
            &addr,
            &config,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP);
    assert_eq!(command.ext_fields, None);
    assert_eq!(
        command.body.as_deref().unwrap(),
        success["requestBody"].as_str().unwrap().as_bytes()
    );
    client.shutdown().await;

    let error_case = &operation["brokerError"];
    let (addr, peer) = scripted_peer_with_remark(
        error_case["responseCode"].as_i64().unwrap() as i32,
        Some(error_case["responseRemark"].as_str().unwrap().into()),
        None,
        None,
    )
    .await;
    let (error_api, client) = api().await;
    let error = error_api
        .mq_client_admin()
        .update_or_create_subscription_group(
            &addr,
            &config,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    let command = peer.await.unwrap();
    assert_eq!(command.code, request::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP);
    assert_eq!(command.ext_fields, None);
    assert_eq!(
        command.body.as_deref().unwrap(),
        error_case["requestBody"].as_str().unwrap().as_bytes()
    );
    assert!(matches!(
        error,
        RpcError::Response { code: 1, remark: Some(remark), .. }
            if remark == "subscription group update failed"
    ));
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
async fn query_message_unboxes_nullable_java_timestamp_at_the_result_boundary() {
    let (api, client) = api().await;
    for timestamp in [None, Some("not-a-long")] {
        let mut fields = HeaderMap::from([("indexLastUpdatePhyoffset".into(), "7".into())]);
        if let Some(timestamp) = timestamp {
            fields.insert("indexLastUpdateTimestamp".into(), timestamp.into());
        }
        let (addr, peer) = scripted_peer(
            response::SUCCESS,
            Some(fields),
            Some(java_message_record("ipv4-v1")),
        )
        .await;
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
                reason,
                ..
            } if reason.contains("indexLastUpdateTimestamp")
        ));
    }
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
    java_query_message_response_header(100, 1).encode_fields(&mut first_header);
    let (first_address, first_peer) = scripted_peer(
        response::SUCCESS,
        Some(first_header),
        Some(message_record.clone()),
    )
    .await;

    let mut second_header = HeaderMap::new();
    java_query_message_response_header(200, 2).encode_fields(&mut second_header);
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
    java_query_message_response_header(200, 20).encode_fields(&mut first_header);
    let (first_address, first_peer) = scripted_peer_after(
        response::SUCCESS,
        Some(first_header),
        Some(first_record),
        Duration::from_millis(75),
    )
    .await;

    let mut second_header = HeaderMap::new();
    java_query_message_response_header(100, 10).encode_fields(&mut second_header);
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
    java_query_message_response_header(100, 1).encode_fields(&mut header);
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
    java_query_message_response_header(100, 1).encode_fields(&mut header);
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
    java_query_message_response_header(100, 1).encode_fields(&mut header);
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
    java_query_message_response_header(100, 1).encode_fields(&mut header);
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
    let header = {
        let mut __phase18_header_13 = ViewMessageRequestHeader::default();
        __phase18_header_13.set_topic(Some("orders".into()));
        __phase18_header_13.set_offset(Some(1_700_000_000_041));
        __phase18_header_13
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
