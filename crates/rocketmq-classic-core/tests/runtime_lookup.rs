use bytes::BytesMut;
use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntime, ClientRuntimeManager, RuntimeError},
};
use rocketmq_classic_protocol::{
    body::{MessageRequestMode, QueryAssignmentRequestBody},
    codes::request,
    heartbeat::MessageModel,
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};

async fn read(socket: &mut TcpStream) -> RemotingCommand {
    let mut length = [0; 4];
    socket.read_exact(&mut length).await.unwrap();
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    socket.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
        .unwrap()
        .unwrap()
}

async fn reply(
    socket: &mut TcpStream,
    command: &RemotingCommand,
    code: i32,
    body: Option<Vec<u8>>,
) {
    let mut response = RemotingCommand {
        opaque: command.opaque,
        code,
        body,
        ..Default::default()
    };
    response.mark_response();
    socket
        .write_all(&frame::encode(&response, DEFAULT_MAX_FRAME_SIZE).unwrap())
        .await
        .unwrap();
}

async fn setup(
    broker: Option<String>,
    vip: bool,
) -> (Arc<ClientRuntime>, tokio::task::JoinHandle<()>) {
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("lookup");
    config.client_ip = Some("192.0.2.6".into());
    config.namesrv_addr = Some(nameserver.local_addr().unwrap().to_string());
    config.vip_channel_enabled = vip;
    config.enable_heartbeat_channel_event_listener = false;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = nameserver.accept().await.unwrap();
        let command = read(&mut socket).await;
        assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
        assert_eq!(command.ext_fields.as_ref().unwrap()["topic"], "topic");
        let brokers = broker.map_or_else(Vec::new, |address| {
            vec![serde_json::json!({
                "cluster":"cluster","brokerName":"broker","brokerAddrs":{"0":address}
            })]
        });
        let route = serde_json::to_vec(
            &serde_json::json!({"queueDatas":[],"brokerDatas":brokers,"filterServerTable":{}}),
        )
        .unwrap();
        reply(&mut socket, &command, 0, Some(route)).await;
        let mut byte = [0];
        assert_eq!(
            socket.read(&mut byte).await.unwrap(),
            0,
            "unexpected extra route lookup"
        );
    });
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    (runtime, peer)
}

#[tokio::test]
async fn uncached_consumer_lookup_refreshes_once_and_assignment_reuses_raw_topic_broker() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
    let java_lookup = fixture["runtimeLookupInvocation"].clone();
    let java_consumer_call = java_lookup["consumerCalls"][0].clone();
    let java_assignment_call = java_lookup["assignmentCalls"][0].clone();
    let java_consumer_ids: Vec<Option<String>> =
        serde_json::from_value(java_lookup["consumerOutcome"].clone()).unwrap();
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (runtime, name_peer) = setup(Some(broker.local_addr().unwrap().to_string()), false).await;
    let peer_java_consumer_call = java_consumer_call.clone();
    let peer_java_assignment_call = java_assignment_call.clone();
    let peer_java_consumer_ids = java_consumer_ids.clone();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = broker.accept().await.unwrap();
        let command = read(&mut socket).await;
        assert_eq!(command.code, request::GET_CONSUMER_LIST_BY_GROUP);
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["consumerGroup"],
            peer_java_consumer_call["group"].as_str().unwrap()
        );
        reply(
            &mut socket,
            &command,
            0,
            Some(
                serde_json::to_vec(&serde_json::json!({
                    "consumerIdList": peer_java_consumer_ids
                }))
                .unwrap(),
            ),
        )
        .await;
        let command = read(&mut socket).await;
        assert_eq!(command.code, request::QUERY_ASSIGNMENT);
        let body: QueryAssignmentRequestBody =
            serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
        let java_body: QueryAssignmentRequestBody = serde_json::from_value(serde_json::json!({
            "topic": peer_java_assignment_call["topic"],
            "consumerGroup": peer_java_assignment_call["group"],
            "clientId": peer_java_assignment_call["clientId"],
            "strategyName": peer_java_assignment_call["strategy"],
            "messageModel": peer_java_assignment_call["messageModel"],
        }))
        .unwrap();
        assert_eq!(body, java_body);
        let assignment = serde_json::json!({"messageQueue":{"topic":"topic","brokerName":"broker","queueId":2},"mode":"POP","attachments":{"key":"value"}});
        reply(
            &mut socket,
            &command,
            0,
            Some(
                serde_json::to_vec(&serde_json::json!({
                    "messageQueueAssignments": [null, assignment.clone(), assignment]
                }))
                .unwrap(),
            ),
        )
        .await;
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });
    assert_eq!(runtime.find_broker_address_by_topic("topic").unwrap(), None);
    assert_eq!(
        runtime
            .find_consumer_id_list("topic", java_consumer_call["group"].as_str().unwrap())
            .await
            .unwrap(),
        Some(java_consumer_ids)
    );
    let assignments = runtime
        .query_assignment(
            java_assignment_call["topic"].as_str().unwrap(),
            java_assignment_call["group"].as_str().unwrap(),
            java_assignment_call["strategy"].as_str().unwrap(),
            MessageModel::Broadcasting,
            java_assignment_call["timeout"].as_i64().unwrap() as i32,
        )
        .await
        .unwrap()
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(
        assignments.len(),
        fixture["assignmentDuplicateCount"].as_u64().unwrap() as usize + 1
    );
    assert!(assignments.contains(&None));
    let assignment = assignments.iter().find_map(Option::as_ref).unwrap();
    assert_eq!(assignment.mode, Some(MessageRequestMode::Pop));
    assert_eq!(
        assignment
            .message_queue
            .as_ref()
            .unwrap()
            .with(|queue| queue.queue_id()),
        2
    );
    assert_eq!(
        assignment.attachments.as_ref().unwrap().snapshot()["key"],
        Some("value".into())
    );
    runtime.remoting().shutdown().await;
    peer.await.unwrap();
    name_peer.await.unwrap();
}

#[tokio::test]
async fn query_first_refreshes_and_both_lookup_rpcs_apply_vip_port() {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap();
    let (runtime, name_peer) =
        setup(Some(format!("127.0.0.1:{}", address.port() + 2)), false).await;
    runtime.config().write().unwrap().vip_channel_enabled = true;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = broker.accept().await.unwrap();
        let command = read(&mut socket).await;
        assert_eq!(command.code, request::QUERY_ASSIGNMENT);
        reply(
            &mut socket,
            &command,
            0,
            Some(br#"{"messageQueueAssignments":null}"#.to_vec()),
        )
        .await;
        let command = read(&mut socket).await;
        assert_eq!(command.code, request::GET_CONSUMER_LIST_BY_GROUP);
        reply(
            &mut socket,
            &command,
            0,
            Some(br#"{"consumerIdList":null}"#.to_vec()),
        )
        .await;
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });
    assert!(
        runtime
            .query_assignment(
                "topic",
                "consumer",
                "strategy",
                MessageModel::Clustering,
                3000
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        runtime
            .find_consumer_id_list("topic", "consumer")
            .await
            .unwrap()
            .is_none()
    );
    runtime.remoting().shutdown().await;
    peer.await.unwrap();
    name_peer.await.unwrap();
}

#[tokio::test]
async fn consumer_lookup_swallows_rpc_errors_but_assignment_propagates_them() {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (runtime, name_peer) = setup(Some(broker.local_addr().unwrap().to_string()), false).await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = broker.accept().await.unwrap();
        for (code, body) in [(1, None), (0, None), (0, Some(b"invalid".to_vec()))] {
            let command = read(&mut socket).await;
            assert_eq!(command.code, request::GET_CONSUMER_LIST_BY_GROUP);
            reply(&mut socket, &command, code, body).await;
        }
        for (code, body) in [(1, None), (0, Some(b"invalid".to_vec()))] {
            let command = read(&mut socket).await;
            assert_eq!(command.code, request::QUERY_ASSIGNMENT);
            reply(&mut socket, &command, code, body).await;
        }
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });
    for _ in 0..3 {
        assert!(
            runtime
                .find_consumer_id_list("topic", "consumer")
                .await
                .unwrap()
                .is_none()
        );
    }
    for _ in 0..2 {
        assert!(matches!(
            runtime
                .query_assignment(
                    "topic",
                    "consumer",
                    "strategy",
                    MessageModel::Clustering,
                    3000
                )
                .await,
            Err(RuntimeError::Rpc(_))
        ));
    }
    runtime.remoting().shutdown().await;
    peer.await.unwrap();
    name_peer.await.unwrap();
}

#[tokio::test]
async fn missing_broker_returns_none_and_expired_assignment_emits_no_frame() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
    let java_lookup = fixture["runtimeLookupInvocation"].clone();
    assert!(java_lookup["noBrokerConsumerOutcome"].is_null());
    assert!(java_lookup["noBrokerAssignmentOutcome"].is_null());
    assert_eq!(
        java_lookup["noBrokerRefreshTopics"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(
        java_lookup["noBrokerConsumerCalls"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        java_lookup["noBrokerAssignmentCalls"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let (runtime, name_peer) = setup(None, false).await;
    let java_consumer_result: Option<Vec<Option<String>>> =
        serde_json::from_value(java_lookup["noBrokerConsumerOutcome"].clone()).unwrap();
    assert_eq!(
        runtime
            .find_consumer_id_list("topic", "consumer")
            .await
            .unwrap(),
        java_consumer_result
    );
    runtime.remoting().shutdown().await;
    name_peer.await.unwrap();
    let (runtime, name_peer) = setup(None, false).await;
    let java_assignment_result = java_lookup["noBrokerAssignmentOutcome"].clone();
    assert!(
        runtime
            .query_assignment(
                "topic",
                "consumer",
                "strategy",
                MessageModel::Clustering,
                3000
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(java_assignment_result.is_null());
    runtime.remoting().shutdown().await;
    name_peer.await.unwrap();
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (runtime, name_peer) = setup(Some(broker.local_addr().unwrap().to_string()), false).await;
    runtime
        .update_topic_route("topic", Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    for timeout in [0, -1] {
        assert!(matches!(
            runtime
                .query_assignment(
                    "topic",
                    "consumer",
                    "strategy",
                    MessageModel::Clustering,
                    timeout
                )
                .await,
            Err(RuntimeError::Rpc(_))
        ));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(30), broker.accept())
            .await
            .is_err()
    );
    runtime.remoting().shutdown().await;
    name_peer.await.unwrap();
}

#[tokio::test]
async fn query_topic_route_refreshes_only_missing_cache_even_for_empty_brokers() {
    let (runtime, name_peer) = setup(None, false).await;
    assert!(runtime.get_existing_topic_route("topic").unwrap().is_none());
    let first = runtime.query_topic_route("topic").await.unwrap().unwrap();
    assert!(
        first
            .snapshot()
            .unwrap()
            .broker_datas_snapshot()
            .unwrap()
            .is_empty()
    );
    let cached = runtime.get_existing_topic_route("topic").unwrap().unwrap();
    assert!(first.ptr_eq(&cached));
    let queried = runtime.query_topic_route("topic").await.unwrap().unwrap();
    assert!(first.ptr_eq(&queried));
    runtime.remoting().shutdown().await;
    name_peer.await.unwrap();
}
