use bytes::BytesMut;
use rocketmq_classic_core::{
    config::ClientConfig,
    route::RouteSnapshot,
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
async fn null_topic_broker_selection_matches_invoked_java_failure() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    assert_eq!(
        fixture["runtimeNullTopicBrokerSelection"]["outcome"],
        "NullPointerException"
    );
    let mut config = ClientConfig::new("null-broker-topic");
    config.namesrv_addr = Some(String::new());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    assert!(matches!(
        runtime.find_broker_address_by_topic_nullable(None),
        Err(RuntimeError::InvalidRoute("null broker lookup topic"))
    ));
}

#[tokio::test]
async fn null_topic_route_lookups_match_invoked_java_control_flow() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let java = &fixture["runtimeNullTopicRouteLookups"];
    assert_eq!(java["existing"]["outcome"], "NullPointerException");
    assert_eq!(java["query"]["outcome"], "NullPointerException");
    assert_eq!(java["updateMissing"]["outcome"], "OK");
    assert_eq!(java["updateMissing"]["changed"], false);
    assert_eq!(java["updatePresent"]["outcome"], "NullPointerException");
    assert_eq!(java["requestsAfterMissing"], 1);
    assert_eq!(java["requestsAfterPresent"], 2);
    assert!(java["requestedNullTopic"].as_bool().unwrap());

    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new("null-topic-route-lookups");
    config.namesrv_addr = Some(nameserver.local_addr().unwrap().to_string());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.remoting().start().unwrap();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = nameserver.accept().await.unwrap();
        for body in [Some(b"null".to_vec()), Some(b"{}".to_vec())] {
            let command = read(&mut socket).await;
            assert_eq!(command.code, request::GET_ROUTEINFO_BY_TOPIC);
            assert!(
                !command
                    .ext_fields
                    .as_ref()
                    .is_some_and(|fields| fields.contains_key("topic"))
            );
            reply(&mut socket, &command, 0, body).await;
        }
    });

    assert!(matches!(
        runtime.get_existing_topic_route_nullable(None),
        Err(RuntimeError::InvalidRoute("null route lookup topic"))
    ));
    assert!(matches!(
        runtime.query_topic_route_nullable(None).await,
        Err(RuntimeError::InvalidRoute("null route lookup topic"))
    ));
    let deadline = Instant::now() + Duration::from_secs(2);
    assert!(
        !runtime
            .update_topic_route_nullable(None, deadline)
            .await
            .unwrap()
    );
    assert!(matches!(
        runtime
            .update_topic_route_nullable(None, Instant::now() + Duration::from_secs(2))
            .await,
        Err(RuntimeError::InvalidRoute("null topic route key"))
    ));
    assert!(runtime.route_snapshot().unwrap().raw_routes.is_empty());
    peer.await.unwrap();
    runtime.remoting().shutdown().await;
}

#[tokio::test]
async fn null_topic_consumer_and_assignment_lookups_match_invoked_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
    let java = &fixture["runtimeNullBrokerLookupRpcTopics"];
    assert_eq!(java["consumerList"]["outcome"], "NullPointerException");
    assert_eq!(java["assignment"]["outcome"], "NullPointerException");

    let mut config = ClientConfig::new("null-broker-lookup-rpc-topics");
    config.namesrv_addr = Some(String::new());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    assert!(matches!(
        runtime
            .find_consumer_id_list_nullable(None, Some("consumer"))
            .await,
        Err(RuntimeError::InvalidRoute("null broker lookup topic"))
    ));
    assert!(matches!(
        runtime
            .query_assignment_nullable(
                None,
                Some("consumer"),
                Some("strategy"),
                Some(MessageModel::Broadcasting),
                3000
            )
            .await,
        Err(RuntimeError::InvalidRoute("null broker lookup topic"))
    ));
}

#[tokio::test]
async fn null_consumer_group_matches_java_rpc_and_omits_header_field() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
    let java_lookup = &fixture["runtimeLookupInvocation"];
    let api_case = &fixture["consumerListNullGroupApiCase"];
    assert_eq!(
        java_lookup["nullGroupConsumerOutcome"],
        serde_json::json!(["b", "a", "b"])
    );
    assert_eq!(
        java_lookup["nullGroupConsumerCalls"][0]["group"],
        serde_json::Value::Null
    );
    assert_eq!(api_case["requestCode"], request::GET_CONSUMER_LIST_BY_GROUP);
    assert_eq!(api_case["consumerGroupFieldPresent"], false);
    assert_eq!(api_case["consumerIds"], serde_json::json!(["client"]));

    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker.local_addr().unwrap().to_string();
    let (runtime, name_peer) = setup(Some(broker_address), false).await;
    let broker_peer = tokio::spawn(async move {
        let (mut socket, _) = broker.accept().await.unwrap();
        let command = read(&mut socket).await;
        assert_eq!(command.code, request::GET_CONSUMER_LIST_BY_GROUP);
        assert!(
            !command
                .ext_fields
                .as_ref()
                .is_some_and(|fields| fields.contains_key("consumerGroup"))
        );
        reply(
            &mut socket,
            &command,
            0,
            Some(br#"{"consumerIdList":["client"]}"#.to_vec()),
        )
        .await;
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });

    assert_eq!(
        runtime
            .find_consumer_id_list_nullable(Some("topic"), None)
            .await
            .unwrap(),
        Some(vec![Some("client".into())])
    );
    runtime.remoting().shutdown().await;
    broker_peer.await.unwrap();
    name_peer.await.unwrap();
}

#[tokio::test]
async fn null_assignment_fields_match_java_request_body() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
    let java_runtime = &fixture["runtimeLookupInvocation"]["nullAssignmentFieldsCalls"][0];
    assert_eq!(java_runtime["topic"], "topic");
    assert_eq!(java_runtime["group"], serde_json::Value::Null);
    assert_eq!(java_runtime["strategy"], serde_json::Value::Null);
    assert_eq!(java_runtime["messageModel"], serde_json::Value::Null);
    let java_api = &fixture["queryAssignmentNullInputsApiCase"];
    assert_eq!(java_api["requestCode"], request::QUERY_ASSIGNMENT);
    assert_eq!(java_api["assignmentCount"], 0);
    assert_eq!(java_api["body"]["topic"], "topic");
    assert_eq!(java_api["body"]["clientId"], "client");
    for key in ["consumerGroup", "strategyName", "messageModel"] {
        assert!(java_api["body"].get(key).is_none(), "{key}");
    }

    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker.local_addr().unwrap().to_string();
    let (runtime, name_peer) = setup(Some(broker_address), false).await;
    let broker_peer = tokio::spawn(async move {
        let (mut socket, _) = broker.accept().await.unwrap();
        let command = read(&mut socket).await;
        assert_eq!(command.code, request::QUERY_ASSIGNMENT);
        let body: serde_json::Value =
            serde_json::from_slice(command.body.as_ref().unwrap()).unwrap();
        assert_eq!(body["topic"], "topic");
        assert_eq!(body["clientId"].as_str().unwrap(), "192.0.2.6@DEFAULT");
        for key in ["consumerGroup", "strategyName", "messageModel"] {
            assert!(body.get(key).is_none(), "{key}");
        }
        reply(
            &mut socket,
            &command,
            0,
            Some(br#"{"messageQueueAssignments":[]}"#.to_vec()),
        )
        .await;
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });

    assert!(
        runtime
            .query_assignment_nullable(Some("topic"), None, None, None, 3000)
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
    runtime.remoting().shutdown().await;
    broker_peer.await.unwrap();
    name_peer.await.unwrap();
}

#[test]
fn null_broker_address_lookups_match_invoked_java_results() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/route.json")).unwrap();
    let java = &fixture["runtimeNullBrokerAddressLookups"];
    for name in ["admin", "publish", "subscribe"] {
        assert_eq!(java[name]["outcome"], "OK", "{name}");
        assert!(java[name]["isNull"].as_bool().unwrap(), "{name}");
    }

    let snapshot = RouteSnapshot::default();
    assert!(snapshot.admin_address_nullable(None).unwrap().is_none());
    assert!(snapshot.publish_address_nullable(None).is_none());
    assert!(
        snapshot
            .subscribe_address_nullable(None, 0, true)
            .unwrap()
            .is_none()
    );
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
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
    let java_vip = &fixture["runtimeLookupVipMutation"];
    assert_eq!(
        java_vip["consumerAddressVipDisabled"],
        serde_json::json!(["127.0.0.1:10913"])
    );
    assert_eq!(
        java_vip["assignmentAddressVipDisabled"],
        serde_json::json!(["127.0.0.1:10913"])
    );
    assert_eq!(
        java_vip["consumerAddressVipEnabled"],
        serde_json::json!(["127.0.0.1:10911"])
    );
    assert_eq!(
        java_vip["assignmentAddressVipEnabled"],
        serde_json::json!(["127.0.0.1:10911"])
    );

    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = broker.local_addr().unwrap();
    let (runtime, name_peer) =
        setup(Some(format!("127.0.0.1:{}", address.port() + 2)), false).await;
    assert!(!runtime.config().read().unwrap().vip_channel_enabled);
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
async fn consumer_lookup_uses_timeout_mutated_after_runtime_construction() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
    let java_call = &fixture["runtimeLookupInvocation"]["consumerCallsAfterTimeoutMutation"][0];
    let timeout_ms = java_call["timeout"].as_u64().unwrap();
    let group = java_call["group"].as_str().unwrap().to_owned();
    assert_eq!(timeout_ms, 150);

    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (runtime, name_peer) = setup(Some(broker.local_addr().unwrap().to_string()), false).await;
    assert_eq!(runtime.config().read().unwrap().mq_client_api_timeout, 3000);
    runtime.config().write().unwrap().mq_client_api_timeout = timeout_ms as i32;
    let peer_group = group.clone();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = broker.accept().await.unwrap();
        let command = read(&mut socket).await;
        assert_eq!(command.code, request::GET_CONSUMER_LIST_BY_GROUP);
        assert_eq!(
            command.ext_fields.as_ref().unwrap()["consumerGroup"],
            peer_group
        );
        tokio::time::sleep(Duration::from_millis(timeout_ms * 3)).await;
        let mut response = RemotingCommand {
            opaque: command.opaque,
            code: 0,
            body: Some(br#"{"consumerIdList":["late"]}"#.to_vec()),
            ..Default::default()
        };
        response.mark_response();
        if let Ok(bytes) = frame::encode(&response, DEFAULT_MAX_FRAME_SIZE) {
            let _ = socket.write_all(&bytes).await;
        }
    });

    assert!(
        runtime
            .find_consumer_id_list("topic", &group)
            .await
            .unwrap()
            .is_none(),
        "the late Broker response must miss the Java-recorded live timeout"
    );
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
    let java_timeouts = java_lookup["nonpositiveAssignmentCalls"]
        .as_array()
        .unwrap()
        .iter()
        .map(|call| call["timeout"].as_i64().unwrap() as i32)
        .collect::<Vec<_>>();
    assert_eq!(java_timeouts, [0, -1]);
    for timeout in java_timeouts {
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
