use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_core::{api::MQClientApi, error::RpcError};
use rocketmq_classic_protocol::{
    body::{MessageRequestMode, QueryAssignmentRequestBody, SetMessageRequestModeRequestBody},
    header::{NotificationRequestHeader, UnregisterClientRequestHeader},
    heartbeat::{HeartbeatData, MessageModel},
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

fn body(value: &Value) -> Option<Vec<u8>> {
    value.as_str().map(|encoded| {
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let (mut result, mut bits, mut count) = (Vec::new(), 0u32, 0);
        for byte in encoded.bytes().take_while(|byte| *byte != b'=') {
            bits = (bits << 6)
                | alphabet
                    .iter()
                    .position(|item| *item == byte)
                    .expect("base64 digit") as u32;
            count += 6;
            if count >= 8 {
                count -= 8;
                result.push((bits >> count) as u8);
                bits &= (1 << count) - 1;
            }
        }
        result
    })
}

#[tokio::test]
async fn invoked_lifecycle_extension_cases_match_rust() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rpc-invoked-ext-more");
    let names = [
        "heartbeat-success",
        "heartbeat-error",
        "heartbeat-unknown",
        "heartbeat-v2-success",
        "heartbeat-v2-default",
        "heartbeat-v2-error",
        "heartbeat-v2-unknown",
        "unregister-success",
        "unregister-error",
        "unregister-unknown",
        "assignment-empty",
        "assignment-populated",
        "assignment-error",
        "assignment-unknown",
        "assignment-no-body",
        "assignment-invalid-json",
        "request-mode-success",
        "request-mode-error",
        "request-mode-unknown",
        "notification-stats-success",
        "notification-bool-success",
        "notification-error",
        "notification-unknown",
        "notification-defaults",
        "lite-topic-info-success",
        "lite-topic-info-defaults",
        "lite-topic-info-null-body",
        "lite-topic-info-null-inputs",
        "lite-topic-info-error",
        "lite-topic-info-unknown",
        "lite-topic-info-invalid-body",
        "lite-broker-info-success",
        "lite-broker-info-defaults",
        "lite-broker-info-null-body",
        "lite-broker-info-error",
        "lite-broker-info-unknown",
        "lite-broker-info-invalid-body",
        "parent-topic-info-success",
        "parent-topic-info-defaults",
        "parent-topic-info-null-body",
        "parent-topic-info-null-topic",
        "parent-topic-info-error",
        "parent-topic-info-unknown",
        "parent-topic-info-invalid-body",
    ];
    assert_eq!(fs::read_dir(&dir).unwrap().count(), names.len() + 35);
    for name in names {
        let fixture: Value =
            serde_json::from_slice(&fs::read(dir.join(format!("{name}.json"))).unwrap()).unwrap();
        assert_eq!(fixture["case"], name);
        assert_eq!(
            fixture["mode"],
            if name.starts_with("notification-") || name.starts_with("lite-topic-info-") {
                "ASYNC"
            } else {
                "SYNC"
            }
        );
        assert_eq!(
            fixture["provenance"]["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(
            fixture["provenance"]["entrypoint"],
            if name.starts_with("notification-") {
                if name == "notification-bool-success" {
                    "MQClientAPIExt.notification"
                } else {
                    "MQClientAPIExt.notificationWithPollingStats"
                }
            } else if name.starts_with("lite-topic-info-") {
                "MQClientAPIExt.getLiteTopicInfoAsync"
            } else if name.starts_with("parent-topic-info-") {
                "MQClientAPIImpl.getParentTopicInfo"
            } else {
                "MQClientAPIImpl"
            }
        );
        assert_eq!(fixture["provenance"]["serverBaselineContacted"], false);
        if name.starts_with("notification-") {
            let notify_result = &fixture["notifyResultApi"];
            assert_eq!(notify_result["publicFieldCount"], 0);
            assert_eq!(notify_result["publicConstructorCount"], 1);
            assert_eq!(notify_result["defaultHasMsg"], false);
            assert_eq!(notify_result["defaultPollingFull"], false);
            assert_eq!(
                notify_result["declaredPublicMethods"],
                serde_json::json!([
                    "isHasMsg:boolean()",
                    "isPollingFull:boolean()",
                    "setHasMsg:void(boolean)",
                    "setPollingFull:void(boolean)",
                    "toString:String()"
                ])
            );
            assert_eq!(
                notify_result["populatedToString"],
                "NotifyResult{hasMsg=true, pollingFull=true}"
            );
        }
        if name.starts_with("lite-topic-info-") {
            let api = &fixture["liteTopicInfoApi"];
            assert_eq!(
                api["requestHeaderConstructors"].as_array().unwrap().len(),
                1
            );
            assert_eq!(api["requestHeaderPublicFields"], serde_json::json!([]));
            assert_eq!(api["responseBodyConstructors"].as_array().unwrap().len(), 1);
            assert_eq!(api["responseBodyPublicFields"], serde_json::json!([]));
            assert_eq!(api["clientGroupConstructors"].as_array().unwrap().len(), 1);
            assert_eq!(api["clientGroupPublicFields"].as_array().unwrap().len(), 2);
            assert_eq!(api["topicOffsetConstructors"].as_array().unwrap().len(), 1);
            assert_eq!(api["topicOffsetPublicFields"], serde_json::json!([]));
            assert_eq!(
                api["requestHeaderPublicMethods"].as_array().unwrap().len(),
                5
            );
            assert_eq!(
                api["responseBodyPublicMethods"].as_array().unwrap().len(),
                10
            );
            assert_eq!(api["clientGroupPublicMethods"].as_array().unwrap().len(), 3);
            assert_eq!(api["topicOffsetPublicMethods"].as_array().unwrap().len(), 7);
        }
        if name.starts_with("lite-broker-info-") {
            let api = &fixture["brokerLiteInfoApi"];
            assert_eq!(api["rpcMethods"].as_array().unwrap().len(), 1);
            assert!(
                api["rpcMethods"][0]
                    .as_str()
                    .unwrap()
                    .contains("getBrokerLiteInfo")
            );
            assert_eq!(api["responseBodyPublicFields"], serde_json::json!([]));
            assert_eq!(api["responseBodyConstructors"].as_array().unwrap().len(), 1);
            assert_eq!(
                api["responseBodyPublicMethods"].as_array().unwrap().len(),
                20
            );
            assert_eq!(api["responseBodyDefaults"]["maxLmqNum"], 0);
            assert_eq!(api["responseBodyDefaults"]["topicMetaNull"], true);
            assert_eq!(api["responseBodyDefaults"]["groupMetaNull"], true);
        }
        if name.starts_with("parent-topic-info-") {
            let api = &fixture["parentTopicInfoApi"];
            assert_eq!(api["rpcMethods"].as_array().unwrap().len(), 1);
            assert!(
                api["rpcMethods"][0]
                    .as_str()
                    .unwrap()
                    .contains("getParentTopicInfo")
            );
            assert_eq!(
                api["requestHeaderConstructors"].as_array().unwrap().len(),
                1
            );
            assert_eq!(api["requestHeaderPublicFields"], serde_json::json!([]));
            assert_eq!(api["responseBodyConstructors"].as_array().unwrap().len(), 1);
            assert_eq!(api["responseBodyPublicFields"], serde_json::json!([]));
            assert_eq!(
                api["requestHeaderPublicMethods"].as_array().unwrap().len(),
                3
            );
            assert_eq!(
                api["responseBodyPublicMethods"].as_array().unwrap().len(),
                10
            );
            assert_eq!(api["responseBodyDefaults"]["ttl"], 0);
            assert_eq!(api["responseBodyDefaults"]["groupsNull"], true);
            assert_eq!(api["groupsGetterReturnsAssignedSet"], true);
            assert_eq!(api["groupsGetterMutationVisible"], true);
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let expected = fixture.clone();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut len = [0; 4];
            stream.read_exact(&mut len).await.unwrap();
            let mut bytes = BytesMut::from(&len[..]);
            bytes.resize(u32::from_be_bytes(len) as usize + 4, 0);
            stream.read_exact(&mut bytes[4..]).await.unwrap();
            let incoming = frame::decode_next(&mut bytes).unwrap().unwrap();
            let request = &expected["request"];
            assert_eq!(incoming.code, request["code"].as_i64().unwrap() as i32);
            let expected_ext_fields = serde_json::from_value::<
                std::collections::BTreeMap<String, String>,
            >(request["ext"].clone())
            .unwrap();
            if expected_ext_fields.is_empty() {
                assert!(
                    incoming
                        .ext_fields
                        .as_ref()
                        .is_none_or(|fields| fields.is_empty())
                );
            } else {
                assert_eq!(incoming.ext_fields, Some(expected_ext_fields.into()));
            }
            assert!(!incoming.is_oneway());
            match (incoming.body.as_ref(), body(&request["bodyBase64"])) {
                (None, None) => (),
                (Some(actual), Some(reference)) => {
                    let actual: Value = serde_json::from_slice(actual).unwrap();
                    let reference: Value = serde_json::from_slice(&reference).unwrap();
                    assert_eq!(actual, reference);
                }
                other => panic!("body presence mismatch: {other:?}"),
            }
            let response = &expected["response"];
            let mut reply = RemotingCommand {
                code: response["code"].as_i64().unwrap() as i32,
                version: response["version"].as_i64().unwrap_or(0) as i32,
                opaque: incoming.opaque,
                ext_fields: Some(serde_json::from_value(response["ext"].clone()).unwrap()),
                body: body(&response["bodyBase64"]),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let api = MQClientApi::new(client.clone());
        let deadline = Instant::now() + Duration::from_secs(3);
        let result = if name.starts_with("notification-") {
            let mut header = NotificationRequestHeader::default();
            header.set_consumer_group("workers".into());
            header.set_topic("orders".into());
            header.set_queue_id(2);
            header.set_poll_time(3_000);
            header.set_born_time(1_000);
            header.set_order(Some(true));
            header.set_attempt_id(Some("attempt-1".into()));
            header.set_lite_consumer(true);
            header.set_client_id(Some("client".into()));
            header.set_exp_type(Some("TAG".into()));
            header.set_exp(Some("red || blue".into()));
            if name == "notification-bool-success" {
                match api.notification(&addr, &header, deadline).await {
                    Ok(has_msg) => format!("hasMsg={has_msg}"),
                    Err(RpcError::Response { code, .. }) => {
                        format!("MQBrokerException;code={code}")
                    }
                    other => panic!("unexpected notification result: {other:?}"),
                }
            } else {
                match api
                    .notification_with_polling_stats(&addr, &header, deadline)
                    .await
                {
                    Ok(result) => format!(
                        "hasMsg={};pollingFull={};toString={result}",
                        result.is_has_msg(),
                        result.is_polling_full()
                    ),
                    Err(RpcError::Response { code, .. }) => {
                        format!("MQBrokerException;code={code}")
                    }
                    other => panic!("unexpected notification stats result: {other:?}"),
                }
            }
        } else if name.starts_with("lite-topic-info-") {
            let (parent_topic, lite_topic) = if name == "lite-topic-info-null-inputs" {
                (None, None)
            } else {
                (Some("lite-parent"), Some("%LMQ%lite-parent%orders"))
            };
            match api
                .get_lite_topic_info(&addr, parent_topic, lite_topic, deadline)
                .await
            {
                Ok(None) => "null".to_owned(),
                Ok(Some(result)) if name == "lite-topic-info-success" => {
                    let subscribers = result.get_subscriber().unwrap();
                    let subscriber_count = subscribers.with(|set| set.len());
                    let topic_offset = result.get_topic_offset().unwrap();
                    topic_offset.with(|offset| {
                        format!(
                            "parent={};lite={};subscribers={subscriber_count};min={};max={};updated={};sharding={}",
                            result.get_parent_topic().unwrap_or("null"),
                            result.get_lite_topic().unwrap_or("null"),
                            offset.get_min_offset(),
                            offset.get_max_offset(),
                            offset.get_last_update_timestamp(),
                            result.is_sharding_to_broker(),
                        )
                    })
                }
                Ok(Some(result)) => format!(
                    "parent={};lite={};subscriberNull={};offsetNull={};sharding={}",
                    result.get_parent_topic().unwrap_or("null"),
                    result.get_lite_topic().unwrap_or("null"),
                    result.get_subscriber().is_none(),
                    result.get_topic_offset().is_none(),
                    result.is_sharding_to_broker(),
                ),
                Err(RpcError::Response { code, .. }) => {
                    format!("MQBrokerException;code={code}")
                }
                Err(RpcError::MalformedResponse { .. }) => "JSONException;malformed".to_owned(),
                other => panic!("unexpected lite topic info result: {other:?}"),
            }
        } else if name.starts_with("lite-broker-info-") {
            match api.get_broker_lite_info(&addr, deadline).await {
                Ok(None) => "null".to_owned(),
                Ok(Some(result)) if name == "lite-broker-info-success" => format!(
                    "store={};max={};current={};subscriptions={};order={};cq={};offset={};events={};topics={};groups={}",
                    result.get_store_type().unwrap_or("null"),
                    result.get_max_lmq_num(),
                    result.get_current_lmq_num(),
                    result.get_lite_subscription_count(),
                    result.get_order_info_count(),
                    result.get_cq_table_size(),
                    result.get_offset_table_size(),
                    result.get_event_map_size(),
                    result.get_topic_meta().unwrap().with(|map| map.len()),
                    result.get_group_meta().unwrap().with(|map| map.len()),
                ),
                Ok(Some(result)) => format!(
                    "store={};max={};current={};topicMetaNull={};groupMetaNull={}",
                    result.get_store_type().unwrap_or("null"),
                    result.get_max_lmq_num(),
                    result.get_current_lmq_num(),
                    result.get_topic_meta().is_none(),
                    result.get_group_meta().is_none(),
                ),
                Err(RpcError::Response { code, .. }) => {
                    format!("MQBrokerException;code={code}")
                }
                Err(RpcError::MalformedResponse { .. }) => "JSONException;malformed".to_owned(),
                other => panic!("unexpected Lite Broker info result: {other:?}"),
            }
        } else if name.starts_with("parent-topic-info-") {
            let topic = if name == "parent-topic-info-null-topic" {
                None
            } else {
                Some("lite-parent")
            };
            match api.get_parent_topic_info(&addr, topic, deadline).await {
                Ok(None) => "null".to_owned(),
                Ok(Some(result)) if name == "parent-topic-info-success" => format!(
                    "topic={};ttl={};groups={};lmq={};liteTopics={}",
                    result.get_topic().unwrap_or("null"),
                    result.get_ttl(),
                    result.get_groups().unwrap().with(|groups| groups.len()),
                    result.get_lmq_num(),
                    result.get_lite_topic_count(),
                ),
                Ok(Some(result)) => format!(
                    "topic={};ttl={};groupsNull={};lmq={};liteTopics={}",
                    result.get_topic().unwrap_or("null"),
                    result.get_ttl(),
                    result.get_groups().is_none(),
                    result.get_lmq_num(),
                    result.get_lite_topic_count(),
                ),
                Err(RpcError::Response { code, .. }) => {
                    format!("MQBrokerException;code={code}")
                }
                Err(RpcError::MalformedResponse { .. }) => "JSONException;malformed".to_owned(),
                other => panic!("unexpected parent topic info result: {other:?}"),
            }
        } else if name.starts_with("heartbeat-v2-") {
            let data = HeartbeatData {
                client_id: Some("client".into()),
                ..Default::default()
            };
            match api.send_heartbeat_v2(&addr, &data, deadline).await {
                Ok(v) => format!(
                    "version={};sub={};support={}",
                    v.version, v.is_sub_change, v.is_support_heartbeat_v2
                ),
                Err(RpcError::Response { code, .. }) => format!("MQBrokerException;code={code}"),
                other => panic!("unexpected v2 result: {other:?}"),
            }
        } else if name.starts_with("heartbeat-") {
            let data = HeartbeatData {
                client_id: Some("client".into()),
                ..Default::default()
            };
            match api.send_heartbeat(&addr, &data, deadline).await {
                Ok(version) => format!("version={version}"),
                Err(RpcError::Response { code, .. }) => format!("MQBrokerException;code={code}"),
                other => panic!("unexpected heartbeat result: {other:?}"),
            }
        } else if name.starts_with("unregister-") {
            let header = {
                let mut __phase18_header_0 = UnregisterClientRequestHeader::default();
                __phase18_header_0.set_client_id("client".into());
                __phase18_header_0.set_producer_group(Some("producer".into()));
                __phase18_header_0.set_consumer_group(Some("consumer".into()));
                __phase18_header_0
            };
            match api.unregister_client(&addr, &header, deadline).await {
                Ok(()) => "ok".into(),
                Err(RpcError::Response { code, .. }) => format!("MQBrokerException;code={code}"),
                other => panic!("unexpected unregister result: {other:?}"),
            }
        } else if name.starts_with("assignment-") {
            let mut request = QueryAssignmentRequestBody::default();
            request.set_topic(Some("oracle-topic".into()));
            request.set_consumer_group(Some("consumer".into()));
            request.set_client_id(Some("client".into()));
            request.set_strategy_name(Some("AVG".into()));
            request.set_message_model(Some(MessageModel::Clustering));
            match api.query_assignment(&addr, &request, deadline).await {
                Ok(v) => {
                    let assignments = v
                        .get_message_queue_assignments()
                        .map(|assignments| assignments.snapshot().into_iter().collect::<Vec<_>>())
                        .unwrap_or_default();
                    if name == "assignment-populated" {
                        let item = assignments
                            .first()
                            .and_then(Option::as_ref)
                            .unwrap()
                            .snapshot();
                        let queue = item.message_queue().unwrap().snapshot();
                        let attachments = item.attachments().unwrap().snapshot();
                        format!(
                            "assignments={};topic={};broker={};queue={};mode=POP;source={}",
                            assignments.len(),
                            queue.topic().unwrap_or_default(),
                            queue.broker_name().unwrap_or_default(),
                            queue.queue_id(),
                            attachments["source"].as_deref().unwrap_or_default()
                        )
                    } else {
                        format!("assignments={}", assignments.len())
                    }
                }
                Err(RpcError::Response { code, .. }) => format!("MQBrokerException;code={code}"),
                Err(RpcError::MalformedResponse { .. }) => if name == "assignment-invalid-json" {
                    "JSONException;malformed"
                } else {
                    "NullPointerException;malformed"
                }
                .into(),
                other => panic!("unexpected assignment result: {other:?}"),
            }
        } else {
            let mut request = SetMessageRequestModeRequestBody::default();
            request.set_topic(Some("oracle-topic".into()));
            request.set_consumer_group(Some("consumer".into()));
            request.set_mode(Some(MessageRequestMode::Pop));
            request.set_pop_share_queue_num(2);
            match api
                .set_message_request_mode(&addr, &request, deadline)
                .await
            {
                Ok(()) => "ok".into(),
                Err(RpcError::Response { code, .. }) => format!("MQClientException;code={code}"),
                other => panic!("unexpected request mode result: {other:?}"),
            }
        };
        peer.await.unwrap();
        client.shutdown().await;
        assert_eq!(result, fixture["outcome"], "{name}");
    }
}
