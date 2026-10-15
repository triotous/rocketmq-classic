use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_core::{api::MQClientApi, error::RpcError};
use rocketmq_classic_protocol::{
    body::{MessageRequestMode, QueryAssignmentRequestBody, SetMessageRequestModeRequestBody},
    header::UnregisterClientRequestHeader,
    heartbeat::{HeartbeatData, MessageModel},
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
    ];
    assert_eq!(fs::read_dir(&dir).unwrap().count(), names.len());
    for name in names {
        let fixture: Value =
            serde_json::from_slice(&fs::read(dir.join(format!("{name}.json"))).unwrap()).unwrap();
        assert_eq!(fixture["case"], name);
        assert_eq!(fixture["mode"], "SYNC");
        assert_eq!(
            fixture["provenance"]["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(fixture["provenance"]["entrypoint"], "MQClientAPIImpl");
        assert_eq!(fixture["provenance"]["serverBaselineContacted"], false);
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
            let incoming = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .unwrap();
            let request = &expected["request"];
            assert_eq!(incoming.code, request["code"].as_i64().unwrap() as i32);
            assert_eq!(
                incoming.ext_fields,
                Some(serde_json::from_value(request["ext"].clone()).unwrap())
            );
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
                .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
                .await
                .unwrap();
        });
        let client = RemotingClient::new(TransportMode::Plain);
        client.start().unwrap();
        let api = MQClientApi::new(client.clone());
        let deadline = Instant::now() + Duration::from_secs(3);
        let result = if name.starts_with("heartbeat-v2-") {
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
            let header = UnregisterClientRequestHeader {
                client_id: "client".into(),
                producer_group: Some("producer".into()),
                consumer_group: Some("consumer".into()),
                ..UnregisterClientRequestHeader::default()
            };
            match api.unregister_client(&addr, &header, deadline).await {
                Ok(()) => "ok".into(),
                Err(RpcError::Response { code, .. }) => format!("MQBrokerException;code={code}"),
                other => panic!("unexpected unregister result: {other:?}"),
            }
        } else if name.starts_with("assignment-") {
            let request = QueryAssignmentRequestBody {
                topic: Some("oracle-topic".into()),
                consumer_group: Some("consumer".into()),
                client_id: Some("client".into()),
                strategy_name: Some("AVG".into()),
                message_model: Some(MessageModel::Clustering),
            };
            match api.query_assignment(&addr, &request, deadline).await {
                Ok(v) => {
                    let assignments = v
                        .message_queue_assignments
                        .map(|assignments| assignments.snapshot().into_iter().collect::<Vec<_>>())
                        .unwrap_or_default();
                    if name == "assignment-populated" {
                        let item = assignments
                            .first()
                            .and_then(Option::as_ref)
                            .unwrap()
                            .snapshot();
                        let queue = item.message_queue.as_ref().unwrap().snapshot();
                        let attachments = item.attachments.as_ref().unwrap().snapshot();
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
            let request = SetMessageRequestModeRequestBody {
                topic: Some("oracle-topic".into()),
                consumer_group: Some("consumer".into()),
                mode: Some(MessageRequestMode::Pop),
                pop_share_queue_num: 2,
            };
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
