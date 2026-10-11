use std::{fs, path::PathBuf, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{
        MQClientApi,
        producer::{SendKind, SendMode, SendStatus},
    },
    error::RpcError,
};
use rocketmq_classic_protocol::header::{
    QueryConsumerOffsetRequestHeader, SendMessageRequestHeaderV1, UpdateConsumerOffsetRequestHeader,
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

fn bytes(value: &Value) -> Option<Vec<u8>> {
    value.as_str().map(|encoded| {
        // Fixture bodies are base64; avoid adding a production dependency for test-only decoding.
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut result = Vec::new();
        let mut bits = 0u32;
        let mut count = 0;
        for char in encoded.bytes().take_while(|char| *char != b'=') {
            let digit = alphabet
                .iter()
                .position(|item| *item == char)
                .expect("base64 digit");
            bits = (bits << 6) | digit as u32;
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

async fn scripted(
    fixture: Value,
    route: bool,
) -> (
    MQClientApi,
    RemotingClient,
    String,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut len = [0; 4];
        stream.read_exact(&mut len).await.unwrap();
        let mut frame_bytes = BytesMut::from(&len[..]);
        frame_bytes.resize(u32::from_be_bytes(len) as usize + 4, 0);
        stream.read_exact(&mut frame_bytes[4..]).await.unwrap();
        let incoming = frame::decode_next(&mut frame_bytes, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        let expected = &fixture["request"];
        assert_eq!(incoming.code, expected["code"].as_i64().unwrap() as i32);
        assert_eq!(
            incoming.ext_fields.as_ref().unwrap(),
            &serde_json::from_value(expected["ext"].clone()).unwrap()
        );
        assert_eq!(
            incoming.body.as_ref(),
            bytes(&expected["bodyBase64"]).as_ref()
        );
        let oneway = fixture["mode"] == "ONEWAY";
        assert_eq!(incoming.is_oneway(), oneway);
        if oneway {
            assert!(fixture["response"].is_null());
            return;
        }
        assert_eq!(fixture["mode"], "SYNC");
        let expected = &fixture["response"];
        let mut reply = RemotingCommand {
            code: expected["code"].as_i64().unwrap() as i32,
            opaque: incoming.opaque,
            ext_fields: Some(serde_json::from_value(expected["ext"].clone()).unwrap()),
            body: bytes(&expected["bodyBase64"]),
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
    });
    let client = RemotingClient::new(TransportMode::Plain);
    if route {
        client.update_name_server_address_list(vec![addr.clone()]);
    }
    client.start().unwrap();
    (MQClientApi::new(client.clone()), client, addr, peer)
}

#[tokio::test]
async fn java_invoked_cases_match_rust_requests_and_outcomes() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rpc-invoked");
    let entries: Vec<_> = fs::read_dir(&dir).unwrap().collect();
    assert_eq!(
        entries.len(),
        16,
        "all pinned Java invoked cases must be compared"
    );
    for name in [
        "route-success",
        "route-not-found",
        "route-unknown",
        "route-no-body",
        "route-invalid-json",
        "send-success",
        "send-unknown",
        "send-no-header",
        "offset-success",
        "offset-not-found",
        "offset-unknown",
        "offset-no-header",
        "update-success",
        "update-error",
        "update-unknown",
        "update-oneway",
    ] {
        let fixture: Value =
            serde_json::from_slice(&fs::read(dir.join(format!("{name}.json"))).unwrap()).unwrap();
        assert_eq!(fixture["case"], name);
        assert_eq!(
            fixture["provenance"]["client"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(fixture["provenance"]["serverBaselineContacted"], false);
        let (api, client, addr, peer) = scripted(fixture.clone(), name.starts_with("route-")).await;
        let deadline = Instant::now() + Duration::from_secs(3);
        let outcome = match name {
            name if name.starts_with("route-") => {
                match api.get_topic_route("oracle-topic", deadline).await {
                    Ok(Some(route)) => {
                        format!("orderTopicConf={}", route.order_topic_conf.unwrap())
                    }
                    Ok(None) => "null".into(),
                    Err(RpcError::Response { code, .. }) => {
                        format!("MQClientException;code={code}")
                    }
                    Err(RpcError::MalformedResponse { .. }) => {
                        if name == "route-invalid-json" {
                            "JsonParseException;malformed".into()
                        } else {
                            "MQClientException;code=0;malformed".into()
                        }
                    }
                    other => panic!("unexpected route result: {other:?}"),
                }
            }
            name if name.starts_with("send-") => {
                let header = SendMessageRequestHeaderV1 {
                    producer_group: "producer".into(),
                    topic: "oracle-topic".into(),
                    default_topic: "TBW102".into(),
                    default_topic_queue_nums: 4,
                    queue_id: 2,
                    born_timestamp: 1,
                    properties: Some(String::new()),
                    reconsume_times: Some(0),
                    unit_mode: Some(false),
                    batch: Some(false),
                    ..Default::default()
                };
                match api
                    .send_message(
                        &addr,
                        "broker",
                        &header,
                        b"payload".to_vec(),
                        "unused",
                        SendKind::V2,
                        SendMode::Sync,
                        deadline,
                    )
                    .await
                {
                    Ok(Some(result)) => {
                        assert_eq!(result.status, SendStatus::SendOk);
                        format!(
                            "SEND_OK;queueOffset={};offsetMsgId={}",
                            result.queue_offset, result.offset_msg_id
                        )
                    }
                    Err(RpcError::Response { code, .. }) => {
                        format!("MQBrokerException;code={code}")
                    }
                    Err(RpcError::MalformedResponse { .. }) => {
                        "NullPointerException;malformed".into()
                    }
                    other => panic!("unexpected send result: {other:?}"),
                }
            }
            name if name.starts_with("update-") => {
                let header = UpdateConsumerOffsetRequestHeader {
                    consumer_group: "consumer".into(),
                    topic: "oracle-topic".into(),
                    queue_id: 2,
                    commit_offset: 42,
                    broker_name: None,
                };
                match api
                    .update_consumer_offset(&addr, &header, name == "update-oneway", deadline)
                    .await
                {
                    Ok(()) => "ok".into(),
                    Err(RpcError::Response { code, .. }) => {
                        format!("MQBrokerException;code={code}")
                    }
                    other => panic!("unexpected update result: {other:?}"),
                }
            }
            name if name.starts_with("offset-") => {
                let header = QueryConsumerOffsetRequestHeader {
                    consumer_group: "consumer".into(),
                    topic: "oracle-topic".into(),
                    queue_id: 2,
                    ..Default::default()
                };
                match api.query_consumer_offset(&addr, &header, deadline).await {
                    Ok(Some(offset)) => format!("offset={offset}"),
                    Ok(None) => "OffsetNotFoundException;code=22".into(),
                    Err(RpcError::Response { code, .. }) => {
                        if code == 22 {
                            format!("OffsetNotFoundException;code={code}")
                        } else {
                            format!("MQBrokerException;code={code}")
                        }
                    }
                    Err(RpcError::MalformedResponse { .. }) => {
                        "NullPointerException;malformed".into()
                    }
                    other => panic!("unexpected offset result: {other:?}"),
                }
            }
            _ => panic!("unexpected case {name}"),
        };
        assert_eq!(outcome, fixture["outcome"], "{name}");
        tokio::time::timeout(Duration::from_secs(3), peer)
            .await
            .expect("scripted peer timed out")
            .expect("scripted peer request comparison failed");
        client.shutdown().await;
    }
}
