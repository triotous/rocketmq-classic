use std::time::Duration;

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{
        MQClientApi,
        extensions::{AckStatus, PopStatus},
    },
    error::RpcError,
};
use rocketmq_classic_protocol::{
    body::{QueryAssignmentRequestBody, SetMessageRequestModeRequestBody},
    codes::{request, response},
    header::{PopMessageRequestHeader, UnregisterClientRequestHeader},
    heartbeat::HeartbeatData,
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{RemotingClient, TransportMode},
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::Instant,
};

async fn peer(
    code: i32,
    status: i32,
    body: Option<Vec<u8>>,
    fields: Option<std::collections::BTreeMap<String, String>>,
) -> (
    MQClientApi,
    RemotingClient,
    String,
    tokio::task::JoinHandle<RemotingCommand>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut len = [0; 4];
        stream.read_exact(&mut len).await.unwrap();
        let mut bytes = BytesMut::from(&len[..]);
        bytes.resize(u32::from_be_bytes(len) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let incoming = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(incoming.code, code);
        let mut reply = RemotingCommand {
            code: status,
            opaque: incoming.opaque,
            version: 515,
            body,
            ext_fields: fields,
            remark: Some("rejected".into()),
            ..RemotingCommand::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
        incoming
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    (MQClientApi::new(client.clone()), client, addr, task)
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[tokio::test]
async fn heartbeat_versions_and_unregister_header() {
    let (api, client, addr, task) = peer(
        request::HEART_BEAT,
        response::SUCCESS,
        None,
        Some(
            [
                ("isSubChange".into(), "true".into()),
                ("isSupportHeartbeatV2".into(), "true".into()),
            ]
            .into(),
        ),
    )
    .await;
    let result = api
        .send_heartbeat_v2(
            &addr,
            &HeartbeatData {
                client_id: Some("client".into()),
                ..Default::default()
            },
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(result.version, 515);
    assert!(result.is_sub_change && result.is_support_heartbeat_v2);
    let incoming = task.await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&incoming.body.unwrap()).unwrap()["clientID"],
        "client"
    );
    client.shutdown().await;

    let (api, client, addr, task) = peer(
        request::UNREGISTER_CLIENT,
        response::SYSTEM_ERROR,
        None,
        None,
    )
    .await;
    let err = api
        .unregister_client(
            &addr,
            &UnregisterClientRequestHeader {
                client_id: "c".into(),
                producer_group: None,
                consumer_group: Some("g".into()),
            },
            deadline(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        RpcError::Response {
            code: response::SYSTEM_ERROR,
            request_code: request::UNREGISTER_CLIENT,
            ..
        }
    ));
    assert_eq!(
        task.await.unwrap().ext_fields.unwrap()["consumerGroup"],
        "g"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn assignment_and_request_mode_shapes() {
    for (status, body) in [
        (
            response::SUCCESS,
            Some(br#"{"messageQueueAssignments":[]}"#.to_vec()),
        ),
        (response::SUCCESS, None),
        (999, None),
    ] {
        let (api, client, addr, task) =
            peer(request::QUERY_ASSIGNMENT, status, body.clone(), None).await;
        let result = api
            .query_assignment(
                &addr,
                &QueryAssignmentRequestBody {
                    topic: Some("orders".into()),
                    ..Default::default()
                },
                deadline(),
            )
            .await;
        match (status, body) {
            (response::SUCCESS, Some(_)) => {
                assert_eq!(result.unwrap().message_queue_assignments.unwrap().len(), 0)
            }
            (response::SUCCESS, None) => {
                assert!(matches!(result, Err(RpcError::MalformedResponse { .. })))
            }
            _ => assert!(matches!(result, Err(RpcError::Response { code: 999, .. }))),
        }
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&task.await.unwrap().body.unwrap())
                .unwrap()["topic"],
            "orders"
        );
        client.shutdown().await;
    }
    let (api, client, addr, task) = peer(
        request::SET_MESSAGE_REQUEST_MODE,
        response::SUCCESS,
        None,
        None,
    )
    .await;
    api.set_message_request_mode(
        &addr,
        &SetMessageRequestModeRequestBody::default(),
        deadline(),
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&task.await.unwrap().body.unwrap()).unwrap()["mode"],
        "PULL"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn pop_status_and_ack_semantics() {
    for (status, expected) in [
        (response::PULL_NOT_FOUND, Some(PopStatus::PollingNotFound)),
        (response::POLLING_TIMEOUT, Some(PopStatus::PollingNotFound)),
        (response::POLLING_FULL, Some(PopStatus::PollingFull)),
        (999, None),
    ] {
        let fields = (status != 999).then(|| {
            [
                ("popTime".into(), "1".into()),
                ("invisibleTime".into(), "2".into()),
                ("reviveQid".into(), "0".into()),
                ("restNum".into(), "3".into()),
            ]
            .into()
        });
        let (api, client, addr, task) = peer(request::POP_MESSAGE, status, None, fields).await;
        let result = api
            .pop_message(&addr, &PopMessageRequestHeader::default(), deadline())
            .await;
        match expected {
            Some(expected) => assert_eq!(result.unwrap().status, expected),
            None => assert!(matches!(result, Err(RpcError::Response { code: 999, .. }))),
        }
        task.await.unwrap();
        client.shutdown().await;
    }
    for status in [response::SUCCESS, 999] {
        let (api, client, addr, task) = peer(request::ACK_MESSAGE, status, None, None).await;
        let result = api
            .ack_message(&addr, &Default::default(), deadline())
            .await
            .unwrap();
        assert_eq!(
            result,
            if status == response::SUCCESS {
                AckStatus::Ok
            } else {
                AckStatus::NoExist
            }
        );
        task.await.unwrap();
        client.shutdown().await;
    }
}

#[tokio::test]
async fn change_invisible_time_returns_java_ack_metadata() {
    for (status, fields) in [
        (
            response::SUCCESS,
            Some(
                [
                    ("popTime".into(), "12".into()),
                    ("invisibleTime".into(), "30".into()),
                    ("reviveQid".into(), "2".into()),
                ]
                .into(),
            ),
        ),
        (response::SUCCESS, None),
        (999, None),
    ] {
        let (api, client, addr, task) =
            peer(request::CHANGE_MESSAGE_INVISIBLETIME, status, None, fields).await;
        let result = api
            .change_invisible_time(&addr, &Default::default(), deadline())
            .await;
        match status {
            response::SUCCESS if result.is_ok() => {
                let result = result.unwrap();
                assert_eq!(result.status, AckStatus::Ok);
                assert_eq!(
                    (result.pop_time, result.invisible_time, result.revive_qid),
                    (Some(12), Some(30), Some(2))
                );
            }
            response::SUCCESS => assert!(matches!(result, Err(RpcError::MalformedResponse { .. }))),
            _ => assert_eq!(result.unwrap().status, AckStatus::NoExist),
        }
        task.await.unwrap();
        client.shutdown().await;
    }
}
