use std::time::Duration;

use bytes::BytesMut;
use rocketmq_classic_core::{
    api::{MQClientApi, consumer::PullStatus},
    error::RpcError,
};
use rocketmq_classic_protocol::{
    body::{LockBatchRequestBody, UnlockBatchRequestBody},
    codes::{request, response},
    header::{
        CommandHeader, ConsumerSendMsgBackRequestHeader, HeaderMap, PullMessageRequestHeader,
        PullMessageResponseHeader, QueryConsumerOffsetRequestHeader,
        QueryConsumerOffsetResponseHeader, UpdateConsumerOffsetRequestHeader,
    },
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

async fn scripted(
    code: i32,
    fields: Option<HeaderMap>,
    body: Option<Vec<u8>>,
    expected: i32,
    oneway: bool,
) -> (MQClientApi, RemotingClient, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut len = [0; 4];
        stream.read_exact(&mut len).await.unwrap();
        let mut bytes = BytesMut::from(&len[..]);
        bytes.resize(u32::from_be_bytes(len) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        let incoming = frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap();
        assert_eq!(incoming.code, expected);
        if !oneway {
            let mut reply = RemotingCommand {
                code,
                ext_fields: fields,
                body,
                opaque: incoming.opaque,
                remark: Some("broker error".into()),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
                .await
                .unwrap();
        }
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.start().unwrap();
    (MQClientApi::new(client.clone()), client, addr)
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

fn fields<H: CommandHeader>(header: &H) -> HeaderMap {
    let mut fields = HeaderMap::new();
    header.encode_fields(&mut fields);
    fields
}

#[tokio::test]
async fn pull_statuses_and_malformed_success() {
    let header = PullMessageRequestHeader::default();
    let response_header = fields(&PullMessageResponseHeader {
        next_begin_offset: 12,
        min_offset: 1,
        max_offset: 30,
        suggest_which_broker_id: 2,
        ..Default::default()
    });
    for (code, status) in [
        (response::SUCCESS, PullStatus::Found),
        (response::PULL_NOT_FOUND, PullStatus::NoNewMessage),
        (
            response::PULL_RETRY_IMMEDIATELY,
            PullStatus::NoMatchedMessage,
        ),
        (response::PULL_OFFSET_MOVED, PullStatus::OffsetIllegal),
    ] {
        let (api, client, addr) = scripted(
            code,
            Some(response_header.clone()),
            Some(vec![0, 255, 42]),
            request::PULL_MESSAGE,
            false,
        )
        .await;
        let result = api.pull_message(&addr, &header, deadline()).await.unwrap();
        assert_eq!(result.status, status);
        assert_eq!(result.next_begin_offset, 12);
        assert_eq!(result.raw_messages, Some(vec![0, 255, 42]));
        client.shutdown().await;
    }
    for (code, fields) in [(response::SUCCESS, None), (9123, None)] {
        let (api, client, addr) = scripted(code, fields, None, request::PULL_MESSAGE, false).await;
        let result = api.pull_message(&addr, &header, deadline()).await;
        if code == response::SUCCESS {
            assert!(matches!(result, Err(RpcError::MalformedResponse { .. })));
        } else {
            assert!(matches!(result, Err(RpcError::Response { code: 9123, .. })));
        }
        client.shutdown().await;
    }
}

#[tokio::test]
async fn offset_lookup_and_update_modes() {
    for code in [response::SUCCESS, response::QUERY_NOT_FOUND, 9123] {
        let (api, client, addr) = scripted(
            code,
            Some(fields(&QueryConsumerOffsetResponseHeader { offset: 7 })),
            None,
            request::QUERY_CONSUMER_OFFSET,
            false,
        )
        .await;
        let result = api
            .query_consumer_offset(
                &addr,
                &QueryConsumerOffsetRequestHeader::default(),
                deadline(),
            )
            .await;
        match code {
            response::SUCCESS => assert_eq!(result.unwrap(), Some(7)),
            response::QUERY_NOT_FOUND => assert_eq!(result.unwrap(), None),
            _ => assert!(matches!(result, Err(RpcError::Response { .. }))),
        }
        client.shutdown().await;
    }
    let (api, client, addr) = scripted(
        response::SUCCESS,
        None,
        None,
        request::QUERY_CONSUMER_OFFSET,
        false,
    )
    .await;
    assert!(matches!(
        api.query_consumer_offset(
            &addr,
            &QueryConsumerOffsetRequestHeader::default(),
            deadline()
        )
        .await,
        Err(RpcError::MalformedResponse { .. })
    ));
    client.shutdown().await;
    for oneway in [false, true] {
        let (api, client, addr) = scripted(
            response::SUCCESS,
            None,
            None,
            request::UPDATE_CONSUMER_OFFSET,
            oneway,
        )
        .await;
        api.update_consumer_offset(
            &addr,
            &UpdateConsumerOffsetRequestHeader::default(),
            oneway,
            deadline(),
        )
        .await
        .unwrap();
        client.shutdown().await;
    }
}

#[tokio::test]
async fn list_send_back_lock_and_unlock() {
    let (api, client, addr) = scripted(
        response::SUCCESS,
        None,
        Some(br#"{"consumerIdList":["a","b"]}"#.to_vec()),
        request::GET_CONSUMER_LIST_BY_GROUP,
        false,
    )
    .await;
    assert_eq!(
        api.get_consumer_list_by_group(&addr, "workers", deadline())
            .await
            .unwrap(),
        Some(vec!["a".into(), "b".into()])
    );
    client.shutdown().await;
    let (api, client, addr) = scripted(
        response::SUCCESS,
        None,
        None,
        request::GET_CONSUMER_LIST_BY_GROUP,
        false,
    )
    .await;
    assert!(matches!(
        api.get_consumer_list_by_group(&addr, "workers", deadline())
            .await,
        Err(RpcError::Response {
            code: response::SUCCESS,
            ..
        })
    ));
    client.shutdown().await;
    let (api, client, addr) =
        scripted(9123, None, None, request::CONSUMER_SEND_MSG_BACK, false).await;
    assert!(matches!(
        api.consumer_send_message_back(
            &addr,
            &ConsumerSendMsgBackRequestHeader::default(),
            deadline()
        )
        .await,
        Err(RpcError::Response {
            request_code: request::CONSUMER_SEND_MSG_BACK,
            ..
        })
    ));
    client.shutdown().await;
    let (api, client, addr) = scripted(
        response::SUCCESS,
        None,
        Some(br#"{"lockOKMQSet":[]}"#.to_vec()),
        request::LOCK_BATCH_MQ,
        false,
    )
    .await;
    assert!(
        api.lock_batch_mq(&addr, &LockBatchRequestBody::default(), deadline())
            .await
            .unwrap()
            .is_empty()
    );
    client.shutdown().await;
    let (api, client, addr) =
        scripted(response::SUCCESS, None, None, request::LOCK_BATCH_MQ, false).await;
    assert!(matches!(
        api.lock_batch_mq(&addr, &LockBatchRequestBody::default(), deadline())
            .await,
        Err(RpcError::MalformedResponse { .. })
    ));
    client.shutdown().await;
    let (api, client, addr) = scripted(9123, None, None, request::LOCK_BATCH_MQ, false).await;
    assert!(matches!(
        api.lock_batch_mq(&addr, &LockBatchRequestBody::default(), deadline())
            .await,
        Err(RpcError::Response { code: 9123, .. })
    ));
    client.shutdown().await;
    for oneway in [true, false] {
        let (api, client, addr) = scripted(
            response::SUCCESS,
            None,
            None,
            request::UNLOCK_BATCH_MQ,
            oneway,
        )
        .await;
        api.unlock_batch_mq(
            &addr,
            &UnlockBatchRequestBody::default(),
            oneway,
            deadline(),
        )
        .await
        .unwrap();
        client.shutdown().await;
    }
}
