use std::{sync::Arc, time::Duration};

use bytes::BytesMut;
use rocketmq_classic_common::message::{MessageExt, MessageQueue};
use rocketmq_classic_consumer::{
    ConsumeConcurrentlyStatus, PushConsumer, PushConsumerConfig, PushMessageListener,
};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::HeaderMap,
};
use rocketmq_classic_remoting::{
    RemotingCommand,
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};

async fn read_command(stream: &mut TcpStream) -> Option<RemotingCommand> {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.ok()?;
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.ok()?;
    frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
        .ok()
        .flatten()
}

async fn write_response(
    stream: &mut TcpStream,
    request: &RemotingCommand,
    code: i32,
    body: Option<Vec<u8>>,
    ext_fields: Option<HeaderMap>,
) {
    let mut reply = RemotingCommand {
        code,
        opaque: request.opaque,
        body,
        ext_fields,
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
        .await
        .unwrap();
}

fn route_body(address: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "queueDatas": [{
            "brokerName": "broker-a",
            "readQueueNums": 4,
            "writeQueueNums": 4,
            "perm": 6,
            "topicSysFlag": 0
        }],
        "brokerDatas": [{
            "cluster": "DefaultCluster",
            "brokerName": "broker-a",
            "brokerAddrs": {"0": address}
        }],
        "filterServerTable": {}
    }))
    .unwrap()
}

async fn nameserver_peer(route: Vec<u8>) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            if command.code == request::GET_ROUTEINFO_BY_TOPIC {
                write_response(
                    &mut stream,
                    &command,
                    response::SUCCESS,
                    Some(route.clone()),
                    None,
                )
                .await;
            } else if !command.is_oneway() {
                write_response(&mut stream, &command, response::SUCCESS, None, None).await;
            }
        }
    });
    (address, task)
}

async fn broker_peer() -> (
    String,
    JoinHandle<()>,
    tokio::sync::mpsc::UnboundedReceiver<RemotingCommand>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (sender, requests) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            let _ = sender.send(command.clone());
            let (code, body, ext_fields) = match command.code {
                request::SEARCH_OFFSET_BY_TIMESTAMP => (
                    response::SUCCESS,
                    None,
                    Some(HeaderMap::from([("offset".into(), "11".into())])),
                ),
                request::GET_MAX_OFFSET => (
                    response::SUCCESS,
                    None,
                    Some(HeaderMap::from([("offset".into(), "22".into())])),
                ),
                request::GET_MIN_OFFSET => (
                    response::SUCCESS,
                    None,
                    Some(HeaderMap::from([("offset".into(), "3".into())])),
                ),
                request::GET_EARLIEST_MSG_STORETIME => (
                    response::SUCCESS,
                    None,
                    Some(HeaderMap::from([("timestamp".into(), "44".into())])),
                ),
                request::PULL_MESSAGE => (
                    response::PULL_NOT_FOUND,
                    None,
                    Some(HeaderMap::from([
                        ("suggestWhichBrokerId".into(), "0".into()),
                        ("nextBeginOffset".into(), "0".into()),
                        ("minOffset".into(), "0".into()),
                        ("maxOffset".into(), "0".into()),
                    ])),
                ),
                request::GET_CONSUMER_LIST_BY_GROUP => (
                    response::SUCCESS,
                    Some(br#"{"consumerIdList":[]}"#.to_vec()),
                    None,
                ),
                _ => (response::SUCCESS, None, None),
            };
            write_response(&mut stream, &command, code, body, ext_fields).await;
        }
    });
    (address, task, requests)
}

#[tokio::test]
async fn push_admin_offset_facade_routes_namespaced_queues_to_the_named_broker() {
    let (broker, broker_task, mut broker_requests) = broker_peer().await;
    let (namesrv, namesrv_task) = nameserver_peer(route_body(&broker)).await;
    let mut config = PushConsumerConfig::new("push-admin-offsets").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    config.client_config_mut().set_namesrv_addr(Some(namesrv));
    let consumer = Arc::new(PushConsumer::new(config));
    consumer
        .register_message_listener(PushMessageListener::concurrently(
            |_messages: Vec<MessageExt>, context| async move {
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        ))
        .await
        .unwrap();
    consumer.start().await.unwrap();

    let queue = MessageQueue::new("orders", "broker-a", 3);
    assert_eq!(consumer.search_offset(&queue, 9).await.unwrap(), 11);
    assert_eq!(consumer.get_max_offset(&queue).await.unwrap(), 22);
    assert_eq!(consumer.get_min_offset(&queue).await.unwrap(), 3);
    assert_eq!(
        consumer.get_earliest_msg_store_time(&queue).await.unwrap(),
        44
    );

    for (expected_code, expected_timestamp) in [
        (request::SEARCH_OFFSET_BY_TIMESTAMP, Some("9")),
        (request::GET_MAX_OFFSET, None),
        (request::GET_MIN_OFFSET, None),
        (request::GET_EARLIEST_MSG_STORETIME, None),
    ] {
        let command = timeout(Duration::from_secs(1), async {
            loop {
                let command = broker_requests.recv().await.unwrap();
                if command.code == expected_code {
                    break command;
                }
            }
        })
        .await
        .unwrap();
        let fields = command.ext_fields.as_ref().unwrap();
        assert_eq!(fields.get("topic"), Some(&"tenant%orders".to_owned()));
        assert_eq!(fields.get("queueId"), Some(&"3".to_owned()));
        assert_eq!(fields.get("bname"), Some(&"broker-a".to_owned()));
        if let Some(timestamp) = expected_timestamp {
            assert_eq!(fields.get("timestamp"), Some(&timestamp.to_owned()));
            assert_eq!(fields.get("boundaryType"), Some(&"LOWER".to_owned()));
        }
        if expected_code == request::GET_MAX_OFFSET {
            assert_eq!(fields.get("committed"), Some(&"true".to_owned()));
        }
    }

    consumer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}
