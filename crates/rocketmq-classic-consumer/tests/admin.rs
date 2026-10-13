use std::{collections::BTreeMap, sync::Arc, time::Duration};

use base64::Engine;
use bytes::BytesMut;
use rocketmq_classic_common::message::{MessageExt, MessageQueue};
use rocketmq_classic_consumer::{
    ConsumeConcurrentlyStatus, PullConsumerConfig, PushConsumer, PushConsumerConfig,
    PushMessageListener, pull::PullConsumer,
};
use rocketmq_classic_protocol::{
    codes::{request, response},
    header::{CommandHeader, HeaderMap, QueryMessageResponseHeader},
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

fn java_message_record() -> Vec<u8> {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
    let record = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "ipv4-v1")
        .unwrap()["recordBase64"]
        .as_str()
        .unwrap();
    base64::engine::general_purpose::STANDARD
        .decode(record)
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
    let record = java_message_record();
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
                request::QUERY_MESSAGE => {
                    let mut fields = HeaderMap::new();
                    QueryMessageResponseHeader {
                        index_last_update_timestamp: 200,
                        index_last_update_phyoffset: 100,
                    }
                    .encode_fields(&mut fields);
                    write_response(
                        &mut stream,
                        &command,
                        response::SUCCESS,
                        Some(record.clone()),
                        Some(fields),
                    )
                    .await;
                    continue;
                }
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
    config
        .client_config_mut()
        .set_instance_name(Some("push-admin-offsets".into()));
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

#[tokio::test]
async fn push_and_pull_query_facades_preserve_java_client_message_ids() {
    let (push_broker, push_broker_task, mut push_requests) = broker_peer().await;
    let (push_namesrv, push_namesrv_task) = nameserver_peer(route_body(&push_broker)).await;
    let mut push_config = PushConsumerConfig::new("push-admin-query").unwrap();
    push_config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    push_config
        .client_config_mut()
        .set_instance_name(Some("push-admin-query".into()));
    push_config
        .client_config_mut()
        .set_namesrv_addr(Some(push_namesrv));
    let push = Arc::new(PushConsumer::new(push_config));
    push.register_message_listener(PushMessageListener::concurrently(
        |_messages: Vec<MessageExt>, context| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    ))
    .await
    .unwrap();
    push.start().await.unwrap();

    let push_result = push
        .query_message_by_unique_key("orders", "producer-unique-1", 32, 10, 99)
        .await
        .unwrap();
    assert_eq!(push_result.message_list.len(), 1);
    assert_eq!(push_result.message_list[0].message().topic(), "orders");
    assert_eq!(
        push_result.message_list[0].message_id(),
        Some("producer-unique-1")
    );
    assert_eq!(
        push_result.message_list[0].offset_message_id(),
        Some("C633641400002A9F0000018BCFE56829")
    );
    let push_query = timeout(Duration::from_secs(1), async {
        loop {
            let command = push_requests.recv().await.unwrap();
            if command.code == request::QUERY_MESSAGE {
                break command;
            }
        }
    })
    .await
    .unwrap();
    let fields = push_query.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"tenant%orders".to_owned()));
    assert_eq!(fields.get("indexType"), Some(&"U".to_owned()));
    assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"true".to_owned()));
    push.shutdown().await.unwrap();
    push_namesrv_task.abort();
    push_broker_task.abort();

    let (pull_broker, pull_broker_task, mut pull_requests) = broker_peer().await;
    let (pull_namesrv, pull_namesrv_task) = nameserver_peer(route_body(&pull_broker)).await;
    let mut pull_config = PullConsumerConfig::new("pull-admin-query").unwrap();
    pull_config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    pull_config
        .client_config_mut()
        .set_instance_name(Some("pull-admin-query".into()));
    pull_config
        .client_config_mut()
        .set_namesrv_addr(Some(pull_namesrv));
    let pull = Arc::new(PullConsumer::new(pull_config));
    pull.start().await.unwrap();

    let pull_result = pull
        .query_message_by_unique_key("orders", "producer-unique-1", 32, 10, 99)
        .await
        .unwrap();
    assert_eq!(pull_result.message_list.len(), 1);
    assert_eq!(pull_result.message_list[0].message().topic(), "orders");
    assert_eq!(
        pull_result.message_list[0].message_id(),
        Some("producer-unique-1")
    );
    assert_eq!(
        pull_result.message_list[0].offset_message_id(),
        Some("C633641400002A9F0000018BCFE56829")
    );
    let pull_query = timeout(Duration::from_secs(1), async {
        loop {
            let command = pull_requests.recv().await.unwrap();
            if command.code == request::QUERY_MESSAGE {
                break command;
            }
        }
    })
    .await
    .unwrap();
    let fields = pull_query.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"tenant%orders".to_owned()));
    assert_eq!(fields.get("indexType"), Some(&"U".to_owned()));
    assert_eq!(fields.get("_UNIQUE_KEY_QUERY"), Some(&"true".to_owned()));
    pull.shutdown().await.unwrap();
    pull_namesrv_task.abort();
    pull_broker_task.abort();
}

#[tokio::test]
async fn push_and_pull_create_topic_and_fetch_subscribe_queues_match_java_facades() {
    let (push_broker, push_broker_task, mut push_requests) = broker_peer().await;
    let (push_namesrv, push_namesrv_task) = nameserver_peer(route_body(&push_broker)).await;
    let mut push_config = PushConsumerConfig::new("push-admin-topic").unwrap();
    push_config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    push_config
        .client_config_mut()
        .set_instance_name(Some("push-admin-topic".into()));
    push_config
        .client_config_mut()
        .set_namesrv_addr(Some(push_namesrv));
    let push = Arc::new(PushConsumer::new(push_config));
    push.register_message_listener(PushMessageListener::concurrently(
        |_messages: Vec<MessageExt>, context| async move {
            (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
        },
    ))
    .await
    .unwrap();
    push.start().await.unwrap();

    let attributes = BTreeMap::from([("+ignored".to_owned(), "true".to_owned())]);
    push.create_topic("TBW102", "created", 3, Some(&attributes))
        .await
        .unwrap();
    let push_create = timeout(Duration::from_secs(1), async {
        loop {
            let command = push_requests.recv().await.unwrap();
            if command.code == request::UPDATE_AND_CREATE_TOPIC {
                break command;
            }
        }
    })
    .await
    .unwrap();
    let fields = push_create.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"tenant%created".to_owned()));
    assert_eq!(fields.get("defaultTopic"), Some(&"TBW102".to_owned()));
    assert_eq!(fields.get("readQueueNums"), Some(&"3".to_owned()));
    assert_eq!(fields.get("writeQueueNums"), Some(&"3".to_owned()));
    assert_eq!(fields.get("topicSysFlag"), Some(&"0".to_owned()));
    assert_eq!(fields.get("attributes"), Some(&String::new()));

    let push_queues = push.fetch_subscribe_message_queues("orders").await.unwrap();
    assert_eq!(push_queues.len(), 4);
    assert!(push_queues.contains(&MessageQueue::new("orders", "broker-a", 0)));
    push.shutdown().await.unwrap();
    push_namesrv_task.abort();
    push_broker_task.abort();

    let (pull_broker, pull_broker_task, mut pull_requests) = broker_peer().await;
    let (pull_namesrv, pull_namesrv_task) = nameserver_peer(route_body(&pull_broker)).await;
    let mut pull_config = PullConsumerConfig::new("pull-admin-topic").unwrap();
    pull_config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    pull_config
        .client_config_mut()
        .set_instance_name(Some("pull-admin-topic".into()));
    pull_config
        .client_config_mut()
        .set_namesrv_addr(Some(pull_namesrv));
    let pull = Arc::new(PullConsumer::new(pull_config));
    pull.start().await.unwrap();

    pull.create_topic_with_sys_flag("TBW102", "created", 2, 7, Some(&attributes))
        .await
        .unwrap();
    let pull_create = timeout(Duration::from_secs(1), async {
        loop {
            let command = pull_requests.recv().await.unwrap();
            if command.code == request::UPDATE_AND_CREATE_TOPIC {
                break command;
            }
        }
    })
    .await
    .unwrap();
    let fields = pull_create.ext_fields.unwrap();
    assert_eq!(fields.get("topic"), Some(&"tenant%created".to_owned()));
    assert_eq!(fields.get("defaultTopic"), Some(&"TBW102".to_owned()));
    assert_eq!(fields.get("readQueueNums"), Some(&"2".to_owned()));
    assert_eq!(fields.get("topicSysFlag"), Some(&"7".to_owned()));
    assert_eq!(fields.get("attributes"), Some(&String::new()));

    let pull_queues = pull.fetch_subscribe_message_queues("orders").await.unwrap();
    assert_eq!(pull_queues.len(), 4);
    assert!(pull_queues.contains(&MessageQueue::new("orders", "broker-a", 0)));
    pull.shutdown().await.unwrap();
    pull_namesrv_task.abort();
    pull_broker_task.abort();
}
