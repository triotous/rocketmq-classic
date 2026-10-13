use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::BytesMut;
use rocketmq_classic_common::topic::TopicValidator;
use rocketmq_classic_core::error::RpcError;
use rocketmq_classic_core::route::broker_vip_channel;
use rocketmq_classic_producer::{Producer, ProducerConfig, ProducerError};
use rocketmq_classic_protocol::codes::{request, response};
use rocketmq_classic_protocol::header::HeaderMap;
use rocketmq_classic_remoting::{
    RemotingCommand,
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
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
) {
    let mut reply = RemotingCommand {
        code,
        opaque: request.opaque,
        body,
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
        .await
        .unwrap();
}

async fn write_response_fields(
    stream: &mut TcpStream,
    request: &RemotingCommand,
    code: i32,
    ext_fields: HeaderMap,
) {
    let mut reply = RemotingCommand {
        code,
        opaque: request.opaque,
        ext_fields: Some(ext_fields),
        ..Default::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
        .await
        .unwrap();
}

async fn name_server_peer(
    route: Vec<u8>,
) -> (
    String,
    JoinHandle<()>,
    mpsc::UnboundedReceiver<RemotingCommand>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (sender, requests) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            let _ = sender.send(command.clone());
            if command.code == request::GET_ROUTEINFO_BY_TOPIC {
                write_response(
                    &mut stream,
                    &command,
                    response::SUCCESS,
                    Some(route.clone()),
                )
                .await;
            } else if !command.is_oneway() {
                write_response(&mut stream, &command, response::SUCCESS, None).await;
            }
        }
    });
    (address, task, requests)
}

async fn broker_peer(
    response_codes: Vec<i32>,
) -> (
    String,
    JoinHandle<()>,
    mpsc::UnboundedReceiver<RemotingCommand>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (sender, requests) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut codes = response_codes.into_iter();
        while let Some(command) = read_command(&mut stream).await {
            let _ = sender.send(command.clone());
            write_response(
                &mut stream,
                &command,
                codes.next().unwrap_or(response::SUCCESS),
                None,
            )
            .await;
        }
    });
    (address, task, requests)
}

async fn offset_broker_peer() -> (
    String,
    JoinHandle<()>,
    mpsc::UnboundedReceiver<RemotingCommand>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (sender, requests) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            let _ = sender.send(command.clone());
            if command.code == request::HEART_BEAT {
                write_response(&mut stream, &command, response::SUCCESS, None).await;
                continue;
            }
            let (code, value) = match command.code {
                request::SEARCH_OFFSET_BY_TIMESTAMP => ("offset", "11"),
                request::GET_MAX_OFFSET => ("offset", "22"),
                request::GET_MIN_OFFSET => ("offset", "3"),
                request::GET_EARLIEST_MSG_STORETIME => ("timestamp", "44"),
                other => panic!("unexpected Broker request {other}"),
            };
            write_response_fields(
                &mut stream,
                &command,
                response::SUCCESS,
                HeaderMap::from([(code.to_owned(), value.to_owned())]),
            )
            .await;
        }
    });
    (address, task, requests)
}

fn route_body(brokers: &[(&str, &str)]) -> Vec<u8> {
    let queue_data = brokers
        .iter()
        .map(|(name, _)| {
            json!({
                "brokerName": name,
                "readQueueNums": 4,
                "writeQueueNums": 4,
                "perm": 6,
                "topicSysFlag": 0
            })
        })
        .collect::<Vec<_>>();
    let broker_data = brokers
        .iter()
        .map(|(name, address)| {
            json!({
                "cluster": "DefaultCluster",
                "brokerName": name,
                "brokerAddrs": {"0": address}
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({
        "queueDatas": queue_data,
        "brokerDatas": broker_data,
        "filterServerTable": {}
    }))
    .unwrap()
}

fn producer_config(namesrv: String) -> ProducerConfig {
    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let mut config = ProducerConfig::new(format!("admin-producer-{id}")).unwrap();
    config.client_config_mut().namesrv_addr = Some(namesrv);
    config.client_config_mut().instance_name = Some(format!("admin-{}-{id}", std::process::id()));
    config
}

#[tokio::test]
async fn create_topic_sorts_brokers_and_accepts_partial_success() {
    let (broker_a, broker_a_task, mut broker_a_requests) =
        broker_peer(vec![response::SERVICE_NOT_AVAILABLE; 5]).await;
    let (broker_b, broker_b_task, mut broker_b_requests) =
        broker_peer(vec![response::SUCCESS]).await;
    let route = route_body(&[("broker-b", &broker_b), ("broker-a", &broker_a)]);
    let (namesrv, namesrv_task, mut namesrv_requests) = name_server_peer(route).await;
    let mut config = producer_config(namesrv);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();

    let attributes = BTreeMap::from([("+unit".to_owned(), "true".to_owned())]);
    producer
        .create_topic("TBW102", "orders", 4, Some(&attributes))
        .await
        .unwrap();

    let route_request = timeout(Duration::from_secs(1), namesrv_requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(route_request.code, request::GET_ROUTEINFO_BY_TOPIC);
    assert_eq!(
        route_request
            .ext_fields
            .as_ref()
            .and_then(|fields| fields.get("topic"))
            .map(String::as_str),
        Some(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC)
    );

    let mut broker_a_count = 0;
    for _ in 0..5 {
        let command = timeout(Duration::from_secs(1), broker_a_requests.recv())
            .await
            .unwrap()
            .unwrap();
        broker_a_count += 1;
        assert_eq!(command.code, request::UPDATE_AND_CREATE_TOPIC);
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("topic"),
            Some(&"tenant%orders".to_owned())
        );
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("defaultTopic"),
            Some(&"TBW102".to_owned())
        );
        assert_eq!(
            command.ext_fields.as_ref().unwrap().get("attributes"),
            Some(&String::new())
        );
    }
    let success = timeout(Duration::from_secs(1), broker_b_requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(success.code, request::UPDATE_AND_CREATE_TOPIC);
    assert_eq!(broker_a_count, 5);

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_a_task.abort();
    broker_b_task.abort();
}

#[tokio::test]
async fn create_topic_validates_topic_before_contacting_nameserver() {
    let (broker, broker_task, _) = broker_peer(vec![]).await;
    let (namesrv, namesrv_task, mut namesrv_requests) =
        name_server_peer(route_body(&[("broker", &broker)])).await;
    let producer = Arc::new(Producer::new(producer_config(namesrv)));
    producer.start().await.unwrap();

    assert!(
        producer
            .create_topic("TBW102", "bad.topic", 4, None)
            .await
            .is_err()
    );
    assert!(
        timeout(Duration::from_millis(50), namesrv_requests.recv())
            .await
            .is_err()
    );

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn create_topic_returns_the_last_broker_error_after_five_attempts() {
    let (broker, broker_task, mut broker_requests) =
        broker_peer(vec![response::NO_PERMISSION; 5]).await;
    let (namesrv, namesrv_task, _) = name_server_peer(route_body(&[("broker", &broker)])).await;
    let producer = Arc::new(Producer::new(producer_config(namesrv)));
    producer.start().await.unwrap();

    let error = producer
        .create_topic("TBW102", "orders", 4, None)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ProducerError::Rpc(RpcError::Response {
            code: response::NO_PERMISSION,
            ..
        })
    ));
    for _ in 0..5 {
        let command = timeout(Duration::from_secs(1), broker_requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(command.code, request::UPDATE_AND_CREATE_TOPIC);
    }

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn producer_admin_offset_facade_routes_namespaced_queues_to_the_named_broker() {
    let (broker, broker_task, mut broker_requests) = offset_broker_peer().await;
    let (namesrv, namesrv_task, mut namesrv_requests) =
        name_server_peer(route_body(&[("broker-a", &broker)])).await;
    let mut config = producer_config(namesrv);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();
    let queue = rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 3);

    assert_eq!(producer.search_offset(&queue, 9).await.unwrap(), 11);
    assert_eq!(producer.get_max_offset(&queue).await.unwrap(), 22);
    assert_eq!(producer.get_min_offset(&queue).await.unwrap(), 3);
    assert_eq!(
        producer.get_earliest_msg_store_time(&queue).await.unwrap(),
        44
    );

    let route_request = timeout(Duration::from_secs(1), namesrv_requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(route_request.code, request::GET_ROUTEINFO_BY_TOPIC);
    assert_eq!(
        route_request.ext_fields.as_ref().unwrap().get("topic"),
        Some(&"tenant%orders".to_owned())
    );

    for (expected_code, expected_timestamp) in [
        (request::SEARCH_OFFSET_BY_TIMESTAMP, Some("9")),
        (request::GET_MAX_OFFSET, None),
        (request::GET_MIN_OFFSET, None),
        (request::GET_EARLIEST_MSG_STORETIME, None),
    ] {
        let command = loop {
            let command = timeout(Duration::from_secs(1), broker_requests.recv())
                .await
                .unwrap()
                .unwrap();
            if command.code != request::HEART_BEAT {
                break command;
            }
        };
        assert_eq!(command.code, expected_code);
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

    producer.shutdown().await.unwrap();
    namesrv_task.abort();
    broker_task.abort();
}

#[test]
fn vip_topic_creation_uses_the_shared_java_address_transform() {
    assert_eq!(
        broker_vip_channel(true, "127.0.0.1:10911").unwrap(),
        "127.0.0.1:10909"
    );
}
