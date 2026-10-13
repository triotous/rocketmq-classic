use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::BytesMut;
use rocketmq_classic_common::message::{Message, MessageQueue, decode_properties};
use rocketmq_classic_core::{
    runtime::{ClientRuntimeManager, ProducerRuntime},
    trace::{MessageType, TraceBean, TraceContext, TraceType},
};
use rocketmq_classic_producer::{
    Producer, ProducerConfig, ProducerError, RecallMessageHandle, SendStatus, TraceDispatcher,
    TraceProducerKind,
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
};

#[derive(Clone, Debug)]
struct SentRequest {
    broker: String,
    request_code: i32,
    producer_group: Option<String>,
    topic: Option<String>,
    body: Option<Vec<u8>>,
    properties: Option<String>,
    unique_id: Option<String>,
    oneway: bool,
}

async fn read_command(stream: &mut TcpStream) -> Option<RemotingCommand> {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.ok()?;
    let frame_length = u32::from_be_bytes(length) as usize;
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(frame_length + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.ok()?;
    frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
        .ok()
        .flatten()
}

async fn wait_for_attempts(attempts: &AtomicUsize, expected: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while attempts.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("broker peer did not receive the expected request");
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
        ..RemotingCommand::default()
    };
    reply.mark_response();
    stream
        .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
        .await
        .unwrap();
}

fn route_body(brokers: &[(&str, &str)]) -> Vec<u8> {
    let queue_datas = brokers
        .iter()
        .map(|(name, _)| {
            json!({
                "brokerName": name,
                "readQueueNums": 1,
                "writeQueueNums": 1,
                "perm": 6,
                "topicSysFlag": 0
            })
        })
        .collect::<Vec<_>>();
    let broker_datas = brokers
        .iter()
        .map(|(name, address)| {
            json!({
                "cluster": "DefaultCluster",
                "brokerName": name,
                "brokerAddrs": {"0": address},
                "enableActingMaster": false
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({
        "orderTopicConf": null,
        "queueDatas": queue_datas,
        "brokerDatas": broker_datas,
        "filterServerTable": {},
        "topicQueueMappingByBroker": {}
    }))
    .unwrap()
}

async fn name_server_peer(
    route: Vec<u8>,
    missing_topics: BTreeSet<String>,
) -> (String, JoinHandle<()>, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_from_task = requests.clone();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            if command.code == request::GET_ROUTEINFO_BY_TOPIC {
                let topic = command
                    .ext_fields
                    .as_ref()
                    .and_then(|fields| fields.get("topic"))
                    .cloned()
                    .unwrap_or_default();
                requests_from_task.lock().unwrap().push(topic.clone());
                if missing_topics.contains(&topic) {
                    write_response(&mut stream, &command, response::TOPIC_NOT_EXIST, None, None)
                        .await;
                } else {
                    write_response(
                        &mut stream,
                        &command,
                        response::SUCCESS,
                        Some(route.clone()),
                        None,
                    )
                    .await;
                }
            } else if !command.is_oneway() {
                write_response(&mut stream, &command, response::SUCCESS, None, None).await;
            }
        }
    });
    (address, task, requests)
}

async fn broker_peer(
    broker: &'static str,
    global_attempts: Arc<AtomicUsize>,
    fail_first: usize,
    store_then_timeout_first: bool,
    requests: Arc<Mutex<Vec<SentRequest>>>,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            if !matches!(
                command.code,
                request::SEND_MESSAGE_V2 | request::SEND_BATCH_MESSAGE
            ) {
                if !command.is_oneway() {
                    write_response(&mut stream, &command, response::SUCCESS, None, None).await;
                }
                continue;
            }
            let properties = command
                .ext_fields
                .as_ref()
                .and_then(|fields| fields.get("i"))
                .cloned();
            let decoded_properties = properties
                .as_deref()
                .map(decode_properties)
                .unwrap_or_default();
            let unique_id = decoded_properties.get("UNIQ_KEY").cloned();
            let producer_group = command
                .ext_fields
                .as_ref()
                .and_then(|fields| fields.get("a"))
                .cloned();
            let topic = command
                .ext_fields
                .as_ref()
                .and_then(|fields| fields.get("b"))
                .cloned();
            requests.lock().unwrap().push(SentRequest {
                broker: broker.to_owned(),
                request_code: command.code,
                producer_group,
                topic,
                body: command.body.clone(),
                properties,
                unique_id,
                oneway: command.is_oneway(),
            });
            let attempt = global_attempts.fetch_add(1, Ordering::SeqCst);
            if command.is_oneway() {
                continue;
            }
            if store_then_timeout_first && attempt == 0 {
                // Treat receipt of the request as a Broker store, then lose
                // the acknowledgement to exercise the duplicate-delivery case.
                continue;
            }
            if attempt < fail_first {
                write_response(
                    &mut stream,
                    &command,
                    response::SERVICE_NOT_AVAILABLE,
                    None,
                    None,
                )
                .await;
            } else {
                write_response(
                    &mut stream,
                    &command,
                    response::SUCCESS,
                    None,
                    Some(
                        [
                            ("msgId".into(), "offset-id".into()),
                            ("queueId".into(), "0".into()),
                            ("queueOffset".into(), "7".into()),
                            ("MSG_REGION".into(), "DefaultRegion".into()),
                        ]
                        .into(),
                    ),
                )
                .await;
            }
        }
    });
    (address, task)
}

async fn recall_broker_peer() -> (String, JoinHandle<()>, Arc<Mutex<Vec<RemotingCommand>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_from_task = requests.clone();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Some(command) = read_command(&mut stream).await {
            if command.code == request::RECALL_MESSAGE {
                requests_from_task.lock().unwrap().push(command.clone());
                write_response(
                    &mut stream,
                    &command,
                    response::SUCCESS,
                    None,
                    Some([("msgId".into(), "recalled-id".into())].into()),
                )
                .await;
            } else if !command.is_oneway() {
                write_response(&mut stream, &command, response::SUCCESS, None, None).await;
            }
        }
    });
    (address, task, requests)
}

fn producer_config(group: &str, nameserver: &str) -> ProducerConfig {
    let mut config = ProducerConfig::new(group).unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver.to_owned()));
    config
        .client_config_mut()
        .set_instance_name(Some(format!("phase7-{group}")));
    config
}

struct UnknownTransaction;

impl rocketmq_classic_producer::TransactionListener for UnknownTransaction {
    fn execute_local_transaction(
        &self,
        _message: &mut Message,
        _argument: Option<&(dyn std::any::Any + Send + Sync)>,
    ) -> Result<
        Option<rocketmq_classic_producer::LocalTransactionState>,
        rocketmq_classic_producer::TransactionCallbackError,
    > {
        Ok(Some(
            rocketmq_classic_producer::LocalTransactionState::Unknown,
        ))
    }

    fn check_local_transaction(
        &self,
        _message: &mut rocketmq_classic_common::message::MessageExt,
    ) -> Result<
        Option<rocketmq_classic_producer::LocalTransactionState>,
        rocketmq_classic_producer::TransactionCallbackError,
    > {
        Ok(Some(
            rocketmq_classic_producer::LocalTransactionState::Unknown,
        ))
    }
}

#[tokio::test]
async fn transaction_half_send_wraps_topic_and_group_once() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, route_requests) =
        name_server_peer(route, BTreeSet::new()).await;
    let mut config = producer_config("transaction-namespace", &nameserver);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    let producer = rocketmq_classic_producer::TransactionProducer::new(config);
    producer.set_transaction_listener(Some(Arc::new(UnknownTransaction)));
    producer.start().await.unwrap();

    let mut message = Message::new("orders", b"half-message".to_vec());
    producer
        .send_message_in_transaction(&mut message, None)
        .await
        .unwrap();

    assert_eq!(message.topic(), "tenant%orders");
    assert!(
        route_requests
            .lock()
            .unwrap()
            .iter()
            .any(|topic| topic == "tenant%orders")
    );
    let half_send = requests
        .lock()
        .unwrap()
        .iter()
        .find(|request| request.request_code == request::SEND_MESSAGE_V2)
        .cloned()
        .expect("transaction half message reached the Broker");
    assert_eq!(half_send.topic.as_deref(), Some("tenant%orders"));
    assert_eq!(
        half_send.producer_group.as_deref(),
        Some("tenant%transaction-namespace")
    );
    let properties = decode_properties(half_send.properties.as_deref().unwrap_or_default());
    assert_eq!(properties.get("TRAN_MSG").map(String::as_str), Some("true"));
    assert_eq!(
        properties.get("PGROUP").map(String::as_str),
        Some("tenant%transaction-namespace")
    );

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn recall_wraps_topic_and_group_once_at_the_namespace_wire_boundary() {
    let (broker, broker_task, broker_requests) = recall_broker_peer().await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, route_requests) =
        name_server_peer(route, BTreeSet::new()).await;
    let mut config = producer_config("recall-namespace", &nameserver);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();

    let handle = RecallMessageHandle::build_handle("orders", "broker-a", "123", "message-id");
    assert_eq!(
        producer.recall_message("orders", &handle).await.unwrap(),
        "recalled-id"
    );

    assert!(
        route_requests
            .lock()
            .unwrap()
            .iter()
            .any(|topic| topic == "tenant%orders")
    );
    {
        let requests = broker_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let fields = requests[0].ext_fields.as_ref().unwrap();
        assert_eq!(
            fields.get("topic").map(String::as_str),
            Some("tenant%orders")
        );
        assert_eq!(
            fields.get("producerGroup").map(String::as_str),
            Some("tenant%recall-namespace")
        );
        assert_eq!(fields.get("recallHandle"), Some(&handle));
    }

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn trace_flush_waits_for_scripted_broker_delivery() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let config = producer_config("trace-flush-parent", &nameserver);
    let dispatcher = TraceDispatcher::new(
        config.client_config(),
        "trace-flush-parent",
        TraceProducerKind::Produce,
        None,
        ClientRuntimeManager::new(),
        None,
    )
    .unwrap();
    dispatcher.start().await.unwrap();
    assert!(dispatcher.append(TraceContext {
        trace_type: Some(TraceType::Pub),
        timestamp_millis: 1,
        region_id: Some("region-a".into()),
        region_name: Some(String::new()),
        group_name: Some("trace-flush-parent".into()),
        cost_time_millis: 2,
        success: true,
        request_id: None,
        context_code: 0,
        access_channel: None,
        trace_beans: Some(vec![TraceBean {
            topic: Some("orders".into()),
            message_id: Some("message-id".into()),
            body_length: 7,
            message_type: Some(MessageType::Normal),
            ..TraceBean::default()
        }]),
    }));

    dispatcher.flush().await.unwrap();

    let delivered = requests.lock().unwrap().clone();
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].broker, "broker-a");
    let body = String::from_utf8(delivered[0].body.clone().unwrap()).unwrap();
    assert!(body.contains("Pub\u{1}"));
    assert!(body.contains("message-id"));
    dispatcher.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn trace_shutdown_waits_for_scripted_broker_delivery() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let mut config = producer_config("trace-shutdown-parent", &nameserver);
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    config
        .client_config_mut()
        .set_namespace_v2(Some("tenant-v2".into()));
    let dispatcher = TraceDispatcher::new(
        config.client_config(),
        "tenant%trace-shutdown-parent",
        TraceProducerKind::Produce,
        Some("custom-trace"),
        ClientRuntimeManager::new(),
        None,
    )
    .unwrap();
    dispatcher.start().await.unwrap();
    assert!(dispatcher.append(TraceContext {
        trace_type: Some(TraceType::Pub),
        timestamp_millis: 1,
        region_id: Some("region-a".into()),
        region_name: Some(String::new()),
        group_name: Some("trace-shutdown-parent".into()),
        cost_time_millis: 2,
        success: true,
        request_id: None,
        context_code: 0,
        access_channel: None,
        trace_beans: Some(vec![TraceBean {
            topic: Some("orders".into()),
            message_id: Some("message-id".into()),
            body_length: 7,
            message_type: Some(MessageType::Normal),
            ..TraceBean::default()
        }]),
    }));

    dispatcher.shutdown().await.unwrap();

    let delivered = requests.lock().unwrap().clone();
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].broker, "broker-a");
    assert_eq!(delivered[0].topic.as_deref(), Some("custom-trace"));
    assert!(delivered[0].producer_group.as_deref().is_some_and(|group| {
        group.starts_with("_INNER_TRACE_PRODUCER-tenant%trace-shutdown-parent-PRODUCE-")
    }));
    let body = String::from_utf8(delivered[0].body.clone().unwrap()).unwrap();
    assert!(body.contains("Pub\u{1}"));
    assert!(body.contains("message-id"));
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn dropping_trace_dispatcher_flushes_queued_data() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let config = producer_config("trace-drop-parent", &nameserver);
    let dispatcher = TraceDispatcher::new(
        config.client_config(),
        "trace-drop-parent",
        TraceProducerKind::Produce,
        Some("custom-trace"),
        ClientRuntimeManager::new(),
        None,
    )
    .unwrap();
    dispatcher.start().await.unwrap();
    assert!(dispatcher.append(TraceContext {
        trace_type: Some(TraceType::Pub),
        timestamp_millis: 1,
        region_id: Some("region-a".into()),
        region_name: Some(String::new()),
        group_name: Some("trace-drop-parent".into()),
        cost_time_millis: 2,
        success: true,
        request_id: None,
        context_code: 0,
        access_channel: None,
        trace_beans: Some(vec![TraceBean {
            topic: Some("orders".into()),
            message_id: Some("message-id".into()),
            body_length: 7,
            message_type: Some(MessageType::Normal),
            ..TraceBean::default()
        }]),
    }));

    drop(dispatcher);
    wait_for_attempts(&attempts, 1).await;

    let delivered = requests.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].broker, "broker-a");
    assert_eq!(delivered[0].topic.as_deref(), Some("custom-trace"));
    let body = String::from_utf8(delivered[0].body.clone().unwrap()).unwrap();
    assert!(body.contains("Pub\u{1}"));
    assert!(body.contains("message-id"));
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn sync_send_retries_across_two_brokers_and_preserves_one_prepared_message() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker_a, broker_a_task) =
        broker_peer("broker-a", attempts.clone(), 2, false, requests.clone()).await;
    let (broker_b, broker_b_task) =
        broker_peer("broker-b", attempts.clone(), 2, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker_a), ("broker-b", &broker_b)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let producer = Arc::new(Producer::new(producer_config("sync-retry", &nameserver)));
    producer.start().await.unwrap();

    let message = Message::new("orders", b"immutable-body".to_vec());
    let result = producer.send(&message).await.unwrap();
    assert_eq!(result.status, SendStatus::SendOk);
    assert_eq!(result.offset_msg_id, "offset-id");
    assert_eq!(result.queue_offset, 7);
    assert!(matches!(result.msg_id.len(), 32 | 56));
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(message.body(), b"immutable-body");
    assert!(!message.properties().contains_key("UNIQ_KEY"));

    let sent = requests.lock().unwrap().clone();
    assert_eq!(sent.len(), 3);
    assert_eq!(
        sent.iter()
            .map(|request| request.unique_id.as_deref())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([Some(result.msg_id.as_str())])
    );
    assert!(
        sent.iter()
            .all(|request| request.body.as_deref() == Some(&b"immutable-body"[..]))
    );
    assert!(
        sent.iter()
            .all(|request| request.request_code == request::SEND_MESSAGE_V2)
    );
    assert!(sent.iter().any(|request| request.broker == "broker-a"));
    assert!(sent.iter().any(|request| request.broker == "broker-b"));

    assert!(matches!(
        producer.start().await,
        Err(ProducerError::AlreadyStarted)
    ));
    producer.shutdown().await.unwrap();
    producer.shutdown().await.unwrap();
    assert!(matches!(
        producer.send(&message).await,
        Err(ProducerError::Shutdown)
    ));
    nameserver_task.abort();
    broker_a_task.abort();
    broker_b_task.abort();
}

#[tokio::test]
async fn default_topic_route_fallback_and_async_mode_response_policy_match_java() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 1, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, route_requests) =
        name_server_peer(route, BTreeSet::from(["orders".to_owned()])).await;
    let mut config = producer_config("future-policy", &nameserver);
    config.set_retry_times_when_send_async_failed(0);
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();

    let message = Message::new("orders", b"future-body".to_vec());
    let result = producer.send_future(&message).await;
    assert!(matches!(result, Err(ProducerError::Rpc(_))));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(
        route_requests
            .lock()
            .unwrap()
            .contains(&"TBW102".to_owned())
    );
    assert!(requests.lock().unwrap().iter().all(|request| {
        request
            .unique_id
            .as_deref()
            .is_some_and(|id| matches!(id.len(), 32 | 56))
    }));

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn topic_without_route_stays_in_producer_refresh_inventory() {
    let route = route_body(&[]);
    let missing_topics = BTreeSet::from(["orders".to_owned(), "TBW102".to_owned()]);
    let (nameserver, nameserver_task, route_requests) =
        name_server_peer(route, missing_topics).await;
    let producer = Arc::new(Producer::new(producer_config("missing-route", &nameserver)));
    producer.start().await.unwrap();

    assert!(matches!(
        producer
            .send(&Message::new("orders", b"no-route".to_vec()))
            .await,
        Err(ProducerError::NoRoute(_))
    ));
    assert!(
        route_requests
            .lock()
            .unwrap()
            .contains(&"orders".to_owned())
    );
    assert!(
        route_requests
            .lock()
            .unwrap()
            .contains(&"TBW102".to_owned())
    );
    assert!(producer.publish_topics().contains("orders"));

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
}

#[tokio::test]
async fn oneway_sends_once_and_custom_queue_membership_is_checked_before_io() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let producer = Arc::new(Producer::new(producer_config("oneway", &nameserver)));
    producer.start().await.unwrap();

    let message = Message::new("orders", b"oneway-body".to_vec());
    let invalid_queue = MessageQueue::new("orders", "not-in-route", 0);
    assert!(matches!(
        producer.send_to_queue(&message, &invalid_queue).await,
        Err(ProducerError::QueueNotInRoute)
    ));
    assert!(requests.lock().unwrap().is_empty());

    producer.send_oneway(&message).await.unwrap();
    wait_for_attempts(&attempts, 1).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert!(requests.lock().unwrap()[0].oneway);

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn public_queue_selector_and_batch_modes_use_the_scripted_route() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker, broker_task) =
        broker_peer("broker-a", attempts.clone(), 0, false, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let producer = Arc::new(Producer::new(producer_config(
        "public-send-modes",
        &nameserver,
    )));
    producer.start().await.unwrap();

    let queues = producer
        .fetch_publish_message_queues("orders")
        .await
        .unwrap();
    assert_eq!(queues.len(), 1);
    let queue = queues[0].clone();
    let message = Message::new("orders", b"single-mode".to_vec());

    producer.send_to_queue(&message, &queue).await.unwrap();
    producer
        .send_to_queue_future_with_timeout(&message, &queue, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    producer
        .send_to_queue_oneway(&message, &queue)
        .await
        .unwrap();
    producer
        .send_with_selector(&message, |queues, _| queues.first().cloned())
        .await
        .unwrap();
    producer
        .send_future_with_selector_with_timeout(
            &message,
            |queues, _| queues.first().cloned(),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
    producer
        .send_oneway_with_selector(&message, |queues, _| queues.first().cloned())
        .await
        .unwrap();

    let batch = [
        Message::new("orders", b"batch-one".to_vec()),
        Message::new("orders", b"batch-two".to_vec()),
    ];
    producer
        .send_batch_with_timeout(&batch, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    producer.send_batch_future(&batch).await.unwrap();
    producer.send_batch_to_queue(&batch, &queue).await.unwrap();
    producer
        .send_batch_to_queue_future_with_timeout(&batch, &queue, std::time::Duration::from_secs(1))
        .await
        .unwrap();

    let sent = requests.lock().unwrap().clone();
    assert_eq!(sent.len(), 10);
    assert_eq!(
        sent.iter()
            .filter(|request| request.request_code == request::SEND_BATCH_MESSAGE)
            .count(),
        4
    );
    assert_eq!(sent.iter().filter(|request| request.oneway).count(), 2);
    assert!(sent.iter().all(|request| request.body.is_some()));

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_task.abort();
}

#[tokio::test]
async fn stored_message_with_lost_ack_can_be_retried_and_duplicated() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (broker_a, broker_a_task) =
        broker_peer("broker-a", attempts.clone(), 0, true, requests.clone()).await;
    let (broker_b, broker_b_task) =
        broker_peer("broker-b", attempts.clone(), 0, true, requests.clone()).await;
    let route = route_body(&[("broker-a", &broker_a), ("broker-b", &broker_b)]);
    let (nameserver, nameserver_task, _) = name_server_peer(route, BTreeSet::new()).await;
    let mut config = producer_config("stored-timeout", &nameserver);
    config.set_send_timeout_millis(500);
    config.set_send_msg_max_timeout_per_request(20);
    let producer = Arc::new(Producer::new(config));
    producer.start().await.unwrap();

    let message = Message::new("orders", b"store-before-timeout".to_vec());
    let result = producer.send(&message).await.unwrap();
    assert_eq!(result.status, SendStatus::SendOk);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let sent = requests.lock().unwrap().clone();
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|request| {
        request.body.as_deref() == Some(&b"store-before-timeout"[..])
            && request.unique_id == sent[0].unique_id
    }));

    producer.shutdown().await.unwrap();
    nameserver_task.abort();
    broker_a_task.abort();
    broker_b_task.abort();
}
