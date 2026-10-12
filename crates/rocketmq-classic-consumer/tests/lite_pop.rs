use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use base64::Engine;
use bytes::BytesMut;
use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_consumer::push::PushConsumerConfig;
use rocketmq_classic_consumer::{
    LitePullConsumer, LitePullConsumerConfig, LitePullConsumerError,
    offset::{OffsetRead, OffsetStore, OffsetStoreFuture, ReadOffsetType},
};
use rocketmq_classic_protocol::codes::{request, response};
use rocketmq_classic_protocol::header::HeaderMap;
use rocketmq_classic_protocol::heartbeat::{ConsumeFromWhere, MessageModel};
use rocketmq_classic_remoting::{
    RemotingCommand,
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
};
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot},
    time::timeout,
};

#[derive(Default)]
struct LiteScriptedOffsetStore {
    memory: Mutex<HashMap<MessageQueue, i64>>,
    persisted: Mutex<HashMap<MessageQueue, i64>>,
}

impl OffsetStore for LiteScriptedOffsetStore {
    fn load(&self) -> OffsetStoreFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn update_offset(&self, queue: &MessageQueue, offset: i64, _increase_only: bool) {
        self.memory.lock().unwrap().insert(queue.clone(), offset);
    }

    fn update_and_freeze_offset(&self, queue: &MessageQueue, offset: i64) {
        self.update_offset(queue, offset, false);
    }

    fn read_offset<'a>(
        &'a self,
        queue: &'a MessageQueue,
        mode: ReadOffsetType,
    ) -> OffsetStoreFuture<'a, OffsetRead> {
        Box::pin(async move {
            let memory = self.memory.lock().unwrap().get(queue).copied();
            let persisted = self.persisted.lock().unwrap().get(queue).copied();
            Ok(match mode {
                ReadOffsetType::ReadFromMemory => memory,
                ReadOffsetType::ReadFromStore => persisted,
                ReadOffsetType::MemoryFirstThenStore => memory.or(persisted),
            }
            .map_or(OffsetRead::NotFound, OffsetRead::Found))
        })
    }

    fn persist<'a>(&'a self, queue: &'a MessageQueue) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async move {
            if let Some(offset) = self.memory.lock().unwrap().get(queue).copied() {
                self.persisted.lock().unwrap().insert(queue.clone(), offset);
            }
            Ok(())
        })
    }

    fn persist_all<'a>(&'a self, queues: &'a [MessageQueue]) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async move {
            let memory = self.memory.lock().unwrap();
            let mut persisted = self.persisted.lock().unwrap();
            for queue in queues {
                if let Some(offset) = memory.get(queue) {
                    persisted.insert(queue.clone(), *offset);
                }
            }
            Ok(())
        })
    }

    fn remove_offset(&self, queue: &MessageQueue) {
        self.memory.lock().unwrap().remove(queue);
    }

    fn clone_offset_table(&self, topic: Option<&str>) -> HashMap<MessageQueue, i64> {
        self.memory
            .lock()
            .unwrap()
            .iter()
            .filter(|(queue, _)| topic.is_none_or(|topic| queue.topic() == Some(topic)))
            .map(|(queue, offset)| (queue.clone(), *offset))
            .collect()
    }

    fn update_consume_offset_to_broker<'a>(
        &'a self,
        _queue: &'a MessageQueue,
        _offset: i64,
        _oneway: bool,
    ) -> OffsetStoreFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct ScriptedBrokerRequest {
    command: RemotingCommand,
    reply: oneshot::Sender<ScriptedReply>,
}

#[derive(Debug)]
struct ScriptedReply {
    code: i32,
    fields: Option<HeaderMap>,
    body: Option<Vec<u8>>,
}

async fn read_command<R: AsyncRead + Unpin>(stream: &mut R) -> Option<RemotingCommand> {
    let mut length = [0; 4];
    stream.read_exact(&mut length).await.ok()?;
    let mut bytes = BytesMut::from(&length[..]);
    bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.ok()?;
    frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE).ok()?
}

async fn write_reply<W: AsyncWrite + Unpin>(stream: &mut W, opaque: i32, reply: ScriptedReply) {
    let mut response_command = RemotingCommand {
        code: reply.code,
        opaque,
        ext_fields: reply.fields,
        body: reply.body,
        ..Default::default()
    };
    response_command.mark_response();
    stream
        .write_all(&frame::encode(&response_command, DEFAULT_MAX_FRAME_SIZE).unwrap())
        .await
        .unwrap();
}

fn broker_route(address: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "queueDatas": [{
            "brokerName": "broker-a",
            "readQueueNums": 1,
            "writeQueueNums": 1,
            "perm": 6,
            "topicSysFlag": 0
        }],
        "brokerDatas": [{
            "cluster": "cluster",
            "brokerName": "broker-a",
            "brokerAddrs": {"0": address}
        }],
        "filterServerTable": {}
    }))
    .unwrap()
}

async fn start_scripted_nameserver(route: Vec<u8>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let route = route.clone();
            tokio::spawn(async move {
                while let Some(command) = read_command(&mut stream).await {
                    let body =
                        (command.code == request::GET_ROUTEINFO_BY_TOPIC).then(|| route.clone());
                    write_reply(
                        &mut stream,
                        command.opaque,
                        ScriptedReply {
                            code: response::SUCCESS,
                            fields: None,
                            body,
                        },
                    )
                    .await;
                }
            });
        }
    });
    (address, task)
}

fn start_scripted_broker(
    listener: TcpListener,
) -> (
    mpsc::UnboundedReceiver<ScriptedBrokerRequest>,
    tokio::task::JoinHandle<()>,
) {
    let (request_tx, request_rx) = mpsc::unbounded_channel::<ScriptedBrokerRequest>();
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let request_tx = request_tx.clone();
            tokio::spawn(async move {
                let (mut reader, writer) = stream.into_split();
                let writer = Arc::new(tokio::sync::Mutex::new(writer));
                while let Some(command) = read_command(&mut reader).await {
                    if matches!(
                        command.code,
                        request::HEART_BEAT
                            | request::UNREGISTER_CLIENT
                            | request::CHECK_CLIENT_CONFIG
                    ) {
                        let writer = writer.clone();
                        let opaque = command.opaque;
                        tokio::spawn(async move {
                            write_reply(
                                &mut *writer.lock().await,
                                opaque,
                                ScriptedReply {
                                    code: response::SUCCESS,
                                    fields: None,
                                    body: None,
                                },
                            )
                            .await;
                        });
                        continue;
                    }
                    let (reply_tx, reply_rx) = oneshot::channel();
                    if request_tx
                        .send(ScriptedBrokerRequest {
                            command: command.clone(),
                            reply: reply_tx,
                        })
                        .is_err()
                    {
                        return;
                    }
                    let writer = writer.clone();
                    let opaque = command.opaque;
                    tokio::spawn(async move {
                        if let Ok(reply) = reply_rx.await {
                            write_reply(&mut *writer.lock().await, opaque, reply).await;
                        }
                    });
                }
            });
        }
    });
    (request_rx, task)
}

async fn next_request(
    receiver: &mut mpsc::UnboundedReceiver<ScriptedBrokerRequest>,
    code: i32,
) -> ScriptedBrokerRequest {
    loop {
        let request = timeout(Duration::from_secs(4), receiver.recv())
            .await
            .expect("scripted broker timed out waiting for request")
            .expect("scripted broker request stream closed");
        assert_eq!(request.command.code, code);
        return request;
    }
}

fn reply_offset(request: ScriptedBrokerRequest, code: i32, name: &str, offset: i64) {
    request
        .reply
        .send(ScriptedReply {
            code,
            fields: Some(HeaderMap::from([(name.into(), offset.to_string())])),
            body: None,
        })
        .unwrap();
}

fn pull_reply(request: ScriptedBrokerRequest, code: i32, offset: i64, message: bool) {
    let fields = HeaderMap::from([
        ("suggestWhichBrokerId".into(), "0".into()),
        ("nextBeginOffset".into(), offset.to_string()),
        ("minOffset".into(), "0".into()),
        ("maxOffset".into(), "30".into()),
    ]);
    let body = message.then(|| {
        let fixture: Value =
            serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
        let mut bytes = base64::engine::general_purpose::STANDARD
            .decode(
                fixture["cases"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|case| case["name"] == "ipv4-v1")
                    .unwrap()["recordBase64"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap();
        bytes[20..28].copy_from_slice(&(offset - 1).to_be_bytes());
        bytes
    });
    request
        .reply
        .send(ScriptedReply {
            code,
            fields: Some(fields),
            body,
        })
        .unwrap();
}

fn lite_config_with_store(
    group: &str,
    instance: &str,
    nameserver: &str,
    store: Arc<LiteScriptedOffsetStore>,
) -> LitePullConsumerConfig {
    let mut config = LitePullConsumerConfig::new(group).unwrap();
    config
        .client_config_mut()
        .set_namesrv_addr(Some(nameserver.to_owned()));
    config
        .client_config_mut()
        .set_instance_name(Some(instance.to_owned()));
    config
        .client_config_mut()
        .set_poll_name_server_interval(60_000);
    config
        .client_config_mut()
        .set_heartbeat_broker_interval(60_000);
    config
        .client_config_mut()
        .set_persist_consumer_offset_interval(60_000);
    config.set_auto_commit(false);
    config
        .set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .unwrap();
    config.set_offset_store(Some(store));
    config
}

fn java_fixture() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/consumer/lite-pop.json");
    serde_json::from_slice(&std::fs::read(path).expect("pinned Java Lite/POP fixture"))
        .expect("valid pinned Java Lite/POP fixture")
}

#[tokio::test]
async fn scripted_lite_seek_pause_resume_commit_and_restart_preserve_delivery_offsets() {
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker_listener.local_addr().unwrap().to_string();
    let route = broker_route(&broker_address);
    let (nameserver, nameserver_task) = start_scripted_nameserver(route).await;
    let (mut requests, broker_task) = start_scripted_broker(broker_listener);
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let store = Arc::new(LiteScriptedOffsetStore::default());

    let first = Arc::new(
        LitePullConsumer::new(lite_config_with_store(
            "lite-scripted-flow",
            "lite-scripted-client-1",
            &nameserver,
            store.clone(),
        ))
        .unwrap(),
    );
    first.assign([queue.clone()]).unwrap();
    first.start().await.unwrap();

    let original_pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        original_pull.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "0"
    );

    let seek_consumer = first.clone();
    let seek_queue = queue.clone();
    let invalid_seek = tokio::spawn(async move { seek_consumer.seek(&seek_queue, 31).await });
    let minimum = next_request(&mut requests, request::GET_MIN_OFFSET).await;
    reply_offset(minimum, response::SUCCESS, "offset", 0);
    let maximum = next_request(&mut requests, request::GET_MAX_OFFSET).await;
    reply_offset(maximum, response::SUCCESS, "offset", 30);
    assert!(matches!(
        invalid_seek.await.unwrap(),
        Err(LitePullConsumerError::InvalidSeekOffset {
            offset: 31,
            minimum: 0,
            maximum: 30
        })
    ));

    let seek_consumer = first.clone();
    let seek_queue = queue.clone();
    let valid_seek = tokio::spawn(async move { seek_consumer.seek(&seek_queue, 10).await });
    let minimum = next_request(&mut requests, request::GET_MIN_OFFSET).await;
    reply_offset(minimum, response::SUCCESS, "offset", 0);
    let maximum = next_request(&mut requests, request::GET_MAX_OFFSET).await;
    reply_offset(maximum, response::SUCCESS, "offset", 30);
    valid_seek.await.unwrap().unwrap();

    // The original pull reply arrives after seek invalidated its generation.
    pull_reply(original_pull, response::SUCCESS, 2, true);
    let pulled_after_seek = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        pulled_after_seek.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "10"
    );
    pull_reply(pulled_after_seek, response::SUCCESS, 11, true);

    let delivered = first.poll(Some(3_000)).await.unwrap();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].queue_offset(), 10);

    let pending_pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        pending_pull.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "11"
    );
    first.pause([queue.clone()]);
    pull_reply(pending_pull, response::PULL_NOT_FOUND, 11, false);
    assert!(
        timeout(Duration::from_millis(150), requests.recv())
            .await
            .is_err()
    );

    first
        .commit_offsets([(queue.clone(), 11)], true)
        .await
        .unwrap();
    first.commit_queues([queue.clone()], true).await.unwrap();
    assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&11));

    first.resume([queue.clone()]);
    let resumed_pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        resumed_pull.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "11"
    );
    first.pause([queue.clone()]);
    pull_reply(resumed_pull, response::PULL_NOT_FOUND, 11, false);
    first.shutdown().await.unwrap();

    let restarted = Arc::new(
        LitePullConsumer::new(lite_config_with_store(
            "lite-scripted-flow",
            "lite-scripted-client-2",
            &nameserver,
            store.clone(),
        ))
        .unwrap(),
    );
    restarted.assign([queue.clone()]).unwrap();
    restarted.start().await.unwrap();
    let restarted_pull = next_request(&mut requests, request::LITE_PULL_MESSAGE).await;
    assert_eq!(
        restarted_pull.command.ext_fields.as_ref().unwrap()["queueOffset"],
        "11"
    );
    restarted.pause([queue]);
    pull_reply(restarted_pull, response::PULL_NOT_FOUND, 11, false);
    restarted.shutdown().await.unwrap();

    nameserver_task.abort();
    broker_task.abort();
}

#[test]
fn pinned_java_lite_pull_surface_defaults_and_setter_rules_are_recorded() {
    let fixture = java_fixture();
    assert_eq!(
        fixture["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );
    let defaults = &fixture["litePullDefaults"];
    assert_eq!(defaults["noArgumentConsumerGroup"], "DEFAULT_CONSUMER");
    assert_eq!(defaults["messageModel"], "CLUSTERING");
    assert_eq!(defaults["autoCommit"], true);
    assert_eq!(defaults["pullThreadNums"], 20);
    assert_eq!(defaults["autoCommitIntervalMillis"], 5_000);
    assert_eq!(defaults["pullBatchSize"], 10);
    assert_eq!(defaults["pullThresholdForAll"], 10_000);
    assert_eq!(defaults["pullThresholdForQueue"], 1_000);
    assert_eq!(defaults["pullThresholdSizeForQueue"], 100);
    assert_eq!(defaults["consumeMaxSpan"], 2_000);
    assert_eq!(defaults["pollTimeoutMillis"], 5_000);
    assert_eq!(defaults["topicMetadataCheckIntervalMillis"], 30_000);
    assert_eq!(defaults["consumeFromWhere"], "CONSUME_FROM_LAST_OFFSET");
    assert_eq!(defaults["consumeTimestampThirtyMinutesAgo"], true);
    assert_eq!(
        fixture["litePullConfigured"]["belowMinimumAutoCommitIntervalIgnored"],
        true
    );
    assert_eq!(
        fixture["litePullConfigured"]["autoCommitIntervalMillis"],
        2_000
    );

    let api = fixture["litePullApi"]
        .as_array()
        .expect("LitePullConsumer methods");
    for method in [
        "buildSubscriptionsForHeartbeat",
        "subscribe(",
        "unsubscribe(",
        "assign(",
        "poll(",
        "seek(",
        "pause(",
        "resume(",
        "commit(",
        "commitSync(",
        "assignment(",
        "committed(",
        "offsetForTimestamp(",
        "fetchMessageQueues(",
        "registerTopicMessageQueueChangeListener(",
        "updateNameServerAddress(",
    ] {
        assert!(
            api.iter()
                .any(|signature| signature.as_str().unwrap().contains(method)),
            "missing Java LitePullConsumer API {method}"
        );
    }
    let config_api = fixture["liteConfigApi"]
        .as_array()
        .expect("Lite config methods");
    for method in [
        "getSubscriptionsForHeartbeat",
        "setConnectBrokerByUser",
        "setDefaultBrokerId",
        "setPullBatchSize",
        "setAutoCommitIntervalMillis",
        "setConsumeFromWhere",
        "setOffsetStore",
        "setEnableMsgTrace",
        "setCustomizedTraceTopic",
    ] {
        assert!(
            config_api
                .iter()
                .any(|signature| signature.as_str().unwrap().contains(method)),
            "missing Java DefaultLitePullConsumer setting API {method}"
        );
    }
    assert_eq!(
        fixture["heartbeatSubscriptions"]["heartbeat-tag"]["expressionType"],
        "TAG"
    );
    assert_eq!(
        fixture["heartbeatSubscriptions"]["heartbeat-tag"]["tagCount"],
        2
    );
    assert_eq!(
        fixture["heartbeatSubscriptions"]["heartbeat-sql"]["expressionType"],
        "SQL92"
    );
    assert_eq!(
        fixture["invalidInputs"]["emptyAssignment"],
        "IllegalArgumentException"
    );
    assert_eq!(
        fixture["invalidInputs"]["subscribeThenAssign"],
        "IllegalStateException"
    );
    assert_eq!(
        fixture["invalidInputs"]["assignThenSubscribe"],
        "MQClientException/IllegalStateException"
    );
    assert_eq!(
        fixture["invalidInputs"]["suspendTimeout"],
        "MQClientException"
    );
    assert_eq!(
        fixture["invalidInputs"]["consumeFromWhere"],
        "RuntimeException"
    );
}

#[test]
fn pinned_java_pop_settings_result_status_and_receipt_fields_are_recorded() {
    let fixture = java_fixture();
    assert_eq!(fixture["pushPopDefaults"]["popThresholdForQueue"], 96);
    assert_eq!(fixture["pushPopDefaults"]["popInvisibleTime"], 60_000);
    assert_eq!(fixture["pushPopDefaults"]["popBatchNums"], 32);
    assert_eq!(fixture["popResultDefaults"]["status"], "FOUND");
    assert_eq!(fixture["popResultDefaults"]["popTime"], 0);
    assert_eq!(
        fixture["popResult"]["statusNames"],
        serde_json::json!(["FOUND", "NO_NEW_MSG", "POLLING_FULL", "POLLING_NOT_FOUND"])
    );
    assert_eq!(fixture["receipt"]["raw"], "17 123 456 2 1 broker-a 4 19");
    assert_eq!(fixture["receipt"]["checkpointQueueOffset"], 17);
    assert_eq!(fixture["receipt"]["popTime"], 123);
    assert_eq!(fixture["receipt"]["invisibleTime"], 456);
    assert_eq!(fixture["receipt"]["reviveQueueId"], 2);
    assert_eq!(fixture["receipt"]["brokerName"], "broker-a");
    assert_eq!(fixture["receipt"]["queueId"], 4);
    assert_eq!(fixture["receipt"]["queueOffset"], 19);
    assert_eq!(
        fixture["invalidReceipts"]["missingFields"],
        "IllegalArgumentException"
    );
    assert_eq!(
        fixture["invalidReceipts"]["invalidQueueOffset"],
        "NumberFormatException"
    );
}

#[test]
fn lite_config_defaults_and_mode_exclusivity_match_java() {
    let config = LitePullConsumerConfig::new("lite-workers").unwrap();
    assert!(config.auto_commit());
    assert_eq!(config.pull_thread_nums(), 20);
    assert_eq!(config.auto_commit_interval_millis(), 5_000);
    assert_eq!(config.pull_batch_size(), 10);
    assert_eq!(config.pull_threshold_for_all(), 10_000);
    assert_eq!(config.pull_threshold_for_queue(), 1_000);
    assert_eq!(config.pull_threshold_size_for_queue(), 100);
    assert_eq!(config.consume_max_span(), 2_000);
    assert_eq!(config.poll_timeout_millis(), 5_000);
    assert_eq!(config.topic_metadata_check_interval_millis(), 30_000);
    assert_eq!(config.broker_suspend_max_time_millis(), 20_000);
    assert_eq!(config.consumer_timeout_millis_when_suspend(), 30_000);
    assert_eq!(config.consumer_pull_timeout_millis(), 10_000);
    assert_eq!(config.message_model(), MessageModel::Clustering);
    assert_eq!(
        config.consume_from_where(),
        ConsumeFromWhere::ConsumeFromLastOffset
    );
    assert!(!config.enable_msg_trace());
    assert!(config.consume_timestamp().len() >= 14);

    let consumer = LitePullConsumer::new(config).unwrap();
    consumer.subscribe_with_expression("orders", "*").unwrap();
    assert!(
        consumer
            .assign(vec![MessageQueue::new("orders", "broker-a", 0)])
            .is_err()
    );

    let config = LitePullConsumerConfig::new("lite-assigned").unwrap();
    let consumer = LitePullConsumer::new(config).unwrap();
    consumer
        .assign(vec![MessageQueue::new("orders", "broker-a", 0)])
        .unwrap();
    assert!(consumer.subscribe_with_expression("orders", "*").is_err());
}

#[tokio::test]
async fn lite_default_constructor_and_consume_origin_validation_match_java() {
    let config = LitePullConsumerConfig::new_default().unwrap();
    assert_eq!(config.consumer_group(), "DEFAULT_CONSUMER");
    let consumer = std::sync::Arc::new(LitePullConsumer::new(config).unwrap());
    assert!(matches!(
        consumer.start().await,
        Err(LitePullConsumerError::Pull(
            rocketmq_classic_consumer::PullConsumerError::DefaultConsumerGroup
        ))
    ));

    let mut config = LitePullConsumerConfig::new("lite-invalid-consume-origin").unwrap();
    assert!(matches!(
        config.set_consume_from_where(ConsumeFromWhere::ConsumeFromMinOffset),
        Err(LitePullConsumerError::InvalidConsumeFromWhere(
            ConsumeFromWhere::ConsumeFromMinOffset
        ))
    ));
    assert!(matches!(
        config.set_consume_from_where(ConsumeFromWhere::ConsumeFromMaxOffset),
        Err(LitePullConsumerError::InvalidConsumeFromWhere(
            ConsumeFromWhere::ConsumeFromMaxOffset
        ))
    ));
    assert!(matches!(
        config
            .set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst),
        Err(LitePullConsumerError::InvalidConsumeFromWhere(
            ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
        ))
    ));
    config
        .set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .unwrap();
    assert_eq!(
        config.consume_from_where(),
        ConsumeFromWhere::ConsumeFromFirstOffset
    );
}

#[tokio::test]
async fn explicit_lite_assignment_returns_the_callers_queue_topic_with_namespace() {
    let mut config = LitePullConsumerConfig::new("lite-namespaced-assignment").unwrap();
    config
        .client_config_mut()
        .set_namespace(Some("tenant".to_owned()));
    let consumer = LitePullConsumer::new(config).unwrap();
    let queue = MessageQueue::new("orders", "broker-a", 0);
    consumer.assign([queue.clone()]).unwrap();

    assert_eq!(
        consumer.assignment().await.unwrap(),
        [queue].into_iter().collect()
    );
}

#[test]
fn push_pop_settings_match_pinned_java_defaults_and_setters() {
    let fixture = java_fixture();
    let defaults = &fixture["pushPopDefaults"];
    let mut config = PushConsumerConfig::new("pop-workers").unwrap();
    assert_eq!(
        config.pop_threshold_for_queue(),
        defaults["popThresholdForQueue"]
    );
    assert_eq!(
        config.pop_invisible_time_millis(),
        defaults["popInvisibleTime"]
    );
    assert_eq!(config.pop_batch_nums(), defaults["popBatchNums"]);

    let configured = &fixture["pushPopConfigured"];
    config.set_pop_threshold_for_queue(configured["popThresholdForQueue"].as_i64().unwrap() as i32);
    config.set_pop_invisible_time_millis(configured["popInvisibleTime"].as_i64().unwrap());
    config.set_pop_batch_nums(configured["popBatchNums"].as_i64().unwrap() as i32);
    assert_eq!(
        config.pop_threshold_for_queue(),
        configured["popThresholdForQueue"]
    );
    assert_eq!(
        config.pop_invisible_time_millis(),
        configured["popInvisibleTime"]
    );
    assert_eq!(config.pop_batch_nums(), configured["popBatchNums"]);
}

#[test]
fn lite_auto_commit_interval_below_java_minimum_is_ignored() {
    let mut config = LitePullConsumerConfig::new("lite-commit").unwrap();
    config.set_auto_commit_interval_millis(999);
    assert_eq!(config.auto_commit_interval_millis(), 5_000);
    config.set_auto_commit_interval_millis(2_000);
    assert_eq!(config.auto_commit_interval_millis(), 2_000);
}

#[test]
fn lite_configured_values_match_the_pinned_java_consumer_settings() {
    let fixture = java_fixture();
    let configured = &fixture["litePullConfigured"];
    let mut config = LitePullConsumerConfig::new("lite-configured-group").unwrap();
    config.set_auto_commit(configured["autoCommit"].as_bool().unwrap());
    config
        .set_auto_commit_interval_millis(configured["autoCommitIntervalMillis"].as_i64().unwrap());
    config
        .set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .unwrap();
    config.set_consume_max_span(configured["consumeMaxSpan"].as_i64().unwrap() as i32);
    config.set_consume_timestamp(configured["consumeTimestamp"].as_str().unwrap());
    config.set_consumer_pull_timeout_millis(
        configured["consumerPullTimeoutMillis"].as_i64().unwrap(),
    );
    config.set_consumer_timeout_millis_when_suspend(
        configured["consumerTimeoutMillisWhenSuspend"]
            .as_i64()
            .unwrap(),
    );
    config.set_customized_trace_topic(Some(
        configured["customizedTraceTopic"]
            .as_str()
            .unwrap()
            .to_owned(),
    ));
    config.set_default_broker_id(17);
    config.set_enable_msg_trace(configured["enableMessageTrace"].as_bool().unwrap());
    config.set_message_model(MessageModel::Broadcasting);
    config.set_poll_timeout_millis(configured["pollTimeoutMillis"].as_i64().unwrap());
    config.set_pull_batch_size(configured["pullBatchSize"].as_i64().unwrap() as i32);
    config.set_pull_thread_nums(configured["pullThreadNums"].as_i64().unwrap() as i32);
    config.set_pull_threshold_for_all(configured["pullThresholdForAll"].as_i64().unwrap());
    config
        .set_pull_threshold_for_queue(configured["pullThresholdForQueue"].as_i64().unwrap() as i32);
    config.set_pull_threshold_size_for_queue(
        configured["pullThresholdSizeForQueue"].as_i64().unwrap() as i32,
    );
    config.set_topic_metadata_check_interval_millis(
        configured["topicMetadataCheckIntervalMillis"]
            .as_i64()
            .unwrap(),
    );
    config.set_unit_mode(configured["unitMode"].as_bool().unwrap());
    config.set_connect_broker_by_user(true);
    config.set_message_queue_listener(Some(std::sync::Arc::new(|_, _, _| {})));

    assert_eq!(config.consumer_group(), "lite-configured-group");
    assert!(!config.auto_commit());
    assert_eq!(config.auto_commit_interval_millis(), 2_000);
    assert_eq!(
        config.consume_from_where(),
        ConsumeFromWhere::ConsumeFromFirstOffset
    );
    assert_eq!(config.consume_max_span(), 29);
    assert_eq!(config.consume_timestamp(), "20000101000000");
    assert_eq!(config.consumer_pull_timeout_millis(), 47);
    assert_eq!(config.consumer_timeout_millis_when_suspend(), 53);
    assert_eq!(
        config.customized_trace_topic(),
        Some("trace-configured-topic")
    );
    assert_eq!(config.default_broker_id(), 17);
    assert!(config.enable_msg_trace());
    assert_eq!(config.message_model(), MessageModel::Broadcasting);
    assert_eq!(config.poll_timeout_millis(), 41);
    assert_eq!(config.pull_batch_size(), 13);
    assert_eq!(config.pull_thread_nums(), 7);
    assert_eq!(config.pull_threshold_for_all(), 23);
    assert_eq!(config.pull_threshold_for_queue(), 31);
    assert_eq!(config.pull_threshold_size_for_queue(), 37);
    assert_eq!(config.topic_metadata_check_interval_millis(), 43);
    assert!(config.unit_mode());
    assert!(config.connect_broker_by_user());
    assert!(config.message_queue_listener().is_some());
}

#[tokio::test]
async fn lite_trace_dispatcher_uses_java_topic_defaults_and_lifecycle() {
    let mut config = LitePullConsumerConfig::new("lite-trace-lifecycle").unwrap();
    config.client_config_mut().set_namesrv_addr(None);
    config.set_enable_msg_trace(true);
    let consumer = std::sync::Arc::new(LitePullConsumer::new(config).unwrap());
    assert!(consumer.trace_dispatcher().is_none());

    consumer.start().await.unwrap();
    let dispatcher = consumer.trace_dispatcher().unwrap();
    assert_eq!(dispatcher.trace_topic_name(), "RMQ_SYS_TRACE_TOPIC");
    consumer.shutdown().await.unwrap();
    assert!(!consumer.is_running().await);
}

#[tokio::test]
async fn lite_start_validates_consumer_timeouts_and_thread_settings_before_runtime_start() {
    let mut config = LitePullConsumerConfig::new("lite-invalid-timeout").unwrap();
    config.set_broker_suspend_max_time_millis(60_000);
    config.set_consumer_timeout_millis_when_suspend(30_000);
    let consumer = std::sync::Arc::new(LitePullConsumer::new(config).unwrap());
    assert!(matches!(
        consumer.start().await,
        Err(LitePullConsumerError::Pull(_))
    ));
    assert!(!consumer.is_running().await);

    let mut config = LitePullConsumerConfig::new("lite-invalid-threads").unwrap();
    config.set_pull_thread_nums(-1);
    let consumer = std::sync::Arc::new(LitePullConsumer::new(config).unwrap());
    assert!(matches!(
        consumer.start().await,
        Err(LitePullConsumerError::InvalidPullThreadCount)
    ));
}

#[tokio::test]
async fn lite_poll_requires_start_and_auto_commit_is_live_configurable() {
    let config = LitePullConsumerConfig::new("lite-poll-state").unwrap();
    let consumer = LitePullConsumer::new(config).unwrap();
    assert!(matches!(
        consumer.poll(Some(0)).await,
        Err(LitePullConsumerError::NotStarted)
    ));
    assert!(consumer.is_auto_commit());
    consumer.set_auto_commit(false);
    assert!(!consumer.is_auto_commit());
}

#[tokio::test]
async fn lite_assignment_is_available_before_start_and_created_shutdown_is_a_noop() {
    let config = LitePullConsumerConfig::new("lite-assignment-before-start").unwrap();
    let consumer = LitePullConsumer::new(config).unwrap();
    let queues = vec![MessageQueue::new("orders", "broker-a", 0)];
    consumer.assign(queues.clone()).unwrap();
    assert_eq!(
        consumer.assignment().await.unwrap(),
        queues.into_iter().collect()
    );

    consumer.shutdown().await.unwrap();
    assert!(!consumer.is_running().await);
    consumer
        .set_sub_expression_for_assign("orders", "paid")
        .unwrap();
}
