use base64::Engine;
use rocketmq_classic_common::message::{
    MessageClientExt, MessageExt, MessageExtSubtype, MessageQueue,
};
use rocketmq_classic_consumer::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, ProcessQueue, PullConsumerConfig, PushConsumer, PushConsumerConfig,
    PushMessageListener,
    offset::{OffsetStore, RemoteBrokerOffsetStore},
    pull::PullConsumer,
};
use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, ConsumerRuntime, RuntimeError},
};
use rocketmq_classic_protocol::heartbeat::ConsumerData;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

struct OtherConsumer;
impl ConsumerRuntime for OtherConsumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData::default()
    }
}

fn callback_record(message: &MessageExt, queue: &MessageQueue) -> Value {
    let message_class = message
        .runtime_subtype_name()
        .map(|name| name.rsplit("::").next().unwrap_or(name))
        .unwrap_or(if message.is_message_client_ext() {
            "MessageClientExt"
        } else {
            "MessageExt"
        });
    let mut record = json!({"messageClass": message_class,
        "topic": message.topic_nullable(), "broker": message.broker_name(),
        "queueTopic": queue.topic(), "queueBroker": queue.broker_name(), "queueId": queue.queue_id(),
        "messageId": message.message_id_for_runtime_type(), "offsetMessageId": message.offset_message_id(),
        "reconsumeTimes": message.reconsume_times(), "queueOffset": message.queue_offset(),
        "propertiesPresent": message.properties_nullable().is_some()});
    if let Some(subtype) = message.runtime_subtype::<CustomDirectMessageExt>() {
        record["messageQueueId"] = json!(message.queue_id());
        record["customField"] = json!(subtype.custom_field());
        record["messageDisplay"] = json!(message.to_string());
    }
    record
}

struct CustomDirectMessageExt {
    broker_was_set: AtomicBool,
    topic_writes: std::sync::atomic::AtomicUsize,
    reconsume_writes: std::sync::atomic::AtomicUsize,
    queue_offset_writes: std::sync::atomic::AtomicUsize,
}

impl CustomDirectMessageExt {
    fn new() -> Self {
        Self {
            broker_was_set: AtomicBool::new(false),
            topic_writes: std::sync::atomic::AtomicUsize::new(0),
            reconsume_writes: std::sync::atomic::AtomicUsize::new(0),
            queue_offset_writes: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn custom_field(&self) -> String {
        format!(
            "topicWrites={};reconsumeWrites={};queueOffsetWrites={}",
            self.topic_writes.load(Ordering::SeqCst),
            self.reconsume_writes.load(Ordering::SeqCst),
            self.queue_offset_writes.load(Ordering::SeqCst)
        )
    }
}

impl MessageExtSubtype for CustomDirectMessageExt {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn topic(&self) -> Option<Option<&str>> {
        Some(Some("%RETRY%tenant%direct"))
    }

    fn set_topic(&self, _topic: Option<String>) -> bool {
        self.topic_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn broker_name(&self) -> Option<Option<&str>> {
        Some(
            self.broker_was_set
                .load(Ordering::SeqCst)
                .then_some("broker-a"),
        )
    }

    fn set_broker_name(&self, broker_name: Option<String>) -> bool {
        self.broker_was_set
            .store(broker_name.is_some(), Ordering::SeqCst);
        true
    }

    fn queue_id(&self) -> Option<i32> {
        Some(37)
    }

    fn set_queue_id(&self, _queue_id: i32) -> bool {
        true
    }

    fn queue_offset(&self) -> Option<i64> {
        Some(987_654_321)
    }

    fn set_queue_offset(&self, _queue_offset: i64) -> bool {
        self.queue_offset_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn message_id(&self) -> Option<Option<String>> {
        Some(Some("subtype-message-id".to_owned()))
    }

    fn set_message_id(&self, _message_id: Option<String>) -> bool {
        true
    }

    fn reconsume_times(&self) -> Option<i32> {
        Some(13)
    }

    fn set_reconsume_times(&self, _reconsume_times: i32) -> bool {
        self.reconsume_writes.fetch_add(1, Ordering::SeqCst);
        true
    }

    fn display_value(&self) -> Option<String> {
        Some(format!(
            "CustomDirectMessageExt[topicWrites={}]",
            self.topic_writes.load(Ordering::SeqCst)
        ))
    }
}

#[tokio::test]
async fn direct_runtime_consumption_matches_java_status_callbacks_and_offset_isolation() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let cases = fixture["directConsume"].as_array().unwrap();
    assert_eq!(cases.len(), 27);
    for case in cases {
        let name = case["name"].as_str().unwrap().to_owned();
        let manager = ClientRuntimeManager::shared_default();
        let mut client_config = ClientConfig::new(format!("runtime-direct-{name}"));
        client_config.set_namesrv_addr(Some(String::new()));
        client_config.set_instance_name(Some(name.clone()));
        client_config.set_namespace(Some("tenant".into()));
        let runtime = manager.get_or_create(&client_config).await.unwrap();
        let store = Arc::new(RemoteBrokerOffsetStore::new(
            runtime.clone(),
            "tenant%direct",
        ));
        let queue = MessageQueue::new("tenant%orders", "broker-a", 3);
        store.update_offset(&queue, 123, false);
        let mut config = PushConsumerConfig::new("direct").unwrap();
        *config.client_config_mut() = client_config.clone();
        let push = Arc::new(PushConsumer::new(config));
        push.set_offset_store(Some(store.clone())).await.unwrap();
        let callbacks = Arc::new(Mutex::new(Vec::new()));
        let records = callbacks.clone();
        let listener_name = name.clone();
        if name.starts_with("orderly") {
            push.register_message_listener(PushMessageListener::orderly(
                move |messages: Vec<MessageExt>, mut context: ConsumeOrderlyContext| {
                    let records = records.clone();
                    let name = listener_name.clone();
                    async move {
                        records
                            .lock()
                            .unwrap()
                            .push(callback_record(&messages[0], context.message_queue()));
                        context.set_auto_commit(false);
                        let status = if name.ends_with("null-status") {
                            None
                        } else if name.ends_with("commit") {
                            Some(ConsumeOrderlyStatus::Commit)
                        } else if name.ends_with("rollback") {
                            Some(ConsumeOrderlyStatus::Rollback)
                        } else if name.ends_with("later") {
                            Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment)
                        } else {
                            Some(ConsumeOrderlyStatus::Success)
                        };
                        (status, context)
                    }
                },
            ))
            .await
            .unwrap();
        } else {
            push.register_message_listener(PushMessageListener::concurrently(
                move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let records = records.clone();
                    let name = listener_name.clone();
                    async move {
                        records
                            .lock()
                            .unwrap()
                            .push(callback_record(&messages[0], context.message_queue()));
                        assert!(!name.ends_with("throws"), "listener failed");
                        let status = if name.ends_with("null-status") {
                            None
                        } else if name.ends_with("later") {
                            Some(ConsumeConcurrentlyStatus::ReconsumeLater)
                        } else {
                            Some(ConsumeConcurrentlyStatus::ConsumeSuccess)
                        };
                        (status, context)
                    }
                },
            ))
            .await
            .unwrap();
        }
        push.start().await.unwrap();
        let push_owner = runtime.select_consumer("tenant%direct").unwrap().unwrap();
        let mut pull_config = PullConsumerConfig::new("pull").unwrap();
        *pull_config.client_config_mut() = client_config;
        let pull = Arc::new(PullConsumer::new(pull_config));
        if name == "pull-group" {
            pull.start().await.unwrap();
        }
        let selected: Arc<dyn ConsumerRuntime> = if name == "other-group" {
            Arc::new(OtherConsumer)
        } else if name == "pull-group" {
            runtime.select_consumer("tenant%pull").unwrap().unwrap()
        } else {
            push_owner
        };
        if name == "push-null-service" {
            push.shutdown().await.unwrap();
        }
        runtime
            .consumer_table()
            .put("selected", selected)
            .await
            .unwrap();
        let client_message = case["inputClient"].as_bool().unwrap();
        let physical = case["inputMessageId"].as_str().map(str::to_owned);
        let unique = case["inputUniq"].as_str().map(str::to_owned);
        // Java accepts a MessageClientExt through its MessageExt parameter and
        // preserves the runtime subclass for the listener callback.
        let message: MessageExt = if name == "concurrent-custom-subtype" {
            let mut message = MessageExt::default();
            message.set_runtime_subtype(Arc::new(CustomDirectMessageExt::new()));
            message.set_topic_nullable(case["inputTopic"].as_str().map(str::to_owned));
            message.set_queue_id(3);
            message.set_body(vec![1]);
            message.set_message_id(physical);
            message
                .set_system_property("RETRY_TOPIC", "subtype-retry-target")
                .unwrap();
            message.set_reconsume_times(2);
            message.set_queue_offset(17);
            message
        } else if client_message {
            let mut message = MessageClientExt::new();
            message.set_topic_nullable(case["inputTopic"].as_str().map(str::to_owned));
            message.set_queue_id(3);
            message.set_body(vec![1]);
            if let Some(unique) = unique.as_deref() {
                message.set_system_property("UNIQ_KEY", unique).unwrap();
            }
            message.set_offset_message_id(physical);
            message.into_message_ext()
        } else {
            let mut message = MessageExt::default();
            message.set_topic_nullable(case["inputTopic"].as_str().map(str::to_owned));
            message.set_queue_id(3);
            message.set_body(vec![1]);
            if let Some(unique) = unique {
                message.set_system_property("UNIQ_KEY", unique).unwrap();
            }
            message.set_message_id(physical);
            message
        };
        let message = (!case["nullMessage"].as_bool().unwrap()).then_some(message);
        let result = runtime
            .consume_message_directly(message, case["group"].as_str(), case["broker"].as_str())
            .await;
        if case["error"].is_null() {
            let result = result.unwrap();
            assert_eq!(
                result.is_some(),
                case["resultPresent"].as_bool().unwrap(),
                "{name}"
            );
            if let Some(result) = result {
                assert_eq!(
                    serde_json::to_value(result.get_consume_result()).unwrap(),
                    case["result"],
                    "{name}"
                );
                assert_eq!(
                    result.is_order(),
                    case["order"].as_bool().unwrap(),
                    "{name}"
                );
                assert_eq!(
                    result.is_auto_commit(),
                    case["autoCommit"].as_bool().unwrap(),
                    "{name}"
                );
                assert_eq!(
                    result.get_remark().is_some(),
                    case["remarkPresent"].as_bool().unwrap(),
                    "{name}"
                );
                assert!(result.get_spent_time_mills() >= 0);
            }
        } else {
            assert!(
                matches!(
                    result,
                    Err(RuntimeError::ConsumerOperation(_) | RuntimeError::InvalidRoute(_))
                ),
                "{name}: {result:?}"
            );
        }
        assert_eq!(
            json!(*callbacks.lock().unwrap()),
            case["callbacks"],
            "{name}"
        );
        assert_eq!(
            store.clone_offset_table(None),
            HashMap::from([(queue, 123)]),
            "{name}"
        );
        runtime.consumer_table().remove("selected").await.unwrap();
        pull.shutdown().await.unwrap();
        push.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }
}

#[test]
fn custom_subtype_queue_offset_matches_java_process_queue() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let expected = &fixture["customSubtypeProcessQueue"];

    let mut message = MessageExt::default();
    message.set_runtime_subtype(Arc::new(CustomDirectMessageExt::new()));
    message.set_queue_offset(17);
    message.set_body(vec![1, 2, 3]);
    let process_queue = ProcessQueue::new();
    let dispatch = process_queue.put_message(&[message.clone()]);
    let offsets = process_queue
        .get_msg_tree_map()
        .read()
        .keys()
        .copied()
        .collect::<Vec<_>>();
    let subtype = message.runtime_subtype::<CustomDirectMessageExt>().unwrap();

    assert_eq!(
        json!({
            "dispatch": dispatch,
            "count": process_queue.get_msg_count().get(),
            "size": process_queue.get_msg_size().get(),
            "offsets": offsets,
            "customField": subtype.custom_field(),
        }),
        *expected
    );
}

#[tokio::test]
async fn inbound_direct_consume_decodes_client_ids_before_invoking_the_owner() {
    use rocketmq_classic_common::message::{DecodeOptions, decode_one, encode_message_ext};
    use rocketmq_classic_core::runtime::ConsumerOperation;
    use rocketmq_classic_protocol::codes::{request, response};
    use rocketmq_classic_remoting::RemotingCommand;
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/messages/records.json")).unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "ipv4-v1")
        .unwrap();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(case["recordBase64"].as_str().unwrap())
        .unwrap();
    let (mut message, _) = decode_one(&bytes, DecodeOptions::default()).unwrap();
    let physical_id = message.message_id().unwrap().to_owned();
    message.set_topic("tenant%orders");
    message.set_store_size(0);
    message
        .set_system_property("UNIQ_KEY", "wire-client-id")
        .unwrap();
    let body = encode_message_ext(&message, false).unwrap();
    let mut config = PushConsumerConfig::new("wire-client-ids").unwrap();
    config
        .client_config_mut()
        .set_client_ip(Some("wire-client-ids".into()));
    config
        .client_config_mut()
        .set_namespace(Some("tenant".into()));
    config
        .client_config_mut()
        .set_namesrv_addr(Some(String::new()));
    let runtime = ClientRuntimeManager::shared_default()
        .get_or_create(config.client_config())
        .await
        .unwrap();
    let consumer = Arc::new(PushConsumer::new(config));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let callbacks = observed.clone();
    consumer
        .register_message_listener(PushMessageListener::concurrently(
            move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let callbacks = callbacks.clone();
                async move {
                    callbacks
                        .lock()
                        .unwrap()
                        .push(callback_record(&messages[0], context.message_queue()));
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        ))
        .await
        .unwrap();
    consumer.start().await.unwrap();
    let processor = runtime
        .select_consumer("tenant%wire-client-ids")
        .unwrap()
        .unwrap()
        .inbound_processor(ConsumerOperation::ConsumeDirectly)
        .unwrap();
    let result = tokio::task::spawn_blocking(move || {
        processor.process(RemotingCommand {
            code: request::CONSUME_MESSAGE_DIRECTLY,
            ext_fields: Some(
                std::collections::BTreeMap::from([
                    ("consumerGroup".into(), "tenant%wire-client-ids".into()),
                    ("brokerName".into(), "wire-broker".into()),
                ])
                .into(),
            ),
            body: Some(body),
            ..Default::default()
        })
    })
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(result.code, response::SUCCESS);
    let callbacks = observed.lock().unwrap().clone();
    assert_eq!(callbacks.len(), 1);
    assert_eq!(callbacks[0]["messageId"], "wire-client-id");
    assert_eq!(callbacks[0]["offsetMessageId"], physical_id);
    assert_eq!(callbacks[0]["topic"], "orders");
    assert_eq!(callbacks[0]["queueTopic"], "tenant%orders");
    consumer.shutdown().await.unwrap();
    runtime.shutdown().await.unwrap();
}
