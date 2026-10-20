use rocketmq_classic_common::message::{MessageExt, MessageQueue};
use rocketmq_classic_consumer::{
    ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
    ConsumeOrderlyStatus, PullConsumerConfig, PushConsumer, PushConsumerConfig,
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
    sync::{Arc, Mutex},
};

struct OtherConsumer;
impl ConsumerRuntime for OtherConsumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData::default()
    }
}

fn callback_record(message: &MessageExt, queue: &MessageQueue) -> Value {
    json!({"topic": message.topic(), "broker": message.broker_name(),
        "queueTopic": queue.topic(), "queueBroker": queue.broker_name(), "queueId": queue.queue_id()})
}

#[tokio::test]
async fn direct_runtime_consumption_matches_java_status_callbacks_and_offset_isolation() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let cases = fixture["directConsume"].as_array().unwrap();
    assert_eq!(cases.len(), 16);
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
        let mut message = MessageExt::default();
        message.set_topic("tenant%orders");
        message.set_queue_id(3);
        message.set_body(vec![1]);
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
