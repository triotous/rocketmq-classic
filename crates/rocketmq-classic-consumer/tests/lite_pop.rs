use std::path::PathBuf;

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_consumer::push::PushConsumerConfig;
use rocketmq_classic_consumer::{LitePullConsumer, LitePullConsumerConfig, LitePullConsumerError};
use rocketmq_classic_protocol::heartbeat::{ConsumeFromWhere, MessageModel};
use serde_json::Value;

fn java_fixture() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/consumer/lite-pop.json");
    serde_json::from_slice(&std::fs::read(path).expect("pinned Java Lite/POP fixture"))
        .expect("valid pinned Java Lite/POP fixture")
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
