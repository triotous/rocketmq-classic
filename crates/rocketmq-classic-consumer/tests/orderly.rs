use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_consumer::{ConsumeOrderlyContext, ConsumeOrderlyStatus, PushConsumerConfig};

#[test]
fn public_orderly_settings_context_and_statuses_match_java_551_fixture() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/consumer/orderly-settings.json"
    ))
    .expect("Java orderly fixture is valid JSON");

    let mut config = PushConsumerConfig::new("orderly-fixture").unwrap();
    let defaults = &fixture["pushConsumerDefaults"];
    assert_eq!(config.max_reconsume_times(), defaults["maxReconsumeTimes"]);
    assert_eq!(
        config.suspend_current_queue_time_millis(),
        defaults["suspendCurrentQueueTimeMillis"]
    );
    assert_eq!(
        config.consume_timeout_minutes(),
        defaults["consumeTimeoutMinutes"]
    );
    assert_eq!(
        config.consume_message_batch_max_size(),
        defaults["consumeMessageBatchMaxSize"]
    );
    assert_eq!(config.pull_batch_size(), defaults["pullBatchSize"]);
    assert_eq!(
        config.pull_batch_size_in_bytes(),
        defaults["pullBatchSizeInBytes"]
    );

    config.set_max_reconsume_times(
        fixture["configuredPublicSettings"]["maxReconsumeTimes"]
            .as_i64()
            .unwrap() as i32,
    );
    config.set_suspend_current_queue_time_millis(
        fixture["configuredPublicSettings"]["suspendCurrentQueueTimeMillis"]
            .as_i64()
            .unwrap(),
    );
    config.set_pull_batch_size(
        fixture["configuredPublicSettings"]["pullBatchSize"]
            .as_i64()
            .unwrap() as i32,
    );
    config.set_pull_batch_size_in_bytes(
        fixture["configuredPublicSettings"]["pullBatchSizeInBytes"]
            .as_i64()
            .unwrap() as i32,
    );
    assert_eq!(config.max_reconsume_times(), 7);
    assert_eq!(config.pull_batch_size(), 7);
    assert_eq!(config.pull_batch_size_in_bytes(), 12_345);

    let queue_fixture = &fixture["orderlyContextDefaults"]["messageQueue"];
    let queue = MessageQueue::new(
        queue_fixture["topic"].as_str().unwrap(),
        queue_fixture["brokerName"].as_str().unwrap(),
        queue_fixture["queueId"].as_i64().unwrap() as i32,
    );
    let mut context = ConsumeOrderlyContext::new(queue.clone());
    assert_eq!(context.message_queue(), &queue);
    assert_eq!(
        context.auto_commit(),
        fixture["orderlyContextDefaults"]["autoCommit"]
    );
    assert_eq!(
        context.suspend_current_queue_time_millis(),
        fixture["orderlyContextDefaults"]["suspendCurrentQueueTimeMillis"]
    );
    context.set_auto_commit(false);
    context.set_suspend_current_queue_time_millis(7_654);
    assert_eq!(
        context.auto_commit(),
        fixture["mutatedContext"]["autoCommit"]
    );
    assert_eq!(
        context.suspend_current_queue_time_millis(),
        fixture["mutatedContext"]["suspendCurrentQueueTimeMillis"]
    );

    let statuses = [
        ("SUCCESS", ConsumeOrderlyStatus::Success),
        ("ROLLBACK", ConsumeOrderlyStatus::Rollback),
        ("COMMIT", ConsumeOrderlyStatus::Commit),
        (
            "SUSPEND_CURRENT_QUEUE_A_MOMENT",
            ConsumeOrderlyStatus::SuspendCurrentQueueAMoment,
        ),
    ];
    let expected_statuses = fixture["orderlyStatuses"].as_array().unwrap();
    assert_eq!(statuses.len(), expected_statuses.len());
    for ((name, _), expected) in statuses.iter().zip(expected_statuses) {
        assert_eq!(*name, expected.as_str().unwrap());
    }

    assert_eq!(fixture["lockConstants"]["maxLiveTimeMillis"], 30_000);
    assert_eq!(fixture["lockConstants"]["intervalMillis"], 20_000);
    assert_eq!(
        fixture["orderlyRuntimeProperties"]["unlockDelayTimeMillis"],
        20_000
    );
    assert_eq!(
        fixture["orderlyRuntimeProperties"]["maxContinuousConsumeMillis"],
        60_000
    );
}
