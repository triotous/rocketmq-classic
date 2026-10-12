use std::path::PathBuf;

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
