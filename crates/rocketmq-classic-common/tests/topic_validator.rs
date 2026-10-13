use std::collections::BTreeSet;

use rocketmq_classic_common::topic::TopicValidator;
use serde_json::Value;

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../fixtures/namespace/topic-validator.json"
    ))
    .unwrap()
}

fn optional_string(value: &Value) -> Option<&str> {
    value.as_str()
}

#[test]
fn public_topic_validator_surface_matches_pinned_java_fixture() {
    let fixture = fixture();
    let constants = &fixture["constants"];
    assert_eq!(
        constants["AUTO_CREATE_TOPIC_KEY_TOPIC"],
        TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_SCHEDULE_TOPIC"],
        TopicValidator::RMQ_SYS_SCHEDULE_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_BENCHMARK_TOPIC"],
        TopicValidator::RMQ_SYS_BENCHMARK_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_TRANS_HALF_TOPIC"],
        TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC"],
        TopicValidator::RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_TRACE_TOPIC"],
        TopicValidator::RMQ_SYS_TRACE_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_TRANS_OP_HALF_TOPIC"],
        TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC"],
        TopicValidator::RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC"],
        TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_SELF_TEST_TOPIC"],
        TopicValidator::RMQ_SYS_SELF_TEST_TOPIC
    );
    assert_eq!(
        constants["RMQ_SYS_OFFSET_MOVED_EVENT"],
        TopicValidator::RMQ_SYS_OFFSET_MOVED_EVENT
    );
    assert_eq!(
        constants["RMQ_SYS_ROCKSDB_OFFSET_TOPIC"],
        TopicValidator::RMQ_SYS_ROCKSDB_OFFSET_TOPIC
    );
    assert_eq!(
        constants["SYSTEM_TOPIC_PREFIX"],
        TopicValidator::SYSTEM_TOPIC_PREFIX
    );
    assert_eq!(
        constants["SYNC_BROKER_MEMBER_GROUP_PREFIX"],
        TopicValidator::SYNC_BROKER_MEMBER_GROUP_PREFIX
    );

    let valid_char_map = TopicValidator::valid_char_bit_map();
    let initial_valid_characters = valid_char_map
        .read()
        .unwrap()
        .iter()
        .enumerate()
        .filter_map(|(code, valid)| valid.then_some(char::from_u32(code as u32).unwrap()))
        .collect::<String>();
    assert_eq!(fixture["validCharacters"], initial_valid_characters);

    for case in fixture["topicCases"].as_array().unwrap() {
        let value = optional_string(&case["value"]);
        let result = TopicValidator::validate_topic(value);
        assert_eq!(
            case["valid"],
            result.is_valid(),
            "topic case {}",
            case["name"]
        );
        assert_eq!(
            case["remark"],
            result.remark(),
            "topic case {}",
            case["name"]
        );
        if let Some(value) = value {
            assert_eq!(
                case["illegal"],
                TopicValidator::is_topic_or_group_illegal(value),
                "topic character case {}",
                case["name"]
            );
        }
    }

    for case in fixture["groupCases"].as_array().unwrap() {
        let value = optional_string(&case["value"]);
        let result = TopicValidator::validate_group(value);
        assert_eq!(
            case["valid"],
            result.is_valid(),
            "group case {}",
            case["name"]
        );
        assert_eq!(
            case["remark"],
            result.remark(),
            "group case {}",
            case["name"]
        );
        if let Some(value) = value {
            assert_eq!(
                case["illegal"],
                TopicValidator::is_topic_or_group_illegal(value),
                "group character case {}",
                case["name"]
            );
        }
    }

    let classification = &fixture["classification"];
    let custom_system_topic = classification["customSystemTopic"].as_str().unwrap();
    let custom_blocked_topic = classification["customBlockedTopic"].as_str().unwrap();
    TopicValidator::add_system_topic(custom_system_topic);
    TopicValidator::not_allowed_send_topic_set()
        .write()
        .unwrap()
        .insert(custom_blocked_topic.to_owned());
    assert_eq!(
        classification["systemTopics"],
        serde_json::to_value(
            TopicValidator::system_topic_set()
                .read()
                .unwrap()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        )
        .unwrap()
    );
    assert_eq!(
        classification["blockedTopics"],
        serde_json::to_value(
            TopicValidator::not_allowed_send_topic_set()
                .read()
                .unwrap()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        )
        .unwrap()
    );
    assert_eq!(
        classification["customSystemTopicIsSystem"],
        TopicValidator::is_system_topic(custom_system_topic)
    );
    assert_eq!(
        classification["customSystemTopicIsBlocked"],
        TopicValidator::is_not_allowed_send_topic(custom_system_topic)
    );
    assert_eq!(
        classification["customBlockedTopicIsSystem"],
        TopicValidator::is_system_topic(custom_blocked_topic)
    );
    assert_eq!(
        classification["customBlockedTopicIsBlocked"],
        TopicValidator::is_not_allowed_send_topic(custom_blocked_topic)
    );
    assert_eq!(
        classification["traceTopicIsBlocked"],
        TopicValidator::is_not_allowed_send_topic(TopicValidator::RMQ_SYS_TRACE_TOPIC)
    );
    assert_eq!(
        classification["scheduleTopicIsBlocked"],
        TopicValidator::is_not_allowed_send_topic(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC)
    );

    let mutation = &fixture["validCharMutation"];
    let tilde_was_valid = mutation["wasInitiallyValid"].as_bool().unwrap();
    {
        let mut map = valid_char_map.write().unwrap();
        map[b'~' as usize] = true;
    }
    assert_eq!(
        mutation["illegalAfterAllowing"],
        TopicValidator::is_topic_or_group_illegal("custom~")
    );
    assert_eq!(
        mutation["topicValidationAfterAllowing"],
        TopicValidator::validate_topic(Some("custom~")).is_valid()
    );
    {
        let mut map = valid_char_map.write().unwrap();
        map[b'~' as usize] = tilde_was_valid;
    }
    assert_eq!(mutation["restored"], true);
}
