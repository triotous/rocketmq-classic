use rocketmq_classic_common::{message::Message, sysflag::message::CompressionType};
use rocketmq_classic_producer::{
    ProducerConfig, ProducerError,
    validation::{
        validate_group, validate_message, validate_send_topic, validate_system_topic,
        validate_topic,
    },
};

#[test]
fn java_default_settings_are_present() {
    let config = ProducerConfig::new("producer-test").unwrap();
    assert_eq!(config.producer_group(), "producer-test");
    assert_eq!(config.create_topic_key(), "TBW102");
    assert_eq!(config.default_topic_queue_nums(), 4);
    assert_eq!(config.send_timeout_millis(), 3_000);
    assert_eq!(config.send_msg_max_timeout_per_request(), -1);
    assert_eq!(config.compress_threshold(), 4_096);
    assert_eq!(config.compress_type(), CompressionType::Zlib);
    assert_eq!(config.compress_level(), 5);
    assert_eq!(config.retry_times_when_send_failed(), 2);
    assert_eq!(config.retry_times_when_send_async_failed(), 2);
    assert!(!config.retry_another_broker_when_not_store_ok());
    assert_eq!(config.max_message_size(), 4 * 1024 * 1024);
}

#[test]
fn java_validation_boundaries_and_reserved_topics_are_preserved() {
    let config = ProducerConfig::new("producer-test").unwrap();
    assert!(validate_group(&"g".repeat(120)).is_ok());
    assert!(matches!(
        validate_group(&"g".repeat(121)),
        Err(ProducerError::GroupTooLong)
    ));
    assert!(validate_topic(&"t".repeat(127)).is_ok());
    assert!(matches!(
        validate_topic(&"t".repeat(128)),
        Err(ProducerError::TopicTooLong)
    ));
    assert!(matches!(
        validate_topic(&format!("%RETRY%{}", "t".repeat(121))),
        Err(ProducerError::TopicTooLong)
    ));
    assert!(validate_system_topic("rmq_sys_private").is_err());
    assert!(validate_send_topic("RMQ_SYS_TRACE_TOPIC").is_ok());
    assert!(validate_send_topic("SCHEDULE_TOPIC_XXXX").is_err());

    let empty = Message::new("orders", Vec::new());
    assert!(matches!(
        validate_message(Some(&empty), &config),
        Err(ProducerError::EmptyBody)
    ));
    let max_body = Message::new("orders", vec![0; 4 * 1024 * 1024]);
    assert!(validate_message(Some(&max_body), &config).is_ok());
    let oversized = Message::new("orders", vec![0; 4 * 1024 * 1024 + 1]);
    assert!(matches!(
        validate_message(Some(&oversized), &config),
        Err(ProducerError::BodyTooLarge { .. })
    ));
    assert!(matches!(
        validate_message(None, &config),
        Err(ProducerError::NullMessage)
    ));
}

#[test]
fn pinned_java_producer_core_fixture_matches_defaults_and_validation() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let defaults = &fixture["defaults"];
    let mut config = ProducerConfig::new(defaults["producerGroup"].as_str().unwrap()).unwrap();
    config.set_create_topic_key(defaults["createTopicKey"].as_str().unwrap());
    config.set_default_topic_queue_nums(defaults["defaultTopicQueueNums"].as_i64().unwrap() as i32);
    config.set_send_timeout_millis(defaults["sendMsgTimeout"].as_i64().unwrap() as i32);
    config.set_send_msg_max_timeout_per_request(
        defaults["sendMsgMaxTimeoutPerRequest"].as_i64().unwrap() as i32,
    );
    config.set_compress_threshold(defaults["compressMsgBodyOverHowmuch"].as_i64().unwrap() as i32);
    config.set_compress_type(
        CompressionType::of(Some(defaults["compressType"].as_str().unwrap())).unwrap(),
    );
    config.set_compress_level(defaults["compressLevel"].as_i64().unwrap() as i32);
    config.set_retry_times_when_send_failed(
        defaults["retryTimesWhenSendFailed"].as_i64().unwrap() as i32
    );
    config.set_retry_times_when_send_async_failed(
        defaults["retryTimesWhenSendAsyncFailed"].as_i64().unwrap() as i32,
    );
    for code in defaults["retryResponseCodes"].as_array().unwrap() {
        config.add_retry_response_code(code.as_i64().unwrap() as i32);
    }
    config.set_retry_another_broker_when_not_store_ok(
        defaults["retryAnotherBrokerWhenNotStoreOK"]
            .as_bool()
            .unwrap(),
    );
    config.set_max_message_size(defaults["maxMessageSize"].as_i64().unwrap() as i32);
    assert_eq!(config.producer_group(), "DEFAULT_PRODUCER");
    assert_eq!(config.create_topic_key(), "TBW102");
    assert_eq!(config.default_topic_queue_nums(), 4);
    assert_eq!(config.send_timeout_millis(), 3_000);
    assert_eq!(
        config.send_msg_max_timeout_per_request(),
        defaults["sendMsgMaxTimeoutPerRequest"].as_i64().unwrap() as i32
    );
    assert_eq!(config.compress_threshold(), 4_096);
    assert_eq!(config.compress_type(), CompressionType::Zlib);
    assert_eq!(config.compress_level(), 5);
    assert_eq!(config.retry_times_when_send_failed(), 2);
    assert_eq!(config.retry_times_when_send_async_failed(), 2);
    assert_eq!(
        config
            .retry_response_codes()
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        defaults["retryResponseCodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|code| code.as_i64().unwrap() as i32)
            .collect::<Vec<_>>()
    );
    assert!(!config.retry_another_broker_when_not_store_ok());
    assert_eq!(config.max_message_size(), 4 * 1024 * 1024);

    for row in fixture["groups"].as_array().unwrap() {
        assert_eq!(
            validate_group(row["input"].as_str().unwrap()).is_ok(),
            row["valid"].as_bool().unwrap(),
            "group {:?}",
            row["input"]
        );
    }
    for row in fixture["topics"].as_array().unwrap() {
        assert_eq!(
            validate_topic(row["input"].as_str().unwrap()).is_ok(),
            row["valid"].as_bool().unwrap(),
            "topic {:?}",
            row["input"]
        );
    }
    for row in fixture["systemTopics"].as_array().unwrap() {
        let topic = row["topic"].as_str().unwrap();
        assert_eq!(validate_system_topic(topic).is_err(), row["system"]);
        assert_eq!(validate_send_topic(topic).is_err(), row["sendForbidden"]);
        assert_eq!(validate_send_topic(topic).is_ok(), row["sendAllowed"]);
    }
    for row in fixture["messages"].as_array().unwrap() {
        let case = row["input"].as_str().unwrap();
        let valid = match case {
            "null-message" => validate_message(None, &config).is_ok(),
            // Message owns a non-null Vec in Rust. Java's null and empty body
            // cases therefore map to the same empty-body rejection.
            "null-body" | "empty-body" => {
                validate_message(Some(&Message::new("orders", Vec::new())), &config).is_ok()
            }
            "body-max" => validate_message(
                Some(&Message::new(
                    "orders",
                    vec![0; config.max_message_size() as usize],
                )),
                &config,
            )
            .is_ok(),
            "body-max-plus-one" => validate_message(
                Some(&Message::new(
                    "orders",
                    vec![0; config.max_message_size() as usize + 1],
                )),
                &config,
            )
            .is_ok(),
            "multi-dispatch-separator" => {
                let mut message = Message::new("orders", [1]);
                message
                    .set_system_property("INNER_MULTI_DISPATCH", "a/b")
                    .unwrap();
                validate_message(Some(&message), &config).is_ok()
            }
            other => panic!("unknown Java producer fixture case: {other}"),
        };
        assert_eq!(valid, row["valid"], "message case {case}");
    }
}
