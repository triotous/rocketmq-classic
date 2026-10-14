use std::{collections::HashMap, sync::Arc};

use rocketmq_classic_common::{
    message::{Message, MessageQueue},
    sysflag::message::CompressionType,
};
use rocketmq_classic_producer::{
    AsyncSenderExecutor, AsyncSenderTask, CheckForbiddenContext, CheckForbiddenHook,
    CommunicationMode, Producer, ProducerConfig, ProducerError, ProducerServiceState,
    SendMessageContext, SendMessageHook, SendMessageType,
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
fn pinned_java_default_producer_impl_public_surface_is_recorded() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let inventory = &fixture["producerImplementationApi"];
    let methods = inventory["publicMethods"].as_array().unwrap();
    let constructors = inventory["publicConstructors"].as_array().unwrap();

    assert_eq!(
        inventory["className"],
        "org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl"
    );
    assert_eq!(inventory["publicMethodCount"].as_u64(), Some(82));
    assert_eq!(methods.len(), 82);
    assert_eq!(constructors.len(), 2);
    assert!(constructors.iter().any(|constructor| {
        constructor
            == "org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl(org.apache.rocketmq.client.producer.DefaultMQProducer,org.apache.rocketmq.remoting.RPCHook)"
    }));
    for method in [
        "boolean org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.hasCheckForbiddenHook()",
        "boolean org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.hasSendMessageHook()",
        "java.util.concurrent.ConcurrentMap org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getTopicPublishInfoTable()",
        "org.apache.rocketmq.common.ServiceState org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getServiceState()",
        "void org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.setServiceState(org.apache.rocketmq.common.ServiceState)",
        "boolean org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.isUnitMode()",
        "org.apache.rocketmq.client.producer.DefaultMQProducer org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getDefaultMQProducer()",
        "org.apache.rocketmq.client.latency.MQFaultStrategy org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getMqFaultStrategy()",
        "org.apache.rocketmq.client.impl.factory.MQClientInstance org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getMqClientFactory()",
        "org.apache.rocketmq.client.impl.factory.MQClientInstance org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getmQClientFactory()",
    ] {
        assert!(
            methods.iter().any(|candidate| candidate == method),
            "missing {method}"
        );
    }
}

#[test]
fn implementation_hook_helpers_match_pinned_java_fixture() {
    use std::sync::Mutex;

    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationHookExecution"];
    let producer = Producer::new(ProducerConfig::new("producer-implementation-hooks").unwrap());

    assert_eq!(
        producer.has_check_forbidden_hook(),
        expected["checkForbiddenInitiallyPresent"]
    );
    assert_eq!(
        producer.has_send_message_hook(),
        expected["sendMessageInitiallyPresent"]
    );

    let check_events = Arc::new(Mutex::new(Vec::<String>::new()));
    struct CheckFirst(Arc<Mutex<Vec<String>>>);
    impl CheckForbiddenHook for CheckFirst {
        fn hook_name(&self) -> &str {
            "fixture-check-first"
        }

        fn check_forbidden(
            &self,
            context: &mut CheckForbiddenContext<'_>,
        ) -> Result<(), ProducerError> {
            self.0.lock().unwrap().push("first".into());
            context
                .message
                .set_system_property("DIRECT_CHECK_HOOK", "mutated")
                .unwrap();
            context.queue.set_queue_id(4);
            Ok(())
        }
    }
    struct CheckReject(Arc<Mutex<Vec<String>>>);
    impl CheckForbiddenHook for CheckReject {
        fn hook_name(&self) -> &str {
            "fixture-check-reject"
        }

        fn check_forbidden(
            &self,
            context: &mut CheckForbiddenContext<'_>,
        ) -> Result<(), ProducerError> {
            self.0.lock().unwrap().push(format!(
                "reject:{}",
                context
                    .message
                    .properties()
                    .get("DIRECT_CHECK_HOOK")
                    .map(String::as_str)
                    .unwrap_or("<null>")
            ));
            Err(ProducerError::ForbiddenTopic)
        }
    }
    struct CheckLast(Arc<Mutex<Vec<String>>>);
    impl CheckForbiddenHook for CheckLast {
        fn hook_name(&self) -> &str {
            "fixture-check-last"
        }

        fn check_forbidden(
            &self,
            _context: &mut CheckForbiddenContext<'_>,
        ) -> Result<(), ProducerError> {
            self.0.lock().unwrap().push("unreached".into());
            Ok(())
        }
    }

    producer.register_check_forbidden_hook(Arc::new(CheckFirst(check_events.clone())));
    producer.register_check_forbidden_hook(Arc::new(CheckReject(check_events.clone())));
    producer.register_check_forbidden_hook(Arc::new(CheckLast(check_events.clone())));
    assert_eq!(
        producer.has_check_forbidden_hook(),
        expected["checkForbiddenPresentAfterRegistration"]
    );

    let mut check_message = Message::new("orders", [1]);
    let mut check_queue = MessageQueue::new("orders", "broker-a", 1);
    let check_result = {
        let mut context = CheckForbiddenContext {
            name_srv_addr: None,
            group: "producer-implementation-hooks".into(),
            message: &mut check_message,
            queue: &mut check_queue,
            broker_addr: "127.0.0.1:10911".into(),
            communication_mode: CommunicationMode::Sync,
            send_result: None,
            exception: None,
            arg: None,
            unit_mode: false,
        };
        producer.execute_check_forbidden_hook(&mut context)
    };
    assert!(check_result.is_err());
    assert!(expected["checkFailure"].is_string());
    assert_eq!(
        *check_events.lock().unwrap(),
        expected["checkEvents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event.as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        check_message
            .properties()
            .get("DIRECT_CHECK_HOOK")
            .map(String::as_str),
        expected["checkMessageMarker"].as_str()
    );
    assert_eq!(
        check_queue.queue_id(),
        expected["checkQueueId"].as_i64().unwrap() as i32
    );

    let send_events = Arc::new(Mutex::new(Vec::<String>::new()));
    struct SendFirst(Arc<Mutex<Vec<String>>>);
    impl SendMessageHook for SendFirst {
        fn hook_name(&self) -> &str {
            "fixture-send-first"
        }

        fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
            self.0.lock().unwrap().push("before:first".into());
            context
                .message
                .set_system_property("DIRECT_SEND_HOOK", "before")
                .unwrap();
        }

        fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
            self.0.lock().unwrap().push("after:first".into());
            context
                .message
                .set_system_property("DIRECT_SEND_HOOK", "after")
                .unwrap();
        }
    }
    struct SendPanics(Arc<Mutex<Vec<String>>>);
    impl SendMessageHook for SendPanics {
        fn hook_name(&self) -> &str {
            "fixture-send-throws"
        }

        fn send_message_before(&self, _context: &mut SendMessageContext<'_>) {
            self.0.lock().unwrap().push("before:throws".into());
            panic!("fixture before failure");
        }

        fn send_message_after(&self, _context: &mut SendMessageContext<'_>) {
            self.0.lock().unwrap().push("after:throws".into());
            panic!("fixture after failure");
        }
    }
    struct SendLast(Arc<Mutex<Vec<String>>>);
    impl SendMessageHook for SendLast {
        fn hook_name(&self) -> &str {
            "fixture-send-last"
        }

        fn send_message_before(&self, context: &mut SendMessageContext<'_>) {
            let marker = context
                .message
                .properties()
                .get("DIRECT_SEND_HOOK")
                .map(String::as_str)
                .unwrap_or("<null>");
            self.0.lock().unwrap().push(format!("before:last:{marker}"));
        }

        fn send_message_after(&self, context: &mut SendMessageContext<'_>) {
            let marker = context
                .message
                .properties()
                .get("DIRECT_SEND_HOOK")
                .map(String::as_str)
                .unwrap_or("<null>");
            self.0.lock().unwrap().push(format!("after:last:{marker}"));
        }
    }

    producer.register_send_message_hook(Arc::new(SendFirst(send_events.clone())));
    producer.register_send_message_hook(Arc::new(SendPanics(send_events.clone())));
    producer.register_send_message_hook(Arc::new(SendLast(send_events.clone())));
    assert_eq!(
        producer.has_send_message_hook(),
        expected["sendMessagePresentAfterRegistration"]
    );
    let mut send_message = Message::new("orders", [2]);
    let mut send_context = SendMessageContext {
        producer_group: "producer-implementation-hooks".into(),
        producer: &producer,
        message: &mut send_message,
        queue: MessageQueue::new("orders", "broker-a", 2),
        broker_addr: "127.0.0.1:10911".into(),
        born_host: None,
        communication_mode: CommunicationMode::Sync,
        send_result: None,
        exception: None,
        message_type: SendMessageType::Normal,
        namespace: None,
        trace_context: None,
        properties: None,
    };
    producer.execute_send_message_hook_before(&mut send_context);
    producer.execute_send_message_hook_after(&mut send_context);

    assert_eq!(
        *send_events.lock().unwrap(),
        expected["sendEvents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event.as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        send_message
            .properties()
            .get("DIRECT_SEND_HOOK")
            .map(String::as_str),
        expected["sendMessageMarker"].as_str()
    );
}

#[test]
fn implementation_state_accessors_match_pinned_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationStateAccessors"];
    let mut config = ProducerConfig::new("producer-implementation-state").unwrap();
    config.client_config_mut().set_unit_mode(true);
    let producer = Producer::new(config);

    assert_eq!(
        producer.service_state().unwrap().as_java_name(),
        expected["initialServiceState"]
    );
    assert_eq!(
        producer.config().producer_group(),
        expected["defaultProducerGroup"]
    );
    assert_eq!(producer.is_unit_mode(), expected["unitMode"]);
    assert!(expected["defaultProducerIdentity"].as_bool().unwrap());
    assert!(expected["clientFactoryInitiallyNull"].as_bool().unwrap());

    let states = ProducerServiceState::values();
    let expected_states = expected["serviceStateValues"].as_array().unwrap();
    let expected_ordinals = expected["serviceStateOrdinals"].as_array().unwrap();
    assert_eq!(states.len(), expected_states.len());
    assert_eq!(states.len(), expected_ordinals.len());
    for ((state, expected_name), expected_ordinal) in states
        .into_iter()
        .zip(expected_states)
        .zip(expected_ordinals)
    {
        assert_eq!(state.as_java_name(), expected_name.as_str().unwrap());
        assert_eq!(state.name(), expected_name.as_str().unwrap());
        assert_eq!(state.ordinal(), expected_ordinal.as_i64().unwrap() as i32);
        assert_eq!(
            ProducerServiceState::value_of_name(expected_name.as_str()),
            Ok(state)
        );
        producer.set_service_state(Some(state));
        assert_eq!(producer.service_state(), Some(state));
    }
    assert_eq!(
        ProducerServiceState::value_of_name(None),
        Err(rocketmq_classic_protocol::codes::JavaEnumLookupError::NullName)
    );
    producer.set_service_state(None);
    assert!(expected["nullServiceState"].as_bool().unwrap());
    assert_eq!(producer.service_state(), None);
}

#[test]
fn producer_compressor_snapshot_and_operations_match_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let producer = Producer::new(ProducerConfig::new("compressor-fixture").unwrap());
    let input = [0, 1, 2, 3, 127, 255];

    for row in fixture["compressorSettings"]["types"].as_array().unwrap() {
        let kind = CompressionType::of(Some(row["type"].as_str().unwrap())).unwrap();
        producer.set_compress_type(kind);
        let compressor = producer.compressor();
        assert_eq!(compressor.compression_type(), kind, "{row}");
        assert!(row["roundTrip"].as_bool().unwrap(), "{row}");

        let compressed = compressor.compress(&input, 5).unwrap();
        assert_eq!(compressor.decompress(&compressed).unwrap(), input, "{row}");
        let expected_class = match kind {
            CompressionType::Lz4 => "Lz4Compressor",
            CompressionType::Zstd => "ZstdCompressor",
            CompressionType::Zlib => "ZlibCompressor",
        };
        assert_eq!(row["class"], expected_class, "{row}");
    }

    producer.set_compress_type(CompressionType::Zlib);
    let retained = producer.compressor();
    producer.set_compress_type(CompressionType::Lz4);
    assert_eq!(retained.compression_type(), CompressionType::Zlib);
    assert!(
        fixture["compressorSettings"]["retainedRoundTripAfterUpdate"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(
        retained
            .decompress(&retained.compress(&input, 5).unwrap())
            .unwrap(),
        input
    );
    let current = producer.compressor();
    assert_eq!(
        current.compression_type().name(),
        fixture["compressorSettings"]["currentTypeAfterUpdate"]
            .as_str()
            .unwrap()
    );
    assert_eq!(
        fixture["compressorSettings"]["retainedClassAfterUpdate"],
        "ZlibCompressor"
    );
    assert_eq!(
        fixture["compressorSettings"]["currentClassAfterUpdate"],
        "Lz4Compressor"
    );
}

#[test]
fn producer_auto_batch_getter_and_setter_follow_accumulator_lifecycle() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let mut config = ProducerConfig::new("auto-batch-lifecycle-fixture").unwrap();
    config.set_auto_batch(true);
    let producer = Producer::new(config);

    assert!(
        !producer.auto_batch(),
        "Java returns false before accumulator initialization"
    );
    producer.init_produce_accumulator();
    assert_eq!(
        producer.auto_batch(),
        fixture["autoBatchLifecycle"]["enabledAfterAccumulator"]
    );
    producer.set_auto_batch(false);
    assert_eq!(
        producer.auto_batch(),
        fixture["autoBatchLifecycle"]["disabledAfterRuntimeToggle"]
    );
    producer.set_auto_batch(true);
    assert_eq!(
        producer.auto_batch(),
        fixture["autoBatchLifecycle"]["enabledAfterRuntimeToggle"]
    );
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
            .snapshot()
            .into_iter()
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

    let runtime = &fixture["runtimeSettings"];
    let producer = Producer::new(ProducerConfig::new("runtime-settings-fixture").unwrap());
    producer.set_enable_backpressure_for_async_mode(true);
    producer
        .set_backpressure_async_send_num(runtime["sendNumAfterUpdate"].as_i64().unwrap() as i32);
    producer
        .set_backpressure_async_send_size(runtime["sendSizeAfterUpdate"].as_i64().unwrap() as i32);
    assert_eq!(
        producer.enable_backpressure_for_async_mode(),
        runtime["backpressureEnabledAfterToggle"]
    );
    assert_eq!(
        producer.backpressure_async_send_num(),
        runtime["sendNumAfterUpdate"].as_i64().unwrap() as i32
    );
    assert_eq!(
        producer.backpressure_async_send_size(),
        runtime["sendSizeAfterUpdate"].as_i64().unwrap() as i32
    );
    producer.set_backpressure_async_send_num(1);
    producer.set_backpressure_async_send_size(1);
    assert_eq!(
        producer.backpressure_async_send_num(),
        runtime["clampedSendNumAfterUpdate"].as_i64().unwrap() as i32
    );
    assert_eq!(
        producer.backpressure_async_send_size(),
        runtime["clampedSendSizeAfterUpdate"].as_i64().unwrap() as i32
    );
    let live_codes = producer.retry_response_codes();
    assert!(live_codes.insert(9001));
    assert_eq!(
        live_codes.contains(&9001),
        runtime["setMutationVisibleThroughGetter"]
    );
    producer.add_retry_response_code(9002);
    assert_eq!(
        live_codes.contains(&9002),
        runtime["addMethodVisibleThroughExistingSet"]
    );
    live_codes.remove(&9001);
    assert_eq!(
        !producer.retry_response_codes().contains(&9001),
        runtime["setRemovalVisibleThroughGetter"]
    );

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

    let executor_fixture = &fixture["executorSettings"];
    assert_eq!(executor_fixture["defaultAsyncExecutorStable"], true);
    assert_eq!(executor_fixture["customAsyncExecutorRetained"], true);
    assert_eq!(executor_fixture["nullAsyncExecutorRestoresDefault"], true);
    assert_eq!(
        executor_fixture["callbackExecutorBeforeStart"],
        "NullPointerException"
    );

    struct UnusedExecutor;
    impl AsyncSenderExecutor for UnusedExecutor {
        fn execute(&self, task: AsyncSenderTask) -> Result<(), AsyncSenderTask> {
            Err(task)
        }
    }

    let producer = Producer::new(ProducerConfig::new("executor-fixture").unwrap());
    let default_executor = producer.async_sender_executor();
    assert!(Arc::ptr_eq(
        &default_executor,
        &producer.async_sender_executor()
    ));
    let executor: Arc<dyn AsyncSenderExecutor> = Arc::new(UnusedExecutor);
    producer.set_async_sender_executor(Some(executor.clone()));
    assert!(Arc::ptr_eq(&producer.async_sender_executor(), &executor));
    producer.set_async_sender_executor(None);
    assert!(Arc::ptr_eq(
        &producer.async_sender_executor(),
        &default_executor
    ));
}

#[tokio::test]
async fn producer_callback_executor_requires_an_initialized_client() {
    let producer = Producer::new(ProducerConfig::new("callback-before-start").unwrap());
    assert!(matches!(
        producer.set_callback_executor(None).await,
        Err(ProducerError::NotStarted)
    ));
}

#[test]
fn check_forbidden_context_and_modes_match_pinned_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["checkForbiddenContext"];
    let mut message = Message::new("orders", b"body".to_vec());
    let mut queue = rocketmq_classic_common::message::MessageQueue::new("orders", "broker-a", 7);
    let context = CheckForbiddenContext {
        name_srv_addr: Some("namesrv.example:9876".into()),
        group: "producer-group".into(),
        message: &mut message,
        queue: &mut queue,
        broker_addr: "broker.example:10911".into(),
        communication_mode: CommunicationMode::Async,
        send_result: None,
        exception: None,
        arg: Some(Box::new("opaque-argument".to_owned())),
        unit_mode: true,
    };

    assert_eq!(
        context.name_srv_addr.as_deref(),
        expected["nameSrvAddr"].as_str()
    );
    assert_eq!(context.group, expected["group"].as_str().unwrap());
    assert_eq!(context.message.topic(), expected["messageTopic"]);
    assert_eq!(
        context.message.body(),
        expected["messageBody"].as_str().unwrap().as_bytes()
    );
    assert_eq!(
        context.queue.topic(),
        Some(expected["queueTopic"].as_str().unwrap())
    );
    assert_eq!(
        context.queue.broker_name(),
        Some(expected["queueBroker"].as_str().unwrap())
    );
    assert_eq!(
        context.queue.queue_id(),
        expected["queueId"].as_i64().unwrap() as i32
    );
    assert_eq!(
        context.broker_addr,
        expected["brokerAddr"].as_str().unwrap()
    );
    assert_eq!(
        context.communication_mode.java_name(),
        expected["communicationMode"].as_str().unwrap()
    );
    assert_eq!(
        context.send_result.is_none(),
        expected["sendResultIsNull"].as_bool().unwrap()
    );
    assert_eq!(
        context.exception.is_none(),
        expected["exceptionIsNull"].as_bool().unwrap()
    );
    assert_eq!(
        context
            .arg
            .as_deref()
            .and_then(|value| value.downcast_ref::<String>())
            .map(String::as_str),
        expected["arg"].as_str()
    );
    assert_eq!(context.unit_mode, expected["unitMode"].as_bool().unwrap());
    assert_eq!(
        CommunicationMode::values().map(CommunicationMode::java_name),
        expected["communicationModes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|mode| mode.as_str().unwrap())
            .collect::<Vec<_>>()
            .as_slice()
    );
}

#[test]
fn send_message_context_surface_matches_pinned_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["sendMessageContext"]["values"];
    let producer = Producer::new(ProducerConfig::new("producer-group").unwrap());
    let mut message = Message::new("orders", b"body".to_vec());
    let hook_failure = std::io::Error::other("hook failure");
    let context = SendMessageContext {
        producer_group: "producer-group".into(),
        producer: &producer,
        message: &mut message,
        queue: MessageQueue::new("orders", "broker-a", 7),
        broker_addr: "broker.example:10911".into(),
        born_host: Some("192.0.2.1".into()),
        communication_mode: CommunicationMode::Async,
        send_result: None,
        exception: Some(&hook_failure),
        message_type: SendMessageType::Delay,
        namespace: Some("tenant".into()),
        trace_context: Some(Box::new("trace-context".to_owned())),
        properties: Some(HashMap::from([("source".to_owned(), "hook".to_owned())])),
    };

    assert_eq!(context.producer_group, expected["producerGroup"]);
    assert_eq!(context.producer.producer_group(), "producer-group");
    assert_eq!(context.message.topic(), expected["messageTopic"]);
    assert_eq!(
        context.message.body(),
        expected["messageBody"].as_str().unwrap().as_bytes()
    );
    assert_eq!(
        context.queue.topic(),
        Some(expected["queueTopic"].as_str().unwrap())
    );
    assert_eq!(
        context.queue.broker_name(),
        Some(expected["queueBroker"].as_str().unwrap())
    );
    assert_eq!(
        context.queue.queue_id(),
        expected["queueId"].as_i64().unwrap() as i32
    );
    assert_eq!(context.broker_addr, expected["brokerAddr"]);
    assert_eq!(context.born_host.as_deref(), expected["bornHost"].as_str());
    assert_eq!(
        context.communication_mode.java_name(),
        expected["communicationMode"]
    );
    assert!(context.send_result.is_none());
    let exception_message = context.exception.map(ToString::to_string);
    assert_eq!(
        exception_message.as_deref(),
        expected["exceptionMessage"].as_str()
    );
    assert!(
        context
            .exception
            .is_some_and(|exception| exception.is::<std::io::Error>())
    );
    assert_eq!(
        context
            .trace_context
            .as_deref()
            .and_then(|value| value.downcast_ref::<String>())
            .map(String::as_str),
        expected["traceContext"].as_str()
    );
    assert_eq!(
        context.properties.as_ref().unwrap()["source"],
        expected["props"]["source"]
    );
    assert_eq!(context.message_type.java_name(), expected["messageType"]);
    assert_eq!(context.namespace.as_deref(), expected["namespace"].as_str());
    assert_eq!(
        CommunicationMode::values().map(CommunicationMode::java_name),
        fixture["sendMessageContext"]["communicationModes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|mode| mode.as_str().unwrap())
            .collect::<Vec<_>>()
            .as_slice()
    );
    assert_eq!(
        SendMessageType::values().map(SendMessageType::java_name),
        [
            "Normal_Msg",
            "Trans_Msg_Half",
            "Trans_msg_Commit",
            "Delay_Msg",
            "Order_Msg"
        ]
    );
}
