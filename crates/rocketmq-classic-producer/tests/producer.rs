use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use rocketmq_classic_common::{
    message::{Message, MessageQueue},
    sysflag::message::CompressionType,
};
use rocketmq_classic_core::route::TopicRouteDataHandle;
use rocketmq_classic_producer::{
    AsyncSenderExecutor, AsyncSenderTask, CheckForbiddenContext, CheckForbiddenHook,
    CommunicationMode, Producer, ProducerConfig, ProducerError, ProducerPublishQueueList,
    ProducerQueueIndex, ProducerServiceState, PublishInfoQueueFilter, SendMessageContext,
    SendMessageHook, SendMessageType, TopicPublishInfo, TopicPublishInfoTableError,
    validation::{
        validate_group, validate_message, validate_send_topic, validate_system_topic,
        validate_topic,
    },
};
use rocketmq_classic_protocol::route::{QueueData, TopicRouteData};

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
        "org.apache.rocketmq.common.message.MessageQueue org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.invokeMessageQueueSelector(org.apache.rocketmq.common.message.Message,org.apache.rocketmq.client.producer.MessageQueueSelector,java.lang.Object,long)",
        "java.util.Set org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getPublishTopicList()",
        "boolean org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.isPublishTopicNeedUpdate(java.lang.String)",
        "void org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.updateTopicPublishInfo(java.lang.String,org.apache.rocketmq.client.impl.producer.TopicPublishInfo)",
        "org.apache.rocketmq.client.producer.TransactionCheckListener org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.checkListener()",
        "org.apache.rocketmq.client.producer.TransactionListener org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.getCheckListener()",
        "void org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.checkTransactionState(java.lang.String,org.apache.rocketmq.common.message.MessageExt,org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader)",
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
fn pinned_java_default_producer_facade_and_inherited_interfaces_are_recorded() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let surfaces = &fixture["producerFacadeApi"];
    let producer = &surfaces["DefaultMQProducer"];
    let producer_methods = producer["publicMethods"].as_array().unwrap();

    assert_eq!(
        producer["className"],
        "org.apache.rocketmq.client.producer.DefaultMQProducer"
    );
    assert_eq!(producer["publicMethodCount"].as_u64(), Some(183));
    assert_eq!(producer_methods.len(), 183);
    assert_eq!(producer["declaredPublicMethodCount"].as_u64(), Some(107));
    assert_eq!(
        producer["declaredPublicMethods"].as_array().unwrap().len(),
        107
    );
    let constructors = producer["publicConstructors"].as_array().unwrap();
    assert_eq!(constructors.len(), 11);
    for constructor in [
        "org.apache.rocketmq.client.producer.DefaultMQProducer()",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,boolean,java.lang.String)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,java.lang.String)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,java.lang.String,org.apache.rocketmq.remoting.RPCHook)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,java.lang.String,org.apache.rocketmq.remoting.RPCHook,boolean,java.lang.String)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,org.apache.rocketmq.remoting.RPCHook)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,org.apache.rocketmq.remoting.RPCHook,boolean,java.lang.String)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,org.apache.rocketmq.remoting.RPCHook,java.util.List)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(java.lang.String,org.apache.rocketmq.remoting.RPCHook,java.util.List,boolean,java.lang.String)",
        "org.apache.rocketmq.client.producer.DefaultMQProducer(org.apache.rocketmq.remoting.RPCHook)",
    ] {
        assert!(
            constructors
                .iter()
                .any(|candidate| candidate.as_str() == Some(constructor)),
            "missing constructor {constructor}"
        );
    }
    for method in [
        "void org.apache.rocketmq.client.ClientConfig.setNamesrvAddr(java.lang.String)",
        "org.apache.rocketmq.client.producer.SendResult org.apache.rocketmq.client.producer.DefaultMQProducer.send(org.apache.rocketmq.common.message.Message,org.apache.rocketmq.client.producer.MessageQueueSelector,java.lang.Object)",
    ] {
        assert!(
            producer_methods
                .iter()
                .any(|candidate| candidate.as_str() == Some(method)),
            "missing {method}"
        );
    }
    let declared_methods = producer["declaredPublicMethods"].as_array().unwrap();
    for method in [
        "org.apache.rocketmq.client.producer.SendResult org.apache.rocketmq.client.producer.DefaultMQProducer.sendDirect(org.apache.rocketmq.common.message.Message,org.apache.rocketmq.common.message.MessageQueue,org.apache.rocketmq.client.producer.SendCallback)",
        "org.apache.rocketmq.client.producer.SendResult org.apache.rocketmq.client.producer.DefaultMQProducer.sendByAccumulator(org.apache.rocketmq.common.message.Message,org.apache.rocketmq.common.message.MessageQueue,org.apache.rocketmq.client.producer.SendCallback)",
    ] {
        assert!(
            declared_methods
                .iter()
                .any(|candidate| candidate.as_str() == Some(method)),
            "missing {method}"
        );
    }

    let mq_producer = &surfaces["MQProducer"];
    assert_eq!(
        mq_producer["className"],
        "org.apache.rocketmq.client.producer.MQProducer"
    );
    assert_eq!(mq_producer["publicMethodCount"].as_u64(), Some(42));
    assert_eq!(mq_producer["publicMethods"].as_array().unwrap().len(), 42);
    assert_eq!(mq_producer["declaredPublicMethodCount"].as_u64(), Some(34));
    assert_eq!(
        mq_producer["declaredPublicMethods"]
            .as_array()
            .unwrap()
            .len(),
        34
    );
    assert!(
        mq_producer["publicConstructors"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(mq_producer["publicMethods"].as_array().unwrap().iter().any(|candidate| {
        candidate.as_str()
            == Some("long org.apache.rocketmq.client.MQAdmin.maxOffset(org.apache.rocketmq.common.message.MessageQueue)")
    }));
    let mq_admin = &surfaces["MQAdmin"];
    assert_eq!(mq_admin["className"], "org.apache.rocketmq.client.MQAdmin");
    assert_eq!(mq_admin["publicMethodCount"].as_u64(), Some(8));
    assert_eq!(mq_admin["publicMethods"].as_array().unwrap().len(), 8);
    assert_eq!(mq_admin["declaredPublicMethodCount"].as_u64(), Some(8));
    assert_eq!(
        mq_admin["declaredPublicMethods"].as_array().unwrap().len(),
        8
    );
    assert!(
        mq_admin["publicConstructors"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn pinned_java_returned_runtime_and_fault_strategy_surfaces_are_recorded() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let client_factory = &fixture["producerClientFactoryApi"];
    assert_eq!(
        client_factory["className"],
        "org.apache.rocketmq.client.impl.factory.MQClientInstance"
    );
    assert_eq!(
        client_factory["publicConstructors"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(client_factory["publicMethodCount"].as_u64(), Some(51));
    assert_eq!(
        client_factory["publicMethods"].as_array().unwrap().len(),
        51
    );
    for method in [
        "java.lang.String org.apache.rocketmq.client.impl.factory.MQClientInstance.getClientId()",
        "org.apache.rocketmq.remoting.protocol.route.TopicRouteData org.apache.rocketmq.client.impl.factory.MQClientInstance.queryTopicRouteData(java.lang.String)",
        "boolean org.apache.rocketmq.client.impl.factory.MQClientInstance.sendHeartbeatToAllBrokerWithLock()",
        "org.apache.rocketmq.client.impl.MQClientAPIImpl org.apache.rocketmq.client.impl.factory.MQClientInstance.getMQClientAPIImpl()",
    ] {
        assert!(
            client_factory["publicMethods"]
                .as_array()
                .unwrap()
                .iter()
                .any(|candidate| candidate.as_str() == Some(method)),
            "missing {method}"
        );
    }

    let strategy = &fixture["mqFaultStrategyApi"];
    assert_eq!(
        strategy["className"],
        "org.apache.rocketmq.client.latency.MQFaultStrategy"
    );
    assert_eq!(strategy["publicConstructors"].as_array().unwrap().len(), 2);
    assert_eq!(strategy["declaredPublicMethodCount"].as_u64(), Some(16));
    assert_eq!(strategy["publicMethodCount"].as_u64(), Some(17));
    assert!(
        strategy["publicMethods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|method| {
                method.as_str()
                    == Some("void org.apache.rocketmq.client.latency.MQFaultStrategy.start()")
            })
    );
    assert!(
        strategy["publicMethods"]
            .as_array()
            .unwrap()
            .iter()
            .any(|method| {
                method.as_str()
                    == Some("void org.apache.rocketmq.common.utils.StartAndShutdown.preShutdown()")
            })
    );

    let broker_filter = &fixture["mqFaultStrategyBrokerFilterApi"];
    assert_eq!(
        broker_filter["className"],
        "org.apache.rocketmq.client.latency.MQFaultStrategy$BrokerFilter"
    );
    assert_eq!(
        broker_filter["publicConstructors"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(broker_filter["publicMethodCount"].as_u64(), Some(2));
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
fn implementation_publish_info_table_and_nested_aliases_match_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationPublishInfoTable"];
    let producer = Producer::new(ProducerConfig::new("producer-publish-info-table").unwrap());
    let table = producer.get_topic_publish_info_table();
    let info = TopicPublishInfo::default();
    let original = MessageQueue::new("orders", "broker-a", 0);
    let replacement = MessageQueue::new("orders", "broker-b", 1);
    let original_list = ProducerPublishQueueList::new(vec![original.clone()]);
    info.set_message_queue_list(Some(original_list.clone()));

    let mut route = TopicRouteData::default();
    route.set_queue_datas_owned(Some(vec![Some(QueueData {
        broker_name: Some("broker-b".to_owned()),
        write_queue_nums: 3,
        ..QueueData::default()
    })]));
    let route = TopicRouteDataHandle::new(route);
    info.set_topic_route_data(Some(route.clone()));
    info.set_order_topic(true);
    info.set_have_topic_router_info(true);
    let send_index = ProducerQueueIndex::default();
    info.set_send_which_queue(send_index.clone());

    assert!(table.put("orders", info.clone()).is_none());
    let from_table = producer
        .get_topic_publish_info_table()
        .get("orders")
        .unwrap();
    assert_eq!(
        table.get("orders").unwrap().ptr_eq(&info),
        expected["tableValueRetainsIdentity"]
    );
    assert!(from_table.ptr_eq(&info));
    assert!(
        from_table
            .message_queue_list()
            .unwrap()
            .ptr_eq(&original_list)
    );
    assert!(from_table.topic_route_data().unwrap().ptr_eq(&route));
    assert!(from_table.send_which_queue().ptr_eq(&send_index));

    let retained_list = from_table.message_queue_list().unwrap();
    retained_list.with_mut(|queues| {
        queues.clear();
        queues.push(replacement.clone());
    });
    assert_eq!(
        from_table.ok(),
        expected["queueListMutationAffectsOk"].as_bool().unwrap()
    );
    assert_eq!(
        from_table.select_one_message_queue().unwrap(),
        replacement,
        "the same live queue-list mutation must change subsequent selection"
    );
    assert_eq!(
        from_table.is_order_topic(),
        expected["orderTopic"].as_bool().unwrap()
    );
    assert_eq!(
        from_table.have_topic_router_info(),
        expected["haveTopicRouterInfo"].as_bool().unwrap()
    );
    assert_eq!(
        from_table
            .get_write_queue_nums_by_broker("broker-b")
            .unwrap(),
        expected["writeQueueNumsByBroker"].as_i64().unwrap() as i32
    );
    assert!(from_table.to_string().contains("haveTopicRouterInfo=true"));

    let stale_list = from_table.message_queue_list().unwrap();
    from_table.set_message_queues(Some(vec![original.clone()]));
    stale_list.with_mut(Vec::clear);
    let current_list = from_table.message_queue_list().unwrap();
    assert!(!current_list.ptr_eq(&stale_list));
    assert_eq!(current_list.snapshot(), [original]);

    let replacement_info = TopicPublishInfo::default();
    let previous = table.put("orders", replacement_info.clone()).unwrap();
    assert!(previous.ptr_eq(&info));
    assert!(table.get("orders").unwrap().ptr_eq(&replacement_info));
    assert!(table.remove("orders").is_some());
    assert!(table.is_empty());
    assert!(expected["getterReturnsLiveMap"].as_bool().unwrap());
    assert!(expected["queueListRetainsIdentity"].as_bool().unwrap());
    assert!(
        expected["queueListMutationAffectsSelection"]
            .as_bool()
            .unwrap()
    );
    assert!(expected["sendIndexRetainsIdentity"].as_bool().unwrap());
    assert!(expected["routeDataRetainsIdentity"].as_bool().unwrap());
    assert!(expected["listSetterReplacesAlias"].as_bool().unwrap());
    assert!(
        expected["tablePutReturnsPreviousIdentity"]
            .as_bool()
            .unwrap()
    );
    assert!(expected["replacementValueIsVisible"].as_bool().unwrap());
    assert!(expected["removeAffectsRetainedMap"].as_bool().unwrap());
    assert!(expected["toStringContainsState"].as_bool().unwrap());
}

#[test]
fn publish_info_table_remapping_and_live_views_match_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationPublishInfoTableAdvanced"];
    let table = Producer::new(ProducerConfig::new("publish-info-table-advanced").unwrap())
        .get_topic_publish_info_table();
    let first = TopicPublishInfo::default();
    let second = TopicPublishInfo::default();
    let third = TopicPublishInfo::default();

    assert!(
        table
            .compute_if_absent("compute", |_| Some(first.clone()))
            .unwrap()
            .ptr_eq(&first)
    );
    assert!(
        table
            .compute_if_present("compute", |_, _| Some(second.clone()))
            .unwrap()
            .ptr_eq(&second)
    );
    assert_eq!(
        expected["computeIfPresentIdentity"].as_bool(),
        Some(table.get("compute").unwrap().ptr_eq(&second))
    );
    assert!(
        table
            .merge("compute", third.clone(), |current, _| Some(current.clone()))
            .unwrap()
            .ptr_eq(&second)
    );
    assert_eq!(
        expected["mergeKeepsCurrentIdentity"].as_bool(),
        Some(table.get("compute").unwrap().ptr_eq(&second))
    );
    assert!(table.compute("compute", |_, _| None).is_none());
    assert!(!table.contains_key("compute"));
    assert!(expected["computeIfAbsentIdentity"].as_bool().unwrap());
    assert!(expected["computeNullRemoves"].as_bool().unwrap());

    table.put("replace", first.clone());
    assert!(
        table
            .replace("replace", second.clone())
            .unwrap()
            .ptr_eq(&first)
    );
    assert!(table.replace_if_equal("replace", &second, third.clone()));
    table.replace_all(|_, _| second.clone());
    assert!(table.get("replace").unwrap().ptr_eq(&second));
    assert!(
        expected["replaceReturnsPreviousIdentity"]
            .as_bool()
            .unwrap()
    );
    assert!(
        expected["conditionalReplaceUsesIdentity"]
            .as_bool()
            .unwrap()
    );
    assert!(expected["replaceAllUsesLiveValues"].as_bool().unwrap());

    let keys = table.key_set_view();
    table.put("key-view", first.clone());
    assert!(keys.remove("key-view"));
    assert!(!table.contains_key("key-view"));
    table.put("key-bulk-remove", first.clone());
    table.put("key-bulk-retain", second.clone());
    assert_eq!(
        keys.remove_all(&BTreeSet::from(["key-bulk-remove".to_owned()])),
        1
    );
    assert_eq!(
        keys.retain_all(&BTreeSet::from(["key-bulk-retain".to_owned()])),
        1
    );
    assert!(matches!(
        table.put_all_nullable_rows(Some(&[
            (Some("put-all-before".to_owned()), Some(first.clone())),
            (Some("put-all-null".to_owned()), None),
            (Some("put-all-after".to_owned()), Some(third.clone())),
        ])),
        Err(TopicPublishInfoTableError::NullValue)
    ));
    assert!(table.contains_key("put-all-before"));
    assert!(!table.contains_key("put-all-after"));
    table.remove("put-all-before");
    assert!(expected["keyViewBulkMutation"].as_bool().unwrap());
    assert!(expected["putAllNullValueKeepsEarlier"].as_bool().unwrap());

    let values = table.values_view();
    table.put("value-view", first.clone());
    assert!(values.contains(&first));
    assert!(values.remove(&first));
    assert!(!table.contains_key("value-view"));
    table.put("value-bulk-a", first.clone());
    table.put("value-bulk-b", first.clone());
    table.put("value-bulk-c", second.clone());
    assert_eq!(values.remove_all(&[first.clone()]), 2);
    assert!(table.contains_key("value-bulk-c"));
    table.put("value-retain-a", first.clone());
    table.put("value-retain-b", second.clone());
    assert_eq!(values.retain_all(&[first.clone()]), 3);
    assert!(table.contains_key("value-retain-a"));
    assert!(expected["valuesViewBulkMutation"].as_bool().unwrap());

    let entries = table.entry_set_view();
    table.put("entry-view", first.clone());
    let entry = entries
        .snapshot()
        .into_iter()
        .find(|entry| entry.key() == "entry-view")
        .unwrap();
    assert!(entry.set_value(third.clone()).ptr_eq(&first));
    assert!(table.get("entry-view").unwrap().ptr_eq(&third));
    assert!(entries.remove("entry-view", &third));
    assert!(!table.contains_key("entry-view"));
    table.put("entry-bulk-remove", first.clone());
    table.put("entry-bulk-retain", third.clone());
    assert_eq!(
        entries.remove_all(&[("entry-bulk-remove".to_owned(), first.clone())]),
        1
    );
    assert_eq!(
        entries.retain_all(&[("entry-bulk-retain".to_owned(), third.clone())]),
        1
    );
    assert_eq!(table.len(), 1);
    assert!(table.get("entry-bulk-retain").unwrap().ptr_eq(&third));
    assert!(expected["entryViewBulkMutation"].as_bool().unwrap());

    table.put("key-remove-if", first.clone());
    assert_eq!(keys.remove_if(|topic| topic == "key-remove-if"), 1);
    assert!(!table.contains_key("key-remove-if"));
    table.put("value-remove-if-match", first.clone());
    table.put("value-remove-if-keep", second.clone());
    assert_eq!(values.remove_if(|value| value.ptr_eq(&first)), 1);
    assert!(!table.contains_key("value-remove-if-match"));
    assert!(table.get("value-remove-if-keep").unwrap().ptr_eq(&second));
    table.put("entry-remove-if-match", first.clone());
    table.put("entry-remove-if-keep", third.clone());
    assert_eq!(
        entries.remove_if(|topic, _| topic == "entry-remove-if-match"),
        1
    );
    assert!(!table.contains_key("entry-remove-if-match"));
    assert!(table.get("entry-remove-if-keep").unwrap().ptr_eq(&third));
    assert!(expected["keyViewRemoveIfMutation"].as_bool().unwrap());
    assert!(expected["valuesViewRemoveIfMutation"].as_bool().unwrap());
    assert!(expected["entryViewRemoveIfMutation"].as_bool().unwrap());

    table.clear();
    assert!(keys.is_empty());
    assert!(values.is_empty());
    assert!(entries.is_empty());
    assert!(expected["keyViewRemovesFromMap"].as_bool().unwrap());
    assert!(expected["valuesViewRemovesByIdentity"].as_bool().unwrap());
    assert!(
        expected["entryViewSetValueWritesThrough"]
            .as_bool()
            .unwrap()
    );
    assert!(
        expected["entryViewRemoveUsesKeyAndIdentity"]
            .as_bool()
            .unwrap()
    );
    assert!(expected["clearAffectsRetainedViews"].as_bool().unwrap());

    assert!(
        table
            .get_or_default("missing", second.clone())
            .ptr_eq(&second)
    );
    table.put("map-op-present", first.clone());
    assert!(
        table
            .get_or_default("map-op-present", second.clone())
            .ptr_eq(&first)
    );
    assert!(table.contains_value(&first));
    assert!(
        table
            .put_if_absent("map-op-present", second.clone())
            .unwrap()
            .ptr_eq(&first)
    );
    assert!(
        table
            .put_if_absent("map-op-absent", second.clone())
            .is_none()
    );
    assert!(table.get("map-op-absent").unwrap().ptr_eq(&second));
    assert!(table.remove_if_equal("map-op-absent", &second));
    assert!(!table.contains_key("map-op-absent"));
    table.put("for-each-a", first.clone());
    table.put("for-each-b", second.clone());
    let mut for_each_rows = Vec::new();
    table.for_each(|key, value| {
        for_each_rows.push(format!(
            "{key}:{}",
            if value.ptr_eq(&first) {
                "first"
            } else {
                "other"
            }
        ));
    });
    assert_eq!(for_each_rows.len(), 3);
    assert!(for_each_rows.contains(&"map-op-present:first".to_owned()));
    assert!(for_each_rows.contains(&"for-each-a:first".to_owned()));
    assert!(for_each_rows.contains(&"for-each-b:other".to_owned()));
    assert!(expected["getOrDefaultMissingIdentity"].as_bool().unwrap());
    assert!(expected["getOrDefaultPresentIdentity"].as_bool().unwrap());
    assert!(expected["containsValueIdentity"].as_bool().unwrap());
    assert!(
        expected["putIfAbsentKeepsCurrentIdentity"]
            .as_bool()
            .unwrap()
    );
    assert!(expected["putIfAbsentInserts"].as_bool().unwrap());
    assert!(expected["conditionalRemoveUsesIdentity"].as_bool().unwrap());
    assert!(expected["forEachVisitsLiveEntries"].as_bool().unwrap());
    assert!(expected["sizeAndEmptyMatch"].as_bool().unwrap());
    table.clear();
    assert!(table.is_empty());
    assert!(expected["clearEmptiesLiveMap"].as_bool().unwrap());
}

#[test]
fn publish_info_table_nullable_arguments_match_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationPublishInfoTableAdvanced"];
    let table = Producer::new(ProducerConfig::new("publish-info-table-nullable").unwrap())
        .get_topic_publish_info_table();
    let first = TopicPublishInfo::default();
    let second = TopicPublishInfo::default();
    table.put("nullable", first.clone());

    macro_rules! assert_table_error {
        ($actual:expr, $expected:pat) => {
            assert!(matches!($actual, Err($expected)));
        };
    }

    for name in [
        "nullGetKey",
        "nullGetOrDefaultKey",
        "nullContainsKey",
        "nullContainsValue",
        "nullPutKey",
        "nullPutValue",
        "nullPutIfAbsentKey",
        "nullPutIfAbsentValue",
        "nullPutAllMap",
        "nullRemoveKey",
        "nullReplaceKey",
        "nullReplaceValue",
        "nullConditionalReplaceExpected",
        "nullConditionalReplaceValue",
        "nullComputeIfAbsentKey",
        "nullComputeIfPresentKey",
        "nullComputeKey",
        "nullMergeKey",
        "nullMergeValue",
        "nullComputeIfAbsentCallback",
        "nullComputeIfPresentCallback",
        "nullComputeCallback",
        "nullMergeCallback",
        "nullForEachCallback",
        "nullReplaceAllCallback",
        "nullKeySetContains",
        "nullKeySetRemove",
        "nullValuesContains",
        "nullKeySetRemoveAll",
        "nullKeySetRetainAll",
        "nullValuesRemoveAll",
        "nullValuesRetainAll",
        "nullEntrySetRemoveAll",
        "nullEntrySetRetainAll",
        "nullKeySetRemoveIf",
        "nullValuesRemoveIf",
        "nullEntrySetRemoveIf",
        "nullEntrySetValue",
        "nullReplaceAllResult",
    ] {
        assert_eq!(
            expected[name].as_str(),
            Some("NullPointerException"),
            "{name}"
        );
    }

    assert_eq!(expected["nullConditionalRemoveValue"], true);
    assert_eq!(expected["nullGetOrDefaultFallbackAllowed"], true);
    assert_eq!(expected["nullValuesRemove"], true);
    assert_eq!(expected["nullEntrySetContains"], true);
    assert_eq!(expected["nullEntrySetRemove"], true);
    assert_eq!(expected["nullKeyEntrySetContains"], true);
    assert_eq!(expected["nullValueEntrySetContains"], true);
    assert_eq!(expected["nullKeyEntrySetRemove"], true);
    assert_eq!(expected["nullValueEntrySetRemove"], true);
    assert_eq!(expected["nullArgumentsPreserveMapping"], true);
    assert_eq!(expected["nullEntrySetValuePreservesMapping"], true);
    assert_eq!(expected["nullReplaceAllResultPreservesMapping"], true);
    assert_eq!(expected["computeIfAbsentNullResultAllowed"], true);
    assert_eq!(expected["computeIfPresentNullResultRemoves"], true);
    assert_eq!(expected["computeNullResultRemoves"], true);
    assert_eq!(expected["mergeNullResultRemoves"], true);

    assert_table_error!(
        table.get_nullable(None),
        TopicPublishInfoTableError::NullKey
    );
    assert!(matches!(
        table.get_or_default_nullable(Some("missing"), None),
        Ok(None)
    ));
    assert_table_error!(
        table.contains_key_nullable(None),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.contains_value_nullable(None),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.put_nullable(None, Some(second.clone())),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.put_nullable(Some("nullable-value"), None),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.put_if_absent_nullable(None, Some(second.clone())),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.put_if_absent_nullable(Some("nullable"), None),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.get_or_default_nullable(None, None),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.put_all_nullable_rows(None),
        TopicPublishInfoTableError::NullMap
    );
    assert_table_error!(
        table.remove_nullable(None),
        TopicPublishInfoTableError::NullKey
    );
    assert!(
        !table
            .remove_if_equal_nullable(Some("nullable"), None)
            .unwrap()
    );
    assert_table_error!(
        table.remove_if_equal_nullable(None, None),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.replace_nullable(None, Some(second.clone())),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.replace_nullable(Some("nullable"), None),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.replace_if_equal_nullable(Some("nullable"), None, Some(second.clone())),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.replace_if_equal_nullable(None, None, None),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.replace_if_equal_nullable(Some("nullable"), Some(&first), None),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.compute_if_absent_nullable(None, |_| Some(second.clone())),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.compute_if_present_nullable(None, |_, _| Some(second.clone())),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.compute_nullable(None, |_, _| Some(second.clone())),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.merge_nullable(
            None,
            Some(second.clone()),
            None::<&mut fn(&TopicPublishInfo, &TopicPublishInfo) -> Option<TopicPublishInfo>>
        ),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        table.merge_nullable(
            Some("nullable"),
            None,
            None::<&mut fn(&TopicPublishInfo, &TopicPublishInfo) -> Option<TopicPublishInfo>>
        ),
        TopicPublishInfoTableError::NullValue
    );
    assert_table_error!(
        table.merge_nullable(
            Some("nullable"),
            Some(second.clone()),
            None::<&mut fn(&TopicPublishInfo, &TopicPublishInfo) -> Option<TopicPublishInfo>>,
        ),
        TopicPublishInfoTableError::NullCallback
    );
    assert_table_error!(
        table.compute_if_absent_with_nullable_callback(
            Some("nullable"),
            None::<&mut fn(&str) -> Option<TopicPublishInfo>>,
        ),
        TopicPublishInfoTableError::NullCallback
    );
    assert_table_error!(
        table.compute_if_present_with_nullable_callback(
            Some("nullable"),
            None::<&mut fn(&str, &TopicPublishInfo) -> Option<TopicPublishInfo>>,
        ),
        TopicPublishInfoTableError::NullCallback
    );
    assert_table_error!(
        table.compute_with_nullable_callback(
            Some("nullable"),
            None::<&mut fn(&str, Option<&TopicPublishInfo>) -> Option<TopicPublishInfo>>,
        ),
        TopicPublishInfoTableError::NullCallback
    );
    assert_table_error!(
        table.replace_all_with_nullable_callback(
            None::<&mut fn(&str, &TopicPublishInfo) -> Option<TopicPublishInfo>>,
        ),
        TopicPublishInfoTableError::NullCallback
    );
    assert_table_error!(
        table.for_each_nullable(None::<&mut fn(&str, &TopicPublishInfo)>),
        TopicPublishInfoTableError::NullCallback
    );

    let keys = table.key_set_view();
    let values = table.values_view();
    let entries = table.entry_set_view();
    assert_table_error!(
        keys.contains_nullable(None),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        keys.remove_nullable(None),
        TopicPublishInfoTableError::NullKey
    );
    assert_table_error!(
        keys.remove_all_nullable(None),
        TopicPublishInfoTableError::NullCollection
    );
    assert_table_error!(
        keys.retain_all_nullable(None),
        TopicPublishInfoTableError::NullCollection
    );
    assert_table_error!(
        keys.remove_if_with_nullable_callback(None::<&mut fn(&str) -> bool>),
        TopicPublishInfoTableError::NullCallback
    );
    assert_table_error!(
        values.contains_nullable(None),
        TopicPublishInfoTableError::NullValue
    );
    assert!(matches!(values.remove_nullable(None), Ok(false)));
    assert_table_error!(
        values.remove_all_nullable(None),
        TopicPublishInfoTableError::NullCollection
    );
    assert_table_error!(
        values.retain_all_nullable(None),
        TopicPublishInfoTableError::NullCollection
    );
    assert_table_error!(
        values.remove_if_with_nullable_callback(None::<&mut fn(&TopicPublishInfo) -> bool>),
        TopicPublishInfoTableError::NullCallback
    );
    assert!(!entries.contains_nullable(None));
    assert!(!entries.contains_entry_nullable(None, Some(&first)));
    assert!(!entries.contains_entry_nullable(Some("nullable"), None));
    assert!(!entries.remove_nullable(None));
    assert!(!entries.remove_entry_nullable(None, Some(&first)));
    assert!(!entries.remove_entry_nullable(Some("nullable"), None));
    assert_table_error!(
        entries.remove_all_nullable(None),
        TopicPublishInfoTableError::NullCollection
    );
    assert_table_error!(
        entries.retain_all_nullable(None),
        TopicPublishInfoTableError::NullCollection
    );
    assert_table_error!(
        entries.remove_if_with_nullable_callback(None::<&mut fn(&str, &TopicPublishInfo) -> bool>),
        TopicPublishInfoTableError::NullCallback
    );
    let entry = entries.snapshot().into_iter().next().unwrap();
    assert_table_error!(
        entry.set_value_nullable(None),
        TopicPublishInfoTableError::NullValue
    );
    assert!(entry.value().ptr_eq(&first));
    assert!(table.get("nullable").unwrap().ptr_eq(&first));

    assert!(
        table
            .compute_if_absent_nullable(Some("compute-absent-null"), |_| None)
            .unwrap()
            .is_none()
    );
    table.put("compute-present-null", second.clone());
    assert!(
        table
            .compute_if_present_nullable(Some("compute-present-null"), |_, _| None)
            .unwrap()
            .is_none()
    );
    assert!(!table.contains_key("compute-present-null"));
    table.put("compute-null-result", second.clone());
    assert!(
        table
            .compute_nullable(Some("compute-null-result"), |_, _| None)
            .unwrap()
            .is_none()
    );
    assert!(!table.contains_key("compute-null-result"));
    table.put("merge-null-result", second);
    let mut drop_merge = |_: &TopicPublishInfo, _: &TopicPublishInfo| None;
    assert!(
        table
            .merge_nullable(
                Some("merge-null-result"),
                Some(first.clone()),
                Some(&mut drop_merge)
            )
            .unwrap()
            .is_none()
    );
    assert!(!table.contains_key("merge-null-result"));

    table.clear();
    table.put("replace-all-null-result", first.clone());
    assert_table_error!(
        table.replace_all_nullable(|_, _| None),
        TopicPublishInfoTableError::NullRemappingResult
    );
    assert!(table.get("replace-all-null-result").unwrap().ptr_eq(&first));
}

#[test]
fn publish_info_queue_filter_nullability_and_order_match_java_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
    let expected = &fixture["implementationQueueFilterSelection"];
    let info = TopicPublishInfo::default();
    let queue = MessageQueue::new("orders", "broker-a", 2);
    info.set_message_queue_list(Some(ProducerPublishQueueList::new(vec![queue.clone()])));

    assert_eq!(
        info.select_one_message_queue_with_nullable_filters(None)
            .unwrap(),
        Some(queue.clone())
    );
    assert_eq!(
        info.select_one_message_queue_with_nullable_filters(Some(&[]))
            .unwrap(),
        Some(queue.clone())
    );
    assert_eq!(
        expected["nullFilterArraySelection"].as_i64(),
        Some(i64::from(queue.queue_id()))
    );
    assert_eq!(
        expected["emptyFilterArraySelection"].as_i64(),
        Some(i64::from(queue.queue_id()))
    );

    let null_filter = [None];
    assert!(matches!(
        info.select_one_message_queue_with_nullable_filters(Some(&null_filter)),
        Err(ProducerError::NullPublishInfoQueueFilter)
    ));
    assert_eq!(
        expected["nullFilterWithQueue"].as_str(),
        Some("NullPointerException")
    );

    info.set_message_queue_list(Some(ProducerPublishQueueList::default()));
    assert!(
        info.select_one_message_queue_with_nullable_filters(Some(&null_filter))
            .unwrap()
            .is_none()
    );
    assert!(expected["nullFilterWithEmptyQueueList"].is_null());

    info.set_message_queue_list(None);
    assert!(
        info.select_one_message_queue_with_nullable_filters(Some(&null_filter))
            .unwrap()
            .is_none()
    );
    assert!(expected["nullFilterWithNullQueueList"].is_null());

    let calls = std::sync::Mutex::new(Vec::new());
    let reject = |_: &MessageQueue| {
        calls.lock().unwrap().push("reject");
        false
    };
    let accept = |_: &MessageQueue| {
        calls.lock().unwrap().push("accept");
        true
    };
    let filters: [Option<PublishInfoQueueFilter<'_>>; 2] = [Some(&reject), Some(&accept)];
    info.set_message_queue_list(Some(ProducerPublishQueueList::new(vec![queue.clone()])));
    assert!(
        info.select_one_message_queue_with_nullable_filters(Some(&filters))
            .unwrap()
            .is_none()
    );
    assert_eq!(*calls.lock().unwrap(), ["reject", "accept"]);
    assert!(expected["nonShortCircuitSelection"].is_null());
    assert_eq!(
        *calls.lock().unwrap(),
        expected["nonShortCircuitFilterOrder"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry.as_str().unwrap())
            .collect::<Vec<_>>()
    );
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
