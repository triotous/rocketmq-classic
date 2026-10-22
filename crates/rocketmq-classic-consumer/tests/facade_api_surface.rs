use rocketmq_classic_common::message::{MessageExt, MessageQueue};
use rocketmq_classic_consumer::process_queue::ProcessQueueError;
use rocketmq_classic_consumer::{
    ConsumeMessageService, ConsumeMessageServiceFuture, ConsumerStatsManager, MqHelper,
    PopProcessQueue, ProcessQueue, PullConsumerConfig, PushConsumer, PushConsumerConfig,
    pull::PullConsumer, rebalance::AllocateMessageQueueAveragely,
};
use rocketmq_classic_protocol::body::{
    ConsumeMessageDirectlyResult, ConsumerPopProcessQueueInfo, ConsumerProcessQueueInfo,
};
use rocketmq_classic_protocol::heartbeat::ConsumeType;
use rocketmq_classic_protocol::heartbeat::MessageModel;
use serde_json::Value;
use std::sync::{
    Arc,
    atomic::{AtomicI32, AtomicUsize, Ordering},
};

const JAVA_FACADE_API: &str = include_str!("../../../fixtures/consumer/facade-api.json");

#[derive(Default)]
struct RecordingConsumeMessageService {
    core_pool_size: AtomicI32,
    start_calls: AtomicUsize,
    shutdown_calls: AtomicUsize,
    update_calls: AtomicUsize,
    increase_calls: AtomicUsize,
    decrease_calls: AtomicUsize,
    direct_calls: AtomicUsize,
    pull_submissions: AtomicUsize,
    pop_submissions: AtomicUsize,
}

impl ConsumeMessageService for RecordingConsumeMessageService {
    fn start(&self) -> ConsumeMessageServiceFuture<'_, ()> {
        self.start_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {})
    }

    fn shutdown(&self, _await_termination_millis: i64) -> ConsumeMessageServiceFuture<'_, ()> {
        self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {})
    }

    fn update_core_pool_size(&self, core_pool_size: i32) {
        self.update_calls.fetch_add(1, Ordering::SeqCst);
        self.core_pool_size.store(core_pool_size, Ordering::SeqCst);
    }

    fn inc_core_pool_size(&self) {
        self.increase_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn dec_core_pool_size(&self) {
        self.decrease_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn core_pool_size(&self) -> i32 {
        self.core_pool_size.load(Ordering::SeqCst)
    }

    fn consume_message_directly(
        &self,
        _message: MessageExt,
        _broker_name: Option<String>,
    ) -> ConsumeMessageServiceFuture<'_, ConsumeMessageDirectlyResult> {
        self.direct_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { ConsumeMessageDirectlyResult::default() })
    }

    fn submit_consume_request(
        &self,
        _messages: Vec<Arc<MessageExt>>,
        _process_queue: Arc<ProcessQueue>,
        _message_queue: MessageQueue,
        _dispatch_to_consume: bool,
    ) {
        self.pull_submissions.fetch_add(1, Ordering::SeqCst);
    }

    fn submit_pop_consume_request(
        &self,
        _messages: Vec<MessageExt>,
        _process_queue: Arc<PopProcessQueue>,
        _message_queue: MessageQueue,
    ) {
        self.pop_submissions.fetch_add(1, Ordering::SeqCst);
    }
}

fn method_named(api: &Value, method: &str) -> bool {
    api["declaredPublicMethods"]
        .as_array()
        .expect("declared Java methods")
        .iter()
        .filter_map(Value::as_str)
        .any(|signature| signature.contains(&format!(".{method}(")))
}

#[test]
fn pinned_java_consumer_facade_inventory_covers_declared_and_inherited_methods() {
    let fixture: Value = serde_json::from_str(JAVA_FACADE_API).expect("valid Java API fixture");
    assert_eq!(
        fixture["provenance"]["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );

    let apis = &fixture["apis"];
    for (name, constructors, declared, callable) in [
        ("DefaultMQPushConsumer", 11, 91, 165),
        ("MQPushConsumer", 0, 12, 23),
        ("DefaultMQPullConsumer", 5, 60, 135),
        ("MQPullConsumer", 0, 20, 31),
        ("DefaultLitePullConsumer", 5, 80, 155),
        ("LitePullConsumer", 0, 31, 31),
        ("MQAdmin", 0, 8, 8),
        ("MQHelper", 1, 2, 2),
    ] {
        let api = &apis[name];
        assert_eq!(
            api["publicConstructors"].as_array().unwrap().len(),
            constructors,
            "{name}"
        );
        assert_eq!(
            api["declaredPublicMethodCount"].as_u64(),
            Some(declared),
            "{name}"
        );
        assert_eq!(api["publicMethodCount"].as_u64(), Some(callable), "{name}");
    }

    for name in ["DefaultMQPushConsumer", "DefaultMQPullConsumer"] {
        let api = &apis[name];
        assert!(
            method_named(api, "viewMessage"),
            "{name} exposes viewMessage"
        );
        assert!(
            method_named(api, "queryMessage"),
            "{name} exposes queryMessage"
        );
        assert!(
            !method_named(api, "queryMessageByUniqKey"),
            "{name} keeps unique-key lookup behind viewMessage"
        );
    }

    let helper_methods = &apis["MQHelper"]["declaredPublicMethods"];
    for signature in [
        "void org.apache.rocketmq.client.MQHelper.resetOffsetByTimestamp(org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel,java.lang.String,java.lang.String,long)",
        "void org.apache.rocketmq.client.MQHelper.resetOffsetByTimestamp(org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel,java.lang.String,java.lang.String,java.lang.String,long)",
    ] {
        assert!(
            helper_methods
                .as_array()
                .unwrap()
                .iter()
                .any(|method| method == signature),
            "pinned MQHelper overload: {signature}"
        );
    }

    #[allow(deprecated)]
    {
        std::mem::drop(MqHelper::reset_offset_by_timestamp(
            MessageModel::Clustering,
            String::new(),
            String::new(),
            0,
        ));
    }
    std::mem::drop(MqHelper::reset_offset_by_timestamp_with_instance(
        MessageModel::Clustering,
        String::new(),
        String::new(),
        String::new(),
        0,
    ));
}

#[test]
fn consumer_stats_manager_exposes_the_pinned_java_methods() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    assert_eq!(fixture["consumerStatsManagersAreInstanceScoped"], true);
    assert_eq!(fixture["consumerStatsManagersShareFactoryIdentity"], true);
    assert_eq!(fixture["consumerStatsSamplingSecondSamplerCount"], 5);
    assert_eq!(
        fixture["consumerStatsSamplingBeforeFirstSamplePullTPS"],
        0.0
    );
    assert_eq!(fixture["consumerStatsSamplingAfterFirstSamplePullTPS"], 2.0);
    assert_eq!(
        fixture["consumerStatsSamplingAfterPendingUpdatePullTPS"],
        2.0
    );
    assert_eq!(
        fixture["consumerStatsManagerPublicConstructors"],
        serde_json::json!(["ConsumerStatsManager(ScheduledExecutorService)"])
    );
    let expected = serde_json::json!([
        "consumeStatus(String,String)",
        "incConsumeFailedTPS(String,String,long)",
        "incConsumeOKTPS(String,String,long)",
        "incConsumeRT(String,String,long)",
        "incPullRT(String,String,long)",
        "incPullTPS(String,String,long)",
        "shutdown()",
        "start()"
    ]);
    assert_eq!(fixture["consumerStatsManagerPublicMethods"], expected);

    let stats = ConsumerStatsManager::default();
    stats.start();
    stats.inc_pull_rt("workers", "orders", 12);
    stats.inc_pull_tps("workers", "orders", 3);
    stats.inc_consume_rt("workers", "orders", 4);
    stats.inc_consume_ok_tps("workers", "orders", 2);
    stats.inc_consume_failed_tps("workers", "orders", 1);
    let _status = stats.consume_status("workers", "orders");
    stats.shutdown();
}

#[tokio::test]
async fn consume_message_service_accessor_and_extension_methods_match_java() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/consumer/push-implementation-accessor.json"
    ))
    .unwrap();
    let expected_methods = serde_json::json!([
        "consumeMessageDirectly(MessageExt,String)",
        "decCorePoolSize()",
        "getCorePoolSize()",
        "incCorePoolSize()",
        "shutdown(long)",
        "start()",
        "submitConsumeRequest(List,ProcessQueue,MessageQueue,boolean)",
        "submitPopConsumeRequest(List,PopProcessQueue,MessageQueue)",
        "updateCorePoolSize(int)"
    ]);
    assert_eq!(
        fixture["consumeMessageServicePublicMethods"],
        expected_methods
    );

    let config = PushConsumerConfig::new("consume-service-accessor").unwrap();
    let consumer = PushConsumer::new(config);
    assert!(consumer.get_consume_message_service().is_none());

    let service = Arc::new(RecordingConsumeMessageService::default());
    let service_api: Arc<dyn ConsumeMessageService> = service.clone();
    consumer.set_consume_message_service(Some(service_api.clone()));
    assert!(Arc::ptr_eq(
        &consumer.get_consume_message_service().unwrap(),
        &service_api
    ));

    consumer.update_core_pool_size(5).await.unwrap();
    assert_eq!(service.update_calls.load(Ordering::SeqCst), 1);
    assert_eq!(service.core_pool_size(), 5);
    consumer.update_core_pool_size(20).await.unwrap();
    assert_eq!(service.update_calls.load(Ordering::SeqCst), 1);
    consumer.adjust_thread_pool();
    assert_eq!(service.decrease_calls.load(Ordering::SeqCst), 1);
    assert_eq!(service.increase_calls.load(Ordering::SeqCst), 0);

    service.start().await;
    let _ = service
        .consume_message_directly(MessageExt::default(), None)
        .await;
    service.submit_consume_request(
        vec![],
        Arc::new(ProcessQueue::new()),
        MessageQueue::new("orders", "broker-a", 0),
        true,
    );
    service.submit_pop_consume_request(
        vec![],
        Arc::new(PopProcessQueue::new()),
        MessageQueue::new("orders", "broker-a", 0),
    );
    service.shutdown(0).await;
    assert_eq!(service.start_calls.load(Ordering::SeqCst), 1);
    assert_eq!(service.direct_calls.load(Ordering::SeqCst), 1);
    assert_eq!(service.pull_submissions.load(Ordering::SeqCst), 1);
    assert_eq!(service.pop_submissions.load(Ordering::SeqCst), 1);
    assert_eq!(service.shutdown_calls.load(Ordering::SeqCst), 1);

    consumer.set_consume_message_service(None);
    assert!(consumer.get_consume_message_service().is_none());
}

#[test]
fn pinned_java_nested_rebalance_and_process_queue_api_denominator_is_recorded() {
    let fixture: Value = serde_json::from_str(JAVA_FACADE_API).expect("valid Java API fixture");
    assert_eq!(
        fixture["provenance"]["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );

    for (name, constructors, declared, callable) in [
        ("RebalanceImpl", 1, 32, 32),
        ("RebalancePushImpl", 2, 12, 32),
        ("RebalancePullImpl", 2, 11, 32),
        ("ProcessQueue", 1, 32, 32),
        ("PopProcessQueue", 1, 11, 11),
    ] {
        let api = &fixture["implementationApis"][name];
        assert_eq!(
            api["publicConstructors"].as_array().unwrap().len(),
            constructors,
            "{name} constructors"
        );
        assert_eq!(
            api["declaredPublicMethodCount"].as_u64(),
            Some(declared),
            "{name} declared methods"
        );
        assert_eq!(
            api["publicMethodCount"].as_u64(),
            Some(callable),
            "{name} callable methods"
        );
    }

    let implementations = &fixture["implementationApis"];
    for (owner, method) in [
        ("RebalanceImpl", "getProcessQueueTable"),
        ("RebalanceImpl", "getPopProcessQueueTable"),
        ("ProcessQueue", "cleanExpiredMsg"),
        ("ProcessQueue", "getMsgTreeMap"),
        ("PopProcessQueue", "ack"),
    ] {
        let methods = implementations[owner]["publicMethods"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        assert!(
            methods
                .iter()
                .any(|signature| signature.contains(&format!(".{method}("))),
            "pinned Java {owner}.{method} is inventoried"
        );
    }
}

#[test]
fn every_pinned_java_rebalance_method_has_an_owner_mapping_or_disposition() {
    let fixture: Value = serde_json::from_str(JAVA_FACADE_API).expect("valid Java API fixture");
    let methods = fixture["implementationApis"]["RebalanceImpl"]["publicMethods"]
        .as_array()
        .expect("pinned inherited methods");
    assert_eq!(methods.len(), 32);

    let crosswalk =
        include_str!("../../../docs/compatibility/PHASE_18_REBALANCE_PROCESS_QUEUE_API.md");
    let owners = [
        include_str!("../src/pull/consumer.rs"),
        include_str!("../src/push/consumer.rs"),
    ]
    .join("\n");
    let mappings = [
        ("clientRebalance", "client_rebalance"),
        ("doRebalance", "do_rebalance_with_order"),
        ("lock", "lock"),
        ("lockAll", "lock_all"),
        ("unlock", "unlock"),
        ("unlockAll", "unlock_all"),
        ("removeProcessQueue", "remove_process_queue"),
        (
            "removeUnnecessaryMessageQueue",
            "remove_unnecessary_message_queue",
        ),
        (
            "removeUnnecessaryPopMessageQueue",
            "remove_unnecessary_pop_process_queue",
        ),
        ("removeDirtyOffset", "remove_dirty_offset"),
        ("computePullFromWhere", "compute_pull_from_where"),
        (
            "computePullFromWhereWithException",
            "compute_pull_from_where_with_exception",
        ),
        ("getConsumeInitMode", "get_consume_init_mode"),
        ("getConsumerGroup", "get_consumer_group"),
        ("setConsumerGroup", "set_consumer_group"),
        ("getMessageModel", "message_model"),
        ("setMessageModel", "set_message_model"),
        (
            "getAllocateMessageQueueStrategy",
            "get_allocate_message_queue_strategy",
        ),
        (
            "setAllocateMessageQueueStrategy",
            "set_allocate_message_queue_strategy",
        ),
        ("getSubscriptionInner", "get_subscription_inner"),
        (
            "getTopicSubscribeInfoTable",
            "get_topic_subscribe_info_table",
        ),
        ("getProcessQueueTable", "get_process_queue_table"),
        ("getPopProcessQueueTable", "get_pop_process_queue_table"),
        ("createProcessQueue", "create_process_queue"),
        ("createPopProcessQueue", "create_pop_process_queue"),
        ("getmQClientFactory", "get_client_runtime"),
        ("consumeType", "consume_type"),
        ("destroy", "destroy"),
        ("messageQueueChanged", "message_queue_changed"),
    ];
    for signature in methods {
        let signature = signature.as_str().expect("method signature");
        let java_name = signature
            .split('(')
            .next()
            .expect("method name")
            .rsplit('.')
            .next()
            .expect("declaring class and method");
        assert!(
            crosswalk.contains(java_name),
            "Java {java_name} needs a documented mapping or disposition"
        );
        if let Some((_, rust_name)) = mappings.iter().find(|(name, _)| *name == java_name) {
            assert!(
                owners.contains(&format!("fn {rust_name}")),
                "Java {java_name} maps to a Rust owner method {rust_name}"
            );
        } else {
            assert!(
                [
                    "setmQClientFactory",
                    "dispatchPullRequest",
                    "dispatchPopPullRequest"
                ]
                .contains(&java_name),
                "unexpected unimplemented inherited method {java_name}"
            );
        }
    }
}

#[test]
fn pull_rebalance_matches_pinned_java_pop_and_offset_behavior() {
    let fixture: Value = serde_json::from_str(JAVA_FACADE_API).expect("valid Java API fixture");
    let expected = &fixture["implementationBehaviors"]["rebalancePull"];
    let config = PullConsumerConfig::new("facade-pull-rebalance").unwrap();
    let mut consumer = PullConsumer::new(config);
    consumer.set_consumer_group("pull-group-updated");
    let consumer = Arc::new(consumer);
    consumer.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
    let strategy: Arc<dyn rocketmq_classic_consumer::rebalance::AllocateMessageQueueStrategy> =
        Arc::new(AllocateMessageQueueAveragely);
    consumer.set_allocate_message_queue_strategy(strategy.clone());
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let actual = serde_json::json!({
        "groupAfterSet": consumer.get_consumer_group(),
        "messageModelAfterSet": consumer.message_model().name(),
        "strategyPreservesIdentity": Arc::ptr_eq(&strategy, &consumer.get_allocate_message_queue_strategy()),
        "clientRebalance": consumer.client_rebalance("orders"),
        "consumeType": consumer.consume_type().name(),
        "pullOffset": consumer.compute_pull_from_where(&queue),
        "pullOffsetWithException": consumer.compute_pull_from_where_with_exception(&queue).unwrap(),
        "processQueueCreated": !consumer.create_process_queue().is_dropped(),
        "popProcessQueueCreated": consumer.create_pop_process_queue().is_some(),
        "consumeInitModeError": consumer.get_consume_init_mode().unwrap_err().to_string(),
    });
    assert_eq!(&actual, expected);
    assert_eq!(consumer.consume_type(), ConsumeType::ConsumeActively);
    assert!(
        consumer
            .get_pop_process_queue_table()
            .read()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn rebalance_owner_controls_match_java_client_selection_and_settings() {
    use rocketmq_classic_protocol::heartbeat::MessageModel;

    let pull_config = PullConsumerConfig::new("facade-pull-settings").unwrap();
    let pull = Arc::new(PullConsumer::new(pull_config));
    assert!(pull.get_client_runtime().await.is_none());
    assert_eq!(pull.get_consumer_group(), "facade-pull-settings");
    assert_eq!(pull.message_model(), MessageModel::Clustering);
    assert_eq!(pull.get_allocate_message_queue_strategy().name(), "AVG");
    pull.set_message_model(MessageModel::Broadcasting);
    pull.set_allocate_message_queue_strategy(Arc::new(AllocateMessageQueueAveragely));
    assert_eq!(pull.message_model(), MessageModel::Broadcasting);
    assert!(pull.client_rebalance("orders"));
    assert!(
        pull.get_pop_process_queue_table()
            .read()
            .unwrap()
            .is_empty()
    );

    let push_config = PushConsumerConfig::new("facade-push-settings").unwrap();
    push_config.set_client_rebalance(false);
    let push = Arc::new(PushConsumer::new(push_config));
    assert!(push.get_client_runtime().await.is_none());
    assert!(!push.client_rebalance("orders"));
    push.set_message_model(MessageModel::Broadcasting);
    assert!(push.client_rebalance("orders"));
    push.set_message_model(MessageModel::Clustering);
    push.set_consume_orderly(true);
    assert!(push.client_rebalance("orders"));
    assert!(push.do_rebalance_with_order(true).await);
    push.lock_all().await;
    push.unlock_all(true).await;

    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(ProcessQueue::new());
    push.get_process_queue_table()
        .write()
        .unwrap()
        .insert(queue.clone(), process_queue.clone());
    assert!(push.remove_process_queue(&queue).await);
    assert!(process_queue.is_dropped());
    assert!(push.get_process_queue_table().read().unwrap().is_empty());
}

#[tokio::test]
async fn push_rebalance_methods_match_pinned_java_behavior() {
    use rocketmq_classic_protocol::heartbeat::MessageModel;

    let fixture: Value = serde_json::from_str(JAVA_FACADE_API).expect("valid Java API fixture");
    let expected = &fixture["implementationBehaviors"]["rebalancePush"];
    let config = PushConsumerConfig::new("push-group").unwrap();
    let push = Arc::new(PushConsumer::new(config));
    let client_rebalance_default = push.client_rebalance("orders");
    push.config().set_client_rebalance(false);
    let client_rebalance_clustering_disabled = push.client_rebalance("orders");
    push.set_message_model(MessageModel::Broadcasting);
    let client_rebalance_broadcasting = push.client_rebalance("orders");
    push.set_message_model(MessageModel::Clustering);
    push.set_consumer_group(Some("push-group-updated".to_owned()));
    let strategy = push.get_allocate_message_queue_strategy();
    push.set_allocate_message_queue_strategy(strategy.clone());
    let strategy_preserves_identity =
        Arc::ptr_eq(&strategy, &push.get_allocate_message_queue_strategy());
    let do_rebalance_empty = push.do_rebalance_with_order(false).await;
    let queue = MessageQueue::new("orders", "broker-a", 0);
    let process_queue = Arc::new(push.create_process_queue());
    let pop_process_queue = Arc::new(push.create_pop_process_queue());
    push.get_process_queue_table()
        .write()
        .unwrap()
        .insert(queue.clone(), process_queue.clone());
    push.get_pop_process_queue_table()
        .write()
        .unwrap()
        .insert(queue.clone(), pop_process_queue.clone());
    let remove_unnecessary_pop =
        push.remove_unnecessary_pop_process_queue(&queue, &pop_process_queue);
    push.destroy().await;
    let actual = serde_json::json!({
        "clientRebalanceDefault": client_rebalance_default,
        "clientRebalanceClusteringDisabled": client_rebalance_clustering_disabled,
        "clientRebalanceBroadcasting": client_rebalance_broadcasting,
        "groupAfterSet": push.get_consumer_group().unwrap(),
        "messageModelAfterSet": push.message_model().name(),
        "strategyPreservesIdentity": strategy_preserves_identity,
        "consumeType": push.consume_type().name(),
        "consumeInitMode": push.consume_init_mode(),
        "doRebalanceEmpty": do_rebalance_empty,
        "processQueueCreated": true,
        "popProcessQueueCreated": true,
        "removeUnnecessaryPop": remove_unnecessary_pop,
        "processQueueDroppedByDestroy": process_queue.is_dropped(),
        "popProcessQueueDroppedByDestroy": pop_process_queue.is_dropped(),
        "processTableEmptyAfterDestroy": push.get_process_queue_table().read().unwrap().is_empty(),
        "popTableEmptyAfterDestroy": push.get_pop_process_queue_table().read().unwrap().is_empty(),
    });
    assert_eq!(&actual, expected);
}

#[test]
fn pop_process_queue_public_methods_match_pinned_java_behavior() {
    let fixture: Value = serde_json::from_str(JAVA_FACADE_API).expect("valid Java API fixture");
    let expected = &fixture["implementationBehaviors"]["popProcessQueue"];

    let queue = PopProcessQueue::new();
    queue.inc_found_msg(4);
    queue.dec_found_msg(-2);
    let ack_result = queue.ack();
    queue.set_last_pop_timestamp(123);
    queue.set_dropped(true);

    let mut info = ConsumerPopProcessQueueInfo::default();
    queue.fill_pop_process_queue_info(&mut info);
    let actual = serde_json::json!({
        "ackResult": ack_result,
        "waitAckCount": queue.get_wai_ack_msg_count(),
        "lastPopTimestamp": queue.get_last_pop_timestamp(),
        "dropped": queue.is_dropped(),
        "info": {
            "waitAckCount": info.get_wait_ack_count(),
            "droped": info.is_droped(),
            "lastPopTimestamp": info.get_last_pop_timestamp(),
        },
        "toString": queue.to_string(),
    });
    assert_eq!(&actual, expected);
}

#[test]
fn process_queue_public_methods_match_pinned_java_behavior() {
    let fixture: Value = serde_json::from_str(JAVA_FACADE_API).expect("valid Java API fixture");
    let expected = &fixture["implementationBehaviors"]["processQueue"];

    let mut first = MessageExt::new();
    first.set_topic("orders");
    first.set_queue_id(0);
    first.set_queue_offset(10);
    first.set_body(vec![1, 2]);
    let mut second = MessageExt::new();
    second.set_topic("orders");
    second.set_queue_id(0);
    second.set_queue_offset(11);
    second.set_body(vec![3]);

    let queue = ProcessQueue::new();
    let dispatch = queue
        .put_message(&[first.clone(), second.clone()])
        .expect("valid ProcessQueue MAX_OFFSET");
    let tree_map = queue.get_msg_tree_map();
    let initial_tree_keys = tree_map.read().keys().copied().collect::<Vec<_>>();
    let initial_count = queue.get_msg_count().get();
    let initial_size = queue.get_msg_size().get();
    let initial_span = queue.get_max_span();
    let contains_first = queue.contains_message(&first);
    let has_messages = queue.has_temp_message();
    let first_taken = queue.take_messages(1)[0].queue_offset();
    let after_take_tree_keys = tree_map.read().keys().copied().collect::<Vec<_>>();
    queue.rollback();
    let after_rollback_tree_keys = tree_map.read().keys().copied().collect::<Vec<_>>();
    queue.take_messages(1);
    let committed_offset = queue.commit();
    let after_commit_tree_keys = tree_map.read().keys().copied().collect::<Vec<_>>();
    let next_offset = queue.remove_message(&[std::sync::Arc::new(second)]);
    let final_tree_keys = tree_map.read().keys().copied().collect::<Vec<_>>();

    queue.set_dropped(true);
    queue.set_locked(true);
    queue.set_last_lock_timestamp(123);
    queue.set_last_pull_timestamp(124);
    queue.set_last_consume_timestamp(150);
    let lock_expired = queue.is_lock_expired();
    let pull_expired = queue.is_pull_expired();
    queue.set_msg_acc_cnt(17);
    queue.inc_try_unlock_times();
    queue.inc_try_unlock_times();
    let mut info = ConsumerProcessQueueInfo::default();
    queue.fill_process_queue_info(&mut info);

    let mut original = MessageExt::new();
    original.set_topic("orders");
    original.set_queue_id(0);
    original.set_queue_offset(20);
    original.set_body(vec![4]);
    let mut direct = MessageExt::new();
    direct.set_topic("orders");
    direct.set_queue_id(0);
    direct.set_queue_offset(21);
    direct.set_body(vec![5, 6]);
    let map_queue = ProcessQueue::new();
    map_queue.put_message(&[original]).expect("valid message");
    let counter_previous = map_queue.get_msg_count().get_and_add(4);
    let counter_size_after_add = map_queue.get_msg_size().add_and_get(3);
    {
        let map = map_queue.get_msg_tree_map();
        let mut map = map.write();
        map.insert(21, std::sync::Arc::new(direct));
        map.remove(&20);
    }
    let direct_map = map_queue.get_msg_tree_map();
    let direct_map_keys = direct_map.read().keys().copied().collect::<Vec<_>>();

    let mut original_duplicate = MessageExt::new();
    original_duplicate.set_queue_offset(30);
    original_duplicate.set_body(vec![7, 8]);
    let mut replacement_duplicate = MessageExt::new();
    replacement_duplicate.set_queue_offset(30);
    replacement_duplicate.set_body(vec![9, 10, 11, 12]);
    let duplicate_queue = ProcessQueue::new();
    duplicate_queue
        .put_message(&[original_duplicate])
        .expect("valid message");
    let duplicate_dispatch = duplicate_queue
        .put_message(&[replacement_duplicate])
        .expect("valid message");
    let duplicate_tree = duplicate_queue.get_msg_tree_map();
    let duplicate_body_size = duplicate_tree.read().get(&30).unwrap().body().len();
    let dropped_queue = ProcessQueue::new();
    dropped_queue.set_dropped(true);
    let dropped_dispatch = dropped_queue
        .put_message(&[first.clone()])
        .expect("valid message");

    let mut in_flight_first = MessageExt::new();
    in_flight_first.set_queue_offset(50);
    in_flight_first.set_body(vec![1, 2]);
    let mut in_flight_second = MessageExt::new();
    in_flight_second.set_queue_offset(51);
    in_flight_second.set_body(vec![3, 4, 5]);
    let in_flight_removal_queue = ProcessQueue::new();
    in_flight_removal_queue
        .put_message(&[in_flight_first, in_flight_second])
        .expect("valid messages");
    let in_flight_batch = in_flight_removal_queue.take_messages(1);
    let remove_in_flight_next_offset = in_flight_removal_queue.remove_message(&in_flight_batch);
    let remove_in_flight_ready_tree_keys = in_flight_removal_queue
        .get_msg_tree_map()
        .read()
        .keys()
        .copied()
        .collect::<Vec<_>>();
    let remove_in_flight_count_before_commit = in_flight_removal_queue.get_msg_count().get();
    let remove_in_flight_size_before_commit = in_flight_removal_queue.get_msg_size().get();
    let remove_in_flight_commit_offset = in_flight_removal_queue.commit();

    let mut removed_message = MessageExt::new();
    removed_message.set_queue_offset(60);
    removed_message.set_body(vec![6]);
    let mut following_message = MessageExt::new();
    following_message.set_queue_offset(61);
    following_message.set_body(vec![7, 8]);
    let dispatch_after_remove_queue = ProcessQueue::new();
    dispatch_after_remove_queue.set_last_pull_timestamp(500);
    dispatch_after_remove_queue
        .put_message(&[removed_message.clone()])
        .expect("valid message");
    let remove_last_next_offset =
        dispatch_after_remove_queue.remove_message(&[std::sync::Arc::new(removed_message)]);
    let dispatch_after_remove = dispatch_after_remove_queue
        .put_message(&[following_message])
        .expect("valid message");
    let dispatch_after_remove_tree_keys = dispatch_after_remove_queue
        .get_msg_tree_map()
        .read()
        .keys()
        .copied()
        .collect::<Vec<_>>();
    let remove_in_flight = serde_json::json!({
        "nextOffset": remove_in_flight_next_offset,
        "readyTreeKeys": remove_in_flight_ready_tree_keys,
        "countBeforeCommit": remove_in_flight_count_before_commit,
        "sizeBeforeCommit": remove_in_flight_size_before_commit,
        "commitOffset": remove_in_flight_commit_offset,
        "countAfterCommit": in_flight_removal_queue.get_msg_count().get(),
        "sizeAfterCommit": in_flight_removal_queue.get_msg_size().get(),
    });
    let dispatch_after_remove_result = serde_json::json!({
        "removeNextOffset": remove_last_next_offset,
        "nextDispatch": dispatch_after_remove,
        "count": dispatch_after_remove_queue.get_msg_count().get(),
        "size": dispatch_after_remove_queue.get_msg_size().get(),
        "treeKeys": dispatch_after_remove_tree_keys,
        "lastPullTimestamp": dispatch_after_remove_queue.get_last_pull_timestamp(),
    });
    let mut malformed_max_offset_message = MessageExt::new();
    malformed_max_offset_message.set_queue_offset(70);
    malformed_max_offset_message.set_body(vec![7, 0]);
    malformed_max_offset_message
        .set_system_property("MAX_OFFSET", "not-a-long")
        .unwrap();
    let malformed_max_offset_queue = ProcessQueue::new();
    let malformed_max_offset_error = malformed_max_offset_queue
        .put_message(&[malformed_max_offset_message])
        .expect_err("invalid Java MAX_OFFSET should return a NumberFormatException mapping");
    assert!(matches!(
        malformed_max_offset_error,
        ProcessQueueError::InvalidMaxOffset { value } if value == "not-a-long"
    ));
    let malformed_max_offset_tree_keys = malformed_max_offset_queue
        .get_msg_tree_map()
        .read()
        .keys()
        .copied()
        .collect::<Vec<_>>();
    let mut after_max_offset_error = MessageExt::new();
    after_max_offset_error.set_queue_offset(71);
    after_max_offset_error.set_body(vec![7, 1]);
    let dispatch_after_max_offset_error = malformed_max_offset_queue
        .put_message(&[after_max_offset_error])
        .expect("valid message after malformed property");
    let malformed_max_offset_result = serde_json::json!({
        "errorClass": "java.lang.NumberFormatException",
        "countAfterError": 1,
        "sizeAfterError": 2,
        "treeKeysAfterError": malformed_max_offset_tree_keys,
        "dispatchAfterError": dispatch_after_max_offset_error,
        "finalCount": malformed_max_offset_queue.get_msg_count().get(),
        "finalSize": malformed_max_offset_queue.get_msg_size().get(),
        "finalTreeKeys": malformed_max_offset_queue
            .get_msg_tree_map()
            .read()
            .keys()
            .copied()
            .collect::<Vec<_>>(),
    });
    let actual = serde_json::json!({
        "dispatch": dispatch,
        "initialTreeKeys": initial_tree_keys,
        "initialCount": initial_count,
        "initialSize": initial_size,
        "initialSpan": initial_span,
        "containsFirst": contains_first,
        "hasMessages": has_messages,
        "firstTaken": first_taken,
        "afterTakeTreeKeys": after_take_tree_keys,
        "afterRollbackTreeKeys": after_rollback_tree_keys,
        "committedOffset": committed_offset,
        "afterCommitTreeKeys": after_commit_tree_keys,
        "nextOffset": next_offset,
        "finalTreeKeys": final_tree_keys,
        "finalCount": queue.get_msg_count().get(),
        "finalSize": queue.get_msg_size().get(),
        "lastLockTimestamp": queue.get_last_lock_timestamp(),
        "lastPullTimestamp": queue.get_last_pull_timestamp(),
        "lockExpired": lock_expired,
        "pullExpired": pull_expired,
        "lastConsumeTimestamp": queue.get_last_consume_timestamp(),
        "msgAccCnt": queue.get_msg_acc_cnt(),
        "tryUnlockTimes": queue.get_try_unlock_times(),
        "dropped": queue.is_dropped(),
        "locked": queue.is_locked(),
        "consumeLockPresent": true,
        "info": {
            "cachedMsgCount": info.get_cached_msg_count(),
            "cachedMsgSizeInMiB": info.get_cached_msg_size_in_mib(),
            "locked": info.is_locked(),
            "tryUnlockTimes": info.get_try_unlock_times(),
            "lastLockTimestamp": info.get_last_lock_timestamp(),
            "droped": info.is_droped(),
            "lastPullTimestamp": info.get_last_pull_timestamp(),
            "lastConsumeTimestamp": info.get_last_consume_timestamp(),
        },
        "directMapKeys": direct_map_keys,
        "counterPrevious": counter_previous,
        "counterSizeAfterAdd": counter_size_after_add,
        "directMapCount": map_queue.get_msg_count().get(),
        "directMapSize": map_queue.get_msg_size().get(),
        "duplicateDispatch": duplicate_dispatch,
        "duplicateCount": duplicate_queue.get_msg_count().get(),
        "duplicateSize": duplicate_queue.get_msg_size().get(),
        "duplicateBodySize": duplicate_body_size,
        "droppedDispatch": dropped_dispatch,
        "droppedQueueCount": dropped_queue.get_msg_count().get(),
        "removeInFlight": remove_in_flight,
        "dispatchAfterRemove": dispatch_after_remove_result,
        "malformedMaxOffset": malformed_max_offset_result,
    });
    assert_eq!(&actual, expected);
    queue.clear();
    map_queue.clear();
}

#[test]
fn rebalance_accessors_retain_live_topic_subscription_and_pop_tables() {
    let push = PushConsumer::new(PushConsumerConfig::new("workers").unwrap());
    let push_rebalance = push.get_rebalance_impl();
    let pop_table = push_rebalance.get_pop_process_queue_table();
    assert!(std::sync::Arc::ptr_eq(
        &pop_table,
        &push.get_rebalance_impl().get_pop_process_queue_table()
    ));
    let queue = MessageQueue::new("orders", "broker-a", 1);
    let process_table = push_rebalance.get_process_queue_table();
    assert!(std::sync::Arc::ptr_eq(
        &process_table,
        &push.get_rebalance_impl().get_process_queue_table()
    ));
    process_table.write().unwrap().insert(
        queue.clone(),
        std::sync::Arc::new(push_rebalance.create_process_queue()),
    );
    pop_table.write().unwrap().insert(
        queue.clone(),
        std::sync::Arc::new(push_rebalance.create_pop_process_queue()),
    );
    let topic_table = push_rebalance.get_topic_subscribe_info_table();
    topic_table.write().unwrap().insert(
        "orders".into(),
        std::collections::BTreeSet::from([queue.clone()]),
    );
    assert!(
        push.get_rebalance_impl()
            .get_pop_process_queue_table()
            .read()
            .unwrap()
            .contains_key(&queue)
    );
    assert!(
        push.get_rebalance_impl()
            .get_process_queue_table()
            .read()
            .unwrap()
            .contains_key(&queue)
    );
    assert!(
        push.get_rebalance_impl()
            .get_topic_subscribe_info_table()
            .read()
            .unwrap()["orders"]
            .contains(&queue)
    );

    let pull = PullConsumer::new(PullConsumerConfig::new("workers").unwrap());
    let pull_rebalance = pull.get_rebalance_impl();
    assert!(std::sync::Arc::ptr_eq(
        &pull_rebalance.get_subscription_inner(),
        &pull.get_rebalance_impl().get_subscription_inner()
    ));
    assert!(std::sync::Arc::ptr_eq(
        &pull_rebalance.get_process_queue_table(),
        &pull.get_rebalance_impl().get_process_queue_table()
    ));
    assert!(std::sync::Arc::ptr_eq(
        &pull_rebalance.get_pop_process_queue_table(),
        &pull.get_rebalance_impl().get_pop_process_queue_table()
    ));
    assert!(std::sync::Arc::ptr_eq(
        &pull_rebalance.get_topic_subscribe_info_table(),
        &pull.get_rebalance_impl().get_topic_subscribe_info_table()
    ));
    assert!(pull_rebalance.create_pop_process_queue().is_none());
}

#[test]
fn pinned_java_consumer_callable_method_names_have_phase_crosswalks() {
    let fixture: Value = serde_json::from_str(JAVA_FACADE_API).expect("valid Java API fixture");
    let apis = &fixture["apis"];
    let crosswalks = [
        include_str!("../../../docs/compatibility/PHASE_6_API_CONFIG_MAP.md"),
        include_str!("../../../docs/compatibility/PHASE_9_OFFLINE_METHOD_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_10_OFFLINE_METHOD_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_11_OFFLINE_METHOD_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_12_ORDERLY_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_13_LITE_POP_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_14_TRANSACTION_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_15_REQUEST_REPLY_MATRIX.md"),
        include_str!(
            "../../../docs/compatibility/PHASE_16_NAMESPACE_SECURITY_OPERATIONS_MATRIX.md"
        ),
        include_str!("../../../docs/compatibility/PHASE_17_ADMIN_MATRIX.md"),
    ]
    .join("\n");

    let mut missing = Vec::new();
    for (owner, expected_count) in [
        ("DefaultMQPushConsumer", 165),
        ("MQPushConsumer", 23),
        ("DefaultMQPullConsumer", 135),
        ("MQPullConsumer", 31),
        ("DefaultLitePullConsumer", 155),
        ("LitePullConsumer", 31),
        ("MQAdmin", 8),
    ] {
        let methods = apis[owner]["publicMethods"]
            .as_array()
            .expect("Java callable methods");
        assert_eq!(
            methods.len(),
            expected_count,
            "unexpected {owner} method count"
        );

        for signature in methods {
            let signature = signature.as_str().expect("Java method signature");
            let method_name = signature
                .split('(')
                .next()
                .expect("method name")
                .rsplit('.')
                .next()
                .expect("declaring type and method name");
            if !crosswalks.contains(method_name) {
                missing.push(signature);
            }
        }
    }

    assert!(
        missing.is_empty(),
        "Java consumer facade/inherited methods have no phase crosswalk:\n{}",
        missing.join("\n")
    );
}
