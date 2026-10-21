use rocketmq_classic_common::message::{MessageExt, MessageQueue};
use rocketmq_classic_consumer::{
    PopProcessQueue, ProcessQueue, PullConsumerConfig, PushConsumer, PushConsumerConfig,
    pull::PullConsumer,
};
use rocketmq_classic_protocol::body::{ConsumerPopProcessQueueInfo, ConsumerProcessQueueInfo};
use serde_json::Value;

const JAVA_FACADE_API: &str = include_str!("../../../fixtures/consumer/facade-api.json");

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
    let dispatch = queue.put_message(&[first.clone(), second.clone()]);
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
    map_queue.put_message(&[original]);
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
    duplicate_queue.put_message(&[original_duplicate]);
    let duplicate_dispatch = duplicate_queue.put_message(&[replacement_duplicate]);
    let duplicate_tree = duplicate_queue.get_msg_tree_map();
    let duplicate_body_size = duplicate_tree.read().get(&30).unwrap().body().len();
    let dropped_queue = ProcessQueue::new();
    dropped_queue.set_dropped(true);
    let dropped_dispatch = dropped_queue.put_message(&[first.clone()]);
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
    assert_eq!(
        pull_rebalance
            .create_pop_process_queue()
            .get_wai_ack_msg_count(),
        0
    );
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
