use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, ConsumerRuntime, ProducerRuntime, RuntimeError},
};
use rocketmq_classic_protocol::heartbeat::{ConsumerData, HeartbeatData, SubscriptionData};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};
use tokio::time::{Duration, Instant};

struct Producer;
impl ProducerRuntime for Producer {}

struct Consumer(Mutex<ConsumerData>);
impl ConsumerRuntime for Consumer {
    fn heartbeat_data(&self) -> ConsumerData {
        self.0.lock().unwrap().clone()
    }
}

fn compare_java(name: &str, actual: &HeartbeatData) {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/heartbeat.json")).unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["case"] == name)
        .unwrap();
    let expected: HeartbeatData = serde_json::from_value(case["body"].clone()).unwrap();
    assert_eq!(actual, &expected, "{name}");
}

#[tokio::test]
async fn duplicate_groups_and_stale_identity_tokens_cannot_remove_replacements() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("192.0.2.6"))
        .await
        .unwrap();
    let producer = Arc::new(Producer);
    let old = runtime
        .register_producer("group", producer.clone())
        .unwrap();
    assert!(matches!(
        runtime.register_producer("group", producer.clone()),
        Err(RuntimeError::DuplicateGroup { .. })
    ));
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(runtime.unregister_producer(&old, deadline).await.unwrap());
    let replacement = runtime
        .register_producer("group", producer.clone())
        .unwrap();
    assert!(!runtime.unregister_producer(&old, deadline).await.unwrap());
    assert_eq!(
        runtime
            .prepare_heartbeat_data(false)
            .unwrap()
            .producer_data_set
            .len(),
        1
    );
    assert!(
        runtime
            .unregister_producer(&replacement, deadline)
            .await
            .unwrap()
    );
    assert!(
        runtime
            .prepare_heartbeat_data(false)
            .unwrap()
            .producer_data_set
            .is_empty()
    );
}

#[tokio::test]
async fn heartbeat_snapshots_follow_consumer_versions_and_without_subscription_flag() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("192.0.2.6"))
        .await
        .unwrap();
    let producer = Arc::new(Producer);
    compare_java("empty", &runtime.prepare_heartbeat_data(false).unwrap());
    let consumer = Arc::new(Consumer(Mutex::new(ConsumerData {
        group_name: "consumer".into(),
        subscription_data_set: HashSet::from([SubscriptionData {
            topic: "topic".into(),
            sub_string: "*".into(),
            sub_version: 12,
            ..Default::default()
        }]),
        ..Default::default()
    })));
    let producer_lease = runtime
        .register_producer("producer", producer.clone())
        .unwrap();
    compare_java(
        "producer-only",
        &runtime.prepare_heartbeat_data(false).unwrap(),
    );
    let consumer_lease = runtime
        .register_consumer("consumer", consumer.clone())
        .unwrap();
    let first = runtime.prepare_heartbeat_data(false).unwrap();
    compare_java("both", &first);
    assert_eq!(first.client_id.as_deref(), Some("192.0.2.6@DEFAULT"));
    assert_eq!(first.producer_data_set.len(), 1);
    assert_eq!(
        first
            .consumer_data_set
            .iter()
            .next()
            .unwrap()
            .subscription_data_set
            .iter()
            .next()
            .unwrap()
            .sub_version,
        12
    );
    consumer.0.lock().unwrap().subscription_data_set = HashSet::from([SubscriptionData {
        topic: "other".into(),
        sub_string: "tag".into(),
        sub_version: 13,
        ..Default::default()
    }]);
    let second = runtime.prepare_heartbeat_data(false).unwrap();
    compare_java("subscription-change", &second);
    assert_ne!(first, second);
    let without = runtime.prepare_heartbeat_data(true).unwrap();
    compare_java("without-sub", &without);
    assert!(without.is_without_sub);
    assert!(
        without
            .consumer_data_set
            .iter()
            .next()
            .unwrap()
            .subscription_data_set
            .is_empty()
    );
    let deadline = Instant::now() + Duration::from_secs(1);
    assert!(
        runtime
            .unregister_producer(&producer_lease, deadline)
            .await
            .unwrap()
    );
    compare_java(
        "consumer-only",
        &runtime.prepare_heartbeat_data(false).unwrap(),
    );
    assert_eq!(
        runtime
            .prepare_heartbeat_data(false)
            .unwrap()
            .consumer_data_set
            .len(),
        1
    );
    assert!(
        runtime
            .unregister_consumer(&consumer_lease, deadline)
            .await
            .unwrap()
    );
    compare_java(
        "after-unregister",
        &runtime.prepare_heartbeat_data(false).unwrap(),
    );
    assert!(
        runtime
            .prepare_heartbeat_data(false)
            .unwrap()
            .consumer_data_set
            .is_empty()
    );
}

#[tokio::test]
async fn admin_leases_are_separate_from_heartbeat_groups_and_runtime_identity() {
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("first"))
        .await
        .unwrap();
    let other = manager
        .get_or_create(&ClientConfig::new("other"))
        .await
        .unwrap();
    let lease = runtime.register_admin("admin").unwrap();
    assert!(matches!(
        runtime.register_admin("admin"),
        Err(RuntimeError::DuplicateGroup { .. })
    ));
    assert!(!other.unregister_admin(&lease).unwrap());
    assert!(
        runtime
            .prepare_heartbeat_data(false)
            .unwrap()
            .producer_data_set
            .is_empty()
    );
    assert!(runtime.unregister_admin(&lease).unwrap());
    assert!(!runtime.unregister_admin(&lease).unwrap());
}

#[tokio::test]
async fn heartbeat_callbacks_can_reenter_registry_and_registries_do_not_create_owner_cycles() {
    use rocketmq_classic_core::runtime::ClientRuntime;
    struct ReentrantConsumer(std::sync::Weak<ClientRuntime>);
    impl ConsumerRuntime for ReentrantConsumer {
        fn heartbeat_data(&self) -> ConsumerData {
            let runtime = self.0.upgrade().unwrap();
            assert!(runtime.select_producer("producer").unwrap().is_some());
            assert!(runtime.select_consumer("consumer").unwrap().is_some());
            ConsumerData {
                group_name: "consumer".into(),
                ..Default::default()
            }
        }
    }
    struct OwningProducer(Arc<ClientRuntime>);
    impl ProducerRuntime for OwningProducer {
        fn publish_topics(&self) -> std::collections::BTreeSet<String> {
            assert!(!self.0.client_id().is_empty());
            Default::default()
        }
    }
    let manager = ClientRuntimeManager::new();
    let runtime = manager
        .get_or_create(&ClientConfig::new("first"))
        .await
        .unwrap();
    let weak = Arc::downgrade(&runtime);
    let producer = Arc::new(OwningProducer(runtime.clone()));
    let consumer = Arc::new(ReentrantConsumer(weak.clone()));
    let _producer_lease = runtime
        .register_producer("producer", producer.clone())
        .unwrap();
    let _consumer_lease = runtime
        .register_consumer("consumer", consumer.clone())
        .unwrap();
    let (done, result) = std::sync::mpsc::channel();
    let runner = runtime.clone();
    let thread = std::thread::spawn(move || {
        done.send(runner.prepare_heartbeat_data(false).unwrap())
            .unwrap()
    });
    assert_eq!(
        result
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap()
            .consumer_data_set
            .len(),
        1
    );
    thread.join().unwrap();
    drop(manager);
    drop(runtime);
    assert!(weak.upgrade().is_some()); // Producer owns the runtime.
    drop(producer);
    assert!(weak.upgrade().is_none()); // Registry is weak; no owner cycle.
}
