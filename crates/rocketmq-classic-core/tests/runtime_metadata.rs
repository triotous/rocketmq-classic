use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, RuntimeState},
};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

fn millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::test(start_paused = true)]
async fn boot_timestamp_does_not_follow_simulated_monotonic_clock() {
    let mut config = ClientConfig::new("metadata-clock");
    config.namesrv_addr = Some(String::new());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    let boot = runtime.boot_timestamp();
    tokio::time::advance(std::time::Duration::from_secs(86_400)).await;
    assert_eq!(runtime.boot_timestamp(), boot);
}

#[tokio::test]
async fn boot_timestamp_is_constructor_wall_clock_and_stable_across_shared_lifecycle() {
    let manager = ClientRuntimeManager::new();
    let mut config = ClientConfig::new("metadata");
    config.namesrv_addr = Some(String::new());
    let before = millis();
    let runtime = manager.get_or_create(&config).await.unwrap();
    let after = millis();
    let boot = runtime.boot_timestamp();
    assert!((before..=after).contains(&boot));
    assert_eq!(runtime.state(), RuntimeState::Created);
    let shared = manager.get_or_create(&config).await.unwrap();
    assert!(Arc::ptr_eq(&runtime, &shared));
    assert_eq!(shared.boot_timestamp(), boot);
    runtime.start().await.unwrap();
    assert_eq!(runtime.boot_timestamp(), boot);
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.boot_timestamp(), boot);
    let before = millis();
    let replacement = manager.get_or_create(&config).await.unwrap();
    let after = millis();
    assert!(!Arc::ptr_eq(&runtime, &replacement));
    assert!((before..=after).contains(&replacement.boot_timestamp()));
}

#[tokio::test]
async fn explicit_offset_namespace_conversion_matches_invoked_java() {
    use rocketmq_classic_common::message::MessageQueue;
    use std::collections::BTreeMap;
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/offsets.json")).unwrap();
    let mut config = ClientConfig::new("offset-client");
    config.set_namespace(Some("ignored-config-namespace".into()));
    config.namesrv_addr = Some(String::new());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let rows = |name: &str| {
            case[name]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| {
                    (
                        MessageQueue::new(
                            row["topic"].as_str().unwrap(),
                            row["brokerName"].as_str().unwrap(),
                            row["queueId"].as_i64().unwrap() as i32,
                        ),
                        row["offset"].as_i64().unwrap(),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        let output =
            runtime.parse_offset_table_from_broker(rows("input"), case["namespace"].as_str());
        assert_eq!(output, rows("output"));
        assert_eq!(
            runtime.config().read().unwrap().namespace.as_deref(),
            Some("ignored-config-namespace")
        );
    }
}
