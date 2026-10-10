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
    use rocketmq_classic_core::runtime::RuntimeError;
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
    let rows = |value: &serde_json::Value| {
        if value.is_null() {
            return None;
        }
        Some(
            value
                .as_array()
                .unwrap()
                .iter()
                .map(|row| {
                    let nested_queue = row.get("queue");
                    let queue = nested_queue.unwrap_or(row);
                    let queue = if nested_queue.is_some() && queue.is_null() {
                        None
                    } else {
                        Some(MessageQueue::new_nullable(
                            queue["topic"].as_str().map(str::to_owned),
                            queue["brokerName"].as_str().map(str::to_owned),
                            queue["queueId"].as_i64().unwrap() as i32,
                        ))
                    };
                    (queue, row["offset"].as_i64())
                })
                .collect::<BTreeMap<_, _>>(),
        )
    };
    for case in fixture["cases"].as_array().unwrap() {
        let output = runtime
            .parse_offset_table_from_broker(rows(&case["input"]), case["namespace"].as_str())
            .unwrap();
        assert_eq!(Some(output), rows(&case["output"]));
        assert_eq!(
            runtime.config().read().unwrap().namespace.as_deref(),
            Some("ignored-config-namespace")
        );
    }
    for case in fixture["nullableCases"].as_array().unwrap() {
        let result = runtime
            .parse_offset_table_from_broker(rows(&case["input"]), case["namespace"].as_str());
        if case["error"].is_null() {
            assert_eq!(
                result.unwrap(),
                rows(&case["output"]).unwrap(),
                "{}",
                case["name"]
            );
        } else {
            let expected_reason = match case["name"].as_str().unwrap() {
                "null-input-map"
                | "null-input-map-empty-namespace"
                | "null-input-map-nonempty-namespace" => "null offset table",
                "namespace-null-key-after-valid-queue" => "null queue key with namespace",
                name => panic!("unexpected Java offset-table error case {name}"),
            };
            match result {
                Err(RuntimeError::InvalidOffsetTable(reason)) => {
                    assert_eq!(reason, expected_reason, "{}", case["name"])
                }
                other => panic!("{}: {other:?}", case["name"]),
            }
        }
    }
}
