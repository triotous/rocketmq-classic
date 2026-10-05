use std::sync::Arc;

use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, RuntimeError},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p6_v01_many_first_lookups_share_one_unstarted_runtime_and_transport_owner() {
    let manager = ClientRuntimeManager::new();
    let config = ClientConfig::new("192.0.2.6");
    let mut calls = Vec::new();
    for _ in 0..128 {
        let manager = manager.clone();
        let config = config.clone();
        calls.push(tokio::spawn(async move {
            manager.get_or_create(&config).await.unwrap()
        }));
    }
    let first = calls.remove(0).await.unwrap();
    for call in calls {
        assert!(Arc::ptr_eq(&first, &call.await.unwrap()));
    }
    assert_eq!(first.client_id(), config.build_client_id());
}

#[tokio::test]
async fn p6_v02_incompatible_transport_configuration_is_not_silently_shared() {
    let manager = ClientRuntimeManager::new();
    let mut config = ClientConfig::new("192.0.2.6");
    let first = manager.get_or_create(&config).await.unwrap();
    config.namesrv_addr = Some("127.0.0.1:9876".into());
    assert!(matches!(
        manager.get_or_create(&config).await,
        Err(RuntimeError::ConfigurationConflict { .. })
    ));
    config.namesrv_addr = None;
    config.decode_read_body = false; // Consumer decoder policy, not transport ownership.
    assert!(Arc::ptr_eq(
        &first,
        &manager.get_or_create(&config).await.unwrap()
    ));
}

#[tokio::test]
async fn p6_v02_failed_construction_can_retry_and_stale_removal_preserves_replacement() {
    let manager = ClientRuntimeManager::new();
    let mut config = ClientConfig::new("192.0.2.6");
    config.client_callback_executor_threads = -1;
    assert!(manager.get_or_create(&config).await.is_err());
    config.client_callback_executor_threads = 1;
    let first = manager.get_or_create(&config).await.unwrap();
    assert!(
        manager
            .remove_client_factory(first.client_id(), &first)
            .unwrap()
    );
    let replacement = manager.get_or_create(&config).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &replacement));
    assert!(
        !manager
            .remove_client_factory(first.client_id(), &first)
            .unwrap()
    );
    assert!(Arc::ptr_eq(
        &replacement,
        &manager.get_or_create(&config).await.unwrap()
    ));
}
