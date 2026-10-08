use rocketmq_classic_core::{config::ClientConfig, runtime::ClientRuntimeManager};
use std::collections::BTreeMap;
use std::sync::Arc;

#[tokio::test]
async fn live_table_keeps_sharing_hooks_and_identity_cleanup_across_explicit_managers() {
    use rocketmq_classic_remoting::client::RpcHook;
    struct Hook;
    impl RpcHook for Hook {}
    let origin = ClientRuntimeManager::new();
    let target = ClientRuntimeManager::new();
    let live = target.factory_table();
    let mut config = ClientConfig::new("cross-manager-table");
    config.namesrv_addr = Some(String::new());
    let id = config.build_client_id();
    let hook: Arc<dyn RpcHook> = Arc::new(Hook);
    let first = origin
        .get_or_create_with_hook(&config, Some(hook.clone()))
        .await
        .unwrap();
    origin.remove_client_factory_by_id(&id).await.unwrap();
    let replacement = origin
        .get_or_create_with_hook(&config, Some(hook.clone()))
        .await
        .unwrap();
    live.put(&id, first.clone()).await.unwrap();
    assert!(Arc::ptr_eq(
        &first,
        &target
            .get_or_create_with_hook(&config, Some(hook.clone()))
            .await
            .unwrap()
    ));
    assert!(matches!(
        target.get_or_create(&config).await,
        Err(rocketmq_classic_core::runtime::RuntimeError::ConfigurationConflict { .. })
    ));
    assert!(matches!(
        target
            .get_or_create_with_hook(&config, Some(Arc::new(Hook)))
            .await,
        Err(rocketmq_classic_core::runtime::RuntimeError::ConfigurationConflict { .. })
    ));
    let mut incompatible = config.clone();
    incompatible.namespace_v2 = Some("different".into());
    assert!(matches!(
        target
            .get_or_create_with_hook(&incompatible, Some(hook.clone()))
            .await,
        Err(rocketmq_classic_core::runtime::RuntimeError::ConfigurationConflict { .. })
    ));
    first.start().await.unwrap();
    assert!(first.shutdown().await.unwrap());
    assert!(live.is_empty().unwrap());
    assert!(Arc::ptr_eq(
        &replacement,
        &origin
            .get_or_create_with_hook(&config, Some(hook.clone()))
            .await
            .unwrap()
    ));
    live.put(&id, replacement.clone()).await.unwrap();
    live.put("alias", replacement.clone()).await.unwrap();
    replacement.start().await.unwrap();
    assert!(replacement.shutdown().await.unwrap());
    assert!(!live.contains_key(&id).unwrap());
    assert!(Arc::ptr_eq(
        &live.get("alias").unwrap().unwrap(),
        &replacement
    ));
    live.clear().await.unwrap();
    assert!(live.is_empty().unwrap());
    assert!(origin.factory_table().is_empty().unwrap());
    drop(target);
    live.put(&id, first.clone()).await.unwrap();
    assert!(live.contains_key(&id).unwrap());
    live.clear().await.unwrap();
}

#[tokio::test]
async fn concurrent_put_if_absent_publishes_one_identity_and_clear_releases_owners() {
    let source = ClientRuntimeManager::new();
    let first = source
        .get_or_create(&ClientConfig::new("first-table-value"))
        .await
        .unwrap();
    let second = source
        .get_or_create(&ClientConfig::new("second-table-value"))
        .await
        .unwrap();
    let target = ClientRuntimeManager::new();
    let live = target.factory_table();
    let mut tasks = Vec::new();
    for i in 0..64 {
        let live = live.clone();
        let value = if i % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        tasks.push(tokio::spawn(async move {
            live.put_if_absent("alias", value.clone())
                .await
                .map(|previous| (previous, value))
        }));
    }
    let mut inserted = 0;
    let mut observed = None;
    for task in tasks {
        let (previous, candidate) = task.await.unwrap().unwrap();
        match previous {
            None => {
                inserted += 1;
                observed = Some(candidate);
            }
            Some(previous) => {
                assert!(Arc::ptr_eq(&previous, &first) || Arc::ptr_eq(&previous, &second));
            }
        }
    }
    assert_eq!(inserted, 1);
    let selected = live.get("alias").unwrap().unwrap();
    assert!(Arc::ptr_eq(&selected, &observed.take().unwrap()));
    assert_eq!(live.len().unwrap(), 1);
    let weak_first = Arc::downgrade(&first);
    let weak_second = Arc::downgrade(&second);
    drop(source);
    drop(first);
    drop(second);
    drop(selected);
    live.clear().await.unwrap();
    assert!(weak_first.upgrade().is_none());
    assert!(weak_second.upgrade().is_none());
}

#[tokio::test]
async fn retained_factory_table_and_atomic_mutations_match_invoked_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"];
    let manager = ClientRuntimeManager::new();
    let live = manager.factory_table();
    let second_view = live.clone();
    let config = ClientConfig::new("table-oracle");
    let id = config.build_client_id();
    assert!(live.is_empty().unwrap());
    let first = manager.get_or_create(&config).await.unwrap();
    manager.remove_client_factory_by_id(&id).await.unwrap();
    let second = manager.get_or_create(&config).await.unwrap();
    assert_eq!(
        Arc::ptr_eq(&live.get(&id).unwrap().unwrap(), &second),
        java["retainedViewSeesSecond"]
    );
    assert_eq!(
        Arc::ptr_eq(&live.get_or_default(&id, first.clone()).unwrap(), &second),
        java["getOrDefaultPresent"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live.get_or_default("missing", first.clone()).unwrap(),
            &first
        ),
        java["getOrDefaultAbsent"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live.put(&id, first.clone()).await.unwrap().unwrap(),
            &second
        ),
        java["putReturnsPrevious"]
    );
    assert_eq!(
        Arc::ptr_eq(&manager.get_or_create(&config).await.unwrap(), &first),
        java["managerSeesPut"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .put_if_absent(&id, second.clone())
                .await
                .unwrap()
                .unwrap(),
            &first
        ) && Arc::ptr_eq(&live.get(&id).unwrap().unwrap(), &first),
        java["putIfAbsentKeepsCurrent"]
    );
    assert_eq!(
        !live
            .replace_if_same(&id, &second, first.clone())
            .await
            .unwrap(),
        java["staleReplaceFails"]
    );
    assert_eq!(
        live.replace_if_same(&id, &first, second.clone())
            .await
            .unwrap(),
        java["matchingReplaceSucceeds"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live.replace(&id, first.clone()).await.unwrap().unwrap(),
            &second
        ),
        java["replaceReturnsPrevious"]
    );
    assert_eq!(
        !live.remove_if_same(&id, &second).await.unwrap(),
        java["staleRemoveFails"]
    );
    assert_eq!(
        live.remove_if_same(&id, &first).await.unwrap(),
        java["matchingRemoveSucceeds"]
    );
    assert_eq!(
        live.replace(&id, second.clone()).await.unwrap().is_none(),
        java["absentReplaceReturnsNull"]
    );
    assert_eq!(
        live.remove(&id).await.unwrap().is_none(),
        java["absentRemoveReturnsNull"]
    );
    assert_eq!(
        live.put_if_absent(&id, first.clone())
            .await
            .unwrap()
            .is_none(),
        java["absentPutIfAbsentReturnsNull"]
    );
    live.put("", second.clone()).await.unwrap();
    assert_eq!(
        live.len().unwrap(),
        java["sizeWithAlias"].as_u64().unwrap() as usize
    );
    assert!(live.contains_key("").unwrap());
    assert_eq!(
        Arc::ptr_eq(&live.get("").unwrap().unwrap(), &second),
        java["aliasHasSecond"]
    );
    let bulk_values = BTreeMap::from([
        ("bulk-a".to_owned(), first.clone()),
        ("bulk-b".to_owned(), second.clone()),
    ]);
    live.put_all(bulk_values).await.unwrap();
    assert_eq!(
        Arc::ptr_eq(&live.get("bulk-a").unwrap().unwrap(), &first)
            && Arc::ptr_eq(&live.get("bulk-b").unwrap().unwrap(), &second)
            && live.len().unwrap() == 4,
        java["putAllAdds"]
    );
    assert!(Arc::ptr_eq(&second_view.snapshot().unwrap()[""], &second));
    live.clear().await.unwrap();
    assert_eq!(
        second_view.is_empty().unwrap(),
        java["retainedViewEmptyAfterClear"]
    );
    assert_eq!(
        first.state(),
        rocketmq_classic_core::runtime::RuntimeState::Created
    );
    assert_eq!(
        second.state(),
        rocketmq_classic_core::runtime::RuntimeState::Created
    );
}
