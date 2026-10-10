use rocketmq_classic_core::{config::ClientConfig, runtime::ClientRuntimeManager};
use std::collections::{BTreeMap, BTreeSet};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

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
    assert!(Arc::ptr_eq(
        &first,
        &target.get_or_create(&config).await.unwrap()
    ));
    assert!(Arc::ptr_eq(
        &first,
        &target
            .get_or_create_with_hook(&config, Some(Arc::new(Hook)))
            .await
            .unwrap()
    ));
    let mut incompatible = config.clone();
    incompatible.namespace_v2 = Some("different".into());
    assert!(Arc::ptr_eq(
        &first,
        &target
            .get_or_create_with_hook(&incompatible, Some(hook.clone()))
            .await
            .unwrap()
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
    let key_view = live.key_set_view();
    let values_view = live.values_view();
    let entry_view = live.entry_set_view();
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
        live.contains_value(&second).unwrap(),
        java["containsValueSecond"]
    );
    assert_eq!(
        !live.contains_value(&first).unwrap(),
        java["containsValueAbsent"]
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
    let expected = BTreeMap::from([
        (id.clone(), first.clone()),
        (String::new(), second.clone()),
        ("bulk-a".to_owned(), first.clone()),
        ("bulk-b".to_owned(), second.clone()),
    ]);
    let mut visited = BTreeMap::new();
    second_view
        .for_each(|key, value| {
            // A callback can reenter the table because traversal holds no manager lock.
            let _ = second_view.len().unwrap();
            visited.insert(
                key.to_owned(),
                expected
                    .get(key)
                    .is_some_and(|expected| Arc::ptr_eq(value, expected)),
            );
        })
        .unwrap();
    assert_eq!(
        visited.len() == expected.len() && visited.values().all(|matches| *matches),
        java["forEachVisitsCurrentMappings"]
    );
    assert_eq!(
        key_view.contains("bulk-a").unwrap() && key_view.len().unwrap() == 4,
        java["keyViewSeesBulk"]
    );
    assert_eq!(
        values_view.contains(&first).unwrap()
            && values_view.contains(&second).unwrap()
            && values_view.len().unwrap() == 4,
        java["valuesViewSeesBulk"]
    );
    assert_eq!(
        entry_view.contains("bulk-a", &first).unwrap() && entry_view.len().unwrap() == 4,
        java["entryViewSeesBulk"]
    );
    assert_eq!(
        key_view.remove_if(|key| key == "bulk-b").await.unwrap() > 0,
        java["keyViewRemovesBulk"]
    );
    assert_eq!(
        entry_view
            .remove_if(|key, value| key == "bulk-a" && Arc::ptr_eq(value, &first))
            .await
            .unwrap()
            > 0,
        java["entryViewRemovesBulk"]
    );
    assert_eq!(
        values_view
            .remove_if(|value| Arc::ptr_eq(value, &first))
            .await
            .unwrap()
            > 0,
        java["valuesViewRemovesCurrent"]
    );
    assert_eq!(
        !key_view.contains("bulk-b").unwrap()
            && !entry_view.contains("bulk-a", &first).unwrap()
            && !key_view.contains(&id).unwrap()
            && values_view.len().unwrap() == 1,
        java["viewsSeeRemovals"]
    );
    live.put("bulk-c", first.clone()).await.unwrap();
    live.put("bulk-d", second.clone()).await.unwrap();
    assert_eq!(
        key_view
            .remove_all(&BTreeSet::from(["bulk-c".to_owned()]))
            .await
            .unwrap()
            > 0,
        java["keyViewRemoveAll"]
    );
    assert_eq!(
        values_view.remove_all(&[second.clone()]).await.unwrap() > 0 && live.is_empty().unwrap(),
        java["valuesViewRemoveAll"]
    );
    live.put("keep", first.clone()).await.unwrap();
    live.put("drop", second.clone()).await.unwrap();
    let retained_entry = BTreeMap::from([("keep".to_owned(), first.clone())]);
    assert_eq!(
        entry_view.retain_all(&retained_entry).await.unwrap() > 0
            && entry_view.len().unwrap() == 1
            && entry_view.contains("keep", &first).unwrap(),
        java["entryViewRetainAll"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .compute_if_absent("compute-a", |_| Some(first.clone()))
                .await
                .unwrap()
                .unwrap(),
            &first
        ),
        java["computeIfAbsentMaps"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .compute_if_absent("compute-a", |_| Some(second.clone()))
                .await
                .unwrap()
                .unwrap(),
            &first
        ),
        java["computeIfAbsentKeepsCurrent"]
    );
    assert_eq!(
        live.compute_if_absent("compute-null", |_| None)
            .await
            .unwrap()
            .is_none()
            && !live.contains_key("compute-null").unwrap(),
        java["computeIfAbsentNullLeavesAbsent"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .compute_if_present("compute-a", |_, _| Some(second.clone()))
                .await
                .unwrap()
                .unwrap(),
            &second
        ) && entry_view.contains("compute-a", &second).unwrap(),
        java["computeIfPresentReplaces"]
    );
    assert_eq!(
        live.compute_if_present("compute-a", |_, _| None)
            .await
            .unwrap()
            .is_none()
            && !live.contains_key("compute-a").unwrap(),
        java["computeIfPresentNullRemoves"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .compute("compute-a", |_, _| Some(first.clone()))
                .await
                .unwrap()
                .unwrap(),
            &first
        ) && entry_view.contains("compute-a", &first).unwrap(),
        java["computeReplaces"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .compute("compute-b", |_, _| Some(second.clone()))
                .await
                .unwrap()
                .unwrap(),
            &second
        ),
        java["computeAbsentInserts"]
    );
    assert_eq!(
        live.compute("compute-b", |_, _| None)
            .await
            .unwrap()
            .is_none()
            && !live.contains_key("compute-b").unwrap(),
        java["computeNullRemoves"]
    );
    assert_eq!(
        live.compute("compute-null-absent", |_, _| None)
            .await
            .unwrap()
            .is_none()
            && !live.contains_key("compute-null-absent").unwrap(),
        java["computeAbsentNullLeavesAbsent"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .merge("compute-a", second.clone(), |_, added| Some(added.clone()))
                .await
                .unwrap()
                .unwrap(),
            &second
        ),
        java["mergeCombines"]
    );
    assert_eq!(
        Arc::ptr_eq(
            &live
                .merge("merge-new", first.clone(), |_, _| None)
                .await
                .unwrap()
                .unwrap(),
            &first
        ),
        java["mergeAbsentInserts"]
    );
    live.put("merge-remove", second.clone()).await.unwrap();
    assert_eq!(
        live.merge("merge-remove", first.clone(), |_, _| None)
            .await
            .unwrap()
            .is_none()
            && !live.contains_key("merge-remove").unwrap(),
        java["mergeNullRemoves"]
    );
    // The Java fixture also retains the manager ID mapping from its preceding
    // identity-removal contention case, so replaceAll observes four entries.
    live.put(&id, first.clone()).await.unwrap();
    live.replace_all(|_, _| second.clone()).await.unwrap();
    assert_eq!(
        live.len().unwrap() == 3
            && live
                .snapshot()
                .unwrap()
                .values()
                .all(|value| Arc::ptr_eq(value, &second)),
        java["replaceAllUpdatesMappings"]
    );
    assert!(Arc::ptr_eq(
        &second_view.snapshot().unwrap()["keep"],
        &second
    ));
    live.clear().await.unwrap();
    assert_eq!(
        second_view.is_empty().unwrap(),
        java["retainedViewEmptyAfterClear"]
    );
    assert_eq!(
        key_view.is_empty().unwrap()
            && values_view.is_empty().unwrap()
            && entry_view.is_empty().unwrap(),
        java["collectionViewsEmptyAfterClear"]
    );
    live.put("iterator-key", first.clone()).await.unwrap();
    assert_eq!(
        key_view.remove("iterator-key").await.unwrap() && live.is_empty().unwrap(),
        java["keyIteratorRemove"]
    );
    live.put("iterator-value", first.clone()).await.unwrap();
    assert_eq!(
        values_view.remove(&first).await.unwrap() && live.is_empty().unwrap(),
        java["valuesIteratorRemove"]
    );
    live.put("iterator-entry", first.clone()).await.unwrap();
    assert_eq!(
        entry_view.remove("iterator-entry", &first).await.unwrap() && live.is_empty().unwrap(),
        java["entryIteratorRemove"]
    );
    live.put("entry-set-value", first.clone()).await.unwrap();
    let entry = entry_view.entry_snapshots().unwrap().pop().unwrap();
    assert_eq!(
        Arc::ptr_eq(&entry.set_value(second.clone()).await.unwrap(), &first),
        java["entrySetValueReturnsPrevious"]
    );
    assert_eq!(
        Arc::ptr_eq(&entry.value().unwrap(), &second),
        java["entrySetValueUpdatesEntry"]
    );
    assert_eq!(
        Arc::ptr_eq(&live.get("entry-set-value").unwrap().unwrap(), &second),
        java["entrySetValueUpdatesTable"]
    );
    live.put("entry-set-value", first.clone()).await.unwrap();
    assert_eq!(
        Arc::ptr_eq(&entry.value().unwrap(), &second),
        java["entrySetValueEntryIsSnapshot"]
    );
    assert_eq!(
        Arc::ptr_eq(&entry.set_value(second.clone()).await.unwrap(), &second),
        java["entrySetValueReturnsCapturedPrevious"]
    );
    assert_eq!(
        Arc::ptr_eq(&live.get("entry-set-value").unwrap().unwrap(), &second),
        java["entrySetValueReplacesCurrentMapping"]
    );
    live.clear().await.unwrap();
    assert_eq!(
        first.state(),
        rocketmq_classic_core::runtime::RuntimeState::Created
    );
    assert_eq!(
        second.state(),
        rocketmq_classic_core::runtime::RuntimeState::Created
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_factory_table_remapping_serializes_same_key_callback_like_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"];
    let manager = ClientRuntimeManager::new();
    let live = manager.factory_table();
    let first = manager
        .get_or_create(&ClientConfig::new("factory-remap-race-current"))
        .await
        .unwrap();
    let second = manager
        .get_or_create(&ClientConfig::new("factory-remap-race-next"))
        .await
        .unwrap();
    live.put("same-key", first).await.unwrap();

    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback_calls_for_task = callback_calls.clone();
    let replacement = second.clone();
    let table_for_compute = live.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let computation = tokio::spawn(async move {
        table_for_compute
            .compute("same-key", move |_, _| {
                if callback_calls_for_task.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                Some(replacement.clone())
            })
            .await
            .unwrap()
            .unwrap()
    });
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();

    let table_for_writer = live.clone();
    let writer_value = second.clone();
    let (writer_started_tx, writer_started_rx) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        writer_started_tx.send(()).unwrap();
        table_for_writer
            .put("same-key", writer_value)
            .await
            .unwrap();
    });
    writer_started_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let writer_waits_for_callback = !writer.is_finished();
    release_tx.send(()).unwrap();

    let computed = computation.await.unwrap();
    writer.await.unwrap();
    assert_eq!(
        writer_waits_for_callback,
        java["remappingWriterWaitsForCallback"]
    );
    assert_eq!(
        callback_calls.load(Ordering::SeqCst) == 1,
        java["remappingCallbackRunsOnce"]
    );
    assert_eq!(
        Arc::ptr_eq(&computed, &second)
            && live
                .get("same-key")
                .unwrap()
                .is_some_and(|value| Arc::ptr_eq(&value, &second)),
        java["remappingReturnsAndKeepsSecond"]
    );
    live.remove("same-key").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn identity_safe_manager_removal_waits_for_factory_table_remapping() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerTable"];
    let manager = ClientRuntimeManager::new();
    let live = manager.factory_table();
    let config = ClientConfig::new("factory-remap-manager-remove");
    let id = config.build_client_id();
    let first = manager.get_or_create(&config).await.unwrap();
    let second = manager
        .get_or_create(&ClientConfig::new("factory-remap-manager-remove-next"))
        .await
        .unwrap();

    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback_calls_for_task = callback_calls.clone();
    let expected_first = first.clone();
    let replacement = second.clone();
    let table_for_compute = live.clone();
    let compute_key = id.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let computation = tokio::spawn(async move {
        table_for_compute
            .compute_if_present(compute_key, move |_, current| {
                assert!(Arc::ptr_eq(current, &expected_first));
                if callback_calls_for_task.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                Some(replacement.clone())
            })
            .await
            .unwrap()
            .unwrap()
    });
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();

    let manager_for_removal = manager.clone();
    let id_for_removal = id.clone();
    let expected_for_removal = first.clone();
    let (removal_started_tx, removal_started_rx) = tokio::sync::oneshot::channel();
    let removal = tokio::task::spawn_blocking(move || {
        removal_started_tx.send(()).unwrap();
        manager_for_removal
            .remove_client_factory(&id_for_removal, &expected_for_removal)
            .unwrap()
    });
    removal_started_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let removal_waits_for_remapping = !removal.is_finished();
    release_tx.send(()).unwrap();

    let computed = computation.await.unwrap();
    let removed_first = removal.await.unwrap();
    assert_eq!(
        removal_waits_for_remapping,
        java["managerIdentityRemovalWaitsForRemapping"]
    );
    assert_eq!(
        callback_calls.load(Ordering::SeqCst) == 1,
        java["managerIdentityRemovalRemappingRunsOnce"]
    );
    assert_eq!(
        !removed_first
            && Arc::ptr_eq(&computed, &second)
            && live
                .get(&id)
                .unwrap()
                .is_some_and(|value| Arc::ptr_eq(&value, &second)),
        java["managerIdentityRemovalKeepsRemappingResult"]
    );
    live.remove(&id).await.unwrap();
}

#[tokio::test]
async fn remapping_callback_panic_does_not_poison_factory_table_mutations() {
    let manager = ClientRuntimeManager::new();
    let live = manager.factory_table();
    let first = manager
        .get_or_create(&ClientConfig::new("factory-remap-panic-current"))
        .await
        .unwrap();
    let second = manager
        .get_or_create(&ClientConfig::new("factory-remap-panic-next"))
        .await
        .unwrap();
    let key = first.client_id().to_owned();
    live.put(&key, first.clone()).await.unwrap();

    let table_for_compute = live.clone();
    let compute_key = key.clone();
    let compute = tokio::spawn(async move {
        table_for_compute
            .compute(&compute_key, |_, _| -> Option<Arc<_>> {
                panic!("mapping callback failed")
            })
            .await
    });
    let error = match compute.await {
        Ok(_) => panic!("mapping callback unexpectedly completed"),
        Err(error) => error,
    };
    assert!(error.is_panic());

    let previous = live.put(&key, second.clone()).await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&previous, &first));
    assert!(Arc::ptr_eq(&live.get(&key).unwrap().unwrap(), &second));
}
