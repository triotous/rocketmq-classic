use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, ConsumerRuntime, ProducerRuntime, RuntimeError},
};
use rocketmq_classic_protocol::heartbeat::ConsumerData;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

struct Producer;
impl ProducerRuntime for Producer {}
struct Consumer;
impl ConsumerRuntime for Consumer {
    fn heartbeat_data(&self) -> ConsumerData {
        ConsumerData::default()
    }
}

fn same<T: ?Sized>(value: Option<Arc<T>>, expected: &Arc<T>) -> bool {
    value.is_some_and(|value| Arc::ptr_eq(&value, expected))
}

macro_rules! check_table {
    ($runtime:ident, $table:ident, $first:ident, $second:ident, $select:ident, $expected:expr) => {{
        let mut outcomes = BTreeMap::new();
        let retained = $table.clone();
        outcomes.insert("initialEmpty", $table.is_empty().unwrap());
        outcomes.insert(
            "firstPutAbsent",
            $table.put("group", $first.clone()).await.unwrap().is_none(),
        );
        outcomes.insert(
            "selectorSeesFirst",
            same($runtime.$select("group").unwrap(), &$first),
        );
        outcomes.insert(
            "putReturnsFirst",
            same($table.put("group", $second.clone()).await.unwrap(), &$first),
        );
        outcomes.insert(
            "selectorSeesReplacement",
            same($runtime.$select("group").unwrap(), &$second)
                && same(retained.get("group").unwrap(), &$second),
        );
        outcomes.insert(
            "putIfAbsentKeepsSecond",
            same(
                $table.put_if_absent("group", $first.clone()).await.unwrap(),
                &$second,
            ),
        );
        outcomes.insert(
            "staleReplaceRejected",
            !$table
                .replace_if_same("group", &$first, $second.clone())
                .await
                .unwrap(),
        );
        outcomes.insert(
            "conditionalReplaceAccepted",
            $table
                .replace_if_same("group", &$second, $first.clone())
                .await
                .unwrap(),
        );
        outcomes.insert(
            "replaceReturnsFirst",
            same(
                $table.replace("group", $second.clone()).await.unwrap(),
                &$first,
            ),
        );
        outcomes.insert(
            "staleRemoveRejected",
            !$table.remove_if_same("group", &$first).await.unwrap(),
        );
        outcomes.insert(
            "conditionalRemoveAccepted",
            $table.remove_if_same("group", &$second).await.unwrap(),
        );
        outcomes.insert(
            "absentReplaceNull",
            $table
                .replace("group", $first.clone())
                .await
                .unwrap()
                .is_none(),
        );
        outcomes.insert(
            "absentRemoveNull",
            $table.remove("group").await.unwrap().is_none(),
        );
        outcomes.insert(
            "putIfAbsentAbsentNull",
            $table
                .put_if_absent("group", $first.clone())
                .await
                .unwrap()
                .is_none(),
        );
        outcomes.insert(
            "emptyKeyAccepted",
            $table.put("", $second.clone()).await.unwrap().is_none(),
        );
        outcomes.insert(
            "sizeTwo",
            $table.len().unwrap() == 2
                && $table.snapshot().unwrap().len() == 2
                && $table.contains_key("").unwrap(),
        );
        $table.clear().await.unwrap();
        outcomes.insert("retainedViewSeesClear", retained.is_empty().unwrap());
        outcomes.insert(
            "selectorSeesClear",
            $runtime.$select("group").unwrap().is_none(),
        );
        outcomes.insert(
            "putAfterClearAccepted",
            $table
                .put("group", $second.clone())
                .await
                .unwrap()
                .is_none()
                && same($runtime.$select("group").unwrap(), &$second),
        );
        $table.clear().await.unwrap();
        assert_eq!(serde_json::to_value(outcomes).unwrap(), $expected);
    }};
}

#[tokio::test]
async fn retained_registry_tables_match_every_invoked_java_primitive_outcome() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("registry-tables"))
        .await
        .unwrap();
    let producer = runtime.producer_table();
    let first: Arc<dyn ProducerRuntime> = Arc::new(Producer);
    let second: Arc<dyn ProducerRuntime> = Arc::new(Producer);
    check_table!(
        runtime,
        producer,
        first,
        second,
        select_producer,
        fixture["registryTables"]["producer"]
    );
    let consumer = runtime.consumer_table();
    let first: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let second: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    check_table!(
        runtime,
        consumer,
        first,
        second,
        select_consumer,
        fixture["registryTables"]["consumer"]
    );
}

#[tokio::test]
async fn conditional_registry_mutations_match_java_custom_value_equality() {
    struct EqualProducer(&'static str);
    impl ProducerRuntime for EqualProducer {
        fn map_value_equality_key(&self) -> Option<String> {
            Some(self.0.to_owned())
        }
    }
    struct EqualConsumer(&'static str);
    impl ConsumerRuntime for EqualConsumer {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData::default()
        }
        fn map_value_equality_key(&self) -> Option<String> {
            Some(self.0.to_owned())
        }
    }
    struct DynamicConsumer(Arc<RwLock<String>>);
    impl ConsumerRuntime for DynamicConsumer {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData::default()
        }
        fn map_value_equality_key(&self) -> Option<String> {
            Some(self.0.read().unwrap().clone())
        }
    }
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let expected = &fixture["registryTables"]["customEquality"];
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("registry-custom-equality"))
        .await
        .unwrap();
    let producers = runtime.producer_table();
    let producer_a: Arc<dyn ProducerRuntime> = Arc::new(EqualProducer("producer-equal"));
    let producer_b: Arc<dyn ProducerRuntime> = Arc::new(EqualProducer("producer-equal"));
    let producer_c: Arc<dyn ProducerRuntime> = Arc::new(EqualProducer("producer-replacement"));
    let producer_other: Arc<dyn ProducerRuntime> = Arc::new(EqualProducer("producer-other"));
    producers.put("replace", producer_a.clone()).await.unwrap();
    assert!(producers.contains_value(&producer_b).unwrap());
    assert!(
        producers
            .replace_if_equal("replace", &producer_b, producer_c)
            .await
            .unwrap()
    );
    producers.put("remove", producer_a.clone()).await.unwrap();
    assert!(
        producers
            .remove_if_equal("remove", &producer_b)
            .await
            .unwrap()
    );
    producers
        .put("values-remove", producer_a.clone())
        .await
        .unwrap();
    let producer_values_remove = producers.values_view().remove(&producer_b).await.unwrap();
    producers
        .put("entry-remove", producer_a.clone())
        .await
        .unwrap();
    let producer_entries = producers.entry_set_view();
    let producer_entry_contains = producer_entries
        .contains("entry-remove", &producer_b)
        .unwrap();
    let producer_entry_remove = producer_entries
        .remove("entry-remove", &producer_b)
        .await
        .unwrap();
    producers
        .put("values-remove-all", producer_a.clone())
        .await
        .unwrap();
    let producer_values_remove_all = producers
        .values_view()
        .remove_all(&[producer_b.clone()])
        .await
        .unwrap()
        == 1;
    producers
        .put("values-retain-all", producer_a.clone())
        .await
        .unwrap();
    producers
        .put("other-values-retain-all", producer_other.clone())
        .await
        .unwrap();
    let producer_values_retain_all = producers
        .values_view()
        .retain_all(&[producer_b.clone()])
        .await
        .unwrap()
        > 0
        && same(producers.get("values-retain-all").unwrap(), &producer_a)
        && !producers.contains_key("other-values-retain-all").unwrap();
    producers
        .put("entry-retain-all", producer_a.clone())
        .await
        .unwrap();
    producers
        .put("other-entry-retain-all", producer_other.clone())
        .await
        .unwrap();
    let producer_entry_retain_all = producers
        .entry_set_view()
        .retain_all(&BTreeMap::from([(
            "entry-retain-all".to_owned(),
            producer_b.clone(),
        )]))
        .await
        .unwrap()
        > 0
        && same(producers.get("entry-retain-all").unwrap(), &producer_a)
        && !producers.contains_key("other-entry-retain-all").unwrap();

    let consumers = runtime.consumer_table();
    let consumer_a: Arc<dyn ConsumerRuntime> = Arc::new(EqualConsumer("consumer-equal"));
    let consumer_b: Arc<dyn ConsumerRuntime> = Arc::new(EqualConsumer("consumer-equal"));
    let consumer_c: Arc<dyn ConsumerRuntime> = Arc::new(EqualConsumer("consumer-replacement"));
    let consumer_other: Arc<dyn ConsumerRuntime> = Arc::new(EqualConsumer("consumer-other"));
    consumers.put("replace", consumer_a.clone()).await.unwrap();
    assert!(consumers.contains_value(&consumer_b).unwrap());
    assert!(
        consumers
            .replace_if_equal("replace", &consumer_b, consumer_c.clone())
            .await
            .unwrap()
    );
    consumers.put("remove", consumer_a.clone()).await.unwrap();
    assert!(
        consumers
            .remove_if_equal("remove", &consumer_b)
            .await
            .unwrap()
    );
    consumers
        .put("values-remove", consumer_a.clone())
        .await
        .unwrap();
    let consumer_values_remove = consumers.values_view().remove(&consumer_b).await.unwrap();
    consumers
        .put("entry-remove", consumer_a.clone())
        .await
        .unwrap();
    let consumer_entries = consumers.entry_set_view();
    let consumer_entry_contains = consumer_entries
        .contains("entry-remove", &consumer_b)
        .unwrap();
    let consumer_entry_remove = consumer_entries
        .remove("entry-remove", &consumer_b)
        .await
        .unwrap();
    consumers
        .put("values-remove-all", consumer_a.clone())
        .await
        .unwrap();
    let consumer_values_remove_all = consumers
        .values_view()
        .remove_all(&[consumer_b.clone()])
        .await
        .unwrap()
        == 1;
    consumers
        .put("values-retain-all", consumer_a.clone())
        .await
        .unwrap();
    consumers
        .put("other-values-retain-all", consumer_other.clone())
        .await
        .unwrap();
    let consumer_values_retain_all = consumers
        .values_view()
        .retain_all(&[consumer_b.clone()])
        .await
        .unwrap()
        > 0
        && same(consumers.get("values-retain-all").unwrap(), &consumer_a)
        && !consumers.contains_key("other-values-retain-all").unwrap();
    consumers
        .put("entry-retain-all", consumer_a.clone())
        .await
        .unwrap();
    consumers
        .put("other-entry-retain-all", consumer_other.clone())
        .await
        .unwrap();
    let consumer_entry_retain_all = consumers
        .entry_set_view()
        .retain_all(&BTreeMap::from([(
            "entry-retain-all".to_owned(),
            consumer_b.clone(),
        )]))
        .await
        .unwrap()
        > 0
        && same(consumers.get("entry-retain-all").unwrap(), &consumer_a)
        && !consumers.contains_key("other-entry-retain-all").unwrap();

    let stored_key = Arc::new(RwLock::new("dynamic-before".to_owned()));
    let probe_key = Arc::new(RwLock::new("dynamic-before".to_owned()));
    let dynamic_stored: Arc<dyn ConsumerRuntime> = Arc::new(DynamicConsumer(stored_key.clone()));
    let dynamic_probe: Arc<dyn ConsumerRuntime> = Arc::new(DynamicConsumer(probe_key.clone()));
    consumers
        .put("dynamic-equality", dynamic_stored.clone())
        .await
        .unwrap();
    let dynamic_contains_before = consumers.contains_value(&dynamic_probe).unwrap();
    *stored_key.write().unwrap() = "dynamic-after".to_owned();
    let dynamic_contains_after_stored_mutation = !consumers.contains_value(&dynamic_probe).unwrap();
    *probe_key.write().unwrap() = "dynamic-after".to_owned();
    let dynamic_contains_after_both_mutations = consumers.contains_value(&dynamic_probe).unwrap();

    let replace_stored_key = Arc::new(RwLock::new("replace-before".to_owned()));
    let replace_expected_key = Arc::new(RwLock::new("replace-before".to_owned()));
    let replace_stored: Arc<dyn ConsumerRuntime> =
        Arc::new(DynamicConsumer(replace_stored_key.clone()));
    let replace_expected: Arc<dyn ConsumerRuntime> =
        Arc::new(DynamicConsumer(replace_expected_key.clone()));
    consumers
        .put("dynamic-replace", replace_stored.clone())
        .await
        .unwrap();
    *replace_expected_key.write().unwrap() = "stale".to_owned();
    let dynamic_replace_rejects_changed_expected = !consumers
        .replace_if_equal("dynamic-replace", &replace_expected, consumer_c.clone())
        .await
        .unwrap();
    *replace_stored_key.write().unwrap() = "replace-after".to_owned();
    *replace_expected_key.write().unwrap() = "replace-after".to_owned();
    let dynamic_replace_matches_current_equality = consumers
        .replace_if_equal("dynamic-replace", &replace_expected, consumer_c)
        .await
        .unwrap();
    assert_eq!(
        serde_json::json!({
            "producerContainsEqualValue": true,
            "producerValuesRemoveByEqualValue": producer_values_remove,
            "producerEntryContainsEqualValue": producer_entry_contains,
            "producerEntryRemoveByEqualValue": producer_entry_remove,
            "producerValuesRemoveAllByEqualValue": producer_values_remove_all,
            "producerValuesRetainAllByEqualValue": producer_values_retain_all,
            "producerEntryRetainAllByEqualValue": producer_entry_retain_all,
            "consumerContainsEqualValue": true,
            "consumerValuesRemoveByEqualValue": consumer_values_remove,
            "consumerEntryContainsEqualValue": consumer_entry_contains,
            "consumerEntryRemoveByEqualValue": consumer_entry_remove,
            "consumerValuesRemoveAllByEqualValue": consumer_values_remove_all,
            "consumerValuesRetainAllByEqualValue": consumer_values_retain_all,
            "consumerEntryRetainAllByEqualValue": consumer_entry_retain_all,
            "dynamicContainsBeforeMutation": dynamic_contains_before,
            "dynamicContainsAfterStoredMutation": dynamic_contains_after_stored_mutation,
            "dynamicContainsAfterBothMutations": dynamic_contains_after_both_mutations,
            "dynamicReplaceRejectsChangedExpected": dynamic_replace_rejects_changed_expected,
            "dynamicReplaceMatchesCurrentEquality": dynamic_replace_matches_current_equality,
            "producerReplaceByEqualValue": true,
            "producerRemoveByEqualValue": true,
            "consumerReplaceByEqualValue": true,
            "consumerRemoveByEqualValue": true
        }),
        *expected
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_default_map_operations_match_invoked_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("registry-default-operations"))
        .await
        .unwrap();
    macro_rules! check_operations {
        ($table:expr, $first:expr, $second:expr, $expected:expr) => {{
            let table = $table;
            let first = $first;
            let second = $second;
            let mut calls = 0;
            let compute_if_absent = table
                .compute_if_absent("a", |_| {
                    calls += 1;
                    Some(first.clone())
                })
                .await
                .unwrap();
            let compute_if_absent_again = table
                .compute_if_absent("a", |_| {
                    calls += 1;
                    Some(second.clone())
                })
                .await
                .unwrap();
            let compute_if_present = table
                .compute_if_present("a", |_, _| Some(second.clone()))
                .await
                .unwrap();
            let get_or_default_present =
                Arc::ptr_eq(&table.get_or_default("a", first.clone()).unwrap(), &second);
            let get_or_default_absent =
                Arc::ptr_eq(&table.get_or_default("missing", second.clone()).unwrap(), &second);
            let compute_if_present_absent = table
                .compute_if_present("missing", |_, _| Some(second.clone()))
                .await
                .unwrap();
            let compute_insert = table
                .compute("b", |_, _| Some(first.clone()))
                .await
                .unwrap();
            let compute_remove = table.compute("b", |_, _| None).await.unwrap();
            let merge_insert = table
                .merge("c", first.clone(), |_, _| Some(second.clone()))
                .await
                .unwrap();
            let merge_update = table
                .merge("c", second.clone(), |_, _| Some(second.clone()))
                .await
                .unwrap();
            table
                .replace_all(|_, _| first.clone())
                .await
                .unwrap();
            table
                .put("replace-all-race", first.clone())
                .await
                .unwrap();
            let replace_all_calls = Arc::new(AtomicUsize::new(0));
            let replace_all_initial_is_first = Arc::new(AtomicBool::new(false));
            let replace_all_retry_is_second = Arc::new(AtomicBool::new(false));
            let (replace_all_started_tx, replace_all_started_rx) = mpsc::channel();
            let (release_replace_all_tx, release_replace_all_rx) = mpsc::channel();
            let replace_all_table = table.clone();
            let first_for_replace_all = first.clone();
            let second_for_replace_all = second.clone();
            let calls_for_replace_all = replace_all_calls.clone();
            let initial_for_replace_all = replace_all_initial_is_first.clone();
            let retry_for_replace_all = replace_all_retry_is_second.clone();
            let replacing = tokio::spawn(async move {
                replace_all_table
                    .replace_all(move |key, current| {
                        if key != "replace-all-race" {
                            return current.clone();
                        }
                        let call = calls_for_replace_all.fetch_add(1, Ordering::SeqCst);
                        if call == 0 {
                            initial_for_replace_all
                                .store(Arc::ptr_eq(current, &first_for_replace_all), Ordering::SeqCst);
                            replace_all_started_tx.send(()).unwrap();
                            release_replace_all_rx.recv().unwrap();
                        } else if call == 1 {
                            retry_for_replace_all
                                .store(Arc::ptr_eq(current, &second_for_replace_all), Ordering::SeqCst);
                        }
                        first_for_replace_all.clone()
                    })
                    .await
                    .unwrap()
            });
            tokio::task::spawn_blocking(move || replace_all_started_rx.recv().unwrap())
                .await
                .unwrap();
            let replace_all_writer_table = table.clone();
            let writer_value = second.clone();
            let (replace_all_writer_started_tx, replace_all_writer_started_rx) =
                tokio::sync::oneshot::channel();
            let mut replace_all_writer = tokio::spawn(async move {
                replace_all_writer_started_tx.send(()).unwrap();
                replace_all_writer_table
                    .put("replace-all-race", writer_value)
                    .await
                    .unwrap()
            });
            replace_all_writer_started_rx.await.unwrap();
            let replace_all_writer_result =
                tokio::time::timeout(Duration::from_millis(100), &mut replace_all_writer).await;
            let replace_all_writer_runs = replace_all_writer_result.is_ok();
            release_replace_all_tx.send(()).unwrap();
            replacing.await.unwrap();
            let replace_all_writer_previous = match replace_all_writer_result {
                Ok(result) => result.unwrap(),
                Err(_) => replace_all_writer.await.unwrap(),
            };
            let replace_all_retry_outcome = replace_all_calls.load(Ordering::SeqCst) == 2
                && replace_all_initial_is_first.load(Ordering::SeqCst)
                && replace_all_retry_is_second.load(Ordering::SeqCst);
            let replace_all_writer_observed_old =
                replace_all_writer_previous.is_some_and(|previous| Arc::ptr_eq(&previous, &first));
            let replace_all_final_is_computed = table
                .get("replace-all-race")
                .unwrap()
                .is_some_and(|current| Arc::ptr_eq(&current, &first));
            table.remove("replace-all-race").await.unwrap();
            let mut for_each_count = 0;
            table
                .for_each(|_, _| for_each_count += 1)
                .unwrap();
            let mut additions = BTreeMap::new();
            additions.insert("d".to_owned(), second.clone());
            additions.insert("e".to_owned(), first.clone());
            table.put_all(additions).await.unwrap();
            table.put("remapping-race", first.clone()).await.unwrap();
            let callback_calls = Arc::new(AtomicUsize::new(0));
            let callback_read_current = Arc::new(AtomicBool::new(false));
            let (callback_started_tx, callback_started_rx) = mpsc::channel();
            let (release_callback_tx, release_callback_rx) = mpsc::channel();
            let callback_table = table.clone();
            let callback_read_table = callback_table.clone();
            let callback_first = first.clone();
            let callback_second = second.clone();
            let callback_calls_for_task = callback_calls.clone();
            let callback_read_for_task = callback_read_current.clone();
            let remapping = tokio::spawn(async move {
                callback_table
                    .compute("remapping-race", move |key, old| {
                        callback_calls_for_task.fetch_add(1, Ordering::SeqCst);
                        callback_read_for_task.store(
                            old
                                .zip(callback_read_table.get(key).ok().flatten().as_ref())
                                .is_some_and(|(old, observed)| Arc::ptr_eq(old, observed))
                                && old
                                    .is_some_and(|old| Arc::ptr_eq(old, &callback_first)),
                            Ordering::SeqCst,
                        );
                        callback_started_tx.send(()).unwrap();
                        release_callback_rx.recv().unwrap();
                        Some(callback_second.clone())
                    })
                    .await
                    .unwrap()
            });
            tokio::task::spawn_blocking(move || callback_started_rx.recv().unwrap())
                .await
                .unwrap();
            let writer_table = table.clone();
            let writer_first = first.clone();
            let mut writer = tokio::spawn(async move {
                writer_table
                    .put("remapping-race", writer_first)
                    .await
                    .unwrap()
            });
            let writer_waits = tokio::time::timeout(Duration::from_millis(100), &mut writer)
                .await
                .is_err();
            release_callback_tx.send(()).unwrap();
            let remapping_result = remapping.await.unwrap();
            let writer_previous = writer.await.unwrap();
            let remapping_returns_and_keeps_writer_value = same(remapping_result, &second)
                && same(table.get("remapping-race").unwrap(), &first)
                && same(writer_previous, &second);
            table.remove("remapping-race").await.unwrap();
            let actual = serde_json::json!({
                "computeIfAbsentInsert": same(compute_if_absent, &first),
                "computeIfAbsentNoRemap": same(compute_if_absent_again, &first) && calls == 1,
                "computeIfPresentUpdate": same(compute_if_present, &second),
                "computeIfPresentAbsent": compute_if_present_absent.is_none(),
                "getOrDefaultPresent": get_or_default_present,
                "getOrDefaultAbsent": get_or_default_absent,
                "computeInsert": same(compute_insert, &first),
                "computeRemove": compute_remove.is_none() && !table.contains_key("b").unwrap(),
                "mergeInsert": same(merge_insert, &first),
                "mergeUpdate": same(merge_update, &second),
                "replaceAll": same(table.get("a").unwrap(), &first)
                    && same(table.get("c").unwrap(), &first),
                "replaceAllWriterRunsDuringCallback": replace_all_writer_runs,
                "replaceAllCallbackRetriesAfterWriter": replace_all_retry_outcome,
                "replaceAllWriterObservedOldValue": replace_all_writer_observed_old,
                "replaceAllFinalValueIsComputed": replace_all_final_is_computed,
                "forEachCount": for_each_count,
                "putAllAdds": same(table.get("d").unwrap(), &second)
                    && same(table.get("e").unwrap(), &first)
                    && table.len().unwrap() == 4,
                "remappingWriterWaitsForCallback": writer_waits,
                "remappingCallbackRunsOnce": callback_calls.load(Ordering::SeqCst) == 1,
                "remappingCallbackReadsCurrent": callback_read_current.load(Ordering::SeqCst),
                "remappingReturnsAndKeepsWriterValue": remapping_returns_and_keeps_writer_value,
                "keys": table.keys().unwrap(),
                "valuesCount": table.values().unwrap().len()
            });
            assert_eq!(actual, $expected);
            table.clear().await.unwrap();
        }};
    }
    check_operations!(
        runtime.producer_table(),
        Arc::new(Producer) as Arc<dyn ProducerRuntime>,
        Arc::new(Producer) as Arc<dyn ProducerRuntime>,
        fixture["registryTables"]["producerDefaultOperations"].clone()
    );
    check_operations!(
        runtime.consumer_table(),
        Arc::new(Consumer) as Arc<dyn ConsumerRuntime>,
        Arc::new(Consumer) as Arc<dyn ConsumerRuntime>,
        fixture["registryTables"]["consumerDefaultOperations"].clone()
    );
}

#[tokio::test]
async fn retained_registry_collection_views_match_invoked_java() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("registry-live-views"))
        .await
        .unwrap();
    macro_rules! exercise_live_views {
        ($table:expr, $first:expr, $second:expr, $expected:expr) => {{
            let table = $table;
            let first = $first;
            let second = $second;
            let keys = table.key_set_view();
            let values = table.values_view();
            let entries = table.entry_set_view();
            table.put("a", first.clone()).await.unwrap();
            let retained_views_see_initial_put = keys.contains("a").unwrap()
                && values.contains(&first).unwrap()
                && entries.contains("a", &first).unwrap();
            table.put("b", second.clone()).await.unwrap();
            let retained_views_see_later_put = keys.len().unwrap() == 2
                && values.len().unwrap() == 2
                && entries.len().unwrap() == 2;
            let key_view_remove = keys.remove("a").await.unwrap()
                && !table.contains_key("a").unwrap();
            let value_view_remove = values.remove(&second).await.unwrap()
                && !table.contains_key("b").unwrap();
            table.put("c", first.clone()).await.unwrap();
            let entry_view_remove = entries.remove("c", &first).await.unwrap()
                && !table.contains_key("c").unwrap();
            table.put("d", second.clone()).await.unwrap();
            let retained_views_see_repopulation = keys.contains("d").unwrap()
                && values.contains(&second).unwrap()
                && entries.contains("d", &second).unwrap();
            keys.clear().await.unwrap();
            let key_view_clear = table.is_empty().unwrap();
            table.put("e", first.clone()).await.unwrap();
            values.clear().await.unwrap();
            let value_view_clear = table.is_empty().unwrap();
            table.put("f", second.clone()).await.unwrap();
            entries.clear().await.unwrap();
            let entry_view_clear = table.is_empty().unwrap();
            table.put("remove-key", first.clone()).await.unwrap();
            table.put("keep-key", second.clone()).await.unwrap();
            let key_view_remove_if = keys
                .remove_if(|key| key == "remove-key")
                .await
                .unwrap()
                == 1
                && !table.contains_key("remove-key").unwrap();
            table.clear().await.unwrap();
            table.put("keep-key", first.clone()).await.unwrap();
            table.put("drop-key", second.clone()).await.unwrap();
            let key_view_retain_all = keys
                .retain_all(&std::collections::BTreeSet::from(["keep-key".to_owned()]))
                .await
                .unwrap()
                == 1
                && table.len().unwrap() == 1
                && table.contains_key("keep-key").unwrap();

            table.clear().await.unwrap();
            table.put("remove-value", first.clone()).await.unwrap();
            table.put("keep-value", second.clone()).await.unwrap();
            let value_view_remove_if = values
                .remove_if(|value| Arc::ptr_eq(value, &first))
                .await
                .unwrap()
                == 1
                && !table.contains_key("remove-value").unwrap();
            table.clear().await.unwrap();
            table.put("keep-value", second.clone()).await.unwrap();
            table.put("drop-value", first.clone()).await.unwrap();
            let value_view_retain_all = values
                .retain_all(&[second.clone()])
                .await
                .unwrap()
                == 1
                && table.len().unwrap() == 1
                && table.contains_key("keep-value").unwrap();

            table.clear().await.unwrap();
            table.put("remove-entry", first.clone()).await.unwrap();
            table.put("keep-entry", second.clone()).await.unwrap();
            let entry_view_remove_if = entries
                .remove_if(|key, _| key == "remove-entry")
                .await
                .unwrap()
                == 1
                && !table.contains_key("remove-entry").unwrap();
            table.clear().await.unwrap();
            table.put("keep-entry", second.clone()).await.unwrap();
            table.put("drop-entry", first.clone()).await.unwrap();
            let entry_view_retain_all = entries
                .retain_all(&BTreeMap::from([("keep-entry".to_owned(), second.clone())]))
                .await
                .unwrap()
                == 1
                && table.len().unwrap() == 1
                && table.contains_key("keep-entry").unwrap();

            table.clear().await.unwrap();
            table.put("remove-all-key", first.clone()).await.unwrap();
            table.put("keep-all-key", second.clone()).await.unwrap();
            let key_view_remove_all = keys
                .remove_all(&std::collections::BTreeSet::from(["remove-all-key".to_owned()]))
                .await
                .unwrap()
                == 1
                && !table.contains_key("remove-all-key").unwrap();
            table.clear().await.unwrap();
            table.put("remove-all-value", first.clone()).await.unwrap();
            table.put("keep-all-value", second.clone()).await.unwrap();
            let value_view_remove_all = values
                .remove_all(&[first.clone()])
                .await
                .unwrap()
                == 1
                && !table.contains_key("remove-all-value").unwrap();
            table.clear().await.unwrap();
            table.put("remove-all-entry", first.clone()).await.unwrap();
            table.put("keep-all-entry", second.clone()).await.unwrap();
            let entry_view_remove_all = entries
                .remove_all(&BTreeMap::from([("remove-all-entry".to_owned(), first.clone())]))
                .await
                .unwrap()
                == 1
                && !table.contains_key("remove-all-entry").unwrap();

            table.clear().await.unwrap();
            table.put("entry-set-value", first.clone()).await.unwrap();
            let entry = entries.entry_snapshots().unwrap().pop().unwrap();
            let entry_set_value_returns_previous =
                Arc::ptr_eq(&entry.set_value(second.clone()).await.unwrap(), &first);
            let entry_set_value_updates_entry = Arc::ptr_eq(&entry.value().unwrap(), &second);
            let entry_set_value_updates_table = table
                .get("entry-set-value")
                .unwrap()
                .is_some_and(|value| Arc::ptr_eq(&value, &second));
            table.put("entry-set-value", first.clone()).await.unwrap();
            let entry_set_value_entry_is_snapshot =
                Arc::ptr_eq(&entry.value().unwrap(), &second);
            let entry_set_value_returns_captured_previous =
                Arc::ptr_eq(&entry.set_value(second.clone()).await.unwrap(), &second);
            let entry_set_value_replaces_current_mapping = table
                .get("entry-set-value")
                .unwrap()
                .is_some_and(|value| Arc::ptr_eq(&value, &second));
            assert_eq!(
                serde_json::json!({
                    "retainedViewsSeeInitialPut": retained_views_see_initial_put,
                    "retainedViewsSeeLaterPut": retained_views_see_later_put,
                    "keyViewRemove": key_view_remove,
                    "valueViewRemove": value_view_remove,
                    "entryViewRemove": entry_view_remove,
                    "retainedViewsSeeRepopulation": retained_views_see_repopulation,
                    "keyViewClear": key_view_clear,
                    "valueViewClear": value_view_clear,
                    "entryViewClear": entry_view_clear,
                    "keyViewRemoveIf": key_view_remove_if,
                    "keyViewRetainAll": key_view_retain_all,
                    "valueViewRemoveIf": value_view_remove_if,
                    "valueViewRetainAll": value_view_retain_all,
                    "entryViewRemoveIf": entry_view_remove_if,
                    "entryViewRetainAll": entry_view_retain_all,
                    "keyViewRemoveAll": key_view_remove_all,
                    "valueViewRemoveAll": value_view_remove_all,
                    "entryViewRemoveAll": entry_view_remove_all,
                    "entrySetValueReturnsPrevious": entry_set_value_returns_previous,
                    "entrySetValueUpdatesEntry": entry_set_value_updates_entry,
                    "entrySetValueUpdatesTable": entry_set_value_updates_table,
                    "entrySetValueEntryIsSnapshot": entry_set_value_entry_is_snapshot,
                    "entrySetValueReturnsCapturedPrevious": entry_set_value_returns_captured_previous,
                    "entrySetValueReplacesCurrentMapping": entry_set_value_replaces_current_mapping,
                }),
                $expected
            );
            table.clear().await.unwrap();
        }};
    }
    exercise_live_views!(
        runtime.producer_table(),
        Arc::new(Producer) as Arc<dyn ProducerRuntime>,
        Arc::new(Producer) as Arc<dyn ProducerRuntime>,
        fixture["registryTables"]["producerLiveViews"].clone()
    );
    exercise_live_views!(
        runtime.consumer_table(),
        Arc::new(Consumer) as Arc<dyn ConsumerRuntime>,
        Arc::new(Consumer) as Arc<dyn ConsumerRuntime>,
        fixture["registryTables"]["consumerLiveViews"].clone()
    );
}

#[tokio::test]
async fn group_unregister_matches_invoked_java_consumer_methods_and_removes_current_binding() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&ClientConfig::new("group-unregister"))
        .await
        .unwrap();
    let table = runtime.consumer_table();
    let first: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let second: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let lease = runtime
        .register_consumer("group", first.clone())
        .await
        .unwrap();
    let mut outcomes = BTreeMap::new();
    outcomes.insert("consumerRegisterAccepted", true);
    outcomes.insert(
        "consumerDirectPutReturnsRegistered",
        same(table.put("group", second.clone()).await.unwrap(), &first),
    );
    outcomes.insert(
        "consumerRegisterAfterDirectPutRejected",
        matches!(
            runtime.register_consumer("group", first).await,
            Err(RuntimeError::DuplicateGroup { .. })
        ),
    );
    assert!(
        !runtime
            .unregister_consumer(&lease, tokio::time::Instant::now())
            .await
            .unwrap()
    );
    assert!(
        runtime
            .unregister_consumer_by_group("group", tokio::time::Instant::now())
            .await
            .unwrap()
    );
    outcomes.insert(
        "consumerUnregisterRemovesCurrentMapping",
        table.get("group").unwrap().is_none(),
    );
    for (name, outcome) in outcomes {
        assert_eq!(
            serde_json::json!(outcome),
            fixture["registryTables"][name],
            "{name}"
        );
    }
    assert!(
        !runtime
            .unregister_consumer_by_group("group", tokio::time::Instant::now())
            .await
            .unwrap()
    );
    let producer: Arc<dyn ProducerRuntime> = Arc::new(Producer);
    runtime
        .producer_table()
        .put("", producer.clone())
        .await
        .unwrap();
    assert!(
        runtime
            .unregister_producer_by_group("", tokio::time::Instant::now())
            .await
            .unwrap()
    );
    assert!(
        !runtime
            .unregister_producer_by_group("", tokio::time::Instant::now())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn live_table_replacement_invalidates_old_lease_without_retaining_runtime_or_owners() {
    let manager = ClientRuntimeManager::new();
    let mut config = ClientConfig::new("registry-lease");
    config.namesrv_addr = Some(String::new());
    let runtime = manager.get_or_create(&config).await.unwrap();
    runtime.start().await.unwrap();
    let table = runtime.consumer_table();
    let first: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let second: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let lease = runtime
        .register_consumer("registered", first.clone())
        .await
        .unwrap();
    assert!(same(
        table.put("registered", second.clone()).await.unwrap(),
        &first
    ));
    assert!(matches!(
        runtime.register_consumer("registered", first.clone()).await,
        Err(RuntimeError::DuplicateGroup { .. })
    ));
    assert!(
        !runtime
            .unregister_consumer(&lease, tokio::time::Instant::now())
            .await
            .unwrap()
    );
    assert!(same(table.get("registered").unwrap(), &second));
    assert!(table.remove_if_same("registered", &second).await.unwrap());
    assert!(runtime.shutdown().await.unwrap());
    let weak_runtime = Arc::downgrade(&runtime);
    drop(runtime);
    drop(manager);
    assert!(weak_runtime.upgrade().is_none());
    assert!(table.put("after", second.clone()).await.unwrap().is_none());
    assert!(same(table.get("after").unwrap(), &second));
    let weak_owner = Arc::downgrade(&second);
    drop(second);
    assert!(weak_owner.upgrade().is_none());
    assert!(table.get("after").unwrap().is_none());
    assert!(table.contains_key("after").unwrap()); // Binding does not disappear on owner drop.
    table.clear().await.unwrap();
    assert!(table.is_empty().unwrap());
}

#[tokio::test]
async fn running_table_writes_wait_for_unregister_and_cancelled_queue_writes_do_not_commit() {
    use bytes::BytesMut;
    use rocketmq_classic_remoting::{
        RemotingCommand,
        codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::oneshot,
        time::{Duration, Instant},
    };
    async fn read(socket: &mut TcpStream) -> RemotingCommand {
        let mut length = [0; 4];
        socket.read_exact(&mut length).await.unwrap();
        let mut bytes = BytesMut::from(&length[..]);
        bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
        socket.read_exact(&mut bytes[4..]).await.unwrap();
        frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap()
    }
    async fn reply(socket: &mut TcpStream, request: &RemotingCommand, body: Option<Vec<u8>>) {
        let mut response = RemotingCommand {
            opaque: request.opaque,
            body,
            ..Default::default()
        };
        response.mark_response();
        socket
            .write_all(&frame::encode(&response, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
    }
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker.local_addr().unwrap().to_string();
    let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nameserver_address = nameserver.local_addr().unwrap().to_string();
    let name_peer = tokio::spawn(async move {
        let (mut socket, _) = nameserver.accept().await.unwrap();
        let request = read(&mut socket).await;
        assert_eq!(request.code, 105);
        let route = serde_json::to_vec(&serde_json::json!({"queueDatas":[],"brokerDatas":[{"cluster":"cluster","brokerName":"broker","brokerAddrs":{"0":broker_address}}],"filterServerTable":{}})).unwrap();
        reply(&mut socket, &request, Some(route)).await;
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });
    let (seen, received) = oneshot::channel();
    let (release, proceed) = oneshot::channel();
    let broker_peer = tokio::spawn(async move {
        let (mut socket, _) = broker.accept().await.unwrap();
        let request = read(&mut socket).await;
        assert_eq!(request.code, 35);
        assert_eq!(
            request.ext_fields.as_ref().unwrap()["consumerGroup"],
            "registered"
        );
        seen.send(()).unwrap();
        proceed.await.unwrap();
        reply(&mut socket, &request, None).await;
        for (field, group) in [
            ("consumerGroup", "absent"),
            ("producerGroup", "producer"),
            ("consumerGroup", "registered"),
        ] {
            let request = read(&mut socket).await;
            assert_eq!(request.code, 35);
            let fields = request.ext_fields.as_ref().unwrap();
            assert_eq!(fields[field], group);
            assert_eq!(fields["clientID"], "registry-rpc-order@DEFAULT");
            let other = if field == "consumerGroup" {
                "producerGroup"
            } else {
                "consumerGroup"
            };
            assert!(!fields.contains_key(other));
            if group == "absent" {
                let mut response = RemotingCommand {
                    code: 1,
                    opaque: request.opaque,
                    ..Default::default()
                };
                response.mark_response();
                socket
                    .write_all(&frame::encode(&response, DEFAULT_MAX_FRAME_SIZE).unwrap())
                    .await
                    .unwrap();
            } else {
                reply(&mut socket, &request, None).await;
            }
        }
        let internal = read(&mut socket).await;
        assert_eq!(internal.code, 35);
        assert_eq!(
            internal.ext_fields.as_ref().unwrap()["producerGroup"],
            "CLIENT_INNER_PRODUCER"
        );
        reply(&mut socket, &internal, None).await;
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });
    let mut config = ClientConfig::new("registry-rpc-order");
    config.namesrv_addr = Some(nameserver_address);
    config.enable_heartbeat_channel_event_listener = false;
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    runtime.start().await.unwrap();
    runtime
        .update_topic_route("topic", Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    let table = runtime.consumer_table();
    let first: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let second: Arc<dyn ConsumerRuntime> = Arc::new(Consumer);
    let lease = runtime
        .register_consumer("registered", first.clone())
        .await
        .unwrap();
    let entry = table
        .entry_set_view()
        .entry_snapshots()
        .unwrap()
        .into_iter()
        .find(|entry| entry.key() == "registered")
        .unwrap();
    table
        .put("cancelled-unregister", second.clone())
        .await
        .unwrap();
    let unregister = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .unregister_consumer_by_group("registered", Instant::now() + Duration::from_secs(3))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), received)
        .await
        .unwrap()
        .unwrap();
    let write = {
        let table = table.clone();
        let second = second.clone();
        tokio::spawn(async move { table.put("registered", second).await })
    };
    let cancelled = {
        let table = table.clone();
        let second = second.clone();
        tokio::spawn(async move { table.put("cancelled", second).await })
    };
    let cancelled_entry_set_value = {
        let entry = entry.clone();
        let second = second.clone();
        tokio::spawn(async move { entry.set_value(second).await })
    };
    let cancelled_unregister = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .unregister_consumer_by_group(
                    "cancelled-unregister",
                    Instant::now() + Duration::from_secs(3),
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!write.is_finished());
    assert!(!cancelled.is_finished());
    assert!(!cancelled_entry_set_value.is_finished());
    assert!(!cancelled_unregister.is_finished());
    assert!(table.get("registered").unwrap().is_none()); // Reads are not blocked by the actor's RPC.
    assert!(Arc::ptr_eq(&entry.value().unwrap(), &first));
    cancelled.abort();
    assert!(matches!(cancelled.await, Err(error) if error.is_cancelled()));
    cancelled_entry_set_value.abort();
    assert!(matches!(cancelled_entry_set_value.await, Err(error) if error.is_cancelled()));
    assert!(Arc::ptr_eq(&entry.value().unwrap(), &first));
    cancelled_unregister.abort();
    assert!(matches!(cancelled_unregister.await, Err(error) if error.is_cancelled()));
    release.send(()).unwrap();
    assert!(unregister.await.unwrap().unwrap());
    assert!(write.await.unwrap().unwrap().is_none());
    assert!(same(table.get("registered").unwrap(), &second));
    assert!(Arc::ptr_eq(&entry.value().unwrap(), &first));
    assert!(Arc::ptr_eq(
        &entry.set_value(second.clone()).await.unwrap(),
        &first
    ));
    assert!(Arc::ptr_eq(&entry.value().unwrap(), &second));
    assert!(same(table.get("registered").unwrap(), &second));
    assert!(!table.contains_key("cancelled").unwrap());
    assert!(same(table.get("cancelled-unregister").unwrap(), &second));
    assert!(
        !runtime
            .unregister_consumer(&lease, Instant::now())
            .await
            .unwrap()
    );
    // Absent keys still issue RPCs and Broker rejection remains best-effort.
    assert!(
        !runtime
            .unregister_consumer_by_group("absent", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
    );
    let producer: Arc<dyn ProducerRuntime> = Arc::new(Producer);
    runtime
        .producer_table()
        .put("producer", producer)
        .await
        .unwrap();
    // Weak-owner expiry does not make the binding absent.
    assert!(
        runtime
            .unregister_producer_by_group("producer", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
    );
    assert!(
        runtime
            .unregister_consumer_by_group("registered", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
    );
    table.clear().await.unwrap();
    assert!(runtime.shutdown().await.unwrap());
    // Terminal maps can change without reviving transport or its control worker.
    table.put("after-stop", second.clone()).await.unwrap();
    assert!(
        runtime
            .unregister_consumer_by_group("after-stop", Instant::now())
            .await
            .unwrap()
    );
    assert!(
        !runtime
            .unregister_consumer_by_group("after-stop", Instant::now())
            .await
            .unwrap()
    );
    tokio::time::timeout(Duration::from_secs(5), broker_peer)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), name_peer)
        .await
        .unwrap()
        .unwrap();
}
