use super::{
    ClientRuntime, ConsumerRuntime, ProducerRuntime, RuntimeError, RuntimeState,
    registry::GroupRegistry,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
};

#[derive(Clone, Copy)]
pub(super) enum TableKind {
    Producer,
    Consumer,
}
#[derive(Clone)]
pub(super) enum TableValue {
    Producer(Arc<dyn ProducerRuntime>),
    Consumer(Arc<dyn ConsumerRuntime>),
}
#[derive(Clone)]
pub(super) enum ValueComparison {
    Identity,
    JavaEquals(Option<String>),
}
#[derive(Clone)]
pub(super) enum TableMutation {
    Put {
        key: String,
        value: TableValue,
        value_equality_key: Option<String>,
        only_absent: bool,
    },
    Replace {
        key: String,
        value: TableValue,
        value_equality_key: Option<String>,
        expected: Option<TableValue>,
        comparison: Option<ValueComparison>,
    },
    Remove {
        key: String,
        expected: Option<TableValue>,
        comparison: Option<ValueComparison>,
    },
    Clear,
}
pub(super) struct TableChange {
    pub previous: Option<TableValue>,
    pub matched: bool,
}

#[derive(Clone)]
struct TableHandle {
    kind: TableKind,
    groups: Arc<GroupRegistry>,
    runtime: Weak<ClientRuntime>,
}
impl TableHandle {
    async fn mutate(&self, mutation: TableMutation) -> Result<TableChange, RuntimeError> {
        if let Some(runtime) = self.runtime.upgrade() {
            // Starting can invoke addressing capabilities that reenter the raw
            // maps; no unregister transaction is active in that state. All
            // other live actor states serialize against pending unregisters.
            if runtime.state() != RuntimeState::Starting {
                return runtime.mutate_registry_table(self.kind, mutation).await;
            }
        }
        // Raw maps remain available before start and after stop. Do not create
        // a control worker or revive transport for these primitive writes.
        self.groups.mutate_table(self.kind, &mutation)
    }
}

macro_rules! table {
    ($name:ident, $owner:ident, $kind:ident, $select:ident, $key_set:ident, $values_view:ident, $entry_set:ident) => {
        /// Live registry mapping using the runtime's weak-owner contract.
        /// Callers retain owner Arcs; owner drop does not remove its key.
        /// Primitive writes neither unregister from Brokers nor stop owners.
        #[derive(Clone)]
        pub struct $name(TableHandle);
        impl $name {
            pub(super) fn new(runtime: &Arc<ClientRuntime>) -> Self {
                Self(TableHandle {
                    kind: TableKind::$kind,
                    groups: runtime.groups.clone(),
                    runtime: Arc::downgrade(runtime),
                })
            }
            pub fn get(&self, key: &str) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                self.0.groups.$select(key)
            }
            /// Map.getOrDefault counterpart; the default is used only when the key is absent.
            pub fn get_or_default(
                &self,
                key: &str,
                default: Arc<dyn $owner>,
            ) -> Result<Arc<dyn $owner>, RuntimeError> {
                Ok(self.get(key)?.unwrap_or(default))
            }
            pub fn contains_key(&self, key: &str) -> Result<bool, RuntimeError> {
                self.0.groups.table_contains_key(self.0.kind, key)
            }
            pub fn contains_value(
                &self,
                value: &Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                let equality_key = value.map_value_equality_key();
                self.0.groups.table_contains_value(
                    self.0.kind,
                    &TableValue::$kind(value.clone()),
                    equality_key.as_deref(),
                )
            }
            pub fn len(&self) -> Result<usize, RuntimeError> {
                self.0.groups.table_len(self.0.kind)
            }
            pub fn is_empty(&self) -> Result<bool, RuntimeError> {
                Ok(self.len()? == 0)
            }
            /// Live handle for the backing map's key-set view.
            pub fn key_set_view(&self) -> $key_set {
                $key_set(self.0.clone())
            }
            /// Live handle for the backing map's values collection view.
            pub fn values_view(&self) -> $values_view {
                $values_view(self.0.clone())
            }
            /// Live handle for the backing map's entry-set view.
            pub fn entry_set_view(&self) -> $entry_set {
                $entry_set(self.0.clone())
            }
            /// Detached owned iteration copy; this handle itself remains live.
            pub fn snapshot(&self) -> Result<BTreeMap<String, Arc<dyn $owner>>, RuntimeError> {
                Ok(self
                    .0
                    .groups
                    .table_snapshot(self.0.kind)?
                    .into_iter()
                    .filter_map(|(key, value)| match value {
                        TableValue::$kind(owner) => Some((key, owner)),
                        _ => None,
                    })
                    .collect())
            }
            /// Detached key view corresponding to Java's keySet iteration;
            /// mutations require explicit table methods.
            pub fn keys(&self) -> Result<BTreeSet<String>, RuntimeError> {
                Ok(self.snapshot()?.into_keys().collect())
            }
            /// Detached value view corresponding to Java's values iteration.
            pub fn values(&self) -> Result<Vec<Arc<dyn $owner>>, RuntimeError> {
                Ok(self.snapshot()?.into_values().collect())
            }
            /// Visits a detached snapshot. Entry order is unspecified by this API.
            pub fn for_each(&self, mut action: impl FnMut(&str, &Arc<dyn $owner>)) -> Result<(), RuntimeError> {
                for (key, value) in self.snapshot()? {
                    action(&key, &value);
                }
                Ok(())
            }
            pub async fn put(
                &self,
                key: impl Into<String>,
                value: Arc<dyn $owner>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let value_equality_key = value.map_value_equality_key();
                self.write(TableMutation::Put {
                    key: key.into(),
                    value: TableValue::$kind(value),
                    value_equality_key,
                    only_absent: false,
                })
                .await
            }
            pub async fn put_if_absent(
                &self,
                key: impl Into<String>,
                value: Arc<dyn $owner>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let value_equality_key = value.map_value_equality_key();
                self.write(TableMutation::Put {
                    key: key.into(),
                    value: TableValue::$kind(value),
                    value_equality_key,
                    only_absent: true,
                })
                .await
            }
            /// Inserts each entry independently, matching Map.putAll's per-entry writes.
            pub async fn put_all(
                &self,
                entries: BTreeMap<String, Arc<dyn $owner>>,
            ) -> Result<(), RuntimeError> {
                for (key, value) in entries {
                    self.put(key, value).await?;
                }
                Ok(())
            }
            pub async fn replace(
                &self,
                key: impl Into<String>,
                value: Arc<dyn $owner>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let value_equality_key = value.map_value_equality_key();
                let change = self
                    .0
                    .mutate(TableMutation::Replace {
                        key: key.into(),
                        value: TableValue::$kind(value),
                        value_equality_key,
                        expected: None,
                        comparison: None,
                    })
                    .await?;
                Ok(if change.matched {
                    Self::previous(change)
                } else {
                    None
                })
            }
            pub async fn replace_if_same(
                &self,
                key: impl Into<String>,
                expected: &Arc<dyn $owner>,
                value: Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                let value_equality_key = value.map_value_equality_key();
                Ok(self
                    .0
                    .mutate(TableMutation::Replace {
                        key: key.into(),
                        value_equality_key,
                        value: TableValue::$kind(value),
                        expected: Some(TableValue::$kind(expected.clone())),
                        comparison: Some(ValueComparison::Identity),
                    })
                    .await?
                    .matched)
            }
            /// Java ConcurrentMap conditional replacement using owner value
            /// equality, with identity as the default owner equality.
            pub async fn replace_if_equal(
                &self,
                key: impl Into<String>,
                expected: &Arc<dyn $owner>,
                value: Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                let value_equality_key = value.map_value_equality_key();
                let expected_key = expected.map_value_equality_key();
                Ok(self
                    .0
                    .mutate(TableMutation::Replace {
                        key: key.into(),
                        value: TableValue::$kind(value),
                        value_equality_key,
                        expected: Some(TableValue::$kind(expected.clone())),
                        comparison: Some(ValueComparison::JavaEquals(expected_key)),
                    })
                    .await?
                    .matched)
            }
            pub async fn remove(
                &self,
                key: impl Into<String>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                self.write(TableMutation::Remove {
                    key: key.into(),
                    expected: None,
                    comparison: None,
                })
                .await
            }
            pub async fn remove_if_same(
                &self,
                key: impl Into<String>,
                expected: &Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                Ok(self
                    .0
                    .mutate(TableMutation::Remove {
                        key: key.into(),
                        expected: Some(TableValue::$kind(expected.clone())),
                        comparison: Some(ValueComparison::Identity),
                    })
                    .await?
                    .matched)
            }
            /// Java ConcurrentMap conditional removal using owner value
            /// equality, with identity as the default owner equality.
            pub async fn remove_if_equal(
                &self,
                key: impl Into<String>,
                expected: &Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                let expected_key = expected.map_value_equality_key();
                Ok(self
                    .0
                    .mutate(TableMutation::Remove {
                        key: key.into(),
                        expected: Some(TableValue::$kind(expected.clone())),
                        comparison: Some(ValueComparison::JavaEquals(expected_key)),
                    })
                    .await?
                    .matched)
            }
            /// ConcurrentMap-style remapping operations use conditional table
            /// mutations and may call the closure again after a concurrent write.
            pub async fn compute_if_absent(
                &self,
                key: impl Into<String>,
                mut mapping: impl FnMut(&str) -> Option<Arc<dyn $owner>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let key = key.into();
                if let Some(current) = self.get(&key)? {
                    return Ok(Some(current));
                }
                let Some(value) = mapping(&key) else {
                    return Ok(None);
                };
                Ok(Some(
                    self.put_if_absent(key, value.clone())
                        .await?
                        .unwrap_or(value),
                ))
            }
            pub async fn compute_if_present(
                &self,
                key: impl Into<String>,
                mut remapping: impl FnMut(&str, &Arc<dyn $owner>) -> Option<Arc<dyn $owner>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let key = key.into();
                loop {
                    let Some(current) = self.get(&key)? else {
                        return Ok(None);
                    };
                    match remapping(&key, &current) {
                        Some(value) => {
                            if self.replace_if_equal(&key, &current, value.clone()).await? {
                                return Ok(Some(value));
                            }
                        }
                        None => {
                            if self.remove_if_equal(&key, &current).await? {
                                return Ok(None);
                            }
                        }
                    }
                }
            }
            pub async fn compute(
                &self,
                key: impl Into<String>,
                mut remapping: impl FnMut(
                    &str,
                    Option<&Arc<dyn $owner>>,
                ) -> Option<Arc<dyn $owner>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let key = key.into();
                loop {
                    let current = self.get(&key)?;
                    let value = remapping(&key, current.as_ref());
                    match (current.as_ref(), value) {
                        (None, None) => return Ok(None),
                        (None, Some(value)) => {
                            if self.put_if_absent(&key, value.clone()).await?.is_none() {
                                return Ok(Some(value));
                            }
                        }
                        (Some(current), Some(value)) => {
                            if self.replace_if_equal(&key, current, value.clone()).await? {
                                return Ok(Some(value));
                            }
                        }
                        (Some(current), None) => {
                            if self.remove_if_equal(&key, current).await? {
                                return Ok(None);
                            }
                        }
                    }
                }
            }
            pub async fn merge(
                &self,
                key: impl Into<String>,
                value: Arc<dyn $owner>,
                mut remapping: impl FnMut(
                    &Arc<dyn $owner>,
                    &Arc<dyn $owner>,
                ) -> Option<Arc<dyn $owner>>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let key = key.into();
                loop {
                    let Some(current) = self.get(&key)? else {
                        if self.put_if_absent(&key, value.clone()).await?.is_none() {
                            return Ok(Some(value));
                        }
                        continue;
                    };
                    match remapping(&current, &value) {
                        Some(next) => {
                            if self.replace_if_equal(&key, &current, next.clone()).await? {
                                return Ok(Some(next));
                            }
                        }
                        None => {
                            if self.remove_if_equal(&key, &current).await? {
                                return Ok(None);
                            }
                        }
                    }
                }
            }
            pub async fn replace_all(
                &self,
                mut remapping: impl FnMut(&str, &Arc<dyn $owner>) -> Arc<dyn $owner>,
            ) -> Result<(), RuntimeError> {
                for key in self.keys()? {
                    loop {
                        let Some(current) = self.get(&key)? else {
                            break;
                        };
                        let value = remapping(&key, &current);
                        if self.replace_if_equal(&key, &current, value).await? {
                            break;
                        }
                    }
                }
                Ok(())
            }
            pub async fn clear(&self) -> Result<(), RuntimeError> {
                self.0.mutate(TableMutation::Clear).await.map(|_| ())
            }
            async fn write(
                &self,
                mutation: TableMutation,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                Ok(Self::previous(self.0.mutate(mutation).await?))
            }
            fn previous(change: TableChange) -> Option<Arc<dyn $owner>> {
                change.previous.and_then(|value| match value {
                    TableValue::$kind(owner) => Some(owner),
                    _ => None,
                })
            }
        }
    };
}
macro_rules! table_views {
    ($table:ident, $owner:ident, $kind:ident, $key_set:ident, $values_view:ident, $entry_set:ident) => {
        #[derive(Clone)]
        pub struct $key_set(TableHandle);
        impl $key_set {
            pub fn contains(&self, key: &str) -> Result<bool, RuntimeError> {
                self.0.groups.table_contains_key(self.0.kind, key)
            }
            pub fn len(&self) -> Result<usize, RuntimeError> {
                self.0.groups.table_len(self.0.kind)
            }
            pub fn is_empty(&self) -> Result<bool, RuntimeError> {
                Ok(self.len()? == 0)
            }
            /// Detached iteration copy from this live view.
            pub fn snapshot(&self) -> Result<BTreeSet<String>, RuntimeError> {
                Ok(self
                    .0
                    .groups
                    .table_snapshot(self.0.kind)?
                    .into_keys()
                    .collect())
            }
            pub async fn remove(&self, key: impl Into<String>) -> Result<bool, RuntimeError> {
                self.0
                    .mutate(TableMutation::Remove {
                        key: key.into(),
                        expected: None,
                        comparison: None,
                    })
                    .await
                    .map(|change| change.matched)
            }
            /// Removes keys accepted by the predicate, visiting a detached key copy.
            pub async fn remove_if(
                &self,
                mut predicate: impl FnMut(&str) -> bool,
            ) -> Result<usize, RuntimeError> {
                let mut removed = 0;
                for key in self.snapshot()? {
                    if predicate(&key) && self.remove(key).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            /// Removes keys rejected by the predicate, visiting a detached key copy.
            pub async fn retain(
                &self,
                mut predicate: impl FnMut(&str) -> bool,
            ) -> Result<usize, RuntimeError> {
                let mut removed = 0;
                for key in self.snapshot()? {
                    if !predicate(&key) && self.remove(key).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            pub async fn remove_all(&self, keys: &BTreeSet<String>) -> Result<usize, RuntimeError> {
                self.remove_if(|key| keys.contains(key)).await
            }
            pub async fn retain_all(&self, keys: &BTreeSet<String>) -> Result<usize, RuntimeError> {
                self.retain(|key| keys.contains(key)).await
            }
            pub async fn clear(&self) -> Result<(), RuntimeError> {
                self.0.mutate(TableMutation::Clear).await.map(|_| ())
            }
        }

        #[derive(Clone)]
        pub struct $values_view(TableHandle);
        impl $values_view {
            fn table(&self) -> $table {
                $table(self.0.clone())
            }
            pub fn contains(&self, value: &Arc<dyn $owner>) -> Result<bool, RuntimeError> {
                self.table().contains_value(value)
            }
            pub fn len(&self) -> Result<usize, RuntimeError> {
                self.0.groups.table_len(self.0.kind)
            }
            pub fn is_empty(&self) -> Result<bool, RuntimeError> {
                Ok(self.len()? == 0)
            }
            /// Detached iteration copy from this live view.
            pub fn snapshot(&self) -> Result<Vec<Arc<dyn $owner>>, RuntimeError> {
                self.table().values()
            }
            pub async fn remove(&self, value: &Arc<dyn $owner>) -> Result<bool, RuntimeError> {
                for key in self.table().keys()? {
                    if self.table().remove_if_equal(key, value).await? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            /// Removes values accepted by the predicate from a detached entry copy.
            pub async fn remove_if(
                &self,
                mut predicate: impl FnMut(&Arc<dyn $owner>) -> bool,
            ) -> Result<usize, RuntimeError> {
                let table = self.table();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    if predicate(&value) && table.remove_if_equal(key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            /// Removes values rejected by the predicate from a detached entry copy.
            pub async fn retain(
                &self,
                mut predicate: impl FnMut(&Arc<dyn $owner>) -> bool,
            ) -> Result<usize, RuntimeError> {
                let table = self.table();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    if !predicate(&value) && table.remove_if_equal(key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            pub async fn remove_all(
                &self,
                values: &[Arc<dyn $owner>],
            ) -> Result<usize, RuntimeError> {
                self.mutate_membership(values, false).await
            }
            pub async fn retain_all(
                &self,
                values: &[Arc<dyn $owner>],
            ) -> Result<usize, RuntimeError> {
                self.mutate_membership(values, true).await
            }
            async fn mutate_membership(
                &self,
                values: &[Arc<dyn $owner>],
                retain_matches: bool,
            ) -> Result<usize, RuntimeError> {
                let table = self.table();
                let entry_set = table.entry_set_view();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    let mut matches = false;
                    for candidate in values {
                        if entry_set.contains(&key, candidate)? {
                            matches = true;
                            break;
                        }
                    }
                    if matches != retain_matches && table.remove_if_equal(&key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            pub async fn clear(&self) -> Result<(), RuntimeError> {
                self.0.mutate(TableMutation::Clear).await.map(|_| ())
            }
        }

        #[derive(Clone)]
        pub struct $entry_set(TableHandle);
        impl $entry_set {
            fn table(&self) -> $table {
                $table(self.0.clone())
            }
            pub fn contains(
                &self,
                key: &str,
                value: &Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                let equality_key = value.map_value_equality_key();
                self.0.groups.table_contains_entry(
                    self.0.kind,
                    key,
                    &TableValue::$kind(value.clone()),
                    equality_key.as_deref(),
                )
            }
            pub fn len(&self) -> Result<usize, RuntimeError> {
                self.0.groups.table_len(self.0.kind)
            }
            pub fn is_empty(&self) -> Result<bool, RuntimeError> {
                Ok(self.len()? == 0)
            }
            /// Detached iteration copy from this live view.
            pub fn snapshot(&self) -> Result<BTreeMap<String, Arc<dyn $owner>>, RuntimeError> {
                self.table().snapshot()
            }
            pub async fn remove(
                &self,
                key: impl Into<String>,
                value: &Arc<dyn $owner>,
            ) -> Result<bool, RuntimeError> {
                self.table().remove_if_equal(key, value).await
            }
            /// Removes entries accepted by the predicate from a detached entry copy.
            pub async fn remove_if(
                &self,
                mut predicate: impl FnMut(&str, &Arc<dyn $owner>) -> bool,
            ) -> Result<usize, RuntimeError> {
                let table = self.table();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    if predicate(&key, &value) && table.remove_if_equal(key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            /// Removes entries rejected by the predicate from a detached entry copy.
            pub async fn retain(
                &self,
                mut predicate: impl FnMut(&str, &Arc<dyn $owner>) -> bool,
            ) -> Result<usize, RuntimeError> {
                let table = self.table();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    if !predicate(&key, &value) && table.remove_if_equal(key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            pub async fn remove_all(
                &self,
                entries: &BTreeMap<String, Arc<dyn $owner>>,
            ) -> Result<usize, RuntimeError> {
                self.mutate_membership(entries, false).await
            }
            pub async fn retain_all(
                &self,
                entries: &BTreeMap<String, Arc<dyn $owner>>,
            ) -> Result<usize, RuntimeError> {
                self.mutate_membership(entries, true).await
            }
            async fn mutate_membership(
                &self,
                candidates: &BTreeMap<String, Arc<dyn $owner>>,
                retain_matches: bool,
            ) -> Result<usize, RuntimeError> {
                let table = self.table();
                let mut removed = 0;
                for (key, value) in table.snapshot()? {
                    let matches = match candidates.get(&key) {
                        Some(candidate) => self.contains(&key, candidate)?,
                        None => false,
                    };
                    if matches != retain_matches && table.remove_if_equal(&key, &value).await? {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            pub async fn clear(&self) -> Result<(), RuntimeError> {
                self.0.mutate(TableMutation::Clear).await.map(|_| ())
            }
        }
    };
}
table!(
    ProducerTable,
    ProducerRuntime,
    Producer,
    select_producer,
    ProducerKeySetView,
    ProducerValuesView,
    ProducerEntrySetView
);
table!(
    ConsumerTable,
    ConsumerRuntime,
    Consumer,
    select_consumer,
    ConsumerKeySetView,
    ConsumerValuesView,
    ConsumerEntrySetView
);
table_views!(
    ProducerTable,
    ProducerRuntime,
    Producer,
    ProducerKeySetView,
    ProducerValuesView,
    ProducerEntrySetView
);
table_views!(
    ConsumerTable,
    ConsumerRuntime,
    Consumer,
    ConsumerKeySetView,
    ConsumerValuesView,
    ConsumerEntrySetView
);

#[cfg(test)]
mod tests {
    use super::super::{ClientRuntimeManager, registry::ConsumerRuntime};
    use super::*;
    use crate::config::ClientConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(start_paused = true)]
    async fn startup_addressing_capability_can_reenter_live_table_without_waiting_on_itself() {
        use std::{future::Future, pin::Pin, sync::Mutex};
        struct Producer;
        impl ProducerRuntime for Producer {}
        struct Addressing {
            table: Mutex<Option<ProducerTable>>,
            owner: Arc<dyn ProducerRuntime>,
        }
        impl crate::namesrv::TopAddressing for Addressing {
            fn fetch_ns_addr(&self) -> Pin<Box<dyn Future<Output = Option<String>> + Send + '_>> {
                Box::pin(async move {
                    let table = self.table.lock().unwrap().clone().unwrap();
                    table
                        .put("from-addressing", self.owner.clone())
                        .await
                        .unwrap();
                    Some("127.0.0.1:1".into()) // Only an address result; no peer is contacted.
                })
            }
        }
        let provider = Arc::new(Addressing {
            table: Mutex::new(None),
            owner: Arc::new(Producer),
        });
        let manager = ClientRuntimeManager::with_top_addressing(provider.clone());
        let mut config = ClientConfig::new("table-addressing-reentry");
        config.namesrv_addr = None;
        let runtime = manager.get_or_create(&config).await.unwrap();
        let table = runtime.producer_table();
        *provider.table.lock().unwrap() = Some(table.clone());
        tokio::time::timeout(tokio::time::Duration::from_secs(5), runtime.start())
            .await
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(
            &table.get("from-addressing").unwrap().unwrap(),
            &provider.owner
        ));
        table.clear().await.unwrap();
        assert!(runtime.shutdown().await.unwrap());
    }

    #[tokio::test]
    async fn raw_maps_create_no_worker_put_if_absent_is_atomic_and_owner_drop_reenters() {
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&ClientConfig::new("table-atomic"))
            .await
            .unwrap();
        let table = runtime.producer_table();
        struct Producer;
        impl ProducerRuntime for Producer {}
        let owner: Arc<dyn ProducerRuntime> = Arc::new(Producer);
        let mut calls = Vec::new();
        for _ in 0..64 {
            let table = table.clone();
            let owner = owner.clone();
            calls.push(tokio::spawn(async move {
                table.put_if_absent("group", owner).await.unwrap().is_none()
            }));
        }
        let mut installed = 0;
        for call in calls {
            installed += usize::from(call.await.unwrap());
        }
        assert_eq!(installed, 1);
        let detached = table.snapshot().unwrap();
        table.clear().await.unwrap();
        assert_eq!(detached.len(), 1);
        assert!(table.is_empty().unwrap());
        assert!(runtime.control.existing_sender().unwrap().is_none());
        struct ReentrantOwner {
            table: ConsumerTable,
            drops: Arc<AtomicUsize>,
        }
        impl ConsumerRuntime for ReentrantOwner {
            fn heartbeat_data(&self) -> rocketmq_classic_protocol::heartbeat::ConsumerData {
                Default::default()
            }
        }
        impl Drop for ReentrantOwner {
            fn drop(&mut self) {
                assert!(self.table.get("drop").unwrap().is_none());
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        let consumer = runtime.consumer_table();
        let drops = Arc::new(AtomicUsize::new(0));
        consumer
            .put(
                "drop",
                Arc::new(ReentrantOwner {
                    table: consumer.clone(),
                    drops: drops.clone(),
                }),
            )
            .await
            .unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(consumer.contains_key("drop").unwrap());
        consumer.clear().await.unwrap();
        assert!(runtime.control.existing_sender().unwrap().is_none());
    }
}
