use super::{
    ClientRuntime, ConsumerRuntime, ProducerRuntime, RuntimeError, RuntimeState,
    registry::GroupRegistry,
};
use std::{
    collections::BTreeMap,
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
pub(super) enum TableMutation {
    Put {
        key: String,
        value: TableValue,
        only_absent: bool,
    },
    Replace {
        key: String,
        value: TableValue,
        expected: Option<TableValue>,
    },
    Remove {
        key: String,
        expected: Option<TableValue>,
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
    ($name:ident, $owner:ident, $kind:ident, $select:ident) => {
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
            pub fn contains_key(&self, key: &str) -> Result<bool, RuntimeError> {
                self.0.groups.table_contains_key(self.0.kind, key)
            }
            pub fn len(&self) -> Result<usize, RuntimeError> {
                self.0.groups.table_len(self.0.kind)
            }
            pub fn is_empty(&self) -> Result<bool, RuntimeError> {
                Ok(self.len()? == 0)
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
            pub async fn put(
                &self,
                key: impl Into<String>,
                value: Arc<dyn $owner>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                self.write(TableMutation::Put {
                    key: key.into(),
                    value: TableValue::$kind(value),
                    only_absent: false,
                })
                .await
            }
            pub async fn put_if_absent(
                &self,
                key: impl Into<String>,
                value: Arc<dyn $owner>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                self.write(TableMutation::Put {
                    key: key.into(),
                    value: TableValue::$kind(value),
                    only_absent: true,
                })
                .await
            }
            pub async fn replace(
                &self,
                key: impl Into<String>,
                value: Arc<dyn $owner>,
            ) -> Result<Option<Arc<dyn $owner>>, RuntimeError> {
                let change = self
                    .0
                    .mutate(TableMutation::Replace {
                        key: key.into(),
                        value: TableValue::$kind(value),
                        expected: None,
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
                Ok(self
                    .0
                    .mutate(TableMutation::Replace {
                        key: key.into(),
                        value: TableValue::$kind(value),
                        expected: Some(TableValue::$kind(expected.clone())),
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
                    })
                    .await?
                    .matched)
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
table!(ProducerTable, ProducerRuntime, Producer, select_producer);
table!(ConsumerTable, ConsumerRuntime, Consumer, select_consumer);

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
