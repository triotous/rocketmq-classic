use super::RuntimeError;
use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_protocol::heartbeat::ConsumerData;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
};

/// Narrow MQProducerInner contract consumed by the shared runtime. Actual send
/// and transaction behavior belongs to the producer's owning implementation.
pub trait ProducerRuntime: Send + Sync + 'static {
    /// Current key used only by Java-style conditional map operations. `None`
    /// keeps Object identity semantics. Implementations with value-based Java
    /// `equals` should return the same key exactly when those values compare
    /// equal. The key is sampled when values are compared, outside registry locks.
    fn map_value_equality_key(&self) -> Option<String> {
        None
    }
    fn transaction_processor(
        &self,
    ) -> Option<Arc<dyn rocketmq_classic_remoting::client::RequestProcessor>> {
        None
    }
    /// Phase 15 supplies the runtime's correlation dispatcher, including its
    /// unknown/late-reply acknowledgement path. The responder's producerGroup
    /// is never a requester-routing key.
    fn reply_processor(
        &self,
        _correlation_id: &str,
    ) -> Option<Arc<dyn rocketmq_classic_remoting::client::RequestProcessor>> {
        None
    }
    fn publish_topics(&self) -> BTreeSet<String> {
        BTreeSet::new()
    }
    fn is_publish_topic_need_update(&self, _topic: &str) -> bool {
        false
    }
    fn update_topic_publish_info(&self, _topic: &str, _info: Arc<crate::route::TopicSnapshot>) {}
}

/// MQConsumerInner capabilities; optional futures are absent until an owning
/// consumer phase implements the operation. Absence is not reported as success.
pub trait ConsumerRuntime: Send + Sync + 'static {
    /// Current key used only by Java-style conditional map operations. `None`
    /// keeps Object identity semantics. Implementations with value-based Java
    /// `equals` should return the same key exactly when those values compare
    /// equal. The key is sampled when values are compared, outside registry locks.
    fn map_value_equality_key(&self) -> Option<String> {
        None
    }
    fn pull_message(
        &self,
        _request: Arc<dyn super::MessageRequest>,
    ) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
        None
    }
    fn pop_message(
        &self,
        _request: Arc<dyn super::MessageRequest>,
    ) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
        None
    }
    fn inbound_processor(
        &self,
        _operation: ConsumerOperation,
    ) -> Option<Arc<dyn rocketmq_classic_remoting::client::RequestProcessor>> {
        None
    }
    fn heartbeat_data(&self) -> ConsumerData;
    fn is_subscribe_topic_need_update(&self, _topic: &str) -> bool {
        false
    }
    fn update_topic_subscribe_info(&self, _topic: &str, _queues: BTreeSet<MessageQueue>) {}
    fn persist_consumer_offset(&self) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
        None
    }
    fn adjust_thread_pool(&self) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
        None
    }
    fn try_rebalance(&self) -> Option<Pin<Box<dyn Future<Output = bool> + Send + '_>>> {
        None
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumerOperation {
    ResetOffset,
    ConsumerStatus,
    RunningInfo,
    ConsumeDirectly,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum GroupKind {
    Producer,
    Consumer,
    Admin,
}

/// Registration identity, not a reference-count lease. Unregister explicitly;
/// cloning/dropping this token does not alter shared runtime lifecycle.
#[derive(Clone)]
pub struct GroupLease {
    pub(super) group: String,
    pub(super) kind: GroupKind,
    pub(super) identity: Arc<()>,
}

impl GroupLease {
    pub fn group(&self) -> &str {
        &self.group
    }
}

struct TableEntry<T: ?Sized> {
    identity: Arc<()>,
    handle: Weak<T>,
}
type ProducerEntry = TableEntry<dyn ProducerRuntime>;
type ConsumerEntry = TableEntry<dyn ConsumerRuntime>;

struct AdminEntry {
    identity: Arc<()>,
    // MQClientInstance retains MQAdminExtInner only to keep registration and
    // runtime ownership alive; Phase 17 supplies the admin operations.
    _owner: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

#[derive(Default)]
struct Entries {
    producers: BTreeMap<String, ProducerEntry>,
    consumers: BTreeMap<String, ConsumerEntry>,
    admins: BTreeMap<String, AdminEntry>,
}

#[derive(Default)]
pub(super) struct GroupRegistry {
    entries: Mutex<Entries>,
}

pub(super) struct RegistrySnapshot {
    pub producers: Vec<(String, Arc<dyn ProducerRuntime>)>,
    pub consumers: Vec<Arc<dyn ConsumerRuntime>>,
}

pub(super) type ConsumerEntries = Vec<(String, Arc<dyn ConsumerRuntime>)>;

impl GroupRegistry {
    pub(super) fn table_len(&self, kind: super::tables::TableKind) -> Result<usize, RuntimeError> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(match kind {
            super::tables::TableKind::Producer => entries.producers.len(),
            super::tables::TableKind::Consumer => entries.consumers.len(),
        })
    }
    pub(super) fn table_contains_key(
        &self,
        kind: super::tables::TableKind,
        key: &str,
    ) -> Result<bool, RuntimeError> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(match kind {
            super::tables::TableKind::Producer => entries.producers.contains_key(key),
            super::tables::TableKind::Consumer => entries.consumers.contains_key(key),
        })
    }
    pub(super) fn table_snapshot(
        &self,
        kind: super::tables::TableKind,
    ) -> Result<BTreeMap<String, super::tables::TableValue>, RuntimeError> {
        use super::tables::{TableKind, TableValue};
        let entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(match kind {
            TableKind::Producer => entries
                .producers
                .iter()
                .filter_map(|(key, entry)| {
                    entry
                        .handle
                        .upgrade()
                        .map(|value| (key.clone(), TableValue::Producer(value)))
                })
                .collect(),
            TableKind::Consumer => entries
                .consumers
                .iter()
                .filter_map(|(key, entry)| {
                    entry
                        .handle
                        .upgrade()
                        .map(|value| (key.clone(), TableValue::Consumer(value)))
                })
                .collect(),
        })
    }
    pub(super) fn mutate_table(
        &self,
        kind: super::tables::TableKind,
        mutation: &super::tables::TableMutation,
    ) -> Result<super::tables::TableChange, RuntimeError> {
        use super::tables::{TableKind, TableValue};
        {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            match kind {
                TableKind::Producer => mutate_map(
                    &mut entries.producers,
                    mutation,
                    |value| match value {
                        TableValue::Producer(owner) => Some(owner),
                        _ => None,
                    },
                    TableValue::Producer,
                ),
                TableKind::Consumer => mutate_map(
                    &mut entries.consumers,
                    mutation,
                    |value| match value {
                        TableValue::Consumer(owner) => Some(owner),
                        _ => None,
                    },
                    TableValue::Consumer,
                ),
            }
        }
    }

    pub fn has_active_users(&self, inner_producer: &str) -> Result<bool, RuntimeError> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(!entries.consumers.is_empty()
            || !entries.admins.is_empty()
            || entries
                .producers
                .keys()
                .any(|group| group != inner_producer))
    }

    pub fn clear(&self) -> Result<(), RuntimeError> {
        let previous = {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            std::mem::take(&mut *entries)
        };
        drop(previous);
        Ok(())
    }
    pub fn register_producer(
        &self,
        group: &str,
        handle: Arc<dyn ProducerRuntime>,
    ) -> Result<GroupLease, RuntimeError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if entries.producers.contains_key(group) {
            return Err(RuntimeError::DuplicateGroup {
                group: group.into(),
            });
        }
        let lease = GroupLease {
            group: group.into(),
            kind: GroupKind::Producer,
            identity: Arc::new(()),
        };
        entries.producers.insert(
            group.into(),
            ProducerEntry {
                identity: lease.identity.clone(),
                handle: Arc::downgrade(&handle),
            },
        );
        Ok(lease)
    }

    pub fn register_consumer(
        &self,
        group: &str,
        handle: Arc<dyn ConsumerRuntime>,
    ) -> Result<GroupLease, RuntimeError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if entries.consumers.contains_key(group) {
            return Err(RuntimeError::DuplicateGroup {
                group: group.into(),
            });
        }
        let lease = GroupLease {
            group: group.into(),
            kind: GroupKind::Consumer,
            identity: Arc::new(()),
        };
        entries.consumers.insert(
            group.into(),
            ConsumerEntry {
                identity: lease.identity.clone(),
                handle: Arc::downgrade(&handle),
            },
        );
        Ok(lease)
    }

    pub fn register_admin(&self, group: &str) -> Result<GroupLease, RuntimeError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if entries.admins.contains_key(group) {
            return Err(RuntimeError::DuplicateGroup {
                group: group.into(),
            });
        }
        let lease = GroupLease {
            group: group.into(),
            kind: GroupKind::Admin,
            identity: Arc::new(()),
        };
        entries.admins.insert(
            group.into(),
            AdminEntry {
                identity: lease.identity.clone(),
                _owner: None,
            },
        );
        Ok(lease)
    }

    pub fn register_admin_ext(
        &self,
        group: Option<&str>,
        owner: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Result<bool, RuntimeError> {
        let (Some(group), Some(owner)) = (group, owner) else {
            return Ok(false);
        };
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if entries.admins.contains_key(group) {
            drop(entries);
            drop(owner);
            return Ok(false);
        }
        entries.admins.insert(
            group.into(),
            AdminEntry {
                identity: Arc::new(()),
                _owner: Some(owner),
            },
        );
        Ok(true)
    }

    pub fn unregister_admin_ext(&self, group: Option<&str>) -> Result<(), RuntimeError> {
        let Some(group) = group else {
            return Err(RuntimeError::InvalidRoute("null admin group"));
        };
        let removed = {
            self.entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?
                .admins
                .remove(group)
        };
        drop(removed);
        Ok(())
    }

    pub fn unregister(&self, lease: &GroupLease, kind: GroupKind) -> Result<bool, RuntimeError> {
        if lease.kind != kind {
            return Ok(false);
        }
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let identity = match kind {
            GroupKind::Producer => entries
                .producers
                .get(&lease.group)
                .map(|entry| &entry.identity),
            GroupKind::Consumer => entries
                .consumers
                .get(&lease.group)
                .map(|entry| &entry.identity),
            GroupKind::Admin => entries
                .admins
                .get(&lease.group)
                .map(|entry| &entry.identity),
        };
        if !identity.is_some_and(|identity| Arc::ptr_eq(identity, &lease.identity)) {
            return Ok(false);
        }
        let removed_admin = match kind {
            GroupKind::Producer => {
                entries.producers.remove(&lease.group);
                None
            }
            GroupKind::Consumer => {
                entries.consumers.remove(&lease.group);
                None
            }
            GroupKind::Admin => entries.admins.remove(&lease.group),
        };
        drop(entries);
        drop(removed_admin);
        Ok(true)
    }

    pub(super) fn remove_by_group(
        &self,
        group: &str,
        kind: GroupKind,
    ) -> Result<bool, RuntimeError> {
        let (removed, removed_admin) = {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            match kind {
                GroupKind::Producer => (entries.producers.remove(group).is_some(), None),
                GroupKind::Consumer => (entries.consumers.remove(group).is_some(), None),
                GroupKind::Admin => {
                    let entry = entries.admins.remove(group);
                    (entry.is_some(), entry)
                }
            }
        };
        drop(removed_admin);
        Ok(removed)
    }

    pub fn snapshot(&self) -> Result<RegistrySnapshot, RuntimeError> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(RegistrySnapshot {
            producers: entries
                .producers
                .iter()
                .filter_map(|(group, entry)| {
                    entry.handle.upgrade().map(|handle| (group.clone(), handle))
                })
                .collect(),
            consumers: entries
                .consumers
                .values()
                .filter_map(|entry| entry.handle.upgrade())
                .collect(),
        })
    }

    pub(super) fn consumer_entries(&self) -> Result<ConsumerEntries, RuntimeError> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(entries
            .consumers
            .iter()
            .filter_map(|(group, entry)| {
                entry.handle.upgrade().map(|handle| (group.clone(), handle))
            })
            .collect())
    }

    pub fn select_producer(
        &self,
        group: &str,
    ) -> Result<Option<Arc<dyn ProducerRuntime>>, RuntimeError> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(entries
            .producers
            .get(group)
            .and_then(|entry| entry.handle.upgrade()))
    }

    pub fn select_consumer(
        &self,
        group: &str,
    ) -> Result<Option<Arc<dyn ConsumerRuntime>>, RuntimeError> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(entries
            .consumers
            .get(group)
            .and_then(|entry| entry.handle.upgrade()))
    }
}

fn mutate_map<T: ?Sized>(
    map: &mut BTreeMap<String, TableEntry<T>>,
    mutation: &super::tables::TableMutation,
    extract: impl for<'a> Fn(&'a super::tables::TableValue) -> Option<&'a Arc<T>>,
    wrap: impl Fn(Arc<T>) -> super::tables::TableValue,
) -> Result<super::tables::TableChange, RuntimeError> {
    use super::tables::{TableChange, TableMutation, ValueComparison};
    let key = match mutation {
        TableMutation::Put { key, .. }
        | TableMutation::Replace { key, .. }
        | TableMutation::Remove { key, .. } => key,
        TableMutation::Clear => {
            map.clear();
            return Ok(TableChange {
                previous: None,
                matched: true,
            });
        }
    };
    let present = map.contains_key(key);
    let previous = map.get(key).and_then(|entry| entry.handle.upgrade());
    let matches = |expected: &Option<super::tables::TableValue>,
                   comparison: &Option<ValueComparison>| {
        let Some(expected) = expected.as_ref() else {
            return true;
        };
        let Some(expected_value) = extract(expected) else {
            return false;
        };
        match comparison {
            Some(ValueComparison::Identity) => previous
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, expected_value)),
            None => false,
        }
    };
    let matched = match mutation {
        TableMutation::Put { only_absent, .. } => !only_absent || !present,
        TableMutation::Replace {
            expected,
            comparison,
            ..
        }
        | TableMutation::Remove {
            expected,
            comparison,
            ..
        } => present && matches(expected, comparison),
        TableMutation::Clear => false,
    };
    if matched {
        match mutation {
            TableMutation::Put { value, .. } | TableMutation::Replace { value, .. } => {
                let value = extract(value).ok_or(RuntimeError::RegistryPoisoned)?;
                map.insert(
                    key.clone(),
                    TableEntry {
                        identity: Arc::new(()),
                        handle: Arc::downgrade(value),
                    },
                );
            }
            TableMutation::Remove { .. } => {
                map.remove(key);
            }
            TableMutation::Clear => {}
        }
    }
    Ok(TableChange {
        previous: previous.map(wrap),
        matched,
    })
}

#[cfg(test)]
mod admin_owner_drop_tests {
    use super::*;
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };

    struct ReenterOnDrop {
        registry: Weak<GroupRegistry>,
        reentered: Arc<AtomicBool>,
    }

    impl Drop for ReenterOnDrop {
        fn drop(&mut self) {
            let Some(registry) = self.registry.upgrade() else {
                return;
            };
            let owner: Arc<dyn std::any::Any + Send + Sync> = Arc::new(());
            if matches!(
                registry.register_admin_ext(Some("from-drop"), Some(owner)),
                Ok(true)
            ) {
                self.reentered.store(true, Ordering::SeqCst);
            }
        }
    }

    fn assert_admin_owner_drop_can_reenter(
        remove: impl FnOnce(&GroupRegistry) -> Result<(), RuntimeError> + Send + 'static,
    ) {
        let registry = Arc::new(GroupRegistry::default());
        let reentered = Arc::new(AtomicBool::new(false));
        let owner = Arc::new(ReenterOnDrop {
            registry: Arc::downgrade(&registry),
            reentered: reentered.clone(),
        });
        let erased_owner: Arc<dyn std::any::Any + Send + Sync> = owner.clone();
        assert!(
            registry
                .register_admin_ext(Some("admin"), Some(erased_owner))
                .unwrap()
        );
        drop(owner);

        let (reply, receive) = mpsc::channel();
        let worker_registry = registry.clone();
        let worker = thread::spawn(move || {
            remove(&worker_registry).unwrap();
            let contains_reentered = worker_registry
                .entries
                .lock()
                .unwrap()
                .admins
                .contains_key("from-drop");
            let _ = reply.send(contains_reentered);
        });

        assert_eq!(
            receive.recv_timeout(Duration::from_secs(2)),
            Ok(true),
            "admin owner destructor must be able to re-enter the registry"
        );
        worker.join().unwrap();
        assert!(reentered.load(Ordering::SeqCst));
    }

    #[test]
    fn unconditional_admin_removal_drops_owner_after_unlock() {
        assert_admin_owner_drop_can_reenter(|registry| {
            registry.unregister_admin_ext(Some("admin"))
        });
    }

    #[test]
    fn registry_clear_drops_admin_owner_after_unlock() {
        assert_admin_owner_drop_can_reenter(GroupRegistry::clear);
    }
}
