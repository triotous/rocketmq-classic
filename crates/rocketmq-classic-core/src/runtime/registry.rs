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
    /// Phase 9 must wake the runtime-wide rebalance service, not just this group.
    ConsumerIdsChanged,
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

struct ProducerEntry {
    identity: Arc<()>,
    handle: Weak<dyn ProducerRuntime>,
}
struct ConsumerEntry {
    identity: Arc<()>,
    handle: Weak<dyn ConsumerRuntime>,
}

#[derive(Default)]
struct Entries {
    producers: BTreeMap<String, ProducerEntry>,
    consumers: BTreeMap<String, ConsumerEntry>,
    admins: BTreeMap<String, Arc<()>>,
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
        *self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)? = Entries::default();
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
        entries.admins.insert(group.into(), lease.identity.clone());
        Ok(lease)
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
            GroupKind::Admin => entries.admins.get(&lease.group),
        };
        if !identity.is_some_and(|identity| Arc::ptr_eq(identity, &lease.identity)) {
            return Ok(false);
        }
        match kind {
            GroupKind::Producer => {
                entries.producers.remove(&lease.group);
            }
            GroupKind::Consumer => {
                entries.consumers.remove(&lease.group);
            }
            GroupKind::Admin => {
                entries.admins.remove(&lease.group);
            }
        }
        Ok(true)
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
