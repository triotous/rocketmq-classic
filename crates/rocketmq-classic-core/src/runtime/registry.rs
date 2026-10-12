use super::RuntimeError;
use rocketmq_classic_common::message::MessageExt;
use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_protocol::heartbeat::ConsumerData;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
};
use tokio::{
    sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, oneshot},
    time::Instant,
};

/// Pinned Java 5.5.1 bounds its async sender queue at 50,000 entries. Reuse
/// that admission ceiling for pending request/reply correlations so a missing
/// responder cannot grow the shared runtime table without limit.
const DEFAULT_PENDING_REPLY_CAPACITY: usize = 50_000;

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
    handle: Arc<T>,
}
type ProducerEntry = TableEntry<dyn ProducerRuntime>;
type ConsumerEntry = TableEntry<dyn ConsumerRuntime>;

struct AdminEntry {
    identity: Arc<()>,
    // MQClientInstance retains MQAdminExtInner only to keep registration and
    // runtime ownership alive; Phase 17 supplies the admin operations.
    _owner: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

struct PendingReplyEntry {
    owner: Arc<()>,
    deadline: Instant,
    send_succeeded: bool,
    sender: oneshot::Sender<PendingReplyCompletion>,
    _admission: OwnedSemaphorePermit,
}

#[derive(Default)]
struct Entries {
    producers: BTreeMap<String, ProducerEntry>,
    consumers: BTreeMap<String, ConsumerEntry>,
    admins: BTreeMap<String, AdminEntry>,
    pending_replies: HashMap<String, PendingReplyEntry>,
}

/// Terminal result delivered to the owner of one pending request/reply entry.
#[derive(Debug)]
pub enum PendingReplyCompletion {
    Reply(MessageExt),
    Timeout { send_succeeded: bool },
    SendFailed(String),
    ProducerShutdown,
}

/// Identity-bound operations for the currently pending correlation entry.
#[derive(Clone)]
pub struct PendingReplyControl {
    registry: Weak<GroupRegistry>,
    correlation_id: String,
    owner: Arc<()>,
}

impl PendingReplyControl {
    pub fn mark_send_succeeded(&self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        if let Ok(mut entries) = registry.entries.lock() {
            if let Some(entry) = entries.pending_replies.get_mut(&self.correlation_id) {
                if Arc::ptr_eq(&entry.owner, &self.owner) {
                    entry.send_succeeded = true;
                }
            }
        }
    }

    /// Returns false when another terminal path already claimed the entry.
    pub fn fail_send(&self, cause: String) -> bool {
        let Some(registry) = self.registry.upgrade() else {
            return false;
        };
        let sender = {
            let Ok(mut entries) = registry.entries.lock() else {
                return false;
            };
            let owned = entries
                .pending_replies
                .get(&self.correlation_id)
                .is_some_and(|entry| Arc::ptr_eq(&entry.owner, &self.owner));
            if owned {
                entries
                    .pending_replies
                    .remove(&self.correlation_id)
                    .map(|entry| entry.sender)
            } else {
                None
            }
        };
        sender.is_some_and(|sender| {
            sender
                .send(PendingReplyCompletion::SendFailed(cause))
                .is_ok()
        })
    }

    pub fn cancel(&self) -> bool {
        let Some(registry) = self.registry.upgrade() else {
            return false;
        };
        let Ok(mut entries) = registry.entries.lock() else {
            return false;
        };
        let owned = entries
            .pending_replies
            .get(&self.correlation_id)
            .is_some_and(|entry| Arc::ptr_eq(&entry.owner, &self.owner));
        owned
            && entries
                .pending_replies
                .remove(&self.correlation_id)
                .is_some()
    }

    /// Complete the owned entry with timeout only when its monotonic deadline
    /// has elapsed. A reply or shared-scheduler scan may already own it.
    pub fn expire_if_due(&self, now: Instant) -> bool {
        let Some(registry) = self.registry.upgrade() else {
            return false;
        };
        let expired = {
            let Ok(mut entries) = registry.entries.lock() else {
                return false;
            };
            let expired = entries
                .pending_replies
                .get(&self.correlation_id)
                .is_some_and(|entry| {
                    Arc::ptr_eq(&entry.owner, &self.owner) && entry.deadline <= now
                });
            if expired {
                entries.pending_replies.remove(&self.correlation_id)
            } else {
                None
            }
        };
        let Some(entry) = expired else {
            return false;
        };
        let _ = entry.sender.send(PendingReplyCompletion::Timeout {
            send_succeeded: entry.send_succeeded,
        });
        true
    }
}

/// Cancellable receiver for one producer-owned request/reply correlation.
pub struct PendingReplyRequest {
    receiver: oneshot::Receiver<PendingReplyCompletion>,
    control: PendingReplyControl,
}

impl PendingReplyRequest {
    pub fn control(&self) -> PendingReplyControl {
        self.control.clone()
    }

    pub async fn receive(&mut self) -> Result<PendingReplyCompletion, oneshot::error::RecvError> {
        (&mut self.receiver).await
    }
}

impl Drop for PendingReplyRequest {
    fn drop(&mut self) {
        self.control.cancel();
    }
}

pub(super) type TableMutationGate = AsyncMutex<()>;
type TableMutationGates =
    Mutex<HashMap<(super::tables::TableKind, String), Weak<TableMutationGate>>>;

pub(super) struct GroupRegistry {
    entries: Mutex<Entries>,
    table_mutation_gates: TableMutationGates,
    pending_reply_admission: Arc<Semaphore>,
}

impl Default for GroupRegistry {
    fn default() -> Self {
        Self::with_pending_reply_capacity(DEFAULT_PENDING_REPLY_CAPACITY)
    }
}

pub(super) struct RegistrySnapshot {
    pub producers: Vec<(String, Arc<dyn ProducerRuntime>)>,
    pub consumers: Vec<Arc<dyn ConsumerRuntime>>,
}

pub(super) type ConsumerEntries = Vec<(String, Arc<dyn ConsumerRuntime>)>;

impl GroupRegistry {
    fn with_pending_reply_capacity(capacity: usize) -> Self {
        Self {
            entries: Mutex::default(),
            table_mutation_gates: Mutex::default(),
            pending_reply_admission: Arc::new(Semaphore::new(capacity.max(1))),
        }
    }

    pub(super) fn table_mutation_gate(
        &self,
        kind: super::tables::TableKind,
        key: &str,
    ) -> Result<Arc<TableMutationGate>, RuntimeError> {
        let mut gates = self
            .table_mutation_gates
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let identity = (kind, key.to_owned());
        if let Some(gate) = gates.get(&identity).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        let gate = Arc::new(AsyncMutex::new(()));
        gates.insert(identity, Arc::downgrade(&gate));
        if gates.len() >= 256 {
            gates.retain(|_, gate| gate.strong_count() > 0);
        }
        Ok(gate)
    }

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
                .map(|(key, entry)| (key.clone(), TableValue::Producer(entry.handle.clone())))
                .collect(),
            TableKind::Consumer => entries
                .consumers
                .iter()
                .map(|(key, entry)| (key.clone(), TableValue::Consumer(entry.handle.clone())))
                .collect(),
        })
    }
    pub(super) fn mutate_table(
        &self,
        kind: super::tables::TableKind,
        mutation: &super::tables::TableMutation,
    ) -> Result<super::tables::TableChange, RuntimeError> {
        use super::tables::{TableKind, TableValue};
        if matches!(mutation, super::tables::TableMutation::Clear) {
            match kind {
                TableKind::Producer => {
                    let removed = {
                        let mut entries = self
                            .entries
                            .lock()
                            .map_err(|_| RuntimeError::RegistryPoisoned)?;
                        std::mem::take(&mut entries.producers)
                    };
                    drop(removed);
                }
                TableKind::Consumer => {
                    let removed = {
                        let mut entries = self
                            .entries
                            .lock()
                            .map_err(|_| RuntimeError::RegistryPoisoned)?;
                        std::mem::take(&mut entries.consumers)
                    };
                    drop(removed);
                }
            }
            return Ok(super::tables::TableChange {
                previous: None,
                matched: true,
            });
        }
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
        let (previous, pending) = {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            let pending = std::mem::take(&mut entries.pending_replies);
            (std::mem::take(&mut *entries), pending)
        };
        for entry in pending.into_values() {
            let _ = entry.sender.send(PendingReplyCompletion::ProducerShutdown);
        }
        drop(previous);
        Ok(())
    }

    pub async fn register_pending_reply(
        self: &Arc<Self>,
        lease: &GroupLease,
        correlation_id: &str,
        deadline: Instant,
    ) -> Result<PendingReplyRequest, RuntimeError> {
        if lease.kind != GroupKind::Producer {
            return Err(RuntimeError::InvalidRoute(
                "request/reply requires a producer registration",
            ));
        }
        let admission = tokio::time::timeout_at(
            deadline,
            self.pending_reply_admission.clone().acquire_owned(),
        )
        .await
        .map_err(|_| RuntimeError::RequestReplyAdmissionTimeout)?
        .map_err(|_| RuntimeError::RequestReplyAdmissionTimeout)?;
        if Instant::now() >= deadline {
            return Err(RuntimeError::RequestReplyAdmissionTimeout);
        }
        let (sender, receiver) = oneshot::channel();
        {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            let active = entries
                .producers
                .get(&lease.group)
                .is_some_and(|entry| Arc::ptr_eq(&entry.identity, &lease.identity));
            if !active {
                return Err(RuntimeError::InvalidRoute(
                    "producer registration is no longer active",
                ));
            }
            if entries.pending_replies.contains_key(correlation_id) {
                return Err(RuntimeError::DuplicateCorrelation {
                    correlation_id: correlation_id.to_owned(),
                });
            }
            entries.pending_replies.insert(
                correlation_id.to_owned(),
                PendingReplyEntry {
                    owner: lease.identity.clone(),
                    deadline,
                    send_succeeded: false,
                    sender,
                    _admission: admission,
                },
            );
        }
        Ok(PendingReplyRequest {
            receiver,
            control: PendingReplyControl {
                registry: Arc::downgrade(self),
                correlation_id: correlation_id.to_owned(),
                owner: lease.identity.clone(),
            },
        })
    }

    pub fn complete_pending_reply(&self, correlation_id: &str, reply: MessageExt) -> bool {
        let entry = self
            .entries
            .lock()
            .ok()
            .and_then(|mut entries| entries.pending_replies.remove(correlation_id));
        entry.is_some_and(|entry| {
            entry
                .sender
                .send(PendingReplyCompletion::Reply(reply))
                .is_ok()
        })
    }

    pub fn expire_pending_replies(&self, now: Instant) -> usize {
        let expired = {
            let Ok(mut entries) = self.entries.lock() else {
                return 0;
            };
            let expired_ids = entries
                .pending_replies
                .iter()
                .filter(|(_, entry)| entry.deadline <= now)
                .map(|(correlation_id, _)| correlation_id.clone())
                .collect::<Vec<_>>();
            expired_ids
                .into_iter()
                .filter_map(|correlation_id| {
                    entries
                        .pending_replies
                        .remove(&correlation_id)
                        .map(|entry| (entry.sender, entry.send_succeeded))
                })
                .collect::<Vec<_>>()
        };
        let count = expired.len();
        for (sender, send_succeeded) in expired {
            let _ = sender.send(PendingReplyCompletion::Timeout { send_succeeded });
        }
        count
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
            drop(entries);
            drop(handle);
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
                handle,
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
            drop(entries);
            drop(handle);
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
                handle,
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
        let (removed_producer, removed_consumer, removed_admin) = match kind {
            GroupKind::Producer => (entries.producers.remove(&lease.group), None, None),
            GroupKind::Consumer => (None, entries.consumers.remove(&lease.group), None),
            GroupKind::Admin => (None, None, entries.admins.remove(&lease.group)),
        };
        let cancelled = if kind == GroupKind::Producer {
            take_pending_replies_for_owner(&mut entries, &lease.identity)
        } else {
            Vec::new()
        };
        drop(entries);
        notify_pending_reply_shutdown(cancelled);
        drop(removed_producer);
        drop(removed_consumer);
        drop(removed_admin);
        Ok(true)
    }

    pub(super) fn remove_by_group(
        &self,
        group: &str,
        kind: GroupKind,
    ) -> Result<bool, RuntimeError> {
        let (removed, removed_producer, removed_consumer, removed_admin, cancelled) = {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            match kind {
                GroupKind::Producer => {
                    let entry = entries.producers.remove(group);
                    let cancelled = entry
                        .as_ref()
                        .map(|entry| take_pending_replies_for_owner(&mut entries, &entry.identity))
                        .unwrap_or_default();
                    (entry.is_some(), entry, None, None, cancelled)
                }
                GroupKind::Consumer => {
                    let entry = entries.consumers.remove(group);
                    (entry.is_some(), None, entry, None, Vec::new())
                }
                GroupKind::Admin => {
                    let entry = entries.admins.remove(group);
                    (entry.is_some(), None, None, entry, Vec::new())
                }
            }
        };
        notify_pending_reply_shutdown(cancelled);
        drop(removed_producer);
        drop(removed_consumer);
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
                .map(|(group, entry)| (group.clone(), entry.handle.clone()))
                .collect(),
            consumers: entries
                .consumers
                .values()
                .map(|entry| entry.handle.clone())
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
            .map(|(group, entry)| (group.clone(), entry.handle.clone()))
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
            .map(|entry| entry.handle.clone()))
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
            .map(|entry| entry.handle.clone()))
    }
}

fn take_pending_replies_for_owner(
    entries: &mut Entries,
    owner: &Arc<()>,
) -> Vec<oneshot::Sender<PendingReplyCompletion>> {
    let owned_ids = entries
        .pending_replies
        .iter()
        .filter(|(_, entry)| Arc::ptr_eq(&entry.owner, owner))
        .map(|(correlation_id, _)| correlation_id.clone())
        .collect::<Vec<_>>();
    owned_ids
        .into_iter()
        .filter_map(|correlation_id| {
            entries
                .pending_replies
                .remove(&correlation_id)
                .map(|entry| entry.sender)
        })
        .collect()
}

fn notify_pending_reply_shutdown(senders: Vec<oneshot::Sender<PendingReplyCompletion>>) {
    for sender in senders {
        let _ = sender.send(PendingReplyCompletion::ProducerShutdown);
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
        TableMutation::Clear => unreachable!("clear is detached before map mutation"),
    };
    let present = map.contains_key(key);
    let previous = map.get(key).map(|entry| entry.handle.clone());
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
                        handle: value.clone(),
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
mod pending_reply_admission_tests {
    use super::*;
    use std::{sync::Arc, time::Duration};

    struct Producer;
    impl ProducerRuntime for Producer {}

    #[tokio::test]
    async fn pending_reply_slots_are_bounded_until_terminal_removal() {
        let java_fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/producer/request-reply.json"
        ))
        .unwrap();
        assert_eq!(
            DEFAULT_PENDING_REPLY_CAPACITY,
            java_fixture["asyncSenderQueueCapacity"].as_u64().unwrap() as usize
        );

        let registry = Arc::new(GroupRegistry::with_pending_reply_capacity(1));
        let producer: Arc<dyn ProducerRuntime> = Arc::new(Producer);
        let lease = registry
            .register_producer("request-owner", producer)
            .unwrap();
        let first_deadline = Instant::now() + Duration::from_secs(2);
        let first = registry
            .register_pending_reply(&lease, "first", first_deadline)
            .await
            .unwrap();

        let second_deadline = Instant::now() + Duration::from_millis(25);
        assert!(matches!(
            registry
                .register_pending_reply(&lease, "second", second_deadline)
                .await,
            Err(RuntimeError::RequestReplyAdmissionTimeout)
        ));

        drop(first);
        let reply_deadline = Instant::now() + Duration::from_secs(2);
        let mut reply_pending = registry
            .register_pending_reply(&lease, "reply", reply_deadline)
            .await
            .unwrap();
        assert!(registry.complete_pending_reply(
            "reply",
            MessageExt::new(rocketmq_classic_common::message::Message::new(
                "reply-topic",
                b"reply".to_vec()
            ))
        ));
        assert!(matches!(
            reply_pending.receive().await.unwrap(),
            PendingReplyCompletion::Reply(_)
        ));

        let failure_deadline = Instant::now() + Duration::from_secs(2);
        let mut failure_pending = registry
            .register_pending_reply(&lease, "failure", failure_deadline)
            .await
            .unwrap();
        assert!(
            failure_pending
                .control()
                .fail_send("scripted failure".into())
        );
        assert!(matches!(
            failure_pending.receive().await.unwrap(),
            PendingReplyCompletion::SendFailed(cause) if cause == "scripted failure"
        ));

        let timeout_deadline = Instant::now() + Duration::from_secs(2);
        let mut timeout_pending = registry
            .register_pending_reply(&lease, "timeout", timeout_deadline)
            .await
            .unwrap();
        assert_eq!(registry.expire_pending_replies(timeout_deadline), 1);
        assert!(matches!(
            timeout_pending.receive().await.unwrap(),
            PendingReplyCompletion::Timeout { .. }
        ));

        let shutdown_deadline = Instant::now() + Duration::from_secs(2);
        let mut shutdown_pending = registry
            .register_pending_reply(&lease, "shutdown", shutdown_deadline)
            .await
            .unwrap();
        assert!(registry.unregister(&lease, GroupKind::Producer).unwrap());
        assert!(matches!(
            shutdown_pending.receive().await.unwrap(),
            PendingReplyCompletion::ProducerShutdown
        ));

        let replacement: Arc<dyn ProducerRuntime> = Arc::new(Producer);
        let replacement_lease = registry
            .register_producer("request-owner", replacement)
            .unwrap();
        let available_deadline = Instant::now() + Duration::from_secs(2);
        let available = registry
            .register_pending_reply(&replacement_lease, "available", available_deadline)
            .await
            .unwrap();
        drop(available);
        assert!(
            registry
                .unregister(&replacement_lease, GroupKind::Producer)
                .unwrap()
        );
    }
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

#[cfg(test)]
mod runtime_owner_drop_tests {
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

    impl ProducerRuntime for ReenterOnDrop {}

    impl ConsumerRuntime for ReenterOnDrop {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData::default()
        }
    }

    struct ReplacementOwner;

    impl ProducerRuntime for ReplacementOwner {}

    impl ConsumerRuntime for ReplacementOwner {
        fn heartbeat_data(&self) -> ConsumerData {
            ConsumerData::default()
        }
    }

    impl Drop for ReenterOnDrop {
        fn drop(&mut self) {
            let Some(registry) = self.registry.upgrade() else {
                return;
            };
            if registry.register_admin("from-drop").is_ok() {
                self.reentered.store(true, Ordering::SeqCst);
            }
        }
    }

    fn assert_runtime_owner_drop_can_reenter(
        kind: GroupKind,
        remove: impl FnOnce(&GroupRegistry, &GroupLease) + Send + 'static,
    ) {
        let registry = Arc::new(GroupRegistry::default());
        let reentered = Arc::new(AtomicBool::new(false));
        let owner = Arc::new(ReenterOnDrop {
            registry: Arc::downgrade(&registry),
            reentered: reentered.clone(),
        });
        let lease = match kind {
            GroupKind::Producer => {
                let owner: Arc<dyn ProducerRuntime> = owner.clone();
                registry.register_producer("owner", owner).unwrap()
            }
            GroupKind::Consumer => {
                let owner: Arc<dyn ConsumerRuntime> = owner.clone();
                registry.register_consumer("owner", owner).unwrap()
            }
            GroupKind::Admin => unreachable!(),
        };
        drop(owner);

        let (reply, receive) = mpsc::channel();
        let worker_registry = registry.clone();
        let worker = thread::spawn(move || {
            remove(&worker_registry, &lease);
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
            "producer/consumer owner destructor must be able to re-enter the registry"
        );
        worker.join().unwrap();
        assert!(reentered.load(Ordering::SeqCst));
    }

    #[test]
    fn producer_lease_and_group_removals_drop_owner_after_unlock() {
        assert_runtime_owner_drop_can_reenter(GroupKind::Producer, |registry, lease| {
            assert!(registry.unregister(lease, GroupKind::Producer).unwrap());
        });
        assert_runtime_owner_drop_can_reenter(GroupKind::Producer, |registry, _| {
            assert!(
                registry
                    .remove_by_group("owner", GroupKind::Producer)
                    .unwrap()
            );
        });
    }

    #[test]
    fn consumer_lease_and_group_removals_drop_owner_after_unlock() {
        assert_runtime_owner_drop_can_reenter(GroupKind::Consumer, |registry, lease| {
            assert!(registry.unregister(lease, GroupKind::Consumer).unwrap());
        });
        assert_runtime_owner_drop_can_reenter(GroupKind::Consumer, |registry, _| {
            assert!(
                registry
                    .remove_by_group("owner", GroupKind::Consumer)
                    .unwrap()
            );
        });
    }

    #[test]
    fn raw_table_remove_replace_clear_and_registry_clear_drop_owner_after_unlock() {
        use super::super::tables::{TableKind, TableMutation, TableValue};

        for kind in [GroupKind::Producer, GroupKind::Consumer] {
            let table_kind = match kind {
                GroupKind::Producer => TableKind::Producer,
                GroupKind::Consumer => TableKind::Consumer,
                GroupKind::Admin => unreachable!(),
            };
            assert_runtime_owner_drop_can_reenter(kind, move |registry, _| {
                let change = registry
                    .mutate_table(
                        table_kind,
                        &TableMutation::Remove {
                            key: "owner".into(),
                            expected: None,
                            comparison: None,
                        },
                    )
                    .unwrap();
                drop(change);
            });

            let table_kind = match kind {
                GroupKind::Producer => TableKind::Producer,
                GroupKind::Consumer => TableKind::Consumer,
                GroupKind::Admin => unreachable!(),
            };
            assert_runtime_owner_drop_can_reenter(kind, move |registry, _| {
                let replacement = match kind {
                    GroupKind::Producer => TableValue::Producer(Arc::new(ReplacementOwner)),
                    GroupKind::Consumer => TableValue::Consumer(Arc::new(ReplacementOwner)),
                    GroupKind::Admin => unreachable!(),
                };
                let change = registry
                    .mutate_table(
                        table_kind,
                        &TableMutation::Replace {
                            key: "owner".into(),
                            expected: None,
                            comparison: None,
                            value: replacement,
                        },
                    )
                    .unwrap();
                drop(change);
            });

            let table_kind = match kind {
                GroupKind::Producer => TableKind::Producer,
                GroupKind::Consumer => TableKind::Consumer,
                GroupKind::Admin => unreachable!(),
            };
            assert_runtime_owner_drop_can_reenter(kind, move |registry, _| {
                registry
                    .mutate_table(table_kind, &TableMutation::Clear)
                    .unwrap();
            });
            assert_runtime_owner_drop_can_reenter(kind, |registry, _| {
                registry.clear().unwrap();
            });
        }
    }
}
