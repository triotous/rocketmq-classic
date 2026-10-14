use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicI32, Ordering},
    },
    thread::ThreadId,
};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::{
    route::{TopicRouteDataHandle, TopicSnapshot},
    runtime::RuntimeError,
};
use rocketmq_classic_protocol::route::TopicRouteData;

use crate::ProducerError;

/// Shared mutable queue-list handle corresponding to
/// `TopicPublishInfo.getMessageQueueList()`.
#[derive(Clone, Default)]
pub struct ProducerPublishQueueList(Arc<RwLock<Vec<MessageQueue>>>);

impl ProducerPublishQueueList {
    pub fn new(queues: Vec<MessageQueue>) -> Self {
        Self(Arc::new(RwLock::new(queues)))
    }

    pub fn snapshot(&self) -> Vec<MessageQueue> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn len(&self) -> usize {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Mutates the Java list object retained by every alias to this handle.
    pub fn with_mut<R>(&self, update: impl FnOnce(&mut Vec<MessageQueue>) -> R) -> R {
        update(
            &mut self
                .0
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// Shared mutable counterpart to Java's `ThreadLocalIndex`.
#[derive(Clone, Default)]
pub struct ProducerQueueIndex(Arc<Mutex<HashMap<ThreadId, AtomicI32>>>);

impl ProducerQueueIndex {
    pub fn increment_and_get(&self) -> i32 {
        let thread = std::thread::current().id();
        let mut indexes = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = indexes
            .entry(thread)
            .or_insert_with(|| AtomicI32::new(random_i32()));
        index.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
    }

    pub fn reset(&self) {
        let thread = std::thread::current().id();
        let mut indexes = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        indexes.insert(thread, AtomicI32::new(random_i32() & i32::MAX));
    }

    pub fn current(&self) -> Option<i32> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&std::thread::current().id())
            .map(|index| index.load(Ordering::Relaxed))
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

fn random_i32() -> i32 {
    use std::hash::{BuildHasher, Hasher};
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish() as i32
}

#[derive(Clone)]
struct PublishInfoData {
    order_topic: bool,
    have_topic_router_info: bool,
    message_queue_list: Option<ProducerPublishQueueList>,
    send_which_queue: ProducerQueueIndex,
    topic_route_data: Option<TopicRouteDataHandle>,
    template: TopicSnapshot,
}

/// Mutable producer route value corresponding to Java's `TopicPublishInfo`.
/// Cloned handles share the same mutable value, queue list, route data, and
/// per-thread queue cursor.
#[derive(Clone)]
pub struct TopicPublishInfo(Arc<RwLock<PublishInfoData>>);

impl Default for TopicPublishInfo {
    fn default() -> Self {
        Self::from_snapshot(
            TopicSnapshot {
                route: Arc::new(TopicRouteData::default()),
                publish_queues: Vec::new(),
                subscribe_queues: Default::default(),
                endpoints: Default::default(),
                order_topic: false,
            },
            false,
        )
    }
}

impl TopicPublishInfo {
    pub fn from_snapshot(snapshot: TopicSnapshot, have_topic_router_info: bool) -> Self {
        let topic_route_data = Some(TopicRouteDataHandle::new((*snapshot.route).clone()));
        let message_queue_list = Some(ProducerPublishQueueList::new(
            snapshot.publish_queues.clone(),
        ));
        Self(Arc::new(RwLock::new(PublishInfoData {
            order_topic: snapshot.order_topic,
            have_topic_router_info,
            message_queue_list,
            send_which_queue: ProducerQueueIndex::default(),
            topic_route_data,
            template: snapshot,
        })))
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn is_order_topic(&self) -> bool {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .order_topic
    }

    pub fn set_order_topic(&self, value: bool) {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .order_topic = value;
    }

    pub fn ok(&self) -> bool {
        self.message_queue_list()
            .is_some_and(|queues| !queues.is_empty())
    }

    pub fn message_queue_list(&self) -> Option<ProducerPublishQueueList> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .message_queue_list
            .clone()
    }

    pub fn set_message_queue_list(&self, queues: Option<ProducerPublishQueueList>) {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .message_queue_list = queues;
    }

    pub fn set_message_queues(&self, queues: Option<Vec<MessageQueue>>) {
        self.set_message_queue_list(queues.map(ProducerPublishQueueList::new));
    }

    pub fn send_which_queue(&self) -> ProducerQueueIndex {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .send_which_queue
            .clone()
    }

    pub fn set_send_which_queue(&self, index: ProducerQueueIndex) {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .send_which_queue = index;
    }

    pub fn have_topic_router_info(&self) -> bool {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .have_topic_router_info
    }

    pub fn set_have_topic_router_info(&self, value: bool) {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .have_topic_router_info = value;
    }

    pub fn topic_route_data(&self) -> Option<TopicRouteDataHandle> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .topic_route_data
            .clone()
    }

    pub fn set_topic_route_data(&self, route: Option<TopicRouteDataHandle>) {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .topic_route_data = route;
    }

    pub fn reset_index(&self) {
        self.send_which_queue().reset();
    }

    pub(crate) fn next_index(&self, queue_count: usize) -> usize {
        if queue_count == 0 {
            return 0;
        }
        (self.send_which_queue().increment_and_get() & i32::MAX) as usize % queue_count
    }

    /// Advances the retained ThreadLocalIndex and selects from the current
    /// live queue list using Java's nonnegative mask and increment-first order.
    pub fn select_one_message_queue(&self) -> Result<MessageQueue, ProducerError> {
        let data = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let queues = data
            .message_queue_list
            .as_ref()
            .ok_or(ProducerError::NullPublishInfoQueueList)?;
        let queue_count = queues.len();
        if queue_count == 0 {
            return Err(ProducerError::EmptyPublishInfoQueueList);
        }
        let position =
            (data.send_which_queue.increment_and_get() & i32::MAX) as usize % queue_count;
        Ok(queues.snapshot()[position].clone())
    }

    /// Applies all filters for each candidate, matching Java's non-short-
    /// circuit boolean accumulation and returning `None` when no queue passes.
    pub fn select_one_message_queue_with_filter<F>(
        &self,
        filters: &[F],
    ) -> Result<Option<MessageQueue>, ProducerError>
    where
        F: Fn(&MessageQueue) -> bool,
    {
        let data = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(queues) = &data.message_queue_list else {
            return Ok(None);
        };
        let queues = queues.snapshot();
        if queues.is_empty() {
            return Ok(None);
        }
        for _ in 0..queues.len() {
            let index =
                (data.send_which_queue.increment_and_get() & i32::MAX) as usize % queues.len();
            let queue = &queues[index];
            let mut accepted = true;
            for filter in filters {
                accepted &= filter(queue);
            }
            if accepted {
                return Ok(Some(queue.clone()));
            }
        }
        Ok(None)
    }

    pub fn select_one_message_queue_avoiding(
        &self,
        last_broker: Option<&str>,
    ) -> Result<MessageQueue, ProducerError> {
        let Some(last_broker) = last_broker else {
            return self.select_one_message_queue();
        };
        let attempts = self.message_queue_list().map_or(0, |queues| queues.len());
        for _ in 0..attempts {
            let queue = self.select_one_message_queue()?;
            if queue.broker_name() != Some(last_broker) {
                return Ok(queue);
            }
        }
        self.select_one_message_queue()
    }

    pub fn get_write_queue_nums_by_broker(&self, broker_name: &str) -> Result<i32, ProducerError> {
        let route = self
            .topic_route_data()
            .ok_or(ProducerError::NullPublishInfoRouteData)?
            .snapshot()?;
        let Some(queue_data) = route.queue_datas_snapshot() else {
            return Err(ProducerError::NullPublishInfoQueueData);
        };
        for queue in queue_data {
            let Some(queue) = queue else {
                return Err(ProducerError::NullPublishInfoQueueData);
            };
            let Some(name) = queue.broker_name.as_deref() else {
                return Err(ProducerError::NullPublishInfoBrokerName);
            };
            if name == broker_name {
                return Ok(queue.write_queue_nums);
            }
        }
        Ok(-1)
    }

    pub fn snapshot(&self) -> Result<TopicSnapshot, RuntimeError> {
        let data = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let route = data
            .topic_route_data
            .as_ref()
            .map(TopicRouteDataHandle::snapshot)
            .transpose()?
            .unwrap_or_else(|| (*data.template.route).clone());
        Ok(TopicSnapshot {
            route: Arc::new(route),
            publish_queues: data
                .message_queue_list
                .map_or_else(Vec::new, |queues| queues.snapshot()),
            subscribe_queues: data.template.subscribe_queues,
            endpoints: data.template.endpoints,
            order_topic: data.order_topic,
        })
    }
}

impl std::fmt::Display for TopicPublishInfo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let queues = data.message_queue_list.map_or_else(
            || "null".to_owned(),
            |queues| {
                let queues = queues.snapshot();
                format!(
                    "[{}]",
                    queues
                        .iter()
                        .map(java_queue_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            },
        );
        write!(
            formatter,
            "TopicPublishInfo [orderTopic={}, messageQueueList={}, sendWhichQueue=ThreadLocalIndex{{threadLocalIndex={}}}, haveTopicRouterInfo={}]",
            data.order_topic,
            queues,
            data.send_which_queue
                .current()
                .map_or_else(|| "null".to_owned(), |value| value.to_string()),
            data.have_topic_router_info
        )
    }
}

fn java_queue_string(queue: &MessageQueue) -> String {
    format!(
        "MessageQueue [topic={}, brokerName={}, queueId={}]",
        queue.topic().unwrap_or("null"),
        queue.broker_name().unwrap_or("null"),
        queue.queue_id()
    )
}

/// Shared live topic-to-publish-info table corresponding to
/// `DefaultMQProducerImpl.getTopicPublishInfoTable()`.
#[derive(Clone, Default)]
pub struct TopicPublishInfoTable(Arc<RwLock<HashMap<String, TopicPublishInfo>>>);

impl TopicPublishInfoTable {
    pub fn get(&self, topic: &str) -> Option<TopicPublishInfo> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(topic)
            .cloned()
    }

    pub fn contains_key(&self, topic: &str) -> bool {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(topic)
    }

    pub fn len(&self) -> usize {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn put(
        &self,
        topic: impl Into<String>,
        info: TopicPublishInfo,
    ) -> Option<TopicPublishInfo> {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(topic.into(), info)
    }

    pub fn put_if_absent(
        &self,
        topic: impl Into<String>,
        info: TopicPublishInfo,
    ) -> Option<TopicPublishInfo> {
        use std::collections::hash_map::Entry;
        match self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(topic.into())
        {
            Entry::Occupied(entry) => Some(entry.get().clone()),
            Entry::Vacant(entry) => {
                entry.insert(info);
                None
            }
        }
    }

    pub fn remove(&self, topic: &str) -> Option<TopicPublishInfo> {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(topic)
    }

    pub fn clear(&self) {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    pub fn snapshot(&self) -> BTreeMap<String, TopicPublishInfo> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(topic, info)| (topic.clone(), info.clone()))
            .collect()
    }

    pub fn keys(&self) -> BTreeSet<String> {
        self.snapshot().into_keys().collect()
    }
}
