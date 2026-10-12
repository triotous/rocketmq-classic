mod config;
mod trace;

pub use config::*;
pub use trace::{
    LiteTraceBean, LiteTraceContext, LiteTraceDispatcher, LiteTraceError, LiteTraceKind,
};

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_protocol::{
    SharedValue,
    heartbeat::{ConsumeFromWhere, ConsumerSubscriptionDataSet},
};
use tokio::{
    task::JoinHandle,
    time::{Instant, sleep, timeout_at},
};

use crate::{
    config::PullConsumerConfig,
    error::{LitePullConsumerError, PullConsumerError},
    pull::{MessageSelector, ProcessedPullResult, PullConsumer, build_subscription},
    push::{MessageQueueListener, parse_java_timestamp_millis},
};

pub type TopicMessageQueueChangeListener = Arc<dyn Fn(&str, &BTreeSet<MessageQueue>) + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubscriptionMode {
    None,
    Subscribe,
    Assign,
}

struct QueueState {
    generation: u64,
    fetched_offset: Option<i64>,
    delivered_offset: Option<i64>,
    paused: bool,
}

struct BufferedBatch {
    queue: MessageQueue,
    generation: u64,
    messages: Vec<rocketmq_classic_common::message::MessageExt>,
    body_bytes: usize,
}

#[derive(Default)]
struct MessageBuffer {
    batches: VecDeque<BufferedBatch>,
    messages: usize,
    body_bytes: usize,
}

/// Lite Pull consumer with Java-compatible subscribe/assign exclusivity,
/// queue-scoped prefetch, and separate fetched/delivered/committed positions.
pub struct LitePullConsumer {
    config: LitePullConsumerConfig,
    engine: RwLock<Option<Arc<PullConsumer>>>,
    lifecycle: tokio::sync::Mutex<Lifecycle>,
    lifecycle_gate: tokio::sync::Mutex<()>,
    start_invoked: AtomicBool,
    running: AtomicBool,
    auto_commit: AtomicBool,
    connect_broker_by_user: AtomicBool,
    default_broker_id: AtomicI64,
    mode: RwLock<SubscriptionMode>,
    subscriptions: RwLock<BTreeMap<String, MessageSelector>>,
    assigned: RwLock<BTreeSet<MessageQueue>>,
    assign_expressions: RwLock<HashMap<String, String>>,
    queue_states: Mutex<HashMap<MessageQueue, QueueState>>,
    workers: Mutex<HashMap<MessageQueue, JoinHandle<()>>>,
    pull_permits: Arc<tokio::sync::Semaphore>,
    monitor: Mutex<Option<JoinHandle<()>>>,
    buffer: Mutex<MessageBuffer>,
    ready: tokio::sync::Notify,
    reconcile: tokio::sync::Notify,
    reconcile_gate: tokio::sync::Mutex<()>,
    next_generation: AtomicU64,
    next_auto_commit: Mutex<Option<Instant>>,
    heartbeat_subscriptions: Arc<RwLock<ConsumerSubscriptionDataSet>>,
    queue_change_listeners: RwLock<HashMap<String, TopicMessageQueueChangeListener>>,
    last_topic_queues: Mutex<HashMap<String, BTreeSet<MessageQueue>>>,
    pending_topic_listener_seed: Mutex<HashSet<String>>,
    message_queue_listener: RwLock<Option<MessageQueueListener>>,
    name_server_address: RwLock<Option<String>>,
    trace_dispatcher: RwLock<Option<Arc<LiteTraceDispatcher>>>,
}

enum Lifecycle {
    Created,
    Starting,
    Started,
    Shutdown,
}

impl LitePullConsumer {
    pub fn new(config: LitePullConsumerConfig) -> Result<Self, LitePullConsumerError> {
        let auto_commit = config.auto_commit();
        let connect_broker_by_user = config.connect_broker_by_user();
        let default_broker_id = config.default_broker_id();
        let message_queue_listener = config.message_queue_listener();
        let pull_permits = Arc::new(tokio::sync::Semaphore::new(
            usize::try_from(config.pull_thread_nums().max(1)).unwrap_or(1),
        ));
        Ok(Self {
            config,
            engine: RwLock::new(None),
            lifecycle: tokio::sync::Mutex::new(Lifecycle::Created),
            lifecycle_gate: tokio::sync::Mutex::new(()),
            start_invoked: AtomicBool::new(false),
            running: AtomicBool::new(false),
            auto_commit: AtomicBool::new(auto_commit),
            connect_broker_by_user: AtomicBool::new(connect_broker_by_user),
            default_broker_id: AtomicI64::new(default_broker_id),
            mode: RwLock::new(SubscriptionMode::None),
            subscriptions: RwLock::new(BTreeMap::new()),
            assigned: RwLock::new(BTreeSet::new()),
            assign_expressions: RwLock::new(HashMap::new()),
            queue_states: Mutex::new(HashMap::new()),
            workers: Mutex::new(HashMap::new()),
            pull_permits,
            monitor: Mutex::new(None),
            buffer: Mutex::new(MessageBuffer::default()),
            ready: tokio::sync::Notify::new(),
            reconcile: tokio::sync::Notify::new(),
            reconcile_gate: tokio::sync::Mutex::new(()),
            next_generation: AtomicU64::new(1),
            next_auto_commit: Mutex::new(None),
            heartbeat_subscriptions: Arc::new(RwLock::new(ConsumerSubscriptionDataSet::new())),
            queue_change_listeners: RwLock::new(HashMap::new()),
            last_topic_queues: Mutex::new(HashMap::new()),
            pending_topic_listener_seed: Mutex::new(HashSet::new()),
            message_queue_listener: RwLock::new(message_queue_listener),
            name_server_address: RwLock::new(None),
            trace_dispatcher: RwLock::new(None),
        })
    }

    pub fn config(&self) -> &LitePullConsumerConfig {
        &self.config
    }

    pub fn is_auto_commit(&self) -> bool {
        self.auto_commit.load(Ordering::Acquire)
    }

    pub async fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Returns the consume trace dispatcher after tracing has been enabled at start.
    pub fn trace_dispatcher(&self) -> Option<Arc<LiteTraceDispatcher>> {
        self.trace_dispatcher
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn normalized_topic(&self, topic: &str) -> Result<String, LitePullConsumerError> {
        self.config
            .client_config()
            .clone()
            .with_namespace(Some(topic))?
            .ok_or(LitePullConsumerError::Pull(
                PullConsumerError::InvalidSubscription("topic is empty".into()),
            ))
    }

    fn normalized_queue(
        &self,
        queue: &MessageQueue,
    ) -> Result<MessageQueue, LitePullConsumerError> {
        let mut client_config = self.config.client_config().clone();
        Ok(client_config.queue_with_namespace(queue)?)
    }

    fn select_mode(&self, mode: SubscriptionMode) -> Result<(), LitePullConsumerError> {
        let mut current = self
            .mode
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *current == SubscriptionMode::None {
            *current = mode;
        } else if *current != mode {
            return Err(LitePullConsumerError::SubscriptionModeConflict);
        }
        Ok(())
    }

    pub fn subscribe(&self, topic: &str) -> Result<(), LitePullConsumerError> {
        self.subscribe_with_selector(topic, &MessageSelector::by_tag("*"))
    }

    pub fn subscribe_with_expression(
        &self,
        topic: &str,
        expression: &str,
    ) -> Result<(), LitePullConsumerError> {
        self.subscribe_with_selector(topic, &MessageSelector::by_tag(expression))
    }

    pub fn subscribe_with_selector(
        &self,
        topic: &str,
        selector: &MessageSelector,
    ) -> Result<(), LitePullConsumerError> {
        if topic.is_empty() {
            return Err(PullConsumerError::InvalidSubscription(
                "Topic can not be null or empty.".into(),
            )
            .into());
        }
        self.select_mode(SubscriptionMode::Subscribe)?;
        let normalized_topic = self.normalized_topic(topic)?;
        build_subscription(
            &normalized_topic,
            Some(selector.expression()),
            Some(selector.expression_type()),
        )
        .map_err(|error| PullConsumerError::InvalidSubscription(error.to_string()))?;
        self.subscriptions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(normalized_topic.clone(), selector.clone());

        if let Some(engine) = self.engine() {
            engine.register_topic(&normalized_topic)?;
            engine.add_register_subscriptions(&normalized_topic, Some(selector))?;
        }
        self.reconcile.notify_one();
        Ok(())
    }

    pub fn subscribe_with_listener(
        &self,
        topic: &str,
        selector: &MessageSelector,
        listener: MessageQueueListener,
    ) -> Result<(), LitePullConsumerError> {
        self.subscribe_with_selector(topic, selector)?;
        *self
            .message_queue_listener
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(listener.clone());
        if let Some(engine) = self.engine() {
            engine.set_message_queue_listener(Some(listener));
        }
        Ok(())
    }

    pub async fn unsubscribe(&self, topic: &str) -> Result<bool, LitePullConsumerError> {
        let normalized_topic = self.normalized_topic(topic)?;
        let removed = self
            .subscriptions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&normalized_topic)
            .is_some();
        let removed_queues = {
            let mut states = self
                .queue_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let queues = states
                .keys()
                .filter(|queue| queue.topic() == Some(normalized_topic.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            for queue in &queues {
                states.remove(queue);
            }
            queues
        };
        let mut removed_workers = Vec::new();
        {
            let mut workers = self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for queue in &removed_queues {
                if let Some(worker) = workers.remove(queue) {
                    worker.abort();
                    removed_workers.push(worker);
                }
            }
        }
        for queue in &removed_queues {
            self.remove_queue_batches(queue);
        }
        for worker in removed_workers {
            let _ = worker.await;
        }
        self.last_topic_queues
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&normalized_topic);
        if let Some(engine) = self.engine() {
            let _ = engine.unregister_topic(&normalized_topic)?;
        }
        self.reconcile.notify_one();
        Ok(removed)
    }

    pub fn assign<I>(&self, message_queues: I) -> Result<(), LitePullConsumerError>
    where
        I: IntoIterator<Item = MessageQueue>,
    {
        let queues = message_queues
            .into_iter()
            .map(|queue| self.normalized_queue(&queue))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if queues.is_empty() {
            return Err(LitePullConsumerError::EmptyAssignment);
        }
        self.select_mode(SubscriptionMode::Assign)?;
        *self
            .assigned
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = queues;
        self.reconcile.notify_one();
        Ok(())
    }

    pub fn set_sub_expression_for_assign(
        &self,
        topic: &str,
        expression: &str,
    ) -> Result<(), LitePullConsumerError> {
        if expression.trim().is_empty() {
            return Err(LitePullConsumerError::EmptyAssignmentExpression);
        }
        if self.start_invoked.load(Ordering::Acquire) {
            return Err(LitePullConsumerError::AssignmentExpressionAfterStartup);
        }
        self.select_mode(SubscriptionMode::Assign)?;
        let topic = self.normalized_topic(topic)?;
        self.assign_expressions
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(topic, expression.to_owned());
        Ok(())
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        self.start_invoked.store(true, Ordering::Release);
        {
            let mut state = self.lifecycle.lock().await;
            match &*state {
                Lifecycle::Started | Lifecycle::Starting => {
                    return Err(LitePullConsumerError::AlreadyStarted);
                }
                Lifecycle::Shutdown => return Err(LitePullConsumerError::Shutdown),
                Lifecycle::Created => *state = Lifecycle::Starting,
            }
        }
        if self.config.pull_thread_nums() < 0 {
            *self.lifecycle.lock().await = Lifecycle::Shutdown;
            return Err(LitePullConsumerError::InvalidPullThreadCount);
        }
        if self.config.topic_metadata_check_interval_millis() <= 0 {
            *self.lifecycle.lock().await = Lifecycle::Shutdown;
            return Err(LitePullConsumerError::InvalidTopicMetadataCheckInterval);
        }
        if !self.config.validate_suspend_timeout() {
            *self.lifecycle.lock().await = Lifecycle::Shutdown;
            return Err(PullConsumerError::InvalidSuspendTimeout {
                consumer_timeout_millis: self.config.consumer_timeout_millis_when_suspend(),
                broker_suspend_millis: self.config.broker_suspend_max_time_millis(),
            }
            .into());
        }
        let trace_dispatcher = if self.config.enable_msg_trace() {
            match LiteTraceDispatcher::new(
                self.config.client_config(),
                self.config.consumer_group(),
                self.config.customized_trace_topic(),
            ) {
                Ok(dispatcher) => {
                    let dispatcher = Arc::new(dispatcher);
                    *self
                        .trace_dispatcher
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(dispatcher.clone());
                    Some(dispatcher)
                }
                Err(error) => {
                    tracing::error!(error = %error, "Lite consume trace dispatcher initialization failed");
                    None
                }
            }
        } else {
            None
        };
        let engine = match self.prepare_engine().await {
            Ok(engine) => engine,
            Err(error) => {
                *self.lifecycle.lock().await = Lifecycle::Shutdown;
                return Err(error);
            }
        };
        if let Err(error) = engine.start().await {
            *self.lifecycle.lock().await = Lifecycle::Shutdown;
            return Err(error.into());
        }
        {
            let mut lifecycle = self.lifecycle.lock().await;
            if !matches!(*lifecycle, Lifecycle::Starting) {
                drop(lifecycle);
                let _ = engine.shutdown().await;
                return Err(LitePullConsumerError::Shutdown);
            }
            *self
                .engine
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(engine.clone());
            self.running.store(true, Ordering::Release);
            *lifecycle = Lifecycle::Started;
        }
        let listener_topics = self
            .queue_change_listeners
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for topic in listener_topics {
            if let Ok(queues) = engine.fetch_message_queues(&topic).await {
                self.last_topic_queues
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(topic, queues);
            }
        }
        self.sync_workers().await;
        if let Some(dispatcher) = trace_dispatcher {
            let name_server_address = self.config.client_config().namesrv_addr.clone();
            let access_channel = self
                .config
                .client_config()
                .access_channel()
                .unwrap_or_default();
            if let Err(error) = dispatcher.start(name_server_address, access_channel).await {
                tracing::warn!(error = %error, "Lite consume trace dispatcher start failed");
            }
        }
        let consumer = self.clone();
        let task = tokio::spawn(async move {
            let metadata_period = Duration::from_millis(
                consumer.config.topic_metadata_check_interval_millis() as u64,
            );
            let mut next_metadata_refresh = Instant::now() + metadata_period;
            loop {
                if !consumer.running.load(Ordering::Acquire) {
                    break;
                }
                consumer.sync_workers().await;
                if Instant::now() >= next_metadata_refresh {
                    if let Some(engine) = consumer.engine() {
                        let topics = consumer
                            .subscriptions
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .keys()
                            .cloned()
                            .collect::<Vec<_>>();
                        for topic in topics {
                            let _ = engine.refresh_topic_route(&topic).await;
                        }
                    }
                    next_metadata_refresh = Instant::now() + metadata_period;
                }
                tokio::select! {
                    _ = consumer.reconcile.notified() => {}
                    _ = sleep(Duration::from_millis(200)) => {}
                }
            }
        });
        *self
            .monitor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task);
        Ok(())
    }

    async fn prepare_engine(&self) -> Result<Arc<PullConsumer>, LitePullConsumerError> {
        let mut pull_config = PullConsumerConfig::new(self.config.consumer_group())?;
        *pull_config.client_config_mut() = self.config.client_config().clone();
        if let Some(address) = self
            .name_server_address
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            pull_config
                .client_config_mut()
                .set_namesrv_addr(Some(address));
        }
        pull_config
            .set_broker_suspend_max_time_millis(self.config.broker_suspend_max_time_millis());
        pull_config.set_consumer_timeout_millis_when_suspend(
            self.config.consumer_timeout_millis_when_suspend(),
        );
        pull_config.set_consumer_pull_timeout_millis(self.config.consumer_pull_timeout_millis());
        pull_config.set_message_model(self.config.message_model());
        pull_config.set_consume_from_where(self.config.consume_from_where());
        pull_config.set_unit_mode(self.config.unit_mode());
        pull_config
            .set_allocate_message_queue_strategy(self.config.allocate_message_queue_strategy());
        pull_config.set_message_queue_listener(
            self.message_queue_listener
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        );
        let engine = Arc::new(PullConsumer::new(pull_config));
        engine.configure_pull_api(
            self.connect_broker_by_user.load(Ordering::Acquire),
            self.default_broker_id.load(Ordering::Acquire),
        );
        engine.set_additional_heartbeat_subscriptions(self.heartbeat_subscriptions.clone());
        if let Some(store) = self.config.offset_store() {
            engine.set_offset_store(Some(store)).await?;
        }
        if *self
            .mode
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == SubscriptionMode::Subscribe
        {
            for (topic, selector) in self
                .subscriptions
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
            {
                engine.register_topic(topic)?;
                engine.add_register_subscriptions(topic, Some(selector))?;
            }
        }
        Ok(engine)
    }

    pub async fn shutdown(&self) -> Result<(), LitePullConsumerError> {
        let _lifecycle_gate = self.lifecycle_gate.lock().await;
        self.running.store(false, Ordering::Release);
        self.ready.notify_waiters();
        self.reconcile.notify_waiters();
        {
            let mut state = self.lifecycle.lock().await;
            if matches!(&*state, Lifecycle::Shutdown) {
                return Ok(());
            }
            if matches!(&*state, Lifecycle::Created) {
                return Ok(());
            }
            *state = Lifecycle::Shutdown;
        }

        let monitor = self
            .monitor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(task) = monitor {
            task.abort();
            let _ = task.await;
        }
        let workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain()
            .map(|(_, worker)| worker)
            .collect::<Vec<_>>();
        for worker in &workers {
            worker.abort();
        }
        for worker in workers {
            let _ = worker.await;
        }
        if let Some(engine) = self.engine() {
            engine.shutdown().await?;
        }
        if let Some(dispatcher) = self.trace_dispatcher() {
            if let Err(error) = dispatcher.shutdown().await {
                tracing::warn!(error = %error, "Lite consume trace dispatcher shutdown failed");
            }
        }
        Ok(())
    }

    pub async fn poll(
        &self,
        timeout_millis: Option<i64>,
    ) -> Result<Vec<rocketmq_classic_common::message::MessageExt>, LitePullConsumerError> {
        if !self.running.load(Ordering::Acquire) {
            return Err(LitePullConsumerError::NotStarted);
        }
        let timeout_millis = timeout_millis.unwrap_or(self.config.poll_timeout_millis());
        if timeout_millis < 0 {
            return Err(LitePullConsumerError::NegativePollTimeout);
        }
        self.maybe_auto_commit().await?;
        let deadline = Instant::now() + Duration::from_millis(timeout_millis as u64);

        loop {
            let notified = self.ready.notified();
            if let Some(batch) = self.take_ready_batch() {
                if let Some(dispatcher) = self.trace_dispatcher() {
                    let contexts = trace::consume_contexts(
                        &batch,
                        self.config.consumer_group(),
                        self.config.client_config(),
                        trace::epoch_millis(),
                        trace::epoch_millis(),
                    );
                    for context in contexts {
                        let _ = dispatcher.append(context);
                    }
                }
                return Ok(batch);
            }
            if timeout_millis == 0 {
                return Ok(Vec::new());
            }
            if timeout_at(deadline, notified).await.is_err() {
                return Ok(Vec::new());
            }
        }
    }

    pub async fn assignment(&self) -> Result<BTreeSet<MessageQueue>, LitePullConsumerError> {
        let mode = *self
            .mode
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if mode == SubscriptionMode::Assign {
            return Ok(self
                .assigned
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone());
        }
        Ok(self
            .queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect())
    }

    pub async fn fetch_message_queues(
        &self,
        topic: &str,
    ) -> Result<BTreeSet<MessageQueue>, LitePullConsumerError> {
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        Ok(engine.fetch_message_queues(topic).await?)
    }

    pub async fn offset_for_timestamp(
        &self,
        queue: &MessageQueue,
        timestamp_millis: i64,
    ) -> Result<i64, LitePullConsumerError> {
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        Ok(engine.search_offset(queue, timestamp_millis).await?)
    }

    pub async fn committed(&self, queue: &MessageQueue) -> Result<i64, LitePullConsumerError> {
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        Ok(engine.fetch_consume_offset(queue, false).await?)
    }

    pub async fn seek(
        self: &Arc<Self>,
        queue: &MessageQueue,
        offset: i64,
    ) -> Result<(), LitePullConsumerError> {
        let queue = self.normalized_queue(queue)?;
        self.require_assigned(&queue)?;
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        let minimum = engine.get_min_offset(&queue).await?;
        let maximum = engine.get_max_offset(&queue).await?;
        if offset < minimum || offset > maximum {
            return Err(LitePullConsumerError::InvalidSeekOffset {
                offset,
                minimum,
                maximum,
            });
        }
        let _reconcile = self.reconcile_gate.lock().await;

        let generation = self
            .next_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| LitePullConsumerError::GenerationExhausted)?;
        {
            let mut states = self
                .queue_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(state) = states.get_mut(&queue) else {
                return Err(LitePullConsumerError::QueueNotAssigned);
            };
            state.generation = generation;
            state.fetched_offset = Some(offset);
            state.delivered_offset = Some(offset);
        }
        self.remove_queue_batches(&queue);
        self.abort_queue_worker(&queue).await;
        self.spawn_queue_worker(queue, generation);
        self.ready.notify_waiters();
        Ok(())
    }

    pub async fn seek_to_begin(
        self: &Arc<Self>,
        queue: &MessageQueue,
    ) -> Result<(), LitePullConsumerError> {
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        let offset = engine.get_min_offset(queue).await?;
        self.seek(queue, offset).await
    }

    pub async fn seek_to_end(
        self: &Arc<Self>,
        queue: &MessageQueue,
    ) -> Result<(), LitePullConsumerError> {
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        let offset = engine.get_max_offset(queue).await?;
        self.seek(queue, offset).await
    }

    pub fn pause<I>(&self, message_queues: I)
    where
        I: IntoIterator<Item = MessageQueue>,
    {
        let mut states = self
            .queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for queue in message_queues
            .into_iter()
            .filter_map(|queue| self.normalized_queue(&queue).ok())
        {
            if let Some(state) = states.get_mut(&queue) {
                state.paused = true;
            }
        }
    }

    pub fn resume<I>(&self, message_queues: I)
    where
        I: IntoIterator<Item = MessageQueue>,
    {
        let mut states = self
            .queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for queue in message_queues
            .into_iter()
            .filter_map(|queue| self.normalized_queue(&queue).ok())
        {
            if let Some(state) = states.get_mut(&queue) {
                state.paused = false;
            }
        }
        self.reconcile.notify_one();
    }

    pub async fn commit(&self) -> Result<(), LitePullConsumerError> {
        self.commit_all(false).await
    }

    pub async fn commit_sync(&self) -> Result<(), LitePullConsumerError> {
        self.commit().await
    }

    pub async fn commit_queues<I>(
        &self,
        message_queues: I,
        persist: bool,
    ) -> Result<(), LitePullConsumerError>
    where
        I: IntoIterator<Item = MessageQueue>,
    {
        let queues = message_queues
            .into_iter()
            .map(|queue| self.normalized_queue(&queue))
            .collect::<Result<Vec<_>, _>>()?;
        if queues.is_empty() {
            return Ok(());
        }
        let offsets = {
            let states = self
                .queue_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            queues
                .iter()
                .filter_map(|queue| {
                    states.get(queue).and_then(|state| {
                        state.delivered_offset.map(|offset| (queue.clone(), offset))
                    })
                })
                .collect::<Vec<_>>()
        };
        self.apply_commits(offsets, &queues, persist).await
    }

    pub async fn commit_offsets<I>(
        &self,
        offsets: I,
        persist: bool,
    ) -> Result<(), LitePullConsumerError>
    where
        I: IntoIterator<Item = (MessageQueue, i64)>,
    {
        let offsets = offsets
            .into_iter()
            .map(|(queue, offset)| Ok((self.normalized_queue(&queue)?, offset)))
            .collect::<Result<Vec<_>, LitePullConsumerError>>()?;
        if offsets.is_empty() {
            return Ok(());
        }
        let queues = offsets
            .iter()
            .map(|(queue, _)| queue.clone())
            .collect::<Vec<_>>();
        self.apply_commits(offsets, &queues, persist).await
    }

    async fn apply_commits(
        &self,
        offsets: Vec<(MessageQueue, i64)>,
        persist_queues: &[MessageQueue],
        persist: bool,
    ) -> Result<(), LitePullConsumerError> {
        let engine = self.engine().ok_or(LitePullConsumerError::NotStarted)?;
        for (queue, offset) in offsets {
            if offset == -1 || !self.is_assigned(&queue) {
                continue;
            }
            engine.update_consume_offset(&queue, offset).await?;
        }
        if persist {
            engine.persist_offsets(persist_queues).await?;
        }
        Ok(())
    }

    pub async fn commit_sync_offsets<I>(
        &self,
        offsets: I,
        persist: bool,
    ) -> Result<(), LitePullConsumerError>
    where
        I: IntoIterator<Item = (MessageQueue, i64)>,
    {
        self.commit_offsets(offsets, persist).await
    }

    pub fn set_auto_commit(&self, enabled: bool) {
        self.auto_commit.store(enabled, Ordering::Release);
    }

    pub fn is_connect_broker_by_user(&self) -> bool {
        self.connect_broker_by_user.load(Ordering::Acquire)
    }

    pub fn set_connect_broker_by_user(&self, enabled: bool) {
        self.connect_broker_by_user
            .store(enabled, Ordering::Release);
        self.configure_engine_pull_api();
    }

    pub fn default_broker_id(&self) -> i64 {
        self.default_broker_id.load(Ordering::Acquire)
    }

    pub fn set_default_broker_id(&self, broker_id: i64) {
        self.default_broker_id.store(broker_id, Ordering::Release);
        self.configure_engine_pull_api();
    }

    pub fn build_subscriptions_for_heartbeat(
        &self,
        selectors: &HashMap<String, Option<MessageSelector>>,
    ) -> Result<(), LitePullConsumerError> {
        {
            let mut subscriptions = self
                .heartbeat_subscriptions
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            subscriptions.clear();
            for (topic, selector) in selectors {
                let selector = selector.as_ref().ok_or_else(|| {
                    PullConsumerError::InvalidSubscription("message selector is null".into())
                })?;
                subscriptions.insert(Some(SharedValue::from(
                    build_subscription(
                        topic,
                        Some(selector.expression()),
                        Some(selector.expression_type()),
                    )
                    .map_err(|error| PullConsumerError::InvalidSubscription(error.to_string()))?,
                )));
            }
        }
        if let Some(engine) = self.engine() {
            engine.set_additional_heartbeat_subscriptions(self.heartbeat_subscriptions.clone());
        }
        Ok(())
    }

    pub fn subscriptions_for_heartbeat(&self) -> Arc<RwLock<ConsumerSubscriptionDataSet>> {
        self.heartbeat_subscriptions.clone()
    }

    pub fn register_topic_message_queue_change_listener(
        &self,
        topic: &str,
        listener: TopicMessageQueueChangeListener,
    ) -> Result<(), LitePullConsumerError> {
        let topic = self.normalized_topic(topic)?;
        if self.running.load(Ordering::Acquire) {
            self.pending_topic_listener_seed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(topic.clone());
        }
        self.queue_change_listeners
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(topic, listener);
        self.reconcile.notify_one();
        Ok(())
    }

    pub async fn update_name_server_address(
        &self,
        address: impl Into<String>,
    ) -> Result<(), LitePullConsumerError> {
        let address = address.into();
        *self
            .name_server_address
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(address.clone());
        if let Some(engine) = self.engine() {
            engine.update_name_server_address(&address).await?;
        }
        Ok(())
    }

    fn engine(&self) -> Option<Arc<PullConsumer>> {
        self.engine
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn configure_engine_pull_api(&self) {
        if let Some(engine) = self.engine() {
            engine.configure_pull_api(
                self.connect_broker_by_user.load(Ordering::Acquire),
                self.default_broker_id.load(Ordering::Acquire),
            );
        }
    }

    fn require_assigned(&self, queue: &MessageQueue) -> Result<(), LitePullConsumerError> {
        if self.is_assigned(queue) {
            Ok(())
        } else {
            Err(LitePullConsumerError::QueueNotAssigned)
        }
    }

    fn is_assigned(&self, queue: &MessageQueue) -> bool {
        self.queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(queue)
    }

    async fn sync_workers(self: &Arc<Self>) {
        if !self.running.load(Ordering::Acquire) {
            return;
        }
        let Ok(_gate) = self.reconcile_gate.try_lock() else {
            return;
        };
        let mode = *self
            .mode
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut desired = BTreeSet::new();
        if mode == SubscriptionMode::Assign {
            desired = self
                .assigned
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
        } else if mode == SubscriptionMode::Subscribe {
            if let Some(engine) = self.engine() {
                let topics = self
                    .subscriptions
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                for topic in topics {
                    if let Ok(queues) = engine.fetch_message_queues_in_balance(&topic).await {
                        desired.extend(queues);
                    }
                    self.update_topic_queue_change(&engine, &topic).await;
                }
            }
        }

        let current = self
            .queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let removed = current.difference(&desired).cloned().collect::<Vec<_>>();
        let added = desired.difference(&current).cloned().collect::<Vec<_>>();
        let mut removed_workers = Vec::new();
        {
            let mut states = self
                .queue_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for queue in &removed {
                states.remove(queue);
            }
            for queue in &added {
                let generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
                states.insert(
                    queue.clone(),
                    QueueState {
                        generation,
                        fetched_offset: None,
                        delivered_offset: None,
                        paused: false,
                    },
                );
            }
            let mut workers = self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for queue in &removed {
                if let Some(worker) = workers.remove(queue) {
                    worker.abort();
                    removed_workers.push(worker);
                }
            }
            for queue in &current {
                if workers.get(queue).is_some_and(JoinHandle::is_finished) {
                    if let Some(worker) = workers.remove(queue) {
                        removed_workers.push(worker);
                    }
                }
            }
        }
        for worker in removed_workers {
            let _ = worker.await;
        }
        for queue in removed {
            self.remove_queue_batches(&queue);
        }
        for queue in desired {
            let generation = self
                .queue_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&queue)
                .map(|state| state.generation);
            let has_worker = self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&queue);
            if let Some(generation) = generation.filter(|_| !has_worker) {
                self.spawn_queue_worker(queue, generation);
            }
        }
    }

    async fn update_topic_queue_change(&self, engine: &PullConsumer, topic: &str) {
        let listeners = self
            .queue_change_listeners
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(topic)
            .cloned();
        let Some(listener) = listeners else {
            return;
        };
        let Ok(queues) = engine.fetch_message_queues(topic).await else {
            return;
        };
        let changed = {
            let should_seed = self
                .pending_topic_listener_seed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(topic);
            let mut known = self
                .last_topic_queues
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if should_seed {
                known.insert(topic.to_owned(), queues.clone());
                false
            } else if known.get(topic) == Some(&queues) {
                false
            } else {
                known.insert(topic.to_owned(), queues.clone());
                true
            }
        };
        if changed {
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| listener(topic, &queues)));
        }
    }

    fn spawn_queue_worker(self: &Arc<Self>, queue: MessageQueue, generation: u64) {
        let consumer = self.clone();
        let worker_queue = queue.clone();
        let handle = tokio::spawn(async move {
            consumer.pull_loop(worker_queue, generation).await;
        });
        self.workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(queue, handle);
    }

    async fn abort_queue_worker(&self, queue: &MessageQueue) {
        let worker = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(queue);
        if let Some(worker) = worker {
            worker.abort();
            let _ = worker.await;
        }
    }

    async fn pull_loop(self: Arc<Self>, queue: MessageQueue, generation: u64) {
        loop {
            if !self.running.load(Ordering::Acquire) {
                return;
            }
            let (paused, mut offset) = {
                let states = self
                    .queue_states
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let Some(state) = states
                    .get(&queue)
                    .filter(|state| state.generation == generation)
                else {
                    return;
                };
                (state.paused, state.fetched_offset)
            };
            if paused || self.is_flow_controlled(&queue) {
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            let Some(engine) = self.engine() else {
                return;
            };
            if offset.is_none() {
                match self.initial_offset(&engine, &queue).await {
                    Ok(value) if value >= 0 => {
                        offset = Some(value);
                        if let Some(state) = self
                            .queue_states
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .get_mut(&queue)
                            .filter(|state| state.generation == generation)
                        {
                            state.fetched_offset = Some(value);
                            state.delivered_offset.get_or_insert(value);
                        } else {
                            return;
                        }
                    }
                    _ => {
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            }
            let selector = self.selector_for_queue(&queue);
            let Some(offset) = offset else {
                continue;
            };
            let Ok(_permit) = self.pull_permits.clone().acquire_owned().await else {
                return;
            };
            let result = engine
                .lite_pull_with_selector(
                    &queue,
                    &selector,
                    offset,
                    self.config.pull_batch_size(),
                    self.config.consumer_timeout_millis_when_suspend(),
                )
                .await;
            match result {
                Ok(result) => match self.accept_pull_result(&queue, generation, result) {
                    None => return,
                    Some(false) => sleep(Duration::from_millis(10)).await,
                    Some(true) => {}
                },
                Err(_) => sleep(Duration::from_secs(1)).await,
            }
        }
    }

    fn selector_for_queue(&self, queue: &MessageQueue) -> MessageSelector {
        let topic = queue.topic().unwrap_or_default();
        if *self
            .mode
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == SubscriptionMode::Assign
        {
            return self
                .assign_expressions
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(topic)
                .map(MessageSelector::by_tag)
                .unwrap_or_else(|| MessageSelector::by_tag("*"));
        }
        self.subscriptions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(topic)
            .cloned()
            .unwrap_or_else(|| MessageSelector::by_tag("*"))
    }

    async fn initial_offset(
        &self,
        engine: &PullConsumer,
        queue: &MessageQueue,
    ) -> Result<i64, LitePullConsumerError> {
        let stored = engine.fetch_consume_offset(queue, false).await?;
        if stored >= 0 {
            return Ok(stored);
        }
        match self.config.consume_from_where() {
            ConsumeFromWhere::ConsumeFromFirstOffset => Ok(0),
            ConsumeFromWhere::ConsumeFromMinOffset
            | ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst => {
                Ok(engine.get_min_offset(queue).await?)
            }
            ConsumeFromWhere::ConsumeFromTimestamp => {
                let timestamp = parse_java_timestamp_millis(self.config.consume_timestamp())
                    .ok_or(LitePullConsumerError::InvalidConsumeTimestamp)?;
                let offset = engine.search_offset(queue, timestamp).await?;
                if offset >= 0 {
                    Ok(offset)
                } else {
                    Ok(engine.get_max_offset(queue).await?)
                }
            }
            ConsumeFromWhere::ConsumeFromMaxOffset | ConsumeFromWhere::ConsumeFromLastOffset => {
                Ok(engine.get_max_offset(queue).await?)
            }
        }
    }

    fn accept_pull_result(
        &self,
        queue: &MessageQueue,
        generation: u64,
        mut result: ProcessedPullResult,
    ) -> Option<bool> {
        let next_offset = result.result.next_begin_offset;
        if next_offset < 0 {
            return Some(false);
        }
        let messages = result.messages.take().unwrap_or_default();
        let body_bytes = messages
            .iter()
            .map(|message| message.message().body().len())
            .sum::<usize>();
        let mut states = self
            .queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = states
            .get_mut(queue)
            .filter(|state| state.generation == generation)?;
        state.fetched_offset = Some(next_offset);
        drop(states);

        let had_messages = !messages.is_empty();
        if had_messages {
            let mut buffer = self
                .buffer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            buffer.messages = buffer.messages.saturating_add(messages.len());
            buffer.body_bytes = buffer.body_bytes.saturating_add(body_bytes);
            buffer.batches.push_back(BufferedBatch {
                queue: queue.clone(),
                generation,
                messages,
                body_bytes,
            });
            drop(buffer);
            self.ready.notify_one();
        }
        Some(had_messages)
    }

    fn is_flow_controlled(&self, queue: &MessageQueue) -> bool {
        let buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let queued_requests = buffer.batches.len() as i64;
        if queued_requests.saturating_mul(i64::from(self.config.pull_batch_size()))
            > self.config.pull_threshold_for_all()
        {
            return true;
        }
        let mut queue_count = 0usize;
        let mut queue_bytes = 0usize;
        let mut minimum_offset = None;
        let mut maximum_offset = None;
        for batch in &buffer.batches {
            if &batch.queue == queue {
                queue_count = queue_count.saturating_add(batch.messages.len());
                queue_bytes = queue_bytes.saturating_add(batch.body_bytes);
                for offset in batch.messages.iter().map(|message| message.queue_offset()) {
                    minimum_offset =
                        Some(minimum_offset.map_or(offset, |value: i64| value.min(offset)));
                    maximum_offset =
                        Some(maximum_offset.map_or(offset, |value: i64| value.max(offset)));
                }
            }
        }
        if i64::try_from(queue_count).unwrap_or(i64::MAX)
            > i64::from(self.config.pull_threshold_for_queue())
        {
            return true;
        }
        let queue_size_mib = queue_bytes / (1024 * 1024);
        if i64::try_from(queue_size_mib).unwrap_or(i64::MAX)
            > i64::from(self.config.pull_threshold_size_for_queue())
        {
            return true;
        }
        minimum_offset
            .zip(maximum_offset)
            .is_some_and(|(minimum, maximum)| {
                maximum.saturating_sub(minimum) > i64::from(self.config.consume_max_span())
            })
    }

    fn take_ready_batch(&self) -> Option<Vec<rocketmq_classic_common::message::MessageExt>> {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while let Some(batch) = buffer.batches.pop_front() {
            buffer.messages = buffer.messages.saturating_sub(batch.messages.len());
            buffer.body_bytes = buffer.body_bytes.saturating_sub(batch.body_bytes);
            let mut states = self
                .queue_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(state) = states
                .get_mut(&batch.queue)
                .filter(|state| state.generation == batch.generation)
            else {
                continue;
            };
            if let Some(last_offset) = batch
                .messages
                .iter()
                .map(|message| message.queue_offset())
                .max()
            {
                state.delivered_offset = Some(last_offset.saturating_add(1));
            }
            return Some(batch.messages);
        }
        None
    }

    fn remove_queue_batches(&self, queue: &MessageQueue) {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut retained = VecDeque::with_capacity(buffer.batches.len());
        buffer.messages = 0;
        buffer.body_bytes = 0;
        while let Some(batch) = buffer.batches.pop_front() {
            if &batch.queue != queue {
                buffer.messages = buffer.messages.saturating_add(batch.messages.len());
                buffer.body_bytes = buffer.body_bytes.saturating_add(batch.body_bytes);
                retained.push_back(batch);
            }
        }
        buffer.batches = retained;
    }

    async fn maybe_auto_commit(&self) -> Result<(), LitePullConsumerError> {
        if !self.auto_commit.load(Ordering::Acquire) {
            return Ok(());
        }
        let now = Instant::now();
        let due = {
            let mut deadline = self
                .next_auto_commit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if deadline.is_none_or(|deadline| now >= deadline) {
                *deadline = Some(
                    now + Duration::from_millis(
                        self.config.auto_commit_interval_millis().max(0) as u64
                    ),
                );
                true
            } else {
                false
            }
        };
        if due {
            self.commit_all(false).await?;
        }
        Ok(())
    }

    async fn commit_all(&self, persist: bool) -> Result<(), LitePullConsumerError> {
        let offsets = self
            .queue_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter_map(|(queue, state)| {
                state.delivered_offset.map(|offset| (queue.clone(), offset))
            })
            .collect::<Vec<_>>();
        self.commit_offsets(offsets, persist).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_common::message::{Message, MessageExt};
    use rocketmq_classic_core::api::consumer::{PullResultExt, PullStatus};

    fn consumer() -> LitePullConsumer {
        LitePullConsumer::new(LitePullConsumerConfig::new("lite-test").unwrap()).unwrap()
    }

    #[test]
    fn heartbeat_builder_mutates_the_retained_subscription_set_in_place() {
        let consumer = consumer();
        let retained = consumer.subscriptions_for_heartbeat();
        let mut selectors = HashMap::new();
        selectors.insert("orders".to_owned(), Some(MessageSelector::by_tag("paid")));
        selectors.insert(
            "invoices".to_owned(),
            Some(MessageSelector::by_sql("amount > 0")),
        );
        consumer
            .build_subscriptions_for_heartbeat(&selectors)
            .unwrap();
        assert_eq!(retained.read().unwrap().len(), 2);
        consumer
            .build_subscriptions_for_heartbeat(&HashMap::new())
            .unwrap();
        assert!(Arc::ptr_eq(
            &retained,
            &consumer.subscriptions_for_heartbeat()
        ));
        assert!(retained.read().unwrap().is_empty());
    }

    #[test]
    fn delivered_offset_advances_on_poll_without_moving_fetched_position() {
        let consumer = consumer();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        consumer.queue_states.lock().unwrap().insert(
            queue.clone(),
            QueueState {
                generation: 7,
                fetched_offset: Some(21),
                delivered_offset: Some(10),
                paused: false,
            },
        );
        let mut first = MessageExt::new(Message::new("orders", b"a".to_vec()));
        first.set_queue_offset(19);
        let mut second = MessageExt::new(Message::new("orders", b"b".to_vec()));
        second.set_queue_offset(20);
        let body_bytes = 2;
        consumer
            .buffer
            .lock()
            .unwrap()
            .batches
            .push_back(BufferedBatch {
                queue: queue.clone(),
                generation: 7,
                messages: vec![first, second],
                body_bytes,
            });
        consumer.buffer.lock().unwrap().messages = 2;
        consumer.buffer.lock().unwrap().body_bytes = body_bytes;

        let batch = consumer.take_ready_batch().unwrap();
        assert_eq!(batch.len(), 2);
        let states = consumer.queue_states.lock().unwrap();
        let state = states.get(&queue).unwrap();
        assert_eq!(state.fetched_offset, Some(21));
        assert_eq!(state.delivered_offset, Some(21));
    }

    #[test]
    fn flow_control_uses_java_request_count_message_and_whole_mib_boundaries() {
        let mut config = LitePullConsumerConfig::new("lite-flow-control").unwrap();
        config.set_pull_threshold_for_all(20);
        config.set_pull_threshold_for_queue(2);
        config.set_pull_threshold_size_for_queue(1);
        config.set_consume_max_span(5);
        let consumer = LitePullConsumer::new(config).unwrap();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let batch = |offsets: &[i64], body_bytes| BufferedBatch {
            queue: queue.clone(),
            generation: 1,
            messages: offsets
                .iter()
                .map(|offset| {
                    let mut message = MessageExt::new(Message::new("orders", Vec::new()));
                    message.set_queue_offset(*offset);
                    message
                })
                .collect(),
            body_bytes,
        };

        let mut buffer = consumer.buffer.lock().unwrap();
        buffer.batches.push_back(batch(&[2, 7], 1_999_999));
        drop(buffer);
        assert!(!consumer.is_flow_controlled(&queue));

        let mut buffer = consumer.buffer.lock().unwrap();
        buffer.batches.push_back(batch(&[8], 1));
        drop(buffer);
        assert!(consumer.is_flow_controlled(&queue));

        consumer.buffer.lock().unwrap().batches.clear();
        consumer
            .buffer
            .lock()
            .unwrap()
            .batches
            .push_back(batch(&[2], 2 * 1024 * 1024));
        assert!(consumer.is_flow_controlled(&queue));
    }

    #[test]
    fn a_late_pull_result_from_an_old_generation_cannot_advance_offsets() {
        let consumer = consumer();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        consumer.queue_states.lock().unwrap().insert(
            queue.clone(),
            QueueState {
                generation: 8,
                fetched_offset: Some(21),
                delivered_offset: Some(18),
                paused: false,
            },
        );
        let result = ProcessedPullResult {
            result: PullResultExt {
                status: PullStatus::NoNewMessage,
                next_begin_offset: 25,
                min_offset: 0,
                max_offset: 30,
                suggest_which_broker_id: 0,
                offset_delta: None,
                raw_messages: None,
            },
            messages: None,
        };
        assert_eq!(consumer.accept_pull_result(&queue, 7, result), None);
        let states = consumer.queue_states.lock().unwrap();
        let state = states.get(&queue).unwrap();
        assert_eq!(state.fetched_offset, Some(21));
        assert_eq!(state.delivered_offset, Some(18));
    }
}
