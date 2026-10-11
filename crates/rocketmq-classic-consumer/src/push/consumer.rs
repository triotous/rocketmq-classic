use std::{
    collections::{BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use rocketmq_classic_common::{
    message::{DecodeLimits, DecodeOptions, MessageQueue},
    sysflag::pull::{build_pull_sys_flag, clear_commit_offset_flag},
};
use rocketmq_classic_core::api::consumer::PullStatus;
use rocketmq_classic_core::runtime::{
    ClientRuntime, ClientRuntimeManager, ConsumerRuntime, GroupLease,
};
use rocketmq_classic_protocol::{
    SharedValue,
    codes::response,
    header::PullMessageRequestHeader,
    heartbeat::{ConsumeType, ConsumerData, ConsumerSubscriptionDataSet, SubscriptionData},
};
use tokio::time::Instant;

#[cfg(test)]
use std::sync::Mutex;

use crate::{
    consume::ConcurrentConsumeService,
    error::PushConsumerError,
    offset::{LocalFileOffsetStore, OffsetStore, RemoteBrokerOffsetStore},
    offset::{OffsetRead, ReadOffsetType},
    process_queue::ProcessQueue,
    pull::{PullAPIWrapper, build_subscription, resolve_pull_broker_address},
};

use super::{
    config::PushConsumerConfig,
    listener::PushMessageListener,
    pull_service::{
        PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL,
        PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL, PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION,
        PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND, PullRequest, PullRpcWorkers, PullScheduler,
        effective_queue_thresholds, flow_control_reason,
    },
};

type ConsumerFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

const BROKER_SUSPEND_MAX_TIME_MILLIS: i64 = 20_000;
const CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND: i64 = 30_000;

struct PullContext {
    runtime: Arc<ClientRuntime>,
    offset_store: Arc<dyn OffsetStore>,
    scheduler: Arc<PullScheduler>,
}

struct StartupServices {
    consume_service: Arc<ConcurrentConsumeService>,
    pull_scheduler: Arc<PullScheduler>,
    pull_rpc_workers: Arc<PullRpcWorkers>,
    consumer: Weak<PushConsumer>,
}

struct PullAttemptError {
    delay_millis: i64,
    reason: String,
}

impl PullAttemptError {
    fn retry(reason: impl Into<String>) -> Self {
        Self {
            delay_millis: PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION,
            reason: reason.into(),
        }
    }

    fn from_rpc(error: rocketmq_classic_core::error::RpcError) -> Self {
        let delay_millis = match &error {
            rocketmq_classic_core::error::RpcError::Response { code, .. }
                if *code == response::FLOW_CONTROL =>
            {
                PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL
            }
            _ => PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION,
        };
        Self {
            delay_millis,
            reason: error.to_string(),
        }
    }
}

enum State {
    Created,
    Started {
        runtime: Weak<ClientRuntime>,
        lease: GroupLease,
        offset_store: Arc<dyn OffsetStore>,
        consume_service: Arc<ConcurrentConsumeService>,
        pull_scheduler: Arc<PullScheduler>,
        pull_rpc_workers: Arc<PullRpcWorkers>,
    },
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupStage {
    RuntimeCreated,
    OffsetStoreLoaded,
    ConsumeServiceStarted,
    PullSchedulerStarted,
    PullRpcWorkersStarted,
    ConsumerRegistered,
    RuntimeStarted,
    RoutesUpdated,
    BrokerChecked,
    HeartbeatSent,
}

impl StartupStage {
    #[cfg(test)]
    const fn name(self) -> &'static str {
        match self {
            Self::RuntimeCreated => "runtime-created",
            Self::OffsetStoreLoaded => "offset-store-loaded",
            Self::ConsumeServiceStarted => "consume-service-started",
            Self::PullSchedulerStarted => "pull-scheduler-started",
            Self::PullRpcWorkersStarted => "pull-rpc-workers-started",
            Self::ConsumerRegistered => "consumer-registered",
            Self::RuntimeStarted => "runtime-started",
            Self::RoutesUpdated => "routes-updated",
            Self::BrokerChecked => "broker-checked",
            Self::HeartbeatSent => "heartbeat-sent",
        }
    }

    #[cfg(test)]
    const fn all() -> [Self; 10] {
        [
            Self::RuntimeCreated,
            Self::OffsetStoreLoaded,
            Self::ConsumeServiceStarted,
            Self::PullSchedulerStarted,
            Self::PullRpcWorkersStarted,
            Self::ConsumerRegistered,
            Self::RuntimeStarted,
            Self::RoutesUpdated,
            Self::BrokerChecked,
            Self::HeartbeatSent,
        ]
    }
}

/// Java-compatible Push consumer startup and shared-runtime registration.
/// Queue allocation and concurrent consumption remain partial until later
/// pipeline stages are attached.
pub struct PushConsumer {
    config: PushConsumerConfig,
    manager: ClientRuntimeManager,
    state: tokio::sync::Mutex<State>,
    offset_store: std::sync::Mutex<Option<Arc<dyn OffsetStore>>>,
    listener: RwLock<Option<PushMessageListener>>,
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    topic_queues: Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>>,
    process_queues: Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>>,
    offset_corrections_pending: Arc<RwLock<HashSet<MessageQueue>>>,
    #[allow(dead_code)] // Allocates assignment generations for Phase 11.
    next_generation: AtomicU64,
    pull_api: PullAPIWrapper,
    paused: AtomicBool,
    #[cfg(test)]
    startup_failure: Mutex<Option<StartupStage>>,
}

impl PushConsumer {
    pub fn new(config: PushConsumerConfig) -> Self {
        Self::with_manager(config, ClientRuntimeManager::shared_default())
    }

    fn with_manager(config: PushConsumerConfig, manager: ClientRuntimeManager) -> Self {
        let pull_api = PullAPIWrapper::new(config.unit_mode());
        Self {
            config,
            manager,
            state: tokio::sync::Mutex::new(State::Created),
            offset_store: std::sync::Mutex::new(None),
            listener: RwLock::new(None),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            topic_queues: Arc::new(RwLock::new(HashMap::new())),
            process_queues: Arc::new(RwLock::new(HashMap::new())),
            offset_corrections_pending: Arc::new(RwLock::new(HashSet::new())),
            next_generation: AtomicU64::new(1),
            pull_api,
            paused: AtomicBool::new(false),
            #[cfg(test)]
            startup_failure: Mutex::new(None),
        }
    }

    pub fn config(&self) -> &PushConsumerConfig {
        &self.config
    }

    pub fn get_offset_store(&self) -> Option<Arc<dyn OffsetStore>> {
        self.offset_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub async fn set_offset_store(
        &self,
        store: Option<Arc<dyn OffsetStore>>,
    ) -> Result<(), PushConsumerError> {
        let state = self.state.lock().await;
        match &*state {
            State::Created => {}
            State::Started { .. } => return Err(PushConsumerError::OffsetStoreAfterStartup),
            State::Shutdown => return Err(PushConsumerError::Shutdown),
        }
        *self
            .offset_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = store;
        Ok(())
    }

    pub async fn register_message_listener(
        &self,
        listener: PushMessageListener,
    ) -> Result<(), PushConsumerError> {
        let state = self.state.lock().await;
        match &*state {
            State::Created => {}
            State::Started { .. } => return Err(PushConsumerError::ConfigurationAfterStartup),
            State::Shutdown => return Err(PushConsumerError::Shutdown),
        }
        *write_lock(&self.listener) = Some(listener);
        Ok(())
    }

    pub async fn subscribe(
        &self,
        topic: &str,
        sub_expression: &str,
    ) -> Result<(), PushConsumerError> {
        self.subscribe_with_expression_type(topic, Some("TAG"), sub_expression)
            .await
    }

    pub async fn subscribe_with_expression_type(
        &self,
        topic: &str,
        expression_type: Option<&str>,
        expression: &str,
    ) -> Result<(), PushConsumerError> {
        let state = self.state.lock().await;
        match &*state {
            State::Created => {}
            State::Started { .. } => return Err(PushConsumerError::ConfigurationAfterStartup),
            State::Shutdown => return Err(PushConsumerError::Shutdown),
        }
        let mut client_config = self.config.client_config().clone();
        let topic = client_config.with_namespace(Some(topic))?.ok_or(
            PushConsumerError::InvalidSubscription("topic is empty".to_owned()),
        )?;
        let subscription = build_subscription(&topic, Some(expression), expression_type)
            .map_err(|error| PushConsumerError::InvalidSubscription(error.to_string()))?;
        write_lock(&self.subscriptions).insert(topic, subscription);
        Ok(())
    }

    pub async fn unsubscribe(&self, topic: &str) -> Result<bool, PushConsumerError> {
        let state = self.state.lock().await;
        match &*state {
            State::Created => {}
            State::Started { .. } => return Err(PushConsumerError::ConfigurationAfterStartup),
            State::Shutdown => return Err(PushConsumerError::Shutdown),
        }
        let mut client_config = self.config.client_config().clone();
        let topic = client_config.with_namespace(Some(topic))?.ok_or(
            PushConsumerError::InvalidSubscription("topic is empty".to_owned()),
        )?;
        Ok(write_lock(&self.subscriptions).remove(&topic).is_some())
    }

    pub fn subscriptions(&self) -> HashMap<String, SubscriptionData> {
        read_lock(&self.subscriptions).clone()
    }

    pub async fn suspend(&self) {
        if self.paused.swap(true, Ordering::AcqRel) {
            return;
        }
        if let State::Started { pull_scheduler, .. } = &*self.state.lock().await {
            pull_scheduler.pause_all();
        }
    }

    pub async fn resume(&self) {
        if !self.paused.swap(false, Ordering::AcqRel) {
            return;
        }
        let state = self.state.lock().await;
        if let State::Started {
            runtime,
            pull_scheduler,
            ..
        } = &*state
        {
            pull_scheduler.resume_all();
            if let Some(runtime) = runtime.upgrade() {
                runtime.rebalance_immediately();
            }
        }
    }

    /// Internal assignment activation used by Phase 11 and private pipeline
    /// tests. It is deliberately not part of the public Push API.
    #[allow(dead_code)] // Activated by Phase 11; exercised through private tests in this phase.
    pub(crate) async fn activate_process_queue(
        &self,
        input_queue: &MessageQueue,
        initial_offset: i64,
    ) -> Result<Arc<ProcessQueue>, PushConsumerError> {
        if initial_offset < 0 {
            return Err(PushConsumerError::InvalidOffset);
        }
        let mut client_config = self.config.client_config().clone();
        let queue = client_config.queue_with_namespace(input_queue)?;
        let topic = queue
            .topic()
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?;
        if queue.broker_name().is_none_or(str::is_empty) {
            return Err(PushConsumerError::InvalidQueue);
        }
        if !read_lock(&self.subscriptions).contains_key(topic) {
            return Err(PushConsumerError::MissingSubscriptionForQueue(
                topic.to_owned(),
            ));
        }
        let mut group_config = self.config.client_config().clone();
        let consumer_group = group_config
            .with_namespace(Some(self.config.consumer_group()))?
            .ok_or(PushConsumerError::InvalidConsumerGroup)?;
        if read_lock(&self.offset_corrections_pending).contains(&queue) {
            return Err(PushConsumerError::OffsetCorrectionPending(
                queue.to_string(),
            ));
        }
        let (scheduler, offset_store) = match &*self.state.lock().await {
            State::Created => return Err(PushConsumerError::NotStarted),
            State::Shutdown => return Err(PushConsumerError::Shutdown),
            State::Started {
                pull_scheduler,
                offset_store,
                ..
            } => (pull_scheduler.clone(), offset_store.clone()),
        };
        if let Some(existing) = read_lock(&self.process_queues).get(&queue).cloned() {
            if existing
                .snapshot(existing.generation())
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                return Ok(existing);
            }
        }
        let generation = self
            .next_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| PushConsumerError::GenerationExhausted)?;
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), generation));
        if let Some(previous) =
            write_lock(&self.process_queues).insert(queue.clone(), process_queue.clone())
        {
            let _ = previous.drop_assignment(previous.generation());
            scheduler.cancel(previous.queue(), previous.generation());
        }
        offset_store.update_offset(&queue, initial_offset, false);
        let request = Arc::new(PullRequest::new(
            consumer_group,
            process_queue.clone(),
            initial_offset,
        ));
        if self.paused.load(Ordering::Acquire) {
            scheduler.pause(request.queue(), request.generation());
        }
        if let Err(error) = scheduler.schedule(request, 0) {
            let mut queues = write_lock(&self.process_queues);
            if queues
                .get(&queue)
                .is_some_and(|current| current.generation() == generation)
            {
                queues.remove(&queue);
            }
            return Err(PushConsumerError::PullScheduler(error.to_string()));
        }
        Ok(process_queue)
    }

    pub(crate) async fn remove_process_queue(
        &self,
        input_queue: &MessageQueue,
        generation: u64,
    ) -> bool {
        let mut client_config = self.config.client_config().clone();
        let Ok(queue) = client_config.queue_with_namespace(input_queue) else {
            return false;
        };
        let (scheduler, offset_store, runtime) = match &*self.state.lock().await {
            State::Started {
                runtime,
                offset_store,
                pull_scheduler,
                ..
            } => (
                pull_scheduler.clone(),
                offset_store.clone(),
                runtime.upgrade(),
            ),
            State::Created | State::Shutdown => return false,
        };
        let process_queue = {
            let mut queues = write_lock(&self.process_queues);
            if queues
                .get(&queue)
                .is_none_or(|process_queue| process_queue.generation() != generation)
            {
                return false;
            }
            queues.remove(&queue)
        };
        if let Some(process_queue) = process_queue {
            let _ = process_queue.drop_assignment(generation);
            scheduler.cancel(&queue, generation);
            offset_store.remove_offset(&queue);
            write_lock(&self.offset_corrections_pending).remove(&queue);
            if let Some(runtime) = runtime {
                runtime.rebalance_immediately();
            }
            true
        } else {
            false
        }
    }

    async fn pull_context(&self) -> Option<PullContext> {
        let state = self.state.lock().await;
        match &*state {
            State::Started {
                runtime,
                offset_store,
                pull_scheduler,
                ..
            } => Some(PullContext {
                runtime: runtime.upgrade()?,
                offset_store: offset_store.clone(),
                scheduler: pull_scheduler.clone(),
            }),
            State::Created | State::Shutdown => None,
        }
    }

    pub(crate) async fn run_pull_worker_request(self: Arc<Self>, request: Arc<PullRequest>) {
        let Some(context) = self.pull_context().await else {
            return;
        };
        let queue = request.queue().clone();
        let generation = request.generation();
        let process_queue = request.process_queue();
        if !request.is_current() {
            context.scheduler.cancel(&queue, generation);
            return;
        }
        let _ = process_queue.set_last_pull_timestamp(generation, epoch_millis());

        if self.paused.load(Ordering::Acquire) {
            let _ = context
                .scheduler
                .schedule(request, PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND);
            return;
        }

        let assigned_queue_count = read_lock(&self.process_queues)
            .keys()
            .filter(|assigned| assigned.topic() == queue.topic())
            .count();
        let thresholds = effective_queue_thresholds(&self.config, assigned_queue_count);
        if let Ok(snapshot) = process_queue.snapshot(generation) {
            if let Some(reason) = flow_control_reason(&snapshot, thresholds) {
                tracing::trace!(
                    consumer_group = %request.consumer_group(),
                    queue_id = queue.queue_id(),
                    generation,
                    ?reason,
                    cached_count = snapshot.cached_count,
                    cached_bytes = snapshot.cached_bytes,
                    max_span = snapshot.max_span,
                    "push pull paused by queue flow control"
                );
                let _ = context
                    .scheduler
                    .schedule(request, PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL);
                return;
            }
        }

        let Some(topic) = queue.topic() else {
            let _ = context
                .scheduler
                .schedule(request, PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION);
            return;
        };
        let subscription = read_lock(&self.subscriptions).get(topic).cloned();
        let Some(subscription) = subscription else {
            tracing::warn!(
                topic,
                queue_id = queue.queue_id(),
                "push pull has no subscription"
            );
            let _ = context
                .scheduler
                .schedule(request, PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION);
            return;
        };

        match self
            .fetch_push_result(&context, &request, &subscription)
            .await
        {
            Ok(result) => {
                self.apply_pull_result(request, context, result).await;
            }
            Err(error) => {
                tracing::debug!(
                    consumer_group = %request.consumer_group(),
                    topic,
                    queue_id = queue.queue_id(),
                    generation,
                    delay_millis = error.delay_millis,
                    error = %error.reason,
                    "push pull request will be retried"
                );
                let _ = context.scheduler.schedule(request, error.delay_millis);
            }
        }
    }

    async fn fetch_push_result(
        &self,
        context: &PullContext,
        request: &PullRequest,
        subscription: &SubscriptionData,
    ) -> Result<crate::pull::ProcessedPullResult, PullAttemptError> {
        let queue = request.queue();
        let topic = queue
            .topic()
            .filter(|topic| !topic.is_empty())
            .ok_or_else(|| PullAttemptError::retry("message queue has no topic"))?;
        let broker_name = queue
            .broker_name()
            .filter(|broker| !broker.is_empty())
            .ok_or_else(|| PullAttemptError::retry("message queue has no broker name"))?;
        let deadline =
            Instant::now() + Duration::from_millis(CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND as u64);
        let suggested_broker_id = self.pull_api.recalculate_pull_from_which_node(queue);
        let mut route = context
            .runtime
            .route_snapshot()
            .map_err(|error| PullAttemptError::retry(error.to_string()))?;
        let mut broker = route
            .subscribe_address(broker_name, suggested_broker_id, false)
            .map_err(|error| PullAttemptError::retry(error.to_string()))?;
        if broker.is_none() {
            context
                .runtime
                .update_topic_route(topic, deadline)
                .await
                .map_err(|error| PullAttemptError::retry(error.to_string()))?;
            route = context
                .runtime
                .route_snapshot()
                .map_err(|error| PullAttemptError::retry(error.to_string()))?;
            broker = route
                .subscribe_address(broker_name, suggested_broker_id, false)
                .map_err(|error| PullAttemptError::retry(error.to_string()))?;
        }
        let broker = broker.ok_or_else(|| {
            PullAttemptError::retry(format!("broker {broker_name} was not found in route"))
        })?;

        let class_filter = subscription.is_class_filter_mode();
        let sub_expression = if self.config.post_subscription_when_pull() && !class_filter {
            subscription.get_sub_string().map(str::to_owned)
        } else {
            None
        };
        let commit_offset = if self.config.message_model()
            == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
        {
            match context
                .offset_store
                .read_offset(queue, ReadOffsetType::ReadFromMemory)
                .await
                .map_err(|error| PullAttemptError::retry(error.to_string()))?
            {
                OffsetRead::Found(offset) if offset > 0 => offset,
                OffsetRead::Found(_) | OffsetRead::NotFound => 0,
            }
        } else {
            0
        };
        let mut sys_flag = build_pull_sys_flag(
            commit_offset > 0,
            true,
            sub_expression.is_some(),
            class_filter,
        );
        let commit_offset = if broker.slave {
            sys_flag = clear_commit_offset_flag(sys_flag);
            0
        } else {
            commit_offset
        };
        let broker_address = if class_filter {
            let route_data = route
                .raw_routes
                .get(topic)
                .ok_or_else(|| PullAttemptError::retry("class-filter route was not found"))?
                .snapshot()
                .map_err(|error| PullAttemptError::retry(error.to_string()))?;
            resolve_pull_broker_address(&route_data, topic, &broker.address, sys_flag)
                .map_err(|error| PullAttemptError::retry(error.to_string()))?
                .ok_or_else(|| PullAttemptError::retry("class-filter broker was not found"))?
        } else {
            broker.address
        };
        let mut client_config = self.config.client_config().clone();
        let mut group_config = client_config.clone();
        let consumer_group = group_config
            .with_namespace(Some(self.config.consumer_group()))
            .map_err(|error| PullAttemptError::retry(error.to_string()))?
            .ok_or_else(|| PullAttemptError::retry("consumer group is empty"))?;
        let header = PullMessageRequestHeader {
            consumer_group,
            topic: topic.to_owned(),
            queue_id: queue.queue_id(),
            queue_offset: request.next_offset(),
            max_msg_nums: self.config.pull_batch_size(),
            sys_flag,
            commit_offset,
            suspend_timeout_millis: BROKER_SUSPEND_MAX_TIME_MILLIS,
            subscription: sub_expression,
            sub_version: subscription.get_sub_version(),
            expression_type: subscription.get_expression_type().map(str::to_owned),
            max_msg_bytes: Some(self.config.pull_batch_size_in_bytes()),
            broker_name: Some(broker_name.to_owned()),
            ..PullMessageRequestHeader::default()
        };
        let pull_result = context
            .runtime
            .api()
            .pull_message(&broker_address, &header, deadline)
            .await
            .map_err(PullAttemptError::from_rpc)?;
        let mut processed = self
            .pull_api
            .process_pull_result(
                queue,
                pull_result,
                subscription,
                DecodeOptions {
                    read_body: client_config.decode_read_body,
                    decompress_body: client_config.decode_decompress_body,
                    ..DecodeOptions::default()
                },
                DecodeLimits::default(),
            )
            .map_err(|error| PullAttemptError::retry(error.to_string()))?;
        if let Some(messages) = processed.messages.as_mut() {
            for message in messages {
                let message_topic = message.message().topic().to_owned();
                let topic = client_config
                    .without_namespace(Some(&message_topic))
                    .map_err(|error| PullAttemptError::retry(error.to_string()))?;
                if let Some(topic) = topic {
                    message.message_mut().set_topic(topic);
                }
            }
        }
        Ok(processed)
    }

    async fn apply_pull_result(
        self: &Arc<Self>,
        request: Arc<PullRequest>,
        context: PullContext,
        processed: crate::pull::ProcessedPullResult,
    ) {
        let queue = request.queue().clone();
        let generation = request.generation();
        let process_queue = request.process_queue();
        if !request.is_current() {
            context.scheduler.cancel(&queue, generation);
            return;
        }
        let next_offset = processed.result.next_begin_offset;
        request.set_next_offset(next_offset);
        match processed.result.status {
            PullStatus::Found => {
                let Some(messages) = processed.messages else {
                    let _ = context
                        .scheduler
                        .schedule(request, PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION);
                    return;
                };
                if messages.is_empty() {
                    let _ = context.scheduler.schedule(request, 0);
                    return;
                }
                if !request.is_current() {
                    return;
                }
                let outcome = match process_queue.insert_messages(generation, messages) {
                    Ok(outcome) => outcome,
                    Err(_) => return,
                };
                if outcome.dispatch_to_consume {
                    tracing::trace!(
                        topic = ?queue.topic(),
                        queue_id = queue.queue_id(),
                        generation,
                        inserted_count = outcome.inserted_count,
                        "push process queue has ready messages"
                    );
                }
                let delay = self.config.pull_interval_millis();
                let _ = context.scheduler.schedule(request, delay);
            }
            PullStatus::NoNewMessage | PullStatus::NoMatchedMessage => {
                if let Ok(snapshot) = process_queue.snapshot(generation) {
                    if snapshot.cached_count == 0 {
                        context
                            .offset_store
                            .update_offset(&queue, next_offset, true);
                    }
                }
                let _ = context.scheduler.schedule(request, 0);
            }
            PullStatus::OffsetIllegal => {
                if process_queue.drop_assignment(generation) == Ok(true) {
                    write_lock(&self.offset_corrections_pending).insert(queue.clone());
                    context.scheduler.cancel(&queue, generation);
                    self.schedule_offset_recovery(
                        context.runtime,
                        context.offset_store,
                        queue,
                        generation,
                        next_offset,
                    );
                }
            }
        }
    }

    fn schedule_offset_recovery(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        offset_store: Arc<dyn OffsetStore>,
        queue: MessageQueue,
        generation: u64,
        corrected_offset: i64,
    ) {
        offset_store.update_and_freeze_offset(&queue, corrected_offset);
        let consumer = Arc::downgrade(self);
        let retry_runtime = runtime.clone();
        let retry_store = offset_store.clone();
        let retry_queue = queue.clone();
        let task_retry_runtime = retry_runtime.clone();
        let task_retry_store = retry_store.clone();
        let task_retry_queue = retry_queue.clone();
        let task = async move {
            match offset_store.persist(&queue).await {
                Ok(()) => {
                    if let Some(consumer) = consumer.upgrade() {
                        let _ = consumer.remove_process_queue(&queue, generation).await;
                    }
                }
                Err(error) => {
                    tracing::error!(
                        queue = ?queue,
                        generation,
                        error = %error,
                        "failed to persist corrected push pull offset"
                    );
                    if let Some(consumer) = consumer.upgrade() {
                        consumer.schedule_offset_recovery_retry(
                            task_retry_runtime,
                            task_retry_store,
                            task_retry_queue,
                            generation,
                            corrected_offset,
                        );
                    }
                }
            }
        };
        if let Err(error) = runtime.execute_task(task) {
            tracing::error!(
                queue = ?retry_queue,
                generation,
                error = %error,
                "failed to schedule corrected push offset persistence"
            );
            self.schedule_offset_recovery_retry(
                retry_runtime,
                retry_store,
                retry_queue,
                generation,
                corrected_offset,
            );
        }
    }

    fn schedule_offset_recovery_retry(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        offset_store: Arc<dyn OffsetStore>,
        queue: MessageQueue,
        generation: u64,
        corrected_offset: i64,
    ) {
        let consumer = Arc::downgrade(self);
        let task_runtime = runtime.clone();
        let task_queue = queue.clone();
        if let Err(error) = runtime.execute_task_later(
            async move {
                if let Some(consumer) = consumer.upgrade() {
                    consumer.schedule_offset_recovery(
                        task_runtime,
                        offset_store,
                        task_queue,
                        generation,
                        corrected_offset,
                    );
                }
            },
            PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION,
        ) {
            tracing::error!(
                queue = ?queue,
                generation,
                error = %error,
                "failed to reschedule corrected push offset persistence"
            );
        }
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), PushConsumerError> {
        let mut state = self.state.lock().await;
        match &*state {
            State::Started { .. } => return Err(PushConsumerError::AlreadyStarted),
            State::Shutdown => return Err(PushConsumerError::Shutdown),
            State::Created => {}
        }

        let mut client_config = self.config.client_config().clone();
        let consumer_group = client_config
            .with_namespace(Some(self.config.consumer_group()))?
            .ok_or(PushConsumerError::InvalidConsumerGroup)?;
        self.config.validate(&consumer_group)?;
        let listener = read_lock(&self.listener)
            .clone()
            .ok_or(PushConsumerError::MissingMessageListener)?;
        if matches!(listener, PushMessageListener::Orderly(_)) {
            return Err(PushConsumerError::UnsupportedListenerMode);
        }
        let PushMessageListener::Concurrently(concurrent_listener) = listener else {
            return Err(PushConsumerError::UnsupportedListenerMode);
        };
        let consume_service = Arc::new(ConcurrentConsumeService::new(
            self.config.consume_thread_min(),
            self.config.consume_thread_max(),
            concurrent_listener,
        ));
        let pull_scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());

        self.copy_subscriptions(&consumer_group);

        let runtime = self.manager.get_or_create(&client_config).await?;
        if let Err(error) = self.checkpoint(StartupStage::RuntimeCreated) {
            let _ = runtime.shutdown().await;
            return Err(error);
        }
        let offset_store = match self.select_offset_store(&runtime, &consumer_group) {
            Ok(store) => store,
            Err(error) => {
                let _ = runtime.shutdown().await;
                return Err(error);
            }
        };
        let services = StartupServices {
            consume_service: consume_service.clone(),
            pull_scheduler: pull_scheduler.clone(),
            pull_rpc_workers: pull_rpc_workers.clone(),
            consumer: Arc::downgrade(self),
        };
        let startup = self
            .start_registered(&runtime, &consumer_group, offset_store.clone(), &services)
            .await;
        match startup {
            Ok(lease) => {
                *state = State::Started {
                    runtime: Arc::downgrade(&runtime),
                    lease,
                    offset_store,
                    consume_service,
                    pull_scheduler,
                    pull_rpc_workers,
                };
                Ok(())
            }
            Err((error, lease)) => {
                pull_scheduler.stop().await;
                pull_rpc_workers
                    .stop(self.config.await_termination_millis_when_shutdown())
                    .await;
                consume_service
                    .shutdown(self.config.await_termination_millis_when_shutdown())
                    .await;
                if let Some(lease) = lease {
                    let _ = runtime
                        .unregister_consumer(&lease, self.unregister_deadline())
                        .await;
                }
                let _ = runtime.shutdown().await;
                Err(error)
            }
        }
    }

    async fn start_registered(
        &self,
        runtime: &Arc<ClientRuntime>,
        consumer_group: &str,
        offset_store: Arc<dyn OffsetStore>,
        services: &StartupServices,
    ) -> Result<GroupLease, (PushConsumerError, Option<GroupLease>)> {
        if let Err(error) = offset_store.load().await {
            return Err((error.into(), None));
        }
        if let Err(error) = self.checkpoint(StartupStage::OffsetStoreLoaded) {
            return Err((error, None));
        }
        services.consume_service.start().await;
        if let Err(error) = self.checkpoint(StartupStage::ConsumeServiceStarted) {
            return Err((error, None));
        }
        services
            .pull_scheduler
            .start(Arc::downgrade(runtime))
            .map_err(|error| (PushConsumerError::PullScheduler(error.to_string()), None))?;
        if let Err(error) = self.checkpoint(StartupStage::PullSchedulerStarted) {
            return Err((error, None));
        }
        if let Err(error) = services
            .pull_rpc_workers
            .start(services.consumer.clone(), services.pull_scheduler.clone())
        {
            return Err((PushConsumerError::PullScheduler(error.to_string()), None));
        }
        if let Err(error) = self.checkpoint(StartupStage::PullRpcWorkersStarted) {
            return Err((error, None));
        }
        let owner: Arc<dyn ConsumerRuntime> = Arc::new(PushConsumerRegistration {
            group: consumer_group.to_owned(),
            message_model: self.config.message_model(),
            consume_from_where: self.config.consume_from_where(),
            unit_mode: self.config.unit_mode(),
            subscriptions: self.subscriptions.clone(),
            topic_queues: self.topic_queues.clone(),
            offset_store: Arc::downgrade(&offset_store),
            pull_scheduler: services.pull_scheduler.clone(),
            pull_rpc_workers: services.pull_rpc_workers.clone(),
        });
        let lease = match runtime.register_consumer(consumer_group, owner).await {
            Ok(lease) => lease,
            Err(error) => return Err((error.into(), None)),
        };
        if let Err(error) = self.checkpoint(StartupStage::ConsumerRegistered) {
            return Err((error, Some(lease)));
        }
        if let Err(error) = runtime.start().await {
            return Err((error.into(), Some(lease)));
        }
        if let Err(error) = self.checkpoint(StartupStage::RuntimeStarted) {
            return Err((error, Some(lease)));
        }
        if let Err(error) = runtime.update_all_topic_routes().await {
            return Err((error.into(), Some(lease)));
        }
        if let Err(error) = self.checkpoint(StartupStage::RoutesUpdated) {
            return Err((error, Some(lease)));
        }
        if let Err(error) = runtime.check_client_in_broker().await {
            return Err((error.into(), Some(lease)));
        }
        if let Err(error) = self.checkpoint(StartupStage::BrokerChecked) {
            return Err((error, Some(lease)));
        }
        match runtime.send_heartbeat_to_all_brokers().await {
            Ok(should_rebalance) => {
                if should_rebalance {
                    runtime.rebalance_immediately();
                }
            }
            Err(error) => return Err((error.into(), Some(lease))),
        }
        if let Err(error) = self.checkpoint(StartupStage::HeartbeatSent) {
            return Err((error, Some(lease)));
        }
        Ok(lease)
    }

    fn checkpoint(&self, stage: StartupStage) -> Result<(), PushConsumerError> {
        #[cfg(test)]
        if *self
            .startup_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == Some(stage)
        {
            return Err(PushConsumerError::TestStartupFault(stage.name()));
        }
        #[cfg(not(test))]
        let _ = stage;
        Ok(())
    }

    fn copy_subscriptions(&self, consumer_group: &str) {
        if self.config.message_model()
            != rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
        {
            let retry_topic = format!("%RETRY%{consumer_group}");
            let mut subscription =
                SubscriptionData::new(Some(retry_topic.clone()), Some("*".to_owned()));
            subscription.set_sub_version(epoch_millis());
            write_lock(&self.subscriptions).insert(retry_topic, subscription);
        }
    }

    fn select_offset_store(
        &self,
        runtime: &Arc<ClientRuntime>,
        consumer_group: &str,
    ) -> Result<Arc<dyn OffsetStore>, PushConsumerError> {
        let mut configured = self
            .offset_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(store) = configured.clone() {
            return Ok(store);
        }
        let store: Arc<dyn OffsetStore> = match self.config.message_model() {
            rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting => Arc::new(
                LocalFileOffsetStore::new(runtime.client_id(), consumer_group)?,
            ),
            rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
            | rocketmq_classic_protocol::heartbeat::MessageModel::LiteSelective => Arc::new(
                RemoteBrokerOffsetStore::new(runtime.clone(), consumer_group),
            ),
        };
        *configured = Some(store.clone());
        Ok(store)
    }

    pub async fn shutdown(&self) -> Result<(), PushConsumerError> {
        let (runtime, lease, offset_store, consume_service, pull_scheduler, pull_rpc_workers) = {
            let mut state = self.state.lock().await;
            if matches!(&*state, State::Created | State::Shutdown) {
                return Ok(());
            }
            match std::mem::replace(&mut *state, State::Shutdown) {
                State::Started {
                    runtime,
                    lease,
                    offset_store,
                    consume_service,
                    pull_scheduler,
                    pull_rpc_workers,
                } => (
                    runtime.upgrade(),
                    lease,
                    offset_store,
                    consume_service,
                    pull_scheduler,
                    pull_rpc_workers,
                ),
                State::Created | State::Shutdown => unreachable!("state checked before move"),
            }
        };
        pull_scheduler.stop().await;
        pull_rpc_workers
            .stop(self.config.await_termination_millis_when_shutdown())
            .await;
        consume_service
            .shutdown(self.config.await_termination_millis_when_shutdown())
            .await;
        let Some(runtime) = runtime else {
            return Ok(());
        };
        let queues = offset_store
            .clone_offset_table(None)
            .into_keys()
            .collect::<Vec<_>>();
        let _ = offset_store.persist_all(&queues).await;
        let unregister = runtime
            .unregister_consumer(&lease, self.unregister_deadline())
            .await;
        let shutdown = runtime.shutdown().await;
        unregister?;
        shutdown?;
        Ok(())
    }

    pub async fn is_running(&self) -> bool {
        matches!(&*self.state.lock().await, State::Started { .. })
    }

    fn unregister_deadline(&self) -> Instant {
        let timeout = self.config.client_config().mq_client_api_timeout().max(0) as u64;
        Instant::now() + Duration::from_millis(timeout)
    }
}

impl ConsumerRuntime for PushConsumerRegistration {
    fn pull_message(
        &self,
        request: Arc<dyn rocketmq_classic_core::runtime::MessageRequest>,
    ) -> Option<ConsumerFuture<'_>> {
        let request = request.as_any().downcast_ref::<PullRequest>()?;
        let request = Arc::new(request.clone());
        if let Err(request) = self.pull_rpc_workers.try_submit(request) {
            let _ = self.pull_scheduler.schedule(
                request.clone(),
                PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL,
            );
            self.pull_scheduler.finish(&request);
        }
        Some(Box::pin(async {}))
    }

    fn heartbeat_data(&self) -> ConsumerData {
        #[allow(clippy::mutable_key_type)]
        let subscription_data_set = read_lock(&self.subscriptions)
            .values()
            .cloned()
            .map(|subscription| Some(SharedValue::from(subscription)))
            .collect::<ConsumerSubscriptionDataSet>();
        ConsumerData {
            group_name: Some(self.group.clone()),
            consume_type: Some(ConsumeType::ConsumePassively),
            message_model: Some(self.message_model),
            consume_from_where: Some(self.consume_from_where),
            subscription_data_set: Some(SharedValue::from(subscription_data_set)),
            unit_mode: self.unit_mode,
        }
    }

    fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        read_lock(&self.subscriptions).contains_key(topic)
    }

    fn update_topic_subscribe_info(&self, topic: &str, queues: BTreeSet<MessageQueue>) {
        write_lock(&self.topic_queues).insert(topic.to_owned(), queues);
    }

    fn persist_consumer_offset(&self) -> Option<ConsumerFuture<'_>> {
        let store = self.offset_store.upgrade()?;
        Some(Box::pin(async move {
            let queues = store
                .clone_offset_table(None)
                .into_keys()
                .collect::<Vec<_>>();
            let _ = store.persist_all(&queues).await;
        }))
    }
}

struct PushConsumerRegistration {
    group: String,
    message_model: rocketmq_classic_protocol::heartbeat::MessageModel,
    consume_from_where: rocketmq_classic_protocol::heartbeat::ConsumeFromWhere,
    unit_mode: bool,
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    topic_queues: Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>>,
    pull_scheduler: Arc<PullScheduler>,
    pull_rpc_workers: Arc<PullRpcWorkers>,
    offset_store: Weak<dyn OffsetStore>,
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset::OffsetStoreFuture;
    use rocketmq_classic_common::message::{Message, MessageExt};
    use rocketmq_classic_core::api::consumer::PullResultExt;
    use std::collections::HashMap;

    #[derive(Default)]
    struct RecordingOffsetStore {
        values: Mutex<HashMap<MessageQueue, i64>>,
        updates: Mutex<Vec<(MessageQueue, i64, bool)>>,
    }

    impl OffsetStore for RecordingOffsetStore {
        fn load(&self) -> OffsetStoreFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn update_offset(&self, queue: &MessageQueue, offset: i64, increase_only: bool) {
            self.updates
                .lock()
                .unwrap()
                .push((queue.clone(), offset, increase_only));
            let mut values = self.values.lock().unwrap();
            if !increase_only || values.get(queue).is_none_or(|current| offset > *current) {
                values.insert(queue.clone(), offset);
            }
        }

        fn update_and_freeze_offset(&self, queue: &MessageQueue, offset: i64) {
            self.update_offset(queue, offset, false);
        }

        fn read_offset<'a>(
            &'a self,
            queue: &'a MessageQueue,
            _mode: ReadOffsetType,
        ) -> OffsetStoreFuture<'a, OffsetRead> {
            Box::pin(async move {
                Ok(self
                    .values
                    .lock()
                    .unwrap()
                    .get(queue)
                    .copied()
                    .map_or(OffsetRead::NotFound, OffsetRead::Found))
            })
        }

        fn persist<'a>(&'a self, _queue: &'a MessageQueue) -> OffsetStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn persist_all<'a>(&'a self, _queues: &'a [MessageQueue]) -> OffsetStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn remove_offset(&self, queue: &MessageQueue) {
            self.values.lock().unwrap().remove(queue);
        }

        fn clone_offset_table(&self, topic: Option<&str>) -> HashMap<MessageQueue, i64> {
            self.values
                .lock()
                .unwrap()
                .iter()
                .filter(|(queue, _)| topic.is_none_or(|topic| queue.topic() == Some(topic)))
                .map(|(queue, offset)| (queue.clone(), *offset))
                .collect()
        }

        fn update_consume_offset_to_broker<'a>(
            &'a self,
            _queue: &'a MessageQueue,
            _offset: i64,
            _oneway: bool,
        ) -> OffsetStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    fn pull_result(status: PullStatus, next_begin_offset: i64) -> crate::pull::ProcessedPullResult {
        crate::pull::ProcessedPullResult {
            result: PullResultExt {
                status,
                next_begin_offset,
                min_offset: 0,
                max_offset: next_begin_offset,
                suggest_which_broker_id: 0,
                offset_delta: None,
                raw_messages: None,
            },
            messages: (status == PullStatus::Found).then(Vec::new),
        }
    }

    fn message(offset: i64) -> MessageExt {
        let mut message = MessageExt::new(Message::new("orders", b"body".to_vec()));
        message.set_queue_offset(offset);
        message
    }

    #[test]
    fn broker_flow_control_uses_the_java_retry_delay() {
        let flow_control =
            PullAttemptError::from_rpc(rocketmq_classic_core::error::RpcError::Response {
                request_code: 11,
                peer: "broker-a".to_owned(),
                code: response::FLOW_CONTROL,
                remark: None,
            });
        assert_eq!(
            flow_control.delay_millis,
            PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL
        );

        let failure = PullAttemptError::from_rpc(
            rocketmq_classic_core::error::RpcError::Unsupported("test failure"),
        );
        assert_eq!(failure.delay_millis, PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION);
    }

    #[tokio::test]
    async fn push_pull_outcomes_insert_found_messages_and_correct_only_drained_offsets() {
        let mut config = PushConsumerConfig::new("pull-outcomes").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let scheduler = Arc::new(PullScheduler::new());
        let context = || PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler: scheduler.clone(),
        };

        let empty_queue = MessageQueue::new("orders", "broker-a", 0);
        let empty_process_queue = Arc::new(ProcessQueue::new(empty_queue.clone(), 1));
        let empty_request = Arc::new(PullRequest::new("pull-outcomes", empty_process_queue, 10));
        scheduler.schedule(empty_request.clone(), 0).unwrap();
        let active_empty_request = scheduler.take_due().unwrap();
        consumer
            .apply_pull_result(
                empty_request.clone(),
                context(),
                pull_result(PullStatus::NoNewMessage, 20),
            )
            .await;
        scheduler.finish(&active_empty_request);
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(empty_queue.clone(), 20, true)]
        );
        let follow_up = scheduler.take_due().unwrap();
        scheduler.finish(&follow_up);

        let unresolved_queue = MessageQueue::new("orders", "broker-a", 1);
        let unresolved_process_queue = Arc::new(ProcessQueue::new(unresolved_queue.clone(), 2));
        unresolved_process_queue
            .insert_messages(2, vec![message(30)])
            .unwrap();
        let unresolved_request = Arc::new(PullRequest::new(
            "pull-outcomes",
            unresolved_process_queue.clone(),
            30,
        ));
        scheduler.schedule(unresolved_request.clone(), 0).unwrap();
        let active_unresolved_request = scheduler.take_due().unwrap();
        consumer
            .apply_pull_result(
                unresolved_request.clone(),
                context(),
                pull_result(PullStatus::NoMatchedMessage, 35),
            )
            .await;
        scheduler.finish(&active_unresolved_request);
        assert_eq!(offset_store.updates.lock().unwrap().len(), 1);
        assert_eq!(
            unresolved_process_queue.snapshot(2).unwrap().cached_count,
            1
        );
        let follow_up = scheduler.take_due().unwrap();
        scheduler.finish(&follow_up);

        let found_queue = MessageQueue::new("orders", "broker-a", 2);
        let found_process_queue = Arc::new(ProcessQueue::new(found_queue, 3));
        let found_request = Arc::new(PullRequest::new(
            "pull-outcomes",
            found_process_queue.clone(),
            40,
        ));
        scheduler.schedule(found_request.clone(), 0).unwrap();
        let active_found_request = scheduler.take_due().unwrap();
        let mut found = pull_result(PullStatus::Found, 42);
        found.messages = Some(vec![message(40)]);
        consumer
            .apply_pull_result(found_request.clone(), context(), found)
            .await;
        scheduler.finish(&active_found_request);
        let snapshot = found_process_queue.snapshot(3).unwrap();
        assert_eq!(snapshot.cached_count, 1);
        assert_eq!(found_request.next_offset(), 42);
    }

    #[tokio::test]
    async fn push_pull_rejects_stale_results_and_fences_offset_illegal_recovery() {
        let config = PushConsumerConfig::new("pull-generation-fence").unwrap();
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let scheduler = Arc::new(PullScheduler::new());
        let context = || PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler: scheduler.clone(),
        };

        let stale_queue = MessageQueue::new("orders", "broker-a", 3);
        let stale_process_queue = Arc::new(ProcessQueue::new(stale_queue, 4));
        let stale_request = Arc::new(PullRequest::new(
            "pull-generation-fence",
            stale_process_queue.clone(),
            50,
        ));
        scheduler.schedule(stale_request.clone(), 0).unwrap();
        let active_stale_request = scheduler.take_due().unwrap();
        stale_process_queue.drop_assignment(4).unwrap();
        let mut stale_result = pull_result(PullStatus::Found, 51);
        stale_result.messages = Some(vec![message(50)]);
        consumer
            .apply_pull_result(stale_request, context(), stale_result)
            .await;
        scheduler.finish(&active_stale_request);
        assert_eq!(stale_process_queue.snapshot(4).unwrap().cached_count, 0);
        assert!(offset_store.updates.lock().unwrap().is_empty());

        let illegal_queue = MessageQueue::new("orders", "broker-a", 4);
        let illegal_process_queue = Arc::new(ProcessQueue::new(illegal_queue.clone(), 5));
        let illegal_request = Arc::new(PullRequest::new(
            "pull-generation-fence",
            illegal_process_queue.clone(),
            60,
        ));
        consumer
            .apply_pull_result(
                illegal_request,
                context(),
                pull_result(PullStatus::OffsetIllegal, 55),
            )
            .await;
        assert!(illegal_process_queue.snapshot(5).unwrap().dropped);
        assert!(read_lock(&consumer.offset_corrections_pending).contains(&illegal_queue));
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(illegal_queue, 55, false)]
        );

        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn startup_rejects_missing_and_orderly_listeners_before_runtime_acquisition() {
        let consumer = Arc::new(PushConsumer::with_manager(
            PushConsumerConfig::new("missing-listener").unwrap(),
            ClientRuntimeManager::new(),
        ));
        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::MissingMessageListener)
        ));
        assert!(!consumer.is_running().await);

        let consumer = Arc::new(PushConsumer::with_manager(
            PushConsumerConfig::new("orderly-listener").unwrap(),
            ClientRuntimeManager::new(),
        ));
        consumer
            .register_message_listener(PushMessageListener::Orderly(Arc::new(EmptyOrderlyListener)))
            .await
            .unwrap();
        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::UnsupportedListenerMode)
        ));
        assert!(!consumer.is_running().await);
    }

    #[tokio::test]
    async fn startup_registers_push_consumer_and_shutdown_releases_it() {
        let mut config = PushConsumerConfig::new("push-lifecycle").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        consumer
            .register_message_listener(PushMessageListener::concurrently(
                |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                    (
                        Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                        context,
                    )
                },
            ))
            .await
            .unwrap();
        consumer.subscribe("orders", "*").await.unwrap();
        consumer.subscribe("temporary", "*").await.unwrap();
        assert!(consumer.unsubscribe("temporary").await.unwrap());
        assert!(!consumer.unsubscribe("temporary").await.unwrap());
        let subscriptions = consumer.subscriptions();
        assert_eq!(
            subscriptions
                .get("tenant%orders")
                .and_then(|subscription| subscription.sub_string.as_deref()),
            Some("*")
        );

        consumer.start().await.unwrap();
        assert!(consumer.is_running().await);
        let (consume_service, pull_scheduler, pull_rpc_workers) =
            match &*consumer.state.lock().await {
                State::Started {
                    consume_service,
                    pull_scheduler,
                    pull_rpc_workers,
                    ..
                } => (
                    consume_service.clone(),
                    pull_scheduler.clone(),
                    pull_rpc_workers.clone(),
                ),
                _ => panic!("consumer should own a started consume service"),
            };
        assert_eq!(consume_service.worker_count(), 20);
        assert_eq!(pull_scheduler.worker_count(), 1);
        assert_eq!(pull_rpc_workers.worker_count(), 4);
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let registration = runtime
            .select_consumer("tenant%push-lifecycle")
            .unwrap()
            .expect("started consumer should be registered");
        let heartbeat_topics = registration
            .heartbeat_data()
            .subscription_data_set
            .expect("push consumers publish subscriptions")
            .snapshot()
            .into_iter()
            .flatten()
            .filter_map(|subscription| subscription.snapshot().topic)
            .collect::<std::collections::BTreeSet<_>>();
        assert!(heartbeat_topics.contains("tenant%orders"));
        assert!(heartbeat_topics.contains("%RETRY%tenant%push-lifecycle"));
        assert!(consumer.get_offset_store().is_some());
        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::AlreadyStarted)
        ));
        assert!(matches!(
            consumer.set_offset_store(None).await,
            Err(PushConsumerError::OffsetStoreAfterStartup)
        ));
        assert!(matches!(
            consumer.unsubscribe("orders").await,
            Err(PushConsumerError::ConfigurationAfterStartup)
        ));

        consumer.shutdown().await.unwrap();
        assert!(!consumer.is_running().await);
        assert_eq!(consume_service.worker_count(), 0);
        assert_eq!(pull_scheduler.worker_count(), 0);
        assert_eq!(pull_rpc_workers.worker_count(), 0);
        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn duplicate_group_registration_rolls_back_without_running_state() {
        let manager = ClientRuntimeManager::new();
        let make_consumer = || {
            let mut config = PushConsumerConfig::new("same-group").unwrap();
            config.client_config_mut().set_namesrv_addr(None);
            Arc::new(PushConsumer::with_manager(config, manager.clone()))
        };
        let first = make_consumer();
        let second = make_consumer();
        let listener = || {
            PushMessageListener::concurrently(
                |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                    (
                        Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                        context,
                    )
                },
            )
        };
        first.register_message_listener(listener()).await.unwrap();
        second.register_message_listener(listener()).await.unwrap();

        first.start().await.unwrap();
        assert!(matches!(
            second.start().await,
            Err(PushConsumerError::Runtime(_))
        ));
        assert!(!second.is_running().await);
        assert!(first.is_running().await);
        first.shutdown().await.unwrap();
        assert!(!first.is_running().await);
    }

    #[tokio::test]
    async fn startup_rolls_back_each_runtime_registration_and_route_stage() {
        for stage in StartupStage::all() {
            let mut config = PushConsumerConfig::new(format!("rollback-{}", stage.name())).unwrap();
            config.client_config_mut().set_namesrv_addr(None);
            let manager = ClientRuntimeManager::new();
            let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
            consumer
                .register_message_listener(PushMessageListener::concurrently(
                    |_messages: Vec<rocketmq_classic_common::message::MessageExt>, context| async move {
                        (
                            Some(super::super::listener::ConsumeConcurrentlyStatus::ConsumeSuccess),
                            context,
                        )
                    },
                ))
                .await
                .unwrap();
            *consumer
                .startup_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(stage);

            assert!(matches!(
                consumer.start().await,
                Err(PushConsumerError::TestStartupFault(_))
            ));
            assert!(!consumer.is_running().await);
            let runtime = manager
                .get_or_create(consumer.config().client_config())
                .await
                .unwrap();
            assert!(
                runtime
                    .select_consumer(consumer.config().consumer_group())
                    .unwrap()
                    .is_none()
            );
            runtime.shutdown().await.unwrap();
        }
    }

    struct EmptyOrderlyListener;
    impl super::super::listener::MessageListenerOrderly for EmptyOrderlyListener {
        fn consume_message(
            &self,
            _messages: Vec<rocketmq_classic_common::message::MessageExt>,
            context: super::super::listener::ConsumeOrderlyContext,
        ) -> super::super::listener::OrderlyListenerFuture {
            Box::pin(async move {
                (
                    super::super::listener::ConsumeOrderlyStatus::Success,
                    context,
                )
            })
        }
    }

    #[test]
    fn clustering_start_injects_retry_subscription_and_broadcast_does_not() {
        let clustering = PushConsumer::with_manager(
            PushConsumerConfig::new("orders").unwrap(),
            ClientRuntimeManager::new(),
        );
        clustering.copy_subscriptions("orders");
        assert!(clustering.subscriptions().contains_key("%RETRY%orders"));

        let mut config = PushConsumerConfig::new("broadcast-orders").unwrap();
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let broadcasting = PushConsumer::with_manager(config, ClientRuntimeManager::new());
        broadcasting.copy_subscriptions("broadcast-orders");
        assert!(
            !broadcasting
                .subscriptions()
                .keys()
                .any(|topic| topic.starts_with("%RETRY%"))
        );
    }
}
