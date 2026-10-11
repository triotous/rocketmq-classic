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

#[cfg(test)]
use std::collections::VecDeque;

use rocketmq_classic_common::{
    message::{
        DecodeLimits, DecodeOptions, Message, MessageQueue, constants::*, encode_properties,
    },
    sysflag::pull::{build_pull_sys_flag, clear_commit_offset_flag},
};
use rocketmq_classic_core::api::consumer::PullStatus;
use rocketmq_classic_core::api::producer::{SendKind, SendMode};
use rocketmq_classic_core::route::broker_vip_channel;
use rocketmq_classic_core::runtime::{
    ClientRuntime, ClientRuntimeManager, ConsumerRuntime, GroupLease,
};
use rocketmq_classic_protocol::{
    SharedValue,
    codes::response,
    header::{
        ConsumerSendMsgBackRequestHeader, PullMessageRequestHeader, SendMessageRequestHeaderV1,
    },
    heartbeat::{ConsumeType, ConsumerData, ConsumerSubscriptionDataSet, SubscriptionData},
};
use tokio::time::Instant;

#[cfg(test)]
use std::sync::Mutex;

use crate::{
    consume::{ConcurrentConsumeService, ConsumeRequest, ConsumeSubmitError},
    error::PushConsumerError,
    offset::{LocalFileOffsetStore, OffsetStore, RemoteBrokerOffsetStore},
    offset::{OffsetRead, ReadOffsetType},
    process_queue::ProcessQueue,
    pull::{PullAPIWrapper, build_subscription, resolve_pull_broker_address},
};

use super::{
    config::PushConsumerConfig,
    listener::{
        ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently,
        PushMessageListener,
    },
    pull_service::{
        PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL,
        PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL, PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION,
        PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND, PullRequest, PullRpcWorkers, PullScheduler,
        effective_queue_thresholds, flow_control_reason,
    },
};

type ConsumerFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

#[cfg(test)]
#[derive(Default)]
struct ScriptedRetryTransport {
    broker_results: VecDeque<Result<(), String>>,
    retry_results: VecDeque<Result<(), String>>,
    broker_headers: Vec<ConsumerSendMsgBackRequestHeader>,
    retry_messages: Vec<Message>,
}

const BROKER_SUSPEND_MAX_TIME_MILLIS: i64 = 20_000;
const CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND: i64 = 30_000;
const CONSUME_RETRY_DELAY_MILLIS: i64 = 5_000;
const CONSUMER_SEND_BACK_TIMEOUT_MILLIS: u64 = 5_000;

struct PullContext {
    runtime: Arc<ClientRuntime>,
    offset_store: Arc<dyn OffsetStore>,
    scheduler: Arc<PullScheduler>,
    consume_service: Arc<ConcurrentConsumeService>,
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
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
    next_generation: AtomicU64,
    next_retry_message_id: AtomicU64,
    pull_api: PullAPIWrapper,
    paused: AtomicBool,
    #[cfg(test)]
    startup_failure: Mutex<Option<StartupStage>>,
    #[cfg(test)]
    scripted_retry_transport: Mutex<ScriptedRetryTransport>,
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
            next_retry_message_id: AtomicU64::new(0),
            pull_api,
            paused: AtomicBool::new(false),
            #[cfg(test)]
            startup_failure: Mutex::new(None),
            #[cfg(test)]
            scripted_retry_transport: Mutex::new(ScriptedRetryTransport::default()),
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
                consume_service,
                ..
            } => Some(PullContext {
                runtime: runtime.upgrade()?,
                offset_store: offset_store.clone(),
                scheduler: pull_scheduler.clone(),
                consume_service: consume_service.clone(),
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
                    self.submit_consume_batch(&context, process_queue, generation);
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

    fn submit_consume_batch(
        self: &Arc<Self>,
        context: &PullContext,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
    ) {
        let batch = match process_queue.take_batch(
            generation,
            self.config.consume_message_batch_max_size().max(1) as usize,
        ) {
            Ok(Some(batch)) => batch,
            Ok(None) | Err(_) => return,
        };
        let request = ConsumeRequest {
            process_queue,
            batch,
        };
        match context.consume_service.try_submit(request) {
            Ok(()) => {}
            Err(ConsumeSubmitError::Full(request)) => {
                self.requeue_unsubmitted_batch(&request);
                self.schedule_consume_retry(
                    context.runtime.clone(),
                    request.process_queue.clone(),
                    request.batch.generation(),
                    PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL,
                );
            }
            Err(ConsumeSubmitError::Closed(request)) => {
                self.requeue_unsubmitted_batch(&request);
            }
        }
    }

    fn requeue_unsubmitted_batch(&self, request: &ConsumeRequest) {
        let _ = request.process_queue.complete_batch(
            request.batch.generation(),
            request.batch.id(),
            &[],
        );
    }

    fn schedule_consume_retry(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
        delay_millis: i64,
    ) {
        let consumer = Arc::downgrade(self);
        let task = async move {
            let Some(consumer) = consumer.upgrade() else {
                return;
            };
            let Some(context) = consumer.pull_context().await else {
                return;
            };
            consumer.submit_consume_batch(&context, process_queue, generation);
        };
        if let Err(error) = runtime.execute_task_later(task, delay_millis) {
            tracing::debug!(
                generation,
                error = %error,
                "failed to schedule push consume submission retry"
            );
        }
    }

    pub(crate) async fn run_consume_request(
        self: Arc<Self>,
        request: ConsumeRequest,
        listener: Arc<dyn MessageListenerConcurrently>,
    ) {
        let Some(context) = self.pull_context().await else {
            self.requeue_unsubmitted_batch(&request);
            return;
        };
        let generation = request.batch.generation();
        if !request
            .process_queue
            .snapshot(generation)
            .is_ok_and(|snapshot| !snapshot.dropped)
        {
            return;
        }

        let messages = request
            .batch
            .messages()
            .iter()
            .map(|message| message.as_ref().clone())
            .collect::<Vec<_>>();
        let callback_context = ConsumeConcurrentlyContext::new(request.batch.queue().clone());
        let fallback_context = callback_context.clone();
        let mut callback = AbortOnDrop(tokio::spawn(async move {
            listener.consume_message(messages, callback_context).await
        }));
        let timeout_millis = self
            .config
            .consume_timeout_minutes()
            .max(1)
            .saturating_mul(60_000) as u64;
        let callback_result =
            tokio::time::timeout(Duration::from_millis(timeout_millis), &mut callback.0).await;
        let (status, returned_context) = match callback_result {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                tracing::error!(
                    topic = ?request.batch.queue().topic(),
                    queue_id = request.batch.queue().queue_id(),
                    generation,
                    error = %error,
                    "push concurrent listener failed"
                );
                (None, fallback_context)
            }
            Err(_) => {
                callback.0.abort();
                let _ = (&mut callback.0).await;
                tracing::warn!(
                    topic = ?request.batch.queue().topic(),
                    queue_id = request.batch.queue().queue_id(),
                    generation,
                    "push concurrent listener timed out"
                );
                (None, fallback_context)
            }
        };
        self.complete_consume_request(request, context, status, returned_context)
            .await;
    }

    async fn complete_consume_request(
        self: &Arc<Self>,
        request: ConsumeRequest,
        context: PullContext,
        status: Option<ConsumeConcurrentlyStatus>,
        callback_context: ConsumeConcurrentlyContext,
    ) {
        let process_queue = request.process_queue;
        let batch = request.batch;
        let generation = batch.generation();
        let queue = batch.queue().clone();
        if !process_queue
            .snapshot(generation)
            .is_ok_and(|snapshot| !snapshot.dropped)
        {
            return;
        }

        let batch_len = batch.offsets().len();
        if batch_len == 0 {
            let _ = process_queue.complete_batch(generation, batch.id(), &[]);
            return;
        }
        let ack_index = match status {
            Some(ConsumeConcurrentlyStatus::ConsumeSuccess) => callback_context
                .ack_index()
                .clamp(-1, batch_len.saturating_sub(1) as i32),
            Some(ConsumeConcurrentlyStatus::ReconsumeLater) | None => -1,
        };
        let acknowledged = (ack_index + 1).max(0) as usize;
        let mut terminal_offsets = batch.offsets()[..acknowledged].to_vec();
        let mut local_retry_needed = false;
        if self.config.message_model()
            == rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
        {
            terminal_offsets.extend_from_slice(&batch.offsets()[acknowledged..]);
        } else {
            for (offset, message) in batch.offsets()[acknowledged..]
                .iter()
                .copied()
                .zip(&batch.messages()[acknowledged..])
            {
                if !process_queue
                    .snapshot(generation)
                    .is_ok_and(|snapshot| !snapshot.dropped)
                {
                    return;
                }
                if self
                    .send_message_back(
                        &context.runtime,
                        &queue,
                        message.as_ref(),
                        callback_context.delay_level_when_next_consume(),
                    )
                    .await
                {
                    terminal_offsets.push(offset);
                } else if process_queue
                    .increment_reconsume_times(generation, offset)
                    .is_err()
                {
                    return;
                } else {
                    local_retry_needed = true;
                }
            }
        }

        let completion =
            match process_queue.complete_batch(generation, batch.id(), &terminal_offsets) {
                Ok(completion) => completion,
                Err(_) => return,
            };
        if let Some(offset) = completion.next_commit_offset {
            if process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                context.offset_store.update_offset(&queue, offset, true);
            }
        }
        if completion.has_ready_messages {
            if local_retry_needed {
                self.schedule_consume_retry(
                    context.runtime,
                    process_queue,
                    generation,
                    CONSUME_RETRY_DELAY_MILLIS,
                );
            } else {
                self.submit_consume_batch(&context, process_queue, generation);
            }
        }
    }

    async fn send_message_back(
        &self,
        runtime: &Arc<ClientRuntime>,
        queue: &MessageQueue,
        message: &rocketmq_classic_common::message::MessageExt,
        delay_level: i32,
    ) -> bool {
        let mut client_config = self.config.client_config().clone();
        let consumer_group = client_config
            .with_namespace(Some(self.config.consumer_group()))
            .ok()
            .flatten();
        let origin_topic = client_config
            .with_namespace(Some(message.message().topic()))
            .ok()
            .flatten();
        let Some(consumer_group) = consumer_group else {
            tracing::error!("push send-back has no consumer group");
            return false;
        };
        let Some(origin_topic) = origin_topic else {
            tracing::error!("push send-back has no origin topic");
            return false;
        };
        let broker_name = message
            .broker_name()
            .or(queue.broker_name())
            .map(str::to_owned);
        let header = ConsumerSendMsgBackRequestHeader {
            group: consumer_group.clone(),
            broker_name: broker_name.clone(),
            origin_topic: origin_topic.clone(),
            offset: message.commit_log_offset(),
            delay_level,
            origin_msg_id: message.message_id().map(str::to_owned),
            max_reconsume_times: Some(self.config.effective_max_reconsume_times()),
            unit_mode: self.config.unit_mode(),
        };

        match self
            .try_send_message_back_to_broker(runtime, &header, broker_name.as_deref())
            .await
        {
            Ok(()) => return true,
            Err(error) => tracing::debug!(
                topic = ?queue.topic(),
                queue_id = queue.queue_id(),
                offset = message.queue_offset(),
                error = %error,
                "push send-back Broker attempt failed; trying retry-topic publication"
            ),
        }

        let sequence = self.next_retry_message_id.fetch_add(1, Ordering::Relaxed);
        let retry_topic = format!("%RETRY%{consumer_group}");
        let retry_message =
            self.build_retry_message(runtime, message, retry_topic, origin_topic, sequence);
        match self.send_retry_message(runtime, &retry_message).await {
            Ok(()) => true,
            Err(error) => {
                tracing::error!(
                    topic = ?queue.topic(),
                    queue_id = queue.queue_id(),
                    offset = message.queue_offset(),
                    error = %error,
                    "push send-back and retry-topic fallback both failed"
                );
                false
            }
        }
    }

    async fn try_send_message_back_to_broker(
        &self,
        runtime: &Arc<ClientRuntime>,
        header: &ConsumerSendMsgBackRequestHeader,
        broker_name: Option<&str>,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            let mut scripted = self
                .scripted_retry_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scripted.broker_headers.push(header.clone());
            if let Some(result) = scripted.broker_results.pop_front() {
                return result;
            }
        }

        let broker_name = broker_name.ok_or_else(|| "message has no broker name".to_owned())?;
        let deadline = Instant::now() + Duration::from_millis(CONSUMER_SEND_BACK_TIMEOUT_MILLIS);
        let mut route = runtime
            .route_snapshot()
            .map_err(|error| error.to_string())?;
        let mut address = route.publish_address(broker_name).map(str::to_owned);
        if address.is_none() {
            runtime
                .update_topic_route(&header.origin_topic, deadline)
                .await
                .map_err(|error| error.to_string())?;
            route = runtime
                .route_snapshot()
                .map_err(|error| error.to_string())?;
            address = route.publish_address(broker_name).map(str::to_owned);
        }
        let address =
            address.ok_or_else(|| format!("broker {broker_name} has no master address"))?;
        let address =
            broker_vip_channel(self.config.client_config().vip_channel_enabled(), &address)
                .map_err(|error| error.to_string())?;
        runtime
            .api()
            .consumer_send_message_back(&address, header, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    fn build_retry_message(
        &self,
        runtime: &ClientRuntime,
        original: &rocketmq_classic_common::message::MessageExt,
        retry_topic: String,
        origin_topic: String,
        sequence: u64,
    ) -> Message {
        let reconsume_times = original.reconsume_times().saturating_add(1);
        let mut retry = Message::new(retry_topic, original.message().body().to_vec());
        retry.set_flag(original.message().flag());
        for (key, value) in original.message().properties() {
            if key != PROPERTY_TRANSACTION_PREPARED {
                let _ = retry.set_system_property(key.clone(), value.clone());
            }
        }
        let origin_message_id = original
            .message()
            .properties()
            .get(PROPERTY_ORIGIN_MESSAGE_ID)
            .map(String::as_str)
            .filter(|value| !value.trim().is_empty())
            .or_else(|| original.message_id());
        let _ = retry.set_system_property(PROPERTY_RETRY_TOPIC, origin_topic);
        if let Some(origin_message_id) = origin_message_id {
            let _ =
                retry.set_system_property(PROPERTY_ORIGIN_MESSAGE_ID, origin_message_id.to_owned());
        }
        let _ = retry.set_system_property(PROPERTY_RECONSUME_TIME, reconsume_times.to_string());
        let _ = retry.set_system_property(
            PROPERTY_MAX_RECONSUME_TIMES,
            self.config.effective_max_reconsume_times().to_string(),
        );
        let _ = retry.set_system_property(
            PROPERTY_DELAY_TIME_LEVEL,
            3i32.saturating_add(original.reconsume_times()).to_string(),
        );
        if retry
            .properties()
            .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .is_none_or(|value| value.trim().is_empty())
        {
            let unique_id = format!("{}-{}-{}", runtime.client_id(), epoch_millis(), sequence);
            let _ = retry.set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, unique_id);
        }
        retry
    }

    async fn send_retry_message(
        &self,
        runtime: &Arc<ClientRuntime>,
        retry: &Message,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            let mut scripted = self
                .scripted_retry_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scripted.retry_messages.push(retry.clone());
            if let Some(result) = scripted.retry_results.pop_front() {
                return result;
            }
        }

        const DEFAULT_CREATE_TOPIC_KEY: &str = "TBW102";
        const DEFAULT_TOPIC_QUEUE_COUNT: i32 = 4;
        const DEFAULT_COMPRESS_THRESHOLD: usize = 4 * 1024;
        const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;
        const INNER_PRODUCER_GROUP: &str = "CLIENT_INNER_PRODUCER";

        if retry.body().len() > DEFAULT_MAX_MESSAGE_SIZE {
            return Err("retry message exceeds the default producer size limit".into());
        }
        let topic = retry.topic();
        let timeout_millis = self.config.client_config().mq_client_api_timeout().max(0) as u64;
        let deadline = Instant::now() + Duration::from_millis(timeout_millis.max(1));
        let route = runtime
            .route_snapshot()
            .map_err(|error| error.to_string())?;
        if !route.topics.contains_key(topic) {
            runtime
                .update_topic_route(topic, deadline)
                .await
                .map_err(|error| error.to_string())?;
            let route = runtime
                .route_snapshot()
                .map_err(|error| error.to_string())?;
            if !route.topics.contains_key(topic) {
                runtime
                    .update_topic_route_from_default(topic, DEFAULT_TOPIC_QUEUE_COUNT, deadline)
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
        let route = runtime
            .route_snapshot()
            .map_err(|error| error.to_string())?;
        let topic_snapshot = route
            .topics
            .get(topic)
            .ok_or_else(|| format!("retry topic {topic} has no route"))?;
        if topic_snapshot.publish_queues.is_empty() {
            return Err(format!("retry topic {topic} has no writable queues"));
        }
        let sequence = self.next_retry_message_id.fetch_add(1, Ordering::Relaxed);
        let queue =
            &topic_snapshot.publish_queues[sequence as usize % topic_snapshot.publish_queues.len()];
        let broker_name = queue
            .broker_name()
            .ok_or_else(|| "retry queue has no broker name".to_owned())?;
        let address = route
            .publish_address(broker_name)
            .ok_or_else(|| format!("retry broker {broker_name} has no master address"))?;
        let address =
            broker_vip_channel(self.config.client_config().vip_channel_enabled(), address)
                .map_err(|error| error.to_string())?;

        let mut properties = retry.properties().clone();
        let reconsume_times = properties
            .remove(PROPERTY_RECONSUME_TIME)
            .and_then(|value| value.parse::<i32>().ok());
        let max_reconsume_times = properties
            .remove(PROPERTY_MAX_RECONSUME_TIMES)
            .and_then(|value| value.parse::<i32>().ok());
        let mut body = retry.body().to_vec();
        let mut sys_flag = 0;
        if body.len() >= DEFAULT_COMPRESS_THRESHOLD {
            if let Ok(compressed) = rocketmq_classic_common::compression::compress(
                rocketmq_classic_common::sysflag::message::CompressionType::Zlib,
                &body,
                5,
            ) {
                body = compressed;
                sys_flag |= rocketmq_classic_common::sysflag::message::COMPRESSED_FLAG
                    | rocketmq_classic_common::sysflag::message::CompressionType::Zlib
                        .get_compression_flag();
            }
        }
        let unique_id = properties
            .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .cloned()
            .ok_or_else(|| "retry message has no unique id".to_owned())?;
        let header = SendMessageRequestHeaderV1 {
            producer_group: INNER_PRODUCER_GROUP.into(),
            topic: topic.to_owned(),
            default_topic: DEFAULT_CREATE_TOPIC_KEY.into(),
            default_topic_queue_nums: DEFAULT_TOPIC_QUEUE_COUNT,
            queue_id: queue.queue_id(),
            sys_flag,
            born_timestamp: epoch_millis(),
            flag: retry.flag(),
            properties: Some(encode_properties(&properties)),
            reconsume_times,
            unit_mode: Some(self.config.unit_mode()),
            max_reconsume_times,
            batch: Some(false),
        };
        runtime
            .api()
            .send_message(
                &address,
                broker_name,
                &header,
                body,
                &unique_id,
                SendKind::V2,
                SendMode::Sync,
                deadline,
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
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
        services
            .consume_service
            .start(services.consumer.clone())
            .await;
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

    fn retry_test_message(offset: i64) -> MessageExt {
        // Java 5.5.1 producer-produced `ipv4-v1` record from
        // fixtures/messages/records.json (SHA-256 2F93B0A748E645D8AD2CEA027ABDF2C356ED16B74CEAF70A63D97B0A04861BD0CF).
        use base64::Engine;
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/messages/records.json"))
                .unwrap();
        let decoded = &fixture["cases"][0]["decoded"];
        let properties: HashMap<String, String> =
            serde_json::from_value(decoded["properties"].clone()).unwrap();
        let mut body = Message::new(
            decoded["topic"].as_str().unwrap(),
            base64::engine::general_purpose::STANDARD
                .decode(decoded["bodyBase64"].as_str().unwrap())
                .unwrap(),
        );
        body.set_flag(decoded["flag"].as_i64().unwrap() as i32);
        for (key, value) in properties {
            body.set_system_property(key, value).unwrap();
        }
        let mut message = MessageExt::new(body);
        message.set_queue_offset(offset);
        message.set_broker_name(Some("broker-a".into()));
        message.set_commit_log_offset(offset + 100);
        message.set_message_id(Some(format!("broker-msg-{offset}")));
        message.set_reconsume_times(2);
        message
            .message_mut()
            .set_system_property(PROPERTY_ORIGIN_MESSAGE_ID, "origin-msg")
            .unwrap();
        message
            .message_mut()
            .set_system_property(PROPERTY_RECONSUME_TIME, "2")
            .unwrap();
        message
            .message_mut()
            .set_system_property(PROPERTY_MAX_RECONSUME_TIMES, "7")
            .unwrap();
        message
            .message_mut()
            .set_system_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "source-uniq")
            .unwrap();
        message
            .message_mut()
            .set_system_property(PROPERTY_TRANSACTION_PREPARED, "true")
            .unwrap();
        message
            .message_mut()
            .set_system_property(PROPERTY_KEYS, "order-1")
            .unwrap();
        message
    }

    fn consume_request(
        queue: MessageQueue,
        generation: u64,
        messages: Vec<MessageExt>,
    ) -> ConsumeRequest {
        let process_queue = Arc::new(ProcessQueue::new(queue, generation));
        process_queue.insert_messages(generation, messages).unwrap();
        let batch = process_queue
            .take_batch(generation, usize::MAX)
            .unwrap()
            .unwrap();
        ConsumeRequest {
            process_queue,
            batch,
        }
    }

    fn idle_consume_service() -> Arc<ConcurrentConsumeService> {
        Arc::new(ConcurrentConsumeService::new(
            1,
            1,
            Arc::new(
                |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                },
            ),
        ))
    }

    async fn install_started_consumer(
        consumer: &Arc<PushConsumer>,
        manager: &ClientRuntimeManager,
        offset_store: Arc<dyn OffsetStore>,
        consume_service: Arc<ConcurrentConsumeService>,
        pull_scheduler: Arc<PullScheduler>,
        pull_rpc_workers: Arc<PullRpcWorkers>,
    ) -> Arc<ClientRuntime> {
        let mut client_config = consumer.config.client_config().clone();
        let consumer_group = client_config
            .with_namespace(Some(consumer.config.consumer_group()))
            .unwrap()
            .unwrap();
        let runtime = manager
            .get_or_create(consumer.config.client_config())
            .await
            .unwrap();
        let owner: Arc<dyn ConsumerRuntime> = Arc::new(PushConsumerRegistration {
            group: consumer_group.clone(),
            message_model: consumer.config.message_model(),
            consume_from_where: consumer.config.consume_from_where(),
            unit_mode: consumer.config.unit_mode(),
            subscriptions: consumer.subscriptions.clone(),
            topic_queues: consumer.topic_queues.clone(),
            pull_scheduler: pull_scheduler.clone(),
            pull_rpc_workers: pull_rpc_workers.clone(),
            offset_store: Arc::downgrade(&offset_store),
        });
        let lease = runtime
            .register_consumer(&consumer_group, owner)
            .await
            .unwrap();
        *consumer.state.lock().await = State::Started {
            runtime: Arc::downgrade(&runtime),
            lease,
            offset_store,
            consume_service: consume_service.clone(),
            pull_scheduler: pull_scheduler.clone(),
            pull_rpc_workers: pull_rpc_workers.clone(),
        };
        consume_service.start(Arc::downgrade(consumer)).await;
        pull_scheduler.start(Arc::downgrade(&runtime)).unwrap();
        pull_rpc_workers
            .start(Arc::downgrade(consumer), pull_scheduler)
            .unwrap();
        runtime
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
        let consume_service = idle_consume_service();
        let context = || PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler: scheduler.clone(),
            consume_service: consume_service.clone(),
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
        let consume_service = idle_consume_service();
        let context = || PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler: scheduler.clone(),
            consume_service: consume_service.clone(),
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
    async fn concurrent_ack_index_sends_only_the_unacknowledged_tail_back() {
        let config = PushConsumerConfig::new("consume-ack-index").unwrap();
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let scheduler = Arc::new(PullScheduler::new());
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler,
            consume_service: idle_consume_service(),
        };
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let request = consume_request(
            queue.clone(),
            31,
            vec![retry_test_message(10), retry_test_message(11)],
        );
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_results
            .push_back(Ok(()));
        let mut callback_context = ConsumeConcurrentlyContext::new(queue.clone());
        callback_context.set_ack_index(0);
        consumer
            .clone()
            .complete_consume_request(
                request,
                context,
                Some(ConsumeConcurrentlyStatus::ConsumeSuccess),
                callback_context,
            )
            .await;

        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert_eq!(scripted.broker_headers.len(), 1);
            assert_eq!(scripted.broker_headers[0].origin_topic, "orders");
            assert_eq!(scripted.broker_headers[0].offset, 111);
            assert_eq!(
                scripted.broker_headers[0].origin_msg_id.as_deref(),
                Some("broker-msg-11")
            );
            assert_eq!(scripted.broker_headers[0].max_reconsume_times, Some(16));
            assert!(scripted.retry_messages.is_empty());
        }
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 12, true)]
        );
        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn broker_send_back_failure_publishes_java_compatible_retry_metadata() {
        let mut config = PushConsumerConfig::new("consume-retry-fallback").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler: Arc::new(PullScheduler::new()),
            consume_service: idle_consume_service(),
        };
        let queue = MessageQueue::new("orders", "broker-a", 1);
        let request = consume_request(queue.clone(), 32, vec![retry_test_message(20)]);
        {
            let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
            scripted
                .broker_results
                .push_back(Err("broker refused send-back".into()));
            scripted.retry_results.push_back(Ok(()));
        }
        let callback_context = ConsumeConcurrentlyContext::new(queue.clone());
        consumer
            .clone()
            .complete_consume_request(
                request,
                context,
                Some(ConsumeConcurrentlyStatus::ReconsumeLater),
                callback_context,
            )
            .await;

        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            let retry = &scripted.retry_messages[0];
            assert_eq!(retry.topic(), "%RETRY%tenant%consume-retry-fallback");
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_RETRY_TOPIC)
                    .map(String::as_str),
                Some("tenant%orders")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_ORIGIN_MESSAGE_ID)
                    .map(String::as_str),
                Some("origin-msg")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_RECONSUME_TIME)
                    .map(String::as_str),
                Some("3")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_MAX_RECONSUME_TIMES)
                    .map(String::as_str),
                Some("16")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_DELAY_TIME_LEVEL)
                    .map(String::as_str),
                Some("5")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                    .map(String::as_str),
                Some("source-uniq")
            );
            assert_eq!(
                retry.properties().get(PROPERTY_KEYS).map(String::as_str),
                Some("order-1")
            );
            assert!(
                !retry
                    .properties()
                    .contains_key(PROPERTY_TRANSACTION_PREPARED)
            );
        }
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 21, true)]
        );
        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn failed_send_back_retains_and_increments_messages_for_redelivery() {
        let config = PushConsumerConfig::new("consume-retry-retention").unwrap();
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler: Arc::new(PullScheduler::new()),
            consume_service: idle_consume_service(),
        };
        let queue = MessageQueue::new("orders", "broker-a", 2);
        let request = consume_request(queue.clone(), 33, vec![retry_test_message(30)]);
        let process_queue = request.process_queue.clone();
        {
            let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
            scripted
                .broker_results
                .push_back(Err("broker refused send-back".into()));
            scripted
                .retry_results
                .push_back(Err("retry-topic publish failed".into()));
        }
        consumer
            .clone()
            .complete_consume_request(
                request,
                context,
                Some(ConsumeConcurrentlyStatus::ReconsumeLater),
                ConsumeConcurrentlyContext::new(queue.clone()),
            )
            .await;

        let snapshot = process_queue.snapshot(33).unwrap();
        assert_eq!(snapshot.cached_count, 1);
        assert_eq!(snapshot.in_flight_count, 0);
        let redelivery = process_queue.take_batch(33, 1).unwrap().unwrap();
        assert_eq!(redelivery.messages()[0].reconsume_times(), 3);
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 30, true)]
        );
        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn concurrent_listener_success_commits_the_complete_fixture_batch() {
        let mut config = PushConsumerConfig::new("consume-success").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let consume_service = idle_consume_service();
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            consume_service,
            scheduler,
            pull_rpc_workers,
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let request = consume_request(
            queue.clone(),
            50,
            vec![retry_test_message(60), retry_test_message(61)],
        );
        let process_queue = request.process_queue.clone();
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                assert_eq!(messages.len(), 2);
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        );
        consumer
            .clone()
            .run_consume_request(request, listener)
            .await;

        let snapshot = process_queue.snapshot(50).unwrap();
        assert_eq!(snapshot.cached_count, 0);
        assert_eq!(snapshot.in_flight_count, 0);
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 62, true)]
        );
        let _ = runtime.shutdown().await;
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_listener_without_status_sends_fixture_back_for_retry() {
        let mut config = PushConsumerConfig::new("consume-null-status").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let consume_service = idle_consume_service();
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            consume_service,
            scheduler,
            pull_rpc_workers,
        )
        .await;
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_results
            .push_back(Ok(()));

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let request = consume_request(queue.clone(), 51, vec![retry_test_message(70)]);
        let process_queue = request.process_queue.clone();
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                (None, context)
            },
        );
        consumer
            .clone()
            .run_consume_request(request, listener)
            .await;

        assert_eq!(process_queue.snapshot(51).unwrap().cached_count, 0);
        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert_eq!(scripted.broker_headers.len(), 1);
            assert_eq!(scripted.broker_headers[0].offset, 170);
        }
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 71, true)]
        );
        let _ = runtime.shutdown().await;
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_listener_panic_is_isolated_and_retried() {
        let mut config = PushConsumerConfig::new("consume-listener-panic").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let consume_service = idle_consume_service();
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            consume_service,
            scheduler,
            pull_rpc_workers,
        )
        .await;
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_results
            .push_back(Ok(()));

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let request = consume_request(queue.clone(), 52, vec![retry_test_message(80)]);
        let process_queue = request.process_queue.clone();
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |_messages: Vec<MessageExt>, _context: ConsumeConcurrentlyContext| async move {
                panic!("scripted concurrent-listener panic")
            },
        );
        consumer
            .clone()
            .run_consume_request(request, listener)
            .await;

        assert_eq!(process_queue.snapshot(52).unwrap().cached_count, 0);
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 81, true)]
        );
        let _ = runtime.shutdown().await;
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn private_fixed_assignment_retries_java_fixture_with_broker_limit() {
        let mut config = PushConsumerConfig::new("consume-fixed-assignment").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        consumer.subscribe("orders", "*").await.unwrap();
        consumer.suspend().await;
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let consume_service = idle_consume_service();
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            consume_service,
            scheduler,
            pull_rpc_workers,
        )
        .await;
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .broker_results
            .push_back(Ok(()));

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = consumer.activate_process_queue(&queue, 90).await.unwrap();
        let generation = process_queue.generation();
        process_queue
            .insert_messages(generation, vec![retry_test_message(90)])
            .unwrap();
        let batch = process_queue
            .take_batch(generation, usize::MAX)
            .unwrap()
            .unwrap();
        consumer
            .clone()
            .complete_consume_request(
                ConsumeRequest {
                    process_queue: process_queue.clone(),
                    batch,
                },
                consumer.pull_context().await.unwrap(),
                Some(ConsumeConcurrentlyStatus::ReconsumeLater),
                ConsumeConcurrentlyContext::new(queue.clone()),
            )
            .await;

        assert_eq!(process_queue.snapshot(generation).unwrap().cached_count, 0);
        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert_eq!(scripted.broker_headers.len(), 1);
            assert_eq!(scripted.broker_headers[0].offset, 190);
            assert_eq!(
                scripted.broker_headers[0].origin_msg_id.as_deref(),
                Some("broker-msg-90")
            );
            assert_eq!(scripted.broker_headers[0].max_reconsume_times, Some(16));
        }
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue.clone(), 90, false), (queue, 91, true)]
        );
        let _ = runtime.shutdown().await;
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn broadcasting_drops_failed_messages_without_clustering_retry() {
        let mut config = PushConsumerConfig::new("consume-broadcast-failure").unwrap();
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: offset_store.clone(),
            scheduler: Arc::new(PullScheduler::new()),
            consume_service: idle_consume_service(),
        };
        let queue = MessageQueue::new("orders", "broker-a", 3);
        let request = consume_request(queue.clone(), 34, vec![retry_test_message(40)]);
        let process_queue = request.process_queue.clone();
        consumer
            .clone()
            .complete_consume_request(
                request,
                context,
                Some(ConsumeConcurrentlyStatus::ReconsumeLater),
                ConsumeConcurrentlyContext::new(queue.clone()),
            )
            .await;

        assert_eq!(process_queue.snapshot(34).unwrap().cached_count, 0);
        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert!(scripted.broker_headers.is_empty());
            assert!(scripted.retry_messages.is_empty());
        }
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 41, true)]
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
