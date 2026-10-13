use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc, OnceLock, RwLock, Weak,
        atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering},
    },
    time::Duration,
};

#[cfg(test)]
use std::collections::VecDeque;

use rocketmq_classic_common::{
    message::{
        DecodeLimits, DecodeOptions, Message, MessageExt, MessageQueue, constants::*,
        encode_properties,
    },
    sysflag::pull::{build_pull_sys_flag, clear_commit_offset_flag},
};
use rocketmq_classic_core::api::extensions::AckStatus;
use rocketmq_classic_core::api::producer::{SendKind, SendMode};
use rocketmq_classic_core::api::{
    admin::{AdminQueryRequest, QueryResult, decode_message_id},
    consumer::PullStatus,
};
use rocketmq_classic_core::route::broker_vip_channel;
use rocketmq_classic_core::runtime::{
    ClientRuntime, ClientRuntimeManager, ConsumerOperation, ConsumerRuntime, GroupLease,
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::{
        ConsumeMessageDirectlyResult, ConsumeMessageResult, ConsumerPopProcessQueueInfo,
        ConsumerProcessQueueInfo, ConsumerRunningInfoBody, LockBatchRequestBody,
        MessageQueueAssignment, MessageRequestMode, UnlockBatchRequestBody,
    },
    codes::{request, response},
    header::{
        BoundaryType, ConsumerSendMsgBackRequestHeader, GetEarliestMsgStoretimeRequestHeader,
        GetMaxOffsetRequestHeader, GetMinOffsetRequestHeader, PopMessageRequestHeader,
        PullMessageRequestHeader, SearchOffsetRequestHeader, SendMessageRequestHeaderV1,
        ViewMessageRequestHeader,
    },
    heartbeat::{
        ConsumeFromWhere, ConsumeType, ConsumerData, ConsumerSubscriptionDataSet, SubscriptionData,
    },
};
use rocketmq_classic_remoting::client::RpcHook;
use tokio::time::Instant;

#[cfg(test)]
use std::sync::Mutex;

use crate::{
    consume::{
        ConcurrentConsumeService, ConsumeMessageContext, ConsumeMessageHook,
        ConsumeMessageHookRegistry, ConsumeRequest, ConsumeService, ConsumeSubmitError,
        ConsumeTraceHook, ORDERLY_LOCK_RETRY_DELAY_MILLIS, ORDERLY_LOCK_RPC_TIMEOUT_MILLIS,
        OrderlyConsumeRequest, OrderlyConsumeService, OrderlyDisposition, OrderlyLockSettings,
        OrderlySubmitError, orderly_disposition, suspend_delay_millis,
    },
    error::PushConsumerError,
    offset::{LocalFileOffsetStore, OffsetStore, OffsetStoreError, RemoteBrokerOffsetStore},
    offset::{OffsetRead, ReadOffsetType},
    pop::{PopProcessQueue, PopReceipt, PopStatus, process_pop_reply},
    process_queue::{ConsumeBatch, LockAttemptResult, OrderlyCompletion, ProcessQueue},
    pull::{PullAPIWrapper, build_subscription, resolve_pull_broker_address},
    rebalance::RebalanceService,
    stats::global_consumer_stats,
};

use super::{
    config::{PushConsumerConfig, parse_java_timestamp_millis},
    listener::{
        ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, ConsumeOrderlyContext,
        ConsumeOrderlyStatus, MessageListenerConcurrently, MessageListenerOrderly,
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
type ScriptedLockResult = Result<Option<HashSet<Option<MessageQueue>>>, String>;
#[cfg(test)]
type ScriptedLockWaiter = tokio::sync::oneshot::Receiver<ScriptedLockResult>;

#[cfg(test)]
#[derive(Default)]
struct ScriptedRetryTransport {
    broker_results: VecDeque<Result<(), String>>,
    retry_results: VecDeque<Result<(), String>>,
    broker_headers: Vec<ConsumerSendMsgBackRequestHeader>,
    broker_targets: Vec<(Option<String>, Option<String>)>,
    retry_messages: Vec<Message>,
}

#[cfg(test)]
#[derive(Default)]
struct ScriptedLockTransport {
    lock_results: VecDeque<ScriptedLockResult>,
    lock_waiters: VecDeque<ScriptedLockWaiter>,
    unlock_results: VecDeque<Result<(), String>>,
    lock_bodies: Vec<LockBatchRequestBody>,
    unlock_bodies: Vec<UnlockBatchRequestBody>,
    lock_requested: Arc<tokio::sync::Notify>,
}

#[derive(Default)]
struct OrderlyLockBatchResult {
    locked: HashSet<MessageQueue>,
    lock_times: HashMap<MessageQueue, (i64, Instant)>,
    responded: HashSet<MessageQueue>,
    uncertain: HashSet<MessageQueue>,
}

const BROKER_SUSPEND_MAX_TIME_MILLIS: i64 = 20_000;
const CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND: i64 = 30_000;
const CONSUME_RETRY_DELAY_MILLIS: i64 = 5_000;
const CONSUMER_SEND_BACK_TIMEOUT_MILLIS: u64 = 5_000;

struct PullContext {
    runtime: Arc<ClientRuntime>,
    offset_store: Arc<dyn OffsetStore>,
    scheduler: Arc<PullScheduler>,
    consume_service: ConsumeService,
}

struct PopConsumeBatchContext {
    process_queue: Arc<PopProcessQueue>,
    runtime: Arc<ClientRuntime>,
    queue: MessageQueue,
    route_topic: String,
    consumer_group: String,
}

struct DesiredAssignment {
    topic: String,
    all_queues: Option<HashSet<MessageQueue>>,
    assigned_queues: HashSet<MessageQueue>,
    pull_queues: HashSet<MessageQueue>,
    pop_queues: HashSet<MessageQueue>,
}

struct BrokerAssignmentQueues {
    assigned: HashSet<MessageQueue>,
    pull: HashSet<MessageQueue>,
    pop: HashSet<MessageQueue>,
}

fn partition_broker_assignments(
    assignments: impl IntoIterator<Item = Option<MessageQueueAssignment>>,
) -> Option<BrokerAssignmentQueues> {
    let mut partition = BrokerAssignmentQueues {
        assigned: HashSet::new(),
        pull: HashSet::new(),
        pop: HashSet::new(),
    };
    for assignment in assignments {
        let assignment = assignment?;
        let Some(queue) = assignment.message_queue.as_ref().map(SharedValue::snapshot) else {
            continue;
        };
        partition.assigned.insert(queue.clone());
        if assignment.mode == Some(MessageRequestMode::Pop) {
            partition.pop.insert(queue);
        } else {
            // Java treats a missing mode the same as PULL during rebalance.
            partition.pull.insert(queue);
        }
    }
    Some(partition)
}

#[derive(Debug, PartialEq, Eq)]
enum InitialOffsetPlan {
    Stored(i64),
    Fixed(i64),
    MaxOffset,
    SearchTimestamp(i64),
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn consume_timeout_exceeded(started_at: Instant, timeout_minutes: i64) -> bool {
    let elapsed_millis = i64::try_from(started_at.elapsed().as_millis()).unwrap_or(i64::MAX);
    let timeout_millis = timeout_minutes.wrapping_mul(60).wrapping_mul(1_000);
    elapsed_millis >= timeout_millis
}

fn orderly_consume_return_type(
    callback_return_type: &'static str,
    status: Option<ConsumeOrderlyStatus>,
    timed_out: bool,
) -> &'static str {
    if timed_out && status.is_some() {
        "TIME_OUT"
    } else {
        callback_return_type
    }
}

struct StartupServices {
    consume_service: ConsumeService,
    orderly_lock_settings: Option<OrderlyLockSettings>,
    renew_orderly_locks: bool,
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
        consume_service: ConsumeService,
        orderly_lock_settings: Option<OrderlyLockSettings>,
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

/// Java-compatible Push startup, assignment lifecycle, listener modes, and POP flow.
pub struct PushConsumer {
    config: PushConsumerConfig,
    manager: ClientRuntimeManager,
    rpc_hook: Option<Arc<dyn RpcHook>>,
    consume_message_hooks: ConsumeMessageHookRegistry,
    trace_dispatcher: OnceLock<Arc<rocketmq_classic_producer::TraceDispatcher>>,
    state: tokio::sync::Mutex<State>,
    offset_store: std::sync::Mutex<Option<Arc<dyn OffsetStore>>>,
    listener: RwLock<Option<PushMessageListener>>,
    pop_listener: RwLock<Option<PushMessageListener>>,
    subscription_config: RwLock<HashMap<String, String>>,
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    topic_queues: Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>>,
    process_queues: Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>>,
    pending_orderly_unlocks: Arc<RwLock<HashSet<MessageQueue>>>,
    rebalance_service: RebalanceService,
    offset_corrections_pending: Arc<RwLock<HashSet<MessageQueue>>>,
    pop_process_queues: Arc<RwLock<HashMap<MessageQueue, Arc<PopProcessQueue>>>>,
    pop_workers: std::sync::Mutex<HashMap<MessageQueue, tokio::task::JoinHandle<()>>>,
    pop_consume_workers: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pop_consume_semaphore: Arc<tokio::sync::Semaphore>,
    next_generation: AtomicU64,
    next_retry_message_id: AtomicU64,
    pull_api: PullAPIWrapper,
    paused: AtomicBool,
    consume_orderly: AtomicBool,
    consumer_start_timestamp: AtomicI64,
    consume_core_pool_size: AtomicI32,
    #[cfg(test)]
    startup_failure: Mutex<Option<StartupStage>>,
    #[cfg(test)]
    scripted_retry_transport: Mutex<ScriptedRetryTransport>,
    #[cfg(test)]
    scripted_lock_transport: Mutex<ScriptedLockTransport>,
    #[cfg(test)]
    rebalance_membership: Mutex<HashMap<String, VecDeque<Option<Vec<String>>>>>,
}

impl PushConsumer {
    pub fn new(config: PushConsumerConfig) -> Self {
        Self::with_manager(config, ClientRuntimeManager::shared_default())
    }

    fn with_manager(config: PushConsumerConfig, manager: ClientRuntimeManager) -> Self {
        Self::with_manager_and_hook(config, manager, None)
    }

    /// Creates a push consumer whose remoting requests use the supplied RPC hook.
    pub fn new_with_rpc_hook(
        config: PushConsumerConfig,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Self {
        Self::with_manager_and_hook(config, ClientRuntimeManager::shared_default(), rpc_hook)
    }

    fn with_manager_and_hook(
        config: PushConsumerConfig,
        manager: ClientRuntimeManager,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Self {
        let pop_consume_limit = usize::try_from(config.consume_thread_max().max(1)).unwrap_or(1);
        let consume_core_pool_size = config.consume_thread_min();
        let pull_api = PullAPIWrapper::new(config.unit_mode());
        let rebalance_service = RebalanceService::new(config.allocate_message_queue_strategy());
        Self {
            config,
            manager,
            rpc_hook,
            consume_message_hooks: ConsumeMessageHookRegistry::default(),
            trace_dispatcher: OnceLock::new(),
            state: tokio::sync::Mutex::new(State::Created),
            offset_store: std::sync::Mutex::new(None),
            listener: RwLock::new(None),
            pop_listener: RwLock::new(None),
            subscription_config: RwLock::new(HashMap::new()),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            topic_queues: Arc::new(RwLock::new(HashMap::new())),
            process_queues: Arc::new(RwLock::new(HashMap::new())),
            pending_orderly_unlocks: Arc::new(RwLock::new(HashSet::new())),
            rebalance_service,
            offset_corrections_pending: Arc::new(RwLock::new(HashSet::new())),
            pop_process_queues: Arc::new(RwLock::new(HashMap::new())),
            pop_workers: std::sync::Mutex::new(HashMap::new()),
            pop_consume_workers: std::sync::Mutex::new(Vec::new()),
            pop_consume_semaphore: Arc::new(tokio::sync::Semaphore::new(pop_consume_limit)),
            next_generation: AtomicU64::new(1),
            next_retry_message_id: AtomicU64::new(0),
            pull_api,
            paused: AtomicBool::new(false),
            consume_orderly: AtomicBool::new(false),
            consumer_start_timestamp: AtomicI64::new(0),
            consume_core_pool_size: AtomicI32::new(consume_core_pool_size),
            #[cfg(test)]
            startup_failure: Mutex::new(None),
            #[cfg(test)]
            scripted_retry_transport: Mutex::new(ScriptedRetryTransport::default()),
            #[cfg(test)]
            scripted_lock_transport: Mutex::new(ScriptedLockTransport::default()),
            #[cfg(test)]
            rebalance_membership: Mutex::new(HashMap::new()),
        }
    }

    pub fn config(&self) -> &PushConsumerConfig {
        &self.config
    }

    /// Returns the message trace dispatcher after trace initialization.
    ///
    /// This corresponds to Java `DefaultMQPushConsumer.getTraceDispatcher()`.
    pub fn trace_dispatcher(&self) -> Option<Arc<rocketmq_classic_producer::TraceDispatcher>> {
        self.trace_dispatcher.get().cloned()
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
        // Java updates the facade and implementation listener references even
        // after startup; already-created consume workers retain their listener.
        *write_lock(&self.listener) = Some(listener);
        Ok(())
    }

    /// Registers a Java-compatible hook around each delivered consume batch.
    pub fn register_consume_message_hook(&self, hook: Arc<dyn ConsumeMessageHook>) {
        self.consume_message_hooks.register(hook);
    }

    fn new_consume_message_context(
        &self,
        messages: Vec<MessageExt>,
        queue: MessageQueue,
    ) -> ConsumeMessageContext {
        let mut client_config = self.config.client_config().clone();
        let namespace = client_config
            .get_namespace()
            .expect("the consumer namespace was validated before consume workers start")
            .map(str::to_owned);
        let consumer_group = client_config
            .with_namespace(Some(self.config.consumer_group()))
            .expect("the consumer namespace was validated before consume workers start")
            .unwrap_or_else(|| self.config.consumer_group().to_owned());
        ConsumeMessageContext::new(consumer_group, Some(messages), queue, namespace)
            .with_properties(HashMap::new())
    }

    /// Sets the configured message listener. This mirrors Java's
    /// `DefaultMQPushConsumer.setMessageListener` entry point.
    pub async fn set_message_listener(
        &self,
        listener: PushMessageListener,
    ) -> Result<(), PushConsumerError> {
        self.register_message_listener(listener).await
    }

    pub fn get_message_listener(&self) -> Option<PushMessageListener> {
        read_lock(&self.listener).clone()
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
        let runtime = {
            let state = self.state.lock().await;
            match &*state {
                State::Created => None,
                State::Started { runtime, .. } => {
                    Some(runtime.upgrade().ok_or(PushConsumerError::NotStarted)?)
                }
                State::Shutdown => return Err(PushConsumerError::Shutdown),
            }
        };
        let mut client_config = self.config.client_config().clone();
        let topic = client_config.with_namespace(Some(topic))?.ok_or(
            PushConsumerError::InvalidSubscription("topic is empty".to_owned()),
        )?;
        let subscription = build_subscription(&topic, Some(expression), expression_type)
            .map_err(|error| PushConsumerError::InvalidSubscription(error.to_string()))?;
        write_lock(&self.subscriptions).insert(topic, subscription);
        if let Some(runtime) = runtime {
            let heartbeat = runtime.send_heartbeat_to_all_brokers().await;
            runtime.rebalance_immediately();
            heartbeat?;
        }
        Ok(())
    }

    pub async fn unsubscribe(&self, topic: &str) -> Result<bool, PushConsumerError> {
        let state = self.state.lock().await;
        let runtime = match &*state {
            State::Created => None,
            State::Started { runtime, .. } => runtime.upgrade(),
            State::Shutdown => return Err(PushConsumerError::Shutdown),
        };
        drop(state);
        // Java's facade passes the argument through without applying the
        // namespace here, unlike subscribe().
        let removed = write_lock(&self.subscriptions).remove(topic).is_some();
        if removed {
            if let Some(runtime) = runtime {
                runtime.rebalance_immediately();
            }
        }
        Ok(removed)
    }

    pub fn subscriptions(&self) -> HashMap<String, SubscriptionData> {
        read_lock(&self.subscriptions).clone()
    }

    /// Returns the legacy `topic -> tag expression` configuration map.
    pub fn get_subscription(&self) -> HashMap<String, String> {
        read_lock(&self.subscription_config).clone()
    }

    /// Replaces the legacy subscription configuration map, applying the
    /// consumer namespace to each topic as the Java setter does.
    pub async fn set_subscription(
        &self,
        subscriptions: HashMap<String, String>,
    ) -> Result<(), PushConsumerError> {
        let mut client_config = self.config.client_config().clone();
        let mut namespaced = HashMap::with_capacity(subscriptions.len());
        for (topic, expression) in subscriptions {
            let topic = client_config.with_namespace(Some(&topic))?.ok_or_else(|| {
                PushConsumerError::InvalidSubscription("topic is empty".to_owned())
            })?;
            namespaced.insert(topic, expression);
        }
        *write_lock(&self.subscription_config) = namespaced;
        Ok(())
    }

    /// Searches a queue by Java's default lower timestamp boundary.
    pub async fn search_offset(
        &self,
        input_queue: &MessageQueue,
        timestamp_millis: i64,
    ) -> Result<i64, PushConsumerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let header = SearchOffsetRequestHeader {
            topic: queue
                .topic()
                .ok_or(PushConsumerError::InvalidQueue)?
                .to_owned(),
            queue_id: queue.queue_id(),
            timestamp: timestamp_millis,
            boundary_type: Some(BoundaryType::Lower),
            broker_name: Some(broker_name),
            ..SearchOffsetRequestHeader::default()
        };
        Ok(runtime
            .api()
            .search_offset(&address, &header, deadline)
            .await?)
    }

    pub async fn get_max_offset(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let header = GetMaxOffsetRequestHeader {
            topic: queue
                .topic()
                .ok_or(PushConsumerError::InvalidQueue)?
                .to_owned(),
            queue_id: queue.queue_id(),
            committed: true,
            broker_name: Some(broker_name),
        };
        Ok(runtime
            .api()
            .get_max_offset(&address, &header, deadline)
            .await?)
    }

    pub async fn get_min_offset(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let header = GetMinOffsetRequestHeader {
            topic: queue
                .topic()
                .ok_or(PushConsumerError::InvalidQueue)?
                .to_owned(),
            queue_id: queue.queue_id(),
            broker_name: Some(broker_name),
        };
        Ok(runtime
            .api()
            .get_min_offset(&address, &header, deadline)
            .await?)
    }

    pub async fn get_earliest_msg_store_time(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let (runtime, queue, broker_name, address, deadline) =
            self.prepare_admin_queue(input_queue).await?;
        let header = GetEarliestMsgStoretimeRequestHeader {
            topic: queue
                .topic()
                .ok_or(PushConsumerError::InvalidQueue)?
                .to_owned(),
            queue_id: queue.queue_id(),
            broker_name: Some(broker_name),
        };
        Ok(runtime
            .api()
            .get_earliest_msg_storetime(&address, &header, deadline)
            .await?)
    }

    pub async fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        _attributes: Option<&BTreeMap<String, String>>,
    ) -> Result<(), PushConsumerError> {
        self.create_topic_with_sys_flag(key, new_topic, queue_num, 0, None)
            .await
    }

    pub async fn create_topic_with_sys_flag(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        _attributes: Option<&BTreeMap<String, String>>,
    ) -> Result<(), PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(new_topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidAdminTopic(
                "The specified topic is blank.".into(),
            ))?;
        crate::admin::validate_topic(&topic).map_err(PushConsumerError::InvalidAdminTopic)?;
        crate::admin::create_topic(&context.runtime, key, &topic, queue_num, topic_sys_flag)
            .await?;
        Ok(())
    }

    pub async fn fetch_subscribe_message_queues(
        &self,
        input_topic: &str,
    ) -> Result<BTreeSet<MessageQueue>, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(input_topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidAdminTopic(
                "The specified topic is blank.".into(),
            ))?;
        let timeout = Duration::from_millis(
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64,
        );
        crate::admin::fetch_subscribe_queues(&context.runtime, &topic, timeout)
            .await?
            .into_iter()
            .map(|queue| {
                let topic = client_config.without_namespace(queue.topic())?;
                Ok(MessageQueue::new_nullable(
                    topic,
                    queue.broker_name().map(str::to_owned),
                    queue.queue_id(),
                ))
            })
            .collect::<Result<BTreeSet<_>, rocketmq_classic_core::config::ConfigError>>()
            .map_err(PushConsumerError::from)
    }

    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<QueryResult, PushConsumerError> {
        self.query_messages(topic, key, max_num, begin_timestamp, end_timestamp, false)
            .await
    }

    pub async fn query_message_by_unique_key(
        &self,
        topic: &str,
        unique_key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<QueryResult, PushConsumerError> {
        self.query_messages(
            topic,
            unique_key,
            max_num,
            begin_timestamp,
            end_timestamp,
            true,
        )
        .await
    }

    /// Views a message by Java's physical offset message ID.
    pub async fn view_message(
        &self,
        topic: &str,
        message_id: &str,
    ) -> Result<MessageExt, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let decoded = decode_message_id(message_id).map_err(|_| {
            crate::error::PushConsumerError::Rpc(rocketmq_classic_core::error::RpcError::Response {
                request_code: request::VIEW_MESSAGE_BY_ID,
                peer: topic.to_owned(),
                code: response::NO_MESSAGE,
                remark: Some("query message by id finished, but no message.".into()),
            })
        })?;
        let header = ViewMessageRequestHeader {
            topic: Some(topic.to_owned()),
            offset: decoded.offset,
        };
        let timeout_millis =
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64;
        Ok(context
            .runtime
            .api()
            .view_message(
                &decoded.address.to_string(),
                &header,
                Instant::now() + Duration::from_millis(timeout_millis),
            )
            .await?)
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_messages(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
        unique_key: bool,
    ) -> Result<QueryResult, PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?;
        let mut query = if unique_key {
            AdminQueryRequest::by_unique_key(&topic, key, max_num, begin_timestamp, end_timestamp)
        } else {
            AdminQueryRequest::by_key(&topic, key, max_num, begin_timestamp, end_timestamp)
        };
        query.logical_message_queue = topic.starts_with("%LMQ%");
        let timeout_millis =
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64;
        let deadline = Instant::now() + Duration::from_millis(timeout_millis).saturating_mul(4);
        let mut result = context.runtime.query_message(&query, deadline).await?;
        for message in &mut result.message_list {
            if let Some(topic) = client_config.without_namespace(Some(message.message().topic()))? {
                message.message_mut().set_topic(topic);
            }
        }
        Ok(result)
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

    pub(crate) async fn reset_consumer_offsets(
        &self,
        topic: &str,
        offsets: Vec<(MessageQueue, Option<i64>)>,
    ) -> Result<(), PushConsumerError> {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        self.suspend().await;
        let result = async {
            for (queue, offset) in offsets {
                let Some(offset) = offset.filter(|_| queue.topic() == Some(topic)) else {
                    continue;
                };
                context
                    .offset_store
                    .update_and_freeze_offset(&queue, offset);
                let process_queue = { read_lock(&self.process_queues).get(&queue).cloned() };
                if let Some(process_queue) = process_queue {
                    let generation = process_queue.generation();
                    process_queue
                        .drop_assignment(generation)
                        .map_err(|error| PushConsumerError::PullScheduler(error.to_string()))?;
                    context.scheduler.cancel(&queue, generation);
                    process_queue.clear();
                    if !self
                        .remove_process_queue_safely(&context, &queue, &process_queue)
                        .await
                    {
                        return Err(PushConsumerError::ProcessQueueRemovalPending(
                            queue.to_string(),
                        ));
                    }
                } else {
                    context.offset_store.persist(&queue).await?;
                }
                // Keep the reset value available for the next assignment. Any
                // old callback has already lost its ProcessQueue generation.
                context
                    .offset_store
                    .update_and_freeze_offset(&queue, offset);
            }
            Ok(())
        }
        .await;
        self.resume().await;
        result
    }

    pub(crate) fn running_info_body(
        &self,
        runtime: &ClientRuntime,
        jstack: Option<String>,
        offset_store: Option<Arc<dyn OffsetStore>>,
    ) -> ConsumerRunningInfoBody {
        let mut body = ConsumerRunningInfoBody {
            properties: crate::admin::push_running_info_properties(
                &self.config,
                runtime,
                self.consumer_start_timestamp.load(Ordering::Acquire),
                self.consume_orderly.load(Ordering::Acquire),
                self.consume_core_pool_size.load(Ordering::Acquire),
            ),
            subscription_set: read_lock(&self.subscriptions).values().cloned().collect(),
            jstack,
            ..ConsumerRunningInfoBody::default()
        };

        body.status_table = body
            .subscription_set
            .iter()
            .filter_map(|subscription| {
                subscription.topic.as_ref().map(|topic| {
                    (
                        topic.clone(),
                        global_consumer_stats().status(
                            runtime.client_id(),
                            self.config.consumer_group(),
                            topic,
                        ),
                    )
                })
            })
            .collect();

        let offset_table = offset_store
            .map(|offset_store| offset_store.clone_offset_table(None))
            .unwrap_or_default();
        let process_queues = read_lock(&self.process_queues)
            .iter()
            .map(|(queue, process_queue)| {
                (
                    queue.clone(),
                    process_queue.clone(),
                    process_queue.generation(),
                )
            })
            .collect::<Vec<_>>();
        body.mq_table = process_queues
            .into_iter()
            .filter_map(|(queue, process_queue, generation)| {
                let snapshot = process_queue.snapshot(generation).ok()?;
                Some((
                    queue.clone(),
                    ConsumerProcessQueueInfo {
                        commit_offset: offset_table.get(&queue).copied().unwrap_or(-1),
                        cached_msg_min_offset: snapshot.minimum_offset.unwrap_or(0),
                        cached_msg_max_offset: snapshot.maximum_offset.unwrap_or(0),
                        cached_msg_count: i32::try_from(snapshot.cached_count).unwrap_or(i32::MAX),
                        cached_msg_size_in_mi_b: i32::try_from(
                            snapshot.cached_bytes / (1024 * 1024),
                        )
                        .unwrap_or(i32::MAX),
                        locked: snapshot.locked,
                        try_unlock_times: i64::try_from(snapshot.try_unlock_times)
                            .unwrap_or(i64::MAX),
                        last_lock_timestamp: snapshot.last_lock_timestamp_millis,
                        droped: snapshot.dropped,
                        last_pull_timestamp: snapshot.last_pull_timestamp_millis,
                        last_consume_timestamp: snapshot.last_consume_timestamp_millis,
                        ..ConsumerProcessQueueInfo::default()
                    },
                ))
            })
            .collect();

        let pop_process_queues = read_lock(&self.pop_process_queues)
            .iter()
            .map(|(queue, process_queue)| (queue.clone(), process_queue.clone()))
            .collect::<Vec<_>>();
        body.mq_pop_table = pop_process_queues
            .into_iter()
            .map(|(queue, process_queue)| {
                let wait_ack_count = process_queue.wait_ack_count();
                (
                    queue,
                    ConsumerPopProcessQueueInfo {
                        wait_ack_count: i32::try_from(wait_ack_count).unwrap_or_else(|_| {
                            if wait_ack_count.is_negative() {
                                i32::MIN
                            } else {
                                i32::MAX
                            }
                        }),
                        droped: process_queue.is_dropped(),
                        last_pop_timestamp: process_queue.last_pop_timestamp(),
                    },
                )
            })
            .collect();

        body
    }

    pub(crate) async fn consume_message_directly(
        &self,
        mut message: MessageExt,
        broker_name: Option<String>,
    ) -> Option<ConsumeMessageDirectlyResult> {
        let context = self.pull_context().await?;
        let started = std::time::Instant::now();
        let orderly = matches!(&context.consume_service, ConsumeService::Orderly(_));
        let queue = MessageQueue::new_nullable(
            Some(message.message().topic().to_owned()),
            broker_name.clone(),
            message.queue_id(),
        );
        if !orderly {
            message.set_broker_name(broker_name);
        }
        let mut messages = vec![message];
        self.reset_retry_and_namespace_for_orderly(&mut messages);
        if let Some(message) = messages.first_mut() {
            let offset_message_id = message.message_id().map(str::to_owned);
            let message_id = message
                .message()
                .properties()
                .get(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                .cloned()
                .or_else(|| offset_message_id.clone());
            message.set_offset_message_id(offset_message_id);
            message.set_message_id(message_id);
        }

        let mut result = ConsumeMessageDirectlyResult::default();
        match context.consume_service.listener() {
            PushMessageListener::Concurrently(listener) if !orderly => {
                result.set_order(false);
                result.set_auto_commit(true);
                let callback = tokio::spawn(
                    listener.consume_message(messages, ConsumeConcurrentlyContext::new(queue)),
                );
                match callback.await {
                    Ok((Some(ConsumeConcurrentlyStatus::ConsumeSuccess), _)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrSuccess));
                    }
                    Ok((Some(ConsumeConcurrentlyStatus::ReconsumeLater), _)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrLater));
                    }
                    Ok((None, _)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrReturnNull));
                    }
                    Err(error) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrThrowException));
                        result.set_remark(Some(error.to_string()));
                    }
                }
            }
            PushMessageListener::Orderly(listener) if orderly => {
                result.set_order(true);
                let callback = tokio::spawn(
                    listener.consume_message(messages, ConsumeOrderlyContext::new(queue)),
                );
                match callback.await {
                    Ok((Some(ConsumeOrderlyStatus::Success), context)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrSuccess));
                        result.set_auto_commit(context.auto_commit());
                    }
                    Ok((Some(ConsumeOrderlyStatus::Rollback), context)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrRollback));
                        result.set_auto_commit(context.auto_commit());
                    }
                    Ok((Some(ConsumeOrderlyStatus::Commit), context)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrCommit));
                        result.set_auto_commit(context.auto_commit());
                    }
                    Ok((Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment), context)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrLater));
                        result.set_auto_commit(context.auto_commit());
                    }
                    Ok((None, context)) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrReturnNull));
                        result.set_auto_commit(context.auto_commit());
                    }
                    Err(error) => {
                        result.set_consume_result(Some(ConsumeMessageResult::CrThrowException));
                        result.set_remark(Some(error.to_string()));
                    }
                }
            }
            _ => return None,
        }
        result.set_spent_time_mills(started.elapsed().as_millis().min(i64::MAX as u128) as i64);
        Some(result)
    }

    pub fn is_pause(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    pub fn is_consume_orderly(&self) -> bool {
        self.consume_orderly.load(Ordering::Acquire)
    }

    /// Updates the active concurrent-consume worker count when the requested
    /// value satisfies Java 5.5.1's `updateCorePoolSize` bounds.
    pub async fn update_core_pool_size(&self, core_pool_size: i32) -> bool {
        let state = self.state.lock().await;
        match &*state {
            State::Started {
                consume_service, ..
            } => {
                let updated = consume_service.update_core_pool_size(core_pool_size);
                if updated {
                    self.consume_core_pool_size
                        .store(core_pool_size, Ordering::Release);
                }
                updated
            }
            State::Created | State::Shutdown => false,
        }
    }

    /// Sends a consumed message back to its originating Broker and uses the
    /// Java-compatible retry-topic fallback if the Broker rejects the request.
    pub async fn send_message_back(
        &self,
        message: &MessageExt,
        delay_level: i32,
    ) -> Result<(), PushConsumerError> {
        self.send_message_back_with_broker(message, delay_level, None)
            .await
    }

    /// Sends a consumed message back to the named Broker, matching Java's
    /// three-argument `sendMessageBack` overload.
    pub async fn send_message_back_to_broker(
        &self,
        message: &MessageExt,
        delay_level: i32,
        broker_name: Option<&str>,
    ) -> Result<(), PushConsumerError> {
        self.send_message_back_with_broker(message, delay_level, Some(broker_name))
            .await
    }

    async fn send_message_back_with_broker(
        &self,
        message: &MessageExt,
        delay_level: i32,
        broker_name_override: Option<Option<&str>>,
    ) -> Result<(), PushConsumerError> {
        let runtime = {
            let state = self.state.lock().await;
            match &*state {
                State::Started { runtime, .. } => {
                    runtime.upgrade().ok_or(PushConsumerError::NotStarted)?
                }
                State::Created => return Err(PushConsumerError::NotStarted),
                State::Shutdown => return Err(PushConsumerError::Shutdown),
            }
        };
        let effective_broker = match broker_name_override {
            Some(broker_name) => broker_name.map(str::to_owned),
            None => message.broker_name().map(str::to_owned),
        };
        let queue = MessageQueue::new_nullable_broker(
            message.message().topic().to_owned(),
            effective_broker,
            message.queue_id(),
        );
        if self
            .send_message_back_for_consume(
                &runtime,
                &queue,
                message,
                delay_level,
                broker_name_override,
                false,
            )
            .await
        {
            Ok(())
        } else {
            Err(PushConsumerError::SendMessageBackFailed)
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
        self.activate_process_queue_with_lock(input_queue, initial_offset, None)
            .await
    }

    async fn activate_process_queue_with_lock(
        &self,
        input_queue: &MessageQueue,
        initial_offset: i64,
        orderly_lock_instant: Option<Instant>,
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
            return Err(PushConsumerError::ProcessQueueRemovalPending(
                queue.to_string(),
            ));
        }
        let generation = self
            .next_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| PushConsumerError::GenerationExhausted)?;
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), generation));
        {
            let mut queues = write_lock(&self.process_queues);
            if let Some(existing) = queues.get(&queue) {
                if existing
                    .snapshot(existing.generation())
                    .is_ok_and(|snapshot| !snapshot.dropped)
                {
                    return Ok(existing.clone());
                }
                return Err(PushConsumerError::ProcessQueueRemovalPending(
                    queue.to_string(),
                ));
            }
            queues.insert(queue.clone(), process_queue.clone());
        }
        if let Some(lock_instant) = orderly_lock_instant {
            let _ = process_queue.set_lock_state_at(
                generation,
                true,
                epoch_millis(),
                Some(lock_instant),
            );
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
            scheduler.cancel(&queue, generation);
            let mut queues = write_lock(&self.process_queues);
            if queues
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
            {
                queues.remove(&queue);
                offset_store.remove_offset(&queue);
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

    async fn current_orderly_lock_settings(&self) -> Option<OrderlyLockSettings> {
        match &*self.state.lock().await {
            State::Started {
                orderly_lock_settings,
                ..
            } => *orderly_lock_settings,
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

        let orderly_lock_settings = self.current_orderly_lock_settings().await;
        if orderly_lock_settings.is_some()
            && self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
            && !process_queue
                .is_lock_fresh(
                    generation,
                    orderly_lock_settings
                        .expect("orderly lock settings")
                        .max_live_time_millis,
                )
                .unwrap_or(false)
        {
            let locked = self
                .lock_orderly_process_queues(&context.runtime, std::slice::from_ref(&process_queue))
                .await
                .contains(&queue);
            let delay = if locked {
                10
            } else {
                ORDERLY_LOCK_RETRY_DELAY_MILLIS
            };
            let _ = context.scheduler.schedule(request, delay);
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
        let pull_started = Instant::now();
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
        if processed.result.status == PullStatus::Found {
            let message_count = processed.messages.as_ref().map_or(0, Vec::len);
            global_consumer_stats().record_pull(
                context.runtime.client_id(),
                self.config.consumer_group(),
                topic,
                pull_started.elapsed(),
                message_count,
            );
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
                    self.submit_ready_messages(&context, process_queue, generation);
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
        let ConsumeService::Concurrent(consume_service) = &context.consume_service else {
            return;
        };
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
        match consume_service.try_submit(request) {
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

    fn submit_ready_messages(
        self: &Arc<Self>,
        context: &PullContext,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
    ) {
        match &context.consume_service {
            ConsumeService::Concurrent(_) => {
                self.submit_consume_batch(context, process_queue, generation);
            }
            ConsumeService::Orderly(service) => {
                let request = OrderlyConsumeRequest {
                    process_queue,
                    generation,
                };
                match service.try_submit(request) {
                    Ok(()) => {}
                    Err(OrderlySubmitError::Full(request)) => {
                        self.schedule_orderly_work_retry(
                            context.runtime.clone(),
                            request.process_queue,
                            request.generation,
                            100,
                        );
                    }
                    Err(OrderlySubmitError::Closed(request)) => {
                        let _ = request
                            .process_queue
                            .release_consume_dispatch(request.generation);
                    }
                }
            }
        }
    }

    fn schedule_orderly_work_retry(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
        delay_millis: i64,
    ) {
        let consumer = Arc::downgrade(self);
        let task_process_queue = process_queue.clone();
        let task = async move {
            let Some(consumer) = consumer.upgrade() else {
                return;
            };
            let Some(context) = consumer.pull_context().await else {
                return;
            };
            consumer.submit_ready_messages(&context, process_queue, generation);
        };
        if let Err(error) = runtime.execute_task_later(task, delay_millis) {
            let _ = task_process_queue.release_consume_dispatch(generation);
            tracing::error!(generation, error = %error, "failed to schedule orderly consume retry");
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
        let mut consume_context =
            self.new_consume_message_context(messages, request.batch.queue().clone());
        self.consume_message_hooks
            .execute_before(&mut consume_context);
        let callback_context = ConsumeConcurrentlyContext::new(request.batch.queue().clone());
        let fallback_context = callback_context.clone();
        let listener_messages = consume_context.messages.clone().unwrap_or_default();
        let callback_started = Instant::now();
        let mut callback = AbortOnDrop(tokio::spawn(async move {
            listener
                .consume_message(listener_messages, callback_context)
                .await
        }));
        // Java classifies consume timeout after the listener returns. The
        // timeout is an observation for hooks and statistics, not a reason to
        // cancel the callback.
        let callback_result = (&mut callback.0).await;
        let callback_timed_out =
            consume_timeout_exceeded(callback_started, self.config.consume_timeout_minutes());
        let (status, returned_context, consume_context_type) = match callback_result {
            Ok((status, returned_context)) => {
                let context_type = if status.is_none() {
                    "RETURNNULL"
                } else if callback_timed_out {
                    "TIME_OUT"
                } else {
                    match status {
                        Some(ConsumeConcurrentlyStatus::ConsumeSuccess) => "SUCCESS",
                        Some(ConsumeConcurrentlyStatus::ReconsumeLater) => "FAILED",
                        None => unreachable!("null status was handled above"),
                    }
                };
                (status, returned_context, context_type)
            }
            Err(error) => {
                tracing::error!(
                    topic = ?request.batch.queue().topic(),
                    queue_id = request.batch.queue().queue_id(),
                    generation,
                    error = %error,
                    "push concurrent listener failed"
                );
                (None, fallback_context, "EXCEPTION")
            }
        };
        consume_context
            .properties
            .get_or_insert_with(HashMap::new)
            .insert(
                "CONSUME_CONTEXT_TYPE".to_owned(),
                consume_context_type.to_owned(),
            );
        consume_context.status = Some(
            match status {
                Some(ConsumeConcurrentlyStatus::ConsumeSuccess) => "CONSUME_SUCCESS",
                Some(ConsumeConcurrentlyStatus::ReconsumeLater) | None => "RECONSUME_LATER",
            }
            .to_owned(),
        );
        consume_context.success = status == Some(ConsumeConcurrentlyStatus::ConsumeSuccess);
        consume_context.access_channel = self.config.client_config().access_channel();
        self.consume_message_hooks
            .execute_after(&mut consume_context);
        if let Some(topic) = request.batch.queue().topic() {
            global_consumer_stats().record_consume_rt(
                context.runtime.client_id(),
                self.config.consumer_group(),
                topic,
                callback_started.elapsed(),
            );
        }
        self.complete_consume_request(request, context, status, returned_context)
            .await;
    }

    pub(crate) async fn run_orderly_consume_request(
        self: Arc<Self>,
        request: OrderlyConsumeRequest,
        listener: Arc<dyn MessageListenerOrderly>,
    ) {
        let process_queue = request.process_queue;
        let generation = request.generation;
        let Some(context) = self.pull_context().await else {
            let _ = process_queue.release_consume_dispatch(generation);
            return;
        };
        let gate = process_queue.orderly_consume_gate();
        let _consume_guard = gate.lock_owned().await;
        if !process_queue
            .snapshot(generation)
            .is_ok_and(|snapshot| !snapshot.dropped)
        {
            return;
        }

        let queue = process_queue.queue().clone();
        let orderly_settings = self.current_orderly_lock_settings().await;
        let orderly_clustering = orderly_settings.is_some()
            && self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering;
        let started_at = Instant::now();
        loop {
            if !process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                return;
            }
            if orderly_settings
                .is_some_and(|settings| settings.max_continuous_consume_exceeded(started_at))
            {
                self.schedule_orderly_work_retry(
                    context.runtime.clone(),
                    process_queue.clone(),
                    generation,
                    10,
                );
                return;
            }

            if orderly_clustering {
                let Some(settings) = orderly_settings else {
                    return;
                };
                let lock_fresh = process_queue
                    .is_lock_fresh(generation, settings.max_live_time_millis)
                    .unwrap_or(false);
                if !lock_fresh {
                    self.schedule_orderly_lock_retry(
                        context.runtime.clone(),
                        process_queue.clone(),
                        generation,
                        10,
                    );
                    return;
                }
            }

            let batch = match process_queue.take_batch(
                generation,
                self.config.consume_message_batch_max_size().max(1) as usize,
            ) {
                Ok(Some(batch)) => batch,
                Ok(None) | Err(_) => return,
            };
            let mut messages = batch
                .messages()
                .iter()
                .map(|message| message.as_ref().clone())
                .collect::<Vec<_>>();
            self.reset_retry_and_namespace_for_orderly(&mut messages);
            let mut consume_context =
                self.new_consume_message_context(messages.clone(), queue.clone());
            self.consume_message_hooks
                .execute_before(&mut consume_context);
            messages = consume_context.messages.clone().unwrap_or_default();
            let listener_messages = messages.clone();
            let callback_context = ConsumeOrderlyContext::new(queue.clone());
            let fallback_context = callback_context.clone();
            let listener = listener.clone();
            let callback_started = Instant::now();
            let mut callback = AbortOnDrop(tokio::spawn(async move {
                listener
                    .consume_message(listener_messages, callback_context)
                    .await
            }));
            // Java's consumeTimeout only classifies a completed callback for
            // statistics; it does not interrupt the listener. Keep the queue
            // gate held until the callback actually returns.
            let (status, returned_context, consume_context_type) = match (&mut callback.0).await {
                Ok((status, returned_context)) => {
                    let context_type = match status {
                        Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment) => "FAILED",
                        None => "RETURNNULL",
                        Some(_) => "SUCCESS",
                    };
                    (status, returned_context, context_type)
                }
                Err(error) => {
                    tracing::error!(
                        topic = ?queue.topic(),
                        queue_id = queue.queue_id(),
                        generation,
                        error = %error,
                        "push orderly listener failed"
                    );
                    (None, fallback_context, "EXCEPTION")
                }
            };

            let consume_timed_out =
                consume_timeout_exceeded(callback_started, self.config.consume_timeout_minutes());
            let consume_context_type =
                orderly_consume_return_type(consume_context_type, status, consume_timed_out);
            consume_context
                .properties
                .get_or_insert_with(HashMap::new)
                .insert(
                    "CONSUME_CONTEXT_TYPE".to_owned(),
                    consume_context_type.to_owned(),
                );
            consume_context.status = Some(
                match status {
                    Some(ConsumeOrderlyStatus::Success) => "SUCCESS",
                    Some(ConsumeOrderlyStatus::Rollback) => "ROLLBACK",
                    Some(ConsumeOrderlyStatus::Commit) => "COMMIT",
                    Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment) | None => {
                        "SUSPEND_CURRENT_QUEUE_A_MOMENT"
                    }
                }
                .to_owned(),
            );
            consume_context.success = matches!(
                status,
                Some(ConsumeOrderlyStatus::Success | ConsumeOrderlyStatus::Commit)
            );
            consume_context.access_channel = self.config.client_config().access_channel();
            self.consume_message_hooks
                .execute_after(&mut consume_context);
            if let Some(topic) = queue.topic() {
                global_consumer_stats().record_consume_rt(
                    context.runtime.client_id(),
                    self.config.consumer_group(),
                    topic,
                    callback_started.elapsed(),
                );
            }

            if !process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                return;
            }
            if orderly_clustering {
                let Some(settings) = orderly_settings else {
                    return;
                };
                if !process_queue
                    .is_lock_fresh(generation, settings.max_live_time_millis)
                    .unwrap_or(false)
                {
                    let completion = process_queue.complete_orderly_batch(
                        generation,
                        batch.id(),
                        OrderlyCompletion::RollbackAll,
                    );
                    if completion.is_ok() {
                        self.schedule_orderly_lock_retry(
                            context.runtime.clone(),
                            process_queue.clone(),
                            generation,
                            10,
                        );
                    }
                    return;
                }
            }

            let status = status.unwrap_or(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
            if let Some(topic) = queue.topic() {
                let message_count = batch.offsets().len();
                let (acknowledged, failed) = if returned_context.auto_commit() {
                    match status {
                        ConsumeOrderlyStatus::Success
                        | ConsumeOrderlyStatus::Commit
                        | ConsumeOrderlyStatus::Rollback => (Some(message_count), None),
                        ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                            (None, Some(message_count))
                        }
                    }
                } else {
                    match status {
                        ConsumeOrderlyStatus::Success => (Some(message_count), None),
                        ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                            (None, Some(message_count))
                        }
                        ConsumeOrderlyStatus::Commit | ConsumeOrderlyStatus::Rollback => {
                            (None, None)
                        }
                    }
                };
                global_consumer_stats().record_orderly_result(
                    context.runtime.client_id(),
                    self.config.consumer_group(),
                    topic,
                    acknowledged,
                    failed,
                );
            }
            let retry_required = if status == ConsumeOrderlyStatus::SuspendCurrentQueueAMoment {
                self.check_orderly_reconsume_times(
                    &context,
                    &queue,
                    &process_queue,
                    &batch,
                    &messages,
                    generation,
                )
                .await
            } else {
                false
            };
            let disposition =
                orderly_disposition(returned_context.auto_commit(), Some(status), retry_required);
            let completion = match disposition {
                OrderlyDisposition::Commit => OrderlyCompletion::CommitAll,
                OrderlyDisposition::Hold => OrderlyCompletion::HoldCurrent,
                OrderlyDisposition::Rollback => OrderlyCompletion::RollbackAll,
                OrderlyDisposition::Retry => OrderlyCompletion::RequeueCurrent,
            };
            let outcome =
                match process_queue.complete_orderly_batch(generation, batch.id(), completion) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        tracing::debug!(
                            topic = ?queue.topic(),
                            queue_id = queue.queue_id(),
                            generation,
                            error = %error,
                            "orderly consume result no longer owns its queue batch"
                        );
                        return;
                    }
                };
            if disposition == OrderlyDisposition::Commit {
                if let Some(offset) = outcome.next_commit_offset {
                    if process_queue
                        .snapshot(generation)
                        .is_ok_and(|snapshot| !snapshot.dropped)
                    {
                        context.offset_store.update_offset(&queue, offset, false);
                    }
                }
            }

            match disposition {
                OrderlyDisposition::Rollback => {
                    self.schedule_orderly_work_retry(
                        context.runtime.clone(),
                        process_queue.clone(),
                        generation,
                        suspend_delay_millis(
                            returned_context.suspend_current_queue_time_millis(),
                            self.config.suspend_current_queue_time_millis(),
                        ) as i64,
                    );
                    return;
                }
                OrderlyDisposition::Retry => {
                    self.schedule_orderly_work_retry(
                        context.runtime.clone(),
                        process_queue.clone(),
                        generation,
                        suspend_delay_millis(
                            returned_context.suspend_current_queue_time_millis(),
                            self.config.suspend_current_queue_time_millis(),
                        ) as i64,
                    );
                    return;
                }
                OrderlyDisposition::Commit | OrderlyDisposition::Hold => {
                    if !outcome.has_ready_messages {
                        return;
                    }
                }
            }
        }
    }

    fn reset_retry_and_namespace_for_orderly(&self, messages: &mut [MessageExt]) {
        let mut config = self.config.client_config().clone();
        let consumer_group = config
            .with_namespace(Some(self.config.consumer_group()))
            .ok()
            .flatten();
        let retry_topic = consumer_group.map(|group| format!("%RETRY%{group}"));
        for message in messages {
            if retry_topic.as_deref() == Some(message.message().topic()) {
                if let Some(origin_topic) = message
                    .message()
                    .properties()
                    .get(PROPERTY_RETRY_TOPIC)
                    .cloned()
                {
                    message.message_mut().set_topic(origin_topic);
                }
            }
            if let Ok(Some(topic)) = config.without_namespace(Some(message.message().topic())) {
                message.message_mut().set_topic(topic);
            }
        }
    }

    async fn check_orderly_reconsume_times(
        &self,
        context: &PullContext,
        queue: &MessageQueue,
        process_queue: &Arc<ProcessQueue>,
        batch: &ConsumeBatch,
        messages: &[MessageExt],
        generation: u64,
    ) -> bool {
        let maximum = self.config.effective_orderly_max_reconsume_times();
        let mut retry_required = false;
        for (offset, message) in batch.offsets().iter().copied().zip(messages) {
            if message.reconsume_times() >= maximum {
                if !self
                    .send_message_back_for_consume(&context.runtime, queue, message, 0, None, true)
                    .await
                {
                    if process_queue
                        .increment_reconsume_times(generation, offset)
                        .is_err()
                    {
                        return true;
                    }
                    retry_required = true;
                }
            } else {
                if process_queue
                    .increment_reconsume_times(generation, offset)
                    .is_err()
                {
                    return true;
                }
                retry_required = true;
            }
        }
        retry_required
    }

    fn schedule_orderly_lock_retry(
        self: &Arc<Self>,
        runtime: Arc<ClientRuntime>,
        process_queue: Arc<ProcessQueue>,
        generation: u64,
        delay_millis: i64,
    ) {
        let consumer = Arc::downgrade(self);
        let task_runtime = runtime.clone();
        let task_queue = process_queue.clone();
        let task = async move {
            let Some(consumer) = consumer.upgrade() else {
                return;
            };
            let Some(context) = consumer.pull_context().await else {
                return;
            };
            if !process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                return;
            }
            let locked = consumer
                .lock_orderly_process_queues(&context.runtime, std::slice::from_ref(&process_queue))
                .await
                .contains(process_queue.queue());
            consumer.schedule_orderly_work_retry(
                context.runtime,
                process_queue,
                generation,
                if locked {
                    10
                } else {
                    ORDERLY_LOCK_RETRY_DELAY_MILLIS
                },
            );
        };
        if let Err(error) = task_runtime.execute_task_later(task, delay_millis) {
            let _ = task_queue.release_consume_dispatch(generation);
            tracing::error!(generation, error = %error, "failed to schedule orderly lock retry");
        }
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
        if let Some(topic) = queue.topic() {
            let successful_result = status == Some(ConsumeConcurrentlyStatus::ConsumeSuccess);
            global_consumer_stats().record_concurrent_result(
                context.runtime.client_id(),
                self.config.consumer_group(),
                topic,
                successful_result.then_some(acknowledged),
                batch_len.saturating_sub(acknowledged),
            );
        }
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
                    .send_message_back_for_consume(
                        &context.runtime,
                        &queue,
                        message.as_ref(),
                        callback_context.delay_level_when_next_consume(),
                        None,
                        false,
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
                self.submit_ready_messages(&context, process_queue, generation);
            }
        }
    }

    async fn send_message_back_for_consume(
        &self,
        runtime: &Arc<ClientRuntime>,
        queue: &MessageQueue,
        message: &rocketmq_classic_common::message::MessageExt,
        delay_level: i32,
        broker_name_override: Option<Option<&str>>,
        orderly: bool,
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
        let max_reconsume_times = if orderly {
            self.config.effective_orderly_max_reconsume_times()
        } else {
            self.config.effective_max_reconsume_times()
        };
        if orderly {
            let sequence = self.next_retry_message_id.fetch_add(1, Ordering::Relaxed);
            let retry_topic = format!("%RETRY%{consumer_group}");
            let retry_message = self.build_retry_message(
                runtime,
                message,
                retry_topic,
                message.message().topic().to_owned(),
                sequence,
                max_reconsume_times,
            );
            return match self.send_retry_message(runtime, &retry_message).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::error!(
                        topic = ?queue.topic(),
                        queue_id = queue.queue_id(),
                        offset = message.queue_offset(),
                        error = %error,
                        "orderly retry-topic send failed"
                    );
                    false
                }
            };
        }
        let broker_name = match broker_name_override {
            Some(broker_name) => broker_name,
            None => message.broker_name().or(queue.broker_name()),
        }
        .map(str::to_owned);
        let header = ConsumerSendMsgBackRequestHeader {
            group: consumer_group.clone(),
            broker_name: broker_name.clone(),
            origin_topic: origin_topic.clone(),
            offset: message.commit_log_offset(),
            delay_level,
            origin_msg_id: message.message_id().map(str::to_owned),
            max_reconsume_times: Some(max_reconsume_times),
            unit_mode: self.config.unit_mode(),
        };

        match self
            .try_send_message_back_to_broker(
                runtime,
                &header,
                broker_name.as_deref(),
                message.store_host(),
            )
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
        let retry_message = self.build_retry_message(
            runtime,
            message,
            retry_topic,
            origin_topic,
            sequence,
            max_reconsume_times,
        );
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
        store_host: Option<&str>,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            let mut scripted = self
                .scripted_retry_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scripted.broker_headers.push(header.clone());
            scripted.broker_targets.push((
                broker_name.map(str::to_owned),
                store_host.map(str::to_owned),
            ));
            if let Some(result) = scripted.broker_results.pop_front() {
                return result;
            }
        }

        let deadline = Instant::now() + Duration::from_millis(CONSUMER_SEND_BACK_TIMEOUT_MILLIS);
        let address = if let Some(broker_name) = broker_name {
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
            address.ok_or_else(|| format!("broker {broker_name} has no master address"))?
        } else {
            store_host
                .filter(|address| !address.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| "message has no broker name or store host".to_owned())?
        };
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
        max_reconsume_times: i32,
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
            max_reconsume_times.to_string(),
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
        *write_lock(&self.pop_listener) = Some(listener.clone());
        let orderly_lock_settings = match &listener {
            PushMessageListener::Concurrently(_) => None,
            PushMessageListener::Orderly(_) => Some(
                OrderlyLockSettings::from_java_properties()
                    .map_err(PushConsumerError::OrderlyLockSettings)?,
            ),
        };
        self.consume_orderly
            .store(orderly_lock_settings.is_some(), Ordering::Release);
        let consume_service = match listener {
            PushMessageListener::Concurrently(listener) => {
                ConsumeService::Concurrent(Arc::new(ConcurrentConsumeService::new(
                    self.config.consume_thread_min(),
                    self.config.consume_thread_max(),
                    listener,
                )))
            }
            PushMessageListener::Orderly(listener) => {
                ConsumeService::Orderly(Arc::new(OrderlyConsumeService::new(
                    self.config.consume_thread_min(),
                    self.config.consume_thread_max(),
                    listener,
                )))
            }
        };
        let pull_scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());

        self.copy_subscriptions(&consumer_group)?;

        let runtime = self
            .manager
            .get_or_create_with_hook_and_tls_config(
                &client_config,
                self.rpc_hook.clone(),
                self.config.tls_config().cloned(),
            )
            .await?;
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
            orderly_lock_settings,
            renew_orderly_locks: orderly_lock_settings.is_some()
                && self.config.message_model()
                    == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering,
            pull_scheduler: pull_scheduler.clone(),
            pull_rpc_workers: pull_rpc_workers.clone(),
            consumer: Arc::downgrade(self),
        };
        self.consumer_start_timestamp
            .store(epoch_millis(), Ordering::Release);
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
                    orderly_lock_settings,
                    pull_scheduler,
                    pull_rpc_workers,
                };
                self.start_trace_dispatcher().await;
                runtime.rebalance_immediately();
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
                self.consume_orderly.store(false, Ordering::Release);
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

    async fn start_trace_dispatcher(&self) {
        let client_config = self.config.client_config();
        if !client_config.enable_trace() {
            return;
        }
        let dispatcher = match rocketmq_classic_producer::TraceDispatcher::new(
            client_config,
            self.config.consumer_group(),
            rocketmq_classic_producer::TraceProducerKind::Consume,
            client_config.trace_topic(),
            self.manager.clone(),
            self.rpc_hook.clone(),
        ) {
            Ok(dispatcher) => Arc::new(dispatcher),
            Err(error) => {
                tracing::error!(error = %error, "consume trace dispatcher initialization failed");
                return;
            }
        };
        if let Err(error) = dispatcher.start().await {
            tracing::error!(error = %error, "consume trace dispatcher start failed");
            return;
        }
        self.consume_message_hooks
            .register(Arc::new(ConsumeTraceHook::new(
                dispatcher.clone(),
                self.config.consumer_group(),
                client_config.clone(),
            )));
        let _ = self.trace_dispatcher.set(dispatcher);
    }

    async fn shutdown_trace_dispatcher(&self) {
        if let Some(dispatcher) = self.trace_dispatcher.get() {
            if let Err(error) = dispatcher.shutdown().await {
                tracing::warn!(error = %error, "consume trace dispatcher shutdown failed");
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
            .start(
                services.consumer.clone(),
                services.orderly_lock_settings,
                services.renew_orderly_locks,
            )
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
            consumer: services.consumer.clone(),
            runtime: Arc::downgrade(runtime),
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

    fn copy_subscriptions(&self, consumer_group: &str) -> Result<(), PushConsumerError> {
        let configured = read_lock(&self.subscription_config).clone();
        let configured = configured
            .iter()
            .map(|(topic, expression)| {
                build_subscription(topic, Some(expression), Some("TAG"))
                    .map(|subscription| (topic.clone(), subscription))
                    .map_err(|error| PushConsumerError::InvalidSubscription(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut subscriptions = write_lock(&self.subscriptions);
        subscriptions.extend(configured);
        if self.config.message_model()
            != rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting
        {
            let retry_topic = format!("%RETRY%{consumer_group}");
            let mut subscription =
                SubscriptionData::new(Some(retry_topic.clone()), Some("*".to_owned()));
            subscription.set_sub_version(epoch_millis());
            subscriptions.insert(retry_topic, subscription);
        }
        Ok(())
    }

    fn subscribe_pop_retry_topic(&self, topic: &str, consumer_group: &str) {
        let retry_topic = format!("%RETRY%{consumer_group}_{topic}");
        match build_subscription(&retry_topic, Some("*"), Some("TAG")) {
            Ok(mut subscription) => {
                subscription.set_sub_version(epoch_millis());
                write_lock(&self.subscriptions).insert(retry_topic, subscription);
            }
            Err(error) => {
                tracing::warn!(topic, error = %error, "failed to subscribe POP retry topic")
            }
        }
    }

    fn unsubscribe_pop_retry_topic(&self, topic: &str, consumer_group: &str) {
        let retry_topic = format!("%RETRY%{consumer_group}_{topic}");
        write_lock(&self.subscriptions).remove(&retry_topic);
        write_lock(&self.topic_queues).remove(&retry_topic);
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

    async fn pop_loop(
        self: Arc<Self>,
        process_queue: Arc<PopProcessQueue>,
        consumer_group: String,
        subscription: SubscriptionData,
    ) {
        let queue = process_queue.queue().clone();
        let Some(topic) = queue.topic().map(str::to_owned) else {
            return;
        };
        loop {
            if process_queue.is_dropped() || !self.rebalance_is_started().await {
                return;
            }
            if !read_lock(&self.pop_process_queues)
                .get(&queue)
                .is_some_and(|current| {
                    current.generation() == process_queue.generation()
                        && Arc::ptr_eq(current, &process_queue)
                })
            {
                return;
            }
            let Some(context) = self.pull_context().await else {
                return;
            };
            process_queue.set_last_pop_timestamp(epoch_millis());
            if self.paused.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(
                    PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND as u64,
                ))
                .await;
                continue;
            }
            if process_queue.wait_ack_count() > i64::from(self.config.pop_threshold_for_queue()) {
                tokio::time::sleep(Duration::from_millis(
                    PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL as u64,
                ))
                .await;
                continue;
            }

            let broker_name = queue.broker_name().unwrap_or_default();
            let broker_address = match self
                .resolve_pop_broker_address(&context.runtime, &topic, broker_name)
                .await
            {
                Ok(Some((_, address))) => address,
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(
                        PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION as u64,
                    ))
                    .await;
                    continue;
                }
                Err(error) => {
                    tracing::warn!(queue = %queue, error = %error, "push POP broker lookup failed");
                    tokio::time::sleep(Duration::from_millis(
                        PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION as u64,
                    ))
                    .await;
                    continue;
                }
            };
            let poll_time = BROKER_SUSPEND_MAX_TIME_MILLIS;
            let request = PopMessageRequestHeader {
                consumer_group: consumer_group.clone(),
                topic: topic.clone(),
                queue_id: queue.queue_id(),
                max_msg_nums: self.config.pop_batch_nums(),
                invisible_time: self.config.pop_invisible_time_millis(),
                poll_time,
                born_time: epoch_millis(),
                init_mode: if self.config.consume_from_where()
                    == ConsumeFromWhere::ConsumeFromFirstOffset
                {
                    0
                } else {
                    1
                },
                expression_type: subscription.get_expression_type().map(str::to_owned),
                expression: subscription.get_sub_string().map(str::to_owned),
                order: Some(false),
                attempt_id: None,
            };
            let deadline = Instant::now()
                + Duration::from_millis(poll_time.saturating_add(10_000).max(0) as u64);
            let pop_started = Instant::now();
            let reply = context
                .runtime
                .api()
                .pop_message(&broker_address, &request, deadline)
                .await;
            let reply = match reply {
                Ok(reply) => reply,
                Err(error) => {
                    if matches!(
                        &error,
                        rocketmq_classic_core::error::RpcError::Response {
                            code: response::FLOW_CONTROL,
                            ..
                        }
                    ) {
                        tokio::time::sleep(Duration::from_millis(
                            PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL as u64,
                        ))
                        .await;
                    } else {
                        tracing::debug!(queue = %queue, error = %error, "push POP request failed");
                        tokio::time::sleep(Duration::from_millis(
                            PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION as u64,
                        ))
                        .await;
                    }
                    continue;
                }
            };
            let result = match process_pop_reply(
                reply,
                &request,
                broker_name,
                DecodeOptions {
                    read_body: self.config.client_config().decode_read_body,
                    decompress_body: self.config.client_config().decode_decompress_body,
                    ..DecodeOptions::default()
                },
                DecodeLimits::default(),
            ) {
                Ok(result) => result,
                Err(error) => {
                    tracing::warn!(queue = %queue, error = %error, "push POP response processing failed");
                    tokio::time::sleep(Duration::from_millis(
                        PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION as u64,
                    ))
                    .await;
                    continue;
                }
            };
            if result.status() == &PopStatus::PollingFull {
                tokio::time::sleep(Duration::from_millis(
                    PULL_TIME_DELAY_MILLIS_WHEN_EXCEPTION as u64,
                ))
                .await;
                continue;
            }
            if result.status() != &PopStatus::Found {
                continue;
            }
            let messages = result.messages().unwrap_or_default().to_vec();
            global_consumer_stats().record_pull(
                context.runtime.client_id(),
                &consumer_group,
                &topic,
                pop_started.elapsed(),
                messages.len(),
            );
            let (messages, discarded) = self.filter_pop_messages(&subscription, messages);
            for message in discarded {
                if let Err(error) = self
                    .ack_pop_message(&context.runtime, &message, &topic, &consumer_group)
                    .await
                {
                    tracing::debug!(queue = %queue, error = %error, "failed to ACK a filtered POP message");
                }
            }
            if messages.is_empty() {
                continue;
            }
            process_queue.inc_found_messages(messages.len());
            self.dispatch_pop_messages(
                process_queue.clone(),
                context.runtime.clone(),
                queue.clone(),
                topic.clone(),
                consumer_group.clone(),
                messages,
            )
            .await;
            if self.config.pull_interval_millis() > 0 {
                tokio::time::sleep(Duration::from_millis(
                    self.config.pull_interval_millis() as u64
                ))
                .await;
            }
        }
    }

    async fn resolve_pop_broker_address(
        &self,
        runtime: &Arc<ClientRuntime>,
        topic: &str,
        broker_name: &str,
    ) -> Result<Option<(String, String)>, PushConsumerError> {
        let mut target_broker = broker_name.to_owned();
        let mut route = runtime.route_snapshot()?;
        if broker_name.starts_with("%LMQ%") {
            let logical_queue = MessageQueue::new(topic, broker_name, 0);
            if let Some(broker) = route.broker_name(&logical_queue)? {
                target_broker = broker.to_owned();
            }
        }
        let mut address = route.subscribe_address(&target_broker, 0, true)?;
        if address.is_none() {
            let timeout = i64::from(self.config.client_config().mq_client_api_timeout()).max(0);
            runtime
                .update_topic_route(
                    topic,
                    Instant::now() + Duration::from_millis(timeout as u64),
                )
                .await?;
            route = runtime.route_snapshot()?;
            address = route.subscribe_address(&target_broker, 0, true)?;
        }
        let Some(address) = address else {
            return Ok(None);
        };
        let address = broker_vip_channel(
            self.config.client_config().vip_channel_enabled(),
            &address.address,
        )?;
        Ok(Some((target_broker, address)))
    }

    fn filter_pop_messages(
        &self,
        subscription: &SubscriptionData,
        messages: Vec<MessageExt>,
    ) -> (Vec<MessageExt>, Vec<MessageExt>) {
        let tags = subscription
            .get_tags_set()
            .map(|tags| tags.snapshot())
            .unwrap_or_default();
        let max_reconsume_times = if self.config.max_reconsume_times() == -1 {
            16
        } else {
            self.config.max_reconsume_times()
        };
        let mut accepted = Vec::with_capacity(messages.len());
        let mut discarded = Vec::new();
        for message in messages {
            let tag_matches = subscription.is_class_filter_mode()
                || tags.is_empty()
                || message
                    .message()
                    .tags()
                    .is_some_and(|tag| tags.contains(&Some(tag.to_owned())));
            if !tag_matches || message.reconsume_times() > max_reconsume_times {
                discarded.push(message);
            } else {
                accepted.push(message);
            }
        }
        (accepted, discarded)
    }

    async fn dispatch_pop_messages(
        self: &Arc<Self>,
        process_queue: Arc<PopProcessQueue>,
        runtime: Arc<ClientRuntime>,
        queue: MessageQueue,
        route_topic: String,
        consumer_group: String,
        messages: Vec<MessageExt>,
    ) {
        let Some(listener) = read_lock(&self.pop_listener).clone() else {
            process_queue.dec_found_messages(messages.len());
            return;
        };
        let batch_size = self.config.consume_message_batch_max_size().max(1) as usize;
        for batch in messages.chunks(batch_size) {
            let Ok(permit) = self.pop_consume_semaphore.clone().acquire_owned().await else {
                return;
            };
            let consumer = self.clone();
            let process_queue = process_queue.clone();
            let runtime = runtime.clone();
            let queue = queue.clone();
            let route_topic = route_topic.clone();
            let consumer_group = consumer_group.clone();
            let listener = listener.clone();
            let messages = batch.to_vec();
            let batch_context = PopConsumeBatchContext {
                process_queue,
                runtime,
                queue,
                route_topic,
                consumer_group,
            };
            let worker = tokio::spawn(async move {
                let _permit = permit;
                consumer
                    .consume_pop_batch(batch_context, listener, messages)
                    .await;
            });
            let mut workers = self
                .pop_consume_workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            workers.retain(|worker| !worker.is_finished());
            workers.push(worker);
        }
    }

    async fn consume_pop_batch(
        self: Arc<Self>,
        batch_context: PopConsumeBatchContext,
        listener: PushMessageListener,
        mut messages: Vec<MessageExt>,
    ) {
        let PopConsumeBatchContext {
            process_queue,
            runtime,
            queue,
            route_topic,
            consumer_group,
        } = batch_context;
        if process_queue.is_dropped() {
            return;
        }
        if pop_batch_is_expired(&messages, epoch_millis()) {
            process_queue.dec_found_messages(messages.len());
            tracing::debug!(
                queue = %queue,
                message_count = messages.len(),
                "push POP batch expired before listener dispatch"
            );
            return;
        }
        self.reset_retry_and_namespace_for_pop(&mut messages, &consumer_group);
        let mut consume_context = self.new_consume_message_context(messages.clone(), queue.clone());
        self.consume_message_hooks
            .execute_before(&mut consume_context);
        messages = consume_context.messages.clone().unwrap_or_default();
        let consume_started = Instant::now();
        let (ack_index, delay_level, orderly_delay_millis, status, success, return_type) =
            match listener {
                PushMessageListener::Concurrently(listener) => {
                    let context = ConsumeConcurrentlyContext::new(queue.clone());
                    let listener_messages = messages.clone();
                    let callback_started = Instant::now();
                    let callback = tokio::spawn(async move {
                        listener.consume_message(listener_messages, context).await
                    });
                    match callback.await {
                        Ok((Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)) => (
                            context
                                .ack_index()
                                .min(i32::try_from(messages.len()).unwrap_or(i32::MAX) - 1),
                            context.delay_level_when_next_consume(),
                            None,
                            Some("CONSUME_SUCCESS"),
                            true,
                            if callback_started.elapsed() >= pop_invisible_timeout(&messages) {
                                "TIME_OUT"
                            } else {
                                "SUCCESS"
                            },
                        ),
                        Ok((Some(ConsumeConcurrentlyStatus::ReconsumeLater), context)) => (
                            -1,
                            context.delay_level_when_next_consume(),
                            None,
                            Some("RECONSUME_LATER"),
                            false,
                            if callback_started.elapsed() >= pop_invisible_timeout(&messages) {
                                "TIME_OUT"
                            } else {
                                "FAILED"
                            },
                        ),
                        Ok((None, context)) => (
                            -1,
                            context.delay_level_when_next_consume(),
                            None,
                            Some("RECONSUME_LATER"),
                            false,
                            "RETURNNULL",
                        ),
                        Err(error) => {
                            tracing::warn!(queue = %queue, error = %error, "push POP concurrent listener failed");
                            (-1, 0, None, Some("RECONSUME_LATER"), false, "EXCEPTION")
                        }
                    }
                }
                PushMessageListener::Orderly(listener) => {
                    let context = ConsumeOrderlyContext::new(queue.clone());
                    let listener_messages = messages.clone();
                    let callback_started = Instant::now();
                    let callback = tokio::spawn(async move {
                        listener.consume_message(listener_messages, context).await
                    });
                    match callback.await {
                        Ok((
                            Some(ConsumeOrderlyStatus::Success | ConsumeOrderlyStatus::Commit),
                            context,
                        )) if context.auto_commit() => (
                            i32::try_from(messages.len()).unwrap_or(i32::MAX) - 1,
                            0,
                            None,
                            Some("SUCCESS"),
                            true,
                            if callback_started.elapsed() >= pop_invisible_timeout(&messages) {
                                "TIME_OUT"
                            } else {
                                "SUCCESS"
                            },
                        ),
                        Ok((Some(status), context)) => (
                            -1,
                            0,
                            Some(context.suspend_current_queue_time_millis()),
                            Some(orderly_status_name(status)),
                            false,
                            if callback_started.elapsed() >= pop_invisible_timeout(&messages) {
                                "TIME_OUT"
                            } else if status == ConsumeOrderlyStatus::SuspendCurrentQueueAMoment {
                                "FAILED"
                            } else {
                                "SUCCESS"
                            },
                        ),
                        Ok((None, context)) => (
                            -1,
                            0,
                            Some(context.suspend_current_queue_time_millis()),
                            Some("SUSPEND_CURRENT_QUEUE_A_MOMENT"),
                            false,
                            "RETURNNULL",
                        ),
                        Err(error) => {
                            tracing::warn!(queue = %queue, error = %error, "push POP orderly listener failed");
                            (
                                -1,
                                0,
                                None,
                                Some("SUSPEND_CURRENT_QUEUE_A_MOMENT"),
                                false,
                                "EXCEPTION",
                            )
                        }
                    }
                }
            };
        consume_context
            .properties
            .get_or_insert_with(HashMap::new)
            .insert("CONSUME_CONTEXT_TYPE".to_owned(), return_type.to_owned());
        consume_context.status = status.map(str::to_owned);
        consume_context.success = success;
        consume_context.access_channel = self.config.client_config().access_channel();
        self.consume_message_hooks
            .execute_after(&mut consume_context);

        let message_count = messages.len();
        if let Some(topic) = queue.topic() {
            let stats = global_consumer_stats();
            stats.record_consume_rt(
                runtime.client_id(),
                &consumer_group,
                topic,
                consume_started.elapsed(),
            );
            if message_count > 0 && return_type != "TIME_OUT" && !process_queue.is_dropped() {
                if success {
                    let acknowledged = usize::try_from(ack_index.saturating_add(1))
                        .unwrap_or(0)
                        .min(message_count);
                    stats.record_concurrent_result(
                        runtime.client_id(),
                        &consumer_group,
                        topic,
                        Some(acknowledged),
                        message_count.saturating_sub(acknowledged),
                    );
                } else {
                    stats.record_concurrent_result(
                        runtime.client_id(),
                        &consumer_group,
                        topic,
                        None,
                        message_count,
                    );
                }
            }
        }

        let max_reconsume_times = if self.config.max_reconsume_times() == -1 {
            16
        } else {
            self.config.max_reconsume_times()
        };
        let delay_table = pop_delay_seconds();
        for (index, mut message) in messages.into_iter().enumerate() {
            let acknowledged = i32::try_from(index).unwrap_or(i32::MAX) <= ack_index;
            process_queue.ack();
            if acknowledged {
                if let Err(error) = self
                    .ack_pop_message(&runtime, &message, &route_topic, &consumer_group)
                    .await
                {
                    tracing::debug!(queue = %queue, error = %error, "push POP ACK failed");
                }
                continue;
            }

            if message.reconsume_times() >= max_reconsume_times {
                let max_delay_millis = i64::from(*delay_table.last().unwrap_or(&7_200)) * 1_000;
                if epoch_millis().saturating_sub(message.born_timestamp()) > max_delay_millis * 2 {
                    let _ = self
                        .ack_pop_message(&runtime, &message, &route_topic, &consumer_group)
                        .await;
                    continue;
                }
                let age_millis = epoch_millis().saturating_sub(message.born_timestamp());
                let delay_index = delay_table
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, delay)| age_millis >= i64::from(**delay) * 1_000)
                    .map_or(delay_table.len() - 1, |(index, _)| {
                        (index + 1).min(delay_table.len() - 1)
                    });
                self.change_pop_invisible_time(
                    &runtime,
                    &mut message,
                    &route_topic,
                    &consumer_group,
                    i64::from(delay_table[delay_index]) * 1_000,
                )
                .await;
                continue;
            }

            let invisible_time = if let Some(delay_millis) = orderly_delay_millis {
                delay_millis.clamp(10, 30_000)
            } else {
                let delay_index = if delay_level == 0 {
                    usize::try_from(message.reconsume_times().max(0)).unwrap_or(0)
                } else {
                    usize::try_from(delay_level.max(0)).unwrap_or(0)
                };
                i64::from(
                    *delay_table
                        .get(delay_index)
                        .unwrap_or_else(|| delay_table.last().unwrap()),
                ) * 1_000
            };
            self.change_pop_invisible_time(
                &runtime,
                &mut message,
                &route_topic,
                &consumer_group,
                invisible_time,
            )
            .await;
        }
    }

    fn reset_retry_and_namespace_for_pop(&self, messages: &mut [MessageExt], consumer_group: &str) {
        let mut config = self.config.client_config().clone();
        let retry_topic = format!("%RETRY%{consumer_group}");
        for message in messages {
            if message.message().topic() == retry_topic {
                if let Some(origin_topic) = message
                    .message()
                    .properties()
                    .get(PROPERTY_RETRY_TOPIC)
                    .cloned()
                {
                    message.message_mut().set_topic(origin_topic);
                }
            }
            if let Ok(Some(topic)) = config.without_namespace(Some(message.message().topic())) {
                message.message_mut().set_topic(topic);
            }
        }
    }

    async fn ack_pop_message(
        &self,
        runtime: &Arc<ClientRuntime>,
        message: &MessageExt,
        route_topic: &str,
        consumer_group: &str,
    ) -> Result<AckStatus, PushConsumerError> {
        let raw = message
            .message()
            .properties()
            .get(PROPERTY_POP_CK)
            .ok_or(PushConsumerError::MissingPopReceipt)?;
        let receipt = PopReceipt::parse(raw.clone())?;
        let address = self
            .resolve_pop_broker_address(runtime, route_topic, receipt.broker_name())
            .await?
            .ok_or_else(|| PushConsumerError::PopBrokerNotFound(receipt.broker_name().to_owned()))?
            .1;
        let header = receipt.ack_header(message.message().topic(), consumer_group);
        Ok(runtime
            .api()
            .ack_message(
                &address,
                &header,
                Instant::now() + Duration::from_millis(3_000),
            )
            .await?)
    }

    async fn change_pop_invisible_time(
        &self,
        runtime: &Arc<ClientRuntime>,
        message: &mut MessageExt,
        route_topic: &str,
        consumer_group: &str,
        invisible_time: i64,
    ) {
        let Some(raw) = message.message().properties().get(PROPERTY_POP_CK).cloned() else {
            return;
        };
        let Ok(receipt) = PopReceipt::parse(raw) else {
            return;
        };
        let Ok(Some((_, address))) = self
            .resolve_pop_broker_address(runtime, route_topic, receipt.broker_name())
            .await
        else {
            return;
        };
        let header = receipt.change_invisible_header(
            message.message().topic(),
            consumer_group,
            invisible_time,
        );
        match runtime
            .api()
            .change_invisible_time(
                &address,
                receipt.broker_name(),
                &header,
                Instant::now() + Duration::from_millis(3_000),
            )
            .await
        {
            Ok(result) if result.status == AckStatus::Ok => {
                if let Some(extra_info) = result.extra_info {
                    let _ = message
                        .message_mut()
                        .set_system_property(PROPERTY_POP_CK, extra_info);
                }
            }
            Ok(_) => {}
            Err(error) => tracing::debug!(error = %error, "push POP invisibility update failed"),
        }
    }

    pub(crate) async fn rebalance_once(
        self: &Arc<Self>,
        strategy: &dyn crate::rebalance::AllocateMessageQueueStrategy,
    ) -> bool {
        let Some(context) = self.pull_context().await else {
            return true;
        };
        let mut balanced = true;
        let mut subscriptions = read_lock(&self.subscriptions)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        subscriptions.sort_by(|left, right| java_string_cmp(left, right));
        let subscribed_topics = subscriptions.iter().cloned().collect::<HashSet<_>>();
        let topic_queues = read_lock(&self.topic_queues).clone();
        let mut client_config = self.config.client_config().clone();
        let Some(consumer_group) = client_config
            .with_namespace(Some(self.config.consumer_group()))
            .ok()
            .flatten()
        else {
            return false;
        };

        let mut assignments = Vec::new();
        let mut authoritative_topics = HashSet::new();
        let mut desired_push_queues = HashSet::new();
        let mut desired_pop_queues = HashSet::new();
        let mut missing_route_notifications = Vec::new();
        for topic in &subscriptions {
            if !self.rebalance_is_started().await {
                return true;
            }
            let message_model = self.config.message_model();
            if message_model == rocketmq_classic_protocol::heartbeat::MessageModel::LiteSelective
                && (self.config.client_rebalance() || self.consume_orderly.load(Ordering::Acquire))
            {
                // Java's LiteSelective rebalance uses the default no-local-allocation branch;
                // broker assignment is used when client-side rebalance is disabled.
                continue;
            }

            if message_model == rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting {
                let Some(queue_snapshot) = topic_queues.get(topic) else {
                    missing_route_notifications.push(topic.clone());
                    balanced = false;
                    continue;
                };
                let mut all_queues = queue_snapshot.iter().cloned().collect::<Vec<_>>();
                all_queues.sort();
                let all_queue_set = all_queues.iter().cloned().collect::<HashSet<_>>();
                authoritative_topics.insert(topic.clone());
                desired_push_queues.extend(all_queues.iter().cloned());
                assignments.push(DesiredAssignment {
                    topic: topic.clone(),
                    all_queues: Some(all_queue_set.clone()),
                    assigned_queues: all_queue_set.clone(),
                    pull_queues: all_queue_set,
                    pop_queues: HashSet::new(),
                });
                continue;
            }

            if self.config.client_rebalance() || self.consume_orderly.load(Ordering::Acquire) {
                // Java RebalanceImpl checks the local route table before it
                // queries membership. A missing or empty route notifies with
                // empty sets and leaves the previous assignment untouched.
                let Some(queue_snapshot) =
                    topic_queues.get(topic).filter(|queues| !queues.is_empty())
                else {
                    if !topic.starts_with("%RETRY%") {
                        missing_route_notifications.push(topic.clone());
                    }
                    balanced = false;
                    continue;
                };
                let consumer_ids = self
                    .find_consumer_id_list_for_rebalance(&context.runtime, topic, &consumer_group)
                    .await;
                let mut all_queues = queue_snapshot.iter().cloned().collect::<Vec<_>>();
                all_queues.sort();
                let all_queue_set = all_queues.iter().cloned().collect::<HashSet<_>>();
                let consumer_ids = match consumer_ids {
                    Ok(Some(consumer_ids)) if !consumer_ids.is_empty() => consumer_ids,
                    Ok(Some(_)) | Ok(None) => {
                        balanced = false;
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(topic, error = %error, "push rebalance consumer list lookup failed");
                        balanced = false;
                        continue;
                    }
                };
                let mut consumer_ids = match consumer_ids.into_iter().collect::<Option<Vec<_>>>() {
                    Some(consumer_ids) => consumer_ids,
                    None => {
                        balanced = false;
                        continue;
                    }
                };
                consumer_ids.sort_by(|left, right| java_string_cmp(left, right));
                let allocation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    strategy.allocate(
                        &consumer_group,
                        context.runtime.client_id(),
                        &all_queues,
                        &consumer_ids,
                    )
                }));
                let allocated = match allocation {
                    Ok(Ok(allocated)) => allocated,
                    Ok(Err(error)) => {
                        tracing::warn!(
                            topic,
                            strategy = strategy.name(),
                            error = %error,
                            "push queue allocation failed"
                        );
                        balanced = false;
                        continue;
                    }
                    Err(_) => {
                        tracing::warn!(
                            topic,
                            strategy = strategy.name(),
                            "push queue allocation panicked"
                        );
                        balanced = false;
                        continue;
                    }
                };
                let assigned = allocated.into_iter().collect::<HashSet<_>>();
                desired_push_queues.extend(assigned.iter().cloned());
                assignments.push(DesiredAssignment {
                    topic: topic.clone(),
                    all_queues: Some(all_queue_set),
                    assigned_queues: assigned.clone(),
                    pull_queues: assigned,
                    pop_queues: HashSet::new(),
                });
                authoritative_topics.insert(topic.clone());
                continue;
            } else {
                // Broker assignment does not depend on the local route table.
                let result = context
                    .runtime
                    .query_assignment(
                        topic,
                        &consumer_group,
                        strategy.name(),
                        self.config.message_model(),
                        3_000,
                    )
                    .await;
                #[allow(clippy::mutable_key_type)]
                let assignment_set = match result {
                    Ok(Some(assignment_set)) => assignment_set,
                    Ok(None) => {
                        balanced = false;
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(topic, error = %error, "push broker assignment lookup failed");
                        balanced = false;
                        continue;
                    }
                };
                let Some(BrokerAssignmentQueues {
                    assigned,
                    pull: pull_queues,
                    pop: pop_queues,
                }) = partition_broker_assignments(assignment_set)
                else {
                    balanced = false;
                    continue;
                };
                if !topic.starts_with("%RETRY%") {
                    if pop_queues.is_empty() && !pull_queues.is_empty() {
                        self.subscribe_pop_retry_topic(topic, &consumer_group);
                    } else if !pop_queues.is_empty() && pull_queues.is_empty() {
                        self.unsubscribe_pop_retry_topic(topic, &consumer_group);
                    }
                }
                desired_push_queues.extend(pull_queues.iter().cloned());
                desired_pop_queues.extend(pop_queues.iter().cloned());
                authoritative_topics.insert(topic.clone());
                assignments.push(DesiredAssignment {
                    topic: topic.clone(),
                    all_queues: None,
                    assigned_queues: assigned,
                    pull_queues,
                    pop_queues,
                });
            };
        }

        let current_queues = read_lock(&self.process_queues)
            .iter()
            .map(|(queue, process_queue)| (queue.clone(), process_queue.clone()))
            .collect::<Vec<_>>();
        let mut changed_queues = HashSet::new();
        for (queue, process_queue) in current_queues {
            if !self.rebalance_is_started().await {
                return true;
            }
            let Some(topic) = queue.topic() else {
                balanced = false;
                continue;
            };
            let generation = process_queue.generation();
            let snapshot = match process_queue.snapshot(generation) {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    balanced = false;
                    continue;
                }
            };
            let should_remove = snapshot.dropped
                || !subscribed_topics.contains(topic)
                || (authoritative_topics.contains(topic) && !desired_push_queues.contains(&queue));
            if !should_remove {
                continue;
            }
            if !snapshot.dropped {
                if process_queue.drop_assignment(generation).is_err() {
                    balanced = false;
                    continue;
                }
                context.scheduler.cancel(&queue, generation);
            }
            match self
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
            {
                true => {
                    changed_queues.insert(queue);
                }
                false => balanced = false,
            }
        }

        let current_pop_queues = read_lock(&self.pop_process_queues)
            .iter()
            .map(|(queue, process_queue)| (queue.clone(), process_queue.clone()))
            .collect::<Vec<_>>();
        for (queue, process_queue) in current_pop_queues {
            if !self.rebalance_is_started().await {
                return true;
            }
            let Some(topic) = queue.topic() else {
                balanced = false;
                continue;
            };
            let should_remove = process_queue.is_dropped()
                || process_queue.is_pull_expired()
                || !subscribed_topics.contains(topic)
                || (authoritative_topics.contains(topic) && !desired_pop_queues.contains(&queue));
            if !should_remove {
                continue;
            }
            process_queue.drop_assignment();
            let removed = {
                let mut queues = write_lock(&self.pop_process_queues);
                if queues
                    .get(&queue)
                    .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
                {
                    queues.remove(&queue);
                    true
                } else {
                    false
                }
            };
            if !removed {
                continue;
            }
            let worker = self
                .pop_workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&queue);
            if let Some(worker) = worker {
                worker.abort();
                let _ = worker.await;
            }
            changed_queues.insert(queue);
        }

        let mut additions = desired_push_queues.into_iter().collect::<Vec<_>>();
        additions.sort();
        let orderly_clustering = self.current_orderly_lock_settings().await.is_some()
            && self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering;
        for queue in additions {
            if !self.rebalance_is_started().await {
                return true;
            }
            let Some(topic) = queue.topic() else {
                balanced = false;
                continue;
            };
            if !subscribed_topics.contains(topic) {
                continue;
            }
            if let Some(existing) = read_lock(&self.process_queues).get(&queue).cloned() {
                if existing
                    .snapshot(existing.generation())
                    .is_ok_and(|snapshot| !snapshot.dropped)
                {
                    continue;
                }
                // A dropped generation stays registered until its persisted
                // offset cleanup succeeds; a replacement waits for that retry.
                balanced = false;
                continue;
            }
            if read_lock(&self.offset_corrections_pending).contains(&queue) {
                balanced = false;
                continue;
            }
            let orderly_lock_instant = if orderly_clustering {
                let lock_result = self
                    .lock_orderly_queues_raw(&context.runtime, std::slice::from_ref(&queue))
                    .await;
                if lock_result.uncertain.contains(&queue) {
                    self.unlock_orderly_queues_tracking(
                        &context.runtime,
                        std::slice::from_ref(&queue),
                    )
                    .await;
                }
                if !lock_result.locked.contains(&queue) {
                    balanced = false;
                    continue;
                }
                let Some((_, lock_instant)) = lock_result.lock_times.get(&queue).copied() else {
                    balanced = false;
                    continue;
                };
                if !self.rebalance_is_started().await {
                    self.unlock_orderly_queues_tracking(
                        &context.runtime,
                        std::slice::from_ref(&queue),
                    )
                    .await;
                    return true;
                }
                Some(lock_instant)
            } else {
                None
            };
            let orderly_locked = orderly_lock_instant.is_some();
            context.offset_store.remove_offset(&queue);
            let initial_offset = match self
                .compute_initial_offset(&context.runtime, &context.offset_store, &queue)
                .await
            {
                Ok(offset) => offset,
                Err(error) => {
                    tracing::warn!(queue = %queue, error = %error, "push start offset lookup failed");
                    if orderly_locked {
                        self.unlock_orderly_queues_tracking(
                            &context.runtime,
                            std::slice::from_ref(&queue),
                        )
                        .await;
                    }
                    balanced = false;
                    continue;
                }
            };
            match self
                .activate_process_queue_with_lock(&queue, initial_offset, orderly_lock_instant)
                .await
            {
                Ok(_) => {
                    changed_queues.insert(queue);
                }
                Err(error) => {
                    tracing::warn!(queue = %queue, error = %error, "push queue activation failed");
                    if orderly_locked {
                        self.unlock_orderly_queues_tracking(
                            &context.runtime,
                            std::slice::from_ref(&queue),
                        )
                        .await;
                    }
                    balanced = false;
                }
            }
        }

        let mut pop_additions = desired_pop_queues.into_iter().collect::<Vec<_>>();
        pop_additions.sort();
        for queue in pop_additions {
            if !self.rebalance_is_started().await {
                return true;
            }
            let Some(topic) = queue.topic() else {
                balanced = false;
                continue;
            };
            if !subscribed_topics.contains(topic) {
                continue;
            }
            if read_lock(&self.process_queues)
                .get(&queue)
                .is_some_and(|process_queue| {
                    process_queue
                        .snapshot(process_queue.generation())
                        .is_ok_and(|snapshot| !snapshot.dropped)
                })
            {
                balanced = false;
                continue;
            }
            let subscription = match read_lock(&self.subscriptions).get(topic).cloned() {
                Some(subscription) => subscription,
                None => {
                    balanced = false;
                    continue;
                }
            };
            let mut workers = self
                .pop_workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if workers
                .get(&queue)
                .is_some_and(|worker| !worker.is_finished())
            {
                if read_lock(&self.pop_process_queues).contains_key(&queue) {
                    continue;
                }
                workers.remove(&queue).unwrap().abort();
            } else {
                workers.remove(&queue);
            }
            drop(workers);

            if let Some(existing) = read_lock(&self.pop_process_queues).get(&queue) {
                if !existing.is_dropped() && !existing.is_pull_expired() {
                    balanced = false;
                    continue;
                }
            }
            let generation = match self.next_generation.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |value| value.checked_add(1),
            ) {
                Ok(generation) => generation,
                Err(_) => {
                    balanced = false;
                    continue;
                }
            };
            let process_queue = Arc::new(PopProcessQueue::new(queue.clone(), generation));
            {
                let mut queues = write_lock(&self.pop_process_queues);
                if queues
                    .get(&queue)
                    .is_some_and(|current| !current.is_dropped() && !current.is_pull_expired())
                {
                    balanced = false;
                    continue;
                }
                queues.insert(queue.clone(), process_queue.clone());
            }
            let consumer = self.clone();
            let pop_queue = process_queue.clone();
            let group = consumer_group.clone();
            let worker = tokio::spawn(async move {
                consumer.pop_loop(pop_queue, group, subscription).await;
            });
            self.pop_workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(queue.clone(), worker);
            changed_queues.insert(queue);
        }

        for assignment in &assignments {
            let expected = assignment
                .pull_queues
                .iter()
                .filter(|queue| queue.topic() == Some(assignment.topic.as_str()))
                .cloned()
                .collect::<HashSet<_>>();
            let working = read_lock(&self.process_queues)
                .iter()
                .filter(|(queue, process_queue)| {
                    queue.topic() == Some(assignment.topic.as_str())
                        && process_queue
                            .snapshot(process_queue.generation())
                            .is_ok_and(|snapshot| !snapshot.dropped)
                })
                .map(|(queue, _)| queue.clone())
                .collect::<HashSet<_>>();
            if expected != working {
                balanced = false;
            }
            let expected_pop = assignment
                .pop_queues
                .iter()
                .filter(|queue| queue.topic() == Some(assignment.topic.as_str()))
                .cloned()
                .collect::<HashSet<_>>();
            let working_pop = read_lock(&self.pop_process_queues)
                .iter()
                .filter(|(queue, process_queue)| {
                    queue.topic() == Some(assignment.topic.as_str()) && !process_queue.is_dropped()
                })
                .map(|(queue, _)| queue.clone())
                .collect::<HashSet<_>>();
            if expected_pop != working_pop {
                balanced = false;
            }
            let queue_changed = changed_queues.iter().any(|queue| {
                queue.topic() == Some(assignment.topic.as_str())
                    || assignment.assigned_queues.contains(queue)
            });
            if queue_changed
                && !self
                    .notify_assignment_changed(&context.runtime, assignment)
                    .await
            {
                balanced = false;
            }
        }
        for topic in missing_route_notifications {
            if !self.rebalance_is_started().await {
                return true;
            }
            let empty = HashSet::new();
            let assignment = DesiredAssignment {
                topic,
                all_queues: Some(empty.clone()),
                assigned_queues: empty.clone(),
                pull_queues: empty,
                pop_queues: HashSet::new(),
            };
            if !self
                .notify_assignment_changed(&context.runtime, &assignment)
                .await
            {
                balanced = false;
            }
        }
        balanced
    }

    async fn rebalance_is_started(&self) -> bool {
        matches!(&*self.state.lock().await, State::Started { .. })
    }

    async fn find_consumer_id_list_for_rebalance(
        &self,
        runtime: &Arc<ClientRuntime>,
        topic: &str,
        consumer_group: &str,
    ) -> Result<Option<Vec<Option<String>>>, rocketmq_classic_core::runtime::RuntimeError> {
        #[cfg(test)]
        {
            let scripted = self
                .rebalance_membership
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_mut(topic)
                .and_then(VecDeque::pop_front);
            if let Some(scripted) = scripted {
                return Ok(scripted.map(|ids| ids.into_iter().map(Some).collect()));
            }
        }
        runtime.find_consumer_id_list(topic, consumer_group).await
    }

    async fn remove_process_queue_safely(
        &self,
        context: &PullContext,
        queue: &MessageQueue,
        process_queue: &Arc<ProcessQueue>,
    ) -> bool {
        let generation = process_queue.generation();
        if !self.rebalance_is_started().await {
            return false;
        }
        let orderly_clustering = self.current_orderly_lock_settings().await.filter(|_| {
            self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
        });
        let _consume_guard = if orderly_clustering.is_some() {
            let gate = process_queue.orderly_consume_gate();
            match tokio::time::timeout(Duration::from_millis(500), gate.lock_owned()).await {
                Ok(guard) => Some(guard),
                Err(_) => {
                    let force_unlock = orderly_clustering.is_some_and(|settings| {
                        process_queue
                            .is_lock_older_than(generation, settings.unlock_delay_millis)
                            .unwrap_or(false)
                    });
                    if force_unlock {
                        None
                    } else {
                        let _ = process_queue.increment_try_unlock_times(generation);
                        return false;
                    }
                }
            }
        } else {
            None
        };
        if let Err(error) = context.offset_store.persist(queue).await {
            tracing::warn!(queue = %queue, generation, error = %error, "push queue offset cleanup deferred");
            return false;
        }
        if orderly_clustering.is_some()
            && !self
                .unlock_orderly_process_queues(
                    &context.runtime,
                    std::slice::from_ref(process_queue),
                )
                .await
        {
            return false;
        }
        let mut queues = write_lock(&self.process_queues);
        if !queues
            .get(queue)
            .is_some_and(|current| Arc::ptr_eq(current, process_queue))
        {
            return false;
        }
        context.offset_store.remove_offset(queue);
        queues.remove(queue);
        write_lock(&self.offset_corrections_pending).remove(queue);
        true
    }

    async fn compute_initial_offset(
        &self,
        runtime: &Arc<ClientRuntime>,
        offset_store: &Arc<dyn OffsetStore>,
        queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let stored = offset_store
            .read_offset(queue, ReadOffsetType::ReadFromStore)
            .await?;
        let retry_topic = queue
            .topic()
            .is_some_and(|topic| topic.starts_with("%RETRY%"));
        let plan = initial_offset_plan(
            self.config.consume_from_where(),
            retry_topic,
            stored,
            self.config.consume_timestamp(),
        )?;
        let offset = match plan {
            InitialOffsetPlan::Stored(offset) | InitialOffsetPlan::Fixed(offset) => offset,
            InitialOffsetPlan::MaxOffset => self.query_max_offset(runtime, queue).await?,
            InitialOffsetPlan::SearchTimestamp(timestamp) => {
                self.query_search_offset(runtime, queue, timestamp).await?
            }
        };
        if offset < 0 {
            return Err(PushConsumerError::InvalidOffset);
        }
        Ok(offset)
    }

    async fn query_max_offset(
        &self,
        runtime: &Arc<ClientRuntime>,
        queue: &MessageQueue,
    ) -> Result<i64, PushConsumerError> {
        let topic = queue.topic().ok_or(PushConsumerError::InvalidQueue)?;
        let broker_name = queue.broker_name().ok_or(PushConsumerError::InvalidQueue)?;
        let deadline = self.offset_rpc_deadline();
        let address = self
            .find_offset_broker(runtime, topic, broker_name, deadline)
            .await?;
        let header = GetMaxOffsetRequestHeader {
            topic: topic.to_owned(),
            queue_id: queue.queue_id(),
            committed: true,
            broker_name: Some(broker_name.to_owned()),
        };
        Ok(runtime
            .api()
            .get_max_offset(&address, &header, deadline)
            .await?)
    }

    async fn query_search_offset(
        &self,
        runtime: &Arc<ClientRuntime>,
        queue: &MessageQueue,
        timestamp: i64,
    ) -> Result<i64, PushConsumerError> {
        let topic = queue.topic().ok_or(PushConsumerError::InvalidQueue)?;
        let broker_name = queue.broker_name().ok_or(PushConsumerError::InvalidQueue)?;
        let deadline = self.offset_rpc_deadline();
        let address = self
            .find_offset_broker(runtime, topic, broker_name, deadline)
            .await?;
        let header = SearchOffsetRequestHeader {
            topic: topic.to_owned(),
            lite_topic: None,
            queue_id: queue.queue_id(),
            timestamp,
            boundary_type: Some(BoundaryType::Lower),
            broker_name: Some(broker_name.to_owned()),
        };
        Ok(runtime
            .api()
            .search_offset(&address, &header, deadline)
            .await?)
    }

    async fn find_offset_broker(
        &self,
        runtime: &Arc<ClientRuntime>,
        topic: &str,
        broker_name: &str,
        deadline: Instant,
    ) -> Result<String, PushConsumerError> {
        let mut route = runtime.route_snapshot()?;
        let mut address = route.publish_address(broker_name).map(str::to_owned);
        if address.is_none() {
            runtime.update_topic_route(topic, deadline).await?;
            route = runtime.route_snapshot()?;
            address = route.publish_address(broker_name).map(str::to_owned);
        }
        address.ok_or_else(|| OffsetStoreError::BrokerNotFound(broker_name.to_owned()).into())
    }

    async fn prepare_admin_queue(
        &self,
        input_queue: &MessageQueue,
    ) -> Result<(Arc<ClientRuntime>, MessageQueue, String, String, Instant), PushConsumerError>
    {
        let context = self
            .pull_context()
            .await
            .ok_or(PushConsumerError::NotStarted)?;
        let queue = self
            .config
            .client_config()
            .clone()
            .queue_with_namespace(input_queue)?;
        let topic = queue
            .topic()
            .filter(|topic| !topic.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?;
        let broker_name = queue
            .broker_name()
            .filter(|broker| !broker.is_empty())
            .ok_or(PushConsumerError::InvalidQueue)?
            .to_owned();
        let deadline = self.offset_rpc_deadline();
        let address = self
            .find_offset_broker(&context.runtime, topic, &broker_name, deadline)
            .await?;
        Ok((context.runtime, queue, broker_name, address, deadline))
    }

    async fn request_orderly_lock_batch(
        &self,
        runtime: &Arc<ClientRuntime>,
        broker_name: &str,
        topic: &str,
        body: &LockBatchRequestBody,
    ) -> Result<Option<HashSet<Option<MessageQueue>>>, String> {
        #[cfg(test)]
        {
            let (waiter, result, requested) = {
                let mut scripted = self
                    .scripted_lock_transport
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                scripted.lock_bodies.push(body.clone());
                (
                    scripted.lock_waiters.pop_front(),
                    scripted.lock_results.pop_front(),
                    scripted.lock_requested.clone(),
                )
            };
            requested.notify_one();
            if let Some(waiter) = waiter {
                return waiter
                    .await
                    .map_err(|_| "scripted lock response sender dropped".to_owned())?;
            }
            if let Some(result) = result {
                return result;
            }
        }

        let deadline = Instant::now() + Duration::from_millis(ORDERLY_LOCK_RPC_TIMEOUT_MILLIS);
        let address = self
            .find_offset_broker(runtime, topic, broker_name, deadline)
            .await
            .map_err(|error| error.to_string())?;
        let address =
            broker_vip_channel(self.config.client_config().vip_channel_enabled(), &address)
                .map_err(|error| error.to_string())?;
        runtime
            .api()
            .lock_batch_mq(&address, body, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    async fn request_orderly_unlock_batch(
        &self,
        runtime: &Arc<ClientRuntime>,
        broker_name: &str,
        topic: &str,
        body: &UnlockBatchRequestBody,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            let mut scripted = self
                .scripted_lock_transport
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            scripted.unlock_bodies.push(body.clone());
            if let Some(result) = scripted.unlock_results.pop_front() {
                return result;
            }
        }

        let deadline = Instant::now() + Duration::from_millis(ORDERLY_LOCK_RPC_TIMEOUT_MILLIS);
        let address = self
            .find_offset_broker(runtime, topic, broker_name, deadline)
            .await
            .map_err(|error| error.to_string())?;
        let address =
            broker_vip_channel(self.config.client_config().vip_channel_enabled(), &address)
                .map_err(|error| error.to_string())?;
        runtime
            .api()
            .unlock_batch_mq(&address, body, false, deadline)
            .await
            .map_err(|error| error.to_string())
    }

    fn orderly_group_fields(
        &self,
        runtime: &ClientRuntime,
    ) -> Result<(String, String), PushConsumerError> {
        let mut config = self.config.client_config().clone();
        let group = config
            .with_namespace(Some(self.config.consumer_group()))?
            .ok_or(PushConsumerError::InvalidConsumerGroup)?;
        Ok((group, runtime.client_id().to_owned()))
    }

    async fn lock_orderly_queues_raw(
        &self,
        runtime: &Arc<ClientRuntime>,
        queues: &[MessageQueue],
    ) -> OrderlyLockBatchResult {
        let mut result = OrderlyLockBatchResult::default();
        let mut groups = BTreeMap::<String, (String, HashSet<Option<MessageQueue>>)>::new();
        for queue in queues {
            let (Some(broker), Some(topic)) = (queue.broker_name(), queue.topic()) else {
                continue;
            };
            let group = groups
                .entry(broker.to_owned())
                .or_insert_with(|| (topic.to_owned(), HashSet::new()));
            group.1.insert(Some(queue.clone()));
        }
        let (consumer_group, client_id) = match self.orderly_group_fields(runtime) {
            Ok(fields) => fields,
            Err(error) => {
                tracing::error!(error = %error, "cannot build orderly lock request identity");
                result.uncertain.extend(queues.iter().cloned());
                return result;
            }
        };

        for (broker_name, (topic, requested)) in groups {
            let requested_queues = requested
                .iter()
                .filter_map(|queue| queue.as_ref().cloned())
                .collect::<Vec<_>>();
            let body = LockBatchRequestBody {
                consumer_group: Some(consumer_group.clone()),
                client_id: Some(client_id.clone()),
                only_this_broker: false,
                mq_set: Some(SharedValue::new(
                    requested
                        .iter()
                        .cloned()
                        .map(|queue| queue.map(SharedValue::new))
                        .collect(),
                )),
            };
            match self
                .request_orderly_lock_batch(runtime, &broker_name, &topic, &body)
                .await
            {
                Ok(Some(locked_queues)) => {
                    result.responded.extend(requested_queues.iter().cloned());
                    let received_at = Instant::now();
                    let received_at_millis = epoch_millis();
                    for queue in requested_queues {
                        if locked_queues.contains(&Some(queue.clone())) {
                            result.locked.insert(queue.clone());
                            result
                                .lock_times
                                .insert(queue, (received_at_millis, received_at));
                        }
                    }
                }
                // Java's LockBatchResponseBody getter is nullable. Its caller
                // treats a null set as an exceptional response and preserves
                // the previous ProcessQueue lock state.
                Ok(None) => {}
                Err(error) => {
                    result.uncertain.extend(requested_queues);
                    tracing::warn!(
                        broker = %broker_name,
                        error,
                        "orderly batch lock request failed"
                    );
                }
            }
        }
        result
    }

    async fn unlock_orderly_queues_raw(
        &self,
        runtime: &Arc<ClientRuntime>,
        queues: &[MessageQueue],
    ) -> HashSet<MessageQueue> {
        let mut groups = BTreeMap::<String, (String, HashSet<Option<MessageQueue>>)>::new();
        for queue in queues {
            let (Some(broker), Some(topic)) = (queue.broker_name(), queue.topic()) else {
                continue;
            };
            let group = groups
                .entry(broker.to_owned())
                .or_insert_with(|| (topic.to_owned(), HashSet::new()));
            group.1.insert(Some(queue.clone()));
        }
        let (consumer_group, client_id) = match self.orderly_group_fields(runtime) {
            Ok(fields) => fields,
            Err(error) => {
                tracing::error!(error = %error, "cannot build orderly unlock request identity");
                return HashSet::new();
            }
        };

        let mut unlocked = HashSet::new();
        for (broker_name, (topic, requested)) in groups {
            let body = UnlockBatchRequestBody {
                consumer_group: Some(consumer_group.clone()),
                client_id: Some(client_id.clone()),
                only_this_broker: false,
                mq_set: Some(SharedValue::new(
                    requested
                        .iter()
                        .cloned()
                        .map(|queue| queue.map(SharedValue::new))
                        .collect(),
                )),
            };
            match self
                .request_orderly_unlock_batch(runtime, &broker_name, &topic, &body)
                .await
            {
                Ok(()) => {
                    unlocked.extend(requested.into_iter().flatten());
                }
                Err(error) => {
                    tracing::warn!(
                        broker = %broker_name,
                        error,
                        "orderly batch unlock request failed"
                    );
                }
            }
        }
        unlocked
    }

    async fn lock_orderly_process_queues(
        &self,
        runtime: &Arc<ClientRuntime>,
        process_queues: &[Arc<ProcessQueue>],
    ) -> HashSet<MessageQueue> {
        let mut attempts = Vec::new();
        for process_queue in process_queues {
            let generation = process_queue.generation();
            if !process_queue
                .snapshot(generation)
                .is_ok_and(|snapshot| !snapshot.dropped)
            {
                continue;
            }
            if let Ok(attempt_id) = process_queue.begin_lock_attempt(generation) {
                attempts.push((
                    process_queue.queue().clone(),
                    process_queue.clone(),
                    generation,
                    attempt_id,
                ));
            }
        }
        let queues = attempts
            .iter()
            .map(|(queue, _, _, _)| queue.clone())
            .collect::<Vec<_>>();
        let lock_result = self.lock_orderly_queues_raw(runtime, &queues).await;
        let mut applied_locked = HashSet::new();
        let mut stale_locked = Vec::new();
        for (queue, process_queue, generation, attempt_id) in attempts {
            if !lock_result.responded.contains(&queue) {
                continue;
            }
            let returned_locked = lock_result.locked.contains(&queue);
            let applied = if returned_locked {
                let Some((timestamp_millis, lock_instant)) =
                    lock_result.lock_times.get(&queue).copied()
                else {
                    continue;
                };
                process_queue.complete_lock_attempt_at(
                    generation,
                    attempt_id,
                    true,
                    timestamp_millis,
                    lock_instant,
                )
            } else {
                process_queue.complete_lock_attempt(generation, attempt_id, false)
            };
            match applied {
                Ok(LockAttemptResult::Applied) if returned_locked => {
                    applied_locked.insert(queue);
                }
                Ok(LockAttemptResult::Dropped) if returned_locked => {
                    stale_locked.push((queue, process_queue, generation));
                }
                Ok(LockAttemptResult::Applied | LockAttemptResult::Superseded)
                | Ok(LockAttemptResult::Dropped)
                | Err(_) => {}
            }
        }
        if !stale_locked.is_empty() {
            let queues = stale_locked
                .iter()
                .map(|(queue, _, _)| queue.clone())
                .collect::<Vec<_>>();
            let unlocked = self.unlock_orderly_queues_raw(runtime, &queues).await;
            for (queue, process_queue, generation) in stale_locked {
                if unlocked.contains(&queue) {
                    let _ = process_queue.record_unlocked(generation);
                    write_lock(&self.pending_orderly_unlocks).remove(&queue);
                } else {
                    write_lock(&self.pending_orderly_unlocks).insert(queue);
                }
            }
        }
        applied_locked
    }

    async fn unlock_orderly_queues_tracking(
        &self,
        runtime: &Arc<ClientRuntime>,
        queues: &[MessageQueue],
    ) -> bool {
        if queues.is_empty() {
            return true;
        }
        write_lock(&self.pending_orderly_unlocks).extend(queues.iter().cloned());
        let unlocked = self.unlock_orderly_queues_raw(runtime, queues).await;
        let mut pending = write_lock(&self.pending_orderly_unlocks);
        for queue in &unlocked {
            pending.remove(queue);
        }
        queues.iter().all(|queue| unlocked.contains(queue))
    }

    async fn unlock_orderly_process_queues(
        &self,
        runtime: &Arc<ClientRuntime>,
        process_queues: &[Arc<ProcessQueue>],
    ) -> bool {
        let mut targets = process_queues
            .iter()
            .map(|process_queue| (process_queue.queue().clone(), Some(process_queue.clone())))
            .collect::<HashMap<_, _>>();
        {
            let pending = read_lock(&self.pending_orderly_unlocks);
            for queue in pending.iter() {
                targets.entry(queue.clone()).or_insert(None);
            }
        }
        let queues = targets.keys().cloned().collect::<Vec<_>>();
        if queues.is_empty() {
            return true;
        }
        write_lock(&self.pending_orderly_unlocks).extend(queues.iter().cloned());
        let unlocked = self.unlock_orderly_queues_raw(runtime, &queues).await;
        for (queue, process_queue) in targets {
            if unlocked.contains(&queue) {
                if let Some(process_queue) = process_queue {
                    let _ = process_queue.record_unlocked(process_queue.generation());
                }
                write_lock(&self.pending_orderly_unlocks).remove(&queue);
            } else if let Some(process_queue) = process_queue {
                let _ = process_queue.increment_try_unlock_times(process_queue.generation());
            }
        }
        queues.iter().all(|queue| unlocked.contains(queue))
    }

    async fn retry_pending_orderly_unlocks(&self, runtime: &Arc<ClientRuntime>) {
        let queues = read_lock(&self.pending_orderly_unlocks)
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let unlocked = self.unlock_orderly_queues_raw(runtime, &queues).await;
        let mut pending = write_lock(&self.pending_orderly_unlocks);
        for queue in unlocked {
            pending.remove(&queue);
        }
    }

    pub(crate) async fn renew_orderly_locks(self: &Arc<Self>) {
        let Some(context) = self.pull_context().await else {
            return;
        };
        if self.current_orderly_lock_settings().await.is_none()
            || self.config.message_model()
                != rocketmq_classic_protocol::heartbeat::MessageModel::Clustering
        {
            return;
        }
        let process_queues = read_lock(&self.process_queues)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        self.lock_orderly_process_queues(&context.runtime, &process_queues)
            .await;
        self.retry_pending_orderly_unlocks(&context.runtime).await;
    }

    fn offset_rpc_deadline(&self) -> Instant {
        Instant::now()
            + Duration::from_millis(
                self.config.client_config().mq_client_api_timeout().max(0) as u64
            )
    }

    async fn notify_assignment_changed(
        &self,
        runtime: &Arc<ClientRuntime>,
        assignment: &DesiredAssignment,
    ) -> bool {
        if let Some(subscription) = write_lock(&self.subscriptions).get_mut(&assignment.topic) {
            subscription.set_sub_version(epoch_millis());
        }
        let mut successful = true;
        if let Err(error) = runtime.send_heartbeat_to_all_brokers().await {
            tracing::warn!(topic = %assignment.topic, error = %error, "push subscription heartbeat after rebalance failed");
            successful = false;
        }
        if let Some(listener) = self.config.message_queue_listener() {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                listener(
                    &assignment.topic,
                    assignment.all_queues.as_ref(),
                    &assignment.assigned_queues,
                );
            }));
            if outcome.is_err() {
                tracing::warn!(topic = %assignment.topic, "push message queue listener panicked");
                successful = false;
            }
        }
        successful
    }

    pub async fn shutdown(&self) -> Result<(), PushConsumerError> {
        let (
            runtime,
            lease,
            offset_store,
            consume_service,
            orderly_lock_settings,
            pull_scheduler,
            pull_rpc_workers,
        ) = {
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
                    orderly_lock_settings,
                    pull_scheduler,
                    pull_rpc_workers,
                } => (
                    runtime.upgrade(),
                    lease,
                    offset_store,
                    consume_service,
                    orderly_lock_settings,
                    pull_scheduler,
                    pull_rpc_workers,
                ),
                State::Created | State::Shutdown => unreachable!("state checked before move"),
            }
        };
        for process_queue in write_lock(&self.pop_process_queues).values() {
            process_queue.drop_assignment();
        }
        write_lock(&self.pop_process_queues).clear();
        let pop_workers = self
            .pop_workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain()
            .map(|(_, worker)| worker)
            .collect::<Vec<_>>();
        for worker in &pop_workers {
            worker.abort();
        }
        for worker in pop_workers {
            let _ = worker.await;
        }
        let pop_consume_workers = self
            .pop_consume_workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
            .collect::<Vec<_>>();
        for mut worker in pop_consume_workers {
            if self.config.await_termination_millis_when_shutdown() > 0
                && tokio::time::timeout(
                    Duration::from_millis(
                        self.config.await_termination_millis_when_shutdown() as u64
                    ),
                    &mut worker,
                )
                .await
                .is_ok()
            {
                continue;
            }
            worker.abort();
            let _ = worker.await;
        }
        let process_queues = read_lock(&self.process_queues)
            .iter()
            .map(|(queue, process_queue)| (queue.clone(), process_queue.clone()))
            .collect::<Vec<_>>();
        for (queue, process_queue) in &process_queues {
            let generation = process_queue.generation();
            let _ = process_queue.drop_assignment(generation);
            pull_scheduler.cancel(queue, generation);
        }
        pull_scheduler.stop().await;
        pull_rpc_workers
            .stop(self.config.await_termination_millis_when_shutdown())
            .await;
        consume_service
            .shutdown(self.config.await_termination_millis_when_shutdown())
            .await;
        let queues = offset_store
            .clone_offset_table(None)
            .into_keys()
            .collect::<Vec<_>>();
        let persist = offset_store.persist_all(&queues).await;
        let orderly_clustering = orderly_lock_settings.is_some()
            && self.config.message_model()
                == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering;
        let unlock_confirmed = if orderly_clustering && persist.is_ok() {
            if let Some(runtime) = runtime.as_ref() {
                let process_queues = process_queues
                    .iter()
                    .map(|(_, process_queue)| process_queue.clone())
                    .collect::<Vec<_>>();
                self.unlock_orderly_process_queues(runtime, &process_queues)
                    .await
            } else {
                false
            }
        } else {
            !orderly_clustering
        };
        write_lock(&self.process_queues).clear();
        let (unregister, shutdown) = if let Some(runtime) = runtime {
            (
                runtime
                    .unregister_consumer(&lease, self.unregister_deadline())
                    .await,
                runtime.shutdown().await,
            )
        } else {
            (Ok(true), Ok(true))
        };
        self.shutdown_trace_dispatcher().await;
        persist?;
        if orderly_clustering && !unlock_confirmed {
            return Err(PushConsumerError::OrderlyUnlockFailed);
        }
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
    fn inbound_processor(
        &self,
        operation: ConsumerOperation,
    ) -> Option<Arc<dyn rocketmq_classic_remoting::client::RequestProcessor>> {
        match operation {
            ConsumerOperation::ResetOffset => {
                Some(Arc::new(crate::admin::ResetOffsetProcessor::new(
                    self.consumer.clone(),
                    self.runtime.clone(),
                )))
            }
            ConsumerOperation::ConsumerStatus => Some(Arc::new(
                crate::admin::ConsumerStatusProcessor::new(self.offset_store.clone()),
            )),
            ConsumerOperation::ConsumeDirectly => {
                Some(Arc::new(crate::admin::DirectConsumeProcessor::new(
                    self.consumer.clone(),
                    self.runtime.clone(),
                )))
            }
            ConsumerOperation::RunningInfo => Some(Arc::new(
                crate::admin::ConsumerRunningInfoProcessor::for_push(
                    self.consumer.clone(),
                    self.offset_store.clone(),
                    self.runtime.clone(),
                ),
            )),
        }
    }

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
        let previous = write_lock(&self.topic_queues).insert(topic.to_owned(), queues.clone());
        if previous.as_ref() != Some(&queues) {
            if let Some(runtime) = self.runtime.upgrade() {
                runtime.rebalance_immediately();
            }
        }
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

    fn try_rebalance(&self) -> Option<Pin<Box<dyn Future<Output = bool> + Send + '_>>> {
        let consumer = self.consumer.clone();
        Some(Box::pin(async move {
            if let Some(consumer) = consumer.upgrade() {
                consumer.rebalance_service.rebalance(&consumer).await
            } else {
                true
            }
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
    consumer: Weak<PushConsumer>,
    runtime: Weak<ClientRuntime>,
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

fn pop_delay_seconds() -> &'static [i32] {
    &[
        10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1_200, 1_800, 3_600, 7_200,
    ]
}

fn pop_batch_is_expired(messages: &[MessageExt], now_millis: i64) -> bool {
    let Some(raw_receipt) = messages
        .first()
        .and_then(|message| message.message().properties().get(PROPERTY_POP_CK))
    else {
        return true;
    };
    let Ok(receipt) = PopReceipt::parse(raw_receipt.clone()) else {
        return true;
    };
    if receipt.pop_time() <= 0 || receipt.invisible_time() <= 0 {
        return true;
    }

    now_millis.saturating_sub(receipt.pop_time()) >= receipt.invisible_time()
}

fn pop_invisible_timeout(messages: &[MessageExt]) -> Duration {
    messages
        .first()
        .and_then(|message| message.message().properties().get(PROPERTY_POP_CK))
        .and_then(|raw| PopReceipt::parse(raw.clone()).ok())
        .map(|receipt| Duration::from_millis(receipt.invisible_time().max(0) as u64))
        .unwrap_or(Duration::MAX)
}

fn orderly_status_name(status: ConsumeOrderlyStatus) -> &'static str {
    match status {
        ConsumeOrderlyStatus::Success => "SUCCESS",
        ConsumeOrderlyStatus::Rollback => "ROLLBACK",
        ConsumeOrderlyStatus::Commit => "COMMIT",
        ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => "SUSPEND_CURRENT_QUEUE_A_MOMENT",
    }
}

fn java_string_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn initial_offset_plan(
    consume_from_where: ConsumeFromWhere,
    retry_topic: bool,
    stored: OffsetRead,
    timestamp: &str,
) -> Result<InitialOffsetPlan, PushConsumerError> {
    match stored {
        OffsetRead::Found(offset) if offset >= 0 => return Ok(InitialOffsetPlan::Stored(offset)),
        OffsetRead::Found(-1) | OffsetRead::NotFound => {}
        OffsetRead::Found(_) => return Err(PushConsumerError::InvalidOffset),
    }
    match consume_from_where {
        ConsumeFromWhere::ConsumeFromFirstOffset => Ok(InitialOffsetPlan::Fixed(0)),
        ConsumeFromWhere::ConsumeFromTimestamp if retry_topic => Ok(InitialOffsetPlan::MaxOffset),
        ConsumeFromWhere::ConsumeFromTimestamp => {
            let timestamp = parse_java_timestamp_millis(timestamp)
                .ok_or(PushConsumerError::InvalidConsumeTimestamp)?;
            Ok(InitialOffsetPlan::SearchTimestamp(timestamp))
        }
        ConsumeFromWhere::ConsumeFromLastOffset
        | ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
        | ConsumeFromWhere::ConsumeFromMinOffset
        | ConsumeFromWhere::ConsumeFromMaxOffset
            if retry_topic =>
        {
            Ok(InitialOffsetPlan::Fixed(0))
        }
        ConsumeFromWhere::ConsumeFromLastOffset
        | ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
        | ConsumeFromWhere::ConsumeFromMinOffset
        | ConsumeFromWhere::ConsumeFromMaxOffset => Ok(InitialOffsetPlan::MaxOffset),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset::OffsetStoreFuture;
    use crate::push::listener::MessageQueueListener;
    use bytes::BytesMut;
    use rocketmq_classic_common::message::{Message, MessageExt};
    use rocketmq_classic_core::api::consumer::PullResultExt;
    use rocketmq_classic_protocol::body::GetConsumerStatusBody;
    use rocketmq_classic_protocol::codes::{request, response};
    use rocketmq_classic_protocol::header::HeaderMap;
    use rocketmq_classic_remoting::client::RequestProcessor;
    use rocketmq_classic_remoting::{
        RemotingCommand,
        codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
    };
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        time::timeout,
    };

    #[test]
    fn orderly_timeout_preserves_java_null_and_exception_priority() {
        assert_eq!(
            orderly_consume_return_type("RETURNNULL", None, true),
            "RETURNNULL"
        );
        assert_eq!(
            orderly_consume_return_type("EXCEPTION", None, true),
            "EXCEPTION"
        );
        assert_eq!(
            orderly_consume_return_type("SUCCESS", Some(ConsumeOrderlyStatus::Success), true),
            "TIME_OUT"
        );
    }

    #[test]
    fn trace_dispatcher_accessor_is_empty_before_start() {
        let config = PushConsumerConfig::new("trace-dispatcher-accessor").unwrap();
        let consumer = PushConsumer::new(config);
        assert!(consumer.trace_dispatcher().is_none());
    }

    #[derive(Default)]
    struct RecordingOffsetStore {
        values: Mutex<HashMap<MessageQueue, i64>>,
        persisted: Mutex<HashMap<MessageQueue, i64>>,
        updates: Mutex<Vec<(MessageQueue, i64, bool)>>,
        fail_persist: Mutex<bool>,
        persist_started: Mutex<Option<Arc<tokio::sync::Barrier>>>,
        persist_release: Mutex<Option<Arc<tokio::sync::Barrier>>>,
    }

    impl RecordingOffsetStore {
        fn seed_persisted(&self, queue: MessageQueue, offset: i64) {
            self.persisted.lock().unwrap().insert(queue, offset);
        }

        fn set_persist_failure(&self, fail: bool) {
            *self.fail_persist.lock().unwrap() = fail;
        }

        fn set_persist_barriers(
            &self,
            started: Arc<tokio::sync::Barrier>,
            release: Arc<tokio::sync::Barrier>,
        ) {
            *self.persist_started.lock().unwrap() = Some(started);
            *self.persist_release.lock().unwrap() = Some(release);
        }
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
            mode: ReadOffsetType,
        ) -> OffsetStoreFuture<'a, OffsetRead> {
            Box::pin(async move {
                let memory = self.values.lock().unwrap().get(queue).copied();
                let stored = self.persisted.lock().unwrap().get(queue).copied();
                Ok(match mode {
                    ReadOffsetType::ReadFromMemory => memory,
                    ReadOffsetType::ReadFromStore => stored,
                    ReadOffsetType::MemoryFirstThenStore => memory.or(stored),
                }
                .map_or(OffsetRead::NotFound, OffsetRead::Found))
            })
        }

        fn persist<'a>(&'a self, queue: &'a MessageQueue) -> OffsetStoreFuture<'a, ()> {
            Box::pin(async move {
                let started = self.persist_started.lock().unwrap().clone();
                if let Some(started) = started {
                    started.wait().await;
                }
                let release = self.persist_release.lock().unwrap().clone();
                if let Some(release) = release {
                    release.wait().await;
                }
                if *self.fail_persist.lock().unwrap() {
                    return Err(OffsetStoreError::Runtime("injected persist failure".into()));
                }
                if let Some(offset) = self.values.lock().unwrap().get(queue).copied() {
                    self.persisted.lock().unwrap().insert(queue.clone(), offset);
                }
                Ok(())
            })
        }

        fn persist_all<'a>(&'a self, queues: &'a [MessageQueue]) -> OffsetStoreFuture<'a, ()> {
            Box::pin(async move {
                if *self.fail_persist.lock().unwrap() {
                    return Err(OffsetStoreError::Runtime("injected persist failure".into()));
                }
                let values = self.values.lock().unwrap();
                let mut persisted = self.persisted.lock().unwrap();
                for queue in queues {
                    if let Some(offset) = values.get(queue) {
                        persisted.insert(queue.clone(), *offset);
                    }
                }
                Ok(())
            })
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

    fn pop_receipt_message(receipt: Option<&str>) -> MessageExt {
        let mut message = MessageExt::new(Message::new("orders", b"body".to_vec()));
        if let Some(receipt) = receipt {
            message
                .message_mut()
                .set_system_property(PROPERTY_POP_CK, receipt)
                .unwrap();
        }
        message
    }

    async fn read_remoting_command(stream: &mut TcpStream) -> RemotingCommand {
        let mut length = [0; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut bytes = BytesMut::from(&length[..]);
        bytes.resize(u32::from_be_bytes(length) as usize + 4, 0);
        stream.read_exact(&mut bytes[4..]).await.unwrap();
        frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
            .unwrap()
            .unwrap()
    }

    async fn write_remoting_reply(
        stream: &mut TcpStream,
        request: &RemotingCommand,
        code: i32,
        fields: Option<HeaderMap>,
    ) {
        write_remoting_reply_with_body(stream, request, code, fields, None).await;
    }

    async fn write_remoting_reply_with_body(
        stream: &mut TcpStream,
        request: &RemotingCommand,
        code: i32,
        fields: Option<HeaderMap>,
        body: Option<Vec<u8>>,
    ) {
        let mut reply = RemotingCommand {
            code,
            opaque: request.opaque,
            ext_fields: fields,
            body,
            ..Default::default()
        };
        reply.mark_response();
        stream
            .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
            .await
            .unwrap();
    }

    fn pop_test_route(address: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "queueDatas": [{
                "brokerName": "broker-a",
                "readQueueNums": 1,
                "writeQueueNums": 1,
                "perm": 6,
                "topicSysFlag": 0
            }],
            "brokerDatas": [{
                "cluster": "cluster",
                "brokerName": "broker-a",
                "brokerAddrs": {"0": address}
            }],
            "filterServerTable": {}
        }))
        .unwrap()
    }

    fn pop_test_message_record() -> Vec<u8> {
        let now = epoch_millis();
        let mut record = Vec::new();
        record.extend_from_slice(&0i32.to_be_bytes());
        record.extend_from_slice(&(-626_843_481i32).to_be_bytes());
        record.extend_from_slice(&0i32.to_be_bytes());
        record.extend_from_slice(&0i32.to_be_bytes());
        record.extend_from_slice(&0i32.to_be_bytes());
        record.extend_from_slice(&10i64.to_be_bytes());
        record.extend_from_slice(&10i64.to_be_bytes());
        record.extend_from_slice(&0i32.to_be_bytes());
        record.extend_from_slice(&now.to_be_bytes());
        record.extend_from_slice(&[127, 0, 0, 1, 0, 0, 0, 0]);
        record.extend_from_slice(&now.to_be_bytes());
        record.extend_from_slice(&[127, 0, 0, 1, 0, 0, 0, 0]);
        record.extend_from_slice(&0i32.to_be_bytes());
        record.extend_from_slice(&0i64.to_be_bytes());
        record.extend_from_slice(&1i32.to_be_bytes());
        record.push(b'x');
        record.push(6);
        record.extend_from_slice(b"orders");
        record.extend_from_slice(&0i16.to_be_bytes());
        let size = i32::try_from(record.len()).unwrap();
        record[..4].copy_from_slice(&size.to_be_bytes());
        record
    }

    async fn accept_remoting_request(
        listener: &TcpListener,
        current: &mut Option<TcpStream>,
    ) -> RemotingCommand {
        loop {
            if current.is_none() {
                *current = Some(listener.accept().await.unwrap().0);
            }
            let stream = current.as_mut().unwrap();
            match timeout(Duration::from_millis(250), read_remoting_command(stream)).await {
                Ok(command) => return command,
                Err(_) => *current = None,
            }
        }
    }

    #[test]
    fn pop_batch_expiry_uses_receipt_invisibility_boundary_and_rejects_invalid_receipts() {
        let message = pop_receipt_message(Some("10 1000 50 2 0 broker-a 0 10"));
        assert!(!pop_batch_is_expired(std::slice::from_ref(&message), 1_049));
        assert!(pop_batch_is_expired(std::slice::from_ref(&message), 1_050));

        assert!(pop_batch_is_expired(&[], 1_049));
        assert!(pop_batch_is_expired(&[pop_receipt_message(None)], 1_049));
        for receipt in [
            "invalid receipt",
            "10 invalid 50 2 0 broker-a 0 10",
            "10 1000 50",
            "10 0 50 2 0 broker-a 0 10",
            "10 1000 0 2 0 broker-a 0 10",
        ] {
            assert!(
                pop_batch_is_expired(&[pop_receipt_message(Some(receipt))], 1_049),
                "receipt should be treated as expired: {receipt}"
            );
        }
    }

    #[tokio::test]
    async fn expired_pop_batch_skips_listener_and_releases_found_message_count() {
        let mut config = PushConsumerConfig::new("expired-pop-batch").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config.client_config())
            .await
            .unwrap();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(PopProcessQueue::new(queue.clone(), 1));
        process_queue.inc_found_messages(1);
        let listener_called = Arc::new(AtomicBool::new(false));
        let listener = PushMessageListener::concurrently({
            let listener_called = listener_called.clone();
            move |_messages, context| {
                listener_called.store(true, Ordering::Release);
                std::future::ready((Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context))
            }
        });

        consumer
            .clone()
            .consume_pop_batch(
                PopConsumeBatchContext {
                    process_queue: process_queue.clone(),
                    runtime: runtime.clone(),
                    queue,
                    route_topic: "orders".to_owned(),
                    consumer_group: "expired-pop-batch".to_owned(),
                },
                listener,
                vec![pop_receipt_message(Some("10 1000 50 2 0 broker-a 0 10"))],
            )
            .await;

        assert!(!listener_called.load(Ordering::Acquire));
        assert_eq!(process_queue.wait_ack_count(), 0);
        let _ = runtime.shutdown().await;
    }

    #[tokio::test]
    async fn pop_consume_dispatches_ack_redelivery_and_replacement_receipt() {
        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nameserver_address = nameserver.local_addr().unwrap().to_string();
        let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker_address = broker_listener.local_addr().unwrap().to_string();
        let route = pop_test_route(&broker_address);

        let nameserver_peer = tokio::spawn(async move {
            let (mut stream, _) = nameserver.accept().await.unwrap();
            let incoming = read_remoting_command(&mut stream).await;
            assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: incoming.opaque,
                body: Some(route),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
                .await
                .unwrap();
        });

        let now = epoch_millis();
        let initial_receipt = format!("10 {now} 60000 2 0 broker-a 0 10");
        let redelivery_receipt = format!("10 {now} 30000 2 0 broker-a 0 10");
        let replacement_receipt = format!("10 {} 45000 3 0 broker-a 0 10", now + 1);
        let expected_receipts = [
            initial_receipt.clone(),
            initial_receipt.clone(),
            redelivery_receipt.clone(),
            replacement_receipt.clone(),
        ];
        let broker_peer = tokio::spawn(async move {
            let mut stream = None;
            let expected_codes = [
                request::ACK_MESSAGE,
                request::CHANGE_MESSAGE_INVISIBLETIME,
                request::CHANGE_MESSAGE_INVISIBLETIME,
                request::ACK_MESSAGE,
            ];
            for (index, expected_code) in expected_codes.into_iter().enumerate() {
                let incoming = accept_remoting_request(&broker_listener, &mut stream).await;
                assert_eq!(incoming.code, expected_code);
                let fields = incoming.ext_fields.as_ref().unwrap();
                assert_eq!(
                    fields.get("consumerGroup"),
                    Some(&"pop-lifecycle-group".into())
                );
                assert_eq!(fields.get("topic"), Some(&"orders".into()));
                assert_eq!(fields.get("queueId"), Some(&"0".into()));
                assert_eq!(fields.get("offset"), Some(&"10".into()));
                assert_eq!(fields.get("extraInfo"), Some(&expected_receipts[index]));
                if expected_code == request::CHANGE_MESSAGE_INVISIBLETIME {
                    let (pop_time, invisible_time, revive_qid) = if index == 1 {
                        (now, 30_000, 2)
                    } else {
                        (now + 1, 45_000, 3)
                    };
                    assert_eq!(
                        fields.get("invisibleTime"),
                        Some(&(if index == 1 { "10000" } else { "30000" }).to_owned())
                    );
                    write_remoting_reply(
                        stream.as_mut().unwrap(),
                        &incoming,
                        response::SUCCESS,
                        Some(HeaderMap::from([
                            ("popTime".into(), pop_time.to_string()),
                            ("invisibleTime".into(), invisible_time.to_string()),
                            ("reviveQid".into(), revive_qid.to_string()),
                        ])),
                    )
                    .await;
                } else {
                    write_remoting_reply(
                        stream.as_mut().unwrap(),
                        &incoming,
                        response::SUCCESS,
                        None,
                    )
                    .await;
                }
            }
        });

        let mut config = PushConsumerConfig::new("pop-lifecycle-group").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(nameserver_address));
        config
            .client_config_mut()
            .set_instance_name(Some("pop-lifecycle-client".into()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let pop_hook_events = Arc::new(Mutex::new(Vec::new()));
        struct PopHook(Arc<Mutex<Vec<String>>>);
        impl ConsumeMessageHook for PopHook {
            fn hook_name(&self) -> &str {
                "pop-consume-hook"
            }

            fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push("before".into());
            }

            fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push(format!(
                    "{}:{}",
                    context.status.as_deref().unwrap_or("missing"),
                    context.success
                ));
            }
        }
        consumer.register_consume_message_hook(Arc::new(PopHook(pop_hook_events.clone())));
        let runtime = manager
            .get_or_create(consumer.config.client_config())
            .await
            .unwrap();
        runtime.remoting().start().unwrap();
        runtime
            .update_topic_route("orders", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        nameserver_peer.await.unwrap();

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let successful_queue = Arc::new(PopProcessQueue::new(queue.clone(), 1));
        successful_queue.inc_found_messages(1);
        let listener_calls = Arc::new(AtomicUsize::new(0));
        let successful_listener = PushMessageListener::concurrently({
            let listener_calls = listener_calls.clone();
            move |_messages, context| {
                listener_calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready((Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context))
            }
        });
        consumer
            .clone()
            .consume_pop_batch(
                PopConsumeBatchContext {
                    process_queue: successful_queue.clone(),
                    runtime: runtime.clone(),
                    queue: queue.clone(),
                    route_topic: "orders".into(),
                    consumer_group: "pop-lifecycle-group".into(),
                },
                successful_listener,
                vec![pop_receipt_message(Some(&initial_receipt))],
            )
            .await;
        assert_eq!(listener_calls.load(Ordering::SeqCst), 1);
        assert_eq!(successful_queue.wait_ack_count(), 0);

        let retry_queue = Arc::new(PopProcessQueue::new(queue.clone(), 2));
        retry_queue.inc_found_messages(1);
        let retry_listener = PushMessageListener::concurrently(|_messages, context| {
            std::future::ready((Some(ConsumeConcurrentlyStatus::ReconsumeLater), context))
        });
        consumer
            .clone()
            .consume_pop_batch(
                PopConsumeBatchContext {
                    process_queue: retry_queue.clone(),
                    runtime: runtime.clone(),
                    queue: queue.clone(),
                    route_topic: "orders".into(),
                    consumer_group: "pop-lifecycle-group".into(),
                },
                retry_listener,
                vec![pop_receipt_message(Some(&initial_receipt))],
            )
            .await;
        assert_eq!(retry_queue.wait_ack_count(), 0);
        assert_eq!(
            pop_hook_events.lock().unwrap().as_slice(),
            [
                "before",
                "CONSUME_SUCCESS:true",
                "before",
                "RECONSUME_LATER:false"
            ]
        );

        let mut changed_message = pop_receipt_message(Some(&redelivery_receipt));
        consumer
            .change_pop_invisible_time(
                &runtime,
                &mut changed_message,
                "orders",
                "pop-lifecycle-group",
                30_000,
            )
            .await;
        assert_eq!(
            changed_message.message().properties().get(PROPERTY_POP_CK),
            Some(&replacement_receipt)
        );
        assert_eq!(
            consumer
                .ack_pop_message(&runtime, &changed_message, "orders", "pop-lifecycle-group")
                .await
                .unwrap(),
            AckStatus::Ok
        );

        let status =
            global_consumer_stats().status(runtime.client_id(), "pop-lifecycle-group", "orders");
        assert!(status.consume_ok_tps > 0.0);

        broker_peer.await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn broker_assignment_error_preserves_worker_until_empty_assignment() {
        let nameserver = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let nameserver_address = nameserver.local_addr().unwrap().to_string();
        let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let broker_address = broker_listener.local_addr().unwrap().to_string();
        let route = pop_test_route(&broker_address);
        let nameserver_peer = tokio::spawn(async move {
            let (mut stream, _) = nameserver.accept().await.unwrap();
            let incoming = read_remoting_command(&mut stream).await;
            assert_eq!(incoming.code, request::GET_ROUTEINFO_BY_TOPIC);
            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: incoming.opaque,
                body: Some(route),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply, DEFAULT_MAX_FRAME_SIZE).unwrap())
                .await
                .unwrap();
        });

        let pop_request_count = Arc::new(AtomicUsize::new(0));
        let peer_pop_request_count = pop_request_count.clone();
        let broker_peer = tokio::spawn(async move {
            let mut stream = None;
            let mut assignment_calls = 0;
            loop {
                let incoming = accept_remoting_request(&broker_listener, &mut stream).await;
                match incoming.code {
                    request::QUERY_ASSIGNMENT => {
                        let body: serde_json::Value = serde_json::from_slice(
                            incoming.body.as_deref().expect("assignment request body"),
                        )
                        .unwrap();
                        assert_eq!(body["topic"], "orders");
                        assert_eq!(body["consumerGroup"], "pop-assignment-flow");
                        assignment_calls += 1;
                        if assignment_calls == 2 {
                            write_remoting_reply(
                                stream.as_mut().unwrap(),
                                &incoming,
                                response::SYSTEM_ERROR,
                                None,
                            )
                            .await;
                            continue;
                        }
                        let assignments = if assignment_calls == 1 {
                            serde_json::json!([{
                                "messageQueue": {
                                    "topic": "orders",
                                    "brokerName": "broker-a",
                                    "queueId": 0
                                },
                                "mode": "POP"
                            }])
                        } else {
                            serde_json::json!([])
                        };
                        write_remoting_reply_with_body(
                            stream.as_mut().unwrap(),
                            &incoming,
                            response::SUCCESS,
                            None,
                            Some(
                                serde_json::to_vec(&serde_json::json!({
                                    "messageQueueAssignments": assignments
                                }))
                                .unwrap(),
                            ),
                        )
                        .await;
                    }
                    request::POP_MESSAGE => {
                        if peer_pop_request_count.fetch_add(1, Ordering::SeqCst) == 0 {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            write_remoting_reply_with_body(
                                stream.as_mut().unwrap(),
                                &incoming,
                                response::SUCCESS,
                                Some(HeaderMap::from([
                                    ("popTime".into(), epoch_millis().to_string()),
                                    ("invisibleTime".into(), "60000".into()),
                                    ("reviveQid".into(), "2".into()),
                                    ("restNum".into(), "0".into()),
                                ])),
                                Some(pop_test_message_record()),
                            )
                            .await;
                        } else {
                            write_remoting_reply(
                                stream.as_mut().unwrap(),
                                &incoming,
                                response::PULL_NOT_FOUND,
                                None,
                            )
                            .await;
                        }
                    }
                    request::HEART_BEAT
                    | request::UNREGISTER_CLIENT
                    | request::CHECK_CLIENT_CONFIG => {
                        write_remoting_reply(
                            stream.as_mut().unwrap(),
                            &incoming,
                            response::SUCCESS,
                            None,
                        )
                        .await;
                    }
                    code => panic!("unexpected scripted Broker request code {code}"),
                }
            }
        });

        let mut config = PushConsumerConfig::new("pop-assignment-flow").unwrap();
        config.set_client_rebalance(false);
        config
            .client_config_mut()
            .set_namesrv_addr(Some(nameserver_address));
        config
            .client_config_mut()
            .set_instance_name(Some("pop-assignment-client".into()));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            Arc::new(RecordingOffsetStore::default()),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        runtime.remoting().start().unwrap();
        runtime
            .update_topic_route("orders", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        nameserver_peer.await.unwrap();

        let queue = MessageQueue::new("orders", "broker-a", 0);
        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        timeout(Duration::from_secs(3), async {
            while pop_request_count.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("assigned POP worker did not issue POP_MESSAGE");
        timeout(Duration::from_secs(3), async {
            while global_consumer_stats()
                .status(runtime.client_id(), "pop-assignment-flow", "orders")
                .pull_rt
                <= 0.0
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("POP_FOUND must record pull latency in consumer statistics");
        let status =
            global_consumer_stats().status(runtime.client_id(), "pop-assignment-flow", "orders");
        assert!(status.pull_rt > 0.0, "unexpected POP stats: {status:?}");
        assert!(status.pull_tps > 0.0, "unexpected POP stats: {status:?}");
        let process_queue = read_lock(&consumer.pop_process_queues)
            .get(&queue)
            .cloned()
            .expect("Broker POP assignment must activate its queue");
        assert!(!process_queue.is_dropped());
        assert!(
            consumer
                .pop_workers
                .lock()
                .unwrap()
                .get(&queue)
                .is_some_and(|worker| !worker.is_finished())
        );

        assert!(!consumer.rebalance_service.rebalance(&consumer).await);
        assert!(
            !process_queue.is_dropped(),
            "an unavailable Broker assignment is not an authoritative empty assignment"
        );
        assert!(
            consumer
                .pop_workers
                .lock()
                .unwrap()
                .get(&queue)
                .is_some_and(|worker| !worker.is_finished())
        );

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        assert!(process_queue.is_dropped());
        assert!(!read_lock(&consumer.pop_process_queues).contains_key(&queue));
        assert!(!consumer.pop_workers.lock().unwrap().contains_key(&queue));
        let pop_count_after_removal = pop_request_count.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            pop_request_count.load(Ordering::SeqCst),
            pop_count_after_removal,
            "removed POP workers must not issue more Broker requests"
        );

        consumer.shutdown().await.unwrap();
        broker_peer.abort();
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
        install_started_consumer_with_service(
            consumer,
            manager,
            offset_store,
            ConsumeService::Concurrent(consume_service),
            None,
            pull_scheduler,
            pull_rpc_workers,
        )
        .await
    }

    async fn install_started_consumer_with_service(
        consumer: &Arc<PushConsumer>,
        manager: &ClientRuntimeManager,
        offset_store: Arc<dyn OffsetStore>,
        consume_service: ConsumeService,
        orderly_lock_settings: Option<OrderlyLockSettings>,
        pull_scheduler: Arc<PullScheduler>,
        pull_rpc_workers: Arc<PullRpcWorkers>,
    ) -> Arc<ClientRuntime> {
        consumer.consume_orderly.store(
            matches!(&consume_service, ConsumeService::Orderly(_)),
            Ordering::Release,
        );
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
            consumer: Arc::downgrade(consumer),
            runtime: Arc::downgrade(&runtime),
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
            orderly_lock_settings,
            pull_scheduler: pull_scheduler.clone(),
            pull_rpc_workers: pull_rpc_workers.clone(),
        };
        consume_service
            .start(
                Arc::downgrade(consumer),
                orderly_lock_settings,
                orderly_lock_settings.is_some()
                    && consumer.config.message_model()
                        == rocketmq_classic_protocol::heartbeat::MessageModel::Clustering,
            )
            .await;
        pull_scheduler.start(Arc::downgrade(&runtime)).unwrap();
        pull_rpc_workers
            .start(Arc::downgrade(consumer), pull_scheduler)
            .unwrap();
        runtime
    }

    async fn install_started_orderly_consumer(
        consumer: &Arc<PushConsumer>,
        manager: &ClientRuntimeManager,
        offset_store: Arc<dyn OffsetStore>,
        listener: Arc<dyn MessageListenerOrderly>,
        orderly_lock_settings: Option<OrderlyLockSettings>,
    ) -> Arc<ClientRuntime> {
        let orderly_lock_settings = Some(orderly_lock_settings.unwrap_or_else(|| {
            OrderlyLockSettings::from_values(30_000, 20_000)
                .expect("Java default orderly lock settings are valid")
        }));
        let service = Arc::new(OrderlyConsumeService::new(
            consumer.config.consume_thread_min(),
            consumer.config.consume_thread_max(),
            listener,
        ));
        install_started_consumer_with_service(
            consumer,
            manager,
            offset_store,
            ConsumeService::Orderly(service),
            orderly_lock_settings,
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await
    }

    fn script_rebalance_membership(
        consumer: &PushConsumer,
        topic: &str,
        results: impl IntoIterator<Item = Option<Vec<String>>>,
    ) {
        consumer
            .rebalance_membership
            .lock()
            .unwrap()
            .entry(topic.to_owned())
            .or_default()
            .extend(results);
    }

    #[test]
    fn java_string_order_and_consume_timestamp_parsing_match_utility_semantics() {
        assert_eq!(
            java_string_cmp("\u{10000}", "\u{e000}"),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            java_string_cmp("client-a", "client-b"),
            std::cmp::Ordering::Less
        );
        assert!(parse_java_timestamp_millis("20240229010203").is_some());
        assert!(parse_java_timestamp_millis("20230229010203").is_some());
        assert!(parse_java_timestamp_millis("2024022901020x").is_some());
        assert!(parse_java_timestamp_millis("+20240229010203").is_none());
    }

    #[test]
    fn java_initial_offset_fixtures_match_policy_and_retry_topic_selection() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/rebalance/initial-offsets.json"
        ))
        .unwrap();
        let max_offset = fixture["mockMaxOffset"].as_i64().unwrap();
        let search_offset = fixture["mockSearchOffset"].as_i64().unwrap();
        let mut checked = 0;
        for case in fixture["cases"].as_array().unwrap() {
            checked += 1;
            let policy = match case["consumeFromWhere"].as_str().unwrap() {
                "CONSUME_FROM_LAST_OFFSET" => ConsumeFromWhere::ConsumeFromLastOffset,
                "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST" => {
                    ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
                }
                "CONSUME_FROM_MIN_OFFSET" => ConsumeFromWhere::ConsumeFromMinOffset,
                "CONSUME_FROM_MAX_OFFSET" => ConsumeFromWhere::ConsumeFromMaxOffset,
                "CONSUME_FROM_FIRST_OFFSET" => ConsumeFromWhere::ConsumeFromFirstOffset,
                "CONSUME_FROM_TIMESTAMP" => ConsumeFromWhere::ConsumeFromTimestamp,
                other => panic!("unknown fixture consume policy {other}"),
            };
            let topic = case["topic"].as_str().unwrap();
            let stored_offset = case["storedOffset"].as_i64().unwrap();
            let stored = if stored_offset == -1 {
                OffsetRead::NotFound
            } else {
                OffsetRead::Found(stored_offset)
            };
            let plan = initial_offset_plan(
                policy,
                topic.starts_with("%RETRY%"),
                stored,
                case["consumeTimestamp"].as_str().unwrap(),
            );
            if case.get("errorCode").is_some() {
                assert!(matches!(plan, Err(PushConsumerError::InvalidOffset)));
                continue;
            }
            let plan = plan.unwrap();
            let (query, actual_offset) = match plan {
                InitialOffsetPlan::Stored(offset) => ("NONE", offset),
                InitialOffsetPlan::Fixed(offset) => ("NONE", offset),
                InitialOffsetPlan::MaxOffset => ("MAX_OFFSET", max_offset),
                InitialOffsetPlan::SearchTimestamp(_) => ("SEARCH_OFFSET", search_offset),
            };
            assert_eq!(query, case["query"].as_str().unwrap(), "{}", case["id"]);
            assert_eq!(
                actual_offset,
                case["result"].as_i64().unwrap(),
                "{}",
                case["id"]
            );
        }
        assert_eq!(checked, 24);
    }

    #[tokio::test]
    async fn initial_offsets_prefer_store_and_never_turn_query_failures_into_zero() {
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let policies = [
            ConsumeFromWhere::ConsumeFromLastOffset,
            ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst,
            ConsumeFromWhere::ConsumeFromMinOffset,
            ConsumeFromWhere::ConsumeFromMaxOffset,
            ConsumeFromWhere::ConsumeFromFirstOffset,
            ConsumeFromWhere::ConsumeFromTimestamp,
        ];
        for policy in policies {
            let mut config = PushConsumerConfig::new("initial-offset-stored").unwrap();
            config.client_config_mut().set_namesrv_addr(None);
            config.set_consume_from_where(policy);
            let manager = ClientRuntimeManager::new();
            let consumer = PushConsumer::with_manager(config, manager.clone());
            let runtime = manager
                .get_or_create(consumer.config.client_config())
                .await
                .unwrap();
            let store = Arc::new(RecordingOffsetStore::default());
            store.seed_persisted(queue.clone(), 73);
            let store_dyn: Arc<dyn OffsetStore> = store.clone();
            assert_eq!(
                consumer
                    .compute_initial_offset(&runtime, &store_dyn, &queue)
                    .await
                    .unwrap(),
                73,
                "stored offset must take precedence for {policy:?}"
            );
            let _ = runtime.shutdown().await;
        }

        for (policy, topic, expected) in [
            (ConsumeFromWhere::ConsumeFromFirstOffset, "orders", Some(0)),
            (
                ConsumeFromWhere::ConsumeFromLastOffset,
                "%RETRY%group",
                Some(0),
            ),
            (
                ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst,
                "%RETRY%group",
                Some(0),
            ),
            (
                ConsumeFromWhere::ConsumeFromMinOffset,
                "%RETRY%group",
                Some(0),
            ),
            (
                ConsumeFromWhere::ConsumeFromMaxOffset,
                "%RETRY%group",
                Some(0),
            ),
            (ConsumeFromWhere::ConsumeFromTimestamp, "%RETRY%group", None),
            (ConsumeFromWhere::ConsumeFromLastOffset, "orders", None),
            (ConsumeFromWhere::ConsumeFromTimestamp, "orders", None),
        ] {
            let mut config = PushConsumerConfig::new("initial-offset-missing").unwrap();
            config.client_config_mut().set_namesrv_addr(None);
            config.set_consume_from_where(policy);
            config.set_consume_timestamp("20240229010203");
            let manager = ClientRuntimeManager::new();
            let consumer = PushConsumer::with_manager(config, manager.clone());
            let runtime = manager
                .get_or_create(consumer.config.client_config())
                .await
                .unwrap();
            let store = Arc::new(RecordingOffsetStore::default());
            let store_dyn: Arc<dyn OffsetStore> = store.clone();
            let queue = MessageQueue::new(topic, "broker-a", 0);
            let result = consumer
                .compute_initial_offset(&runtime, &store_dyn, &queue)
                .await;
            match expected {
                Some(expected) => assert_eq!(result.unwrap(), expected, "{policy:?} {topic}"),
                None => assert!(
                    result.is_err(),
                    "{policy:?} {topic} must retain the query error"
                ),
            }
            assert!(store.values.lock().unwrap().is_empty());
            let _ = runtime.shutdown().await;
        }
    }

    #[tokio::test]
    async fn broadcasting_rebalance_activates_every_queue_with_local_offsets_and_notifies() {
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        let mut config = PushConsumerConfig::new("rebalance-broadcast").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_client_rebalance(false);
        config.set_message_queue_listener(Some(queue_listener));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let scheduler = Arc::new(PullScheduler::new());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn,
            idle_consume_service(),
            scheduler,
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let first = MessageQueue::new("orders", "broker-a", 0);
        let second = MessageQueue::new("orders", "broker-a", 1);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues).insert(
            "orders".to_owned(),
            BTreeSet::from([first.clone(), second.clone()]),
        );
        store.seed_persisted(first.clone(), 17);
        store.seed_persisted(second.clone(), 29);
        consumer.paused.store(true, Ordering::Release);

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        {
            let queues = read_lock(&consumer.process_queues);
            assert_eq!(queues.len(), 2);
            assert!(queues.contains_key(&first) && queues.contains_key(&second));
        }
        assert_eq!(store.values.lock().unwrap().get(&first), Some(&17));
        assert_eq!(store.values.lock().unwrap().get(&second), Some(&29));
        {
            let changes = changes.lock().unwrap();
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].0, "orders");
            assert_eq!(
                changes[0].1,
                Some(HashSet::from([first.clone(), second.clone()]))
            );
            assert_eq!(changes[0].2, HashSet::from([first, second]));
        }
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn missing_clustering_route_queries_membership_notifies_empty_and_keeps_assignment() {
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        let mut config = PushConsumerConfig::new("rebalance-route-missing").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_queue_listener(Some(queue_listener));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 37));
        store.update_offset(&queue, 45, false);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        script_rebalance_membership(&consumer, "orders", [Some(vec!["peer-a".to_owned()])]);
        consumer.paused.store(true, Ordering::Release);

        assert!(!consumer.rebalance_service.rebalance(&consumer).await);
        assert!(
            read_lock(&consumer.process_queues)
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
        );
        assert!(!process_queue.snapshot(37).unwrap().dropped);
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&45));
        assert_eq!(
            consumer
                .rebalance_membership
                .lock()
                .unwrap()
                .get("orders")
                .unwrap()
                .len(),
            1,
            "missing route data is handled before membership lookup"
        );
        {
            let changes = changes.lock().unwrap();
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].0, "orders");
            assert_eq!(changes[0].1, Some(HashSet::new()));
            assert!(changes[0].2.is_empty());
        }
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn empty_clustering_route_notifies_without_querying_membership_or_dropping_assignment() {
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        let mut config = PushConsumerConfig::new("rebalance-route-empty").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_queue_listener(Some(queue_listener));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 37));
        store.update_offset(&queue, 45, false);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues).insert("orders".to_owned(), BTreeSet::new());
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        script_rebalance_membership(&consumer, "orders", [Some(vec!["peer-a".to_owned()])]);
        consumer.paused.store(true, Ordering::Release);

        assert!(!consumer.rebalance_service.rebalance(&consumer).await);
        assert!(
            read_lock(&consumer.process_queues)
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
        );
        assert!(!process_queue.snapshot(37).unwrap().dropped);
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&45));
        assert_eq!(
            consumer
                .rebalance_membership
                .lock()
                .unwrap()
                .get("orders")
                .unwrap()
                .len(),
            1,
            "an empty route does not query membership"
        );
        {
            let changes = changes.lock().unwrap();
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].0, "orders");
            assert_eq!(changes[0].1, Some(HashSet::new()));
            assert!(changes[0].2.is_empty());
        }
        consumer.shutdown().await.unwrap();
    }

    #[test]
    fn broker_assignment_partition_preserves_mixed_pull_pop_modes() {
        let pull_queue = MessageQueue::new("orders", "broker-a", 0);
        let pop_queue = MessageQueue::new("orders", "broker-a", 1);
        let default_pull_queue = MessageQueue::new("orders", "broker-a", 2);
        let assignments = [
            Some(MessageQueueAssignment {
                message_queue: Some(SharedValue::new(pull_queue.clone())),
                mode: Some(MessageRequestMode::Pull),
                attachments: None,
            }),
            Some(MessageQueueAssignment {
                message_queue: Some(SharedValue::new(pop_queue.clone())),
                mode: Some(MessageRequestMode::Pop),
                attachments: None,
            }),
            Some(MessageQueueAssignment {
                message_queue: Some(SharedValue::new(default_pull_queue.clone())),
                mode: None,
                attachments: None,
            }),
            Some(MessageQueueAssignment {
                message_queue: None,
                mode: Some(MessageRequestMode::Pop),
                attachments: None,
            }),
        ];

        let partition = partition_broker_assignments(assignments).unwrap();
        assert_eq!(
            partition.assigned,
            HashSet::from([
                pull_queue.clone(),
                pop_queue.clone(),
                default_pull_queue.clone()
            ])
        );
        assert_eq!(
            partition.pull,
            HashSet::from([pull_queue, default_pull_queue])
        );
        assert_eq!(partition.pop, HashSet::from([pop_queue]));
    }

    #[test]
    fn broker_assignment_with_a_null_entry_is_not_authoritative() {
        assert!(partition_broker_assignments([None]).is_none());
    }

    #[test]
    fn pop_to_pull_transition_uses_the_java_retry_topic_and_removes_its_route() {
        let mut config = PushConsumerConfig::new("retry-transition").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let consumer = PushConsumer::new(config);
        let retry_topic = "%RETRY%retry-transition_orders";

        consumer.subscribe_pop_retry_topic("orders", "retry-transition");
        assert!(read_lock(&consumer.subscriptions).contains_key(retry_topic));
        write_lock(&consumer.topic_queues).insert(
            retry_topic.to_owned(),
            BTreeSet::from([MessageQueue::new(retry_topic, "broker-a", 0)]),
        );

        consumer.unsubscribe_pop_retry_topic("orders", "retry-transition");
        assert!(!read_lock(&consumer.subscriptions).contains_key(retry_topic));
        assert!(!read_lock(&consumer.topic_queues).contains_key(retry_topic));
    }

    #[tokio::test]
    async fn lite_selective_rebalance_leaves_assignments_for_pop_handling() {
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let queue_listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        let mut config = PushConsumerConfig::new("rebalance-lite-selective").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::LiteSelective);
        config.set_message_queue_listener(Some(queue_listener));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 41));
        store.update_offset(&queue, 52, false);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues)
            .insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        script_rebalance_membership(&consumer, "orders", [Some(vec!["peer-a".to_owned()])]);
        consumer.paused.store(true, Ordering::Release);

        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        assert!(
            read_lock(&consumer.process_queues)
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
        );
        assert_eq!(
            consumer
                .rebalance_membership
                .lock()
                .unwrap()
                .get("orders")
                .unwrap()
                .len(),
            1,
            "LiteSelective must not enter the clustering allocator"
        );
        assert!(changes.lock().unwrap().is_empty());
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unavailable_membership_keeps_the_previous_queue_assignment() {
        let mut config = PushConsumerConfig::new("rebalance-membership-unavailable").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 31));
        store.update_offset(&queue, 45, false);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues)
            .insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        assert!(!consumer.rebalance_service.rebalance(&consumer).await);
        assert!(
            read_lock(&consumer.process_queues)
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
        );
        assert!(!process_queue.snapshot(31).unwrap().dropped);
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&45));
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn client_rebalance_false_uses_broker_assignment_unless_orderly_forces_local() {
        let mut config = PushConsumerConfig::new("rebalance-client-disabled").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_client_rebalance(false);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.seed_persisted(queue.clone(), 19);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues)
            .insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let client_id = runtime.client_id().to_owned();
        script_rebalance_membership(&consumer, "orders", [Some(vec![client_id])]);
        consumer.paused.store(true, Ordering::Release);

        assert!(!consumer.rebalance_service.rebalance(&consumer).await);
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        assert_eq!(
            consumer
                .rebalance_membership
                .lock()
                .unwrap()
                .get("orders")
                .unwrap()
                .len(),
            1,
            "broker assignment mode must not request the local membership list"
        );

        consumer.consume_orderly.store(true, Ordering::Release);
        assert!(consumer.rebalance_service.rebalance(&consumer).await);
        assert!(read_lock(&consumer.process_queues).contains_key(&queue));
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn clustering_membership_join_leave_and_unavailable_cycles_converge() {
        let mut config = PushConsumerConfig::new("rebalance-membership-churn").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let queues = (0..4)
            .map(|queue_id| MessageQueue::new("orders", "broker-a", queue_id))
            .collect::<Vec<_>>();
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues)
            .insert("orders".to_owned(), queues.iter().cloned().collect());
        for (index, queue) in queues.iter().enumerate() {
            store.seed_persisted(queue.clone(), 100 + index as i64);
        }
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let runtime = match &*consumer.state.lock().await {
            State::Started { runtime, .. } => runtime.upgrade().unwrap(),
            State::Created | State::Shutdown => unreachable!(),
        };
        consumer.paused.store(true, Ordering::Release);
        let current_id = runtime.client_id().to_owned();
        let all = queues.clone();
        let expected = |ids: &[&str]| {
            let mut ids = ids.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>();
            ids.sort_by(|left, right| java_string_cmp(left, right));
            consumer
                .config
                .allocate_message_queue_strategy()
                .allocate(consumer.config.consumer_group(), &current_id, &all, &ids)
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>()
        };

        let peer = "peer-z";
        script_rebalance_membership(
            &consumer,
            "orders",
            [
                Some(vec![current_id.clone(), peer.to_owned()]),
                Some(vec![current_id.clone()]),
                Some(vec![
                    "client-a".to_owned(),
                    "client-b".to_owned(),
                    current_id.clone(),
                ]),
                None,
                Some(vec![peer.to_owned()]),
                Some(vec![current_id.clone()]),
            ],
        );
        let expected_sets = [
            expected(&[&current_id, peer]),
            expected(&[&current_id]),
            expected(&["client-a", "client-b", &current_id]),
            expected(&["client-a", "client-b", &current_id]),
            HashSet::new(),
            expected(&[&current_id]),
        ];
        for (round, expected) in expected_sets.iter().enumerate() {
            let balanced = consumer.rebalance_service.rebalance(&consumer).await;
            if round == 3 {
                assert!(!balanced, "unavailable membership must request a retry");
            } else {
                assert!(balanced, "membership round {round} should converge");
            }
            let working = read_lock(&consumer.process_queues)
                .iter()
                .filter(|(queue, process_queue)| {
                    queue.topic() == Some("orders")
                        && process_queue
                            .snapshot(process_queue.generation())
                            .is_ok_and(|snapshot| !snapshot.dropped)
                })
                .map(|(queue, _)| queue.clone())
                .collect::<HashSet<_>>();
            assert_eq!(&working, expected, "membership round {round}");
        }
        assert_eq!(store.persisted.lock().unwrap().len(), queues.len());
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn queue_removal_fences_a_listener_already_in_flight() {
        let mut config = PushConsumerConfig::new("rebalance-listener-race").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let scheduler = Arc::new(PullScheduler::new());
        let consume_service = idle_consume_service();
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn.clone(),
            consume_service.clone(),
            scheduler.clone(),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 50, false);
        let request = consume_request(queue.clone(), 72, vec![message(50)]);
        let process_queue = request.process_queue.clone();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let listener_started = Arc::new(tokio::sync::Barrier::new(2));
        let listener_release = Arc::new(tokio::sync::Barrier::new(2));
        let started = listener_started.clone();
        let release = listener_release.clone();
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let started = started.clone();
                let release = release.clone();
                async move {
                    started.wait().await;
                    release.wait().await;
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        );
        let running_consumer = consumer.clone();
        let listener_task = tokio::spawn(async move {
            running_consumer
                .run_consume_request(request, listener)
                .await;
        });
        listener_started.wait().await;
        process_queue.drop_assignment(72).unwrap();
        scheduler.cancel(&queue, 72);
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: store_dyn,
            scheduler,
            consume_service: ConsumeService::Concurrent(consume_service),
        };
        assert!(
            consumer
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
        );
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        listener_release.wait().await;
        listener_task.await.unwrap();
        assert_eq!(store.values.lock().unwrap().get(&queue), None);
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reset_offset_fences_a_slow_callback_and_persists_only_the_new_value() {
        let mut config = PushConsumerConfig::new("reset-offset-listener-race").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let scheduler = Arc::new(PullScheduler::new());
        let consume_service = idle_consume_service();
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn.clone(),
            consume_service.clone(),
            scheduler,
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 50, false);
        let request = consume_request(queue.clone(), 73, vec![message(50)]);
        let process_queue = request.process_queue.clone();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let listener_started = Arc::new(tokio::sync::Barrier::new(2));
        let listener_release = Arc::new(tokio::sync::Barrier::new(2));
        let started = listener_started.clone();
        let release = listener_release.clone();
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let started = started.clone();
                let release = release.clone();
                async move {
                    started.wait().await;
                    release.wait().await;
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        );
        let running_consumer = consumer.clone();
        let listener_task = tokio::spawn(async move {
            running_consumer
                .run_consume_request(request, listener)
                .await;
        });
        listener_started.wait().await;

        consumer
            .reset_consumer_offsets("orders", vec![(queue.clone(), Some(7))])
            .await
            .unwrap();
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&7));
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&7));

        listener_release.wait().await;
        listener_task.await.unwrap();
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&7));
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reset_offset_fences_a_late_pull_response_before_persisting_the_new_value() {
        let mut config = PushConsumerConfig::new("reset-offset-pull-race").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let scheduler = Arc::new(PullScheduler::new());
        let consume_service = idle_consume_service();
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn.clone(),
            consume_service.clone(),
            scheduler.clone(),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 50, false);
        store.persist(&queue).await.unwrap();
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 73));
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let stale_request = Arc::new(PullRequest::new(
            "reset-offset-pull-race",
            process_queue.clone(),
            50,
        ));
        let persist_started = Arc::new(tokio::sync::Barrier::new(2));
        let persist_release = Arc::new(tokio::sync::Barrier::new(2));
        store.set_persist_barriers(persist_started.clone(), persist_release.clone());

        let reset_consumer = consumer.clone();
        let reset_queue = queue.clone();
        let reset_task = tokio::spawn(async move {
            reset_consumer
                .reset_consumer_offsets("orders", vec![(reset_queue, Some(7))])
                .await
        });
        persist_started.wait().await;
        assert!(process_queue.snapshot(73).unwrap().dropped);
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&7));
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));

        let mut late_result = pull_result(PullStatus::Found, 51);
        late_result.messages = Some(vec![message(50)]);
        consumer
            .apply_pull_result(
                stale_request,
                PullContext {
                    runtime: runtime.clone(),
                    offset_store: store_dyn,
                    scheduler,
                    consume_service: ConsumeService::Concurrent(consume_service),
                },
                late_result,
            )
            .await;
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&7));
        assert_eq!(
            store.updates.lock().unwrap().as_slice(),
            &[(queue.clone(), 50, false), (queue.clone(), 7, false)]
        );

        persist_release.wait().await;
        reset_task.await.unwrap().unwrap();
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&7));
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&7));
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn direct_consume_invokes_listener_without_advancing_regular_offsets() {
        let mut config = PushConsumerConfig::new("direct-consume-offset-isolation").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 50, false);
        store.persist(&queue).await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let listener: Arc<dyn MessageListenerConcurrently> = {
            let calls = calls.clone();
            Arc::new(
                move |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let calls = calls.clone();
                    async move {
                        assert_eq!(messages.len(), 1);
                        assert_eq!(messages[0].message().topic(), "orders");
                        assert_eq!(context.message_queue().topic(), Some("orders"));
                        assert_eq!(context.message_queue().broker_name(), Some("broker-a"));
                        calls.fetch_add(1, Ordering::AcqRel);
                        (Some(ConsumeConcurrentlyStatus::ReconsumeLater), context)
                    }
                },
            )
        };
        let consume_service = Arc::new(ConcurrentConsumeService::new(1, 1, listener));
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn,
            consume_service.clone(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let result = consumer
            .consume_message_directly(message(812), Some("broker-a".into()))
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(
            result.get_consume_result(),
            Some(ConsumeMessageResult::CrLater)
        );
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&50));
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));

        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn running_info_returns_queue_snapshot_and_omits_proxy_credentials() {
        let mut config = PushConsumerConfig::new("running-info-snapshot").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config
            .client_config_mut()
            .set_socks_proxy_config(Some(
                r#"{"10.1.0.0/16":{"addr":"proxy.internal:1080","username":"reader","password":"secret"}}"#.into(),
            ));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        consumer.subscribe("orders", "blue").await.unwrap();
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 17, false);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 19));
        process_queue
            .insert_messages(19, vec![message(11)])
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue);

        let pop_queue = MessageQueue::new("orders", "broker-a", 2);
        let pop_process_queue = Arc::new(PopProcessQueue::new(pop_queue.clone(), 23));
        pop_process_queue.inc_found_messages(3);
        pop_process_queue.set_last_pop_timestamp(1_700_000_000_005);
        write_lock(&consumer.pop_process_queues).insert(pop_queue.clone(), pop_process_queue);

        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn.clone(),
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        consumer
            .consumer_start_timestamp
            .store(1_700_000_000_000, Ordering::Release);
        global_consumer_stats().record_pull(
            runtime.client_id(),
            "running-info-snapshot",
            "orders",
            Duration::from_millis(500),
            2,
        );
        global_consumer_stats().record_consume_rt(
            runtime.client_id(),
            "running-info-snapshot",
            "orders",
            Duration::from_millis(8),
        );
        global_consumer_stats().record_concurrent_result(
            runtime.client_id(),
            "running-info-snapshot",
            "orders",
            Some(3),
            1,
        );

        let processor = crate::admin::ConsumerRunningInfoProcessor::for_push(
            Arc::downgrade(&consumer),
            Arc::downgrade(&store_dyn),
            Arc::downgrade(&runtime),
        );
        let response = processor
            .process(RemotingCommand {
                ext_fields: Some(BTreeMap::from([
                    ("consumerGroup".into(), "running-info-snapshot".into()),
                    (
                        "clientId".into(),
                        "client-id-is-not-used-for-filtering".into(),
                    ),
                    ("jstackEnable".into(), "false".into()),
                ])),
                ..Default::default()
            })
            .unwrap()
            .unwrap();
        assert_eq!(response.code, response::SUCCESS);
        let bytes = response.body.unwrap();
        let json = ConsumerRunningInfoBody::decode_java_json(std::str::from_utf8(&bytes).unwrap())
            .unwrap();
        assert!(json.get("jstack").is_none());
        let properties = json["properties"].as_object().unwrap();
        assert_eq!(properties["PROP_CONSUME_TYPE"], "CONSUME_PASSIVELY");
        assert_eq!(properties["PROP_CONSUMER_START_TIMESTAMP"], "1700000000000");
        assert_eq!(properties["pullThresholdForQueue"], "1000");
        assert!(properties.get("socksProxyConfig").is_none());
        assert!(properties.get("tlsConfig").is_none());
        assert_eq!(json["subscriptionSet"].as_array().unwrap().len(), 1);
        assert_eq!(json["statusTable"]["orders"]["pullRT"], 500.0);
        assert!(json["statusTable"]["orders"]["pullTPS"].as_f64().unwrap() > 0.0);
        assert_eq!(json["statusTable"]["orders"]["consumeRT"], 8.0);
        assert!(
            json["statusTable"]["orders"]["consumeOKTPS"]
                .as_f64()
                .unwrap()
                > 0.0
        );
        assert!(
            json["statusTable"]["orders"]["consumeFailedTPS"]
                .as_f64()
                .unwrap()
                > 0.0
        );
        assert_eq!(json["statusTable"]["orders"]["consumeFailedMsgs"], 1);
        let encoded_queue = r#"{"brokerName":"broker-a","queueId":0,"topic":"orders"}"#;
        let encoded_pop_queue = r#"{"brokerName":"broker-a","queueId":2,"topic":"orders"}"#;
        assert_eq!(
            json["mqTable"][encoded_queue]["commitOffset"],
            serde_json::Value::from(17)
        );
        assert_eq!(json["mqTable"][encoded_queue]["cachedMsgMinOffset"], 11);
        assert_eq!(json["mqTable"][encoded_queue]["cachedMsgMaxOffset"], 11);
        assert_eq!(json["mqTable"][encoded_queue]["cachedMsgCount"], 1);
        assert_eq!(json["mqPopTable"][encoded_pop_queue]["waitAckCount"], 3);
        assert_eq!(
            json["mqPopTable"][encoded_pop_queue]["lastPopTimestamp"],
            1_700_000_000_005i64
        );
        assert!(!std::str::from_utf8(&bytes).unwrap().contains("secret"));

        let response = processor
            .process(RemotingCommand {
                ext_fields: Some(BTreeMap::from([
                    ("consumerGroup".into(), "running-info-snapshot".into()),
                    ("clientId".into(), "ignored-client-id".into()),
                    ("jstackEnable".into(), "true".into()),
                ])),
                ..Default::default()
            })
            .unwrap()
            .unwrap();
        let body = ConsumerRunningInfoBody::decode_java_json(
            std::str::from_utf8(response.body.as_deref().unwrap()).unwrap(),
        )
        .unwrap();
        assert!(
            body["jstack"]
                .as_str()
                .is_some_and(|stack| !stack.is_empty())
        );
        assert!(
            body["jstack"]
                .as_str()
                .is_some_and(|stack| stack.contains("running_info_returns_queue_snapshot"))
        );

        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn consumer_status_stays_responsive_during_a_slow_direct_consume_callback() {
        let mut config = PushConsumerConfig::new("status-during-direct-consume").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        store.update_offset(&queue, 42, false);
        let listener_started = Arc::new(tokio::sync::Barrier::new(2));
        let listener_release = Arc::new(tokio::sync::Barrier::new(2));
        let started = listener_started.clone();
        let release = listener_release.clone();
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                let started = started.clone();
                let release = release.clone();
                async move {
                    started.wait().await;
                    release.wait().await;
                    (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                }
            },
        );
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn.clone(),
            Arc::new(ConcurrentConsumeService::new(1, 1, listener)),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let direct_consume = {
            let consumer = consumer.clone();
            tokio::spawn(async move {
                consumer
                    .consume_message_directly(message(99), Some("broker-a".into()))
                    .await
                    .unwrap()
            })
        };
        listener_started.wait().await;
        let status_processor =
            crate::admin::ConsumerStatusProcessor::new(Arc::downgrade(&store_dyn));
        let response = tokio::time::timeout(Duration::from_secs(1), async {
            status_processor.process(RemotingCommand {
                ext_fields: Some(BTreeMap::from([
                    ("topic".into(), "orders".into()),
                    ("group".into(), "status-during-direct-consume".into()),
                ])),
                ..Default::default()
            })
        })
        .await
        .expect("status snapshot must not wait for a consumer callback")
        .unwrap()
        .unwrap();
        assert_eq!(response.code, response::SUCCESS);
        let body = GetConsumerStatusBody::decode_java_json(
            std::str::from_utf8(response.body.as_deref().unwrap()).unwrap(),
        )
        .unwrap();
        body.message_queue_table
            .unwrap()
            .with(|entries| assert_eq!(entries, &vec![(queue.clone(), Some(42))]));

        listener_release.wait().await;
        assert_eq!(
            direct_consume.await.unwrap().get_consume_result(),
            Some(ConsumeMessageResult::CrSuccess)
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn queue_removal_fences_a_pull_response_already_in_flight() {
        let mut config = PushConsumerConfig::new("rebalance-pull-race").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let store_dyn: Arc<dyn OffsetStore> = store.clone();
        let scheduler = Arc::new(PullScheduler::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store_dyn.clone(),
            idle_consume_service(),
            scheduler.clone(),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 73));
        store.update_offset(&queue, 50, false);
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let request = Arc::new(PullRequest::new(
            "rebalance-pull-race",
            process_queue.clone(),
            50,
        ));
        scheduler.schedule(request.clone(), 0).unwrap();
        let active_request = scheduler.take_due().unwrap();
        let response_ready = Arc::new(tokio::sync::Barrier::new(2));
        let response_release = Arc::new(tokio::sync::Barrier::new(2));
        let task_ready = response_ready.clone();
        let task_release = response_release.clone();
        let pull_consumer = consumer.clone();
        let pull_store = store_dyn.clone();
        let pull_scheduler = scheduler.clone();
        let pull_runtime = runtime.clone();
        let updates_before = store.updates.lock().unwrap().len();
        let pull_task = tokio::spawn(async move {
            task_ready.wait().await;
            task_release.wait().await;
            let context = PullContext {
                runtime: pull_runtime,
                offset_store: pull_store,
                scheduler: pull_scheduler,
                consume_service: ConsumeService::Concurrent(idle_consume_service()),
            };
            let mut result = pull_result(PullStatus::Found, 51);
            result.messages = Some(vec![message(50)]);
            pull_consumer
                .apply_pull_result(request, context, result)
                .await;
        });
        response_ready.wait().await;

        process_queue.drop_assignment(73).unwrap();
        scheduler.cancel(&queue, 73);
        let removal_context = PullContext {
            runtime,
            offset_store: store_dyn,
            scheduler: scheduler.clone(),
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };
        assert!(
            consumer
                .remove_process_queue_safely(&removal_context, &queue, &process_queue)
                .await
        );
        response_release.wait().await;
        pull_task.await.unwrap();
        scheduler.finish(&active_request);

        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        assert_eq!(process_queue.snapshot(73).unwrap().cached_count, 0);
        assert_eq!(store.updates.lock().unwrap().len(), updates_before);
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&50));
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_offset_persist_retains_the_dropped_generation_for_retry() {
        let mut config = PushConsumerConfig::new("rebalance-remove-retry").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            scheduler.clone(),
            pull_rpc_workers,
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 2);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 44));
        process_queue.drop_assignment(44).unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        store.update_offset(&queue, 81, false);
        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: store.clone(),
            scheduler,
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };

        store.set_persist_failure(true);
        assert!(
            !consumer
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
        );
        assert!(
            read_lock(&consumer.process_queues)
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &process_queue))
        );
        assert!(process_queue.snapshot(44).unwrap().dropped);
        assert_eq!(store.values.lock().unwrap().get(&queue), Some(&81));

        store.set_persist_failure(false);
        assert!(
            consumer
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
        );
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));
        assert_eq!(store.persisted.lock().unwrap().get(&queue), Some(&81));
        assert!(!store.values.lock().unwrap().contains_key(&queue));
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn activation_fences_old_generations_and_rolls_back_scheduler_failure() {
        let mut config = PushConsumerConfig::new("rebalance-activation").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let store = Arc::new(RecordingOffsetStore::default());
        let scheduler = Arc::new(PullScheduler::new());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            store.clone(),
            idle_consume_service(),
            scheduler.clone(),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let original = consumer.activate_process_queue(&queue, 10).await.unwrap();
        assert!(Arc::ptr_eq(
            &original,
            &consumer.activate_process_queue(&queue, 999).await.unwrap()
        ));
        original.drop_assignment(original.generation()).unwrap();
        assert!(matches!(
            consumer.activate_process_queue(&queue, 20).await,
            Err(PushConsumerError::ProcessQueueRemovalPending(_))
        ));
        assert!(
            read_lock(&consumer.process_queues)
                .get(&queue)
                .is_some_and(|current| Arc::ptr_eq(current, &original))
        );

        let context = PullContext {
            runtime: runtime.clone(),
            offset_store: store.clone(),
            scheduler: scheduler.clone(),
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
        };
        assert!(
            consumer
                .remove_process_queue_safely(&context, &queue, &original)
                .await
        );
        let replacement = consumer.activate_process_queue(&queue, 20).await.unwrap();
        assert!(replacement.generation() > original.generation());

        let failed_queue = MessageQueue::new("orders", "broker-a", 1);
        scheduler.stop().await;
        assert!(matches!(
            consumer.activate_process_queue(&failed_queue, 30).await,
            Err(PushConsumerError::PullScheduler(_))
        ));
        assert!(!read_lock(&consumer.process_queues).contains_key(&failed_queue));
        assert!(!store.values.lock().unwrap().contains_key(&failed_queue));
        consumer.shutdown().await.unwrap();
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
            consume_service: ConsumeService::Concurrent(consume_service.clone()),
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
            consume_service: ConsumeService::Concurrent(consume_service.clone()),
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
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
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
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
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
    async fn public_send_back_overloads_use_message_and_explicit_broker_names() {
        let config = PushConsumerConfig::new("public-send-back").unwrap();
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;
        {
            let mut scripted = consumer.scripted_retry_transport.lock().unwrap();
            scripted.broker_results.extend([Ok(()), Ok(()), Ok(())]);
        }

        let message = retry_test_message(7);
        consumer.send_message_back(&message, 2).await.unwrap();
        consumer
            .send_message_back_to_broker(&message, 3, Some("broker-override"))
            .await
            .unwrap();
        let mut store_host_message = retry_test_message(8);
        store_host_message.set_broker_name(None);
        store_host_message.set_store_host(Some("127.0.0.1:10911".to_owned()));
        consumer
            .send_message_back_to_broker(&store_host_message, 4, None)
            .await
            .unwrap();

        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert_eq!(scripted.broker_headers.len(), 3);
            assert_eq!(
                scripted.broker_headers[0].broker_name.as_deref(),
                Some("broker-a")
            );
            assert_eq!(scripted.broker_headers[0].delay_level, 2);
            assert_eq!(
                scripted.broker_headers[1].broker_name.as_deref(),
                Some("broker-override")
            );
            assert_eq!(scripted.broker_headers[1].delay_level, 3);
            assert_eq!(scripted.broker_headers[2].broker_name, None);
            assert_eq!(scripted.broker_headers[2].delay_level, 4);
            assert_eq!(
                scripted.broker_targets[2],
                (None, Some("127.0.0.1:10911".to_owned()))
            );
            assert!(scripted.retry_messages.is_empty());
        }
        consumer.shutdown().await.unwrap();
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
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
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
    async fn consume_message_hooks_wrap_the_listener_and_share_context_state() {
        struct Hook(Arc<std::sync::Mutex<Vec<String>>>);

        impl ConsumeMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "consume-hook-test"
            }

            fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
                assert!(!context.success);
                assert!(context.status.is_none());
                assert!(context.access_channel.is_none());
                assert_eq!(context.consumer_group, "tenant%consume-hook-lifecycle");
                assert_eq!(context.namespace.as_deref(), Some("tenant"));
                assert_eq!(context.properties, Some(HashMap::new()));
                context
                    .properties
                    .as_mut()
                    .unwrap()
                    .insert("hook-state".into(), "retained".into());
                self.0.lock().unwrap().push("before".into());
            }

            fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
                assert!(context.success);
                assert_eq!(context.status.as_deref(), Some("CONSUME_SUCCESS"));
                assert_eq!(
                    context.access_channel,
                    Some(rocketmq_classic_core::config::AccessChannel::Local)
                );
                assert_eq!(
                    context
                        .properties
                        .as_ref()
                        .and_then(|properties| properties.get("hook-state"))
                        .map(String::as_str),
                    Some("retained")
                );
                self.0.lock().unwrap().push("after".into());
            }
        }

        let mut config = PushConsumerConfig::new("consume-hook-lifecycle").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        consumer.register_consume_message_hook(Arc::new(Hook(events.clone())));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let request = consume_request(queue, 60, vec![retry_test_message(70)]);
        let listener: Arc<dyn MessageListenerConcurrently> = Arc::new(
            |messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| async move {
                assert_eq!(messages.len(), 1);
                (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
            },
        );
        consumer
            .clone()
            .run_consume_request(request, listener)
            .await;

        assert_eq!(events.lock().unwrap().as_slice(), ["before", "after"]);
        let _ = runtime.shutdown().await;
        consumer.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_consume_timeout_classifies_after_listener_returns_without_cancelling_it() {
        struct Hook(Arc<std::sync::Mutex<Vec<String>>>);

        impl ConsumeMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "consume-timeout-hook"
            }

            fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push("before".into());
            }

            fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
                let return_type = context
                    .properties
                    .as_ref()
                    .and_then(|properties| properties.get("CONSUME_CONTEXT_TYPE"))
                    .map(String::as_str)
                    .unwrap_or("missing");
                self.0.lock().unwrap().push(format!(
                    "after:{}:{}:{return_type}",
                    context.status.as_deref().unwrap_or("missing"),
                    context.success
                ));
            }
        }

        struct CallbackDrop(Arc<AtomicBool>);

        impl Drop for CallbackDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let listener: Arc<dyn MessageListenerConcurrently> = {
            let started = started.clone();
            let release = release.clone();
            let dropped = dropped.clone();
            Arc::new(
                move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let started = started.clone();
                    let release = release.clone();
                    let dropped = dropped.clone();
                    async move {
                        let _drop_signal = CallbackDrop(dropped);
                        started.notify_one();
                        release.notified().await;
                        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                    }
                },
            )
        };

        let mut config = PushConsumerConfig::new("consume-timeout-hook").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_consume_timeout_minutes(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        consumer.register_consume_message_hook(Arc::new(Hook(events.clone())));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            idle_consume_service(),
            Arc::new(PullScheduler::new()),
            Arc::new(PullRpcWorkers::new()),
        )
        .await;

        let request = consume_request(
            MessageQueue::new("orders", "broker-a", 0),
            61,
            vec![retry_test_message(71)],
        );
        let task = tokio::spawn({
            let consumer = consumer.clone();
            async move { consumer.run_consume_request(request, listener).await }
        });
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();

        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        assert!(
            !dropped.load(Ordering::Acquire),
            "consume timeout must classify a completed callback, not cancel it"
        );
        assert_eq!(events.lock().unwrap().as_slice(), ["before"]);

        release.notify_one();
        task.await.unwrap();
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["before", "after:CONSUME_SUCCESS:true:TIME_OUT"]
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
            consume_service: ConsumeService::Concurrent(idle_consume_service()),
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
    async fn startup_rejects_missing_listener_and_starts_orderly_listener() {
        let consumer = Arc::new(PushConsumer::with_manager(
            PushConsumerConfig::new("missing-listener").unwrap(),
            ClientRuntimeManager::new(),
        ));
        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::MissingMessageListener)
        ));
        assert!(!consumer.is_running().await);

        let mut config = PushConsumerConfig::new("orderly-listener").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let consumer = Arc::new(PushConsumer::with_manager(
            config,
            ClientRuntimeManager::new(),
        ));
        consumer
            .register_message_listener(PushMessageListener::Orderly(Arc::new(EmptyOrderlyListener)))
            .await
            .unwrap();
        consumer.start().await.unwrap();
        assert!(consumer.is_running().await);
        assert!(consumer.is_consume_orderly());
        consumer.shutdown().await.unwrap();
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
        assert!(!consumer.unsubscribe("temporary").await.unwrap());
        assert!(consumer.unsubscribe("tenant%temporary").await.unwrap());
        assert!(!consumer.unsubscribe("tenant%temporary").await.unwrap());
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
        assert!(consumer.update_core_pool_size(4).await);
        assert_eq!(consume_service.worker_count(), 4);
        assert!(!consumer.update_core_pool_size(20).await);
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
        consumer.subscribe("late", "*").await.unwrap();
        assert!(
            runtime
                .select_consumer("tenant%push-lifecycle")
                .unwrap()
                .unwrap()
                .heartbeat_data()
                .subscription_data_set
                .unwrap()
                .snapshot()
                .iter()
                .any(|subscription| {
                    subscription.as_ref().is_some_and(|subscription| {
                        subscription.snapshot().topic.as_deref() == Some("tenant%late")
                    })
                })
        );
        assert!(consumer.get_offset_store().is_some());
        assert!(matches!(
            consumer.start().await,
            Err(PushConsumerError::AlreadyStarted)
        ));
        assert!(matches!(
            consumer.set_offset_store(None).await,
            Err(PushConsumerError::OffsetStoreAfterStartup)
        ));
        assert!(consumer.unsubscribe("tenant%orders").await.unwrap());

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
                    Some(super::super::listener::ConsumeOrderlyStatus::Success),
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
        clustering.copy_subscriptions("orders").unwrap();
        assert!(clustering.subscriptions().contains_key("%RETRY%orders"));

        let mut config = PushConsumerConfig::new("broadcast-orders").unwrap();
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        let broadcasting = PushConsumer::with_manager(config, ClientRuntimeManager::new());
        broadcasting.copy_subscriptions("broadcast-orders").unwrap();
        assert!(
            !broadcasting
                .subscriptions()
                .keys()
                .any(|topic| topic.starts_with("%RETRY%"))
        );
    }

    #[tokio::test]
    async fn legacy_subscription_map_is_namespaced_and_copied_into_heartbeat_state() {
        let mut config = PushConsumerConfig::new("orders").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let consumer = PushConsumer::with_manager(config, ClientRuntimeManager::new());
        consumer
            .set_subscription(HashMap::from([(
                "orders".to_owned(),
                "gold || silver".to_owned(),
            )]))
            .await
            .unwrap();

        assert_eq!(
            consumer.get_subscription().get("tenant%orders"),
            Some(&"gold || silver".to_owned())
        );
        consumer.copy_subscriptions("tenant%orders").unwrap();
        assert_eq!(
            consumer
                .subscriptions()
                .get("tenant%orders")
                .and_then(SubscriptionData::get_sub_string),
            Some("gold || silver")
        );
    }

    #[test]
    fn retry_and_dlq_messages_restore_logical_topics_for_orderly_and_pop() {
        let mut config = PushConsumerConfig::new("namespace-consumer").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let consumer = PushConsumer::with_manager(config, ClientRuntimeManager::new());
        let mut client_config = consumer.config.client_config().clone();
        let namespaced_group = client_config
            .with_namespace(Some(consumer.config.consumer_group()))
            .unwrap()
            .unwrap();

        let make_messages = || {
            let mut retry = retry_test_message(1);
            retry
                .message_mut()
                .set_topic(format!("%RETRY%{namespaced_group}"));
            retry
                .message_mut()
                .set_system_property(PROPERTY_RETRY_TOPIC, "tenant%orders")
                .unwrap();

            let mut dlq = retry_test_message(2);
            dlq.message_mut()
                .set_topic(format!("%DLQ%tenant%{}", consumer.config.consumer_group()));

            let mut normal = retry_test_message(3);
            normal.message_mut().set_topic("tenant%orders");
            vec![retry, dlq, normal]
        };

        let mut orderly_messages = make_messages();
        consumer.reset_retry_and_namespace_for_orderly(&mut orderly_messages);
        assert_eq!(
            orderly_messages
                .iter()
                .map(|message| message.message().topic())
                .collect::<Vec<_>>(),
            ["orders", "%DLQ%namespace-consumer", "orders"]
        );

        let mut pop_messages = make_messages();
        consumer.reset_retry_and_namespace_for_pop(&mut pop_messages, &namespaced_group);
        assert_eq!(
            pop_messages
                .iter()
                .map(|message| message.message().topic())
                .collect::<Vec<_>>(),
            ["orders", "%DLQ%namespace-consumer", "orders"]
        );
    }

    #[tokio::test]
    async fn shutdown_aborts_a_slow_listener_without_committing_its_batch() {
        struct NotifyOnDrop(Arc<tokio::sync::Notify>);
        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }

        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(tokio::sync::Notify::new());
        let listener: Arc<dyn MessageListenerConcurrently> = {
            let started = started.clone();
            let release = release.clone();
            let cancelled = cancelled.clone();
            Arc::new(
                move |_messages: Vec<MessageExt>, context: ConsumeConcurrentlyContext| {
                    let started = started.clone();
                    let release = release.clone();
                    let cancelled = cancelled.clone();
                    async move {
                        let _drop_signal = NotifyOnDrop(cancelled);
                        started.notify_one();
                        release.notified().await;
                        (Some(ConsumeConcurrentlyStatus::ConsumeSuccess), context)
                    }
                },
            )
        };
        let mut config = PushConsumerConfig::new("slow-consumer-shutdown").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        config.set_await_termination_millis_when_shutdown(10);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let consume_service = Arc::new(ConcurrentConsumeService::new(1, 1, listener));
        let scheduler = Arc::new(PullScheduler::new());
        let pull_rpc_workers = Arc::new(PullRpcWorkers::new());
        let _runtime = install_started_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            consume_service.clone(),
            scheduler.clone(),
            pull_rpc_workers.clone(),
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 40));
        process_queue
            .insert_messages(40, vec![retry_test_message(50)])
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        offset_store.update_offset(&queue, 50, false);
        let context = consumer.pull_context().await.unwrap();
        consumer.submit_consume_batch(&context, process_queue.clone(), 40);
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();

        consumer.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), cancelled.notified())
            .await
            .unwrap();
        let snapshot = process_queue.snapshot(40).unwrap();
        assert!(snapshot.dropped);
        assert_eq!(snapshot.cached_count, 1);
        assert_eq!(snapshot.in_flight_count, 1);
        assert_eq!(offset_store.values.lock().unwrap().get(&queue), Some(&50));
        assert_eq!(consume_service.worker_count(), 0);
        assert_eq!(scheduler.worker_count(), 0);
        assert_eq!(pull_rpc_workers.worker_count(), 0);
    }

    #[tokio::test]
    async fn orderly_batch_lock_applies_partial_results_grouped_by_broker() {
        let mut config = PushConsumerConfig::new("orderly-partial-lock").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let queues = [
            MessageQueue::new("orders", "broker-a", 0),
            MessageQueue::new("orders", "broker-a", 1),
            MessageQueue::new("orders", "broker-b", 0),
        ];
        let process_queues = queues
            .iter()
            .enumerate()
            .map(|(index, queue)| {
                let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 80 + index as u64));
                if index == 1 {
                    process_queue
                        .set_lock_state(81, true, epoch_millis())
                        .unwrap();
                }
                process_queue
            })
            .collect::<Vec<_>>();
        {
            let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
            scripted.lock_results.extend([
                Ok(Some(HashSet::from([Some(queues[0].clone())]))),
                Ok(Some(HashSet::new())),
            ]);
        }

        let locked = consumer
            .lock_orderly_process_queues(&runtime, &process_queues)
            .await;
        assert_eq!(locked, HashSet::from([queues[0].clone()]));
        assert!(process_queues[0].is_lock_fresh(80, 30_000).unwrap());
        assert!(!process_queues[1].snapshot(81).unwrap().locked);
        assert!(!process_queues[2].snapshot(82).unwrap().locked);

        {
            let scripted = consumer.scripted_lock_transport.lock().unwrap();
            assert_eq!(scripted.lock_bodies.len(), 2);
            let mut grouped_queue_counts = scripted
                .lock_bodies
                .iter()
                .map(|body| body.mq_set.as_ref().unwrap().with(|set| set.len()))
                .collect::<Vec<_>>();
            grouped_queue_counts.sort_unstable();
            assert_eq!(grouped_queue_counts, vec![1, 2]);
        }
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_lock_errors_and_nullable_sets_preserve_previous_state() {
        let mut config = PushConsumerConfig::new("orderly-lock-uncertain").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue, 83));
        process_queue
            .set_lock_state(83, true, epoch_millis())
            .unwrap();
        let queues = std::slice::from_ref(&process_queue);

        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .lock_results
            .push_back(Err("scripted timeout".into()));
        consumer.lock_orderly_process_queues(&runtime, queues).await;
        assert!(process_queue.snapshot(83).unwrap().locked);
        assert!(process_queue.is_lock_fresh(83, 30_000).unwrap());

        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .lock_results
            .push_back(Ok(None));
        consumer.lock_orderly_process_queues(&runtime, queues).await;
        assert!(process_queue.snapshot(83).unwrap().locked);
        assert!(process_queue.is_lock_fresh(83, 30_000).unwrap());

        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .lock_results
            .push_back(Ok(Some(HashSet::new())));
        consumer.lock_orderly_process_queues(&runtime, queues).await;
        assert!(!process_queue.snapshot(83).unwrap().locked);
        assert!(!process_queue.is_lock_fresh(83, 30_000).unwrap());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn older_orderly_renewal_response_cannot_overwrite_the_newer_attempt() {
        let mut config = PushConsumerConfig::new("orderly-lock-race").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 84));
        let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
        let (second_sender, second_receiver) = tokio::sync::oneshot::channel();
        let requested = {
            let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
            scripted
                .lock_waiters
                .extend([first_receiver, second_receiver]);
            scripted.lock_requested.clone()
        };

        let first_notice = requested.notified();
        let first_consumer = consumer.clone();
        let first_runtime = runtime.clone();
        let first_queue = process_queue.clone();
        let first = tokio::spawn(async move {
            first_consumer
                .lock_orderly_process_queues(&first_runtime, &[first_queue])
                .await
        });
        first_notice.await;

        let second_notice = requested.notified();
        let second_consumer = consumer.clone();
        let second_runtime = runtime.clone();
        let second_queue = process_queue.clone();
        let second = tokio::spawn(async move {
            second_consumer
                .lock_orderly_process_queues(&second_runtime, &[second_queue])
                .await
        });
        second_notice.await;

        first_sender
            .send(Ok(Some(HashSet::from([Some(queue.clone())]))))
            .unwrap();
        assert!(first.await.unwrap().is_empty());
        assert!(!process_queue.snapshot(84).unwrap().locked);

        second_sender
            .send(Ok(Some(HashSet::from([Some(queue.clone())]))))
            .unwrap();
        assert_eq!(second.await.unwrap(), HashSet::from([queue]));
        assert!(process_queue.snapshot(84).unwrap().locked);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_batch_lock_age_starts_at_each_broker_response() {
        let mut config = PushConsumerConfig::new("orderly-lock-response-time").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let queue_a = MessageQueue::new("orders", "broker-a", 0);
        let queue_b = MessageQueue::new("orders", "broker-b", 0);
        let process_queue_a = Arc::new(ProcessQueue::new(queue_a.clone(), 94));
        let process_queue_b = Arc::new(ProcessQueue::new(queue_b.clone(), 95));
        let (response_a, waiter_a) = tokio::sync::oneshot::channel();
        let (response_b, waiter_b) = tokio::sync::oneshot::channel();
        let requested = {
            let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
            scripted.lock_waiters.extend([waiter_a, waiter_b]);
            scripted.lock_requested.clone()
        };

        let first_request = requested.notified();
        let task_consumer = consumer.clone();
        let task_runtime = runtime.clone();
        let task_queues = [process_queue_a.clone(), process_queue_b.clone()];
        let lock_task = tokio::spawn(async move {
            task_consumer
                .lock_orderly_process_queues(&task_runtime, &task_queues)
                .await
        });
        first_request.await;
        response_a
            .send(Ok(Some(HashSet::from([Some(queue_a.clone())]))))
            .unwrap();

        let second_request = requested.notified();
        second_request.await;
        tokio::time::advance(Duration::from_millis(30_001)).await;
        response_b
            .send(Ok(Some(HashSet::from([Some(queue_b.clone())]))))
            .unwrap();

        assert_eq!(
            lock_task.await.unwrap(),
            HashSet::from([queue_a.clone(), queue_b.clone()])
        );
        assert!(!process_queue_a.is_lock_fresh(94, 30_000).unwrap());
        assert!(process_queue_b.is_lock_fresh(95, 30_000).unwrap());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn late_orderly_lock_for_dropped_generation_is_unlocked() {
        let mut config = PushConsumerConfig::new("orderly-dropped-lock-race").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 85));
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
        let requested = {
            let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
            scripted.lock_waiters.push_back(response_receiver);
            scripted.unlock_results.push_back(Ok(()));
            scripted.lock_requested.clone()
        };

        let notice = requested.notified();
        let task_consumer = consumer.clone();
        let task_runtime = runtime.clone();
        let task_queue = process_queue.clone();
        let task = tokio::spawn(async move {
            task_consumer
                .lock_orderly_process_queues(&task_runtime, &[task_queue])
                .await
        });
        notice.await;
        process_queue.drop_assignment(85).unwrap();
        response_sender
            .send(Ok(Some(HashSet::from([Some(queue.clone())]))))
            .unwrap();

        assert!(task.await.unwrap().is_empty());
        assert!(process_queue.snapshot(85).unwrap().dropped);
        {
            let scripted = consumer.scripted_lock_transport.lock().unwrap();
            assert_eq!(scripted.unlock_bodies.len(), 1);
        }
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_success_holds_until_commit_and_rollback_replays_in_order() {
        let mut config = PushConsumerConfig::new("orderly-status-flow").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_consume_message_batch_max_size(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let hook_events = Arc::new(Mutex::new(Vec::new()));
        struct Hook(Arc<Mutex<Vec<String>>>);
        impl ConsumeMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "orderly-consume-hook"
            }

            fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push("before".into());
            }

            fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
                self.0.lock().unwrap().push(format!(
                    "{}:{}",
                    context.status.as_deref().unwrap_or("missing"),
                    context.success
                ));
            }
        }
        consumer.register_consume_message_hook(Arc::new(Hook(hook_events.clone())));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let outcomes = Arc::new(Mutex::new(VecDeque::from([
            (Some(ConsumeOrderlyStatus::Success), false, -1),
            (Some(ConsumeOrderlyStatus::Rollback), false, 30_000),
            (Some(ConsumeOrderlyStatus::Success), true, -1),
            (Some(ConsumeOrderlyStatus::Success), true, -1),
        ])));
        let seen = Arc::new(Mutex::new(Vec::<Vec<i64>>::new()));
        let listener: Arc<dyn MessageListenerOrderly> = {
            let outcomes = outcomes.clone();
            let seen = seen.clone();
            Arc::new(
                move |messages: Vec<MessageExt>, mut context: ConsumeOrderlyContext| {
                    let (status, auto_commit, suspend_millis) =
                        outcomes.lock().unwrap().pop_front().unwrap();
                    let offsets = messages
                        .iter()
                        .map(MessageExt::queue_offset)
                        .collect::<Vec<_>>();
                    let seen = seen.clone();
                    async move {
                        seen.lock().unwrap().push(offsets);
                        context.set_auto_commit(auto_commit);
                        context.set_suspend_current_queue_time_millis(suspend_millis);
                        (status, context)
                    }
                },
            )
        };
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            None,
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 86));
        process_queue
            .insert_messages(86, vec![retry_test_message(10), retry_test_message(11)])
            .unwrap();
        let request = OrderlyConsumeRequest {
            process_queue: process_queue.clone(),
            generation: 86,
        };
        consumer
            .clone()
            .run_orderly_consume_request(request, listener.clone())
            .await;
        assert_eq!(process_queue.snapshot(86).unwrap().cached_count, 2);
        assert!(offset_store.updates.lock().unwrap().is_empty());

        let request = OrderlyConsumeRequest {
            process_queue: process_queue.clone(),
            generation: 86,
        };
        consumer
            .clone()
            .run_orderly_consume_request(request, listener)
            .await;
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[vec![10], vec![11], vec![10], vec![11]]
        );
        assert_eq!(process_queue.snapshot(86).unwrap().cached_count, 0);
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue.clone(), 11, false), (queue, 12, false)]
        );
        assert_eq!(
            hook_events.lock().unwrap().as_slice(),
            [
                "before",
                "SUCCESS:true",
                "before",
                "ROLLBACK:false",
                "before",
                "SUCCESS:true",
                "before",
                "SUCCESS:true"
            ]
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_clustering_rebalance_locks_before_activating_pull_queue() {
        let mut config = PushConsumerConfig::new("orderly-lock-before-pull").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        config.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let queue = MessageQueue::new("orders", "broker-a", 0);
        write_lock(&consumer.subscriptions).insert(
            "orders".to_owned(),
            SubscriptionData::new(Some("orders".to_owned()), Some("*".to_owned())),
        );
        write_lock(&consumer.topic_queues)
            .insert("orders".to_owned(), BTreeSet::from([queue.clone()]));
        let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (Some(ConsumeOrderlyStatus::Success), context)
            },
        );
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime =
            install_started_orderly_consumer(&consumer, &manager, offset_store_dyn, listener, None)
                .await;
        let client_id = runtime.client_id().to_owned();
        script_rebalance_membership(
            &consumer,
            "orders",
            [Some(vec![client_id.clone()]), Some(vec![client_id])],
        );
        {
            let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
            scripted.lock_results.extend([
                Ok(Some(HashSet::new())),
                Ok(Some(HashSet::from([Some(queue.clone())]))),
            ]);
            scripted.unlock_results.push_back(Ok(()));
        }
        consumer.paused.store(true, Ordering::Release);

        assert!(
            !consumer
                .rebalance_once(&crate::rebalance::AllocateMessageQueueAveragely)
                .await
        );
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));

        let _ = consumer
            .rebalance_once(&crate::rebalance::AllocateMessageQueueAveragely)
            .await;
        let process_queue = read_lock(&consumer.process_queues)
            .get(&queue)
            .cloned()
            .expect("successful Broker lock permits assignment activation");
        assert!(
            process_queue
                .snapshot(process_queue.generation())
                .unwrap()
                .locked
        );
        assert_eq!(offset_store.values.lock().unwrap().get(&queue), Some(&0));
        {
            let scripted = consumer.scripted_lock_transport.lock().unwrap();
            assert_eq!(scripted.lock_bodies.len(), 2);
            assert!(scripted.lock_bodies.iter().all(|body| {
                body.mq_set.as_ref().is_some_and(|set| {
                    set.with(|queues| queues.contains(&Some(SharedValue::new(queue.clone()))))
                })
            }));
        }

        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_max_continuous_consume_property_yields_between_batches() {
        let mut config = PushConsumerConfig::new("orderly-continuous-limit").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_consume_message_batch_max_size(1);
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let callback_count = Arc::new(AtomicUsize::new(0));
        let listener: Arc<dyn MessageListenerOrderly> = {
            let callback_count = callback_count.clone();
            Arc::new(
                move |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                    let callback_count = callback_count.clone();
                    async move {
                        callback_count.fetch_add(1, Ordering::SeqCst);
                        tokio::time::advance(Duration::from_millis(1)).await;
                        (Some(ConsumeOrderlyStatus::Success), context)
                    }
                },
            )
        };
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let settings = OrderlyLockSettings::from_all_values(30_000, 20_000, 20_000, 0).unwrap();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            Some(settings),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 95));
        process_queue
            .insert_messages(95, vec![retry_test_message(400), retry_test_message(401)])
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        offset_store.update_offset(&queue, 400, false);

        consumer
            .clone()
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue: process_queue.clone(),
                    generation: 95,
                },
                listener,
            )
            .await;

        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
        assert_eq!(process_queue.snapshot(95).unwrap().cached_count, 1);
        assert_eq!(
            process_queue.snapshot(95).unwrap().next_commit_offset,
            Some(401)
        );
        assert_eq!(
            offset_store.updates.lock().unwrap().last(),
            Some(&(queue, 401, false))
        );
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_retry_limit_publishes_retry_topic_and_failed_publish_requeues() {
        let mut config = PushConsumerConfig::new("orderly-retry-limit").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_max_reconsume_times(2);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (
                    Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment),
                    context,
                )
            },
        );
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            None,
        )
        .await;
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .retry_results
            .push_back(Ok(()));

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 87));
        process_queue
            .insert_messages(87, vec![retry_test_message(20)])
            .unwrap();
        consumer
            .clone()
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue: process_queue.clone(),
                    generation: 87,
                },
                listener.clone(),
            )
            .await;
        assert_eq!(process_queue.snapshot(87).unwrap().cached_count, 0);
        assert_eq!(
            offset_store.updates.lock().unwrap().as_slice(),
            &[(queue, 21, false)]
        );
        {
            let scripted = consumer.scripted_retry_transport.lock().unwrap();
            assert!(scripted.broker_headers.is_empty());
            assert_eq!(scripted.retry_messages.len(), 1);
            let retry = &scripted.retry_messages[0];
            assert_eq!(retry.topic(), "%RETRY%orderly-retry-limit");
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_RETRY_TOPIC)
                    .map(String::as_str),
                Some("orders")
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
                Some("2")
            );
            assert_eq!(
                retry
                    .properties()
                    .get(PROPERTY_DELAY_TIME_LEVEL)
                    .map(String::as_str),
                Some("5")
            );
            assert!(
                !retry
                    .properties()
                    .contains_key(PROPERTY_TRANSACTION_PREPARED)
            );
        }
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();

        let mut config = PushConsumerConfig::new("orderly-retry-fallback-failure").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_max_reconsume_times(2);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            None,
        )
        .await;
        consumer
            .scripted_retry_transport
            .lock()
            .unwrap()
            .retry_results
            .push_back(Err("retry publish rejected".into()));
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 88));
        process_queue
            .insert_messages(88, vec![retry_test_message(30)])
            .unwrap();
        consumer
            .clone()
            .run_orderly_consume_request(
                OrderlyConsumeRequest {
                    process_queue: process_queue.clone(),
                    generation: 88,
                },
                listener,
            )
            .await;
        assert_eq!(process_queue.snapshot(88).unwrap().cached_count, 1);
        assert!(offset_store.updates.lock().unwrap().is_empty());
        let redelivery = process_queue.take_batch(88, 1).unwrap().unwrap();
        assert_eq!(redelivery.messages()[0].reconsume_times(), 3);
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_callbacks_on_same_queue_are_serialized_in_offset_order() {
        let mut config = PushConsumerConfig::new("orderly-serialized-queue").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_consume_thread_min(2);
        config.set_consume_thread_max(2);
        config.set_consume_message_batch_max_size(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let first_started = Arc::new(tokio::sync::Notify::new());
        let second_started = Arc::new(tokio::sync::Notify::new());
        let release_first = Arc::new(tokio::sync::Notify::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let listener: Arc<dyn MessageListenerOrderly> = {
            let first_started = first_started.clone();
            let second_started = second_started.clone();
            let release_first = release_first.clone();
            let seen = seen.clone();
            Arc::new(
                move |messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                    let offset = messages[0].queue_offset();
                    let first_started = first_started.clone();
                    let second_started = second_started.clone();
                    let release_first = release_first.clone();
                    let seen = seen.clone();
                    async move {
                        seen.lock().unwrap().push(offset);
                        if offset == 100 {
                            first_started.notify_one();
                            release_first.notified().await;
                        } else {
                            second_started.notify_one();
                        }
                        (Some(ConsumeOrderlyStatus::Success), context)
                    }
                },
            )
        };
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store;
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            None,
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue, 96));
        process_queue
            .insert_messages(96, vec![retry_test_message(100), retry_test_message(101)])
            .unwrap();

        let first_notice = first_started.notified();
        let first_consumer = consumer.clone();
        let first_queue = process_queue.clone();
        let first_listener = listener.clone();
        let first = tokio::spawn(async move {
            first_consumer
                .run_orderly_consume_request(
                    OrderlyConsumeRequest {
                        process_queue: first_queue,
                        generation: 96,
                    },
                    first_listener,
                )
                .await
        });
        first_notice.await;

        let second_notice = second_started.notified();
        let second_consumer = consumer.clone();
        let second_queue = process_queue.clone();
        let second_listener = listener.clone();
        let second = tokio::spawn(async move {
            second_consumer
                .run_orderly_consume_request(
                    OrderlyConsumeRequest {
                        process_queue: second_queue,
                        generation: 96,
                    },
                    second_listener,
                )
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), second_notice)
                .await
                .is_err()
        );

        release_first.notify_one();
        tokio::time::timeout(Duration::from_secs(1), second_started.notified())
            .await
            .expect("the next callback should run after the first releases the queue gate");
        first.await.unwrap();
        second.await.unwrap();
        assert_eq!(seen.lock().unwrap().as_slice(), &[100, 101]);
        assert_eq!(process_queue.snapshot(96).unwrap().cached_count, 0);
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_callbacks_on_different_queues_make_progress_independently() {
        let mut config = PushConsumerConfig::new("orderly-independent-queues").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Broadcasting);
        config.set_consume_thread_min(2);
        config.set_consume_thread_max(2);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let slow_started = Arc::new(tokio::sync::Notify::new());
        let release_slow = Arc::new(tokio::sync::Notify::new());
        let fast_finished = Arc::new(tokio::sync::Notify::new());
        let listener: Arc<dyn MessageListenerOrderly> = {
            let slow_started = slow_started.clone();
            let release_slow = release_slow.clone();
            let fast_finished = fast_finished.clone();
            Arc::new(
                move |messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                    let offset = messages[0].queue_offset();
                    let slow_started = slow_started.clone();
                    let release_slow = release_slow.clone();
                    let fast_finished = fast_finished.clone();
                    async move {
                        if offset == 100 {
                            slow_started.notify_one();
                            release_slow.notified().await;
                        } else {
                            fast_finished.notify_one();
                        }
                        (Some(ConsumeOrderlyStatus::Success), context)
                    }
                },
            )
        };
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            None,
        )
        .await;

        let queue_a = MessageQueue::new("orders", "broker-a", 0);
        let queue_b = MessageQueue::new("orders", "broker-a", 1);
        let process_queue_a = Arc::new(ProcessQueue::new(queue_a, 89));
        let process_queue_b = Arc::new(ProcessQueue::new(queue_b, 90));
        process_queue_a
            .insert_messages(89, vec![retry_test_message(100)])
            .unwrap();
        process_queue_b
            .insert_messages(90, vec![retry_test_message(200)])
            .unwrap();

        let slow_notice = slow_started.notified();
        let task_consumer = consumer.clone();
        let task_listener = listener.clone();
        let task_a = process_queue_a.clone();
        let slow = tokio::spawn(async move {
            task_consumer
                .run_orderly_consume_request(
                    OrderlyConsumeRequest {
                        process_queue: task_a,
                        generation: 89,
                    },
                    task_listener,
                )
                .await
        });
        slow_notice.await;

        let fast_notice = fast_finished.notified();
        let task_consumer = consumer.clone();
        let task_listener = listener.clone();
        let task_b = process_queue_b.clone();
        let fast = tokio::spawn(async move {
            task_consumer
                .run_orderly_consume_request(
                    OrderlyConsumeRequest {
                        process_queue: task_b,
                        generation: 90,
                    },
                    task_listener,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), fast_notice)
            .await
            .expect("the second queue must not wait on the first queue's callback");
        fast.await.unwrap();
        release_slow.notify_one();
        slow.await.unwrap();

        assert_eq!(process_queue_a.snapshot(89).unwrap().cached_count, 0);
        assert_eq!(process_queue_b.snapshot(90).unwrap().cached_count, 0);
        assert_eq!(offset_store.updates.lock().unwrap().len(), 2);
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_lock_loss_during_callback_rolls_back_without_committing() {
        let mut config = PushConsumerConfig::new("orderly-lock-loss").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let callback_started = Arc::new(tokio::sync::Notify::new());
        let release_callback = Arc::new(tokio::sync::Notify::new());
        let listener: Arc<dyn MessageListenerOrderly> = {
            let callback_started = callback_started.clone();
            let release_callback = release_callback.clone();
            Arc::new(
                move |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                    let callback_started = callback_started.clone();
                    let release_callback = release_callback.clone();
                    async move {
                        callback_started.notify_one();
                        release_callback.notified().await;
                        (Some(ConsumeOrderlyStatus::Success), context)
                    }
                },
            )
        };
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let settings = OrderlyLockSettings::from_values(30_000, 20_000).unwrap();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            Some(settings),
        )
        .await;

        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 91));
        process_queue
            .insert_messages(91, vec![retry_test_message(300)])
            .unwrap();
        process_queue
            .set_lock_state(91, true, epoch_millis())
            .unwrap();
        let callback_notice = callback_started.notified();
        let task_consumer = consumer.clone();
        let task_listener = listener.clone();
        let task_queue = process_queue.clone();
        let task = tokio::spawn(async move {
            task_consumer
                .run_orderly_consume_request(
                    OrderlyConsumeRequest {
                        process_queue: task_queue,
                        generation: 91,
                    },
                    task_listener,
                )
                .await
        });
        callback_notice.await;
        tokio::time::advance(Duration::from_millis(30_001)).await;
        release_callback.notify_one();
        task.await.unwrap();

        let snapshot = process_queue.snapshot(91).unwrap();
        assert_eq!(snapshot.cached_count, 1);
        assert_eq!(snapshot.in_flight_count, 0);
        assert!(snapshot.next_commit_offset == Some(300));
        assert!(offset_store.updates.lock().unwrap().is_empty());
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_lock_renewal_uses_java_initial_delay_and_interval() {
        let mut config = PushConsumerConfig::new("orderly-lock-renewal").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store: Arc<dyn OffsetStore> = Arc::new(RecordingOffsetStore::default());
        let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (Some(ConsumeOrderlyStatus::Success), context)
            },
        );
        let settings = OrderlyLockSettings::from_values(30_000, 20_000).unwrap();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store,
            listener,
            Some(settings),
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 92));
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        let requested = {
            let mut scripted = consumer.scripted_lock_transport.lock().unwrap();
            scripted.lock_results.extend([
                Ok(Some(HashSet::from([Some(queue.clone())]))),
                Ok(Some(HashSet::new())),
            ]);
            scripted.unlock_results.push_back(Ok(()));
            scripted.lock_requested.clone()
        };

        let first_notice = requested.notified();
        tokio::time::advance(Duration::from_millis(999)).await;
        tokio::task::yield_now().await;
        assert!(
            consumer
                .scripted_lock_transport
                .lock()
                .unwrap()
                .lock_bodies
                .is_empty()
        );
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::time::timeout(Duration::from_secs(1), first_notice)
            .await
            .expect("first Java renewal starts after 1000ms");
        assert!(process_queue.snapshot(92).unwrap().locked);

        let second_notice = requested.notified();
        tokio::time::advance(Duration::from_millis(20_000)).await;
        tokio::time::timeout(Duration::from_secs(1), second_notice)
            .await
            .expect("subsequent Java renewals use the configured interval");
        assert!(!process_queue.snapshot(92).unwrap().locked);
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn orderly_rebalance_bounds_slow_callback_wait_and_never_commits_dropped_work() {
        let mut config = PushConsumerConfig::new("orderly-slow-rebalance").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .unlock_results
            .push_back(Ok(()));
        let callback_started = Arc::new(tokio::sync::Notify::new());
        let release_callback = Arc::new(tokio::sync::Notify::new());
        let listener: Arc<dyn MessageListenerOrderly> = {
            let callback_started = callback_started.clone();
            let release_callback = release_callback.clone();
            Arc::new(
                move |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| {
                    let callback_started = callback_started.clone();
                    let release_callback = release_callback.clone();
                    async move {
                        callback_started.notify_one();
                        release_callback.notified().await;
                        (Some(ConsumeOrderlyStatus::Success), context)
                    }
                },
            )
        };
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime = install_started_orderly_consumer(
            &consumer,
            &manager,
            offset_store_dyn,
            listener.clone(),
            None,
        )
        .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 93));
        process_queue
            .set_lock_state(93, true, epoch_millis())
            .unwrap();
        process_queue
            .insert_messages(93, vec![retry_test_message(400)])
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        offset_store.update_offset(&queue, 400, false);
        let context = consumer.pull_context().await.unwrap();

        let callback_notice = callback_started.notified();
        let task_consumer = consumer.clone();
        let task_listener = listener.clone();
        let task_queue = process_queue.clone();
        let task = tokio::spawn(async move {
            task_consumer
                .run_orderly_consume_request(
                    OrderlyConsumeRequest {
                        process_queue: task_queue,
                        generation: 93,
                    },
                    task_listener,
                )
                .await
        });
        callback_notice.await;
        process_queue.drop_assignment(93).unwrap();
        let removal_started = Instant::now();
        assert!(
            !consumer
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
        );
        assert!(Instant::now().duration_since(removal_started) <= Duration::from_millis(500));
        assert!(read_lock(&consumer.process_queues).contains_key(&queue));
        tokio::time::advance(Duration::from_millis(20_001)).await;
        assert!(
            consumer
                .remove_process_queue_safely(&context, &queue, &process_queue)
                .await
        );
        assert_eq!(
            offset_store.persisted.lock().unwrap().get(&queue),
            Some(&400)
        );
        assert!(!read_lock(&consumer.process_queues).contains_key(&queue));

        release_callback.notify_one();
        task.await.unwrap();
        assert_eq!(process_queue.snapshot(93).unwrap().in_flight_count, 1);
        assert_eq!(offset_store.updates.lock().unwrap().len(), 1);
        consumer.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orderly_shutdown_persists_offsets_and_reports_unconfirmed_unlock() {
        let mut config = PushConsumerConfig::new("orderly-unlock-failure").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(rocketmq_classic_protocol::heartbeat::MessageModel::Clustering);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PushConsumer::with_manager(config, manager.clone()));
        let offset_store = Arc::new(RecordingOffsetStore::default());
        let listener: Arc<dyn MessageListenerOrderly> = Arc::new(
            |_messages: Vec<MessageExt>, context: ConsumeOrderlyContext| async move {
                (Some(ConsumeOrderlyStatus::Success), context)
            },
        );
        let offset_store_dyn: Arc<dyn OffsetStore> = offset_store.clone();
        let runtime =
            install_started_orderly_consumer(&consumer, &manager, offset_store_dyn, listener, None)
                .await;
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let process_queue = Arc::new(ProcessQueue::new(queue.clone(), 94));
        process_queue
            .set_lock_state(94, true, epoch_millis())
            .unwrap();
        write_lock(&consumer.process_queues).insert(queue.clone(), process_queue.clone());
        offset_store.update_offset(&queue, 500, false);
        consumer
            .scripted_lock_transport
            .lock()
            .unwrap()
            .unlock_results
            .push_back(Err("scripted unlock timeout".to_owned()));

        assert!(matches!(
            consumer.shutdown().await,
            Err(PushConsumerError::OrderlyUnlockFailed)
        ));
        assert_eq!(
            offset_store.persisted.lock().unwrap().get(&queue),
            Some(&500)
        );
        assert!(process_queue.snapshot(94).unwrap().locked);
        assert!(read_lock(&consumer.pending_orderly_unlocks).contains(&queue));
        assert_eq!(
            consumer
                .scripted_lock_transport
                .lock()
                .unwrap()
                .unlock_bodies
                .len(),
            1
        );
        runtime.shutdown().await.unwrap();
    }
}
