use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex, RwLock, Weak,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};

#[cfg(test)]
use std::collections::VecDeque;

use rocketmq_classic_common::{
    message::{DecodeLimits, DecodeOptions, MessageExt, MessageQueue},
    sysflag::pull::{build_pull_sys_flag_with_lite, clear_commit_offset_flag},
};
use rocketmq_classic_core::{
    api::admin::{AdminQueryRequest, QueryResult, decode_message_id},
    route::{BrokerAddress, RouteSnapshot},
    runtime::{
        ClientRuntime, ClientRuntimeManager, ConsumerOperation, ConsumerRuntime, GroupLease,
    },
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::ConsumerRunningInfoBody,
    codes::{request, response},
    header::{
        BoundaryType, GetEarliestMsgStoretimeRequestHeader, GetMaxOffsetRequestHeader,
        GetMinOffsetRequestHeader, PullMessageRequestHeader, SearchOffsetRequestHeader,
        ViewMessageRequestHeader,
    },
    heartbeat::{
        ConsumeFromWhere, ConsumeType, ConsumerData, ConsumerSubscriptionDataSet, MessageModel,
        SubscriptionCodeSet, SubscriptionData, SubscriptionTagSet,
    },
};
use rocketmq_classic_remoting::client::RpcHook;
use tokio::time::Instant;

use crate::{
    config::PullConsumerConfig,
    consume::{ConsumeMessageContext, ConsumeMessageHook, ConsumeMessageHookRegistry},
    error::PullConsumerError,
    offset::{LocalFileOffsetStore, OffsetStore, ReadOffsetType, RemoteBrokerOffsetStore},
    pull::{FilterMessageHook, ProcessedPullResult, PullAPIWrapper, resolve_pull_broker_address},
    push::MessageQueueListener,
    rebalance::AllocateMessageQueueStrategy,
};

const MAX_PULL_MESSAGE_BYTES: i32 = i32::MAX;
const FIRST_BROKER_SUPPORTING_EXPRESSION_FILTER: i32 = 232;

type RegisteredSubscriptionKey = (String, String, String);
type RegisteredSubscriptions = Arc<RwLock<HashMap<RegisteredSubscriptionKey, SubscriptionData>>>;

/// Selector API corresponding to Java `MessageSelector`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSelector {
    expression_type: String,
    expression: String,
}

impl MessageSelector {
    pub fn by_sql(expression: impl Into<String>) -> Self {
        Self {
            expression_type: "SQL92".into(),
            expression: expression.into(),
        }
    }

    pub fn by_tag(expression: impl Into<String>) -> Self {
        Self {
            expression_type: "TAG".into(),
            expression: expression.into(),
        }
    }

    pub fn expression_type(&self) -> &str {
        &self.expression_type
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }
}

enum State {
    Created,
    Started {
        runtime: Weak<ClientRuntime>,
        lease: GroupLease,
        offset_store: Arc<dyn OffsetStore>,
    },
    Shutdown,
}

/// Explicit Pull Consumer facade. Pulling returns decoded messages but leaves
/// the offset unchanged; applications choose when to update and persist it.
pub struct PullConsumer {
    config: PullConsumerConfig,
    manager: ClientRuntimeManager,
    rpc_hook: Option<Arc<dyn RpcHook>>,
    consume_message_hooks: ConsumeMessageHookRegistry,
    state: tokio::sync::Mutex<State>,
    pull_api: PullAPIWrapper,
    offset_store: Mutex<Option<Arc<dyn OffsetStore>>>,
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    register_subscriptions: RegisteredSubscriptions,
    additional_heartbeat_subscriptions: Arc<RwLock<ConsumerSubscriptionDataSet>>,
    topic_queues: Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>>,
    register_topics: Arc<RwLock<BTreeSet<String>>>,
    in_balance_queues: Arc<RwLock<BTreeSet<MessageQueue>>>,
    message_queue_listener: Arc<RwLock<Option<MessageQueueListener>>>,
    enable_rebalance: Arc<AtomicBool>,
    rebalance_active: Arc<AtomicBool>,
    runtime_for_wakeup: RwLock<Option<Weak<ClientRuntime>>>,
    consumer_start_timestamp: AtomicI64,
}

impl PullConsumer {
    pub fn new(config: PullConsumerConfig) -> Self {
        Self::with_manager(config, ClientRuntimeManager::shared_default())
    }

    fn with_manager(config: PullConsumerConfig, manager: ClientRuntimeManager) -> Self {
        Self::with_manager_and_hook(config, manager, None)
    }

    /// Creates a pull consumer whose remoting requests use the supplied RPC hook.
    pub fn new_with_rpc_hook(
        config: PullConsumerConfig,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Self {
        Self::with_manager_and_hook(config, ClientRuntimeManager::shared_default(), rpc_hook)
    }

    fn with_manager_and_hook(
        config: PullConsumerConfig,
        manager: ClientRuntimeManager,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Self {
        let pull_api = PullAPIWrapper::new(config.unit_mode());
        let message_queue_listener = config.message_queue_listener();
        let enable_rebalance = config.enable_rebalance();
        Self {
            config,
            manager,
            rpc_hook,
            consume_message_hooks: ConsumeMessageHookRegistry::default(),
            state: tokio::sync::Mutex::new(State::Created),
            pull_api,
            offset_store: Mutex::new(None),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            register_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            additional_heartbeat_subscriptions: Arc::new(RwLock::new(
                ConsumerSubscriptionDataSet::new(),
            )),
            topic_queues: Arc::new(RwLock::new(HashMap::new())),
            register_topics: Arc::new(RwLock::new(BTreeSet::new())),
            in_balance_queues: Arc::new(RwLock::new(BTreeSet::new())),
            message_queue_listener: Arc::new(RwLock::new(message_queue_listener)),
            enable_rebalance: Arc::new(AtomicBool::new(enable_rebalance)),
            rebalance_active: Arc::new(AtomicBool::new(false)),
            runtime_for_wakeup: RwLock::new(None),
            consumer_start_timestamp: AtomicI64::new(0),
        }
    }

    pub fn config(&self) -> &PullConsumerConfig {
        &self.config
    }

    pub(crate) fn running_info_body(
        &self,
        runtime: &ClientRuntime,
        jstack: Option<String>,
    ) -> ConsumerRunningInfoBody {
        let subscriptions = read_lock(&self.subscriptions)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        ConsumerRunningInfoBody {
            properties: crate::admin::pull_running_info_properties(
                &self.config,
                runtime,
                self.consumer_start_timestamp.load(Ordering::Acquire),
            ),
            subscription_set: subscriptions,
            jstack,
            ..ConsumerRunningInfoBody::default()
        }
    }

    /// Registers a Java-compatible hook around each pull result.
    pub fn register_consume_message_hook(&self, hook: Arc<dyn ConsumeMessageHook>) {
        self.consume_message_hooks.register(hook);
    }

    pub fn register_filter_message_hook(&self, hook: Arc<dyn FilterMessageHook>) {
        self.pull_api.register_filter_message_hook(hook);
    }

    pub(crate) fn configure_pull_api(&self, connect_broker_by_user: bool, default_broker_id: i64) {
        self.pull_api
            .set_connect_broker_by_user(connect_broker_by_user);
        self.pull_api.set_default_broker_id(default_broker_id);
    }

    pub(crate) fn set_additional_heartbeat_subscriptions(
        &self,
        subscriptions: Arc<RwLock<ConsumerSubscriptionDataSet>>,
    ) {
        *write_lock(&self.additional_heartbeat_subscriptions) = subscriptions
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
    }

    pub fn message_queue_listener(&self) -> Option<MessageQueueListener> {
        read_lock(&self.message_queue_listener).clone()
    }

    pub fn set_message_queue_listener(&self, listener: Option<MessageQueueListener>) {
        *write_lock(&self.message_queue_listener) = listener;
    }

    pub fn enable_rebalance(&self) -> bool {
        self.enable_rebalance.load(Ordering::Acquire)
    }

    pub fn set_enable_rebalance(&self, enabled: bool) {
        self.enable_rebalance.store(enabled, Ordering::Release);
        self.wakeup_rebalance();
    }

    /// Returns the configured or model-selected offset store, if available.
    /// Like Java `getOffsetStore`, this is `None` until a store is configured
    /// or startup selects the default implementation.
    pub fn get_offset_store(&self) -> Option<Arc<dyn OffsetStore>> {
        self.offset_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Select a user-supplied OffsetStore before startup. `None` restores the
    /// Java default selected from the message model.
    pub async fn set_offset_store(
        &self,
        offset_store: Option<Arc<dyn OffsetStore>>,
    ) -> Result<(), PullConsumerError> {
        let state = self.state.lock().await;
        if !matches!(&*state, State::Created) {
            return Err(match &*state {
                State::Started { .. } => PullConsumerError::OffsetStoreAfterStartup,
                State::Shutdown => PullConsumerError::Shutdown,
                State::Created => unreachable!(),
            });
        }
        *self
            .offset_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = offset_store;
        Ok(())
    }

    pub fn register_topic(&self, topic: &str) -> Result<(), PullConsumerError> {
        let topic = self.topic_with_namespace(topic)?;
        let registered = write_lock(&self.register_topics).insert(topic.clone());
        write_lock(&self.subscriptions).insert(topic.clone(), subscription_all(&topic));
        if registered {
            self.wakeup_rebalance();
        }
        Ok(())
    }

    pub fn unregister_topic(&self, topic: &str) -> Result<bool, PullConsumerError> {
        let topic = self.topic_with_namespace(topic)?;
        let removed_registration = write_lock(&self.register_topics).remove(&topic);
        let removed_subscription = write_lock(&self.subscriptions).remove(&topic).is_some();
        let removed_registered_subscriptions = {
            let mut subscriptions = write_lock(&self.register_subscriptions);
            let before = subscriptions.len();
            subscriptions.retain(|(registered_topic, _, _), _| registered_topic != &topic);
            subscriptions.len() != before
        };
        if removed_registration || removed_subscription || removed_registered_subscriptions {
            self.wakeup_rebalance();
        }
        Ok(removed_registration || removed_subscription || removed_registered_subscriptions)
    }

    pub fn register_message_queue_listener(
        &self,
        topic: &str,
        listener: Option<MessageQueueListener>,
    ) -> Result<(), PullConsumerError> {
        if let Some(listener) = listener {
            self.set_message_queue_listener(Some(listener));
        }
        self.register_topic(topic)
    }

    pub fn register_topics(&self) -> BTreeSet<String> {
        read_lock(&self.register_topics).clone()
    }

    pub fn register_subscriptions(&self) -> Vec<SubscriptionData> {
        read_lock(&self.register_subscriptions)
            .values()
            .cloned()
            .collect()
    }

    pub fn add_register_subscriptions(
        &self,
        topic: &str,
        selector: Option<&MessageSelector>,
    ) -> Result<(), PullConsumerError> {
        let topic = self.topic_with_namespace(topic)?;
        let default_selector = MessageSelector::by_tag("*");
        let selector = selector.unwrap_or(&default_selector);
        let subscription = build_subscription(
            &topic,
            Some(selector.expression()),
            Some(selector.expression_type()),
        )
        .map_err(|error| PullConsumerError::InvalidSubscription(error.to_string()))?;
        let key = (
            topic,
            selector.expression_type().to_owned(),
            selector.expression().to_owned(),
        );
        write_lock(&self.register_subscriptions).insert(key, subscription);
        self.wakeup_rebalance();
        Ok(())
    }

    pub fn clear_register_subscriptions(&self) {
        write_lock(&self.register_subscriptions).clear();
        self.wakeup_rebalance();
    }

    pub fn set_register_topics<I, S>(&self, topics: I) -> Result<(), PullConsumerError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut normalized = BTreeSet::new();
        for topic in topics {
            normalized.insert(self.topic_with_namespace(topic.as_ref())?);
        }
        let old = std::mem::replace(&mut *write_lock(&self.register_topics), normalized.clone());
        let mut subscriptions = write_lock(&self.subscriptions);
        for topic in old.difference(&normalized) {
            subscriptions.remove(topic);
        }
        for topic in normalized {
            subscriptions.insert(topic.clone(), subscription_all(&topic));
        }
        drop(subscriptions);
        self.wakeup_rebalance();
        Ok(())
    }

    pub async fn fetch_message_queues_in_balance(
        &self,
        topic: &str,
    ) -> Result<BTreeSet<MessageQueue>, PullConsumerError> {
        let _ = self.started_runtime().await?;
        let topic = self.topic_with_namespace(topic)?;
        let queues = read_lock(&self.in_balance_queues)
            .iter()
            .filter(|queue| queue.topic() == Some(topic.as_str()))
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut client_config = self.config.client_config().clone();
        queues
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
            .map_err(PullConsumerError::from)
    }

    /// Returns the route's full queue set, independently of this consumer's
    /// current rebalance allocation.
    pub async fn fetch_message_queues(
        &self,
        input_topic: &str,
    ) -> Result<BTreeSet<MessageQueue>, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let topic = self.topic_with_namespace(input_topic)?;
        if !runtime.route_snapshot()?.topics.contains_key(&topic) {
            let timeout = i64::from(self.config.client_config().mq_client_api_timeout()).max(0);
            runtime
                .update_topic_route(
                    &topic,
                    Instant::now() + Duration::from_millis(timeout as u64),
                )
                .await?;
        }
        // The consumer's topic_queues table is populated by subscription
        // rebalance callbacks. Lite's fetchMessageQueues API also accepts
        // topics this consumer has not subscribed to, so read the complete
        // converted route snapshot instead of the rebalance table.
        let queues = runtime
            .route_snapshot()?
            .topics
            .get(&topic)
            .map(|snapshot| snapshot.subscribe_queues.clone())
            .unwrap_or_default();
        let mut client_config = self.config.client_config().clone();
        queues
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
            .map_err(PullConsumerError::from)
    }

    pub async fn refresh_topic_route(&self, input_topic: &str) -> Result<(), PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let topic = self.topic_with_namespace(input_topic)?;
        let timeout = i64::from(self.config.client_config().mq_client_api_timeout()).max(0);
        runtime
            .update_topic_route(
                &topic,
                Instant::now() + Duration::from_millis(timeout as u64),
            )
            .await?;
        Ok(())
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), PullConsumerError> {
        let mut state = self.state.lock().await;
        match &*state {
            State::Started { .. } => return Err(PullConsumerError::AlreadyStarted),
            State::Shutdown => return Err(PullConsumerError::Shutdown),
            State::Created => {}
        }
        let mut client_config = self.config.client_config().clone();
        let consumer_group = client_config
            .with_namespace(Some(self.config.consumer_group()))?
            .ok_or(PullConsumerError::InvalidConsumerGroup)?;
        self.config.validate(&consumer_group)?;

        let runtime = self
            .manager
            .get_or_create_with_hook_and_tls_config(
                &client_config,
                self.rpc_hook.clone(),
                self.config.tls_config().cloned(),
            )
            .await?;
        let offset_store = self.select_offset_store(&runtime, &consumer_group)?;
        offset_store.load().await?;

        self.consumer_start_timestamp
            .store(epoch_millis(), Ordering::Release);
        let owner: Arc<dyn ConsumerRuntime> = Arc::new(PullConsumerRegistration {
            group: consumer_group.clone(),
            message_model: self.config.message_model(),
            consume_from_where: self.config.consume_from_where(),
            unit_mode: self.config.unit_mode(),
            route_timeout_millis: i64::from(client_config.mq_client_api_timeout()),
            enable_rebalance: self.enable_rebalance.clone(),
            rebalance_active: self.rebalance_active.clone(),
            strategy: self.config.allocate_message_queue_strategy(),
            message_queue_listener: self.message_queue_listener.clone(),
            register_topics: self.register_topics.clone(),
            register_subscriptions: self.register_subscriptions.clone(),
            additional_heartbeat_subscriptions: self.additional_heartbeat_subscriptions.clone(),
            topic_queues: self.topic_queues.clone(),
            in_balance_queues: self.in_balance_queues.clone(),
            offset_store: Arc::downgrade(&offset_store),
            runtime: Arc::downgrade(&runtime),
            consumer: Arc::downgrade(self),
            rebalance_execution: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            rebalance_membership: Mutex::new(HashMap::new()),
        });
        let lease = runtime.register_consumer(&consumer_group, owner).await?;
        self.rebalance_active.store(true, Ordering::Release);
        if let Err(error) = runtime.start().await {
            self.rebalance_active.store(false, Ordering::Release);
            let _ = runtime
                .unregister_consumer(&lease, self.unregister_deadline())
                .await;
            return Err(error.into());
        }
        *state = State::Started {
            runtime: Arc::downgrade(&runtime),
            lease,
            offset_store,
        };
        *write_lock(&self.runtime_for_wakeup) = Some(Arc::downgrade(&runtime));
        runtime.rebalance_immediately();
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), PullConsumerError> {
        let mut state = self.state.lock().await;
        self.rebalance_active.store(false, Ordering::Release);
        let (runtime, lease, offset_store) = match &*state {
            State::Created | State::Shutdown => return Ok(()),
            State::Started {
                runtime,
                lease,
                offset_store,
            } => {
                let Some(runtime) = runtime.upgrade() else {
                    *state = State::Shutdown;
                    return Ok(());
                };
                (runtime, lease.clone(), offset_store.clone())
            }
        };

        let queues = offset_store
            .clone_offset_table(None)
            .into_keys()
            .collect::<Vec<_>>();
        // Java shutdown swallows a persistence error, then still unregisters.
        let _ = offset_store.persist_all(&queues).await;
        runtime
            .unregister_consumer(&lease, self.unregister_deadline())
            .await?;
        runtime.shutdown().await?;
        *state = State::Shutdown;
        *write_lock(&self.runtime_for_wakeup) = None;
        Ok(())
    }

    pub async fn is_running(&self) -> bool {
        matches!(&*self.state.lock().await, State::Started { .. })
    }

    pub async fn update_name_server_address(
        &self,
        addresses: &str,
    ) -> Result<(), PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        runtime.update_name_server_address_list(addresses);
        Ok(())
    }

    pub async fn pull(
        &self,
        queue: &MessageQueue,
        subscription: Option<&str>,
        offset: i64,
        max_message_count: i32,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_timeout(
            queue,
            subscription,
            offset,
            max_message_count,
            self.config.consumer_pull_timeout_millis(),
        )
        .await
    }

    pub async fn pull_with_timeout(
        &self,
        queue: &MessageQueue,
        subscription: Option<&str>,
        offset: i64,
        max_message_count: i32,
        timeout_millis: i64,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression(
            queue,
            subscription,
            Some("TAG"),
            offset,
            max_message_count,
            MAX_PULL_MESSAGE_BYTES,
            timeout_millis,
            false,
        )
        .await
    }

    /// Pull with a caller-selected maximum response size, in bytes.
    ///
    /// The default pull methods use `i32::MAX`, matching Java's unbounded
    /// `maxSizeInBytes` default. A custom size must be positive.
    pub async fn pull_with_max_size(
        &self,
        queue: &MessageQueue,
        subscription: Option<&str>,
        offset: i64,
        max_message_count: i32,
        max_size_in_bytes: i32,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_max_size_and_timeout(
            queue,
            subscription,
            offset,
            max_message_count,
            max_size_in_bytes,
            self.config.consumer_pull_timeout_millis(),
        )
        .await
    }

    pub async fn pull_with_max_size_and_timeout(
        &self,
        queue: &MessageQueue,
        subscription: Option<&str>,
        offset: i64,
        max_message_count: i32,
        max_size_in_bytes: i32,
        timeout_millis: i64,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression(
            queue,
            subscription,
            Some("TAG"),
            offset,
            max_message_count,
            max_size_in_bytes,
            timeout_millis,
            false,
        )
        .await
    }

    pub async fn pull_with_selector(
        &self,
        queue: &MessageQueue,
        selector: &MessageSelector,
        offset: i64,
        max_message_count: i32,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_selector_with_timeout(
            queue,
            selector,
            offset,
            max_message_count,
            self.config.consumer_pull_timeout_millis(),
        )
        .await
    }

    pub async fn pull_selector_with_timeout(
        &self,
        queue: &MessageQueue,
        selector: &MessageSelector,
        offset: i64,
        max_message_count: i32,
        timeout_millis: i64,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression(
            queue,
            Some(&selector.expression),
            Some(&selector.expression_type),
            offset,
            max_message_count,
            MAX_PULL_MESSAGE_BYTES,
            timeout_millis,
            false,
        )
        .await
    }

    pub async fn pull_block_if_not_found(
        &self,
        queue: &MessageQueue,
        subscription: Option<&str>,
        offset: i64,
        max_message_count: i32,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression(
            queue,
            subscription,
            Some("TAG"),
            offset,
            max_message_count,
            MAX_PULL_MESSAGE_BYTES,
            self.config.consumer_pull_timeout_millis(),
            true,
        )
        .await
    }

    pub async fn pull_block_with_selector(
        &self,
        queue: &MessageQueue,
        selector: &MessageSelector,
        offset: i64,
        max_message_count: i32,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression(
            queue,
            Some(&selector.expression),
            Some(&selector.expression_type),
            offset,
            max_message_count,
            MAX_PULL_MESSAGE_BYTES,
            self.config.consumer_pull_timeout_millis(),
            true,
        )
        .await
    }

    pub(crate) async fn lite_pull_with_selector(
        &self,
        queue: &MessageQueue,
        selector: &MessageSelector,
        offset: i64,
        max_message_count: i32,
        timeout_millis: i64,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression_mode(
            queue,
            Some(&selector.expression),
            Some(&selector.expression_type),
            offset,
            max_message_count,
            MAX_PULL_MESSAGE_BYTES,
            timeout_millis,
            true,
            true,
        )
        .await
    }

    /// Search a queue by a signed Java millisecond timestamp. A successful
    /// response of `-1` is preserved as a valid empty-queue offset.
    pub async fn search_offset(
        &self,
        queue: &MessageQueue,
        timestamp_millis: i64,
    ) -> Result<i64, PullConsumerError> {
        self.search_offset_with_boundary(queue, timestamp_millis, BoundaryType::Lower)
            .await
    }

    pub async fn search_offset_with_boundary(
        &self,
        input_queue: &MessageQueue,
        timestamp_millis: i64,
        boundary_type: BoundaryType,
    ) -> Result<i64, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(input_queue)?;
        let deadline = self.offset_rpc_deadline()?;
        let (broker_name, address) = find_publish_broker(&runtime, &queue, deadline).await?;
        let header = SearchOffsetRequestHeader {
            topic: queue
                .topic()
                .ok_or(PullConsumerError::InvalidQueue)?
                .to_owned(),
            queue_id: queue.queue_id(),
            timestamp: timestamp_millis,
            boundary_type: Some(boundary_type),
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
    ) -> Result<i64, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(input_queue)?;
        let deadline = self.offset_rpc_deadline()?;
        let (broker_name, address) = find_publish_broker(&runtime, &queue, deadline).await?;
        let header = GetMaxOffsetRequestHeader {
            topic: queue
                .topic()
                .ok_or(PullConsumerError::InvalidQueue)?
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
    ) -> Result<i64, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(input_queue)?;
        let deadline = self.offset_rpc_deadline()?;
        let (broker_name, address) = find_publish_broker(&runtime, &queue, deadline).await?;
        let header = GetMinOffsetRequestHeader {
            topic: queue
                .topic()
                .ok_or(PullConsumerError::InvalidQueue)?
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
    ) -> Result<i64, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(input_queue)?;
        let deadline = self.offset_rpc_deadline()?;
        let (broker_name, address) = find_publish_broker(&runtime, &queue, deadline).await?;
        let header = GetEarliestMsgStoretimeRequestHeader {
            topic: queue
                .topic()
                .ok_or(PullConsumerError::InvalidQueue)?
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
    ) -> Result<(), PullConsumerError> {
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
    ) -> Result<(), PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(new_topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PullConsumerError::InvalidAdminTopic(
                "The specified topic is blank.".into(),
            ))?;
        crate::admin::validate_topic(&topic).map_err(PullConsumerError::InvalidAdminTopic)?;
        crate::admin::create_topic(&runtime, key, &topic, queue_num, topic_sys_flag).await?;
        Ok(())
    }

    pub async fn fetch_subscribe_message_queues(
        &self,
        input_topic: &str,
    ) -> Result<BTreeSet<MessageQueue>, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let mut client_config = self.config.client_config().clone();
        let topic = self.topic_with_namespace(input_topic)?;
        let timeout = Duration::from_millis(
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64,
        );
        let queues = crate::admin::fetch_subscribe_queues(&runtime, &topic, timeout).await?;
        if queues.is_empty() {
            return Err(crate::error::PullConsumerError::EmptyMessageQueues(topic));
        }
        queues
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
            .map_err(PullConsumerError::from)
    }

    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<QueryResult, PullConsumerError> {
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
    ) -> Result<QueryResult, PullConsumerError> {
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
    ) -> Result<MessageExt, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let decoded = decode_message_id(message_id).map_err(|_| {
            crate::error::PullConsumerError::Rpc(rocketmq_classic_core::error::RpcError::Response {
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
        Ok(runtime
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
    ) -> Result<QueryResult, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        let mut client_config = self.config.client_config().clone();
        let topic = self.topic_with_namespace(topic)?;
        let mut query = if unique_key {
            AdminQueryRequest::by_unique_key(&topic, key, max_num, begin_timestamp, end_timestamp)
        } else {
            AdminQueryRequest::by_key(&topic, key, max_num, begin_timestamp, end_timestamp)
        };
        query.logical_message_queue = topic.starts_with("%LMQ%");
        let timeout_millis =
            i64::from(self.config.client_config().mq_client_api_timeout()).max(0) as u64;
        let deadline = Instant::now() + Duration::from_millis(timeout_millis).saturating_mul(4);
        let mut result = runtime.query_message(&query, deadline).await?;
        for message in &mut result.message_list {
            if let Some(topic) = client_config.without_namespace(Some(message.message().topic()))? {
                message.message_mut().set_topic(topic);
            }
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    async fn pull_with_expression(
        &self,
        input_queue: &MessageQueue,
        expression: Option<&str>,
        expression_type: Option<&str>,
        offset: i64,
        max_message_count: i32,
        max_size_in_bytes: i32,
        timeout_millis: i64,
        suspend: bool,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        self.pull_with_expression_mode(
            input_queue,
            expression,
            expression_type,
            offset,
            max_message_count,
            max_size_in_bytes,
            timeout_millis,
            suspend,
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn pull_with_expression_mode(
        &self,
        input_queue: &MessageQueue,
        expression: Option<&str>,
        expression_type: Option<&str>,
        offset: i64,
        max_message_count: i32,
        max_size_in_bytes: i32,
        timeout_millis: i64,
        suspend: bool,
        lite_pull: bool,
    ) -> Result<ProcessedPullResult, PullConsumerError> {
        let (runtime, _) = self.started_runtime().await?;
        validate_pull_request(
            input_queue,
            offset,
            max_message_count,
            max_size_in_bytes,
            timeout_millis,
        )?;

        let mut client_config = self.config.client_config().clone();
        let queue = client_config.queue_with_namespace(input_queue)?;
        let topic = queue
            .topic()
            .filter(|topic| !topic.is_empty())
            .ok_or(PullConsumerError::InvalidQueue)?;
        let broker_name = queue
            .broker_name()
            .filter(|broker| !broker.is_empty())
            .ok_or(PullConsumerError::InvalidQueue)?
            .to_owned();

        let subscription = build_subscription(topic, expression, expression_type)?;
        let is_tag = is_tag_type(subscription.get_expression_type());
        self.ensure_subscription(topic);
        let sub_version = if is_tag {
            0
        } else {
            subscription.get_sub_version()
        };
        let mut sys_flag = build_pull_sys_flag_with_lite(
            false,
            suspend,
            true,
            subscription.is_class_filter_mode(),
            lite_pull,
        );
        let pull_timeout_millis = if suspend && !lite_pull {
            self.config.consumer_timeout_millis_when_suspend()
        } else {
            timeout_millis
        };
        let deadline = request_deadline(pull_timeout_millis)?;

        let suggested_broker_id = self.pull_api.recalculate_pull_from_which_node(&queue);
        let (route, broker) =
            find_pull_broker(&runtime, &queue, suggested_broker_id, deadline).await?;
        if !is_tag && broker.version < FIRST_BROKER_SUPPORTING_EXPRESSION_FILTER {
            return Err(PullConsumerError::UnsupportedExpressionBroker {
                broker: broker_name,
                version: broker.version,
                expression_type: subscription
                    .get_expression_type()
                    .unwrap_or_default()
                    .to_owned(),
            });
        }
        if broker.slave {
            sys_flag = clear_commit_offset_flag(sys_flag);
        }

        let broker_address = if subscription.is_class_filter_mode() {
            let route_data = route
                .raw_routes
                .get(topic)
                .ok_or_else(|| PullConsumerError::FilterServerNotFound {
                    broker_address: broker.address.clone(),
                    topic: topic.to_owned(),
                })?
                .snapshot()?;
            resolve_pull_broker_address(&route_data, topic, &broker.address, sys_flag)
                .map_err(|_| PullConsumerError::FilterServerNotFound {
                    broker_address: broker.address.clone(),
                    topic: topic.to_owned(),
                })?
                .ok_or_else(|| PullConsumerError::FilterServerNotFound {
                    broker_address: broker.address.clone(),
                    topic: topic.to_owned(),
                })?
        } else {
            broker.address.clone()
        };

        let header = PullMessageRequestHeader {
            consumer_group: self.effective_consumer_group()?,
            topic: topic.to_owned(),
            queue_id: queue.queue_id(),
            queue_offset: offset,
            max_msg_nums: max_message_count,
            sys_flag,
            commit_offset: 0,
            suspend_timeout_millis: self.config.broker_suspend_max_time_millis(),
            subscription: subscription.get_sub_string().map(str::to_owned),
            sub_version,
            expression_type: subscription.get_expression_type().map(str::to_owned),
            max_msg_bytes: Some(max_size_in_bytes),
            broker_name: Some(broker_name),
            ..PullMessageRequestHeader::default()
        };
        let pull_result = runtime
            .api()
            .pull_message(&broker_address, &header, deadline)
            .await?;
        let options = DecodeOptions {
            read_body: client_config.decode_read_body,
            decompress_body: client_config.decode_decompress_body,
            ..DecodeOptions::default()
        };
        let mut processed = self.pull_api.process_pull_result(
            &queue,
            pull_result,
            &subscription,
            options,
            DecodeLimits::default(),
        )?;
        if let Some(messages) = &mut processed.messages {
            for message in messages {
                let topic = client_config.without_namespace(Some(message.message().topic()))?;
                if let Some(topic) = topic {
                    message.message_mut().set_topic(topic);
                }
            }
        }
        self.execute_consume_message_hooks(input_queue, &mut processed)?;
        Ok(processed)
    }

    fn execute_consume_message_hooks(
        &self,
        queue: &MessageQueue,
        processed: &mut ProcessedPullResult,
    ) -> Result<(), PullConsumerError> {
        let mut client_config = self.config.client_config().clone();
        let mut consume_context = ConsumeMessageContext::new(
            self.effective_consumer_group()?,
            processed.messages.take(),
            queue.clone(),
            client_config.get_namespace()?.map(str::to_owned),
        );
        self.consume_message_hooks
            .execute_before(&mut consume_context);
        consume_context.status = Some("CONSUME_SUCCESS".to_owned());
        consume_context.success = true;
        consume_context.access_channel = client_config.access_channel();
        self.consume_message_hooks
            .execute_after(&mut consume_context);
        processed.messages = consume_context.messages;
        Ok(())
    }

    pub async fn update_consume_offset(
        &self,
        queue: &MessageQueue,
        offset: i64,
    ) -> Result<(), PullConsumerError> {
        let (_, store) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(queue)?;
        store.update_offset(&queue, offset, false);
        Ok(())
    }

    pub async fn fetch_consume_offset(
        &self,
        queue: &MessageQueue,
        from_store: bool,
    ) -> Result<i64, PullConsumerError> {
        let (_, store) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(queue)?;
        let mode = if from_store {
            ReadOffsetType::ReadFromStore
        } else {
            ReadOffsetType::MemoryFirstThenStore
        };
        Ok(store.read_offset(&queue, mode).await?.java_value())
    }

    pub async fn persist_offset(&self, queue: &MessageQueue) -> Result<(), PullConsumerError> {
        let (_, store) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(queue)?;
        store.persist(&queue).await?;
        Ok(())
    }

    pub async fn persist_offsets(
        &self,
        input_queues: &[MessageQueue],
    ) -> Result<(), PullConsumerError> {
        let (_, store) = self.started_runtime().await?;
        let queues = input_queues
            .iter()
            .map(|queue| self.queue_with_namespace(queue))
            .collect::<Result<Vec<_>, _>>()?;
        store.persist_all(&queues).await?;
        Ok(())
    }

    pub async fn persist_consumer_offset(&self) -> Result<(), PullConsumerError> {
        let (_, store) = self.started_runtime().await?;
        let queues = store
            .clone_offset_table(None)
            .into_keys()
            .collect::<Vec<_>>();
        store.persist_all(&queues).await?;
        Ok(())
    }

    pub async fn update_consume_offset_to_broker(
        &self,
        queue: &MessageQueue,
        offset: i64,
        oneway: bool,
    ) -> Result<(), PullConsumerError> {
        let (_, store) = self.started_runtime().await?;
        let queue = self.queue_with_namespace(queue)?;
        store
            .update_consume_offset_to_broker(&queue, offset, oneway)
            .await?;
        Ok(())
    }

    fn select_offset_store(
        &self,
        runtime: &Arc<ClientRuntime>,
        consumer_group: &str,
    ) -> Result<Arc<dyn OffsetStore>, PullConsumerError> {
        let mut selected_store = self
            .offset_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(store) = selected_store.clone() {
            return Ok(store);
        }
        let store: Arc<dyn OffsetStore> = match self.config.message_model() {
            MessageModel::Broadcasting => Arc::new(LocalFileOffsetStore::new(
                runtime.client_id(),
                consumer_group,
            )?),
            MessageModel::Clustering | MessageModel::LiteSelective => Arc::new(
                RemoteBrokerOffsetStore::new(runtime.clone(), consumer_group),
            ),
        };
        *selected_store = Some(store.clone());
        Ok(store)
    }

    async fn started_runtime(
        &self,
    ) -> Result<(Arc<ClientRuntime>, Arc<dyn OffsetStore>), PullConsumerError> {
        let state = self.state.lock().await;
        match &*state {
            State::Created => Err(PullConsumerError::NotStarted),
            State::Shutdown => Err(PullConsumerError::Shutdown),
            State::Started {
                runtime,
                offset_store,
                ..
            } => runtime
                .upgrade()
                .map(|runtime| (runtime, offset_store.clone()))
                .ok_or(PullConsumerError::Shutdown),
        }
    }

    fn queue_with_namespace(
        &self,
        queue: &MessageQueue,
    ) -> Result<MessageQueue, PullConsumerError> {
        Ok(self
            .config
            .client_config()
            .clone()
            .queue_with_namespace(queue)?)
    }

    fn topic_with_namespace(&self, topic: &str) -> Result<String, PullConsumerError> {
        if topic.is_empty() {
            return Err(PullConsumerError::InvalidQueue);
        }
        self.config
            .client_config()
            .clone()
            .with_namespace(Some(topic))?
            .filter(|topic| !topic.is_empty())
            .ok_or(PullConsumerError::InvalidQueue)
    }

    fn wakeup_rebalance(&self) {
        if let Some(runtime) = read_lock(&self.runtime_for_wakeup)
            .as_ref()
            .and_then(Weak::upgrade)
        {
            runtime.rebalance_immediately();
        }
    }

    fn effective_consumer_group(&self) -> Result<String, PullConsumerError> {
        let mut client_config = self.config.client_config().clone();
        client_config
            .with_namespace(Some(self.config.consumer_group()))?
            .ok_or(PullConsumerError::InvalidConsumerGroup)
    }

    fn offset_rpc_deadline(&self) -> Result<Instant, PullConsumerError> {
        request_deadline(i64::from(
            self.config.client_config().mq_client_api_timeout(),
        ))
    }

    fn ensure_subscription(&self, topic: &str) {
        write_lock(&self.subscriptions)
            .entry(topic.to_owned())
            .or_insert_with(|| subscription_all(topic));
    }

    fn unregister_deadline(&self) -> Instant {
        Instant::now()
            + Duration::from_millis(self.config.client_config().mq_client_api_timeout.max(0) as u64)
    }
}

struct PullConsumerRegistration {
    group: String,
    message_model: MessageModel,
    consume_from_where: ConsumeFromWhere,
    unit_mode: bool,
    route_timeout_millis: i64,
    enable_rebalance: Arc<AtomicBool>,
    rebalance_active: Arc<AtomicBool>,
    strategy: Arc<dyn AllocateMessageQueueStrategy>,
    message_queue_listener: Arc<RwLock<Option<MessageQueueListener>>>,
    register_topics: Arc<RwLock<BTreeSet<String>>>,
    register_subscriptions: RegisteredSubscriptions,
    additional_heartbeat_subscriptions: Arc<RwLock<ConsumerSubscriptionDataSet>>,
    topic_queues: Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>>,
    in_balance_queues: Arc<RwLock<BTreeSet<MessageQueue>>>,
    offset_store: Weak<dyn OffsetStore>,
    runtime: Weak<ClientRuntime>,
    consumer: Weak<PullConsumer>,
    rebalance_execution: tokio::sync::Mutex<()>,
    #[cfg(test)]
    rebalance_membership: Mutex<HashMap<String, VecDeque<Option<Vec<String>>>>>,
}

impl ConsumerRuntime for PullConsumerRegistration {
    fn inbound_processor(
        &self,
        operation: ConsumerOperation,
    ) -> Option<Arc<dyn rocketmq_classic_remoting::client::RequestProcessor>> {
        match operation {
            ConsumerOperation::ConsumerStatus => Some(Arc::new(
                crate::admin::ConsumerStatusProcessor::new(self.offset_store.clone()),
            )),
            ConsumerOperation::RunningInfo => Some(Arc::new(
                crate::admin::ConsumerRunningInfoProcessor::for_pull(
                    self.consumer.clone(),
                    self.runtime.clone(),
                ),
            )),
            _ => None,
        }
    }

    fn heartbeat_data(&self) -> ConsumerData {
        // Java sends mutable SubscriptionData values in a HashSet; preserve
        // that identity/equality behavior for the heartbeat wire model.
        let registered_subscriptions = read_lock(&self.register_subscriptions);
        #[allow(clippy::mutable_key_type)]
        let mut subscription_data_set = if registered_subscriptions.is_empty() {
            read_lock(&self.register_topics)
                .iter()
                .map(|topic| {
                    let mut subscription = subscription_all(topic);
                    subscription.set_sub_version(0);
                    Some(SharedValue::from(subscription))
                })
                .collect::<ConsumerSubscriptionDataSet>()
        } else {
            registered_subscriptions
                .values()
                .cloned()
                .map(|subscription| Some(SharedValue::from(subscription)))
                .collect::<ConsumerSubscriptionDataSet>()
        };
        subscription_data_set.extend(
            read_lock(&self.additional_heartbeat_subscriptions)
                .iter()
                .cloned(),
        );
        ConsumerData {
            group_name: Some(self.group.clone()),
            consume_type: Some(ConsumeType::ConsumeActively),
            message_model: Some(self.message_model),
            consume_from_where: Some(self.consume_from_where),
            subscription_data_set: Some(SharedValue::from(subscription_data_set)),
            unit_mode: self.unit_mode,
        }
    }

    fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        read_lock(&self.register_topics).contains(topic)
    }

    fn update_topic_subscribe_info(&self, topic: &str, queues: BTreeSet<MessageQueue>) {
        write_lock(&self.topic_queues).insert(topic.to_owned(), queues);
        if let Some(runtime) = self.runtime.upgrade() {
            runtime.rebalance_immediately();
        }
    }

    fn persist_consumer_offset(
        &self,
    ) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>> {
        let store = self.offset_store.upgrade()?;
        Some(Box::pin(async move {
            let queues = store
                .clone_offset_table(None)
                .into_keys()
                .collect::<Vec<_>>();
            let _ = store.persist_all(&queues).await;
        }))
    }

    fn try_rebalance(
        &self,
    ) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>>> {
        Some(Box::pin(self.rebalance_once()))
    }
}

impl PullConsumerRegistration {
    async fn find_consumer_id_list(
        &self,
        runtime: &ClientRuntime,
        topic: &str,
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
        runtime.find_consumer_id_list(topic, &self.group).await
    }

    async fn rebalance_once(&self) -> bool {
        if !self.rebalance_active.load(Ordering::Acquire)
            || !self.enable_rebalance.load(Ordering::Acquire)
        {
            return true;
        }
        let Some(runtime) = self.runtime.upgrade() else {
            return true;
        };
        let _execution = self.rebalance_execution.lock().await;
        if !self.rebalance_active.load(Ordering::Acquire)
            || !self.enable_rebalance.load(Ordering::Acquire)
        {
            return true;
        }

        let registered = read_lock(&self.register_topics).clone();
        let previous = read_lock(&self.in_balance_queues).clone();
        let mut topics = registered.clone();
        topics.extend(
            previous
                .iter()
                .filter_map(|queue| queue.topic().map(str::to_owned)),
        );
        let mut topics = topics.into_iter().collect::<Vec<_>>();
        topics.sort_by(|left, right| java_string_cmp(left, right));
        let mut balanced = true;

        for topic in topics {
            if !self.rebalance_active.load(Ordering::Acquire) {
                return true;
            }
            let is_registered = registered.contains(&topic);
            if !is_registered {
                let changed = self
                    .reconcile_topic(&topic, Some(HashSet::new()), HashSet::new())
                    .await;
                balanced &= changed;
                continue;
            }
            if self.message_model == MessageModel::LiteSelective {
                continue;
            }

            let mut queue_snapshot = read_lock(&self.topic_queues).get(&topic).cloned();
            if queue_snapshot.is_none() {
                let deadline =
                    Instant::now() + Duration::from_millis(self.route_timeout_millis.max(0) as u64);
                if let Err(error) = runtime.update_topic_route(&topic, deadline).await {
                    tracing::warn!(topic, error = %error, "pull rebalance route lookup failed");
                    if self.message_model == MessageModel::Broadcasting
                        || (self.message_model == MessageModel::Clustering
                            && !topic.starts_with("%RETRY%"))
                    {
                        self.notify_message_queue_listener(
                            &topic,
                            Some(HashSet::new()),
                            HashSet::new(),
                        );
                    }
                    balanced = false;
                    continue;
                }
                queue_snapshot = read_lock(&self.topic_queues).get(&topic).cloned();
            }

            match self.message_model {
                MessageModel::Broadcasting => {
                    let Some(queue_snapshot) = queue_snapshot else {
                        self.notify_message_queue_listener(
                            &topic,
                            Some(HashSet::new()),
                            HashSet::new(),
                        );
                        balanced = false;
                        continue;
                    };
                    let all_queues = queue_snapshot.into_iter().collect::<Vec<_>>();
                    let all = all_queues.iter().cloned().collect::<HashSet<_>>();
                    balanced &= self.reconcile_topic(&topic, Some(all.clone()), all).await;
                }
                MessageModel::Clustering => {
                    let Some(queue_snapshot) = queue_snapshot.filter(|queues| !queues.is_empty())
                    else {
                        if !topic.starts_with("%RETRY%") {
                            self.notify_message_queue_listener(
                                &topic,
                                Some(HashSet::new()),
                                HashSet::new(),
                            );
                        }
                        balanced = false;
                        continue;
                    };
                    let mut all_queues = queue_snapshot.into_iter().collect::<Vec<_>>();
                    all_queues.sort_by(java_message_queue_cmp);
                    let all = all_queues.iter().cloned().collect::<HashSet<_>>();
                    let consumer_ids = match self.find_consumer_id_list(&runtime, &topic).await {
                        Ok(Some(consumer_ids)) if !consumer_ids.is_empty() => consumer_ids,
                        Ok(Some(_)) | Ok(None) => {
                            balanced = false;
                            continue;
                        }
                        Err(error) => {
                            tracing::warn!(topic, error = %error, "pull rebalance consumer list lookup failed");
                            balanced = false;
                            continue;
                        }
                    };
                    let Some(mut consumer_ids) =
                        consumer_ids.into_iter().collect::<Option<Vec<_>>>()
                    else {
                        balanced = false;
                        continue;
                    };
                    consumer_ids.sort_by(|left, right| java_string_cmp(left, right));
                    let allocation = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        self.strategy.allocate(
                            &self.group,
                            runtime.client_id(),
                            &all_queues,
                            &consumer_ids,
                        )
                    }));
                    let assigned = match allocation {
                        Ok(Ok(assigned)) => assigned.into_iter().collect::<HashSet<_>>(),
                        Ok(Err(error)) => {
                            tracing::warn!(topic, strategy = self.strategy.name(), error = %error, "pull queue allocation failed");
                            balanced = false;
                            continue;
                        }
                        Err(_) => {
                            tracing::warn!(
                                topic,
                                strategy = self.strategy.name(),
                                "pull queue allocation panicked"
                            );
                            balanced = false;
                            continue;
                        }
                    };
                    balanced &= self.reconcile_topic(&topic, Some(all), assigned).await;
                }
                MessageModel::LiteSelective => {}
            }
        }
        balanced
    }

    async fn reconcile_topic(
        &self,
        topic: &str,
        all_queues: Option<HashSet<MessageQueue>>,
        desired: HashSet<MessageQueue>,
    ) -> bool {
        let current = read_lock(&self.in_balance_queues)
            .iter()
            .filter(|queue| queue.topic() == Some(topic))
            .cloned()
            .collect::<HashSet<_>>();
        let removed = current.difference(&desired).cloned().collect::<Vec<_>>();
        let added = desired.difference(&current).cloned().collect::<Vec<_>>();
        let has_changes = !removed.is_empty() || !added.is_empty();
        let mut changed = false;
        let mut balanced = true;

        if let Some(store) = self.offset_store.upgrade() {
            for queue in removed {
                if !self.rebalance_active.load(Ordering::Acquire) {
                    return false;
                }
                if let Err(error) = store.persist(&queue).await {
                    tracing::warn!(queue = %queue, error = %error, "pull queue offset persistence failed during rebalance removal");
                    balanced = false;
                    continue;
                }
                if !self.rebalance_active.load(Ordering::Acquire) {
                    return false;
                }
                store.remove_offset(&queue);
                if write_lock(&self.in_balance_queues).remove(&queue) {
                    changed = true;
                }
            }

            for queue in added {
                if !self.rebalance_active.load(Ordering::Acquire) {
                    return false;
                }
                store.remove_offset(&queue);
                if write_lock(&self.in_balance_queues).insert(queue) {
                    changed = true;
                }
            }
        } else if has_changes {
            balanced = false;
        }

        if changed && self.rebalance_active.load(Ordering::Acquire) {
            let assigned = read_lock(&self.in_balance_queues)
                .iter()
                .filter(|queue| queue.topic() == Some(topic))
                .cloned()
                .collect::<HashSet<_>>();
            self.notify_message_queue_listener(topic, all_queues, assigned);
        }
        balanced
    }

    fn notify_message_queue_listener(
        &self,
        topic: &str,
        all_queues: Option<HashSet<MessageQueue>>,
        assigned: HashSet<MessageQueue>,
    ) {
        if !self.rebalance_active.load(Ordering::Acquire) {
            return;
        }
        let listener = read_lock(&self.message_queue_listener).clone();
        if let Some(listener) = listener {
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                listener(topic, all_queues.as_ref(), &assigned);
            }));
        }
    }
}

fn java_string_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn java_message_queue_cmp(left: &MessageQueue, right: &MessageQueue) -> std::cmp::Ordering {
    java_string_cmp(
        left.topic().unwrap_or_default(),
        right.topic().unwrap_or_default(),
    )
    .then_with(|| {
        java_string_cmp(
            left.broker_name().unwrap_or_default(),
            right.broker_name().unwrap_or_default(),
        )
    })
    .then_with(|| left.queue_id().cmp(&right.queue_id()))
}

fn subscription_all(topic: &str) -> SubscriptionData {
    let mut subscription = SubscriptionData::new(Some(topic.to_owned()), Some("*".to_owned()));
    subscription.set_sub_version(epoch_millis());
    subscription
}

pub(crate) fn build_subscription(
    topic: &str,
    expression: Option<&str>,
    expression_type: Option<&str>,
) -> Result<SubscriptionData, PullConsumerError> {
    let expression = expression.unwrap_or("");
    let is_string_tag_api = expression_type == Some("TAG");
    let expression_type = expression_type.filter(|kind| !kind.trim().is_empty());
    if expression_type.is_some_and(|kind| kind != "TAG") && expression.is_empty() {
        return Err(PullConsumerError::EmptySqlExpression);
    }
    let mut subscription = SubscriptionData::new(
        Some(topic.to_owned()),
        Some(if expression.is_empty() || expression == "*" {
            "*".to_owned()
        } else {
            expression.to_owned()
        }),
    );
    subscription.set_sub_version(epoch_millis());
    subscription.set_expression_type(expression_type.map(str::to_owned));

    let tags_api = is_string_tag_api || expression_type.is_none_or(|kind| kind == "TAG");
    if tags_api {
        let (mut tags, mut codes) = (SubscriptionTagSet::new(), SubscriptionCodeSet::new());
        if !expression.is_empty() && expression != "*" {
            for tag in expression
                .split("||")
                .map(str::trim)
                .filter(|tag| !tag.is_empty())
            {
                tags.insert(Some(tag.to_owned()));
                codes.insert(Some(java_string_hash(tag)));
            }
        }
        subscription.set_tags_set_owned(Some(tags));
        subscription.set_code_set_owned(Some(codes));
    }
    Ok(subscription)
}

fn is_tag_type(expression_type: Option<&str>) -> bool {
    expression_type
        .is_none_or(|expression_type| expression_type.is_empty() || expression_type == "TAG")
}

fn validate_pull_request(
    queue: &MessageQueue,
    offset: i64,
    max_message_count: i32,
    max_size_in_bytes: i32,
    timeout_millis: i64,
) -> Result<(), PullConsumerError> {
    if queue.topic().is_none_or(str::is_empty) || queue.broker_name().is_none_or(str::is_empty) {
        return Err(PullConsumerError::InvalidQueue);
    }
    if offset < 0 {
        return Err(PullConsumerError::InvalidOffset);
    }
    if max_message_count <= 0 {
        return Err(PullConsumerError::InvalidMaxMessageCount);
    }
    if max_size_in_bytes <= 0 {
        return Err(PullConsumerError::InvalidMaxMessageBytes);
    }
    request_deadline(timeout_millis)?;
    Ok(())
}

fn java_string_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn request_deadline(timeout_millis: i64) -> Result<Instant, PullConsumerError> {
    let timeout_millis =
        u64::try_from(timeout_millis).map_err(|_| PullConsumerError::InvalidTimeout)?;
    Instant::now()
        .checked_add(Duration::from_millis(timeout_millis))
        .ok_or(PullConsumerError::InvalidTimeout)
}

async fn find_pull_broker(
    runtime: &ClientRuntime,
    queue: &MessageQueue,
    broker_id: i64,
    deadline: Instant,
) -> Result<(Arc<RouteSnapshot>, BrokerAddress), PullConsumerError> {
    let topic = queue.topic().ok_or(PullConsumerError::InvalidQueue)?;
    let broker_name = queue.broker_name().ok_or(PullConsumerError::InvalidQueue)?;
    let mut route = runtime.route_snapshot()?;
    let mut broker = route.subscribe_address(broker_name, broker_id, false)?;
    if broker.is_none() {
        runtime.update_topic_route(topic, deadline).await?;
        route = runtime.route_snapshot()?;
        broker = route.subscribe_address(broker_name, broker_id, false)?;
    }
    broker
        .map(|broker| (route, broker))
        .ok_or_else(|| PullConsumerError::BrokerNotFound {
            broker: broker_name.to_owned(),
        })
}

async fn find_publish_broker(
    runtime: &ClientRuntime,
    queue: &MessageQueue,
    deadline: Instant,
) -> Result<(String, String), PullConsumerError> {
    let topic = queue
        .topic()
        .filter(|topic| !topic.is_empty())
        .ok_or(PullConsumerError::InvalidQueue)?;
    let broker_name = queue
        .broker_name()
        .filter(|broker| !broker.is_empty())
        .ok_or(PullConsumerError::InvalidQueue)?
        .to_owned();
    let mut route = runtime.route_snapshot()?;
    let mut address = route.publish_address(&broker_name).map(str::to_owned);
    if address.is_none() {
        runtime.update_topic_route(topic, deadline).await?;
        route = runtime.route_snapshot()?;
        address = route.publish_address(&broker_name).map(str::to_owned);
    }
    address
        .map(|address| (broker_name.clone(), address))
        .ok_or(PullConsumerError::BrokerNotFound {
            broker: broker_name,
        })
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset::OffsetRead;
    use rocketmq_classic_core::api::consumer::PullStatus;
    use rocketmq_classic_core::config::AccessChannel;
    use rocketmq_classic_remoting::{RemotingCommand, client::RequestProcessor};
    use std::collections::{BTreeMap, HashMap, HashSet};

    #[tokio::test]
    async fn pull_running_info_uses_group_and_reports_active_consumer_properties() {
        let mut config = PullConsumerConfig::new("pull-running-info").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PullConsumer::with_manager(config, manager.clone()));
        consumer.register_topic("orders").unwrap();
        let runtime = manager
            .get_or_create(consumer.config().client_config())
            .await
            .unwrap();
        consumer
            .consumer_start_timestamp
            .store(1_700_000_000_000, Ordering::Release);

        let processor = crate::admin::ConsumerRunningInfoProcessor::for_pull(
            Arc::downgrade(&consumer),
            Arc::downgrade(&runtime),
        );
        let response = processor
            .process(RemotingCommand {
                ext_fields: Some(BTreeMap::from([
                    ("consumerGroup".into(), "pull-running-info".into()),
                    ("clientId".into(), "client-id-is-ignored".into()),
                    ("jstackEnable".into(), "false".into()),
                ])),
                ..Default::default()
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            response.code,
            rocketmq_classic_protocol::codes::response::SUCCESS
        );
        let body = ConsumerRunningInfoBody::decode_java_json(
            std::str::from_utf8(response.body.as_deref().unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(body["properties"]["PROP_CONSUME_TYPE"], "CONSUME_ACTIVELY");
        assert_eq!(body["properties"]["consumerPullTimeoutMillis"], "10000");
        assert_eq!(
            body["properties"]["PROP_CONSUMER_START_TIMESTAMP"],
            "1700000000000"
        );
        assert_eq!(body["subscriptionSet"].as_array().unwrap().len(), 1);
        assert_eq!(body["mqTable"], serde_json::json!({}));
        assert_eq!(body["mqPopTable"], serde_json::json!({}));
        assert!(body.get("jstack").is_none());

        runtime.shutdown().await.unwrap();
    }

    #[test]
    fn pull_consume_hooks_receive_empty_pull_context_with_java_success_state() {
        struct Hook(Arc<Mutex<Vec<String>>>);

        impl ConsumeMessageHook for Hook {
            fn hook_name(&self) -> &str {
                "pull-consume-hook"
            }

            fn consume_message_before(&self, context: &mut ConsumeMessageContext) {
                assert!(context.messages.is_none());
                assert!(!context.success);
                assert!(context.properties.is_none());
                assert!(context.access_channel.is_none());
                assert_eq!(context.consumer_group, "tenant%pull-hook-lifecycle");
                assert_eq!(context.namespace.as_deref(), Some("tenant"));
                context.properties = Some(HashMap::new());
                context
                    .properties
                    .as_mut()
                    .unwrap()
                    .insert("shared".into(), "yes".into());
                self.0.lock().unwrap().push("before".into());
            }

            fn consume_message_after(&self, context: &mut ConsumeMessageContext) {
                assert!(context.success);
                assert_eq!(context.status.as_deref(), Some("CONSUME_SUCCESS"));
                assert_eq!(context.access_channel, Some(AccessChannel::Local));
                assert_eq!(
                    context
                        .properties
                        .as_ref()
                        .and_then(|properties| properties.get("shared"))
                        .map(String::as_str),
                    Some("yes")
                );
                self.0.lock().unwrap().push("after".into());
            }
        }

        let mut config = PullConsumerConfig::new("pull-hook-lifecycle").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        let consumer = PullConsumer::new(config);
        let events = Arc::new(Mutex::new(Vec::new()));
        consumer.register_consume_message_hook(Arc::new(Hook(events.clone())));
        let queue = MessageQueue::new("orders", "broker-a", 0);
        let mut processed = ProcessedPullResult {
            result: rocketmq_classic_core::api::consumer::PullResultExt {
                status: PullStatus::NoNewMessage,
                next_begin_offset: 0,
                min_offset: 0,
                max_offset: 0,
                suggest_which_broker_id: 0,
                offset_delta: None,
                raw_messages: None,
            },
            messages: None,
        };

        consumer
            .execute_consume_message_hooks(&queue, &mut processed)
            .unwrap();

        assert_eq!(events.lock().unwrap().as_slice(), ["before", "after"]);
        assert!(processed.messages.is_none());
    }

    #[derive(Default)]
    struct TestOffsetStore {
        values: Mutex<HashMap<MessageQueue, i64>>,
        persisted: Mutex<Vec<MessageQueue>>,
    }

    impl OffsetStore for TestOffsetStore {
        fn load(&self) -> crate::offset::OffsetStoreFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn update_offset(&self, queue: &MessageQueue, offset: i64, _increase_only: bool) {
            self.values.lock().unwrap().insert(queue.clone(), offset);
        }

        fn update_and_freeze_offset(&self, queue: &MessageQueue, offset: i64) {
            self.update_offset(queue, offset, false);
        }

        fn read_offset<'a>(
            &'a self,
            queue: &'a MessageQueue,
            _mode: ReadOffsetType,
        ) -> crate::offset::OffsetStoreFuture<'a, OffsetRead> {
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

        fn persist<'a>(
            &'a self,
            queue: &'a MessageQueue,
        ) -> crate::offset::OffsetStoreFuture<'a, ()> {
            Box::pin(async move {
                self.persisted.lock().unwrap().push(queue.clone());
                Ok(())
            })
        }

        fn persist_all<'a>(
            &'a self,
            queues: &'a [MessageQueue],
        ) -> crate::offset::OffsetStoreFuture<'a, ()> {
            Box::pin(async move {
                self.persisted
                    .lock()
                    .unwrap()
                    .extend(queues.iter().cloned());
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
        ) -> crate::offset::OffsetStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn string_tag_subscription_matches_java_filter_api_and_hashing() {
        let all = build_subscription("orders", None, Some("TAG")).unwrap();
        assert_eq!(all.get_sub_string(), Some("*"));
        assert_eq!(all.get_tags_set().unwrap().snapshot(), HashSet::new());

        let tags = build_subscription("orders", Some("red || blue || red"), Some("TAG")).unwrap();
        assert_eq!(tags.get_sub_string(), Some("red || blue || red"));
        assert_eq!(
            tags.get_tags_set().unwrap().snapshot(),
            HashSet::from([Some("red".to_owned()), Some("blue".to_owned())])
        );
        assert_eq!(
            java_string_hash("blue"),
            "blue".chars().fold(0i32, |hash, ch| {
                hash.wrapping_mul(31).wrapping_add(ch as i32)
            })
        );
    }

    #[test]
    fn selector_requires_sql_expression_but_tag_null_means_all() {
        assert!(matches!(
            build_subscription("orders", Some(""), Some("SQL92")),
            Err(PullConsumerError::EmptySqlExpression)
        ));
        let selector = MessageSelector::by_tag("");
        let all = build_subscription("orders", Some(&selector.expression), Some("TAG")).unwrap();
        assert_eq!(all.get_sub_string(), Some("*"));
    }

    #[test]
    fn expression_type_tag_detection_matches_java() {
        assert!(is_tag_type(None));
        assert!(is_tag_type(Some("")));
        assert!(is_tag_type(Some("TAG")));
        assert!(!is_tag_type(Some("SQL92")));
        assert!(!is_tag_type(Some("CUSTOM")));
    }

    #[test]
    fn namespace_is_applied_to_the_consumer_group_and_registered_topic() {
        let mut config = PullConsumerConfig::new("orders-group").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        let consumer = PullConsumer::new(config);

        assert_eq!(
            consumer.effective_consumer_group().unwrap(),
            "tenant%orders-group"
        );
        consumer.register_topic("orders").unwrap();
        assert!(read_lock(&consumer.subscriptions).contains_key("tenant%orders"));
        assert_eq!(
            consumer.register_topics(),
            BTreeSet::from(["tenant%orders".to_owned()])
        );
    }

    #[test]
    fn pull_rebalance_settings_default_to_java_values_and_registration_sets_listener() {
        let mut config = PullConsumerConfig::new("pull-rebalance-settings").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        assert!(config.enable_rebalance());
        assert_eq!(config.allocate_message_queue_strategy().name(), "AVG");
        assert!(config.message_queue_listener().is_none());

        let consumer = PullConsumer::new(config);
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        consumer
            .register_message_queue_listener("orders", Some(listener.clone()))
            .unwrap();

        assert_eq!(
            consumer.register_topics(),
            BTreeSet::from(["tenant%orders".to_owned()])
        );
        assert!(Arc::ptr_eq(
            consumer.message_queue_listener().as_ref().unwrap(),
            &listener
        ));
        let selector = MessageSelector::by_sql("color = 'blue'");
        consumer
            .add_register_subscriptions("events", Some(&selector))
            .unwrap();
        let registered_subscriptions = consumer.register_subscriptions();
        assert_eq!(registered_subscriptions.len(), 1);
        assert_eq!(
            registered_subscriptions[0].get_topic(),
            Some("tenant%events")
        );
        assert_eq!(
            registered_subscriptions[0].get_expression_type(),
            Some("SQL92")
        );
        assert!(consumer.unregister_topic("events").unwrap());
        consumer.clear_register_subscriptions();
        assert!(consumer.register_subscriptions().is_empty());
        consumer.set_enable_rebalance(false);
        assert!(!consumer.enable_rebalance());
    }

    #[tokio::test]
    async fn pull_runtime_rebalance_assigns_all_broadcast_queues_and_persists_removals() {
        let mut config = PullConsumerConfig::new("pull-rebalance-broadcast").unwrap();
        config.set_message_model(MessageModel::Broadcasting);
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let runtime = manager.get_or_create(config.client_config()).await.unwrap();
        let store = Arc::new(TestOffsetStore::default());
        let store_trait: Arc<dyn OffsetStore> = store.clone();
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        let listener: MessageQueueListener = Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        });
        let first = MessageQueue::new("orders", "broker-a", 0);
        let second = MessageQueue::new("orders", "broker-a", 1);
        store.update_offset(&first, 11, false);
        store.update_offset(&second, 22, false);
        let registration = Arc::new(PullConsumerRegistration {
            group: "pull-rebalance-broadcast".to_owned(),
            message_model: MessageModel::Broadcasting,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            unit_mode: false,
            route_timeout_millis: 100,
            enable_rebalance: Arc::new(AtomicBool::new(true)),
            rebalance_active: Arc::new(AtomicBool::new(true)),
            strategy: config.allocate_message_queue_strategy(),
            message_queue_listener: Arc::new(RwLock::new(Some(listener))),
            register_topics: Arc::new(RwLock::new(BTreeSet::from(["orders".to_owned()]))),
            register_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            additional_heartbeat_subscriptions: Arc::new(RwLock::new(
                ConsumerSubscriptionDataSet::new(),
            )),
            topic_queues: Arc::new(RwLock::new(HashMap::from([(
                "orders".to_owned(),
                BTreeSet::from([first.clone(), second.clone()]),
            )]))),
            in_balance_queues: Arc::new(RwLock::new(BTreeSet::new())),
            offset_store: Arc::downgrade(&store_trait),
            runtime: Arc::downgrade(&runtime),
            consumer: Weak::new(),
            rebalance_execution: tokio::sync::Mutex::new(()),
            rebalance_membership: Mutex::new(HashMap::new()),
        });
        write_lock(&registration.additional_heartbeat_subscriptions)
            .insert(Some(SharedValue::from(subscription_all("heartbeat-only"))));
        let heartbeat_topics = registration
            .heartbeat_data()
            .subscription_data_set
            .unwrap()
            .snapshot()
            .into_iter()
            .flatten()
            .map(|subscription| {
                subscription.with(|subscription| subscription.get_topic().unwrap().to_owned())
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            heartbeat_topics,
            BTreeSet::from(["orders".into(), "heartbeat-only".into()])
        );
        let owner: Arc<dyn ConsumerRuntime> = registration.clone();
        let lease = runtime
            .register_consumer("pull-rebalance-broadcast", owner)
            .await
            .unwrap();

        assert!(runtime.do_rebalance().await.unwrap());
        assert_eq!(
            read_lock(&registration.in_balance_queues).clone(),
            BTreeSet::from([first.clone(), second.clone()])
        );
        assert!(store.values.lock().unwrap().is_empty());
        {
            let changes_guard = changes.lock().unwrap();
            assert!(changes_guard.iter().any(|(topic, all, assigned)| {
                topic == "orders"
                    && all.as_ref().is_some_and(|queues| queues.len() == 2)
                    && assigned.len() == 2
            }));
        }

        assert!(runtime.do_rebalance().await.unwrap());
        assert_eq!(
            changes.lock().unwrap().len(),
            1,
            "unchanged assignments do not notify"
        );

        write_lock(&registration.topic_queues)
            .insert("orders".to_owned(), BTreeSet::from([second.clone()]));
        store.update_offset(&first, 33, false);
        registration
            .enable_rebalance
            .store(false, Ordering::Release);
        assert!(runtime.do_rebalance().await.unwrap());
        assert_eq!(
            read_lock(&registration.in_balance_queues).clone(),
            BTreeSet::from([first.clone(), second.clone()]),
            "disabled rebalance retains the existing assignment"
        );
        assert!(store.persisted.lock().unwrap().is_empty());
        registration.enable_rebalance.store(true, Ordering::Release);
        assert!(runtime.do_rebalance().await.unwrap());
        assert_eq!(
            read_lock(&registration.in_balance_queues).clone(),
            BTreeSet::from([second.clone()])
        );
        assert_eq!(*store.persisted.lock().unwrap(), vec![first.clone()]);
        assert!(!store.values.lock().unwrap().contains_key(&first));
        assert_eq!(changes.lock().unwrap().len(), 2);

        runtime
            .unregister_consumer(&lease, Instant::now() + Duration::from_millis(100))
            .await
            .unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pull_clustering_rebalance_uses_membership_and_keeps_assignment_when_unavailable() {
        let mut config = PullConsumerConfig::new("pull-rebalance-cluster").unwrap();
        config.client_config_mut().set_namesrv_addr(None);
        let manager = ClientRuntimeManager::new();
        let runtime = manager.get_or_create(config.client_config()).await.unwrap();
        let store = Arc::new(TestOffsetStore::default());
        let store_trait: Arc<dyn OffsetStore> = store.clone();
        let first = MessageQueue::new("orders", "broker-a", 0);
        let second = MessageQueue::new("orders", "broker-a", 1);
        let registration = Arc::new(PullConsumerRegistration {
            group: "pull-rebalance-cluster".to_owned(),
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            unit_mode: false,
            route_timeout_millis: 100,
            enable_rebalance: Arc::new(AtomicBool::new(true)),
            rebalance_active: Arc::new(AtomicBool::new(true)),
            strategy: config.allocate_message_queue_strategy(),
            message_queue_listener: Arc::new(RwLock::new(None)),
            register_topics: Arc::new(RwLock::new(BTreeSet::from(["orders".to_owned()]))),
            register_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            additional_heartbeat_subscriptions: Arc::new(RwLock::new(
                ConsumerSubscriptionDataSet::new(),
            )),
            topic_queues: Arc::new(RwLock::new(HashMap::from([(
                "orders".to_owned(),
                BTreeSet::from([first.clone(), second.clone()]),
            )]))),
            in_balance_queues: Arc::new(RwLock::new(BTreeSet::new())),
            offset_store: Arc::downgrade(&store_trait),
            runtime: Arc::downgrade(&runtime),
            consumer: Weak::new(),
            rebalance_execution: tokio::sync::Mutex::new(()),
            rebalance_membership: Mutex::new(HashMap::from([(
                "orders".to_owned(),
                VecDeque::from([Some(vec![runtime.client_id().to_owned()]), Some(Vec::new())]),
            )])),
        });
        let owner: Arc<dyn ConsumerRuntime> = registration.clone();
        let lease = runtime
            .register_consumer("pull-rebalance-cluster", owner)
            .await
            .unwrap();

        assert!(runtime.do_rebalance().await.unwrap());
        let assigned = read_lock(&registration.in_balance_queues).clone();
        assert_eq!(assigned, BTreeSet::from([first, second]));

        write_lock(&registration.topic_queues).insert("orders".to_owned(), BTreeSet::new());
        assert!(!runtime.do_rebalance().await.unwrap());
        assert_eq!(read_lock(&registration.in_balance_queues).clone(), assigned);
        assert_eq!(
            registration
                .rebalance_membership
                .lock()
                .unwrap()
                .get("orders")
                .unwrap()
                .len(),
            1,
            "empty route data is handled before membership lookup"
        );

        write_lock(&registration.topic_queues).insert(
            "orders".to_owned(),
            BTreeSet::from([
                MessageQueue::new("orders", "broker-a", 0),
                MessageQueue::new("orders", "broker-a", 1),
            ]),
        );
        assert!(!runtime.do_rebalance().await.unwrap());
        assert_eq!(read_lock(&registration.in_balance_queues).clone(), assigned);

        runtime
            .unregister_consumer(&lease, Instant::now() + Duration::from_millis(100))
            .await
            .unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[test]
    fn pull_input_validation_rejects_invalid_queue_offset_count_and_timeout() {
        let queue = MessageQueue::new("orders", "broker-a", 0);
        assert!(validate_pull_request(&queue, 0, 1, 1, 0).is_ok());
        assert!(matches!(
            validate_pull_request(&queue, -1, 1, 1, 0),
            Err(PullConsumerError::InvalidOffset)
        ));
        assert!(matches!(
            validate_pull_request(&queue, 0, 0, 1, 0),
            Err(PullConsumerError::InvalidMaxMessageCount)
        ));
        assert!(matches!(
            validate_pull_request(&queue, 0, 1, 0, 0),
            Err(PullConsumerError::InvalidMaxMessageBytes)
        ));
        assert!(matches!(
            validate_pull_request(&queue, 0, 1, -1, 0),
            Err(PullConsumerError::InvalidMaxMessageBytes)
        ));
        assert!(matches!(
            validate_pull_request(&queue, 0, 1, 1, -1),
            Err(PullConsumerError::InvalidTimeout)
        ));
        let invalid_queue = MessageQueue::new("", "broker-a", 0);
        assert!(matches!(
            validate_pull_request(&invalid_queue, 0, 1, 1, 0),
            Err(PullConsumerError::InvalidQueue)
        ));
    }

    #[tokio::test]
    async fn startup_registration_and_shutdown_obey_consumer_lifecycle() {
        let mut config = PullConsumerConfig::new("pull-lifecycle").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".to_owned()));
        config.client_config_mut().set_namesrv_addr(None);
        config.set_message_model(MessageModel::Broadcasting);
        let changes = Arc::new(Mutex::new(Vec::new()));
        let recorded_changes = changes.clone();
        config.set_message_queue_listener(Some(Arc::new(move |topic, all, assigned| {
            recorded_changes.lock().unwrap().push((
                topic.to_owned(),
                all.cloned(),
                assigned.clone(),
            ));
        })));
        let manager = ClientRuntimeManager::new();
        let consumer = Arc::new(PullConsumer::with_manager(config, manager));
        let store = Arc::new(TestOffsetStore::default());

        assert!(consumer.get_offset_store().is_none());
        assert!(matches!(
            consumer.fetch_message_queues_in_balance("orders").await,
            Err(PullConsumerError::NotStarted)
        ));
        consumer
            .set_offset_store(Some(store.clone()))
            .await
            .unwrap();
        consumer.register_topic("orders").unwrap();
        consumer.start().await.unwrap();
        let runtime = consumer.started_runtime().await.unwrap().0;
        write_lock(&consumer.topic_queues).insert(
            "tenant%orders".to_owned(),
            BTreeSet::from([
                MessageQueue::new("tenant%orders", "broker-a", 1),
                MessageQueue::new("tenant%orders", "broker-a", 2),
            ]),
        );
        changes.lock().unwrap().clear();
        assert!(runtime.do_rebalance().await.unwrap());
        assert_eq!(
            consumer
                .fetch_message_queues_in_balance("orders")
                .await
                .unwrap(),
            BTreeSet::from([
                MessageQueue::new("orders", "broker-a", 1),
                MessageQueue::new("orders", "broker-a", 2),
            ])
        );
        {
            let changes = changes.lock().unwrap();
            assert!(changes.iter().any(|(topic, all, assigned)| {
                topic == "tenant%orders"
                    && all.as_ref().is_some_and(|queues| queues.len() == 2)
                    && assigned.len() == 2
            }));
        }
        assert!(consumer.get_offset_store().is_some());
        assert!(consumer.is_running().await);
        assert!(matches!(
            consumer.start().await,
            Err(PullConsumerError::AlreadyStarted)
        ));
        assert!(matches!(
            consumer.set_offset_store(None).await,
            Err(PullConsumerError::OffsetStoreAfterStartup)
        ));

        consumer.shutdown().await.unwrap();
        assert!(consumer.get_offset_store().is_some());
        assert!(!consumer.is_running().await);
        assert!(matches!(
            consumer
                .pull(&MessageQueue::new("orders", "broker-a", 0), None, 0, 1)
                .await,
            Err(PullConsumerError::Shutdown)
        ));
        assert!(matches!(
            consumer.fetch_message_queues_in_balance("orders").await,
            Err(PullConsumerError::Shutdown)
        ));
    }
}
