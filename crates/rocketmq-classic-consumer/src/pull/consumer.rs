use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex, RwLock, Weak},
    time::Duration,
};

use rocketmq_classic_common::{
    message::{DecodeLimits, DecodeOptions, MessageQueue},
    sysflag::pull::{build_pull_sys_flag, clear_commit_offset_flag},
};
use rocketmq_classic_core::{
    route::{BrokerAddress, RouteSnapshot},
    runtime::{ClientRuntime, ClientRuntimeManager, ConsumerRuntime, GroupLease},
};
use rocketmq_classic_protocol::{
    SharedValue,
    header::{
        BoundaryType, GetEarliestMsgStoretimeRequestHeader, GetMaxOffsetRequestHeader,
        GetMinOffsetRequestHeader, PullMessageRequestHeader, SearchOffsetRequestHeader,
    },
    heartbeat::{
        ConsumeFromWhere, ConsumeType, ConsumerData, ConsumerSubscriptionDataSet, MessageModel,
        SubscriptionCodeSet, SubscriptionData, SubscriptionTagSet,
    },
};
use tokio::time::Instant;

use crate::{
    config::PullConsumerConfig,
    error::PullConsumerError,
    offset::{LocalFileOffsetStore, OffsetStore, ReadOffsetType, RemoteBrokerOffsetStore},
    pull::{FilterMessageHook, ProcessedPullResult, PullAPIWrapper, resolve_pull_broker_address},
};

const MAX_PULL_MESSAGE_BYTES: i32 = i32::MAX;
const FIRST_BROKER_SUPPORTING_EXPRESSION_FILTER: i32 = 232;

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
    state: tokio::sync::Mutex<State>,
    pull_api: PullAPIWrapper,
    offset_store: Mutex<Option<Arc<dyn OffsetStore>>>,
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    topic_queues: Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>>,
}

impl PullConsumer {
    pub fn new(config: PullConsumerConfig) -> Self {
        Self::with_manager(config, ClientRuntimeManager::shared_default())
    }

    fn with_manager(config: PullConsumerConfig, manager: ClientRuntimeManager) -> Self {
        let pull_api = PullAPIWrapper::new(config.unit_mode());
        Self {
            config,
            manager,
            state: tokio::sync::Mutex::new(State::Created),
            pull_api,
            offset_store: Mutex::new(None),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            topic_queues: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn config(&self) -> &PullConsumerConfig {
        &self.config
    }

    pub fn register_filter_message_hook(&self, hook: Arc<dyn FilterMessageHook>) {
        self.pull_api.register_filter_message_hook(hook);
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
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(topic))?
            .ok_or(PullConsumerError::InvalidQueue)?;
        write_lock(&self.subscriptions)
            .entry(topic.clone())
            .or_insert_with(|| subscription_all(&topic));
        Ok(())
    }

    pub fn unregister_topic(&self, topic: &str) -> Result<bool, PullConsumerError> {
        let mut client_config = self.config.client_config().clone();
        let topic = client_config
            .with_namespace(Some(topic))?
            .ok_or(PullConsumerError::InvalidQueue)?;
        Ok(write_lock(&self.subscriptions).remove(&topic).is_some())
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

        let runtime = self.manager.get_or_create(&client_config).await?;
        let offset_store = self.select_offset_store(&runtime, &consumer_group)?;
        offset_store.load().await?;

        let owner: Arc<dyn ConsumerRuntime> = Arc::new(PullConsumerRegistration {
            group: consumer_group.clone(),
            message_model: self.config.message_model(),
            unit_mode: self.config.unit_mode(),
            subscriptions: self.subscriptions.clone(),
            topic_queues: self.topic_queues.clone(),
            offset_store: Arc::downgrade(&offset_store),
        });
        let lease = runtime.register_consumer(&consumer_group, owner).await?;
        if let Err(error) = runtime.start().await {
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
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), PullConsumerError> {
        let mut state = self.state.lock().await;
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
        Ok(())
    }

    pub async fn is_running(&self) -> bool {
        matches!(&*self.state.lock().await, State::Started { .. })
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
        let mut sys_flag =
            build_pull_sys_flag(false, suspend, true, subscription.is_class_filter_mode());
        let pull_timeout_millis = if suspend {
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
        Ok(processed)
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
    unit_mode: bool,
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    topic_queues: Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>>,
    offset_store: Weak<dyn OffsetStore>,
}

impl ConsumerRuntime for PullConsumerRegistration {
    fn heartbeat_data(&self) -> ConsumerData {
        // Java sends mutable SubscriptionData values in a HashSet; preserve
        // that identity/equality behavior for the heartbeat wire model.
        #[allow(clippy::mutable_key_type)]
        let subscription_data_set = read_lock(&self.subscriptions)
            .values()
            .cloned()
            .map(|subscription| Some(SharedValue::from(subscription)))
            .collect::<ConsumerSubscriptionDataSet>();
        ConsumerData {
            group_name: Some(self.group.clone()),
            consume_type: Some(ConsumeType::ConsumeActively),
            message_model: Some(self.message_model),
            consume_from_where: Some(ConsumeFromWhere::ConsumeFromLastOffset),
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
}

fn subscription_all(topic: &str) -> SubscriptionData {
    let mut subscription = SubscriptionData::new(Some(topic.to_owned()), Some("*".to_owned()));
    subscription.set_sub_version(epoch_millis());
    subscription
}

fn build_subscription(
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
    use std::collections::HashSet;

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
        config.client_config_mut().set_namesrv_addr(None);
        let consumer = Arc::new(PullConsumer::with_manager(
            config,
            ClientRuntimeManager::new(),
        ));

        assert!(consumer.get_offset_store().is_none());
        consumer.set_offset_store(None).await.unwrap();
        consumer.register_topic("orders").unwrap();
        consumer.start().await.unwrap();
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
    }
}
