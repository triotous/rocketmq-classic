use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock, Weak},
    time::Duration,
};

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::runtime::{
    ClientRuntime, ClientRuntimeManager, ConsumerRuntime, GroupLease,
};
use rocketmq_classic_protocol::{
    SharedValue,
    heartbeat::{ConsumeType, ConsumerData, ConsumerSubscriptionDataSet, SubscriptionData},
};
use tokio::time::Instant;

#[cfg(test)]
use std::sync::Mutex;

use crate::{
    consume::ConcurrentConsumeService,
    error::PushConsumerError,
    offset::{LocalFileOffsetStore, OffsetStore, RemoteBrokerOffsetStore},
    pull::build_subscription,
};

use super::{
    config::PushConsumerConfig, listener::PushMessageListener, pull_service::PullScheduler,
};

type ConsumerFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

enum State {
    Created,
    Started {
        runtime: Weak<ClientRuntime>,
        lease: GroupLease,
        offset_store: Arc<dyn OffsetStore>,
        consume_service: Arc<ConcurrentConsumeService>,
        pull_scheduler: Arc<PullScheduler>,
    },
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupStage {
    RuntimeCreated,
    OffsetStoreLoaded,
    ConsumeServiceStarted,
    PullSchedulerStarted,
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
            Self::ConsumerRegistered => "consumer-registered",
            Self::RuntimeStarted => "runtime-started",
            Self::RoutesUpdated => "routes-updated",
            Self::BrokerChecked => "broker-checked",
            Self::HeartbeatSent => "heartbeat-sent",
        }
    }

    #[cfg(test)]
    const fn all() -> [Self; 9] {
        [
            Self::RuntimeCreated,
            Self::OffsetStoreLoaded,
            Self::ConsumeServiceStarted,
            Self::PullSchedulerStarted,
            Self::ConsumerRegistered,
            Self::RuntimeStarted,
            Self::RoutesUpdated,
            Self::BrokerChecked,
            Self::HeartbeatSent,
        ]
    }
}

/// Java-compatible Push consumer startup and shared-runtime registration.
/// Queue allocation and message pulling remain partial until later pipeline
/// stages are attached.
pub struct PushConsumer {
    config: PushConsumerConfig,
    manager: ClientRuntimeManager,
    state: tokio::sync::Mutex<State>,
    offset_store: std::sync::Mutex<Option<Arc<dyn OffsetStore>>>,
    listener: RwLock<Option<PushMessageListener>>,
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionData>>>,
    topic_queues: Arc<RwLock<HashMap<String, BTreeSet<MessageQueue>>>>,
    #[cfg(test)]
    startup_failure: Mutex<Option<StartupStage>>,
}

impl PushConsumer {
    pub fn new(config: PushConsumerConfig) -> Self {
        Self::with_manager(config, ClientRuntimeManager::shared_default())
    }

    fn with_manager(config: PushConsumerConfig, manager: ClientRuntimeManager) -> Self {
        Self {
            config,
            manager,
            state: tokio::sync::Mutex::new(State::Created),
            offset_store: std::sync::Mutex::new(None),
            listener: RwLock::new(None),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            topic_queues: Arc::new(RwLock::new(HashMap::new())),
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
        let startup = self
            .start_registered(
                &runtime,
                &consumer_group,
                offset_store.clone(),
                consume_service.clone(),
                pull_scheduler.clone(),
            )
            .await;
        match startup {
            Ok(lease) => {
                *state = State::Started {
                    runtime: Arc::downgrade(&runtime),
                    lease,
                    offset_store,
                    consume_service,
                    pull_scheduler,
                };
                Ok(())
            }
            Err((error, lease)) => {
                pull_scheduler.stop().await;
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
        consume_service: Arc<ConcurrentConsumeService>,
        pull_scheduler: Arc<PullScheduler>,
    ) -> Result<GroupLease, (PushConsumerError, Option<GroupLease>)> {
        if let Err(error) = offset_store.load().await {
            return Err((error.into(), None));
        }
        if let Err(error) = self.checkpoint(StartupStage::OffsetStoreLoaded) {
            return Err((error, None));
        }
        consume_service.start().await;
        if let Err(error) = self.checkpoint(StartupStage::ConsumeServiceStarted) {
            return Err((error, None));
        }
        pull_scheduler
            .start(Arc::downgrade(runtime))
            .map_err(|error| (PushConsumerError::PullScheduler(error.to_string()), None))?;
        if let Err(error) = self.checkpoint(StartupStage::PullSchedulerStarted) {
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
        let mut state = self.state.lock().await;
        let (runtime, lease, offset_store, consume_service, pull_scheduler) = match &*state {
            State::Created | State::Shutdown => return Ok(()),
            State::Started {
                runtime,
                lease,
                offset_store,
                consume_service,
                pull_scheduler,
            } => (
                runtime.upgrade(),
                lease.clone(),
                offset_store.clone(),
                consume_service.clone(),
                pull_scheduler.clone(),
            ),
        };
        pull_scheduler.stop().await;
        consume_service
            .shutdown(self.config.await_termination_millis_when_shutdown())
            .await;
        let Some(runtime) = runtime else {
            *state = State::Shutdown;
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
        *state = State::Shutdown;
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
        _request: Arc<dyn rocketmq_classic_core::runtime::MessageRequest>,
    ) -> Option<ConsumerFuture<'_>> {
        None
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
        let (consume_service, pull_scheduler) = match &*consumer.state.lock().await {
            State::Started {
                consume_service,
                pull_scheduler,
                ..
            } => (consume_service.clone(), pull_scheduler.clone()),
            _ => panic!("consumer should own a started consume service"),
        };
        assert_eq!(consume_service.worker_count(), 20);
        assert_eq!(pull_scheduler.worker_count(), 1);
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
