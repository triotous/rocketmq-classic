use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use rocketmq_classic_remoting::client::{
    ClientConfig as TransportConfig, RemotingClient, RpcHook, TransportMode,
};

use super::{
    ConsumerRuntime, GroupLease, ProducerRuntime, RuntimeError, RuntimeState,
    control::{Command, ControlPlane},
    manager::ManagerLink,
    registry::{GroupKind, GroupRegistry},
};
use crate::{api::MQClientApi, config::ClientConfig};

fn epoch_millis(time: std::time::SystemTime) -> i64 {
    match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        // Java's epoch millisecond clock floors a fractional negative instant.
        Err(error) => (-(error.duration().as_nanos().div_ceil(1_000_000) as i128)) as i64,
    }
}

#[cfg(test)]
#[test]
fn constructor_clock_uses_signed_epoch_milliseconds() {
    use std::time::{Duration, UNIX_EPOCH};
    assert_eq!(epoch_millis(UNIX_EPOCH), 0);
    assert_eq!(epoch_millis(UNIX_EPOCH + Duration::from_millis(1234)), 1234);
    assert_eq!(
        epoch_millis(UNIX_EPOCH - Duration::from_millis(1234)),
        -1234
    );
    assert_eq!(epoch_millis(UNIX_EPOCH - Duration::from_nanos(1)), -1);
}

/// A manager-owned transport and typed API. Construction does not start tasks.
pub struct ClientRuntime {
    boot_timestamp: i64,
    config: ClientConfig,
    client_id: String,
    remoting: RemotingClient,
    api: MQClientApi,
    routes: crate::route::RouteCache,
    namesrv_gate: tokio::sync::Mutex<()>,
    pub(super) groups: GroupRegistry,
    pub(super) control: ControlPlane,
    pub(super) inbound: Arc<super::inbound::RuntimeProcessor>,
    pub(super) rebalance: Arc<super::rebalance::RebalanceSignal>,
    pub(super) pull: Arc<super::pull::PullService>,
    manager: OnceLock<ManagerLink>,
    pub(crate) heartbeat: crate::heartbeat::HeartbeatState,
    heartbeat_listener: Option<Arc<crate::heartbeat::HeartbeatChannelListener>>,
    addressing: Arc<dyn crate::namesrv::TopAddressing>,
    discovery: Arc<crate::namesrv::DiscoveryState>,
    #[cfg(test)]
    pub(super) periodic_probe: Arc<super::tasks::TaskProbe>,
    #[cfg(test)]
    pub(super) rebalance_probe: Arc<super::tasks::TaskProbe>,
    #[cfg(test)]
    pub(super) pull_probe: Arc<super::tasks::TaskProbe>,
}

impl ClientRuntime {
    #[cfg(test)]
    pub(super) fn construct(
        config: ClientConfig,
        properties: &HashMap<String, String>,
        hook: Option<Arc<dyn RpcHook>>,
    ) -> Result<Self, RuntimeError> {
        Self::construct_with_top_addressing(config, properties, hook, None)
    }

    pub(super) fn construct_with_top_addressing(
        config: ClientConfig,
        properties: &HashMap<String, String>,
        hook: Option<Arc<dyn RpcHook>>,
        provider: Option<Arc<dyn crate::namesrv::TopAddressing>>,
    ) -> Result<Self, RuntimeError> {
        let boot_timestamp = epoch_millis(std::time::SystemTime::now());
        let rebalance = Arc::new(super::rebalance::RebalanceSignal::from_properties(
            properties,
        )?);
        if config.enable_concurrent_heartbeat && config.concurrent_heartbeat_thread_pool_size <= 0 {
            return Err(
                rocketmq_classic_remoting::TransportError::InvalidConfiguration(
                    "concurrent heartbeat thread count must be positive".into(),
                )
                .into(),
            );
        }
        let callback_executor_threads = usize::try_from(config.client_callback_executor_threads)
            .map_err(|_| {
                rocketmq_classic_remoting::TransportError::InvalidConfiguration(
                    "client callback executor thread count must be nonnegative".into(),
                )
            })?;
        let transport = if config.use_tls {
            let mut properties = properties.clone();
            properties.insert("tls.enable".into(), "true".into());
            TransportMode::from_java_properties(&properties, None)?
        } else {
            TransportMode::Plain
        };
        let heartbeat_listener = config
            .enable_heartbeat_channel_event_listener
            .then(|| Arc::new(crate::heartbeat::HeartbeatChannelListener::default()));
        let remoting = RemotingClient::with_config_and_channel_event_listener(
            TransportConfig {
                transport,
                callback_executor_threads,
                socks_proxy_config: config.socks_proxy_config.clone(),
                scan_available_nameserver: false,
                ..TransportConfig::default()
            },
            heartbeat_listener.clone().map(|listener| {
                listener as Arc<dyn rocketmq_classic_remoting::client::ChannelEventListener>
            }),
        )?;
        if let Some(addresses) = config.get_namesrv_addr() {
            remoting.update_name_server_address_list(crate::namesrv::split_addresses(&addresses));
        }
        super::hooks::register(&remoting, &config, hook);
        let mut addressing = crate::namesrv::DefaultTopAddressing::new(
            crate::namesrv::ws_addr(properties),
            config.unit_name.clone(),
        );
        if let Some(provider) = provider {
            addressing = addressing.with_provider(provider);
        }
        let addressing: Arc<dyn crate::namesrv::TopAddressing> = Arc::new(addressing);
        let discovery = crate::namesrv::DiscoveryState::new(remoting.clone());
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            addressing.register_change_callback(discovery.callback());
        }))
        .map_err(|_| RuntimeError::AddressingCallbackFailed)?;
        let language = config.language;
        Ok(Self {
            boot_timestamp,
            client_id: config.build_client_id(),
            config,
            api: MQClientApi::with_language(remoting.clone(), language),
            remoting,
            routes: crate::route::RouteCache::default(),
            namesrv_gate: tokio::sync::Mutex::new(()),
            groups: GroupRegistry::default(),
            control: ControlPlane::default(),
            inbound: Arc::default(),
            rebalance,
            pull: Arc::default(),
            manager: OnceLock::new(),
            heartbeat: crate::heartbeat::HeartbeatState::default(),
            heartbeat_listener,
            addressing,
            discovery,
            #[cfg(test)]
            periodic_probe: Arc::default(),
            #[cfg(test)]
            rebalance_probe: Arc::default(),
            #[cfg(test)]
            pull_probe: Arc::default(),
        })
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Constructor wall-clock milliseconds, corresponding to getBootTimestamp.
    /// It is independent of Tokio's monotonic timer clock and never resets.
    pub fn boot_timestamp(&self) -> i64 {
        self.boot_timestamp
    }

    /// Consumes source keys and rebuilds them using the explicit namespace,
    /// representing Java's mutable queue-key operation with Rust value ownership.
    pub fn parse_offset_table_from_broker(
        &self,
        offset_table: std::collections::BTreeMap<
            rocketmq_classic_common::message::MessageQueue,
            i64,
        >,
        namespace: Option<&str>,
    ) -> std::collections::BTreeMap<rocketmq_classic_common::message::MessageQueue, i64> {
        let Some(namespace) = namespace.filter(|value| !value.is_empty()) else {
            return offset_table;
        };
        offset_table
            .into_iter()
            .map(|(queue, offset)| {
                let topic = crate::config::namespace::unwrap(Some(namespace), Some(queue.topic()))
                    .expect("typed queue topic cannot be null");
                (
                    rocketmq_classic_common::message::MessageQueue::new(
                        topic,
                        queue.broker_name(),
                        queue.queue_id(),
                    ),
                    offset,
                )
            })
            .collect()
    }

    pub fn execute_pull_request_immediately(
        &self,
        request: Arc<dyn super::MessageRequest>,
    ) -> Result<(), RuntimeError> {
        self.pull.enqueue(request)
    }
    pub fn execute_pop_pull_request_immediately(
        &self,
        request: Arc<dyn super::MessageRequest>,
    ) -> Result<(), RuntimeError> {
        self.pull.enqueue(request)
    }
    pub fn execute_pull_request_later(
        self: &Arc<Self>,
        request: Arc<dyn super::MessageRequest>,
        delay: i64,
    ) -> Result<(), RuntimeError> {
        self.ensure_pull_scheduler()?;
        self.pull.request_later(request, delay)
    }
    pub fn execute_pop_pull_request_later(
        self: &Arc<Self>,
        request: Arc<dyn super::MessageRequest>,
        delay: i64,
    ) -> Result<(), RuntimeError> {
        self.ensure_pull_scheduler()?;
        self.pull.request_later(request, delay)
    }
    pub fn execute_task(
        self: &Arc<Self>,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), RuntimeError> {
        self.ensure_pull_scheduler()?;
        self.pull.task_later(Box::pin(task), 0)
    }
    pub fn execute_task_later(
        self: &Arc<Self>,
        task: impl std::future::Future<Output = ()> + Send + 'static,
        delay: i64,
    ) -> Result<(), RuntimeError> {
        self.ensure_pull_scheduler()?;
        self.pull.task_later(Box::pin(task), delay)
    }

    fn ensure_pull_scheduler(self: &Arc<Self>) -> Result<(), RuntimeError> {
        let state = self.state();
        if !matches!(state, RuntimeState::Created | RuntimeState::Running) {
            return Err(RuntimeError::InvalidState(state));
        }
        self.control
            .sender(self)?
            .send(Command::StartPullScheduler)
            .map_err(|_| RuntimeError::ControlTaskFailed)
    }

    pub fn rebalance_immediately(&self) {
        self.rebalance.wakeup();
    }

    pub fn rebalance_later(&self, delay_millis: i64) -> Result<(), RuntimeError> {
        if delay_millis <= 0 {
            self.rebalance_immediately();
            return Ok(());
        }
        let state = self.state();
        if matches!(
            state,
            RuntimeState::ShuttingDown | RuntimeState::Stopped | RuntimeState::Failed
        ) {
            return Err(RuntimeError::InvalidState(state));
        }
        self.rebalance
            .schedule(tokio::time::Duration::from_millis(delay_millis as u64))
    }

    pub async fn do_rebalance(&self) -> Result<bool, RuntimeError> {
        use std::task::Poll;
        let mut balanced = true;
        for consumer in self.groups.snapshot()?.consumers {
            // Java contains each owner's Throwable independently. Catch both
            // capability construction and asynchronous polling outside locks.
            let operation =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| consumer.try_rebalance()));
            if let Ok(Some(mut operation)) = operation {
                let result = std::future::poll_fn(|context| {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        operation.as_mut().poll(context)
                    }))
                    .unwrap_or(Poll::Ready(true))
                })
                .await;
                balanced &= result;
            }
        }
        Ok(balanced)
    }

    /// Java `adjustThreadPool`: only owners exposing the push adjustment
    /// capability participate; one owner's failure does not end the round.
    pub async fn adjust_thread_pool(&self) -> Result<(), RuntimeError> {
        use std::task::Poll;
        for consumer in self.groups.snapshot()?.consumers {
            let operation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                consumer.adjust_thread_pool()
            }));
            if let Ok(Some(mut operation)) = operation {
                std::future::poll_fn(|context| {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        operation.as_mut().poll(context)
                    }))
                    .unwrap_or(Poll::Ready(()))
                })
                .await;
            }
        }
        Ok(())
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub fn api(&self) -> &MQClientApi {
        &self.api
    }

    pub fn remoting(&self) -> &RemotingClient {
        &self.remoting
    }

    pub async fn fetch_name_server_addr(&self) -> Option<String> {
        use std::{future::Future, task::Poll};
        let mut fetch = Box::pin(async { self.addressing.fetch_ns_addr().await });
        let addresses = std::future::poll_fn(|context| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                fetch.as_mut().poll(context)
            }))
            .unwrap_or(Poll::Ready(None))
        })
        .await;
        self.discovery.fetched(addresses)
    }

    pub fn name_server_addr(&self) -> Option<String> {
        self.discovery.current()
    }

    pub fn route_snapshot(&self) -> Result<Arc<crate::route::RouteSnapshot>, RuntimeError> {
        self.routes.snapshot()
    }

    pub fn find_broker_address_by_topic(
        &self,
        topic: &str,
    ) -> Result<Option<String>, RuntimeError> {
        Ok(self
            .route_snapshot()?
            .broker_address_by_topic(topic)?
            .map(str::to_owned))
    }

    pub fn get_existing_topic_route(
        &self,
        topic: &str,
    ) -> Result<Option<Arc<rocketmq_classic_protocol::route::TopicRouteData>>, RuntimeError> {
        Ok(self
            .route_snapshot()?
            .topics
            .get(topic)
            .map(|cached| cached.route.clone()))
    }

    pub async fn query_topic_route(
        &self,
        topic: &str,
    ) -> Result<Option<Arc<rocketmq_classic_protocol::route::TopicRouteData>>, RuntimeError> {
        if let Some(route) = self.get_existing_topic_route(topic)? {
            return Ok(Some(route));
        }
        self.update_topic_route(topic, tokio::time::Instant::now() + self.rpc_timeout())
            .await?;
        self.get_existing_topic_route(topic)
    }

    async fn lookup_broker_with_route_refresh(
        &self,
        topic: &str,
    ) -> Result<Option<String>, RuntimeError> {
        if let Some(address) = self.find_broker_address_by_topic(topic)? {
            return Ok(Some(address));
        }
        self.update_topic_route(topic, tokio::time::Instant::now() + self.rpc_timeout())
            .await?;
        self.find_broker_address_by_topic(topic)
    }

    /// Runtime lookup follows Java's best-effort consumer-list RPC: route
    /// lookup/refresh errors remain errors, but Broker RPC errors return None.
    pub async fn find_consumer_id_list(
        &self,
        topic: &str,
        group: &str,
    ) -> Result<Option<Vec<String>>, RuntimeError> {
        let Some(address) = self.lookup_broker_with_route_refresh(topic).await? else {
            return Ok(None);
        };
        let Ok(address) =
            crate::route::broker_vip_channel(self.config.vip_channel_enabled, &address)
        else {
            return Ok(None);
        };
        Ok(self
            .api
            .get_consumer_list_by_group(
                &address,
                group,
                tokio::time::Instant::now() + self.rpc_timeout(),
            )
            .await
            .unwrap_or(None))
    }

    /// Assignment keeps Java's explicit signed millisecond timeout and
    /// propagates Broker/transport errors, unlike consumer-list lookup.
    pub async fn query_assignment(
        &self,
        topic: &str,
        consumer_group: &str,
        strategy_name: &str,
        message_model: rocketmq_classic_protocol::heartbeat::MessageModel,
        timeout_millis: i32,
    ) -> Result<
        Option<std::collections::HashSet<rocketmq_classic_protocol::body::MessageQueueAssignment>>,
        RuntimeError,
    > {
        let Some(address) = self.lookup_broker_with_route_refresh(topic).await? else {
            return Ok(None);
        };
        let address = crate::route::broker_vip_channel(self.config.vip_channel_enabled, &address)?;
        let body = rocketmq_classic_protocol::body::QueryAssignmentRequestBody {
            topic: Some(topic.into()),
            consumer_group: Some(consumer_group.into()),
            client_id: Some(self.client_id.clone()),
            strategy_name: Some(strategy_name.into()),
            message_model: Some(message_model),
        };
        let deadline = tokio::time::Instant::now()
            + tokio::time::Duration::from_millis(timeout_millis.max(0) as u64);
        Ok(self
            .api
            .query_assignment(&address, &body, deadline)
            .await?
            .message_queue_assignments
            .map(|assignments| assignments.into_iter().collect()))
    }

    pub async fn update_topic_route(
        &self,
        topic: &str,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        self.update_route(topic, None, deadline).await
    }

    /// Refresh the union of current consumer subscriptions and producer
    /// publish topics. Each topic gets Java's fresh configured RPC budget;
    /// an individual refresh failure does not prevent the remaining topics.
    pub async fn update_all_topic_routes(&self) -> Result<(), RuntimeError> {
        let groups = self.groups.snapshot()?;
        let mut topics = std::collections::BTreeSet::new();
        for consumer in groups.consumers {
            topics.extend(
                consumer
                    .heartbeat_data()
                    .subscription_data_set
                    .into_iter()
                    .map(|sub| sub.topic),
            );
        }
        for (_, producer) in groups.producers {
            topics.extend(producer.publish_topics());
        }
        for topic in topics {
            let _ = self
                .update_topic_route(&topic, tokio::time::Instant::now() + self.rpc_timeout())
                .await;
        }
        Ok(())
    }

    /// Runtime branch for producer default-route fallback. The queue setting
    /// stays producer-owned rather than becoming runtime configuration.
    pub async fn update_topic_route_from_default(
        &self,
        topic: &str,
        default_topic_queue_nums: i32,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        self.update_route(topic, Some(default_topic_queue_nums), deadline)
            .await
    }

    async fn update_route(
        &self,
        topic: &str,
        default_topic_queue_nums: Option<i32>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        let Ok(_gate) = tokio::time::timeout(
            tokio::time::Duration::from_millis(3000),
            self.namesrv_gate.lock(),
        )
        .await
        else {
            return Ok(false);
        };
        let fetched = if default_topic_queue_nums.is_some() {
            self.api.get_default_topic_route(deadline).await
        } else {
            self.api.get_topic_route(topic, deadline).await
        };
        let mut route = match fetched {
            Ok(route) => route,
            // Java catches MQClientException and returns false, but propagates
            // transport failure. Rejection must not discard an existing route.
            Err(error) if MQClientApi::route_error_is_client_exception(&error) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if let Some(limit) = default_topic_queue_nums {
            for data in &mut route.queue_datas {
                let count = limit.min(data.read_queue_nums);
                data.read_queue_nums = count;
                data.write_queue_nums = count;
            }
        }
        let groups = self.groups.snapshot()?;
        let changed = self.routes.route_changed(topic, &route)?;
        let needed = !changed
            && (groups
                .producers
                .iter()
                .any(|(_, producer)| producer.is_publish_topic_need_update(topic))
                || groups
                    .consumers
                    .iter()
                    .any(|consumer| consumer.is_subscribe_topic_need_update(topic)));
        let changed = if needed {
            self.routes.replace_for_owner_update(topic, route, true)?
        } else {
            self.routes.replace(topic, route)?
        };
        if changed || needed {
            if let Some(info) = self.routes.snapshot()?.topics.get(topic).cloned() {
                for (_, producer) in groups.producers {
                    producer.update_topic_publish_info(topic, info.clone());
                }
                for consumer in groups.consumers {
                    consumer.update_topic_subscribe_info(topic, info.subscribe_queues.clone());
                }
            }
        }
        Ok(changed || needed)
    }

    async fn clean_offline_brokers(&self) -> Result<(), RuntimeError> {
        let Ok(_gate) = tokio::time::timeout(
            tokio::time::Duration::from_millis(3000),
            self.namesrv_gate.lock(),
        )
        .await
        else {
            return Ok(());
        };
        self.routes.clean_offline()
    }

    pub(crate) fn record_broker_version(
        &self,
        broker: &str,
        address: &str,
        version: i32,
    ) -> Result<(), RuntimeError> {
        self.routes.record_version(broker, address, version)
    }

    pub(super) async fn run_periodic(
        &self,
        kind: super::tasks::TaskKind,
    ) -> Result<(), RuntimeError> {
        use super::tasks::TaskKind;
        match kind {
            TaskKind::Discovery => {
                self.fetch_name_server_addr().await;
            }
            TaskKind::Route => self.update_all_topic_routes().await?,
            TaskKind::Heartbeat => {
                self.clean_offline_brokers().await?;
                self.send_heartbeat_to_all_brokers().await?;
            }
            TaskKind::Persist => {
                for consumer in self.groups.snapshot()?.consumers {
                    if let Some(operation) = consumer.persist_consumer_offset() {
                        operation.await;
                    }
                }
            }
            TaskKind::Adjust => self.adjust_thread_pool().await?,
        }
        Ok(())
    }

    pub async fn register_producer(
        self: &Arc<Self>,
        group: &str,
        handle: Arc<dyn ProducerRuntime>,
    ) -> Result<GroupLease, RuntimeError> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(Command::RegisterProducer {
                group: group.into(),
                handle,
                reply,
            })
            .map_err(|_| self.control_error())?;
        receive.await.map_err(|_| self.control_error())??.accept()
    }

    pub async fn register_consumer(
        self: &Arc<Self>,
        group: &str,
        handle: Arc<dyn ConsumerRuntime>,
    ) -> Result<GroupLease, RuntimeError> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(Command::RegisterConsumer {
                group: group.into(),
                handle,
                reply,
            })
            .map_err(|_| self.control_error())?;
        receive.await.map_err(|_| self.control_error())??.accept()
    }

    pub async fn register_admin(self: &Arc<Self>, group: &str) -> Result<GroupLease, RuntimeError> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(Command::RegisterAdmin {
                group: group.into(),
                reply,
            })
            .map_err(|_| self.control_error())?;
        receive.await.map_err(|_| self.control_error())??.accept()
    }

    pub fn select_producer(
        &self,
        group: &str,
    ) -> Result<Option<Arc<dyn ProducerRuntime>>, RuntimeError> {
        self.groups.select_producer(group)
    }

    pub fn select_consumer(
        &self,
        group: &str,
    ) -> Result<Option<Arc<dyn ConsumerRuntime>>, RuntimeError> {
        self.groups.select_consumer(group)
    }

    pub fn prepare_heartbeat_data(
        &self,
        without_sub: bool,
    ) -> Result<rocketmq_classic_protocol::heartbeat::HeartbeatData, RuntimeError> {
        use rocketmq_classic_protocol::heartbeat::{HeartbeatData, ProducerData};
        let snapshot = self.groups.snapshot()?;
        let mut heartbeat = HeartbeatData {
            client_id: Some(self.client_id.clone()),
            is_without_sub: without_sub,
            ..Default::default()
        };
        for (group, _) in snapshot.producers {
            heartbeat
                .producer_data_set
                .insert(ProducerData { group_name: group });
        }
        for consumer in snapshot.consumers {
            let mut data = consumer.heartbeat_data();
            if without_sub {
                data.subscription_data_set.clear();
            }
            heartbeat.consumer_data_set.insert(data);
        }
        Ok(heartbeat)
    }

    pub async fn unregister_producer(
        self: &Arc<Self>,
        lease: &GroupLease,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        self.request_unregister(lease, GroupKind::Producer, deadline)
            .await
    }

    pub async fn unregister_consumer(
        self: &Arc<Self>,
        lease: &GroupLease,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        self.request_unregister(lease, GroupKind::Consumer, deadline)
            .await
    }

    pub async fn unregister_admin(
        self: &Arc<Self>,
        lease: &GroupLease,
    ) -> Result<bool, RuntimeError> {
        self.request_unregister(lease, GroupKind::Admin, tokio::time::Instant::now())
            .await
    }

    pub(super) async fn unregister_group(
        &self,
        lease: &GroupLease,
        kind: GroupKind,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        if !self.groups.unregister(lease, kind)? {
            return Ok(false);
        }
        if kind == GroupKind::Admin {
            return Ok(true);
        }
        let header = rocketmq_classic_protocol::header::UnregisterClientRequestHeader {
            client_id: self.client_id.clone(),
            producer_group: (kind == GroupKind::Producer).then(|| lease.group.clone()),
            consumer_group: (kind == GroupKind::Consumer).then(|| lease.group.clone()),
        };
        let snapshot = self.routes.snapshot()?;
        for brokers in snapshot.broker_addresses.values() {
            for address in brokers.values() {
                // Java best effort: one failed peer must not prevent cleanup of
                // other peers. The phase contract bounds all attempts together.
                let _ = self.api.unregister_client(address, &header, deadline).await;
            }
        }
        Ok(true)
    }

    pub fn state(&self) -> RuntimeState {
        self.control.state()
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), RuntimeError> {
        if matches!(self.state(), RuntimeState::Failed | RuntimeState::Stopped) {
            return Err(RuntimeError::InvalidState(self.state()));
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(Command::Start { reply })
            .map_err(|_| self.control_error())?;
        let result = receive.await.map_err(|_| self.control_error())?;
        self.control.join_terminal().await?;
        result
    }

    pub async fn shutdown(self: &Arc<Self>) -> Result<bool, RuntimeError> {
        if matches!(self.state(), RuntimeState::Stopped | RuntimeState::Failed) {
            self.control.join_terminal().await?;
            return Ok(false);
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        if self
            .control
            .sender(self)?
            .send(Command::Shutdown { reply })
            .is_err()
        {
            if matches!(self.state(), RuntimeState::Stopped | RuntimeState::Failed) {
                self.control.join_terminal().await?;
                return Ok(false);
            }
            return Err(self.control_error());
        }
        let result = match receive.await {
            Ok(result) => result,
            Err(_) if self.state() == RuntimeState::Stopped => Ok(false),
            Err(_) => Err(self.control_error()),
        };
        self.control.join_terminal().await?;
        result
    }

    async fn request_unregister(
        self: &Arc<Self>,
        lease: &GroupLease,
        kind: GroupKind,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        if matches!(self.state(), RuntimeState::Stopped | RuntimeState::Failed) {
            return Ok(false);
        }
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.control
            .sender(self)?
            .send(Command::Unregister {
                lease: lease.clone(),
                kind,
                deadline,
                reply: Some(reply),
            })
            .map_err(|_| self.control_error())?;
        receive.await.map_err(|_| self.control_error())?
    }

    fn control_error(&self) -> RuntimeError {
        match self.state() {
            RuntimeState::Failed | RuntimeState::Stopped => {
                RuntimeError::InvalidState(self.state())
            }
            _ => RuntimeError::ControlTaskFailed,
        }
    }

    pub(crate) fn rpc_timeout(&self) -> tokio::time::Duration {
        tokio::time::Duration::from_millis(self.config.mq_client_api_timeout.max(0) as u64)
    }

    pub(super) fn attach_manager(self: &Arc<Self>, link: ManagerLink) {
        let _ = self.manager.set(link);
        self.inbound.bind(self);
        if let Some(listener) = &self.heartbeat_listener {
            listener.bind(self);
        }
    }

    pub(super) fn remove_from_manager(self: &Arc<Self>) -> Result<bool, RuntimeError> {
        self.manager
            .get()
            .map_or(Ok(false), |link| link.remove(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, Instant};

    #[tokio::test(start_paused = true)]
    async fn route_refresh_and_offline_cleanup_share_three_second_gate_without_blocking_readers() {
        let config = ClientConfig::new("192.0.2.6");
        let runtime =
            Arc::new(ClientRuntime::construct(config.clone(), &HashMap::new(), None).unwrap());
        let other = ClientRuntime::construct(config, &HashMap::new(), None).unwrap();
        let held = runtime.namesrv_gate.lock().await;
        let before = runtime.route_snapshot().unwrap();
        let update = {
            let runtime = runtime.clone();
            tokio::spawn(async move {
                runtime
                    .update_topic_route("topic", Instant::now() + Duration::from_secs(30))
                    .await
            })
        };
        let cleanup = {
            let runtime = runtime.clone();
            tokio::spawn(async move { runtime.clean_offline_brokers().await })
        };
        tokio::task::yield_now().await;
        assert!(other.namesrv_gate.try_lock().is_ok());
        assert!(Arc::ptr_eq(&before, &runtime.route_snapshot().unwrap()));
        tokio::time::advance(Duration::from_millis(2999)).await;
        assert!(!update.is_finished());
        assert!(!cleanup.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!update.await.unwrap().unwrap()); // No transport started/fetch attempted.
        cleanup.await.unwrap().unwrap();
        assert!(Arc::ptr_eq(&before, &runtime.route_snapshot().unwrap()));
        drop(held);
        let acquired = runtime.namesrv_gate.lock().await;
        let cancelled = {
            let runtime = runtime.clone();
            tokio::spawn(async move {
                runtime
                    .update_topic_route("cancelled", Instant::now() + Duration::from_secs(30))
                    .await
            })
        };
        tokio::task::yield_now().await;
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        drop(acquired);
        assert!(runtime.namesrv_gate.try_lock().is_ok());
    }
}
