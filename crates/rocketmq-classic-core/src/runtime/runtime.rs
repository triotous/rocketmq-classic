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

/// A manager-owned transport and typed API. Construction does not start tasks.
pub struct ClientRuntime {
    config: ClientConfig,
    client_id: String,
    remoting: RemotingClient,
    api: MQClientApi,
    routes: crate::route::RouteCache,
    pub(super) groups: GroupRegistry,
    pub(super) control: ControlPlane,
    pub(super) inbound: Arc<super::inbound::RuntimeProcessor>,
    manager: OnceLock<ManagerLink>,
    pub(crate) heartbeat: crate::heartbeat::HeartbeatState,
    addressing: Arc<dyn crate::namesrv::TopAddressing>,
    discovery: Arc<crate::namesrv::DiscoveryState>,
    #[cfg(test)]
    pub(super) periodic_probe: Arc<super::tasks::TaskProbe>,
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
        let remoting = RemotingClient::with_config(TransportConfig {
            transport,
            callback_executor_threads,
            socks_proxy_config: config.socks_proxy_config.clone(),
            scan_available_nameserver: false,
            ..TransportConfig::default()
        })?;
        if let Some(addresses) = &config.namesrv_addr {
            remoting.update_name_server_address_list(crate::namesrv::split_addresses(addresses));
        }
        if let Some(hook) = hook {
            remoting.add_rpc_hook(hook);
        }
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
        Ok(Self {
            client_id: config.build_client_id(),
            config,
            api: MQClientApi::new(remoting.clone()),
            remoting,
            routes: crate::route::RouteCache::default(),
            groups: GroupRegistry::default(),
            control: ControlPlane::default(),
            inbound: Arc::default(),
            manager: OnceLock::new(),
            heartbeat: crate::heartbeat::HeartbeatState::default(),
            addressing,
            discovery,
            #[cfg(test)]
            periodic_probe: Arc::default(),
        })
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
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

    pub async fn update_topic_route(
        &self,
        topic: &str,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        let route = self.api.get_topic_route(topic, deadline).await?;
        let groups = self.groups.snapshot()?;
        let needed = groups
            .producers
            .iter()
            .any(|(_, producer)| producer.is_publish_topic_need_update(topic))
            || groups
                .consumers
                .iter()
                .any(|consumer| consumer.is_subscribe_topic_need_update(topic));
        let changed = self.routes.replace(topic, route)?;
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
            TaskKind::Route => {
                let groups = self.groups.snapshot()?;
                let mut topics = std::collections::BTreeSet::new();
                for (_, producer) in groups.producers {
                    topics.extend(producer.publish_topics());
                }
                for consumer in groups.consumers {
                    topics.extend(
                        consumer
                            .heartbeat_data()
                            .subscription_data_set
                            .into_iter()
                            .map(|sub| sub.topic),
                    );
                }
                for topic in topics {
                    let _ = self
                        .update_topic_route(
                            &topic,
                            tokio::time::Instant::now() + self.rpc_timeout(),
                        )
                        .await;
                }
            }
            TaskKind::Heartbeat => {
                self.routes.clean_offline()?;
                self.send_heartbeat_to_all_brokers().await?;
            }
            TaskKind::Persist | TaskKind::Adjust => {
                for consumer in self.groups.snapshot()?.consumers {
                    let operation = match kind {
                        TaskKind::Persist => consumer.persist_consumer_offset(),
                        _ => consumer.adjust_thread_pool(),
                    };
                    if let Some(operation) = operation {
                        operation.await;
                    }
                }
            }
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
    }

    pub(super) fn remove_from_manager(self: &Arc<Self>) -> Result<bool, RuntimeError> {
        self.manager
            .get()
            .map_or(Ok(false), |link| link.remove(self))
    }
}
