use std::{collections::HashMap, sync::Arc};

use rocketmq_classic_remoting::client::{
    ClientConfig as TransportConfig, RemotingClient, RpcHook, TransportMode,
};

use super::{
    ConsumerRuntime, GroupLease, ProducerRuntime, RuntimeError,
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
    groups: GroupRegistry,
}

impl ClientRuntime {
    pub(super) fn construct(
        config: ClientConfig,
        properties: &HashMap<String, String>,
        hook: Option<Arc<dyn RpcHook>>,
    ) -> Result<Self, RuntimeError> {
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
            remoting
                .update_name_server_address_list(addresses.split(';').map(str::to_owned).collect());
        }
        if let Some(hook) = hook {
            remoting.add_rpc_hook(hook);
        }
        Ok(Self {
            client_id: config.build_client_id(),
            config,
            api: MQClientApi::new(remoting.clone()),
            remoting,
            routes: crate::route::RouteCache::default(),
            groups: GroupRegistry::default(),
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

    pub fn route_snapshot(&self) -> Result<Arc<crate::route::RouteSnapshot>, RuntimeError> {
        self.routes.snapshot()
    }

    pub async fn update_topic_route(
        &self,
        topic: &str,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        let route = self.api.get_topic_route(topic, deadline).await?;
        self.routes.replace(topic, route)
    }

    pub fn register_producer(
        &self,
        group: &str,
        handle: Arc<dyn ProducerRuntime>,
    ) -> Result<GroupLease, RuntimeError> {
        self.groups.register_producer(group, handle)
    }

    pub fn register_consumer(
        &self,
        group: &str,
        handle: Arc<dyn ConsumerRuntime>,
    ) -> Result<GroupLease, RuntimeError> {
        self.groups.register_consumer(group, handle)
    }

    pub fn register_admin(&self, group: &str) -> Result<GroupLease, RuntimeError> {
        self.groups.register_admin(group)
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
        &self,
        lease: &GroupLease,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        self.unregister_group(lease, GroupKind::Producer, deadline)
            .await
    }

    pub async fn unregister_consumer(
        &self,
        lease: &GroupLease,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        self.unregister_group(lease, GroupKind::Consumer, deadline)
            .await
    }

    pub fn unregister_admin(&self, lease: &GroupLease) -> Result<bool, RuntimeError> {
        self.groups.unregister(lease, GroupKind::Admin)
    }

    async fn unregister_group(
        &self,
        lease: &GroupLease,
        kind: GroupKind,
        deadline: tokio::time::Instant,
    ) -> Result<bool, RuntimeError> {
        if !self.groups.unregister(lease, kind)? {
            return Ok(false);
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
}
