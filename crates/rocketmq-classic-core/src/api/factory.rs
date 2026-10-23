//! Rust-native pool lifecycle matching Java `MQClientAPIFactory`.
//!
//! The Java constructor writes NameServer and VIP values into JVM-wide system
//! properties. This implementation applies the same effective values to its
//! owned client configuration and NameServer provider, without changing the
//! process environment. Pool selection, mutable array aliasing, address
//! updates, and start/shutdown behavior are represented directly on
//! [`MQClientApiFactory`].

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock, Weak},
    time::Duration,
};

use rand::Rng;
use rocketmq_classic_remoting::client::{
    ClientConfig as RemotingClientConfig, RemotingClient, RequestProcessor, RpcHook, TlsConfig,
};
use rocketmq_classic_remoting::{TransportError, client::TransportMode};
use thiserror::Error;
use tokio::{
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};

use crate::{
    config::{ClientConfig, SharedClientConfig},
    namesrv::{
        DefaultTopAddressing, NameServerUpdateCallback, TopAddressing, java_util_blank,
        split_addresses, ws_addr,
    },
};

use super::MQClientApi;

/// Creates a remoting client for one Java-style `namePrefix + "N_" + index` slot.
///
/// The optional creator constructor on Java `MQClientAPIFactory` accepts an
/// `ObjectCreator<RemotingClient>` with the `NettyClientConfig` and channel
/// listener. Rust passes the generated instance name and the matching remoting
/// settings so callers can inject a native client transport.
pub type RemotingClientCreator =
    dyn Fn(&str, RemotingClientConfig) -> Result<RemotingClient, TransportError> + Send + Sync;

/// NameServer inputs accepted by Java `NameserverAccessConfig`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NameserverAccessConfig {
    namesrv_addr: Option<String>,
    namesrv_domain: Option<String>,
    namesrv_domain_subgroup: Option<String>,
}

impl NameserverAccessConfig {
    pub fn new(
        namesrv_addr: Option<String>,
        namesrv_domain: Option<String>,
        namesrv_domain_subgroup: Option<String>,
    ) -> Self {
        Self {
            namesrv_addr,
            namesrv_domain,
            namesrv_domain_subgroup,
        }
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn namesrv_domain(&self) -> Option<&str> {
        self.namesrv_domain.as_deref()
    }

    pub fn namesrv_domain_subgroup(&self) -> Option<&str> {
        self.namesrv_domain_subgroup.as_deref()
    }
}

#[derive(Debug, Error)]
pub enum MQClientApiFactoryError {
    #[error("The configuration item NamesrvAddr is not configured")]
    MissingNameServerAddress,
    #[error("Nameserver domain subgroup is null")]
    NullNameServerDomainSubgroup,
    #[error("client count must not be negative: {0}")]
    NegativeClientCount(i32),
    #[error("MQClientAPIFactory has not been started")]
    NotStarted,
    #[error("MQClientAPIFactory has no clients")]
    EmptyClientArray,
    #[error("MQClientAPIFactory client slot {0} is null")]
    NullClient(usize),
    #[error("NameServer update callback registration failed")]
    AddressingCallbackFailed,
    #[error("client startup task failed: {0}")]
    StartupTask(String),
    #[error("client shutdown task failed: {0}")]
    ShutdownTask(String),
    #[error(transparent)]
    Transport(#[from] TransportError),
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("client array index {index} is outside length {len}")]
pub struct ClientApiArrayError {
    pub index: usize,
    pub len: usize,
}

/// A shared live view of Java `MQClientAPIFactory.getClients()`'s array.
///
/// Cloned views point at the same slots. Setting a slot to `None` is the Rust
/// equivalent of assigning Java `null` into the returned array.
#[derive(Clone)]
pub struct SharedClientApiArray {
    clients: Arc<RwLock<Vec<Option<Arc<MQClientApi>>>>>,
}

impl SharedClientApiArray {
    fn new(length: usize) -> Self {
        Self {
            clients: Arc::new(RwLock::new(vec![None; length])),
        }
    }

    pub fn len(&self) -> usize {
        self.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> Option<Option<Arc<MQClientApi>>> {
        self.read().get(index).cloned()
    }

    pub fn set(
        &self,
        index: usize,
        client: Option<Arc<MQClientApi>>,
    ) -> Result<(), ClientApiArrayError> {
        let mut clients = self.write();
        let len = clients.len();
        let Some(slot) = clients.get_mut(index) else {
            return Err(ClientApiArrayError { index, len });
        };
        *slot = client;
        Ok(())
    }

    pub fn snapshot(&self) -> Vec<Option<Arc<MQClientApi>>> {
        self.read().clone()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Vec<Option<Arc<MQClientApi>>>> {
        self.clients
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Vec<Option<Arc<MQClientApi>>>> {
        self.clients
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

struct FactoryAddressState {
    clients: RwLock<Option<SharedClientApiArray>>,
    initial: Option<String>,
    current: Mutex<Vec<Option<String>>>,
}

impl FactoryAddressState {
    fn new(address: Option<String>) -> Self {
        Self {
            clients: RwLock::new(None),
            initial: address,
            current: Mutex::new(Vec::new()),
        }
    }

    fn reset(&self, client_count: usize) {
        *self
            .current
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            vec![self.initial.clone(); client_count];
    }

    fn current(&self, index: usize) -> Option<String> {
        self.current
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(index)
            .cloned()
            .flatten()
    }

    fn publish_clients(&self, clients: SharedClientApiArray) {
        *self
            .clients
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(clients);
    }

    fn clients(&self) -> Option<SharedClientApiArray> {
        self.clients
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn update_slot(&self, index: usize, addresses: Option<&str>) -> Option<String> {
        let Some(addresses) = addresses else {
            return self.current(index);
        };
        let changed = {
            let mut current = self
                .current
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let current = current.get_mut(index)?;
            if current.as_deref() == Some(addresses) {
                false
            } else {
                *current = Some(addresses.to_owned());
                true
            }
        };
        if changed {
            if let Some(clients) = self.clients() {
                let addresses = split_addresses(addresses);
                if let Some(Some(client)) = clients.get(index) {
                    client
                        .remoting_client()
                        .update_name_server_address_list(addresses);
                }
            }
        }
        self.current(index)
    }

    fn update_all(&self, addresses: Option<&str>) -> Option<String> {
        let count = self
            .current
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len();
        for index in 0..count {
            self.update_slot(index, addresses);
        }
        addresses
            .map(str::to_owned)
            .or_else(|| (0..count).find_map(|index| self.current(index)))
    }

    fn update_from_discovery(&self, index: usize, addresses: Option<&str>) -> Option<String> {
        self.update_slot(
            index,
            addresses.filter(|addresses| !java_util_blank(addresses)),
        )
    }
}

fn java_process_properties() -> HashMap<String, String> {
    std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .collect()
}

struct FactoryAddressCallback(Weak<FactoryAddressState>, usize);

impl NameServerUpdateCallback for FactoryAddressCallback {
    fn on_name_server_address_change(&self, addresses: Option<&str>) -> Option<String> {
        self.0.upgrade()?.update_slot(self.1, addresses)
    }
}

/// Owns a pool of client API remoting clients, matching Java
/// `MQClientAPIFactory`'s constructor, selection, NameServer update, and
/// lifecycle operations.
pub struct MQClientApiFactory {
    name_prefix: String,
    client_num: i32,
    client_config: ClientConfig,
    remoting_config: RemotingClientConfig,
    client_remoting_processor: Option<Arc<dyn RequestProcessor>>,
    rpc_hook: Option<Arc<dyn RpcHook>>,
    remoting_client_creator: Arc<RemotingClientCreator>,
    top_addressing: Option<Arc<dyn TopAddressing>>,
    address_state: Arc<FactoryAddressState>,
    refresh_shutdown: Mutex<Option<tokio::sync::watch::Sender<bool>>>,
    refresh_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl MQClientApiFactory {
    /// Creates the standard Java-shaped API pool. Tokio supplies the scheduled
    /// per-client NameServer refresh workers that Java schedules through its
    /// `ScheduledExecutorService` constructor argument.
    pub fn new(
        nameserver_access: NameserverAccessConfig,
        name_prefix: impl Into<String>,
        client_num: i32,
        client_remoting_processor: Option<Arc<dyn RequestProcessor>>,
        rpc_hook: Option<Arc<dyn RpcHook>>,
    ) -> Result<Self, MQClientApiFactoryError> {
        Self::build(
            nameserver_access,
            name_prefix.into(),
            client_num,
            client_remoting_processor,
            rpc_hook,
            None,
        )
    }

    /// Constructor counterpart for Java's overload with an injected
    /// `ObjectCreator<RemotingClient>`.
    pub fn with_remoting_client_creator<F>(
        nameserver_access: NameserverAccessConfig,
        name_prefix: impl Into<String>,
        client_num: i32,
        client_remoting_processor: Option<Arc<dyn RequestProcessor>>,
        rpc_hook: Option<Arc<dyn RpcHook>>,
        creator: F,
    ) -> Result<Self, MQClientApiFactoryError>
    where
        F: Fn(&str, RemotingClientConfig) -> Result<RemotingClient, TransportError>
            + Send
            + Sync
            + 'static,
    {
        Self::build(
            nameserver_access,
            name_prefix.into(),
            client_num,
            client_remoting_processor,
            rpc_hook,
            Some(Arc::new(creator)),
        )
    }

    fn build(
        nameserver_access: NameserverAccessConfig,
        name_prefix: String,
        client_num: i32,
        client_remoting_processor: Option<Arc<dyn RequestProcessor>>,
        rpc_hook: Option<Arc<dyn RpcHook>>,
        creator: Option<Arc<RemotingClientCreator>>,
    ) -> Result<Self, MQClientApiFactoryError> {
        let domain = nameserver_access.namesrv_domain.as_deref().unwrap_or("");
        let mut client_config = ClientConfig::default();
        client_config.set_vip_channel_enabled(false);
        client_config.set_decode_read_body(true);
        client_config.set_decode_decompress_body(false);

        let (address, top_addressing) = if domain.is_empty() {
            let address = nameserver_access
                .namesrv_addr
                .as_deref()
                .filter(|address| !address.is_empty())
                .ok_or(MQClientApiFactoryError::MissingNameServerAddress)?
                .to_owned();
            client_config.set_namesrv_addr(Some(address.clone()));
            (Some(address), None)
        } else {
            let subgroup = nameserver_access
                .namesrv_domain_subgroup
                .as_deref()
                .ok_or(MQClientApiFactoryError::NullNameServerDomainSubgroup)?;
            client_config.set_namesrv_addr(None);
            let mut properties = java_process_properties();
            properties.insert("rocketmq.namesrv.domain".into(), domain.to_owned());
            properties.insert(
                "rocketmq.namesrv.domain.subgroup".into(),
                subgroup.to_owned(),
            );
            let provider = Arc::new(DefaultTopAddressing::new(
                ws_addr(&properties),
                client_config.unit_name().map(str::to_owned),
            ));
            (None, Some(provider as Arc<dyn TopAddressing>))
        };

        let java_properties = java_process_properties();
        let mut remoting_config = RemotingClientConfig::default();
        let mut transport = TransportMode::from_java_properties(&java_properties, None)?;
        if client_config.use_tls() && matches!(transport, TransportMode::Plain) {
            transport =
                TransportMode::Tls(TlsConfig::from_java_properties(&java_properties, None)?);
        }
        remoting_config.transport = transport;
        remoting_config.disable_callback_executor = true;

        let address_state = Arc::new(FactoryAddressState::new(address));
        let remoting_client_creator =
            creator.unwrap_or_else(|| Arc::new(|_, config| RemotingClient::with_config(config)));
        Ok(Self {
            name_prefix,
            client_num,
            client_config,
            remoting_config,
            client_remoting_processor,
            rpc_hook,
            remoting_client_creator,
            top_addressing,
            address_state,
            refresh_shutdown: Mutex::new(None),
            refresh_tasks: Mutex::new(Vec::new()),
        })
    }

    /// Replaces the default NameServer provider used by the domain-based
    /// constructor, retaining the provider's change callback behavior.
    pub fn with_top_addressing(
        mut self,
        provider: Arc<dyn TopAddressing>,
    ) -> Result<Self, MQClientApiFactoryError> {
        self.top_addressing = Some(provider);
        Ok(self)
    }

    fn register_address_callback(&self, index: usize) -> Result<(), MQClientApiFactoryError> {
        if let Some(provider) = &self.top_addressing {
            let callback: Arc<dyn NameServerUpdateCallback> = Arc::new(FactoryAddressCallback(
                Arc::downgrade(&self.address_state),
                index,
            ));
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                provider.register_change_callback(callback)
            }))
            .map_err(|_| MQClientApiFactoryError::AddressingCallbackFailed)?;
        }
        Ok(())
    }

    /// Returns a randomly selected API client, or `None` when Java's mutable
    /// client array contains `null` at the selected position.
    pub fn get_client(&self) -> Result<Option<Arc<MQClientApi>>, MQClientApiFactoryError> {
        let clients = self
            .address_state
            .clients()
            .ok_or(MQClientApiFactoryError::NotStarted)?;
        let snapshot = clients.snapshot();
        if snapshot.is_empty() {
            return Err(MQClientApiFactoryError::EmptyClientArray);
        }
        let index = if snapshot.len() == 1 {
            0
        } else {
            rand::rng().random_range(0..snapshot.len())
        };
        Ok(snapshot[index].clone())
    }

    /// Starts all pool clients. A failure leaves the already-created slots
    /// visible, matching Java's partially populated `clients` array.
    pub async fn start(&self) -> Result<(), MQClientApiFactoryError> {
        self.stop_refresh().await;
        let client_count = usize::try_from(self.client_num)
            .map_err(|_| MQClientApiFactoryError::NegativeClientCount(self.client_num))?;
        let clients = SharedClientApiArray::new(client_count);
        self.address_state.reset(client_count);
        self.address_state.publish_clients(clients.clone());
        if client_count == 0 {
            return Ok(());
        }

        for index in 0..client_count {
            if let Some(provider) = &self.top_addressing {
                self.register_address_callback(index)?;
                let addresses = provider.fetch_ns_addr().await;
                self.address_state
                    .update_from_discovery(index, addresses.as_deref());
            }
            let instance_name = format!("{}N_{index}", self.name_prefix);
            let mut client_config = self.client_config.clone();
            client_config.set_instance_name(Some(instance_name.clone()));
            let shared_config: SharedClientConfig = Arc::new(RwLock::new(client_config));
            let mut remoting_config = self.remoting_config.clone();
            remoting_config.disable_callback_executor = true;
            let remoting = (self.remoting_client_creator)(&instance_name, remoting_config)?;
            if let Some(address) = self.address_state.current(index) {
                remoting.update_name_server_address_list(split_addresses(&address));
            }
            if let Some(hook) = &self.rpc_hook {
                remoting.add_rpc_hook(hook.clone());
            }
            if let Some(processor) = &self.client_remoting_processor {
                remoting.register_default_processor(Some(processor.clone()));
            }
            let api = MQClientApi::with_config(remoting.clone(), shared_config);
            remoting.start()?;
            clients
                .set(index, Some(Arc::new(api)))
                .map_err(|error| MQClientApiFactoryError::StartupTask(error.to_string()))?;
        }

        if let Some(provider) = self.top_addressing.clone() {
            self.start_refresh(provider, client_count);
        }
        Ok(())
    }

    /// Propagates a NameServer list change to every currently installed API
    /// client. A `None` address is a Java-null no-op after start.
    pub fn on_name_server_address_change(
        &self,
        addresses: Option<&str>,
    ) -> Result<(), MQClientApiFactoryError> {
        if self.address_state.clients().is_none() {
            return Err(MQClientApiFactoryError::NotStarted);
        }
        self.address_state.update_all(addresses);
        Ok(())
    }

    /// Returns the same mutable client-array view on every call after start.
    pub fn get_clients(&self) -> Option<SharedClientApiArray> {
        self.address_state.clients()
    }

    /// Java `StartAndShutdown.preShutdown()` is an inherited no-op for this
    /// factory; the method remains available on the Rust lifecycle surface.
    pub fn pre_shutdown(&self) {}

    /// Shuts down all installed clients concurrently and cancels domain refresh.
    pub async fn shutdown(&self) -> Result<(), MQClientApiFactoryError> {
        self.stop_refresh().await;
        let clients = self
            .address_state
            .clients()
            .ok_or(MQClientApiFactoryError::NotStarted)?;
        let mut shutdowns = JoinSet::new();
        for (index, client) in clients.snapshot().into_iter().enumerate() {
            let client = client.ok_or(MQClientApiFactoryError::NullClient(index))?;
            let remoting = client.remoting_client();
            shutdowns.spawn(async move { remoting.shutdown().await });
        }
        while let Some(result) = shutdowns.join_next().await {
            result.map_err(|error| MQClientApiFactoryError::ShutdownTask(error.to_string()))?;
        }
        Ok(())
    }

    fn start_refresh(&self, provider: Arc<dyn TopAddressing>, client_count: usize) {
        let (shutdown, _) = tokio::sync::watch::channel(false);
        *self
            .refresh_shutdown
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(shutdown.clone());
        let mut tasks = Vec::with_capacity(client_count);
        for index in 0..client_count {
            let provider = provider.clone();
            let mut stop = shutdown.subscribe();
            let state = Arc::downgrade(&self.address_state);
            tasks.push(tokio::spawn(async move {
                let mut ticks = tokio::time::interval_at(
                    Instant::now() + Duration::from_secs(10),
                    Duration::from_secs(120),
                );
                ticks.set_missed_tick_behavior(MissedTickBehavior::Burst);
                loop {
                    tokio::select! {
                        _ = ticks.tick() => {
                            let addresses = tokio::select! {
                                addresses = provider.fetch_ns_addr() => addresses,
                                changed = stop.changed() => {
                                    if changed.is_err() || *stop.borrow() {
                                        return;
                                    }
                                    continue;
                                }
                            };
                            if let Some(addresses) = addresses {
                                if let Some(state) = state.upgrade() {
                                    state.update_from_discovery(index, Some(&addresses));
                                } else {
                                    return;
                                }
                            }
                        }
                        changed = stop.changed() => {
                            if changed.is_err() || *stop.borrow() {
                                return;
                            }
                        }
                    }
                }
            }));
        }
        *self
            .refresh_tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = tasks;
    }

    async fn stop_refresh(&self) {
        if let Some(shutdown) = self
            .refresh_shutdown
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = shutdown.send(true);
        }
        let tasks = std::mem::take(
            &mut *self
                .refresh_tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        for task in tasks {
            let _ = task.await;
        }
    }
}

impl Drop for MQClientApiFactory {
    fn drop(&mut self) {
        if let Some(shutdown) = self
            .refresh_shutdown
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = shutdown.send(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_classic_remoting::client::TransportMode;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn static_access(address: &str) -> NameserverAccessConfig {
        NameserverAccessConfig::new(Some(address.into()), None, None)
    }

    #[tokio::test]
    async fn factory_starts_named_clients_updates_shared_array_and_shuts_down() {
        let java: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/api-factory.json"
        ))
        .unwrap();
        let address = java["nameserverAccessConfig"][0].as_str().unwrap();
        let domain = java["nameserverAccessConfig"][1]
            .as_str()
            .map(str::to_owned);
        let subgroup = java["nameserverAccessConfig"][2]
            .as_str()
            .map(str::to_owned);
        let access = NameserverAccessConfig::new(Some(address.to_owned()), domain, subgroup);
        assert_eq!(access.namesrv_addr(), Some(address));
        assert_eq!(access.namesrv_domain(), Some(""));
        assert_eq!(access.namesrv_domain_subgroup(), None);
        let name_prefix = "fixture-";
        let created = Arc::new(Mutex::new(Vec::new()));
        let observed = created.clone();
        let factory = MQClientApiFactory::with_remoting_client_creator(
            access,
            name_prefix,
            java["clientCountAfterStart"].as_i64().unwrap() as i32,
            None,
            None,
            move |instance_name, mut config| {
                observed.lock().unwrap().push(instance_name.to_owned());
                config.transport = TransportMode::Plain;
                RemotingClient::with_config(config)
            },
        )
        .unwrap();

        assert_eq!(java["getClientBeforeStartError"], "NullPointerException");
        assert_eq!(java["nullAddressBeforeStartError"], "NullPointerException");
        assert!(factory.get_clients().is_none());
        assert!(!factory.client_config.vip_channel_enabled());
        assert!(factory.client_config.decode_read_body());
        assert!(!factory.client_config.decode_decompress_body());
        assert!(matches!(
            factory.on_name_server_address_change(None),
            Err(MQClientApiFactoryError::NotStarted)
        ));
        factory.pre_shutdown();
        factory.start().await.unwrap();
        assert_eq!(created.lock().unwrap().len(), 2);
        for (index, name) in created.lock().unwrap().iter().enumerate() {
            assert_eq!(name, &format!("{name_prefix}N_{index}"));
        }
        let clients = factory.get_clients().unwrap();
        assert_eq!(
            clients.len(),
            java["clientCountAfterStart"].as_u64().unwrap() as usize
        );
        assert!(factory.get_client().unwrap().is_some());
        for api in clients.snapshot().into_iter().flatten() {
            let mut addresses = api.get_name_server_address_list().unwrap();
            addresses.sort();
            let mut expected = java["clientAddressesAfterStart"][0]
                .as_array()
                .unwrap()
                .iter()
                .map(|address| address.as_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            expected.sort();
            assert_eq!(addresses, expected);
        }

        let methods = java["publicMethods"].as_array().unwrap();
        assert_eq!(methods.len(), 6);
        for method in [
            "getClient()",
            "getClients()",
            "onNameServerAddressChange(java.lang.String)",
            "shutdown()",
            "start()",
            "preShutdown()",
        ] {
            assert!(
                methods
                    .iter()
                    .any(|entry| entry.as_str().unwrap().contains(method))
            );
        }
        assert_eq!(java["publicConstructors"].as_array().unwrap().len(), 2);
        assert!(factory.get_client().unwrap().is_some());
        factory
            .on_name_server_address_change(Some(
                java["clientAddressesAfterChange"][0][0].as_str().unwrap(),
            ))
            .unwrap();
        for api in clients.snapshot().into_iter().flatten() {
            assert_eq!(
                api.get_name_server_address_list(),
                Some(vec![
                    java["clientAddressesAfterChange"][0][0]
                        .as_str()
                        .unwrap()
                        .to_owned()
                ])
            );
        }
        factory.on_name_server_address_change(None).unwrap();
        assert_eq!(
            clients
                .snapshot()
                .into_iter()
                .flatten()
                .map(|api| api.get_name_server_address_list())
                .collect::<Vec<_>>(),
            java["clientAddressesAfterNullChange"]
                .as_array()
                .unwrap()
                .iter()
                .map(|addresses| Some(
                    addresses
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|address| address.as_str().unwrap().to_owned())
                        .collect::<Vec<_>>()
                ))
                .collect::<Vec<_>>()
        );
        assert_eq!(clients.set(0, None), Ok(()));
        assert!(matches!(factory.get_clients().unwrap().get(0), Some(None)));
        factory
            .get_clients()
            .unwrap()
            .set(0, clients.snapshot()[1].clone())
            .unwrap();
        factory.shutdown().await.unwrap();
        assert!(factory.get_clients().is_some());
        assert!(java["clientArrayRetainedAfterShutdown"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn factory_matches_java_configuration_and_empty_pool_failures() {
        assert!(matches!(
            MQClientApiFactory::new(NameserverAccessConfig::default(), "empty-", 1, None, None),
            Err(MQClientApiFactoryError::MissingNameServerAddress)
        ));
        let factory =
            MQClientApiFactory::new(static_access("ns:9876"), "zero-", 0, None, None).unwrap();
        factory.start().await.unwrap();
        assert!(matches!(
            factory.get_client(),
            Err(MQClientApiFactoryError::EmptyClientArray)
        ));
        factory.shutdown().await.unwrap();

        let negative =
            MQClientApiFactory::new(static_access("ns:9876"), "negative-", -1, None, None).unwrap();
        assert!(matches!(
            negative.start().await,
            Err(MQClientApiFactoryError::NegativeClientCount(-1))
        ));
    }

    #[tokio::test]
    async fn factory_domain_refresh_uses_configured_top_addressing_provider() {
        let java: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/api-factory.json"
        ))
        .unwrap();
        struct StaticProvider {
            calls: AtomicUsize,
        }
        impl TopAddressing for StaticProvider {
            fn fetch_ns_addr(
                &self,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send + '_>>
            {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Some("domain-ns:9876".into()) })
            }
        }

        let provider = Arc::new(StaticProvider {
            calls: AtomicUsize::new(0),
        });
        let domain = java["domainPropertyAfterInit"].as_str().unwrap();
        let subgroup = java["domainSubgroupPropertyAfterInit"].as_str().unwrap();
        let access =
            NameserverAccessConfig::new(None, Some(domain.to_owned()), Some(subgroup.to_owned()));
        assert_eq!(access.namesrv_domain(), Some(domain));
        assert_eq!(access.namesrv_domain_subgroup(), Some(subgroup));
        let factory = MQClientApiFactory::new(access, "domain-", 1, None, None)
            .unwrap()
            .with_top_addressing(provider.clone())
            .unwrap();
        factory.start().await.unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            factory
                .get_client()
                .unwrap()
                .unwrap()
                .get_name_server_address_list(),
            Some(vec!["domain-ns:9876".into()])
        );
        factory.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn factory_domain_discovery_keeps_each_clients_address_independent() {
        let java: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/api-factory.json"
        ))
        .unwrap();
        struct SequenceProvider(Mutex<std::collections::VecDeque<String>>);
        impl TopAddressing for SequenceProvider {
            fn fetch_ns_addr(
                &self,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send + '_>>
            {
                let address = self.0.lock().unwrap().pop_front();
                Box::pin(async move { address })
            }
        }

        let expected = java["domainClientAddressesAfterStart"]
            .as_array()
            .unwrap()
            .iter()
            .map(|addresses| {
                addresses
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|address| address.as_str().unwrap().to_owned())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let provider = Arc::new(SequenceProvider(Mutex::new(
            expected
                .iter()
                .map(|addresses| addresses[0].clone())
                .collect(),
        )));
        let access = NameserverAccessConfig::new(
            None,
            Some("factory-domain.example".into()),
            Some("factory-subgroup".into()),
        );
        let factory = MQClientApiFactory::new(
            access,
            "domain-independent-",
            expected.len() as i32,
            None,
            None,
        )
        .unwrap()
        .with_top_addressing(provider)
        .unwrap();

        factory.start().await.unwrap();
        let actual = factory
            .get_clients()
            .unwrap()
            .snapshot()
            .into_iter()
            .map(|client| client.unwrap().get_name_server_address_list().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        factory.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn factory_registers_and_dispatches_name_server_callbacks_per_client() {
        struct RecordingProvider {
            calls: AtomicUsize,
            callbacks: Mutex<Vec<Arc<dyn NameServerUpdateCallback>>>,
        }
        impl TopAddressing for RecordingProvider {
            fn fetch_ns_addr(
                &self,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send + '_>>
            {
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Some(format!("domain-ns-{call}:9876")) })
            }

            fn register_change_callback(&self, callback: Arc<dyn NameServerUpdateCallback>) {
                self.callbacks.lock().unwrap().push(callback);
            }
        }

        let provider = Arc::new(RecordingProvider {
            calls: AtomicUsize::new(0),
            callbacks: Mutex::new(Vec::new()),
        });
        let access = NameserverAccessConfig::new(
            None,
            Some("factory-domain.example".into()),
            Some("factory-subgroup".into()),
        );
        let factory = MQClientApiFactory::new(access.clone(), "domain-callback-", 2, None, None)
            .unwrap()
            .with_top_addressing(provider.clone())
            .unwrap();
        assert!(provider.callbacks.lock().unwrap().is_empty());

        factory.start().await.unwrap();
        let callbacks = provider.callbacks.lock().unwrap().clone();
        assert_eq!(callbacks.len(), 2);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            callbacks[0].on_name_server_address_change(Some("callback-ns-0:9876")),
            Some("callback-ns-0:9876".to_owned())
        );
        let mut addresses = factory
            .get_clients()
            .unwrap()
            .snapshot()
            .into_iter()
            .map(|client| client.unwrap().get_name_server_address_list())
            .collect::<Vec<_>>();
        assert_eq!(
            addresses,
            vec![
                Some(vec!["callback-ns-0:9876".to_owned()]),
                Some(vec!["domain-ns-1:9876".to_owned()])
            ]
        );
        assert_eq!(
            callbacks[1].on_name_server_address_change(Some("callback-ns-1:9876")),
            Some("callback-ns-1:9876".to_owned())
        );
        addresses = factory
            .get_clients()
            .unwrap()
            .snapshot()
            .into_iter()
            .map(|client| client.unwrap().get_name_server_address_list())
            .collect();
        assert_eq!(
            addresses,
            vec![
                Some(vec!["callback-ns-0:9876".to_owned()]),
                Some(vec!["callback-ns-1:9876".to_owned()])
            ]
        );
        assert_eq!(
            callbacks[0].on_name_server_address_change(None),
            Some("callback-ns-0:9876".to_owned())
        );
        factory.shutdown().await.unwrap();

        let zero_provider = Arc::new(RecordingProvider {
            calls: AtomicUsize::new(0),
            callbacks: Mutex::new(Vec::new()),
        });
        let zero = MQClientApiFactory::new(access, "domain-callback-zero-", 0, None, None)
            .unwrap()
            .with_top_addressing(zero_provider.clone())
            .unwrap();
        zero.start().await.unwrap();
        assert!(zero_provider.callbacks.lock().unwrap().is_empty());
        assert_eq!(zero_provider.calls.load(Ordering::SeqCst), 0);
        zero.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn factory_domain_refresh_runs_independently_for_each_client_slot() {
        let java: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/api-factory.json"
        ))
        .unwrap();
        struct NumberedProvider(AtomicUsize);
        impl TopAddressing for NumberedProvider {
            fn fetch_ns_addr(
                &self,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send + '_>>
            {
                let call = self.0.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Some(format!("domain-ns-{call}:9876")) })
            }
        }

        let access = NameserverAccessConfig::new(
            None,
            Some("factory-domain.example".into()),
            Some("factory-subgroup".into()),
        );
        let factory = MQClientApiFactory::new(access, "domain-refresh-", 2, None, None)
            .unwrap()
            .with_top_addressing(Arc::new(NumberedProvider(AtomicUsize::new(0))))
            .unwrap();

        factory.start().await.unwrap();
        assert_eq!(
            factory.refresh_tasks.lock().unwrap().len(),
            java["domainRefreshScheduleCountAfterStart"]
                .as_u64()
                .unwrap() as usize
        );
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(10)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let mut refreshed = factory
            .get_clients()
            .unwrap()
            .snapshot()
            .into_iter()
            .map(|client| {
                client
                    .unwrap()
                    .get_name_server_address_list()
                    .unwrap()
                    .remove(0)
            })
            .collect::<Vec<_>>();
        refreshed.sort();
        assert_eq!(
            refreshed,
            vec!["domain-ns-2:9876".to_owned(), "domain-ns-3:9876".to_owned()]
        );
        factory.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn factory_ignores_java_blank_discovery_and_null_change_inputs() {
        struct BlankProvider;
        impl TopAddressing for BlankProvider {
            fn fetch_ns_addr(
                &self,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send + '_>>
            {
                Box::pin(async { Some(" \t".into()) })
            }
        }

        let access = NameserverAccessConfig::new(
            None,
            Some("factory-domain.example".into()),
            Some("factory-subgroup".into()),
        );
        let factory = MQClientApiFactory::new(access, "blank-", 1, None, None)
            .unwrap()
            .with_top_addressing(Arc::new(BlankProvider))
            .unwrap();
        factory.start().await.unwrap();
        let clients = factory.get_clients().unwrap();
        assert!(clients.get(0).is_some_and(|client| client.is_some()));
        assert_eq!(
            clients
                .get(0)
                .unwrap()
                .unwrap()
                .get_name_server_address_list(),
            None
        );
        factory.on_name_server_address_change(None).unwrap();
        assert_eq!(
            clients
                .get(0)
                .unwrap()
                .unwrap()
                .get_name_server_address_list(),
            None
        );
        factory.shutdown().await.unwrap();
    }
}
