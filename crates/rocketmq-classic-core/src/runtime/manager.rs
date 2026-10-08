use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    sync::{Arc, Mutex, OnceLock, Weak},
};

use rocketmq_classic_remoting::client::RpcHook;
use tokio::sync::Mutex as AsyncMutex;

use super::{ClientRuntime, RuntimeError};
use crate::config::ClientConfig;

type Entries = HashMap<String, Arc<Entry>>;

struct Entry {
    config: ClientConfig,
    properties: HashMap<String, String>,
    hook: Option<Arc<dyn RpcHook>>,
    initialization: AsyncMutex<()>,
    runtime: OnceLock<Arc<ClientRuntime>>,
}

/// Explicit sharing boundary corresponding to MQClientManager's factory table.
/// Keys are installed before any resource construction. Each key initializes
/// independently; a failed entry may be retried by a subsequent lookup.
#[derive(Clone, Default)]
pub struct ClientRuntimeManager {
    entries: Arc<Mutex<Entries>>,
    top_addressing: Option<Arc<dyn crate::namesrv::TopAddressing>>,
}

#[derive(Clone)]
pub(super) struct ManagerLink(Weak<Mutex<Entries>>);

impl ManagerLink {
    pub fn same_table(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.0, &other.0)
    }
    pub fn remove(&self, runtime: &Arc<ClientRuntime>) -> Result<bool, RuntimeError> {
        let Some(entries) = self.0.upgrade() else {
            return Ok(false);
        };
        ClientRuntimeManager {
            entries,
            top_addressing: None,
        }
        .remove_client_factory(runtime.client_id(), runtime)
    }
}

impl ClientRuntimeManager {
    /// A retained live view, corresponding to MQClientManager.getFactoryTable.
    /// Read methods observe published runtimes; mutations await only their key.
    pub fn factory_table(&self) -> FactoryTable {
        FactoryTable {
            manager: Self {
                entries: self.entries.clone(),
                top_addressing: None,
            },
        }
    }
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_top_addressing(provider: Arc<dyn crate::namesrv::TopAddressing>) -> Self {
        Self {
            entries: Arc::default(),
            top_addressing: Some(provider),
        }
    }

    pub async fn get_or_create(
        &self,
        config: &ClientConfig,
    ) -> Result<Arc<ClientRuntime>, RuntimeError> {
        self.get_or_create_with_hook(config, None).await
    }

    pub async fn get_or_create_with_hook(
        &self,
        config: &ClientConfig,
        hook: Option<Arc<dyn RpcHook>>,
    ) -> Result<Arc<ClientRuntime>, RuntimeError> {
        let provider = self.top_addressing.clone();
        self.get_or_create_inner(config, hook, move |entry| {
            let provider = provider.clone();
            async move {
                ClientRuntime::construct_with_top_addressing(
                    entry.config.clone_client_config(),
                    &entry.properties,
                    entry.hook.clone(),
                    provider,
                )
            }
        })
        .await
    }

    async fn get_or_create_inner<F, Fut>(
        &self,
        config: &ClientConfig,
        hook: Option<Arc<dyn RpcHook>>,
        construct: F,
    ) -> Result<Arc<ClientRuntime>, RuntimeError>
    where
        F: Fn(Arc<Entry>) -> Fut,
        Fut: Future<Output = Result<ClientRuntime, RuntimeError>>,
    {
        let client_id = config.build_client_id();
        let properties: HashMap<_, _> = std::env::vars()
            .filter(|(key, _)| {
                key.starts_with("tls.")
                    || key == "rocketmq.namesrv.domain"
                    || key == "rocketmq.namesrv.domain.subgroup"
                    || key == super::rebalance::WAIT_PROPERTY
                    || key == super::rebalance::MIN_PROPERTY
            })
            .collect();
        loop {
            let entry = {
                let mut entries = self
                    .entries
                    .lock()
                    .map_err(|_| RuntimeError::RegistryPoisoned)?;
                let entry = entries.entry(client_id.clone()).or_insert_with(|| {
                    Arc::new(Entry {
                        config: config.clone(),
                        properties: properties.clone(),
                        hook: hook.clone(),
                        initialization: AsyncMutex::new(()),
                        runtime: OnceLock::new(),
                    })
                });
                if !compatible(&entry.config, config)
                    || entry.properties != properties
                    || !same_hook(&entry.hook, &hook)
                {
                    return Err(RuntimeError::ConfigurationConflict { client_id });
                }
                entry.clone()
            };
            let _initialization = entry.initialization.lock().await;
            // A waiter on a failed/removed entry must not initialize an orphan
            // after another lookup installed a replacement for the same ID.
            let current = self
                .entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?
                .get(&client_id)
                .is_some_and(|value| Arc::ptr_eq(value, &entry));
            if !current {
                continue;
            }
            if let Some(runtime) = entry.runtime.get() {
                return Ok(runtime.clone());
            }
            match construct(entry.clone()).await {
                Ok(created) => {
                    let created = Arc::new(created);
                    created.attach_manager(ManagerLink(Arc::downgrade(&self.entries)))?;
                    // The key lock guarantees a sole publisher. Removal reads
                    // this immutable cell without contending with lookup locks.
                    let _ = entry.runtime.set(created.clone());
                    return Ok(created);
                }
                Err(error) => {
                    self.remove_entry(&client_id, &entry)?;
                    return Err(error);
                }
            }
        }
    }

    fn remove_entry(&self, client_id: &str, expected: &Arc<Entry>) -> Result<bool, RuntimeError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if entries
            .get(client_id)
            .is_some_and(|value| Arc::ptr_eq(value, expected))
        {
            entries.remove(client_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Identity-aware cleanup, corresponding to removeClientFactory(id, instance).
    pub fn remove_client_factory(
        &self,
        client_id: &str,
        expected: &Arc<ClientRuntime>,
    ) -> Result<bool, RuntimeError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        let matches = entries.get(client_id).is_some_and(|entry| {
            entry
                .runtime
                .get()
                .is_some_and(|value| Arc::ptr_eq(value, expected))
        });
        if matches {
            entries.remove(client_id);
        }
        Ok(matches)
    }

    /// Unconditional removal corresponding to Java removeClientFactory(id).
    /// Does not shut down the removed runtime. Lifecycle cleanup must continue
    /// to use the identity-aware overload to avoid removing replacements.
    /// Await only this key's initializer, as ConcurrentHashMap removal does for
    /// an in-progress mapping; unrelated keys remain independently available.
    pub async fn remove_client_factory_by_id(&self, client_id: &str) -> Result<bool, RuntimeError> {
        loop {
            let entry = {
                let entries = self
                    .entries
                    .lock()
                    .map_err(|_| RuntimeError::RegistryPoisoned)?;
                let Some(entry) = entries.get(client_id) else {
                    return Ok(false);
                };
                entry.clone()
            };
            let _initialization = entry.initialization.lock().await;
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            match entries.get(client_id) {
                None => return Ok(false),
                Some(current) if Arc::ptr_eq(current, &entry) => {
                    let published = entry.runtime.get().is_some();
                    entries.remove(client_id);
                    return Ok(published);
                }
                // A concurrent identity-safe cleanup/replacement overtook our
                // wait: unconditional removal targets the current mapping.
                Some(_) => continue,
            }
        }
    }
}

fn same_hook(left: &Option<Arc<dyn RpcHook>>, right: &Option<Arc<dyn RpcHook>>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        _ => false,
    }
}

/// Live factory-map access with owned reads and atomic per-key mutations.
/// Removing/replacing mappings does not shut down their runtime. Key aliases
/// are permitted, as in the Java map; lifecycle cleanup uses the runtime's ID.
#[derive(Clone)]
pub struct FactoryTable {
    manager: ClientRuntimeManager,
}

enum TableMutation {
    Put(Arc<ClientRuntime>),
    PutIfAbsent(Arc<ClientRuntime>),
    Replace(Arc<ClientRuntime>),
    ReplaceIfSame(Arc<ClientRuntime>, Arc<ClientRuntime>),
    Remove,
    RemoveIfSame(Arc<ClientRuntime>),
}

impl FactoryTable {
    pub fn get(&self, key: &str) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let entries = self
            .manager
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(entries
            .get(key)
            .and_then(|entry| entry.runtime.get())
            .cloned())
    }

    pub fn contains_key(&self, key: &str) -> Result<bool, RuntimeError> {
        self.get(key).map(|value| value.is_some())
    }

    /// Uses MQClientInstance's inherited Java Object identity equality.
    pub fn contains_value(&self, value: &Arc<ClientRuntime>) -> Result<bool, RuntimeError> {
        let entries = self
            .manager
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(entries
            .values()
            .filter_map(|entry| entry.runtime.get())
            .any(|current| Arc::ptr_eq(current, value)))
    }

    /// Returns the mapped runtime, or the supplied value when the key is absent.
    pub fn get_or_default(
        &self,
        key: &str,
        default: Arc<ClientRuntime>,
    ) -> Result<Arc<ClientRuntime>, RuntimeError> {
        Ok(self.get(key)?.unwrap_or(default))
    }

    pub fn len(&self) -> Result<usize, RuntimeError> {
        let entries = self
            .manager
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(entries
            .values()
            .filter(|entry| entry.runtime.get().is_some())
            .count())
    }

    pub fn is_empty(&self) -> Result<bool, RuntimeError> {
        self.len().map(|len| len == 0)
    }

    /// An explicitly detached copy for iteration. The FactoryTable handle
    /// itself stays live and every subsequent read sees the current mapping.
    pub fn snapshot(&self) -> Result<HashMap<String, Arc<ClientRuntime>>, RuntimeError> {
        let entries = self
            .manager
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        Ok(entries
            .iter()
            .filter_map(|(key, entry)| {
                entry
                    .runtime
                    .get()
                    .map(|runtime| (key.clone(), runtime.clone()))
            })
            .collect())
    }

    /// Visits one detached map snapshot. The callback runs without a manager
    /// lock and may safely reenter this table; Java's weakly consistent live
    /// traversal is not represented by this Rust convenience method.
    pub fn for_each(
        &self,
        mut action: impl FnMut(&str, &Arc<ClientRuntime>),
    ) -> Result<(), RuntimeError> {
        let snapshot = self.snapshot()?;
        for (key, runtime) in &snapshot {
            action(key, runtime);
        }
        Ok(())
    }

    pub fn key_set_view(&self) -> FactoryKeySetView {
        FactoryKeySetView(self.clone())
    }

    pub fn values_view(&self) -> FactoryValuesView {
        FactoryValuesView(self.clone())
    }

    pub fn entry_set_view(&self) -> FactoryEntrySetView {
        FactoryEntrySetView(self.clone())
    }

    pub async fn put(
        &self,
        key: &str,
        value: Arc<ClientRuntime>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        self.mutate(key, TableMutation::Put(value))
            .await
            .map(|(previous, _)| previous)
    }

    /// Applies each mapping independently, matching Map.putAll per-key writes.
    pub async fn put_all(
        &self,
        entries: BTreeMap<String, Arc<ClientRuntime>>,
    ) -> Result<(), RuntimeError> {
        for (key, value) in entries {
            self.put(&key, value).await?;
        }
        Ok(())
    }

    pub async fn put_if_absent(
        &self,
        key: &str,
        value: Arc<ClientRuntime>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        self.mutate(key, TableMutation::PutIfAbsent(value))
            .await
            .map(|(previous, _)| previous)
    }

    /// ConcurrentMap remapping uses conditional writes; a closure may be
    /// invoked again if another writer changes the same mapping.
    pub async fn compute_if_absent(
        &self,
        key: impl Into<String>,
        mut mapping: impl FnMut(&str) -> Option<Arc<ClientRuntime>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let key = key.into();
        if let Some(current) = self.get(&key)? {
            return Ok(Some(current));
        }
        let Some(value) = mapping(&key) else {
            return Ok(None);
        };
        Ok(Some(
            self.put_if_absent(&key, value.clone())
                .await?
                .unwrap_or(value),
        ))
    }

    pub async fn compute_if_present(
        &self,
        key: impl Into<String>,
        mut remapping: impl FnMut(&str, &Arc<ClientRuntime>) -> Option<Arc<ClientRuntime>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let key = key.into();
        loop {
            let Some(current) = self.get(&key)? else {
                return Ok(None);
            };
            match remapping(&key, &current) {
                Some(value) => {
                    if self.replace_if_same(&key, &current, value.clone()).await? {
                        return Ok(Some(value));
                    }
                }
                None => {
                    if self.remove_if_same(&key, &current).await? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    pub async fn compute(
        &self,
        key: impl Into<String>,
        mut remapping: impl FnMut(&str, Option<&Arc<ClientRuntime>>) -> Option<Arc<ClientRuntime>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let key = key.into();
        loop {
            let current = self.get(&key)?;
            let value = remapping(&key, current.as_ref());
            match (current.as_ref(), value) {
                (None, None) => return Ok(None),
                (None, Some(value)) => {
                    if self.put_if_absent(&key, value.clone()).await?.is_none() {
                        return Ok(Some(value));
                    }
                }
                (Some(current), Some(value)) => {
                    if self.replace_if_same(&key, current, value.clone()).await? {
                        return Ok(Some(value));
                    }
                }
                (Some(current), None) => {
                    if self.remove_if_same(&key, current).await? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    pub async fn merge(
        &self,
        key: impl Into<String>,
        value: Arc<ClientRuntime>,
        mut remapping: impl FnMut(
            &Arc<ClientRuntime>,
            &Arc<ClientRuntime>,
        ) -> Option<Arc<ClientRuntime>>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        let key = key.into();
        loop {
            let Some(current) = self.get(&key)? else {
                if self.put_if_absent(&key, value.clone()).await?.is_none() {
                    return Ok(Some(value));
                }
                continue;
            };
            match remapping(&current, &value) {
                Some(next) => {
                    if self.replace_if_same(&key, &current, next.clone()).await? {
                        return Ok(Some(next));
                    }
                }
                None => {
                    if self.remove_if_same(&key, &current).await? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    pub async fn replace_all(
        &self,
        mut remapping: impl FnMut(&str, &Arc<ClientRuntime>) -> Arc<ClientRuntime>,
    ) -> Result<(), RuntimeError> {
        for key in self.snapshot()?.into_keys() {
            loop {
                let Some(current) = self.get(&key)? else {
                    break;
                };
                let value = remapping(&key, &current);
                if self.replace_if_same(&key, &current, value).await? {
                    break;
                }
            }
        }
        Ok(())
    }

    pub async fn replace(
        &self,
        key: &str,
        value: Arc<ClientRuntime>,
    ) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        self.mutate(key, TableMutation::Replace(value))
            .await
            .map(|(previous, _)| previous)
    }

    pub async fn replace_if_same(
        &self,
        key: &str,
        expected: &Arc<ClientRuntime>,
        value: Arc<ClientRuntime>,
    ) -> Result<bool, RuntimeError> {
        self.mutate(key, TableMutation::ReplaceIfSame(expected.clone(), value))
            .await
            .map(|(_, changed)| changed)
    }

    pub async fn remove(&self, key: &str) -> Result<Option<Arc<ClientRuntime>>, RuntimeError> {
        self.mutate(key, TableMutation::Remove)
            .await
            .map(|(previous, _)| previous)
    }

    pub async fn remove_if_same(
        &self,
        key: &str,
        expected: &Arc<ClientRuntime>,
    ) -> Result<bool, RuntimeError> {
        self.mutate(key, TableMutation::RemoveIfSame(expected.clone()))
            .await
            .map(|(_, changed)| changed)
    }

    /// Like ConcurrentMap.clear, not a transaction against concurrent inserts.
    pub async fn clear(&self) -> Result<(), RuntimeError> {
        let keys: Vec<_> = self
            .manager
            .entries
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?
            .keys()
            .cloned()
            .collect();
        for key in keys {
            self.remove(&key).await?;
        }
        Ok(())
    }

    async fn mutate(
        &self,
        key: &str,
        operation: TableMutation,
    ) -> Result<(Option<Arc<ClientRuntime>>, bool), RuntimeError> {
        loop {
            let observed = self
                .manager
                .entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?
                .get(key)
                .cloned();
            // No registry guard crosses await, and no user callback runs while
            // either lock is held. Await initialization only for this key.
            let _initialization = match &observed {
                Some(entry) => Some(entry.initialization.lock().await),
                None => None,
            };
            let mut entries = self
                .manager
                .entries
                .lock()
                .map_err(|_| RuntimeError::RegistryPoisoned)?;
            let current = entries.get(key);
            let unchanged = match (&observed, current) {
                (None, None) => true,
                (Some(old), Some(current)) => Arc::ptr_eq(old, current),
                _ => false,
            };
            if !unchanged {
                continue;
            }
            let previous = current.and_then(|entry| entry.runtime.get()).cloned();
            let same = |expected: &Arc<ClientRuntime>| {
                previous
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, expected))
            };
            let replacement = match &operation {
                TableMutation::Put(value) => Some(value),
                TableMutation::PutIfAbsent(value) if previous.is_none() => Some(value),
                TableMutation::Replace(value) if previous.is_some() => Some(value),
                TableMutation::ReplaceIfSame(expected, value) if same(expected) => Some(value),
                TableMutation::Remove => {
                    entries.remove(key);
                    return Ok((previous.clone(), previous.is_some()));
                }
                TableMutation::RemoveIfSame(expected) if same(expected) => {
                    entries.remove(key);
                    return Ok((previous, true));
                }
                _ => return Ok((previous, false)),
            };
            if let Some(value) = replacement {
                // Retain the actual transport's sharing inputs, not today's
                // process properties or a fabricated fresh hook identity.
                let entry = Entry {
                    config: value.config_snapshot(),
                    properties: value.sharing_properties.clone(),
                    hook: value.sharing_hook.clone(),
                    initialization: AsyncMutex::new(()),
                    runtime: OnceLock::from(value.clone()),
                };
                if key == value.client_id() {
                    value.attach_manager(ManagerLink(Arc::downgrade(&self.manager.entries)))?;
                }
                entries.insert(key.into(), Arc::new(entry));
                return Ok((previous, true));
            }
        }
    }
}

/// Retained live key-set view of a `FactoryTable`.
#[derive(Clone)]
pub struct FactoryKeySetView(FactoryTable);

impl FactoryKeySetView {
    pub fn contains(&self, key: &str) -> Result<bool, RuntimeError> {
        self.0.contains_key(key)
    }

    pub fn len(&self) -> Result<usize, RuntimeError> {
        self.0.len()
    }

    pub fn is_empty(&self) -> Result<bool, RuntimeError> {
        self.0.is_empty()
    }

    /// Returns a detached key copy from the live view.
    pub fn snapshot(&self) -> Result<BTreeSet<String>, RuntimeError> {
        Ok(self.0.snapshot()?.into_keys().collect())
    }

    pub async fn remove(&self, key: &str) -> Result<bool, RuntimeError> {
        self.0.remove(key).await.map(|value| value.is_some())
    }

    /// Removes keys accepted by the predicate from a detached key snapshot.
    pub async fn remove_if(
        &self,
        mut predicate: impl FnMut(&str) -> bool,
    ) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for key in self.snapshot()? {
            if predicate(&key) && self.remove(&key).await? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub async fn remove_all(&self, keys: &BTreeSet<String>) -> Result<usize, RuntimeError> {
        self.remove_if(|key| keys.contains(key)).await
    }

    pub async fn retain_all(&self, keys: &BTreeSet<String>) -> Result<usize, RuntimeError> {
        self.remove_if(|key| !keys.contains(key)).await
    }

    pub async fn clear(&self) -> Result<(), RuntimeError> {
        self.0.clear().await
    }
}

/// Retained live values collection view of a `FactoryTable`.
#[derive(Clone)]
pub struct FactoryValuesView(FactoryTable);

impl FactoryValuesView {
    pub fn contains(&self, value: &Arc<ClientRuntime>) -> Result<bool, RuntimeError> {
        self.0.contains_value(value)
    }

    pub fn len(&self) -> Result<usize, RuntimeError> {
        self.0.len()
    }

    pub fn is_empty(&self) -> Result<bool, RuntimeError> {
        self.0.is_empty()
    }

    /// Returns a detached values copy from the live view.
    pub fn snapshot(&self) -> Result<Vec<Arc<ClientRuntime>>, RuntimeError> {
        Ok(self.0.snapshot()?.into_values().collect())
    }

    /// Removes one matching runtime mapping without shutting the runtime down.
    pub async fn remove(&self, value: &Arc<ClientRuntime>) -> Result<bool, RuntimeError> {
        for (key, current) in self.0.snapshot()? {
            if Arc::ptr_eq(&current, value) && self.0.remove_if_same(&key, value).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Removes values accepted by the predicate from a detached entry snapshot.
    pub async fn remove_if(
        &self,
        mut predicate: impl FnMut(&Arc<ClientRuntime>) -> bool,
    ) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for (key, value) in self.0.snapshot()? {
            if predicate(&value) && self.0.remove_if_same(&key, &value).await? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub async fn remove_all(&self, values: &[Arc<ClientRuntime>]) -> Result<usize, RuntimeError> {
        self.remove_if(|value| values.iter().any(|candidate| Arc::ptr_eq(candidate, value)))
            .await
    }

    pub async fn retain_all(&self, values: &[Arc<ClientRuntime>]) -> Result<usize, RuntimeError> {
        self.remove_if(|value| !values.iter().any(|candidate| Arc::ptr_eq(candidate, value)))
            .await
    }

    pub async fn clear(&self) -> Result<(), RuntimeError> {
        self.0.clear().await
    }
}

/// Retained live entry-set view of a `FactoryTable`.
#[derive(Clone)]
pub struct FactoryEntrySetView(FactoryTable);

impl FactoryEntrySetView {
    pub fn contains(&self, key: &str, value: &Arc<ClientRuntime>) -> Result<bool, RuntimeError> {
        Ok(self
            .0
            .get(key)?
            .is_some_and(|current| Arc::ptr_eq(&current, value)))
    }

    pub fn len(&self) -> Result<usize, RuntimeError> {
        self.0.len()
    }

    pub fn is_empty(&self) -> Result<bool, RuntimeError> {
        self.0.is_empty()
    }

    /// Returns a detached entry copy from the live view.
    pub fn snapshot(&self) -> Result<BTreeMap<String, Arc<ClientRuntime>>, RuntimeError> {
        Ok(self.0.snapshot()?.into_iter().collect())
    }

    pub async fn remove(
        &self,
        key: &str,
        value: &Arc<ClientRuntime>,
    ) -> Result<bool, RuntimeError> {
        self.0.remove_if_same(key, value).await
    }

    /// Removes entries accepted by the predicate from a detached entry snapshot.
    pub async fn remove_if(
        &self,
        mut predicate: impl FnMut(&str, &Arc<ClientRuntime>) -> bool,
    ) -> Result<usize, RuntimeError> {
        let mut removed = 0;
        for (key, value) in self.0.snapshot()? {
            if predicate(&key, &value) && self.0.remove_if_same(&key, &value).await? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub async fn remove_all(
        &self,
        entries: &BTreeMap<String, Arc<ClientRuntime>>,
    ) -> Result<usize, RuntimeError> {
        self.remove_if(|key, value| {
            entries
                .get(key)
                .is_some_and(|candidate| Arc::ptr_eq(candidate, value))
        })
        .await
    }

    pub async fn retain_all(
        &self,
        entries: &BTreeMap<String, Arc<ClientRuntime>>,
    ) -> Result<usize, RuntimeError> {
        self.remove_if(|key, value| {
            !entries
                .get(key)
                .is_some_and(|candidate| Arc::ptr_eq(candidate, value))
        })
        .await
    }

    pub async fn clear(&self) -> Result<(), RuntimeError> {
        self.0.clear().await
    }
}

fn compatible(left: &ClientConfig, right: &ClientConfig) -> bool {
    left.namesrv_addr == right.namesrv_addr
        && left.namespace_v2 == right.namespace_v2
        && left.enable_stream_request_type == right.enable_stream_request_type
        && left.use_tls == right.use_tls
        && left.socks_proxy_config == right.socks_proxy_config
        && left.client_callback_executor_threads == right.client_callback_executor_threads
        && left.poll_name_server_interval == right.poll_name_server_interval
        && left.heartbeat_broker_interval == right.heartbeat_broker_interval
        && left.persist_consumer_offset_interval == right.persist_consumer_offset_interval
        && left.mq_client_api_timeout == right.mq_client_api_timeout
        && left.use_heartbeat_v2 == right.use_heartbeat_v2
        && left.enable_heartbeat_channel_event_listener
            == right.enable_heartbeat_channel_event_listener
        && left.enable_concurrent_heartbeat == right.enable_concurrent_heartbeat
        && left.concurrent_heartbeat_thread_pool_size == right.concurrent_heartbeat_thread_pool_size
        && left.language == right.language
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn unconditional_removal_matches_java_without_stale_identity_cleanup_regression() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        let java = &fixture["managerRemoval"];
        let manager = ClientRuntimeManager::new();
        let config = ClientConfig::new("unconditional-remove");
        let id = config.build_client_id();
        assert!(!manager.remove_client_factory_by_id(&id).await.unwrap());
        let first = manager.get_or_create(&config).await.unwrap();
        assert_eq!(
            manager.entries.lock().unwrap()[&id]
                .runtime
                .get()
                .is_some_and(|value| Arc::ptr_eq(value, &first)),
            java["liveSeesFirst"]
        );
        assert!(manager.remove_client_factory_by_id(&id).await.unwrap());
        assert_eq!(
            !manager.entries.lock().unwrap().contains_key(&id),
            java["absentAfterUnconditional"]
        );
        assert_eq!(first.state(), super::super::RuntimeState::Created);
        let replacement = manager.get_or_create(&config).await.unwrap();
        assert_eq!(
            !Arc::ptr_eq(&first, &replacement),
            java["replacementIdentity"]
        );
        assert!(!manager.remove_client_factory(&id, &first).unwrap());
        assert_eq!(
            Arc::ptr_eq(&replacement, &manager.get_or_create(&config).await.unwrap()),
            java["staleIdentityKeepsReplacement"]
        );
        assert!(manager.remove_client_factory_by_id(&id).await.unwrap());
        assert_eq!(
            !manager.entries.lock().unwrap().contains_key(&id),
            java["unconditionalRemovesReplacement"]
        );
        assert!(!manager.remove_client_factory_by_id(&id).await.unwrap());
        assert_eq!(
            !manager.entries.lock().unwrap().contains_key(&id),
            java["absentRemovalStaysAbsent"]
        );
    }

    #[tokio::test]
    async fn unconditional_removal_waits_only_for_its_key_and_cancellation_preserves_mapping() {
        use tokio::time::{Duration, timeout};
        for cancel in [false, true] {
            let manager = ClientRuntimeManager::new();
            let config = ClientConfig::new("in-progress-removal");
            let ready = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let task_manager = manager.clone();
            let task_config = config.clone();
            let task_ready = ready.clone();
            let task_release = release.clone();
            let creation = tokio::spawn(async move {
                task_manager
                    .get_or_create_inner(&task_config, None, |entry| {
                        let ready = task_ready.clone();
                        let release = task_release.clone();
                        async move {
                            ready.notify_one();
                            release.notified().await;
                            ClientRuntime::construct(entry.config.clone(), &entry.properties, None)
                        }
                    })
                    .await
            });
            ready.notified().await;
            let task_manager = manager.clone();
            let id = config.build_client_id();
            let mut removal =
                tokio::spawn(async move { task_manager.remove_client_factory_by_id(&id).await });
            assert!(
                timeout(Duration::from_millis(20), &mut removal)
                    .await
                    .is_err()
            );
            let other = timeout(
                Duration::from_secs(1),
                manager.get_or_create(&ClientConfig::new("other-key")),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(other.client_id(), "other-key@DEFAULT");
            if cancel {
                removal.abort();
                assert!((&mut removal).await.unwrap_err().is_cancelled());
            } else {
                // Still waiting after unrelated construction, not serializing
                // the whole table or prematurely removing a reserved entry.
                assert!(!removal.is_finished());
            }
            release.notify_one();
            let first = creation.await.unwrap().unwrap();
            if cancel {
                assert!(Arc::ptr_eq(
                    &first,
                    &manager.get_or_create(&config).await.unwrap()
                ));
            } else {
                assert!(removal.await.unwrap().unwrap());
                let replacement = manager.get_or_create(&config).await.unwrap();
                assert!(!Arc::ptr_eq(&first, &replacement));
                assert!(
                    !manager
                        .remove_client_factory(first.client_id(), &first)
                        .unwrap()
                );
            }
        }
    }

    #[tokio::test]
    async fn live_table_waits_for_publication_and_cancelled_mutation_has_no_effect() {
        use tokio::time::{Duration, timeout};
        for action in ["put", "remove", "clear", "cancel"] {
            let manager = ClientRuntimeManager::new();
            let live = manager.factory_table();
            let mut config = ClientConfig::new("reserved-table-key");
            config.namesrv_addr = Some(String::new());
            let source = ClientRuntimeManager::new();
            let replacement = source.get_or_create(&config).await.unwrap();
            let id = config.build_client_id();
            let ready = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let task_manager = manager.clone();
            let task_config = config.clone();
            let task_ready = ready.clone();
            let task_release = release.clone();
            let creation = tokio::spawn(async move {
                task_manager
                    .get_or_create_inner(&task_config, None, |entry| {
                        let ready = task_ready.clone();
                        let release = task_release.clone();
                        async move {
                            ready.notify_one();
                            release.notified().await;
                            ClientRuntime::construct(entry.config.clone(), &entry.properties, None)
                        }
                    })
                    .await
            });
            ready.notified().await;
            assert!(live.get(&id).unwrap().is_none());
            assert_eq!(live.len().unwrap(), 0);
            assert!(live.snapshot().unwrap().is_empty());
            let task_live = live.clone();
            let task_id = id.clone();
            let task_value = replacement.clone();
            let mut mutation = tokio::spawn(async move {
                match action {
                    "put" | "cancel" => task_live.put(&task_id, task_value).await,
                    "remove" => task_live.remove(&task_id).await,
                    _ => task_live.clear().await.map(|()| None),
                }
            });
            assert!(
                timeout(Duration::from_millis(20), &mut mutation)
                    .await
                    .is_err()
            );
            let mut unrelated = ClientConfig::new("unrelated-table-key");
            unrelated.namesrv_addr = Some(String::new());
            timeout(Duration::from_secs(1), manager.get_or_create(&unrelated))
                .await
                .unwrap()
                .unwrap();
            if action == "cancel" {
                mutation.abort();
                assert!(matches!((&mut mutation).await, Err(error) if error.is_cancelled()));
            }
            release.notify_one();
            let created = creation.await.unwrap().unwrap();
            if action == "cancel" {
                assert!(Arc::ptr_eq(&live.get(&id).unwrap().unwrap(), &created));
            } else {
                let previous = mutation.await.unwrap().unwrap();
                if action == "put" || action == "remove" {
                    assert!(Arc::ptr_eq(&previous.unwrap(), &created));
                }
                if action == "put" {
                    assert!(Arc::ptr_eq(&live.get(&id).unwrap().unwrap(), &replacement));
                } else {
                    assert!(live.get(&id).unwrap().is_none());
                }
            }
        }
    }

    #[tokio::test]
    async fn table_insertion_retains_original_property_snapshot_not_current_environment() {
        let mut config = ClientConfig::new("property-table-key");
        config.namesrv_addr = Some(String::new());
        let properties = HashMap::from([(
            "rocketmq.namesrv.domain".into(),
            "captured-table-fixture.invalid".into(),
        )]);
        let runtime =
            Arc::new(ClientRuntime::construct(config.clone(), &properties, None).unwrap());
        let manager = ClientRuntimeManager::new();
        manager
            .factory_table()
            .put(runtime.client_id(), runtime.clone())
            .await
            .unwrap();
        assert_eq!(
            manager.entries.lock().unwrap()[runtime.client_id()].properties,
            properties
        );
        manager.factory_table().clear().await.unwrap();
    }

    #[tokio::test]
    async fn unrelated_keys_construct_concurrently_and_same_key_constructs_once() {
        let manager = ClientRuntimeManager::new();
        let barrier = Arc::new(Barrier::new(2));
        let count = AtomicUsize::new(0);
        let construct = |entry: Arc<Entry>| {
            let barrier = barrier.clone();
            let count = &count;
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                barrier.wait().await;
                ClientRuntime::construct(entry.config.clone(), &entry.properties, None)
            }
        };
        let first = ClientConfig::new("first");
        let second = ClientConfig::new("second");
        let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(
                manager.get_or_create_inner(&first, None, &construct),
                manager.get_or_create_inner(&second, None, &construct)
            )
        })
        .await
        .unwrap();
        assert!(!Arc::ptr_eq(&first.unwrap(), &second.unwrap()));
        assert_eq!(count.load(Ordering::SeqCst), 2);
        let config = ClientConfig::new("first");
        manager
            .get_or_create_inner(&config, None, &construct)
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn stale_failed_entry_cannot_remove_a_replacement_entry() {
        let manager = ClientRuntimeManager::new();
        let config = ClientConfig::new("first");
        let first = manager.get_or_create(&config).await.unwrap();
        let old_entry = manager.entries.lock().unwrap()[first.client_id()].clone();
        manager
            .remove_client_factory(first.client_id(), &first)
            .unwrap();
        let replacement = manager.get_or_create(&config).await.unwrap();
        assert!(!manager.remove_entry(first.client_id(), &old_entry).unwrap());
        assert!(Arc::ptr_eq(
            &replacement,
            &manager.get_or_create(&config).await.unwrap()
        ));
    }

    #[tokio::test]
    async fn identity_cleanup_does_not_depend_on_a_concurrent_lookup_lock() {
        let manager = ClientRuntimeManager::new();
        let config = ClientConfig::new("first");
        let first = manager.get_or_create(&config).await.unwrap();
        let entry = manager.entries.lock().unwrap()[first.client_id()].clone();
        let _lookup_guard = entry.initialization.lock().await;
        assert!(
            manager
                .remove_client_factory(first.client_id(), &first)
                .unwrap()
        );
        let replacement = manager.get_or_create(&config).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &replacement));
    }

    #[tokio::test]
    async fn cancelled_initialization_releases_key_for_retry_without_publishing_resources() {
        let manager = ClientRuntimeManager::new();
        let config = ClientConfig::new("first");
        let (ready, started) = tokio::sync::oneshot::channel();
        let sender = Arc::new(Mutex::new(Some(ready)));
        let task_manager = manager.clone();
        let task_config = config.clone();
        let task = tokio::spawn(async move {
            task_manager
                .get_or_create_inner(&task_config, None, |entry| {
                    let sender = sender.clone();
                    async move {
                        let _unpublished = ClientRuntime::construct(
                            entry.config.clone(),
                            &entry.properties,
                            None,
                        )?;
                        sender.lock().unwrap().take().unwrap().send(()).unwrap();
                        std::future::pending::<Result<ClientRuntime, RuntimeError>>().await
                    }
                })
                .await
        });
        started.await.unwrap();
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        let replacement = manager.get_or_create(&config).await.unwrap();
        assert!(Arc::ptr_eq(
            &replacement,
            &manager.get_or_create(&config).await.unwrap()
        ));
    }
}
