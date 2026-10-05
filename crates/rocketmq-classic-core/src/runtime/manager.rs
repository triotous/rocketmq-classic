use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, OnceLock},
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
}

impl ClientRuntimeManager {
    pub fn new() -> Self {
        Self::default()
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
        self.get_or_create_inner(config, hook, |entry| async move {
            ClientRuntime::construct(
                entry.config.clone_client_config(),
                &entry.properties,
                entry.hook.clone(),
            )
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
            .filter(|(key, _)| key.starts_with("tls."))
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
}

fn same_hook(left: &Option<Arc<dyn RpcHook>>, right: &Option<Arc<dyn RpcHook>>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        _ => false,
    }
}

fn compatible(left: &ClientConfig, right: &ClientConfig) -> bool {
    left.namesrv_addr == right.namesrv_addr
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
