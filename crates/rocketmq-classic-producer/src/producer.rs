use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, OnceLock, RwLock, Weak},
    time::{Duration, Instant as StdInstant},
};

use rocketmq_classic_core::{
    route::TopicSnapshot,
    runtime::{ClientRuntime, ClientRuntimeManager, GroupLease, ProducerRuntime},
};

#[cfg(test)]
use rocketmq_classic_core::api::producer::{SendMode, SendResult};

use crate::{
    ProducerConfig, ProducerError, accumulator::AutoBatchAccumulator, fault::FaultStrategy,
    selector::RoundRobinSelector, unique_id::UniqueIdGenerator, validation,
};

#[cfg(test)]
pub(crate) struct ScriptedSendAttempt {
    pub(crate) broker: String,
    pub(crate) address: String,
    pub(crate) queue: rocketmq_classic_common::message::MessageQueue,
    pub(crate) body: Vec<u8>,
    pub(crate) properties: Option<String>,
    pub(crate) sys_flag: i32,
    pub(crate) unique_id: String,
    pub(crate) mode: SendMode,
    pub(crate) batch: bool,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct ScriptedTransport {
    pub(crate) responses: std::collections::VecDeque<Result<Option<SendResult>, ProducerError>>,
    pub(crate) attempts: Vec<ScriptedSendAttempt>,
}
use tokio::{sync::Semaphore, task::JoinHandle};

enum State {
    Created,
    Started {
        runtime: Weak<ClientRuntime>,
        lease: GroupLease,
    },
    Shutdown,
}

/// Java-compatible producer lifecycle and send surface.
pub struct Producer {
    pub(crate) config: ProducerConfig,
    manager: ClientRuntimeManager,
    state: tokio::sync::Mutex<State>,
    detector_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    pub(crate) auto_batch_accumulator: AutoBatchAccumulator,
    pub(crate) auto_batch_tasks: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
    pub(crate) self_weak: OnceLock<Weak<Producer>>,
    pub(crate) transaction: OnceLock<Arc<crate::transaction::TransactionRuntime>>,
    pub(crate) backpressure_send_num: Arc<Semaphore>,
    pub(crate) backpressure_send_size: Arc<Semaphore>,
    publish_info: RwLock<HashMap<String, Arc<TopicSnapshot>>>,
    pub(crate) selector: RoundRobinSelector,
    pub(crate) fault: FaultStrategy,
    pub(crate) ids: UniqueIdGenerator,
    #[cfg(test)]
    pub(crate) scripted_transport: std::sync::Mutex<ScriptedTransport>,
    #[cfg(test)]
    pub(crate) scripted_end_transactions: std::sync::Mutex<
        Vec<(
            String,
            rocketmq_classic_protocol::header::EndTransactionRequestHeader,
            Option<String>,
        )>,
    >,
    #[cfg(test)]
    pub(crate) scripted_end_failure: std::sync::atomic::AtomicBool,
}

impl Producer {
    pub fn new(config: ProducerConfig) -> Self {
        let manager = ClientRuntimeManager::shared_default();
        Self::with_runtime_manager(config, manager)
    }

    pub(crate) fn with_runtime_manager(
        config: ProducerConfig,
        manager: ClientRuntimeManager,
    ) -> Self {
        let ids = UniqueIdGenerator::new(config.client_config().client_ip());
        let backpressure_send_num = Arc::new(Semaphore::new(
            config.backpressure_async_send_num().max(10) as usize,
        ));
        let backpressure_send_size = Arc::new(Semaphore::new(
            config.backpressure_async_send_size().max(1024 * 1024) as usize,
        ));
        let fault = FaultStrategy::new(
            config.latency_max_millis().to_vec(),
            config.not_available_duration_millis().to_vec(),
        );
        Self {
            ids,
            config,
            manager,
            state: tokio::sync::Mutex::new(State::Created),
            detector_task: tokio::sync::Mutex::new(None),
            auto_batch_accumulator: AutoBatchAccumulator::default(),
            auto_batch_tasks: tokio::sync::Mutex::new(Vec::new()),
            self_weak: OnceLock::new(),
            transaction: OnceLock::new(),
            backpressure_send_num,
            backpressure_send_size,
            publish_info: RwLock::new(HashMap::new()),
            selector: RoundRobinSelector::default(),
            fault,
            #[cfg(test)]
            scripted_transport: std::sync::Mutex::new(ScriptedTransport::default()),
            #[cfg(test)]
            scripted_end_transactions: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            scripted_end_failure: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn config(&self) -> &ProducerConfig {
        &self.config
    }

    pub(crate) async fn started_runtime(&self) -> Result<Arc<ClientRuntime>, ProducerError> {
        let state = self.state.lock().await;
        match &*state {
            State::Created => Err(ProducerError::NotStarted),
            State::Shutdown => Err(ProducerError::Shutdown),
            State::Started { runtime, .. } => runtime.upgrade().ok_or(ProducerError::Shutdown),
        }
    }

    fn unregister_deadline(&self) -> tokio::time::Instant {
        let millis = self.config.client.mq_client_api_timeout.max(0) as u64;
        tokio::time::Instant::now() + tokio::time::Duration::from_millis(millis)
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), ProducerError> {
        let mut state = self.state.lock().await;
        match &*state {
            State::Started { .. } => return Err(ProducerError::AlreadyStarted),
            State::Shutdown => return Err(ProducerError::Shutdown),
            State::Created => {}
        }
        let _ = self.self_weak.set(Arc::downgrade(self));
        validation::validate_group(self.config.producer_group())?;
        self.config.accumulator_settings()?;

        let runtime = self.manager.get_or_create(&self.config.client).await?;
        let owner: Arc<dyn ProducerRuntime> = self.clone();
        let lease = runtime
            .register_producer(self.config.producer_group(), owner)
            .await?;
        if let Err(error) = runtime.start().await {
            let deadline = self.unregister_deadline();
            let _ = runtime.unregister_producer(&lease, deadline).await;
            return Err(error.into());
        }
        *state = State::Started {
            runtime: Arc::downgrade(&runtime),
            lease,
        };
        if self.config.client.start_detector_enable() {
            let task = self.spawn_detector_task(Arc::downgrade(&runtime));
            *self.detector_task.lock().await = Some(task);
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), ProducerError> {
        let mut state = self.state.lock().await;
        if matches!(&*state, State::Created) {
            return Ok(());
        }
        let previous = std::mem::replace(&mut *state, State::Shutdown);
        match previous {
            State::Created | State::Shutdown => {
                self.stop_auto_batch().await;
                self.stop_detector_task().await;
                Ok(())
            }
            State::Started { runtime, lease } => {
                let Some(runtime_arc) = runtime.upgrade() else {
                    self.stop_auto_batch().await;
                    self.stop_detector_task().await;
                    return Ok(());
                };
                let deadline = self.unregister_deadline();
                match runtime_arc.unregister_producer(&lease, deadline).await {
                    Ok(_) => {}
                    Err(error) => {
                        *state = State::Started { runtime, lease };
                        return Err(error.into());
                    }
                }
                match runtime_arc.shutdown().await {
                    Ok(_) => {}
                    Err(error) => {
                        *state = State::Started {
                            runtime: Arc::downgrade(&runtime_arc),
                            lease,
                        };
                        return Err(error.into());
                    }
                }
                self.stop_auto_batch().await;
                self.stop_detector_task().await;
                Ok(())
            }
        }
    }

    fn spawn_detector_task(self: &Arc<Self>, runtime: Weak<ClientRuntime>) -> JoinHandle<()> {
        let producer = Arc::downgrade(self);
        tokio::spawn(async move {
            let period = Duration::from_secs(3);
            let mut interval =
                tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            loop {
                interval.tick().await;
                let Some(producer) = producer.upgrade() else {
                    break;
                };
                let Some(runtime) = runtime.upgrade() else {
                    break;
                };
                producer.detect_brokers(&runtime).await;
            }
        })
    }

    async fn stop_detector_task(&self) {
        if let Some(task) = self.detector_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
    }

    async fn stop_auto_batch(&self) {
        self.auto_batch_accumulator.close().await;
        let mut tasks = self.auto_batch_tasks.lock().await;
        for task in tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
    }

    async fn detect_brokers(&self, runtime: &ClientRuntime) {
        let interval = Duration::from_millis(self.config.client.detect_interval().max(0) as u64);
        let due_brokers = self
            .fault
            .take_due_for_detection(StdInstant::now(), interval);
        if due_brokers.is_empty() {
            return;
        }
        let topic = self
            .publish_info
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .next()
            .cloned();
        let Some(topic) = topic else {
            return;
        };
        let Ok(snapshot) = runtime.route_snapshot() else {
            return;
        };
        for broker in due_brokers {
            let Some(address) = snapshot.publish_address(&broker) else {
                self.fault.remove(&broker);
                continue;
            };
            let Ok(address) = rocketmq_classic_core::route::broker_vip_channel(
                self.config.client.vip_channel_enabled(),
                address,
            ) else {
                continue;
            };
            let deadline = tokio::time::Instant::now()
                + tokio::time::Duration::from_millis(
                    self.config.client.detect_timeout().max(0) as u64
                );
            if runtime
                .api()
                .probe_broker(&address, &topic, deadline)
                .await
                .is_ok()
            {
                self.fault.mark_reachable(&broker);
            }
        }
    }

    pub(crate) fn cached_publish_info(&self, topic: &str) -> Option<Arc<TopicSnapshot>> {
        self.publish_info
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(topic)
            .cloned()
    }

    pub(crate) fn cache_publish_info(&self, topic: &str, info: Arc<TopicSnapshot>) {
        self.publish_info
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(topic.to_owned(), info);
    }
}

impl ProducerRuntime for Producer {
    fn transaction_processor(
        &self,
    ) -> Option<Arc<dyn rocketmq_classic_remoting::client::RequestProcessor>> {
        self.transaction
            .get()
            .map(crate::transaction::TransactionRuntime::request_processor)
    }

    fn publish_topics(&self) -> BTreeSet<String> {
        self.config
            .topics()
            .unwrap_or_default()
            .iter()
            .cloned()
            .collect()
    }

    fn is_publish_topic_need_update(&self, topic: &str) -> bool {
        self.cached_publish_info(topic)
            .is_none_or(|info| info.publish_queues.is_empty())
    }

    fn update_topic_publish_info(&self, topic: &str, info: Arc<TopicSnapshot>) {
        self.cache_publish_info(topic, info);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalid_group_fails_before_creating_or_registering_runtime() {
        let manager = ClientRuntimeManager::new();
        let config = ProducerConfig::new("bad.group").unwrap();
        let producer = Arc::new(Producer::with_runtime_manager(config, manager.clone()));

        assert!(matches!(
            producer.start().await,
            Err(ProducerError::IllegalGroup)
        ));
        assert_eq!(manager.factory_table().len().unwrap(), 0);
    }

    #[tokio::test]
    async fn invalid_accumulator_settings_fail_before_runtime_creation() {
        let manager = ClientRuntimeManager::new();
        let mut config = ProducerConfig::new("accumulator-producer").unwrap();
        config.set_batch_max_delay_millis(0);
        let producer = Arc::new(Producer::with_runtime_manager(config, manager.clone()));

        assert!(matches!(
            producer.start().await,
            Err(ProducerError::InvalidBatchMaxDelay(0))
        ));
        assert_eq!(manager.factory_table().len().unwrap(), 0);
    }

    #[tokio::test]
    async fn runtime_start_failure_unregisters_the_producer_lease() {
        let manager = ClientRuntimeManager::new();
        let mut config = ProducerConfig::new("start-rollback-producer").unwrap();
        config
            .client_config_mut()
            .set_namesrv_addr(Some(String::new()));
        config.client_config_mut().set_poll_name_server_interval(0);
        let runtime = manager.get_or_create(config.client_config()).await.unwrap();
        let producer = Arc::new(Producer::with_runtime_manager(config, manager));

        assert!(matches!(
            producer.start().await,
            Err(ProducerError::Runtime(
                rocketmq_classic_core::runtime::RuntimeError::InvalidSchedule
            ))
        ));
        assert_eq!(runtime.producer_table().len().unwrap(), 0);
        assert!(matches!(
            producer.started_runtime().await,
            Err(ProducerError::NotStarted)
        ));
    }

    #[tokio::test]
    async fn repeated_start_is_rejected_and_shutdown_is_idempotent() {
        let manager = ClientRuntimeManager::new();
        let mut config = ProducerConfig::new("detector-producer").unwrap();
        config.client_config_mut().set_start_detector_enable(true);
        let producer = Arc::new(Producer::with_runtime_manager(config, manager));

        producer.start().await.unwrap();
        assert!(matches!(
            producer.start().await,
            Err(ProducerError::AlreadyStarted)
        ));
        assert!(producer.detector_task.lock().await.is_some());
        producer.shutdown().await.unwrap();
        assert!(producer.detector_task.lock().await.is_none());
        producer.shutdown().await.unwrap();
        assert!(matches!(
            producer.start().await,
            Err(ProducerError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn shutdown_before_start_keeps_producer_startable() {
        let manager = ClientRuntimeManager::new();
        let config = ProducerConfig::new("shutdown-before-start").unwrap();
        let producer = Arc::new(Producer::with_runtime_manager(config, manager));

        producer.shutdown().await.unwrap();
        assert!(matches!(
            producer.started_runtime().await,
            Err(ProducerError::NotStarted)
        ));
        producer.start().await.unwrap();
        producer.shutdown().await.unwrap();
    }
}
