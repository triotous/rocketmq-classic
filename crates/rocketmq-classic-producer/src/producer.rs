use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, OnceLock, RwLock, Weak},
    time::{Duration, Instant as StdInstant},
};

use rocketmq_classic_core::{
    route::TopicSnapshot,
    runtime::{ClientRuntime, ClientRuntimeManager, GroupLease, ProducerRuntime},
    unique_id::UniqueIdGenerator,
};
use rocketmq_classic_remoting::client::RpcHook;

#[cfg(test)]
use rocketmq_classic_core::api::producer::{SendMode, SendResult};

use crate::{
    ProducerConfig, ProducerError, accumulator::AutoBatchAccumulator, fault::FaultStrategy,
    selector::RoundRobinSelector, validation,
};

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct ScriptedSendAttempt {
    pub(crate) broker: String,
    pub(crate) address: String,
    pub(crate) queue: rocketmq_classic_common::message::MessageQueue,
    pub(crate) body: Vec<u8>,
    pub(crate) topic: String,
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
    namespaced_group: OnceLock<String>,
    manager: ClientRuntimeManager,
    rpc_hook: Option<Arc<dyn RpcHook>>,
    state: tokio::sync::Mutex<State>,
    detector_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    pub(crate) auto_batch_accumulator: AutoBatchAccumulator,
    pub(crate) auto_batch_tasks: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
    pub(crate) self_weak: OnceLock<Weak<Producer>>,
    pub(crate) transaction: OnceLock<Arc<crate::transaction::TransactionRuntime>>,
    pub(crate) trace_dispatcher: OnceLock<Arc<crate::trace::TraceDispatcher>>,
    recall_trace_hook: tokio::sync::Mutex<Option<Arc<dyn RpcHook>>>,
    pub(crate) request_callback_tasks: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
    pub(crate) check_forbidden_hooks: crate::hooks::CheckForbiddenHookList,
    pub(crate) send_message_hooks: crate::hooks::SendMessageHookList,
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
    /// Registers a Java-compatible pre-send forbidden check.
    pub fn register_check_forbidden_hook(&self, hook: Arc<dyn crate::CheckForbiddenHook>) {
        self.check_forbidden_hooks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(hook);
    }

    pub(crate) fn check_forbidden_hook_snapshot(&self) -> Vec<Arc<dyn crate::CheckForbiddenHook>> {
        self.check_forbidden_hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Registers a Java-compatible hook for every outbound producer attempt.
    pub fn register_send_message_hook(&self, hook: Arc<dyn crate::SendMessageHook>) {
        self.send_message_hooks
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(hook);
    }

    pub(crate) fn send_message_hook_snapshot(&self) -> Vec<Arc<dyn crate::SendMessageHook>> {
        self.send_message_hooks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn new(config: ProducerConfig) -> Self {
        Self::new_with_rpc_hook(config, None)
    }

    /// Creates a producer whose remoting requests use the supplied RPC hook.
    pub fn new_with_rpc_hook(config: ProducerConfig, rpc_hook: Option<Arc<dyn RpcHook>>) -> Self {
        let manager = ClientRuntimeManager::shared_default();
        Self::with_runtime_manager_and_hook(config, manager, rpc_hook)
    }

    #[cfg(test)]
    pub(crate) fn with_runtime_manager(
        config: ProducerConfig,
        manager: ClientRuntimeManager,
    ) -> Self {
        Self::with_runtime_manager_and_hook(config, manager, None)
    }

    pub(crate) fn with_runtime_manager_and_hook(
        config: ProducerConfig,
        manager: ClientRuntimeManager,
        rpc_hook: Option<Arc<dyn RpcHook>>,
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
            namespaced_group: OnceLock::new(),
            manager,
            rpc_hook,
            state: tokio::sync::Mutex::new(State::Created),
            detector_task: tokio::sync::Mutex::new(None),
            auto_batch_accumulator: AutoBatchAccumulator::default(),
            auto_batch_tasks: tokio::sync::Mutex::new(Vec::new()),
            self_weak: OnceLock::new(),
            transaction: OnceLock::new(),
            trace_dispatcher: OnceLock::new(),
            recall_trace_hook: tokio::sync::Mutex::new(None),
            request_callback_tasks: tokio::sync::Mutex::new(Vec::new()),
            check_forbidden_hooks: Default::default(),
            send_message_hooks: Default::default(),
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

    /// Returns the message trace dispatcher after trace initialization.
    ///
    /// This corresponds to Java `DefaultMQProducer.getTraceDispatcher()`.
    pub fn trace_dispatcher(&self) -> Option<Arc<crate::TraceDispatcher>> {
        self.trace_dispatcher.get().cloned()
    }

    /// Returns the Java-compatible producer group, namespace-wrapped after start.
    pub fn producer_group(&self) -> &str {
        self.namespaced_group
            .get()
            .map(String::as_str)
            .unwrap_or_else(|| self.config.producer_group())
    }

    pub(crate) async fn started_runtime(&self) -> Result<Arc<ClientRuntime>, ProducerError> {
        self.started_registration()
            .await
            .map(|(runtime, _)| runtime)
    }

    pub(crate) async fn started_registration(
        &self,
    ) -> Result<(Arc<ClientRuntime>, GroupLease), ProducerError> {
        let state = self.state.lock().await;
        match &*state {
            State::Created => Err(ProducerError::NotStarted),
            State::Shutdown => Err(ProducerError::Shutdown),
            State::Started { runtime, lease } => runtime
                .upgrade()
                .map(|runtime| (runtime, lease.clone()))
                .ok_or(ProducerError::Shutdown),
        }
    }

    pub(crate) async fn track_request_callback_task(&self, task: JoinHandle<()>) {
        let state = self.state.lock().await;
        if !matches!(&*state, State::Started { .. }) || task.is_finished() {
            drop(task);
            return;
        }
        let mut tasks = self.request_callback_tasks.lock().await;
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }

    async fn retire_request_callback_tasks(&self) {
        let tasks = {
            let mut tasks = self.request_callback_tasks.lock().await;
            std::mem::take(&mut *tasks)
        };
        // Finished tasks can be joined immediately. An unfinished callback is
        // detached so user code cannot hold producer shutdown open; its task
        // owns the producer until it returns and still delivers its terminal
        // request result.
        for task in tasks {
            if task.is_finished() {
                let _ = task.await;
            }
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
        let mut client_config = self.config.client_config().clone();
        let group = client_config
            .with_namespace(Some(self.config.producer_group()))?
            .unwrap_or_else(|| self.config.producer_group().to_owned());
        let _ = self.namespaced_group.set(group);
        validation::validate_group(self.producer_group())?;
        self.config.accumulator_settings()?;

        let runtime = self
            .manager
            .get_or_create_with_hook_and_tls_config(
                &self.config.client,
                self.rpc_hook.clone(),
                self.config.tls_config().cloned(),
            )
            .await?;
        let owner: Arc<dyn ProducerRuntime> = self.clone();
        let lease = runtime
            .register_producer(self.producer_group(), owner)
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
        self.start_trace_dispatcher(&runtime).await;
        if self.config.client.start_detector_enable() {
            let task = self.spawn_detector_task(Arc::downgrade(&runtime));
            *self.detector_task.lock().await = Some(task);
        }
        Ok(())
    }

    async fn start_trace_dispatcher(&self, runtime: &Arc<ClientRuntime>) {
        if !self.config.client.enable_trace() {
            return;
        }
        let dispatcher = match crate::trace::TraceDispatcher::new(
            &self.config.client,
            self.producer_group(),
            crate::TraceProducerKind::Produce,
            self.config.client.trace_topic(),
            self.manager.clone(),
            self.rpc_hook.clone(),
        ) {
            Ok(dispatcher) => Arc::new(dispatcher),
            Err(error) => {
                tracing::error!(error = %error, "message trace dispatcher initialization failed");
                return;
            }
        };
        if let Err(error) = dispatcher.start().await {
            tracing::error!(error = %error, "message trace dispatcher start failed");
            return;
        }
        self.register_send_message_hook(Arc::new(crate::trace::ProducerSendTraceHook::new(
            dispatcher.clone(),
            self.producer_group(),
            self.config.client.clone(),
        )));
        let _ = self.trace_dispatcher.set(dispatcher);
        if crate::trace::recall_trace_property_enabled() {
            self.register_recall_trace_hook(
                runtime,
                self.trace_dispatcher
                    .get()
                    .expect("dispatcher was just set")
                    .clone(),
            )
            .await;
        }
    }

    pub async fn shutdown(&self) -> Result<(), ProducerError> {
        let mut state = self.state.lock().await;
        if matches!(&*state, State::Created) {
            return Ok(());
        }
        let previous = std::mem::replace(&mut *state, State::Shutdown);
        match previous {
            State::Created | State::Shutdown => {
                self.remove_recall_trace_hook(None).await;
                self.stop_auto_batch().await;
                self.stop_detector_task().await;
                self.retire_request_callback_tasks().await;
                self.shutdown_trace_dispatcher().await;
                Ok(())
            }
            State::Started { runtime, lease } => {
                let Some(runtime_arc) = runtime.upgrade() else {
                    self.remove_recall_trace_hook(None).await;
                    self.stop_auto_batch().await;
                    self.stop_detector_task().await;
                    self.retire_request_callback_tasks().await;
                    self.shutdown_trace_dispatcher().await;
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
                self.remove_recall_trace_hook(Some(&runtime_arc)).await;
                self.stop_auto_batch().await;
                self.stop_detector_task().await;
                self.retire_request_callback_tasks().await;
                self.shutdown_trace_dispatcher().await;
                Ok(())
            }
        }
    }

    async fn shutdown_trace_dispatcher(&self) {
        if let Some(dispatcher) = self.trace_dispatcher.get() {
            if let Err(error) = dispatcher.shutdown().await {
                tracing::warn!(error = %error, "message trace dispatcher shutdown failed");
            }
        }
    }

    async fn remove_recall_trace_hook(&self, runtime: Option<&ClientRuntime>) {
        let hook = self.recall_trace_hook.lock().await.take();
        if let (Some(runtime), Some(hook)) = (runtime, hook.as_ref()) {
            runtime.remoting().remove_rpc_hook(hook);
        }
    }

    async fn register_recall_trace_hook(
        &self,
        runtime: &ClientRuntime,
        dispatcher: Arc<crate::trace::TraceDispatcher>,
    ) {
        let hook: Arc<dyn RpcHook> = Arc::new(crate::trace::RecallMessageTraceHook::new(
            dispatcher,
            self.config.client.clone(),
        ));
        runtime.remoting().add_rpc_hook(hook.clone());
        *self.recall_trace_hook.lock().await = Some(hook);
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
        let mut client_config = self.config.client_config().clone();
        self.config
            .topics()
            .unwrap_or_default()
            .iter()
            .filter_map(|topic| {
                client_config
                    .with_namespace(Some(topic))
                    .ok()
                    .flatten()
                    .or_else(|| Some(topic.clone()))
            })
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

    #[test]
    fn configured_publish_topics_are_namespace_wrapped_once() {
        let mut config = ProducerConfig::new("namespace-publish-topics").unwrap();
        config
            .client_config_mut()
            .set_namespace(Some("tenant".into()));
        config.set_topics(Some(vec![
            "orders".into(),
            "tenant%already".into(),
            "%RETRY%workers".into(),
            "RMQ_SYS_TRACE_TOPIC".into(),
        ]));
        let producer = Producer::with_runtime_manager(config, ClientRuntimeManager::new());

        assert_eq!(
            producer.publish_topics(),
            BTreeSet::from([
                "tenant%orders".to_owned(),
                "tenant%already".to_owned(),
                "%RETRY%tenant%workers".to_owned(),
                "RMQ_SYS_TRACE_TOPIC".to_owned(),
            ])
        );
    }

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

    #[tokio::test]
    async fn enabled_trace_starts_and_stops_its_owned_dispatcher() {
        let manager = ClientRuntimeManager::new();
        let mut config = ProducerConfig::new("trace-lifecycle-producer").unwrap();
        config.client_config_mut().set_enable_trace(true);
        let producer = Arc::new(Producer::with_runtime_manager(config, manager));

        producer.start().await.unwrap();
        assert!(producer.trace_dispatcher().is_some());
        let dispatcher = producer.trace_dispatcher().unwrap();
        let trace_producer = dispatcher.trace_producer().unwrap();
        assert_eq!(producer.send_message_hook_snapshot().len(), 1);
        assert!(
            dispatcher.append(rocketmq_classic_core::trace::TraceContext {
                trace_type: Some(rocketmq_classic_core::trace::TraceType::Recall),
                timestamp_millis: 1,
                region_id: None,
                region_name: Some(String::new()),
                group_name: None,
                cost_time_millis: 0,
                success: true,
                request_id: None,
                context_code: 0,
                access_channel: None,
                trace_beans: Some(vec![rocketmq_classic_core::trace::TraceBean::default()]),
            })
        );
        dispatcher.flush().await.unwrap();
        producer.shutdown().await.unwrap();
        let trace = rocketmq_classic_core::trace::TraceContext {
            trace_type: Some(rocketmq_classic_core::trace::TraceType::Recall),
            timestamp_millis: 1,
            region_id: Some("region-a".into()),
            region_name: Some(String::new()),
            group_name: Some("trace-lifecycle-producer".into()),
            cost_time_millis: 0,
            success: true,
            request_id: None,
            context_code: 0,
            access_channel: None,
            trace_beans: Some(vec![]),
        };
        assert!(!dispatcher.append(trace));
        assert!(Arc::ptr_eq(
            &trace_producer,
            &dispatcher.trace_producer().unwrap()
        ));
    }

    #[tokio::test]
    async fn recall_trace_rpc_hook_is_registered_and_removed_with_its_producer() {
        let manager = ClientRuntimeManager::new();
        let config = ProducerConfig::new("recall-trace-lifecycle").unwrap();
        let runtime = manager.get_or_create(config.client_config()).await.unwrap();
        let producer = Arc::new(Producer::with_runtime_manager(
            config.clone(),
            manager.clone(),
        ));
        let dispatcher = Arc::new(
            crate::trace::TraceDispatcher::new(
                config.client_config(),
                config.producer_group(),
                crate::TraceProducerKind::Produce,
                None,
                manager,
                None,
            )
            .unwrap(),
        );
        let hooks_before = runtime.remoting().get_rpc_hooks().len();

        producer
            .register_recall_trace_hook(&runtime, dispatcher)
            .await;
        assert_eq!(runtime.remoting().get_rpc_hooks().len(), hooks_before + 1);
        producer.remove_recall_trace_hook(Some(&runtime)).await;
        assert_eq!(runtime.remoting().get_rpc_hooks().len(), hooks_before);
    }
}
