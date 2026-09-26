mod config;
mod connection;
mod connection_manager;
mod pending;
mod processor;
pub mod tls;

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering},
    },
};

use rand::seq::SliceRandom;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{self, Instant},
};

use crate::{
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
    command::RemotingCommand,
    error::TransportError,
};

pub use config::{ClientConfig, SocksProxyConfig};
pub use connection::TransportMode;
pub use connection_manager::Address;
pub use processor::RequestProcessor;
pub use tls::TlsConfig;

const OUTBOUND_QUEUE_CAPACITY: usize = 128;
const INBOUND_QUEUE_CAPACITY: usize = 128;
const CALLBACK_QUEUE_CAPACITY: usize = 65535;
const CHANNEL_EVENT_QUEUE_CAPACITY: usize = 10001;

#[derive(Clone)]
struct ProcessorRegistration {
    processor: Arc<dyn RequestProcessor>,
    executor: Option<Arc<dyn RequestExecutor>>,
}

#[derive(Clone)]
struct CallbackTracking {
    send_status: Arc<AtomicBool>,
    channel_established: Arc<AtomicBool>,
    opaque: Arc<AtomicI32>,
    request: Arc<Mutex<RemotingCommand>>,
}

#[derive(Clone, Copy)]
struct InvokeOptions {
    admitted: bool,
    retried_go_away: bool,
    run_rpc_hooks: bool,
    close_socket_on_timeout: bool,
    timeout_millis: std::time::Duration,
}

struct CallbackJob {
    callback: Arc<dyn InvokeCallback>,
    context: InvokeCallbackResult,
    channel_established: Arc<AtomicBool>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

struct PendingEntry {
    generation: u64,
    deadline: Instant,
    address: String,
    request_code: i32,
    response: oneshot::Sender<Result<RemotingCommand, TransportError>>,
}

struct ClientInner {
    connections: Mutex<HashMap<String, Arc<connection::Connection>>>,
    connecting: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    pending: Mutex<HashMap<i32, PendingEntry>>,
    opaque: AtomicI32,
    generation: AtomicU64,
    processors: Mutex<HashMap<i32, ProcessorRegistration>>,
    default_processor: Mutex<Option<ProcessorRegistration>>,
    inbound: mpsc::Sender<processor::InboundRequest>,
    shutdown: tokio::sync::watch::Sender<bool>,
    shutdown_complete: tokio::sync::OnceCell<()>,
    closed: AtomicBool,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    security: TransportMode,
    max_frame_size: usize,
    config: ClientConfig,
    outbound_queue_capacity: usize,
    async_admission: Arc<tokio::sync::Semaphore>,
    oneway_admission: Arc<tokio::sync::Semaphore>,
    callback_sender: mpsc::Sender<CallbackJob>,
    callback_executor: Mutex<Option<Arc<dyn CallbackExecutor>>>,
    rpc_hooks: Mutex<Vec<Arc<dyn RpcHook>>>,
    channel_listener: Mutex<Option<Arc<dyn ChannelEventListener>>>,
    channel_event_sender: mpsc::Sender<(String, ChannelEvent)>,
    request_pipeline: Mutex<Option<Arc<dyn RequestPipeline>>>,
    nameservers: Mutex<Vec<String>>,
    available_nameservers: Mutex<HashSet<String>>,
    chosen_nameserver: Mutex<Option<String>>,
    nameserver_index: AtomicU64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestContext {
    pub remote_addr: String,
    pub connection_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestPipelineError {
    Abort {
        response_code: i32,
        remark: Option<String>,
    },
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RpcHookError {
    Abort {
        response_code: i32,
        remark: Option<String>,
    },
    Failed(String),
}

/// Routes a processor task to an application-owned executor. Returning `Err` rejects the task.
pub trait RequestExecutor: Send + Sync + 'static {
    fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), String>;
}

pub trait RequestPipeline: Send + Sync + 'static {
    fn execute(&self, request: &RemotingCommand) -> Result<(), i32>;

    fn execute_with_context(
        &self,
        _context: &RequestContext,
        request: &RemotingCommand,
    ) -> Result<(), i32> {
        self.execute(request)
    }

    fn execute_detailed(
        &self,
        context: &RequestContext,
        request: &RemotingCommand,
    ) -> Result<(), RequestPipelineError> {
        self.execute_with_context(context, request)
            .map_err(|response_code| RequestPipelineError::Abort {
                response_code,
                remark: Some("request pipeline aborted".into()),
            })
    }
}

impl<F> RequestPipeline for F
where
    F: Fn(&RemotingCommand) -> Result<(), i32> + Send + Sync + 'static,
{
    fn execute(&self, request: &RemotingCommand) -> Result<(), i32> {
        self(request)
    }
}

struct PipedRequestPipeline {
    source: Arc<dyn RequestPipeline>,
    existing: Arc<dyn RequestPipeline>,
}

impl RequestPipeline for PipedRequestPipeline {
    fn execute(&self, request: &RemotingCommand) -> Result<(), i32> {
        self.source.execute(request)?;
        self.existing.execute(request)
    }

    fn execute_with_context(
        &self,
        context: &RequestContext,
        request: &RemotingCommand,
    ) -> Result<(), i32> {
        self.source.execute_with_context(context, request)?;
        self.existing.execute_with_context(context, request)
    }

    fn execute_detailed(
        &self,
        context: &RequestContext,
        request: &RemotingCommand,
    ) -> Result<(), RequestPipelineError> {
        self.source.execute_detailed(context, request)?;
        self.existing.execute_detailed(context, request)
    }
}

pub struct ContextRequestPipeline<F>(pub F);

impl<F> RequestPipeline for ContextRequestPipeline<F>
where
    F: Fn(&RequestContext, &RemotingCommand) -> Result<(), i32> + Send + Sync + 'static,
{
    fn execute(&self, request: &RemotingCommand) -> Result<(), i32> {
        (self.0)(
            &RequestContext {
                remote_addr: String::new(),
                connection_generation: 0,
            },
            request,
        )
    }

    fn execute_with_context(
        &self,
        context: &RequestContext,
        request: &RemotingCommand,
    ) -> Result<(), i32> {
        (self.0)(context, request)
    }
}

/// RPC interception hooks; hook panics are isolated from transport tasks.
pub trait RpcHook: Send + Sync + 'static {
    fn before_request(&self, _remote_addr: &str, _request: &RemotingCommand) {}
    fn after_response(
        &self,
        _remote_addr: &str,
        _request: &RemotingCommand,
        _response: &RemotingCommand,
    ) {
    }

    fn before_request_result(
        &self,
        remote_addr: &str,
        request: &RemotingCommand,
    ) -> Result<(), RpcHookError> {
        self.before_request(remote_addr, request);
        Ok(())
    }

    fn after_response_result(
        &self,
        remote_addr: &str,
        request: &RemotingCommand,
        response: &RemotingCommand,
    ) -> Result<(), RpcHookError> {
        self.after_response(remote_addr, request, response);
        Ok(())
    }

    fn after_response_optional_result(
        &self,
        remote_addr: &str,
        request: &RemotingCommand,
        response: Option<&RemotingCommand>,
    ) -> Result<(), RpcHookError> {
        if let Some(response) = response {
            self.after_response_result(remote_addr, request, response)
        } else {
            Ok(())
        }
    }
}

impl<F> RpcHook for F
where
    F: Fn(&str, &RemotingCommand) + Send + Sync + 'static,
{
    fn before_request(&self, remote_addr: &str, request: &RemotingCommand) {
        self(remote_addr, request);
    }
}

/// Completion data corresponding to Java's `ResponseFuture` callback metadata.
#[derive(Clone, Debug)]
pub struct InvokeCallbackResult {
    pub request: RemotingCommand,
    pub response: Option<RemotingCommand>,
    pub error: Option<TransportError>,
    pub send_request_ok: bool,
    pub timed_out: bool,
    pub elapsed: std::time::Duration,
}

impl InvokeCallbackResult {
    fn from_result(
        request: RemotingCommand,
        result: Result<RemotingCommand, TransportError>,
        elapsed: std::time::Duration,
    ) -> Self {
        match result {
            Ok(response) => Self {
                request,
                response: Some(response),
                error: None,
                send_request_ok: true,
                timed_out: false,
                elapsed,
            },
            Err(error) => Self {
                request,
                response: None,
                send_request_ok: !matches!(
                    error,
                    TransportError::Connect(_)
                        | TransportError::Send(_)
                        | TransportError::Tls(_)
                        | TransportError::Closed
                ),
                timed_out: error == TransportError::Timeout,
                error: Some(error),
                elapsed,
            },
        }
    }

    fn result(&self) -> Result<RemotingCommand, TransportError> {
        match (&self.response, &self.error) {
            (Some(response), _) => Ok(response.clone()),
            (None, Some(error)) => Err(error.clone()),
            (None, None) => Err(TransportError::Closed),
        }
    }
}

/// Connection lifecycle notifications corresponding to Java `ChannelEventListener`.
pub trait InvokeCallback: Send + Sync + 'static {
    fn operation_complete(&self, _result: &Result<RemotingCommand, TransportError>) {}
    fn operation_complete_with_context(&self, context: &InvokeCallbackResult) {
        self.operation_complete(&context.result());
    }
    fn operation_succeed(&self, _response: &RemotingCommand) {}
    fn operation_fail(&self, _error: &TransportError) {}
}

impl<F> InvokeCallback for F
where
    F: Fn(Result<RemotingCommand, TransportError>) + Send + Sync + 'static,
{
    fn operation_complete(&self, result: &Result<RemotingCommand, TransportError>) {
        self(result.clone());
    }
}

#[derive(Clone, Copy)]
enum ChannelEvent {
    Connect,
    Close,
    Exception,
    Idle,
    Active,
}

/// Routes callbacks to an application-owned executor, corresponding to Java's `setCallbackExecutor`.
pub trait CallbackExecutor: Send + Sync + 'static {
    fn execute(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), Box<dyn FnOnce() + Send + 'static>>;
}

pub trait ChannelEventListener: Send + Sync + 'static {
    fn on_connect(&self, _remote_addr: &str) {}
    fn on_close(&self, _remote_addr: &str) {}
    fn on_exception(&self, _remote_addr: &str) {}
    fn on_idle(&self, _remote_addr: &str) {}
    fn on_active(&self, _remote_addr: &str) {}
}

/// Deadline-based RocketMQ remoting client. Construction requires an active Tokio runtime.
#[derive(Clone)]
pub struct RemotingClient {
    inner: Arc<ClientInner>,
}

impl RemotingClient {
    pub fn new(security: TransportMode) -> Self {
        Self::with_config(ClientConfig {
            transport: security,
            ..ClientConfig::default()
        })
        .expect("default client configuration is valid")
    }

    pub fn with_config(config: ClientConfig) -> Result<Self, TransportError> {
        config.validate()?;
        let max_frame_size = DEFAULT_MAX_FRAME_SIZE;
        let (inbound, inbound_rx) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let (callback_sender, callback_receiver) = mpsc::channel(CALLBACK_QUEUE_CAPACITY);
        let (channel_event_sender, channel_event_receiver) =
            mpsc::channel(CHANNEL_EVENT_QUEUE_CAPACITY);
        let inner = Arc::new(ClientInner {
            connections: Mutex::new(HashMap::new()),
            connecting: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            opaque: AtomicI32::new(0),
            generation: AtomicU64::new(1),
            processors: Mutex::new(HashMap::new()),
            default_processor: Mutex::new(None),
            inbound,
            shutdown,
            shutdown_complete: tokio::sync::OnceCell::new(),
            closed: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            security: config.transport.clone(),
            max_frame_size,
            async_admission: Arc::new(tokio::sync::Semaphore::new(config.async_semaphore_value)),
            oneway_admission: Arc::new(tokio::sync::Semaphore::new(config.oneway_semaphore_value)),
            callback_sender,
            callback_executor: Mutex::new(None),
            config,
            outbound_queue_capacity: OUTBOUND_QUEUE_CAPACITY,
            rpc_hooks: Mutex::new(Vec::new()),
            channel_listener: Mutex::new(None),
            channel_event_sender,
            request_pipeline: Mutex::new(None),
            nameservers: Mutex::new(Vec::new()),
            available_nameservers: Mutex::new(HashSet::new()),
            chosen_nameserver: Mutex::new(None),
            nameserver_index: AtomicU64::new(0),
        });
        let processor_tasks = processor::start(
            Arc::downgrade(&inner),
            inbound_rx,
            inner.config.worker_threads,
        );
        for task in processor_tasks {
            inner.spawn(task);
        }
        inner.spawn(start_channel_event_worker(
            Arc::downgrade(&inner),
            channel_event_receiver,
            inner.shutdown.subscribe(),
        ));
        let callback_workers = if inner.config.disable_callback_executor {
            0
        } else if inner.config.callback_executor_threads == 0 {
            4
        } else {
            inner.config.callback_executor_threads
        };
        for task in start_callback_workers(
            callback_receiver,
            inner.shutdown.subscribe(),
            callback_workers,
        ) {
            inner.spawn(task);
        }
        let scheduler = tokio::spawn(timeout_scheduler(
            Arc::downgrade(&inner),
            inner.shutdown.subscribe(),
        ));
        inner.spawn(scheduler);
        if inner.config.scan_available_nameserver {
            let scan = tokio::spawn(scan_available_name_servers(
                Arc::downgrade(&inner),
                inner.shutdown.subscribe(),
            ));
            inner.spawn(scan);
        }
        Ok(Self { inner })
    }

    #[cfg(test)]
    fn with_limits(security: TransportMode, max_frame_size: usize, initial_opaque: i32) -> Self {
        let (inbound, inbound_rx) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let (callback_sender, _callback_receiver) = mpsc::channel(CALLBACK_QUEUE_CAPACITY);
        let (channel_event_sender, _channel_event_receiver) =
            mpsc::channel(CHANNEL_EVENT_QUEUE_CAPACITY);
        let inner = Arc::new(ClientInner {
            connections: Mutex::new(HashMap::new()),
            connecting: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            opaque: AtomicI32::new(initial_opaque),
            generation: AtomicU64::new(1),
            processors: Mutex::new(HashMap::new()),
            default_processor: Mutex::new(None),
            inbound,
            shutdown,
            shutdown_complete: tokio::sync::OnceCell::new(),
            closed: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            security: security.clone(),
            max_frame_size,
            config: ClientConfig {
                transport: security.clone(),
                ..ClientConfig::default()
            },
            outbound_queue_capacity: OUTBOUND_QUEUE_CAPACITY,
            async_admission: Arc::new(tokio::sync::Semaphore::new(65535)),
            oneway_admission: Arc::new(tokio::sync::Semaphore::new(65535)),
            callback_sender,
            callback_executor: Mutex::new(None),
            rpc_hooks: Mutex::new(Vec::new()),
            channel_listener: Mutex::new(None),
            channel_event_sender,
            request_pipeline: Mutex::new(None),
            nameservers: Mutex::new(Vec::new()),
            available_nameservers: Mutex::new(HashSet::new()),
            chosen_nameserver: Mutex::new(None),
            nameserver_index: AtomicU64::new(0),
        });
        let processor_tasks = processor::start(
            Arc::downgrade(&inner),
            inbound_rx,
            inner.config.worker_threads,
        );
        for task in processor_tasks {
            inner.spawn(task);
        }
        let scheduler = tokio::spawn(timeout_scheduler(
            Arc::downgrade(&inner),
            inner.shutdown.subscribe(),
        ));
        inner.spawn(scheduler);
        Self { inner }
    }

    /// Replaces the configured NameServer list and randomizes its order when changed.
    pub fn update_name_server_address_list(&self, mut addresses: Vec<String>) {
        if addresses.is_empty() {
            return;
        }
        let mut nameservers = self.inner.nameservers.lock().unwrap();
        if nameservers.len() == addresses.len()
            && addresses
                .iter()
                .all(|address| nameservers.contains(address))
        {
            return;
        }
        let removed = nameservers
            .iter()
            .filter(|address| !addresses.contains(address))
            .cloned()
            .collect::<HashSet<_>>();
        addresses.shuffle(&mut rand::rng());
        *nameservers = addresses;
        let removed_chosen = {
            let mut chosen = self.inner.chosen_nameserver.lock().unwrap();
            if chosen
                .as_ref()
                .is_some_and(|address| removed.contains(address))
            {
                chosen.take()
            } else {
                None
            }
        };
        if let Some(address) = removed_chosen {
            self.inner.connection_closed_for_address(&address);
        }
    }

    pub fn get_name_server_address_list(&self) -> Vec<String> {
        self.inner.nameservers.lock().unwrap().clone()
    }

    pub fn get_available_name_srv_list(&self) -> Vec<String> {
        self.inner
            .available_nameservers
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }

    /// Selects the next configured NameServer. A successful invoke marks it available;
    /// connection failures remove it from the available set.
    pub async fn invoke_name_server(
        &self,
        command: RemotingCommand,
        deadline: Instant,
    ) -> Result<RemotingCommand, TransportError> {
        let addresses = self.get_name_server_address_list();
        if addresses.is_empty() {
            return Err(TransportError::InvalidConfiguration(
                "no NameServer addresses configured".into(),
            ));
        }
        if Instant::now() >= deadline {
            return Err(TransportError::Timeout);
        }
        let chosen = self.inner.chosen_nameserver.lock().unwrap().clone();
        let start = chosen
            .as_ref()
            .and_then(|address| addresses.iter().position(|candidate| candidate == address))
            .unwrap_or_else(|| {
                self.inner.nameserver_index.fetch_add(1, Ordering::Relaxed) as usize
                    % addresses.len()
            });
        let mut last_error = None;
        for offset in 0..addresses.len() {
            if Instant::now() >= deadline {
                return Err(TransportError::Timeout);
            }
            let address = &addresses[(start + offset) % addresses.len()];
            if !self
                .inner
                .nameservers
                .lock()
                .unwrap()
                .iter()
                .any(|configured| configured == address)
            {
                continue;
            }
            let connection = match self.inner.connection(address, deadline).await {
                Ok(connection) => connection,
                Err(error) => {
                    let nameservers = self.inner.nameservers.lock().unwrap();
                    let replacement_exists = self
                        .inner
                        .connections
                        .lock()
                        .unwrap()
                        .get(address)
                        .is_some_and(|current| !current.is_closed());
                    if nameservers.iter().any(|configured| configured == address)
                        && !replacement_exists
                    {
                        self.inner
                            .available_nameservers
                            .lock()
                            .unwrap()
                            .remove(address);
                    }
                    last_error = Some(error);
                    continue;
                }
            };
            let selected = {
                let nameservers = self.inner.nameservers.lock().unwrap();
                if nameservers.iter().any(|configured| configured == address) {
                    *self.inner.chosen_nameserver.lock().unwrap() = Some(address.clone());
                    true
                } else {
                    false
                }
            };
            if !selected {
                continue;
            }
            match self.invoke(address, command, deadline).await {
                Ok(response) => {
                    let nameservers = self.inner.nameservers.lock().unwrap();
                    let connection_is_current =
                        nameservers.iter().any(|configured| configured == address)
                            && !connection.is_closed()
                            && self
                                .inner
                                .connections
                                .lock()
                                .unwrap()
                                .get(address)
                                .is_some_and(|current| current.generation == connection.generation);
                    if connection_is_current {
                        self.inner
                            .available_nameservers
                            .lock()
                            .unwrap()
                            .insert(address.clone());
                    }
                    drop(nameservers);
                    return Ok(response);
                }
                Err(error) => {
                    let nameservers = self.inner.nameservers.lock().unwrap();
                    let connection_is_current =
                        nameservers.iter().any(|configured| configured == address)
                            && !connection.is_closed()
                            && self
                                .inner
                                .connections
                                .lock()
                                .unwrap()
                                .get(address)
                                .is_some_and(|current| current.generation == connection.generation);
                    if connection_is_current {
                        self.inner
                            .available_nameservers
                            .lock()
                            .unwrap()
                            .remove(address);
                        if matches!(
                            error,
                            TransportError::Connect(_)
                                | TransportError::Send(_)
                                | TransportError::Closed
                                | TransportError::Tls(_)
                        ) {
                            let mut chosen = self.inner.chosen_nameserver.lock().unwrap();
                            if chosen.as_deref() == Some(address.as_str()) {
                                *chosen = None;
                            }
                        }
                    }
                    drop(nameservers);
                    return Err(error);
                }
            }
        }
        Err(last_error.unwrap_or(TransportError::Timeout))
    }

    pub async fn get_and_create_name_server_channel_async(
        &self,
        deadline: Instant,
    ) -> Result<Address, TransportError> {
        let addresses = self.get_name_server_address_list();
        if addresses.is_empty() {
            return Err(TransportError::InvalidConfiguration(
                "no NameServer addresses configured".into(),
            ));
        }
        let chosen = {
            let nameservers = self.inner.nameservers.lock().unwrap();
            let chosen = self.inner.chosen_nameserver.lock().unwrap().clone();
            chosen.filter(|address| {
                nameservers.contains(address)
                    && self
                        .inner
                        .connections
                        .lock()
                        .unwrap()
                        .get(address)
                        .is_some_and(|connection| !connection.is_closed())
            })
        };
        if let Some(chosen) = chosen {
            return Address::parse(&chosen);
        }
        let start =
            self.inner.nameserver_index.fetch_add(1, Ordering::Relaxed) as usize % addresses.len();
        let mut last_error = None;
        for offset in 0..addresses.len() {
            if Instant::now() >= deadline {
                return Err(TransportError::Timeout);
            }
            let address = &addresses[(start + offset) % addresses.len()];
            match self.inner.connection(address, deadline).await {
                Ok(_) => {
                    let nameservers = self.inner.nameservers.lock().unwrap();
                    if nameservers.iter().any(|configured| configured == address) {
                        *self.inner.chosen_nameserver.lock().unwrap() = Some(address.clone());
                        return Address::parse(address);
                    }
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or(TransportError::Timeout))
    }

    pub fn add_rpc_hook(&self, hook: Arc<dyn RpcHook>) {
        let mut hooks = self.inner.rpc_hooks.lock().unwrap();
        if !hooks
            .iter()
            .any(|registered| Arc::ptr_eq(registered, &hook))
        {
            hooks.push(hook);
        }
    }

    pub fn clear_rpc_hooks(&self) {
        self.inner.rpc_hooks.lock().unwrap().clear();
    }

    pub fn set_channel_event_listener(&self, listener: Option<Arc<dyn ChannelEventListener>>) {
        *self.inner.channel_listener.lock().unwrap() = listener;
    }

    pub fn get_channel_event_listener(&self) -> Option<Arc<dyn ChannelEventListener>> {
        self.inner.channel_listener.lock().unwrap().clone()
    }

    pub fn set_callback_executor(&self, executor: Option<Arc<dyn CallbackExecutor>>) {
        *self.inner.callback_executor.lock().unwrap() =
            if self.inner.config.disable_callback_executor {
                None
            } else {
                executor
            };
    }

    pub fn get_callback_executor(&self) -> Option<Arc<dyn CallbackExecutor>> {
        if self.inner.config.disable_callback_executor {
            None
        } else {
            self.inner.callback_executor.lock().unwrap().clone()
        }
    }

    pub fn set_request_pipeline(&self, pipeline: Option<Arc<dyn RequestPipeline>>) {
        *self.inner.request_pipeline.lock().unwrap() = pipeline;
    }

    pub fn pipe_request_pipeline(&self, pipeline: Arc<dyn RequestPipeline>) {
        let mut existing = self.inner.request_pipeline.lock().unwrap();
        *existing = Some(match existing.take() {
            Some(existing) => Arc::new(PipedRequestPipeline {
                source: pipeline,
                existing,
            }),
            None => pipeline,
        });
    }

    /// Returns whether the connection to `addr` currently has outbound queue capacity.
    /// Unknown addresses are considered writable, matching the Java client contract.
    pub fn is_channel_writable(&self, addr: &str) -> bool {
        let Ok(address) = Address::parse(addr) else {
            return true;
        };
        self.inner
            .connections
            .lock()
            .unwrap()
            .get(address.as_str())
            .filter(|connection| !connection.is_closed())
            .is_none_or(|connection| connection.is_writable())
    }

    /// Opens or reuses a connection and reports whether it is active before `deadline`.
    pub async fn is_address_reachable(&self, addr: &str, deadline: Instant) -> bool {
        self.inner.connection(addr, deadline).await.is_ok()
    }

    /// Closes any currently cached channels for the supplied addresses.
    pub fn close_channels(&self, addresses: &[String]) {
        for address in addresses {
            let key = Address::parse(address)
                .map(|address| address.as_str().to_owned())
                .unwrap_or_else(|_| address.clone());
            self.inner.interrupt_pull_requests(&key);
            self.inner.connection_closed_for_address(&key);
        }
    }

    pub fn invoke_async(
        &self,
        addr: impl Into<String>,
        command: RemotingCommand,
        deadline: Instant,
        callback: Arc<dyn InvokeCallback>,
    ) -> Result<(), TransportError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        if Instant::now() >= deadline {
            let callback = CallbackJob {
                callback,
                context: InvokeCallbackResult {
                    request: command,
                    response: None,
                    error: Some(TransportError::Timeout),
                    send_request_ok: false,
                    timed_out: true,
                    elapsed: std::time::Duration::ZERO,
                },
                channel_established: Arc::new(AtomicBool::new(true)),
                _permit: None,
            };
            self.inner.dispatch_callback(callback);
            return Ok(());
        }
        let client = self.clone();
        let addr = addr.into();
        let started = Instant::now();
        let callback_request_state = Arc::new(Mutex::new(command.clone()));
        let send_status = Arc::new(AtomicBool::new(false));
        let channel_established = Arc::new(AtomicBool::new(false));
        let callback_opaque = Arc::new(AtomicI32::new(command.opaque));
        let task = tokio::spawn(async move {
            let (result, permit) = match client.inner.connection(&addr, deadline).await {
                Ok(_) => {
                    channel_established.store(true, Ordering::Release);
                    for hook in client.inner.rpc_hooks.lock().unwrap().clone() {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            hook.before_request(&addr, &command)
                        }));
                    }
                    match client
                        .inner
                        .acquire_admission(client.inner.async_admission.clone(), deadline)
                        .await
                    {
                        Ok(permit) => {
                            let mut admission = Some(permit);
                            let result = client
                                .invoke_inner(
                                    &addr,
                                    command,
                                    deadline,
                                    InvokeOptions {
                                        admitted: true,
                                        retried_go_away: false,
                                        run_rpc_hooks: true,
                                        close_socket_on_timeout: false,
                                        timeout_millis: std::time::Duration::ZERO,
                                    },
                                    Some(CallbackTracking {
                                        send_status: send_status.clone(),
                                        channel_established: channel_established.clone(),
                                        opaque: callback_opaque.clone(),
                                        request: callback_request_state.clone(),
                                    }),
                                    &mut admission,
                                )
                                .await;
                            (result, admission)
                        }
                        Err(error) => (Err(error), None),
                    }
                }
                Err(error) => (Err(error), None),
            };
            let mut context = InvokeCallbackResult::from_result(
                callback_request_state.lock().unwrap().clone(),
                result,
                started.elapsed(),
            );
            context.send_request_ok = send_status.load(Ordering::Acquire)
                || (channel_established.load(Ordering::Acquire)
                    && context.error == Some(TransportError::Timeout));
            let job = CallbackJob {
                callback: callback.clone(),
                context,
                channel_established: channel_established.clone(),
                _permit: permit,
            };
            client.inner.dispatch_callback(job);
        });
        self.inner.spawn(task);
        Ok(())
    }

    pub fn register_processor(&self, code: i32, processor: Arc<dyn RequestProcessor>) {
        self.register_processor_with_executor(code, processor, None);
    }

    pub fn register_processor_with_executor(
        &self,
        code: i32,
        processor: Arc<dyn RequestProcessor>,
        executor: Option<Arc<dyn RequestExecutor>>,
    ) {
        self.inner.processors.lock().unwrap().insert(
            code,
            ProcessorRegistration {
                processor,
                executor,
            },
        );
    }

    /// Registers the fallback processor used when no request-code match exists.
    pub fn register_default_processor(&self, processor: Option<Arc<dyn RequestProcessor>>) {
        self.register_default_processor_with_executor(processor, None);
    }

    pub fn register_default_processor_with_executor(
        &self,
        processor: Option<Arc<dyn RequestProcessor>>,
        executor: Option<Arc<dyn RequestExecutor>>,
    ) {
        *self.inner.default_processor.lock().unwrap() =
            processor.map(|processor| ProcessorRegistration {
                processor,
                executor,
            });
    }

    /// Sends one command and completes with the matching opaque response.
    pub async fn invoke(
        &self,
        addr: &str,
        command: RemotingCommand,
        deadline: Instant,
    ) -> Result<RemotingCommand, TransportError> {
        let mut admission = None;
        self.invoke_inner(
            addr,
            command,
            deadline,
            InvokeOptions {
                admitted: false,
                retried_go_away: false,
                run_rpc_hooks: true,
                close_socket_on_timeout: false,
                timeout_millis: std::time::Duration::ZERO,
            },
            None,
            &mut admission,
        )
        .await
    }

    pub async fn invoke_sync(
        &self,
        addr: &str,
        command: RemotingCommand,
        timeout: std::time::Duration,
    ) -> Result<RemotingCommand, TransportError> {
        let mut admission = None;
        self.invoke_inner(
            addr,
            command,
            Instant::now() + timeout,
            InvokeOptions {
                admitted: false,
                retried_go_away: false,
                run_rpc_hooks: false,
                close_socket_on_timeout: true,
                timeout_millis: timeout,
            },
            None,
            &mut admission,
        )
        .await
    }

    async fn invoke_inner(
        &self,
        addr: &str,
        mut command: RemotingCommand,
        deadline: Instant,
        options: InvokeOptions,
        callback: Option<CallbackTracking>,
        admission: &mut Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<RemotingCommand, TransportError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        if Instant::now() >= deadline {
            return Err(TransportError::Timeout);
        }
        let request_for_hooks = command.clone();
        let connection = self.inner.connection(addr, deadline).await?;
        if let Some(callback) = &callback {
            callback.channel_established.store(true, Ordering::Release);
        }
        let timeout_budget = deadline.saturating_duration_since(Instant::now());
        if Instant::now() >= deadline {
            return Err(TransportError::Timeout);
        }
        if options.run_rpc_hooks && !options.retried_go_away && !options.admitted {
            for hook in self.inner.rpc_hooks.lock().unwrap().clone() {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    hook.before_request(addr, &command)
                }));
            }
        }
        if !options.admitted {
            *admission = Some(
                self.inner
                    .acquire_admission(self.inner.async_admission.clone(), deadline)
                    .await?,
            );
        }
        let pending_deadline = if callback.is_some() {
            deadline + std::time::Duration::from_secs(1)
        } else {
            deadline
        };
        let (opaque, receiver) = self.inner.insert_pending(
            connection.generation,
            pending_deadline,
            connection.address().to_owned(),
            command.code,
        )?;
        command.opaque = opaque;
        if let Some(callback) = &callback {
            callback.opaque.store(opaque, Ordering::Release);
            if !options.retried_go_away {
                callback.request.lock().unwrap().opaque = opaque;
            }
        }
        let mut pending =
            pending::PendingGuard::new(Arc::downgrade(&self.inner), opaque, connection.generation);
        let frame = frame::encode(&command, self.inner.max_frame_size)?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        match time::timeout(remaining, connection.send(frame, deadline)).await {
            Ok(Ok(())) => {
                if let Some(callback) = &callback {
                    callback.send_status.store(true, Ordering::Release);
                }
            }
            Ok(Err(error)) => {
                self.inner.cancel_pending(opaque, connection.generation);
                pending.disarm();
                return Err(error);
            }
            Err(_) => {
                self.inner.cancel_pending(opaque, connection.generation);
                pending.disarm();
                return Err(TransportError::Timeout);
            }
        }
        let response_deadline = if callback.is_some() {
            deadline + std::time::Duration::from_secs(1)
        } else {
            deadline
        };
        let response_wait = response_deadline.saturating_duration_since(Instant::now());
        match time::timeout(response_wait, receiver).await {
            Ok(Ok(Ok(response))) => {
                pending.disarm();
                if response.code == rocketmq_classic_protocol::codes::response::GO_AWAY {
                    if self.inner.config.enable_reconnect_for_go_away && !options.retried_go_away {
                        self.inner.connection_closed(connection.generation, None);
                        let retry_request = RemotingCommand {
                            code: request_for_hooks.code,
                            language: rocketmq_classic_protocol::codes::LanguageCode::Java,
                            ext_fields: request_for_hooks.ext_fields.clone(),
                            body: request_for_hooks.body.clone(),
                            ..RemotingCommand::default()
                        };
                        admission.take();
                        let mut retry = Box::pin(self.invoke_inner(
                            addr,
                            retry_request.clone(),
                            deadline,
                            InvokeOptions {
                                admitted: false,
                                retried_go_away: true,
                                run_rpc_hooks: false,
                                close_socket_on_timeout: options.close_socket_on_timeout,
                                timeout_millis: options.timeout_millis,
                            },
                            callback.clone(),
                            admission,
                        ));
                        let result = retry.as_mut().await;
                        if let Ok(response) = &result {
                            for hook in self.inner.rpc_hooks.lock().unwrap().clone() {
                                let _ =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        hook.after_response(addr, &request_for_hooks, response)
                                    }));
                            }
                            if let Some(callback) = &callback {
                                let mut request = retry_request;
                                request.opaque = callback.opaque.load(Ordering::Acquire);
                                *callback.request.lock().unwrap() = request;
                            }
                        }
                        return result;
                    }
                    return Err(TransportError::GoAway);
                }
                if options.run_rpc_hooks && !options.retried_go_away {
                    for hook in self.inner.rpc_hooks.lock().unwrap().clone() {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            hook.after_response(addr, &request_for_hooks, &response)
                        }));
                    }
                }
                Ok(response)
            }
            Ok(Ok(Err(error))) => {
                pending.disarm();
                Err(error)
            }
            Ok(Err(_)) => {
                pending.disarm();
                if Instant::now() >= deadline {
                    Err(TransportError::Timeout)
                } else {
                    Err(TransportError::Closed)
                }
            }
            Err(_) => {
                self.inner.cancel_pending(opaque, connection.generation);
                if options.close_socket_on_timeout
                    && self.inner.config.close_socket_if_timeout
                    && (timeout_budget > std::time::Duration::from_millis(100)
                        || timeout_budget > options.timeout_millis / 4)
                {
                    connection.close();
                }
                pending.disarm();
                Err(TransportError::Timeout)
            }
        }
    }

    /// Starts connection establishment at call time, then schedules a managed one-way operation
    /// and returns a ready result future without waiting for connection completion, admission, or
    /// socket write. Tokio may not poll the connection task before this method returns. Completion
    /// does not imply Broker receipt or storage; post-return transport failures are not observable.
    pub fn invoke_oneway(
        &self,
        addr: &str,
        mut command: RemotingCommand,
        deadline: Instant,
    ) -> std::future::Ready<Result<(), TransportError>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return std::future::ready(Err(TransportError::Shutdown));
        }
        if Instant::now() >= deadline {
            return std::future::ready(Err(TransportError::Timeout));
        }
        let client = self.clone();
        let addr = addr.to_owned();
        let (connection_tx, connection_rx) = tokio::sync::oneshot::channel();
        let connection_client = client.clone();
        let connection_addr = addr.clone();
        let connection_task = tokio::spawn(async move {
            let result = connection_client
                .inner
                .connection(&connection_addr, deadline)
                .await;
            let _ = connection_tx.send(result);
        });
        self.inner.spawn(connection_task);
        let task = tokio::spawn(async move {
            let Ok(Ok(connection)) = connection_rx.await else {
                return;
            };
            for hook in client.inner.rpc_hooks.lock().unwrap().clone() {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    hook.before_request(&addr, &command)
                }));
            }
            command.mark_oneway();
            let Ok(_admission) = client
                .inner
                .acquire_admission(client.inner.oneway_admission.clone(), deadline)
                .await
            else {
                return;
            };
            let Ok(frame) = frame::encode(&command, client.inner.max_frame_size) else {
                return;
            };
            let _ = connection.send(frame, deadline).await;
        });
        self.inner.spawn(task);
        std::future::ready(Ok(()))
    }

    /// Closes connections, fails pending calls, and joins all client-owned tasks.
    pub async fn shutdown(&self) {
        self.inner
            .shutdown_complete
            .get_or_init(|| async {
                self.inner.shutdown();
                let connections = {
                    let mut table = self.inner.connections.lock().unwrap();
                    table
                        .drain()
                        .map(|(_, connection)| connection)
                        .collect::<Vec<_>>()
                };
                for connection in &connections {
                    connection.close();
                }
                self.inner.fail_all_pending();
                let tasks = std::mem::take(&mut *self.inner.tasks.lock().unwrap());
                for task in tasks {
                    let _ = task.await;
                }
                for connection in connections {
                    connection.join().await;
                }
            })
            .await;
    }
}

impl ClientInner {
    async fn acquire_admission(
        &self,
        semaphore: Arc<tokio::sync::Semaphore>,
        deadline: Instant,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, TransportError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        let mut shutdown = self.shutdown.subscribe();
        tokio::select! {
            permit = time::timeout_at(deadline, semaphore.acquire_owned()) => permit
                .map_err(|_| TransportError::Timeout)?
                .map_err(|_| TransportError::Shutdown),
            changed = shutdown.changed() => {
                let _ = changed;
                Err(TransportError::Shutdown)
            }
        }
    }

    fn notify_channel(&self, address: &str, event: ChannelEvent) {
        if self.channel_listener.lock().unwrap().is_some() {
            let _ = self
                .channel_event_sender
                .try_send((address.to_owned(), event));
        }
    }

    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        let _ = self.shutdown.send(true);
    }

    fn insert_pending(
        &self,
        generation: u64,
        deadline: Instant,
        address: String,
        request_code: i32,
    ) -> Result<
        (
            i32,
            oneshot::Receiver<Result<RemotingCommand, TransportError>>,
        ),
        TransportError,
    > {
        if deadline <= Instant::now() {
            return Err(TransportError::Timeout);
        }
        let (sender, receiver) = oneshot::channel();
        let mut pending = self.pending.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        for _ in 0..=pending.len() {
            let opaque = self.opaque.fetch_add(1, Ordering::Relaxed);
            if let std::collections::hash_map::Entry::Vacant(entry) = pending.entry(opaque) {
                entry.insert(PendingEntry {
                    generation,
                    deadline,
                    address,
                    request_code,
                    response: sender,
                });
                return Ok((opaque, receiver));
            }
        }
        Err(TransportError::OpaqueExhausted)
    }

    fn cancel_pending(&self, opaque: i32, generation: u64) {
        self.finish_pending(opaque, generation, Err(TransportError::Closed));
    }

    fn interrupt_pull_requests(&self, address: &str) {
        let requests = self
            .pending
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(opaque, entry)| {
                (entry.address == address
                    && matches!(
                        entry.request_code,
                        rocketmq_classic_protocol::codes::request::PULL_MESSAGE
                            | rocketmq_classic_protocol::codes::request::LITE_PULL_MESSAGE
                    ))
                .then_some((*opaque, entry.generation))
            })
            .collect::<Vec<_>>();
        for (opaque, generation) in requests {
            self.finish_pending(opaque, generation, Err(TransportError::Interrupted));
        }
    }

    fn expire_pending(&self, opaque: i32, generation: u64) {
        self.finish_pending(opaque, generation, Err(TransportError::Timeout));
    }

    fn complete_response(&self, generation: u64, command: RemotingCommand) {
        self.finish_pending(command.opaque, generation, Ok(command));
    }

    fn finish_pending(
        &self,
        opaque: i32,
        generation: u64,
        result: Result<RemotingCommand, TransportError>,
    ) -> bool {
        let mut pending = self.pending.lock().unwrap();
        if pending
            .get(&opaque)
            .is_some_and(|entry| entry.generation == generation)
        {
            if let Some(entry) = pending.remove(&opaque) {
                let result = if entry.deadline <= Instant::now() && result.is_ok() {
                    Err(TransportError::Timeout)
                } else {
                    result
                };
                let _ = entry.response.send(result);
                return true;
            }
        }
        false
    }

    fn fail_generation(&self, generation: u64, error: TransportError) {
        let ids = {
            let pending = self.pending.lock().unwrap();
            pending
                .iter()
                .filter_map(|(id, item)| (item.generation == generation).then_some(*id))
                .collect::<Vec<_>>()
        };
        for id in ids {
            self.finish_pending(id, generation, Err(error.clone()));
        }
    }

    fn fail_all_pending(&self) {
        let pending = self
            .pending
            .lock()
            .unwrap()
            .iter()
            .map(|(opaque, entry)| (*opaque, entry.generation))
            .collect::<Vec<_>>();
        for (opaque, generation) in pending {
            self.finish_pending(opaque, generation, Err(TransportError::Shutdown));
        }
    }

    fn send_overload_response(self: &Arc<Self>, generation: u64, opaque: i32) {
        let client = self.clone();
        self.spawn(tokio::spawn(async move {
            let connection = client
                .connections
                .lock()
                .unwrap()
                .values()
                .find(|connection| connection.generation == generation)
                .cloned();
            let Some(connection) = connection else {
                return;
            };
            let mut response = RemotingCommand {
                code: rocketmq_classic_protocol::codes::response::SYSTEM_BUSY,
                remark: Some("[OVERLOAD]system busy, start flow control for a while".into()),
                opaque,
                ..RemotingCommand::default()
            };
            response.mark_response();
            if let Ok(bytes) = frame::encode(&response, client.max_frame_size) {
                let _ =
                    connection.try_send(bytes, Instant::now() + std::time::Duration::from_secs(30));
            }
        }));
    }

    fn dispatch_callback(&self, job: CallbackJob) {
        if self.config.disable_callback_executor {
            run_callback(job);
            return;
        }
        if let Some(executor) = self.callback_executor.lock().unwrap().clone() {
            let callback = Arc::new(Mutex::new(Some(
                Box::new(move || run_callback(job)) as Box<dyn FnOnce() + Send + 'static>
            )));
            let callback_for_task = callback.clone();
            let task: Box<dyn FnOnce() + Send + 'static> = Box::new(move || {
                if let Some(callback) = callback_for_task.lock().unwrap().take() {
                    callback();
                }
            });
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| executor.execute(task)));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(_rejected)) => {
                    if let Some(callback) = callback.lock().unwrap().take() {
                        callback();
                    }
                }
                Err(_) => {
                    if let Some(callback) = callback.lock().unwrap().take() {
                        callback();
                    }
                }
            }
            return;
        }
        if let Err(error) = self.callback_sender.try_send(job) {
            run_callback(error.into_inner());
        }
    }

    fn spawn(&self, task: JoinHandle<()>) {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        if self.closed.load(Ordering::Acquire) {
            task.abort();
        } else {
            tasks.push(task);
        }
    }
}

fn run_callback(job: CallbackJob) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let connect_failed = !job.channel_established.load(Ordering::Acquire);
        if !connect_failed {
            job.callback.operation_complete_with_context(&job.context);
        }
        match (&job.context.response, &job.context.error) {
            (Some(response), _) => job.callback.operation_succeed(response),
            (None, Some(error)) => job.callback.operation_fail(error),
            (None, None) => job.callback.operation_fail(&TransportError::Closed),
        }
    }));
}

fn start_channel_event_worker(
    inner: Weak<ClientInner>,
    mut receiver: mpsc::Receiver<(String, ChannelEvent)>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let event = tokio::select! {
                event = receiver.recv() => event,
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() { return }
                    continue;
                },
            };
            let Some((address, event)) = event else {
                return;
            };
            let Some(client) = inner.upgrade() else {
                return;
            };
            let listener = client.channel_listener.lock().unwrap().clone();
            let Some(listener) = listener else { continue };
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match event {
                ChannelEvent::Connect => listener.on_connect(&address),
                ChannelEvent::Close => listener.on_close(&address),
                ChannelEvent::Exception => listener.on_exception(&address),
                ChannelEvent::Idle => listener.on_idle(&address),
                ChannelEvent::Active => listener.on_active(&address),
            }));
        }
    })
}

fn start_callback_workers(
    receiver: mpsc::Receiver<CallbackJob>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    worker_count: usize,
) -> Vec<JoinHandle<()>> {
    let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
    (0..worker_count)
        .map(|_| {
            let receiver = receiver.clone();
            let mut shutdown = shutdown.clone();
            tokio::spawn(async move {
                loop {
                    let job = {
                        let mut receiver = receiver.lock().await;
                        tokio::select! {
                            job = receiver.recv() => job,
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    let queued = std::iter::from_fn(|| receiver.try_recv().ok()).collect::<Vec<_>>();
                                    drop(receiver);
                                    for job in queued {
                                        let _ = tokio::task::spawn_blocking(move || run_callback(job)).await;
                                    }
                                    return;
                                }
                                continue;
                            }
                        }
                    };
                    let Some(job) = job else { return };
                    let _ = tokio::task::spawn_blocking(move || run_callback(job)).await;
                }
            })
        })
        .collect()
}

async fn scan_available_name_servers(
    inner: Weak<ClientInner>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let interval_duration = inner
        .upgrade()
        .map(|inner| inner.config.connect_timeout)
        .unwrap_or_else(|| std::time::Duration::from_secs(3));
    let mut interval = time::interval(interval_duration);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut scans = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let Some(inner) = inner.upgrade() else { break };
                let configured = inner.nameservers.lock().unwrap().clone();
                inner
                    .available_nameservers
                    .lock()
                    .unwrap()
                    .retain(|address| configured.contains(address));
                for address in configured {
                    let inner = inner.clone();
                    scans.spawn(async move {
                        let deadline = Instant::now() + inner.config.connect_timeout;
                        let connection = inner.connection(&address, deadline).await.ok();
                        (address, connection)
                    });
                }
            }
            result = scans.join_next(), if !scans.is_empty() => {
                if let Some(Ok((address, connection))) = result {
                    if let Some(inner) = inner.upgrade() {
                        if inner.closed.load(Ordering::Acquire) {
                            continue;
                        }
                        let address_is_configured = inner
                            .nameservers
                            .lock()
                            .unwrap()
                            .iter()
                            .any(|configured| configured == &address);
                        if !address_is_configured {
                            continue;
                        }
                        let current_generation = inner
                            .connections
                            .lock()
                            .unwrap()
                            .get(&address)
                            .filter(|current| !current.is_closed())
                            .map(|current| current.generation);
                        let probe_generation = connection
                            .filter(|connection| !connection.is_closed())
                            .map(|connection| connection.generation);
                        let nameservers = inner.nameservers.lock().unwrap();
                        if nameservers.iter().any(|configured| configured == &address) {
                            match name_server_probe_availability(probe_generation, current_generation) {
                                Some(true) => {
                                    inner.available_nameservers.lock().unwrap().insert(address);
                                }
                                Some(false) => {
                                    inner.available_nameservers.lock().unwrap().remove(&address);
                                }
                                None => {}
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
            result = shutdown.changed() => if result.is_err() || *shutdown.borrow() { break },
        }
    }
    scans.shutdown().await;
}

fn name_server_probe_availability(
    probe_generation: Option<u64>,
    current_generation: Option<u64>,
) -> Option<bool> {
    match (probe_generation, current_generation) {
        (Some(probe), Some(current)) if probe == current => Some(true),
        (_, Some(_)) => None,
        (_, None) => Some(false),
    }
}

async fn timeout_scheduler(
    inner: Weak<ClientInner>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let Some(inner) = inner.upgrade() else { return };
                if !inner.config.max_idle_time.is_zero() {
                    let idle_connections = inner
                        .connections
                        .lock()
                        .unwrap()
                        .values()
                        .filter(|connection| connection.idle_for() >= inner.config.max_idle_time)
                        .cloned()
                        .collect::<Vec<_>>();
                    for connection in idle_connections {
                        inner.notify_channel(connection.address(), ChannelEvent::Idle);
                        connection.close();
                    }
                }
                let now = Instant::now();
                let expired = {
                    let pending = inner.pending.lock().unwrap();
                    pending.iter().filter_map(|(opaque, entry)| (entry.deadline <= now).then_some((*opaque, entry.generation))).collect::<Vec<_>>()
                };
                for (opaque, generation) in expired {
                    inner.expire_pending(opaque, generation);
                }
            }
            result = shutdown.changed() => if result.is_err() || *shutdown.borrow() { return },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_command() -> RemotingCommand {
        RemotingCommand::default()
    }

    #[test]
    fn stale_name_server_probe_does_not_override_current_generation_availability() {
        assert_eq!(name_server_probe_availability(Some(7), Some(7)), Some(true));
        assert_eq!(name_server_probe_availability(Some(7), Some(8)), None);
        assert_eq!(name_server_probe_availability(None, Some(8)), None);
        assert_eq!(name_server_probe_availability(Some(7), None), Some(false));
        assert_eq!(name_server_probe_availability(None, None), Some(false));
    }

    #[tokio::test]
    async fn pending_terminal_race_and_opaque_wrap_never_replace_live_entries() {
        let client =
            RemotingClient::with_limits(TransportMode::Plain, DEFAULT_MAX_FRAME_SIZE, i32::MAX);
        let generation = 9;
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let (first_id, first_rx) = client
            .inner
            .insert_pending(generation, deadline, "127.0.0.1:1".into(), 0)
            .unwrap();
        let (second_id, second_rx) = client
            .inner
            .insert_pending(generation, deadline, "127.0.0.1:1".into(), 0)
            .unwrap();
        assert_eq!(first_id, i32::MAX);
        assert_eq!(second_id, i32::MIN);

        let (barrier_tx, _) = tokio::sync::broadcast::channel(1);
        let mut response_barrier = barrier_tx.subscribe();
        let mut timeout_barrier = barrier_tx.subscribe();
        let response_inner = client.inner.clone();
        let response = tokio::spawn(async move {
            let _ = response_barrier.recv().await;
            response_inner.finish_pending(first_id, generation, Ok(test_command()))
        });
        let timeout_inner = client.inner.clone();
        let timeout = tokio::spawn(async move {
            let _ = timeout_barrier.recv().await;
            timeout_inner.finish_pending(first_id, generation, Err(TransportError::Timeout))
        });
        barrier_tx.send(()).unwrap();
        let response_won = response.await.unwrap();
        let timeout_won = timeout.await.unwrap();
        assert_ne!(response_won, timeout_won);
        assert_eq!(first_rx.await.is_ok(), response_won);
        assert_eq!(client.inner.pending.lock().unwrap().len(), 1);
        client.inner.cancel_pending(second_id, generation);
        assert!(matches!(second_rx.await, Ok(Err(TransportError::Closed))));
        assert_eq!(client.inner.pending.lock().unwrap().len(), 0);
        client.shutdown().await;
        assert!(client.inner.tasks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stale_generation_close_cannot_evict_replacement() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (_first, _) = listener.accept().await.unwrap();
            let (_replacement, _) = listener.accept().await.unwrap();
        });
        let client = RemotingClient::new(TransportMode::Plain);
        let first = client
            .inner
            .connection(
                &address.to_string(),
                Instant::now() + std::time::Duration::from_secs(1),
            )
            .await
            .unwrap();
        let stale_generation = first.generation;
        first.close();
        tokio::time::timeout(std::time::Duration::from_secs(2), first.join())
            .await
            .expect("first connection did not join");
        let replacement = client
            .inner
            .connection(
                &address.to_string(),
                Instant::now() + std::time::Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_ne!(replacement.generation, stale_generation);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            client
                .inner
                .connection_closed(stale_generation, Some(TransportError::Closed));
        })
        .await
        .expect("stale close handler blocked");
        assert!(!replacement.is_closed());
        assert_eq!(
            client
                .inner
                .connections
                .lock()
                .unwrap()
                .get(&address.to_string())
                .unwrap()
                .generation,
            replacement.generation
        );
        peer.await.unwrap();
        client.shutdown().await;
    }

    #[tokio::test]
    async fn cancelled_invoke_guard_removes_only_its_pending_generation() {
        let client = RemotingClient::new(TransportMode::Plain);
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let (opaque, receiver) = client
            .inner
            .insert_pending(5, deadline, "127.0.0.1:1".into(), 0)
            .unwrap();
        let guard = pending::PendingGuard::new(Arc::downgrade(&client.inner), opaque, 5);
        assert_eq!(client.inner.pending.lock().unwrap().len(), 1);
        drop(guard);
        assert_eq!(client.inner.pending.lock().unwrap().len(), 0);
        assert!(matches!(receiver.await, Ok(Err(TransportError::Closed))));
        client.shutdown().await;
    }

    #[tokio::test]
    async fn response_timeout_and_shutdown_race_has_one_terminal_completion() {
        let client = RemotingClient::new(TransportMode::Plain);
        let generation = 21;
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let (opaque, receiver) = client
            .inner
            .insert_pending(generation, deadline, "127.0.0.1:1".into(), 0)
            .unwrap();
        let guard = pending::PendingGuard::new(Arc::downgrade(&client.inner), opaque, generation);
        let barrier = Arc::new(tokio::sync::Barrier::new(5));

        let response_inner = client.inner.clone();
        let response_barrier = barrier.clone();
        let response = tokio::spawn(async move {
            response_barrier.wait().await;
            response_inner.finish_pending(opaque, generation, Ok(test_command()))
        });
        let timeout_inner = client.inner.clone();
        let timeout_barrier = barrier.clone();
        let timeout = tokio::spawn(async move {
            timeout_barrier.wait().await;
            timeout_inner.finish_pending(opaque, generation, Err(TransportError::Timeout))
        });
        let cancel_inner = client.inner.clone();
        let cancel_barrier = barrier.clone();
        let cancel = tokio::spawn(async move {
            cancel_barrier.wait().await;
            cancel_inner.finish_pending(opaque, generation, Err(TransportError::Closed))
        });
        let shutdown_inner = client.inner.clone();
        let shutdown_barrier = barrier.clone();
        let shutdown = tokio::spawn(async move {
            shutdown_barrier.wait().await;
            shutdown_inner.fail_all_pending();
        });
        barrier.wait().await;
        let response_won = response.await.unwrap();
        let timeout_won = timeout.await.unwrap();
        let cancel_won = cancel.await.unwrap();
        shutdown.await.unwrap();
        assert!(
            usize::from(response_won) + usize::from(timeout_won) + usize::from(cancel_won) <= 1
        );
        assert!(matches!(receiver.await, Ok(Ok(_)) | Ok(Err(_))));
        assert_eq!(client.inner.pending.lock().unwrap().len(), 0);
        drop(guard);
        client.shutdown().await;
    }

    #[tokio::test]
    async fn write_watermarks_follow_queued_bytes_with_hysteresis() {
        let (client_io, mut peer_io) = tokio::io::duplex(64);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            32,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            OUTBOUND_QUEUE_CAPACITY,
            Some((8, 3)),
            "test:0".into(),
        );
        let deadline = Instant::now() + std::time::Duration::from_secs(2);
        connection.try_send(vec![0; 8], deadline).unwrap();
        assert!(!connection.is_writable());
        let mut buffer = [0; 8];
        tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut buffer)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !connection.watermark_is_writable() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        connection.close();
        connection.join().await;
    }

    #[tokio::test]
    async fn saturated_writer_obeys_deadline_and_joins() {
        let (client_io, _peer_io) = tokio::io::duplex(1);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            31,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            OUTBOUND_QUEUE_CAPACITY,
            None,
            "test:0".into(),
        );
        let deadline = Instant::now() + std::time::Duration::from_millis(100);
        for _ in 0..super::OUTBOUND_QUEUE_CAPACITY {
            connection.try_send(vec![0; 1024], deadline).unwrap();
        }
        assert!(matches!(
            connection.try_send(vec![0], deadline),
            Err(TransportError::InboundOverloaded)
        ));
        assert!(matches!(
            connection
                .send(
                    vec![0; 1024 * 1024],
                    Instant::now() + std::time::Duration::from_millis(20),
                )
                .await,
            Err(TransportError::Timeout)
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), connection.join())
            .await
            .unwrap();
        assert!(connection.is_closed());
    }

    #[tokio::test]
    async fn pending_calls_complete_on_shutdown_and_owned_tasks_join() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        });
        let client = RemotingClient::new(TransportMode::Plain);
        let invoke_client = client.clone();
        let invoke = tokio::spawn(async move {
            invoke_client
                .invoke(
                    &address.to_string(),
                    test_command(),
                    Instant::now() + std::time::Duration::from_secs(2),
                )
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        client.shutdown().await;
        assert!(matches!(
            invoke.await.unwrap(),
            Err(TransportError::Shutdown)
        ));
        assert!(client.inner.tasks.lock().unwrap().is_empty());
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_is_idempotent_and_new_calls_are_rejected() {
        let client = RemotingClient::new(TransportMode::Plain);
        client.shutdown().await;
        client.shutdown().await;
        assert!(matches!(
            client
                .invoke(
                    "127.0.0.1:1",
                    test_command(),
                    Instant::now() + std::time::Duration::from_secs(1)
                )
                .await,
            Err(TransportError::Shutdown)
        ));
    }
}
