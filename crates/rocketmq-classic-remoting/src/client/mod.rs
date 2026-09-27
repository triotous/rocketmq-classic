mod config;
mod connection;
mod connection_manager;
mod pending;
mod processor;
pub mod tls;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering},
    },
};

use rand::seq::SliceRandom;
use std::task::{Context, Poll, Waker};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{self, Instant},
};

use crate::{
    codec::frame::DEFAULT_MAX_FRAME_SIZE, command::RemotingCommand, error::TransportError,
};

pub use config::{ClientConfig, SocksProxyConfig};
pub use connection::TransportMode;
pub use connection_manager::Address;
pub use processor::RequestProcessor;
pub use tls::{PrivateKeyDecryptionStrategy, TlsConfig};

const OUTBOUND_QUEUE_CAPACITY: usize = 128;
const INBOUND_QUEUE_CAPACITY: usize = 128;
const CHANNEL_EVENT_QUEUE_CAPACITY: usize = 10001;

#[derive(Clone)]
pub struct ProcessorRegistration {
    pub processor: Arc<dyn RequestProcessor>,
    pub executor: Option<Arc<dyn RequestExecutor>>,
}

#[derive(Clone)]
struct CallbackTracking {
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

type CallbackTask = Box<dyn FnOnce() + Send + 'static>;

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
    public_executor: Arc<TokioRequestExecutor>,
    shutdown: tokio::sync::watch::Sender<bool>,
    shutdown_complete: tokio::sync::OnceCell<()>,
    closed: AtomicBool,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    security: TransportMode,
    max_frame_size: usize,
    config: ClientConfig,
    socks_proxy_routes: HashMap<String, config::SocksProxyConfig>,
    outbound_queue_capacity: usize,
    async_admission: Arc<tokio::sync::Semaphore>,
    oneway_admission: Arc<tokio::sync::Semaphore>,
    callback_sender: mpsc::UnboundedSender<CallbackTask>,
    callback_executor: Mutex<Option<Arc<dyn CallbackExecutor>>>,
    rpc_hooks: Mutex<Vec<Arc<dyn RpcHook>>>,
    channel_listener: Option<Arc<dyn ChannelEventListener>>,
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

fn rpc_hook_transport_error(error: RpcHookError) -> TransportError {
    match error {
        RpcHookError::Abort {
            response_code,
            remark,
        } => TransportError::RpcHook(format!(
            "aborted with response code {response_code}: {}",
            remark.unwrap_or_default()
        )),
        RpcHookError::Failed(message) => TransportError::RpcHook(message),
    }
}

fn classify_connection_error(remote_addr: &str, error: TransportError) -> TransportError {
    match error {
        TransportError::Connect(_) | TransportError::Tls(_) | TransportError::Closed => {
            TransportError::ConnectRequest {
                remote_addr: remote_addr.to_owned(),
                cause: None,
            }
        }
        error => error,
    }
}

fn classify_write_error(remote_addr: &str, error: TransportError) -> TransportError {
    match error {
        TransportError::Send(_) | TransportError::Closed | TransportError::Codec(_) => {
            TransportError::SendRequest {
                remote_addr: remote_addr.to_owned(),
                cause: None,
            }
        }
        error => error,
    }
}

fn run_before_rpc_hooks(
    hooks: &[Arc<dyn RpcHook>],
    remote_addr: &str,
    request: &RemotingCommand,
) -> Result<(), RpcHookError> {
    for hook in hooks {
        hook.before_request_result(remote_addr, request)?;
    }
    Ok(())
}

fn run_after_rpc_hooks(
    hooks: &[Arc<dyn RpcHook>],
    remote_addr: &str,
    request: &RemotingCommand,
    response: &RemotingCommand,
) -> Result<(), RpcHookError> {
    for hook in hooks {
        hook.after_response_result(remote_addr, request, response)?;
    }
    Ok(())
}

/// Routes a processor task to an application-owned executor. Returning `Err` rejects the task.
pub trait RequestExecutor: Send + Sync + 'static {
    fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), String>;
}

struct TokioExecutorState {
    closed: AtomicBool,
    admission: Mutex<()>,
}

impl TokioExecutorState {
    fn execute(
        &self,
        sender: &mpsc::UnboundedSender<CallbackTask>,
        task: CallbackTask,
    ) -> Result<(), String> {
        let _admission = self.admission.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return Err("client public executor is shut down".into());
        }
        sender.send(task).map_err(|error| error.to_string())
    }

    fn shutdown(&self) {
        let _admission = self.admission.lock().unwrap();
        self.closed.store(true, Ordering::Release);
    }
}

struct TokioRequestExecutor {
    sender: mpsc::UnboundedSender<CallbackTask>,
    state: Arc<TokioExecutorState>,
}

impl RequestExecutor for TokioRequestExecutor {
    fn execute(&self, task: CallbackTask) -> Result<(), String> {
        self.state.execute(&self.sender, task)
    }
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

/// RPC interception hooks. Hook errors are propagated through the request path. Outbound hook
/// panics propagate from the invoking task; inbound hook panics map to SYSTEM_ERROR.
pub trait RpcHook: Send + Sync + 'static {
    /// Optional equality key used to reproduce Java `List.contains` deduplication.
    fn equality_key(&self) -> Option<String> {
        None
    }

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
                // Java's async wrapper constructs a fresh ResponseFuture for exceptional
                // completion; its public sendRequestOK field therefore keeps the true default.
                send_request_ok: true,
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

struct ClientCallbackExecutor {
    sender: mpsc::UnboundedSender<CallbackTask>,
}

impl CallbackExecutor for ClientCallbackExecutor {
    fn execute(&self, task: CallbackTask) -> Result<(), CallbackTask> {
        self.sender.send(task).map_err(|error| error.0)
    }
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
        Self::with_config_and_channel_event_listener(config, None)
    }

    pub fn with_config_and_channel_event_listener(
        config: ClientConfig,
        channel_listener: Option<Arc<dyn ChannelEventListener>>,
    ) -> Result<Self, TransportError> {
        config.validate()?;
        let socks_proxy_routes = config.parse_socks_proxy_config()?;
        let max_frame_size = DEFAULT_MAX_FRAME_SIZE;
        let (inbound, inbound_rx) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let (callback_sender, callback_receiver) = mpsc::unbounded_channel();
        let (channel_event_sender, channel_event_receiver) =
            mpsc::channel(CHANNEL_EVENT_QUEUE_CAPACITY);
        let default_callback_executor: Arc<dyn CallbackExecutor> =
            Arc::new(ClientCallbackExecutor {
                sender: callback_sender.clone(),
            });
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
            callback_sender: callback_sender.clone(),
            callback_executor: Mutex::new(Some(default_callback_executor)),
            public_executor: Arc::new(TokioRequestExecutor {
                sender: callback_sender.clone(),
                state: Arc::new(TokioExecutorState {
                    closed: AtomicBool::new(false),
                    admission: Mutex::new(()),
                }),
            }),
            config,
            outbound_queue_capacity: OUTBOUND_QUEUE_CAPACITY,
            rpc_hooks: Mutex::new(Vec::new()),
            channel_listener,
            channel_event_sender,
            request_pipeline: Mutex::new(None),
            nameservers: Mutex::new(Vec::new()),
            available_nameservers: Mutex::new(HashSet::new()),
            chosen_nameserver: Mutex::new(None),
            nameserver_index: AtomicU64::new(0),
            socks_proxy_routes,
        });
        let processor_tasks = processor::start(
            Arc::downgrade(&inner),
            inbound_rx,
            inner.config.worker_threads,
            inner.config.disable_netty_worker_group,
        );
        for task in processor_tasks {
            inner.spawn(task);
        }
        inner.spawn(start_channel_event_worker(
            inner.channel_listener.clone(),
            channel_event_receiver,
            inner.shutdown.subscribe(),
        ));
        let callback_workers = if inner.config.callback_executor_threads == 0 {
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
        let (callback_sender, _callback_receiver) = mpsc::unbounded_channel();
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
            public_executor: Arc::new(TokioRequestExecutor {
                sender: callback_sender.clone(),
                state: Arc::new(TokioExecutorState {
                    closed: AtomicBool::new(false),
                    admission: Mutex::new(()),
                }),
            }),
            security: security.clone(),
            max_frame_size,
            config: ClientConfig {
                transport: security.clone(),
                ..ClientConfig::default()
            },
            socks_proxy_routes: HashMap::new(),
            outbound_queue_capacity: OUTBOUND_QUEUE_CAPACITY,
            async_admission: Arc::new(tokio::sync::Semaphore::new(65535)),
            oneway_admission: Arc::new(tokio::sync::Semaphore::new(65535)),
            callback_sender,
            callback_executor: Mutex::new(None),
            rpc_hooks: Mutex::new(Vec::new()),
            channel_listener: None,
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
            false,
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

    pub fn get_name_server_address_list(&self) -> Option<Vec<String>> {
        let nameservers = self.inner.nameservers.lock().unwrap();
        (!nameservers.is_empty()).then(|| nameservers.clone())
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
        let addresses = self.get_name_server_address_list().unwrap_or_default();
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
        let addresses = self.get_name_server_address_list().unwrap_or_default();
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
        if !hooks.iter().any(|registered| {
            Arc::ptr_eq(registered, &hook)
                || registered
                    .equality_key()
                    .is_some_and(|key| hook.equality_key().as_ref() == Some(&key))
        }) {
            hooks.push(hook);
        }
    }

    /// Returns a snapshot of the registered hooks.
    pub fn get_rpc_hooks(&self) -> Vec<Arc<dyn RpcHook>> {
        self.inner.rpc_hooks.lock().unwrap().clone()
    }

    /// Provides lock-protected access to the live RPC-hook list.
    pub fn with_rpc_hooks<R>(&self, operation: impl FnOnce(&mut Vec<Arc<dyn RpcHook>>) -> R) -> R {
        let mut hooks = self.inner.rpc_hooks.lock().unwrap();
        operation(&mut hooks)
    }

    /// Removes a registered hook by identity or its optional Java-equality key.
    pub fn remove_rpc_hook(&self, hook: &Arc<dyn RpcHook>) -> bool {
        let mut hooks = self.inner.rpc_hooks.lock().unwrap();
        let position = hooks.iter().position(|registered| {
            Arc::ptr_eq(registered, hook)
                || registered
                    .equality_key()
                    .is_some_and(|key| hook.equality_key().as_ref() == Some(&key))
        });
        position.is_some_and(|position| {
            hooks.remove(position);
            true
        })
    }

    pub fn clear_rpc_hooks(&self) {
        self.inner.rpc_hooks.lock().unwrap().clear();
    }

    pub fn get_channel_event_listener(&self) -> Option<Arc<dyn ChannelEventListener>> {
        self.inner.channel_listener.clone()
    }

    pub fn set_callback_executor(&self, executor: Option<Arc<dyn CallbackExecutor>>) {
        *self.inner.callback_executor.lock().unwrap() =
            if self.inner.config.disable_callback_executor {
                None
            } else {
                Some(executor.unwrap_or_else(|| {
                    Arc::new(ClientCallbackExecutor {
                        sender: self.inner.callback_sender.clone(),
                    })
                }))
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
                    send_request_ok: true,
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
        let channel_established = Arc::new(AtomicBool::new(false));
        let callback_opaque = Arc::new(AtomicI32::new(command.opaque));
        let task = tokio::spawn(async move {
            let (result, permit) = match client.inner.connection(&addr, deadline).await {
                Ok(_) => {
                    channel_established.store(true, Ordering::Release);
                    let hooks = client.inner.rpc_hooks.lock().unwrap().clone();
                    if let Err(error) = run_before_rpc_hooks(&hooks, &addr, &command) {
                        let context = InvokeCallbackResult::from_result(
                            callback_request_state.lock().unwrap().clone(),
                            Err(rpc_hook_transport_error(error)),
                            started.elapsed(),
                        );
                        client.inner.dispatch_callback(CallbackJob {
                            callback: callback.clone(),
                            context,
                            channel_established: channel_established.clone(),
                            _permit: None,
                        });
                        return;
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
                Err(error) => (Err(classify_connection_error(&addr, error)), None),
            };
            let context = InvokeCallbackResult::from_result(
                callback_request_state.lock().unwrap().clone(),
                result,
                started.elapsed(),
            );
            // invokeAsyncImpl synthesizes a default-true ResponseFuture on failure;
            // the internal requestFail flag is not the public callback's flag.
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

    /// Provides lock-protected access to the live processor table.
    pub fn with_processor_table<R>(
        &self,
        operation: impl FnOnce(&mut HashMap<i32, ProcessorRegistration>) -> R,
    ) -> R {
        let mut processors = self.inner.processors.lock().unwrap();
        operation(&mut processors)
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
                run_rpc_hooks: true,
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
        let connection = self
            .inner
            .connection(addr, deadline)
            .await
            .map_err(|error| classify_connection_error(addr, error))?;
        if let Some(callback) = &callback {
            callback.channel_established.store(true, Ordering::Release);
        }
        let timeout_budget = deadline.saturating_duration_since(Instant::now());
        if Instant::now() >= deadline {
            return Err(TransportError::Timeout);
        }
        if options.run_rpc_hooks && !options.retried_go_away && !options.admitted {
            let hooks = self.inner.rpc_hooks.lock().unwrap().clone();
            run_before_rpc_hooks(&hooks, addr, &command).map_err(rpc_hook_transport_error)?;
        }
        if !options.admitted {
            match self
                .inner
                .acquire_admission(self.inner.async_admission.clone(), deadline)
                .await
            {
                Ok(permit) => *admission = Some(permit),
                Err(error)
                    if options.close_socket_on_timeout && error != TransportError::Shutdown =>
                {
                    connection.close();
                    return Err(TransportError::SendRequest {
                        remote_addr: addr.to_owned(),
                        cause: Some(error.to_string()),
                    });
                }
                Err(error) => return Err(error),
            }
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
        let frame = connection
            .encode_command(&command, self.inner.max_frame_size)
            .map_err(|error| classify_write_error(connection.address(), error))?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        match time::timeout(remaining, connection.send(frame, deadline)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.inner.cancel_pending(opaque, connection.generation);
                pending.disarm();
                return Err(classify_write_error(connection.address(), error));
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
                            let hooks = self.inner.rpc_hooks.lock().unwrap().clone();
                            run_after_rpc_hooks(&hooks, addr, &request_for_hooks, response)
                                .map_err(rpc_hook_transport_error)?;
                            if let Some(callback) = &callback {
                                let mut request = retry_request;
                                request.opaque = callback.opaque.load(Ordering::Acquire);
                                *callback.request.lock().unwrap() = request;
                            }
                        }
                        return result;
                    }
                    return Err(TransportError::SendRequest {
                        remote_addr: connection.address().to_owned(),
                        cause: Some(if options.retried_go_away {
                            format!(
                                "Receive GO_AWAY twice in request from connection generation={}",
                                connection.generation
                            )
                        } else {
                            format!(
                                "Receive GO_AWAY from connection generation={}",
                                connection.generation
                            )
                        }),
                    });
                }
                if options.run_rpc_hooks && !options.retried_go_away {
                    let hooks = self.inner.rpc_hooks.lock().unwrap().clone();
                    run_after_rpc_hooks(&hooks, addr, &request_for_hooks, &response)
                        .map_err(rpc_hook_transport_error)?;
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

    /// Starts connection acquisition at call time, then schedules a managed one-way operation and
    /// returns a ready future without waiting for connection completion, admission, or socket
    /// write. Synchronous acquisition errors are returned directly; post-return transport failures
    /// are not observable. Completion does not imply Broker receipt or storage.
    pub fn invoke_oneway(
        &self,
        addr: &str,
        mut command: RemotingCommand,
        deadline: Instant,
    ) -> Result<std::future::Ready<Result<(), TransportError>>, TransportError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        if Instant::now() >= deadline {
            return Err(TransportError::Timeout);
        }
        connection_manager::Address::parse(addr)?;
        let client = self.clone();
        let addr = addr.to_owned();
        let (connection_tx, connection_rx) = tokio::sync::oneshot::channel();
        let connection_client = client.clone();
        let connection_addr = addr.clone();
        let connection_task = async move {
            let result = connection_client
                .inner
                .connection(&connection_addr, deadline)
                .await;
            let _ = connection_tx.send(result);
        };
        if let Some(connection_task) = spawn_after_initial_poll(connection_task) {
            self.inner.spawn(connection_task);
        }
        let task = tokio::spawn(async move {
            let Ok(Ok(connection)) = connection_rx.await else {
                return;
            };
            let hooks = client.inner.rpc_hooks.lock().unwrap().clone();
            if run_before_rpc_hooks(&hooks, &addr, &command).is_err() {
                return;
            }
            command.mark_oneway();
            let Ok(_admission) = client
                .inner
                .acquire_admission(client.inner.oneway_admission.clone(), deadline)
                .await
            else {
                return;
            };
            let Ok(frame) = connection.encode_command(&command, client.inner.max_frame_size) else {
                return;
            };
            let _ = connection.send(frame, deadline).await;
        });
        self.inner.spawn(task);
        Ok(std::future::ready(Ok(())))
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
        if let Ok(permit) = semaphore.clone().try_acquire_owned() {
            return Ok(permit);
        }
        if Instant::now() >= deadline {
            return Err(TransportError::TooManyRequests);
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
        if self.channel_listener.is_some() {
            let _ = self
                .channel_event_sender
                .try_send((address.to_owned(), event));
        }
    }

    fn shutdown(&self) {
        self.closed.store(true, Ordering::Release);
        self.public_executor.state.shutdown();
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
                .filter_map(|(id, item)| {
                    (item.generation == generation).then_some((*id, item.address.clone()))
                })
                .collect::<Vec<_>>()
        };
        for (id, address) in ids {
            let error = if error == TransportError::Closed {
                TransportError::SendRequest {
                    remote_addr: address,
                    cause: None,
                }
            } else {
                error.clone()
            };
            self.finish_pending(id, generation, Err(error));
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
            if let Ok(bytes) = connection.encode_command(&response, client.max_frame_size) {
                if let Err(error) =
                    connection.try_send(bytes, Instant::now() + std::time::Duration::from_secs(30))
                {
                    client.connection_closed(generation, Some(error));
                }
            }
        }));
    }

    fn dispatch_callback(&self, job: CallbackJob) {
        let task: CallbackTask = Box::new(move || run_callback(job));
        if self.config.disable_callback_executor {
            task();
            return;
        }
        if let Some(executor) = self.callback_executor.lock().unwrap().clone() {
            let task = Arc::new(Mutex::new(Some(task)));
            let task_for_executor = task.clone();
            let dispatched: CallbackTask = Box::new(move || {
                if let Some(task) = task_for_executor.lock().unwrap().take() {
                    task();
                }
            });
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                executor.execute(dispatched)
            }));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => {
                    if let Some(task) = task.lock().unwrap().take() {
                        task();
                    }
                }
            }
            return;
        }
        if let Err(error) = self.callback_sender.send(task) {
            (error.0)();
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

struct NotifyWake(Arc<tokio::sync::Notify>);

impl std::task::Wake for NotifyWake {
    fn wake(self: Arc<Self>) {
        self.0.notify_one();
    }
}

fn spawn_after_initial_poll<F>(future: F) -> Option<JoinHandle<()>>
where
    F: Future<Output = ()> + Send + 'static,
{
    let mut future = Box::pin(future);
    let notify = Arc::new(tokio::sync::Notify::new());
    let waker = Waker::from(Arc::new(NotifyWake(notify.clone())));
    let mut context = Context::from_waker(&waker);
    match Pin::as_mut(&mut future).poll(&mut context) {
        Poll::Ready(()) => None,
        Poll::Pending => Some(tokio::spawn(async move {
            loop {
                let notified = notify.notified();
                tokio::pin!(notified);
                tokio::select! {
                    _ = &mut notified => {}
                    _ = &mut future => break,
                }
            }
        })),
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
    listener: Option<Arc<dyn ChannelEventListener>>,
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
            let Some(listener) = &listener else { continue };
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
    receiver: mpsc::UnboundedReceiver<CallbackTask>,
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
                                    for task in queued {
                                        let _ = tokio::task::spawn_blocking(task).await;
                                    }
                                    return;
                                }
                                continue;
                            }
                        }
                    };
                    let Some(task) = job else { return };
                    let _ = tokio::task::spawn_blocking(task).await;
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
    use std::sync::atomic::AtomicUsize;

    fn test_command() -> RemotingCommand {
        RemotingCommand::default()
    }

    #[tokio::test]
    async fn oneway_connection_acquisition_enters_connect_before_return() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = RemotingClient::new(TransportMode::Plain);
        let _result = client
            .invoke_oneway(
                &address.to_string(),
                test_command(),
                Instant::now() + std::time::Duration::from_secs(1),
            )
            .unwrap();
        {
            let connecting = client.inner.connecting.lock().unwrap();
            let gate = connecting
                .get(&address.to_string())
                .and_then(Weak::upgrade)
                .expect("acquisition should register its single-flight gate before return");
            assert!(gate.try_lock().is_err(), "dial future should hold the gate");
        }
        client.shutdown().await;
    }

    #[tokio::test]
    async fn spawned_connection_future_is_polled_before_return() {
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_future = polls.clone();
        let task = spawn_after_initial_poll(async move {
            polls_for_future.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
        })
        .expect("pending future should be spawned");
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        task.abort();
        let _ = task.await;
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
    async fn admission_matches_java_zero_timeout_and_wait_timeout_categories() {
        let client = RemotingClient::new(TransportMode::Plain);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(0));
        assert_eq!(
            client
                .inner
                .acquire_admission(semaphore.clone(), Instant::now())
                .await
                .unwrap_err(),
            TransportError::TooManyRequests
        );
        assert_eq!(
            client
                .inner
                .acquire_admission(
                    semaphore,
                    Instant::now() + std::time::Duration::from_millis(10),
                )
                .await
                .unwrap_err(),
            TransportError::Timeout
        );
        let available = Arc::new(tokio::sync::Semaphore::new(1));
        let _permit = client
            .inner
            .acquire_admission(available.clone(), Instant::now())
            .await
            .unwrap();
        assert_eq!(
            client
                .inner
                .acquire_admission(available, Instant::now())
                .await
                .unwrap_err(),
            TransportError::TooManyRequests
        );
        client.shutdown().await;
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
    async fn accepted_inbound_requests_are_dispatched_when_shutdown_starts() {
        struct InlineExecutor;

        impl RequestExecutor for InlineExecutor {
            fn execute(&self, task: CallbackTask) -> Result<(), String> {
                task();
                Ok(())
            }
        }

        let client = RemotingClient::new(TransportMode::Plain);
        let processed = Arc::new(AtomicUsize::new(0));
        let processed_by_handler = processed.clone();
        client.register_processor_with_executor(
            test_command().code,
            Arc::new(move |_| {
                processed_by_handler.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }),
            Some(Arc::new(InlineExecutor)),
        );
        client
            .inner
            .inbound
            .try_send(processor::InboundRequest {
                generation: 0,
                remote_addr: "test:0".into(),
                command: test_command(),
            })
            .unwrap();
        client.inner.shutdown.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while processed.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("accepted inbound request was dropped during shutdown");
        client.shutdown().await;
    }

    #[tokio::test]
    async fn callback_workers_drain_tasks_accepted_before_shutdown() {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let completed = Arc::new(AtomicUsize::new(0));
        for _ in 0..32 {
            let completed = completed.clone();
            sender
                .send(Box::new(move || {
                    completed.fetch_add(1, Ordering::SeqCst);
                }) as CallbackTask)
                .unwrap();
        }
        let workers = start_callback_workers(receiver, shutdown.subscribe(), 2);
        shutdown.send(true).unwrap();
        for worker in workers {
            tokio::time::timeout(std::time::Duration::from_secs(2), worker)
                .await
                .expect("callback worker did not drain accepted tasks")
                .unwrap();
        }
        assert_eq!(completed.load(Ordering::SeqCst), 32);
    }

    #[test]
    fn public_executor_rejects_submissions_after_client_shutdown() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let state = TokioExecutorState {
            closed: AtomicBool::new(false),
            admission: Mutex::new(()),
        };
        state
            .execute(&sender, Box::new(|| {}))
            .expect("running executor accepts tasks");
        assert!(receiver.try_recv().is_ok());
        state.shutdown();
        assert!(state.execute(&sender, Box::new(|| {})).is_err());
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn worker_group_toggle_controls_handler_dispatch_path() {
        let config = ClientConfig {
            disable_netty_worker_group: true,
            ..ClientConfig::default()
        };
        let (inbound, receiver) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let (callback_sender, _callback_receiver) = mpsc::unbounded_channel();
        let (channel_event_sender, _channel_event_receiver) =
            mpsc::channel(CHANNEL_EVENT_QUEUE_CAPACITY);
        let executor = Arc::new(TokioRequestExecutor {
            sender: callback_sender.clone(),
            state: Arc::new(TokioExecutorState {
                closed: AtomicBool::new(false),
                admission: Mutex::new(()),
            }),
        });
        let inner = Arc::new(ClientInner {
            connections: Mutex::new(HashMap::new()),
            connecting: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            opaque: AtomicI32::new(0),
            generation: AtomicU64::new(1),
            processors: Mutex::new(HashMap::new()),
            default_processor: Mutex::new(None),
            inbound,
            public_executor: executor,
            shutdown,
            shutdown_complete: tokio::sync::OnceCell::new(),
            closed: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            security: TransportMode::Plain,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            config,
            socks_proxy_routes: HashMap::new(),
            outbound_queue_capacity: OUTBOUND_QUEUE_CAPACITY,
            async_admission: Arc::new(tokio::sync::Semaphore::new(8)),
            oneway_admission: Arc::new(tokio::sync::Semaphore::new(8)),
            callback_sender,
            callback_executor: Mutex::new(None),
            rpc_hooks: Mutex::new(Vec::new()),
            channel_listener: None,
            channel_event_sender,
            request_pipeline: Mutex::new(None),
            nameservers: Mutex::new(Vec::new()),
            available_nameservers: Mutex::new(HashSet::new()),
            chosen_nameserver: Mutex::new(None),
            nameserver_index: AtomicU64::new(0),
        });
        let tasks = processor::start(
            Arc::downgrade(&inner),
            receiver,
            inner.config.worker_threads,
            inner.config.disable_netty_worker_group,
        );
        assert!(tasks.is_empty());
        inner.public_executor.state.shutdown();
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
    async fn pooled_allocator_reuses_frame_capacity_after_successful_write() {
        let (client_io, mut peer_io) = tokio::io::duplex(4096);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            30,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            OUTBOUND_QUEUE_CAPACITY,
            None,
            "test:0".into(),
            true,
        );
        let command = RemotingCommand {
            body: Some(vec![7; 128]),
            ..test_command()
        };
        let first = connection
            .encode_command(&command, DEFAULT_MAX_FRAME_SIZE)
            .unwrap();
        let first_capacity = first.capacity();
        connection
            .send(first, Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
        let mut length = [0; 4];
        tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut length)
            .await
            .unwrap();
        let remaining = u32::from_be_bytes(length) as usize;
        let mut received = vec![0; remaining];
        tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut received)
            .await
            .unwrap();
        assert_eq!(connection.pooled_frame_count(), 1);
        assert!(connection.pooled_frame_capacity() >= first_capacity);

        let second = connection
            .encode_command(&command, DEFAULT_MAX_FRAME_SIZE)
            .unwrap();
        assert!(second.capacity() >= first_capacity);
        connection
            .send(second, Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
        tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut length)
            .await
            .unwrap();
        let remaining = u32::from_be_bytes(length) as usize;
        received.resize(remaining, 0);
        tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut received)
            .await
            .unwrap();
        connection.close();
        connection.join().await;
    }

    #[tokio::test]
    async fn pooled_allocator_recycles_frame_after_encode_failure() {
        let (client_io, _peer_io) = tokio::io::duplex(64);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            23,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            OUTBOUND_QUEUE_CAPACITY,
            None,
            "test:0".into(),
            true,
        );
        assert!(matches!(
            connection.encode_command(&test_command(), 1),
            Err(TransportError::Codec(_))
        ));
        assert_eq!(connection.pooled_frame_count(), 1);
        connection.close();
        connection.join().await;
    }

    #[tokio::test]
    async fn pooled_allocator_recycles_frame_after_queue_admission_timeout() {
        let (client_io, mut peer_io) = tokio::io::duplex(1);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            22,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            1,
            None,
            "test:0".into(),
            true,
        );
        let active_sender = {
            let connection = connection.clone();
            tokio::spawn(async move {
                connection
                    .send(
                        vec![0; 128],
                        Instant::now() + std::time::Duration::from_secs(5),
                    )
                    .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while connection.outbound_capacity() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("writer did not dequeue the active frame");
        let mut first_byte = [0];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut first_byte),
        )
        .await
        .expect("writer did not begin sending the active frame")
        .unwrap();
        connection.try_send(vec![0; 128], Instant::now()).unwrap();
        let result = connection
            .send(
                vec![0; 128],
                Instant::now() + std::time::Duration::from_millis(10),
            )
            .await;
        assert!(matches!(result, Err(TransportError::Timeout)), "{result:?}");
        assert_eq!(connection.pooled_frame_count(), 1);
        connection.close();
        connection.join().await;
        active_sender.abort();
        let _ = active_sender.await;
    }

    #[test]
    fn asynchronous_write_failure_matches_java_request_fail_classification() {
        let remote_addr = "127.0.0.1:10911";
        assert_eq!(
            classify_connection_error(remote_addr, TransportError::Connect("refused".into())),
            TransportError::ConnectRequest {
                remote_addr: remote_addr.into(),
                cause: None,
            }
        );
        assert_eq!(
            classify_connection_error(remote_addr, TransportError::Tls("handshake".into())),
            TransportError::ConnectRequest {
                remote_addr: remote_addr.into(),
                cause: None,
            }
        );
        assert_eq!(
            classify_connection_error(remote_addr, TransportError::Timeout),
            TransportError::Timeout
        );
        assert_eq!(
            classify_write_error(remote_addr, TransportError::Send("broken pipe".into())),
            TransportError::SendRequest {
                remote_addr: remote_addr.into(),
                cause: None,
            }
        );
        assert_eq!(
            classify_write_error(remote_addr, TransportError::Closed),
            TransportError::SendRequest {
                remote_addr: remote_addr.into(),
                cause: None,
            }
        );
        assert_eq!(
            classify_write_error(
                remote_addr,
                TransportError::Codec(crate::error::CodecError::FrameTooLarge {
                    length: 128,
                    limit: 64,
                }),
            ),
            TransportError::SendRequest {
                remote_addr: remote_addr.into(),
                cause: None,
            }
        );
        assert_eq!(
            classify_write_error(remote_addr, TransportError::Timeout),
            TransportError::Timeout
        );
    }

    #[tokio::test]
    async fn pooled_allocator_recycles_frames_after_write_error() {
        let (client_io, peer_io) = tokio::io::duplex(1);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            26,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            OUTBOUND_QUEUE_CAPACITY,
            None,
            "test:0".into(),
            true,
        );
        drop(peer_io);
        let result = connection
            .send(
                vec![0; 128],
                Instant::now() + std::time::Duration::from_secs(2),
            )
            .await;
        assert!(matches!(result, Err(TransportError::Send(_))));
        connection.join().await;
        assert_eq!(connection.pooled_frame_count(), 1);
    }

    #[tokio::test]
    async fn pooled_allocator_recycles_frames_after_send_cancellation_and_shutdown() {
        for shutdown in [false, true] {
            let (client_io, mut peer_io) = tokio::io::duplex(1);
            let connection = connection::Connection::spawn(
                Box::new(client_io),
                if shutdown { 25 } else { 24 },
                Weak::new(),
                DEFAULT_MAX_FRAME_SIZE,
                1,
                None,
                "test:0".into(),
                true,
            );
            let sender = {
                let connection = connection.clone();
                tokio::spawn(async move {
                    connection
                        .send(
                            vec![0; 128],
                            Instant::now() + std::time::Duration::from_secs(5),
                        )
                        .await
                })
            };
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while connection.outbound_capacity() != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("writer did not dequeue the frame");
            let mut first_byte = [0];
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                tokio::io::AsyncReadExt::read_exact(&mut peer_io, &mut first_byte),
            )
            .await
            .expect("writer did not begin sending the frame")
            .unwrap();
            if shutdown {
                connection.close();
            } else {
                sender.abort();
            }
            let _ = sender.await;
            tokio::time::timeout(std::time::Duration::from_secs(1), connection.join())
                .await
                .expect("connection did not stop");
            assert_eq!(connection.pooled_frame_count(), 1);
        }
    }

    #[tokio::test]
    async fn pooled_allocator_keeps_at_most_four_buffers_per_connection() {
        let (client_io, _peer_io) = tokio::io::duplex(64);
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            27,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            OUTBOUND_QUEUE_CAPACITY,
            None,
            "test:0".into(),
            true,
        );
        for _ in 0..8 {
            connection.recycle_frame(Vec::with_capacity(128));
        }
        assert_eq!(connection.pooled_frame_count(), 4);
        connection.close();
        connection.join().await;
    }

    #[tokio::test]
    async fn pooled_allocator_recycles_frames_when_queue_is_full_or_closed() {
        let (client_io, peer_io) = tokio::io::duplex(1);
        let peer_task = tokio::spawn(async move {
            let _peer_io = peer_io;
            std::future::pending::<()>().await;
        });
        let connection = connection::Connection::spawn(
            Box::new(client_io),
            29,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            1,
            None,
            "test:0".into(),
            true,
        );
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        connection.try_send(vec![0; 128], deadline).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while connection.outbound_capacity() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        connection.try_send(vec![0; 128], deadline).unwrap();
        let rejected = connection
            .encode_command(&test_command(), DEFAULT_MAX_FRAME_SIZE)
            .unwrap();
        assert!(matches!(
            connection.try_send(rejected, Instant::now()),
            Err(TransportError::InboundOverloaded)
        ));
        assert!(connection.pooled_frame_count() > 0);
        connection.close();
        connection.join().await;
        peer_task.abort();
        assert!(connection.pooled_frame_count() > 0);

        let (client_io, _peer_io) = tokio::io::duplex(1);
        let closed_connection = connection::Connection::spawn(
            Box::new(client_io),
            28,
            Weak::new(),
            DEFAULT_MAX_FRAME_SIZE,
            1,
            None,
            "test:0".into(),
            true,
        );
        closed_connection.close();
        closed_connection.join().await;
        let rejected = closed_connection
            .encode_command(&test_command(), DEFAULT_MAX_FRAME_SIZE)
            .unwrap();
        assert!(matches!(
            closed_connection.try_send(rejected, Instant::now()),
            Err(TransportError::Closed)
        ));
        assert_eq!(closed_connection.pooled_frame_count(), 1);
        closed_connection.join().await;
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
            true,
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
            true,
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
