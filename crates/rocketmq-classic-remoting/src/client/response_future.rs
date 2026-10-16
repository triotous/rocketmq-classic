use std::{
    fmt,
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{sync::OwnedSemaphorePermit, time::Instant as TokioInstant};

use crate::{codec::frame, command::RemotingCommand, error::TransportError};

use super::{InvokeCallback, connection};

/// A Rust handle for the Netty channel exposed by Java remoting callbacks.
///
/// The handle keeps channel identity after close events while allowing live operations when the
/// connection is still active. `write_and_flush` encodes a RocketMQ command before enqueueing it
/// on the connection's owned writer task.
#[derive(Clone)]
pub struct ChannelHandle {
    state: Arc<ChannelHandleState>,
}

struct ChannelHandleState {
    remote_addr: String,
    generation: u64,
    connection: Mutex<Option<Weak<connection::Connection>>>,
    closed_events: tokio::sync::watch::Sender<bool>,
    closed: Arc<AtomicBool>,
    active: AtomicBool,
    writable: Arc<AtomicBool>,
}

impl ChannelHandle {
    pub(super) fn pending(remote_addr: &str, generation: u64) -> Self {
        let (closed_events, _) = tokio::sync::watch::channel(false);
        Self {
            state: Arc::new(ChannelHandleState {
                remote_addr: super::redact_peer_address(remote_addr),
                generation,
                connection: Mutex::new(None),
                closed_events,
                closed: Arc::new(AtomicBool::new(false)),
                active: AtomicBool::new(false),
                writable: Arc::new(AtomicBool::new(true)),
            }),
        }
    }

    pub(super) fn attach(&self, connection: &Arc<connection::Connection>) {
        *self.state.connection.lock().unwrap() = Some(Arc::downgrade(connection));
        if !self.is_closed() {
            self.state.active.store(true, Ordering::Release);
        }
    }

    pub(super) fn mark_closed(&self) {
        self.state.closed.store(true, Ordering::Release);
        self.state.active.store(false, Ordering::Release);
        self.state.closed_events.send_replace(true);
    }

    pub(super) fn closed_flag(&self) -> Arc<AtomicBool> {
        self.state.closed.clone()
    }

    pub(super) fn writable_flag(&self) -> Arc<AtomicBool> {
        self.state.writable.clone()
    }

    pub(super) fn subscribe_closed(&self) -> tokio::sync::watch::Receiver<bool> {
        self.state.closed_events.subscribe()
    }

    fn connection(&self) -> Option<Arc<connection::Connection>> {
        self.state
            .connection
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
    }

    /// Returns the peer address recorded for this channel, with URI credentials redacted.
    pub fn remote_addr(&self) -> &str {
        &self.state.remote_addr
    }

    /// Returns the connection generation associated with this channel.
    pub fn generation(&self) -> u64 {
        self.state.generation
    }

    /// Reports whether the channel object remains open, including while connection is pending.
    pub fn is_open(&self) -> bool {
        !self.is_closed()
    }

    /// Reports whether the connection has completed setup and remains active.
    pub fn is_active(&self) -> bool {
        self.state.active.load(Ordering::Acquire) && !self.is_closed()
    }

    /// Reports whether the live connection is below its configured write watermarks.
    pub fn is_writable(&self) -> bool {
        self.is_active() && self.state.writable.load(Ordering::Acquire)
    }

    /// Closes this channel and lets the connection owner complete pending requests.
    pub fn close(&self) -> bool {
        if self.state.closed.swap(true, Ordering::AcqRel) {
            return false;
        }
        self.state.active.store(false, Ordering::Release);
        self.state.closed_events.send_replace(true);
        if let Some(connection) = self.connection() {
            connection.close();
        }
        true
    }

    /// Encodes and writes one command through the live channel's owned writer.
    pub async fn write_and_flush(
        &self,
        command: &RemotingCommand,
        deadline: TokioInstant,
    ) -> Result<(), TransportError> {
        let Some(connection) = self.connection().filter(|_| self.is_active()) else {
            return Err(TransportError::Closed);
        };
        connection.send(frame::encode(command)?, deadline).await
    }

    pub(super) fn enqueue_and_flush(
        &self,
        command: &RemotingCommand,
    ) -> Result<
        impl std::future::Future<Output = Result<(), TransportError>> + Send + 'static,
        TransportError,
    > {
        let Some(connection) = self.connection().filter(|_| self.is_active()) else {
            return Err(TransportError::Closed);
        };
        let written = connection.enqueue_without_deadline(frame::encode(command)?)?;
        Ok(async move { written.await.map_err(|_| TransportError::Closed)? })
    }

    fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }
}

impl fmt::Debug for ChannelHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChannelHandle")
            .field("remote_addr", &self.remote_addr())
            .field("generation", &self.generation())
            .field("open", &self.is_open())
            .field("active", &self.is_active())
            .finish()
    }
}

impl PartialEq for ChannelHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl Eq for ChannelHandle {}

/// Callback metadata and completion state corresponding to Java's public `ResponseFuture`.
///
/// The command objects are owned values. Use the `with_*_mut` methods to mutate a command held
/// by the future, preserving Java's live getter/setter behavior without exposing a lock guard.
pub struct ResponseFuture {
    channel: Option<ChannelHandle>,
    opaque: i32,
    request_command: Mutex<Option<RemotingCommand>>,
    response_command: Mutex<Option<RemotingCommand>>,
    timeout_millis: i64,
    begin_timestamp: SystemTime,
    begin_instant: TokioInstant,
    invoke_callback: Option<Arc<dyn InvokeCallback>>,
    cause: Mutex<Option<TransportError>>,
    send_request_ok: AtomicBool,
    interrupted: AtomicBool,
    callback_executed: AtomicBool,
    released: Mutex<Option<OwnedSemaphorePermit>>,
    response_latch: (Mutex<bool>, Condvar),
}

impl ResponseFuture {
    /// Constructs a callback context with the same metadata available from Java's public
    /// `ResponseFuture` constructors. The remoting client normally constructs these internally.
    pub fn new(
        channel: Option<ChannelHandle>,
        opaque: i32,
        request_command: Option<RemotingCommand>,
        timeout_millis: i64,
        invoke_callback: Option<Arc<dyn InvokeCallback>>,
    ) -> Self {
        Self::new_with_permit(
            channel,
            opaque,
            request_command,
            timeout_millis,
            invoke_callback,
            None,
        )
    }

    /// Constructs a future that owns an admission permit released by `release`.
    ///
    /// This is the Rust-owned equivalent of Java's `SemaphoreReleaseOnlyOnce` constructor
    /// argument. Dropping the future also releases the permit through RAII.
    pub fn new_with_permit(
        channel: Option<ChannelHandle>,
        opaque: i32,
        request_command: Option<RemotingCommand>,
        timeout_millis: i64,
        invoke_callback: Option<Arc<dyn InvokeCallback>>,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        let begin_timestamp = SystemTime::now();
        Self {
            channel,
            opaque,
            request_command: Mutex::new(request_command),
            response_command: Mutex::new(None),
            timeout_millis,
            begin_timestamp,
            begin_instant: TokioInstant::now(),
            invoke_callback,
            cause: Mutex::new(None),
            send_request_ok: AtomicBool::new(true),
            interrupted: AtomicBool::new(false),
            callback_executed: AtomicBool::new(false),
            released: Mutex::new(permit),
            response_latch: (Mutex::new(false), Condvar::new()),
        }
    }

    pub(super) fn from_result(
        channel: Option<ChannelHandle>,
        request_command: RemotingCommand,
        result: Result<RemotingCommand, TransportError>,
        timeout_millis: i64,
        begin_instant: TokioInstant,
        invoke_callback: Arc<dyn InvokeCallback>,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Arc<Self> {
        let elapsed = begin_instant.elapsed();
        let begin_timestamp = SystemTime::now().checked_sub(elapsed).unwrap_or(UNIX_EPOCH);
        let (response_command, cause) = match result {
            Ok(response) => (Some(response), None),
            Err(error) => (None, Some(error)),
        };
        Arc::new(Self {
            channel,
            opaque: request_command.opaque,
            request_command: Mutex::new(Some(request_command)),
            response_command: Mutex::new(response_command),
            timeout_millis,
            begin_timestamp,
            begin_instant,
            invoke_callback: Some(invoke_callback),
            cause: Mutex::new(cause),
            // invokeAsyncImpl passes a wrapper-created ResponseFuture to the callback on
            // exceptional completion, so its public sendRequestOK default remains true.
            send_request_ok: AtomicBool::new(true),
            interrupted: AtomicBool::new(false),
            callback_executed: AtomicBool::new(false),
            released: Mutex::new(permit),
            response_latch: (Mutex::new(false), Condvar::new()),
        })
    }

    pub fn channel(&self) -> Option<ChannelHandle> {
        self.channel.clone()
    }

    pub fn opaque(&self) -> i32 {
        self.opaque
    }

    pub fn request_command(&self) -> Option<RemotingCommand> {
        self.request_command.lock().unwrap().clone()
    }

    pub fn with_request_command_mut<R>(
        &self,
        operation: impl FnOnce(Option<&mut RemotingCommand>) -> R,
    ) -> R {
        operation(self.request_command.lock().unwrap().as_mut())
    }

    pub fn response_command(&self) -> Option<RemotingCommand> {
        self.response_command.lock().unwrap().clone()
    }

    pub fn set_response_command(&self, response: Option<RemotingCommand>) {
        *self.response_command.lock().unwrap() = response;
    }

    pub fn with_response_command_mut<R>(
        &self,
        operation: impl FnOnce(Option<&mut RemotingCommand>) -> R,
    ) -> R {
        operation(self.response_command.lock().unwrap().as_mut())
    }

    pub fn cause(&self) -> Option<TransportError> {
        self.cause.lock().unwrap().clone()
    }

    pub fn set_cause(&self, cause: Option<TransportError>) {
        *self.cause.lock().unwrap() = cause;
    }

    pub fn timeout_millis(&self) -> i64 {
        self.timeout_millis
    }

    pub fn is_timeout(&self) -> bool {
        let elapsed_millis = self
            .begin_instant
            .elapsed()
            .as_millis()
            .min(i64::MAX as u128) as i64;
        elapsed_millis > self.timeout_millis
    }

    pub fn begin_timestamp_millis(&self) -> i64 {
        self.begin_timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64
    }

    pub fn invoke_callback(&self) -> Option<Arc<dyn InvokeCallback>> {
        self.invoke_callback.clone()
    }

    pub fn is_send_request_ok(&self) -> bool {
        self.send_request_ok.load(Ordering::Acquire)
    }

    pub fn set_send_request_ok(&self, value: bool) {
        self.send_request_ok.store(value, Ordering::Release);
    }

    pub fn is_interrupted(&self) -> bool {
        self.interrupted.load(Ordering::Acquire)
    }

    /// Stores the response and releases threads waiting in `wait_response`.
    pub fn put_response(&self, response: Option<RemotingCommand>) {
        self.set_response_command(response);
        *self.response_latch.0.lock().unwrap() = true;
        self.response_latch.1.notify_all();
    }

    /// Waits up to `timeout` for `put_response`, then returns the current response value.
    pub fn wait_response(&self, timeout_millis: i64) -> Option<RemotingCommand> {
        let complete = self.response_latch.0.lock().unwrap();
        let _ = self
            .response_latch
            .1
            .wait_timeout_while(
                complete,
                Duration::from_millis(timeout_millis.max(0) as u64),
                |complete| !*complete,
            )
            .unwrap();
        self.response_command()
    }

    /// Runs the callback once, following Java `ResponseFuture.executeInvokeCallback` ordering.
    pub fn execute_invoke_callback(&self) {
        if self.callback_executed.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(callback) = &self.invoke_callback else {
            return;
        };
        match self.execute_callback_result() {
            Ok(response) => callback.operation_succeed(&response),
            Err(error) => callback.operation_fail(&error),
        }
        callback.operation_complete(self);
    }

    /// Marks the future interrupted and attempts its callback, as Java does.
    pub fn interrupt(&self) {
        self.interrupted.store(true, Ordering::Release);
        self.execute_invoke_callback();
    }

    /// Releases async admission exactly once.
    pub fn release(&self) {
        self.released.lock().unwrap().take();
    }

    pub(super) fn dispatch_async_callback(&self, channel_established: bool) {
        if self.callback_executed.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(callback) = &self.invoke_callback else {
            return;
        };
        if channel_established {
            callback.operation_complete(self);
        }
        match self.callback_result() {
            Ok(response) => callback.operation_succeed(&response),
            Err(error) => callback.operation_fail(&error),
        }
    }

    fn callback_result(&self) -> Result<RemotingCommand, TransportError> {
        if let Some(response) = self.response_command() {
            return Ok(response);
        }
        self.cause()
            .map_or_else(|| Err(self.default_failure()), Err)
    }

    fn execute_callback_result(&self) -> Result<RemotingCommand, TransportError> {
        if let Some(response) = self.response_command() {
            return Ok(response);
        }
        let cause = self.cause();
        if !self.is_send_request_ok() {
            return Err(TransportError::SendRequest {
                remote_addr: self
                    .channel
                    .as_ref()
                    .map(|channel| channel.remote_addr().to_owned())
                    .unwrap_or_default(),
                cause: cause.map(crate::error::ErrorSource::new),
            });
        }
        if self.is_timeout() {
            return Err(TransportError::timeout(
                crate::error::TimeoutPhase::Response,
                self.channel
                    .as_ref()
                    .map(|channel| channel.remote_addr().to_owned()),
                Some(Duration::from_millis(self.timeout_millis.max(0) as u64)),
            ));
        }
        Err(cause.unwrap_or_else(|| self.default_failure()))
    }

    fn default_failure(&self) -> TransportError {
        let request = self
            .request_command()
            .map(|request| format!("{request:?}"))
            .unwrap_or_else(|| "null".to_owned());
        TransportError::Remoting(request)
    }
}

impl fmt::Debug for ResponseFuture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResponseFuture")
            .field("response_command", &self.response_command())
            .field("send_request_ok", &self.is_send_request_ok())
            .field("cause", &self.cause())
            .field("opaque", &self.opaque)
            .field("timeout_millis", &self.timeout_millis)
            .field("begin_timestamp", &self.begin_timestamp)
            .finish()
    }
}

impl fmt::Display for ResponseFuture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{ChannelHandle, ResponseFuture};
    use crate::{client::InvokeCallback, command::RemotingCommand, error::TransportError};

    struct RecordingCallback(Arc<Mutex<Vec<&'static str>>>);

    impl InvokeCallback for RecordingCallback {
        fn operation_complete(&self, _future: &ResponseFuture) {
            self.0.lock().unwrap().push("complete");
        }

        fn operation_succeed(&self, _response: &RemotingCommand) {
            self.0.lock().unwrap().push("succeed");
        }

        fn operation_fail(&self, _error: &TransportError) {
            self.0.lock().unwrap().push("fail");
        }
    }

    struct FailureCallback(Arc<Mutex<Option<TransportError>>>);

    impl InvokeCallback for FailureCallback {
        fn operation_fail(&self, error: &TransportError) {
            *self.0.lock().unwrap() = Some(error.clone());
        }
    }

    #[test]
    fn response_future_accessors_mutations_and_callback_order_match_java_surface() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let callback = Arc::new(RecordingCallback(events.clone()));
        let request = RemotingCommand {
            code: 11,
            opaque: 42,
            ..Default::default()
        };
        let response = RemotingCommand {
            code: 12,
            opaque: 42,
            ..Default::default()
        };
        let channel = ChannelHandle::pending("broker:10911", 9);
        let future = ResponseFuture::new(
            Some(channel.clone()),
            42,
            Some(request),
            1000,
            Some(callback.clone()),
        );

        assert_eq!(future.opaque(), 42);
        assert_eq!(future.timeout_millis(), 1000);
        assert!(future.begin_timestamp_millis() > 0);
        assert!(Arc::ptr_eq(
            &future.invoke_callback().unwrap(),
            &(callback.clone() as Arc<dyn InvokeCallback>)
        ));
        let returned_channel = future.channel().unwrap();
        assert_eq!(returned_channel, channel);
        assert_eq!(returned_channel.remote_addr(), "broker:10911");
        assert_eq!(future.request_command().unwrap().code, 11);
        future.with_request_command_mut(|mut request| request.as_mut().unwrap().code = 13);
        assert_eq!(future.request_command().unwrap().code, 13);
        assert!(future.is_send_request_ok());
        future.set_send_request_ok(false);
        assert!(!future.is_send_request_ok());
        future.set_response_command(Some(response.clone()));
        future.with_response_command_mut(|mut value| {
            value.as_mut().unwrap().remark = Some("seen".into())
        });
        assert_eq!(
            future.response_command().unwrap().remark.as_deref(),
            Some("seen")
        );

        future.execute_invoke_callback();
        future.execute_invoke_callback();
        assert_eq!(*events.lock().unwrap(), vec!["succeed", "complete"]);
    }

    #[test]
    fn put_response_unblocks_waiters_and_setter_alone_does_not() {
        let response = RemotingCommand {
            code: 19,
            ..Default::default()
        };
        let future = Arc::new(ResponseFuture::new(None, 0, None, 1000, None));
        future.set_response_command(Some(response.clone()));
        let wait_future = future.clone();
        let waiter = std::thread::spawn(move || {
            let started = std::time::Instant::now();
            let response = wait_future.wait_response(25);
            (response, started.elapsed())
        });
        let (received, elapsed) = waiter.join().unwrap();
        assert_eq!(received, Some(response.clone()));
        assert!(elapsed >= std::time::Duration::from_millis(15));

        let future = Arc::new(ResponseFuture::new(None, 0, None, 1000, None));
        let wait_future = future.clone();
        let waiter = std::thread::spawn(move || wait_future.wait_response(1000));
        std::thread::sleep(std::time::Duration::from_millis(10));
        future.put_response(Some(response.clone()));
        assert_eq!(waiter.join().unwrap(), Some(response));
    }

    #[test]
    fn interruption_and_release_are_idempotent() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let future = ResponseFuture::new(
            None,
            0,
            None,
            1000,
            Some(Arc::new(RecordingCallback(events.clone()))),
        );
        future.interrupt();
        future.interrupt();
        future.release();
        future.release();
        assert!(future.is_interrupted());
        assert_eq!(*events.lock().unwrap(), vec!["fail", "complete"]);

        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let future = ResponseFuture::from_result(
            None,
            RemotingCommand::default(),
            Err(TransportError::Shutdown),
            1000,
            tokio::time::Instant::now(),
            Arc::new(|_: Result<RemotingCommand, TransportError>| {}),
            Some(permit),
        );
        assert_eq!(semaphore.available_permits(), 0);
        future.release();
        future.release();
        assert_eq!(semaphore.available_permits(), 1);

        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let future = ResponseFuture::new_with_permit(None, 0, None, 1000, None, Some(permit));
        assert_eq!(semaphore.available_permits(), 0);
        future.release();
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn direct_callback_failure_matches_java_send_timeout_and_generic_categories() {
        let send_error = Arc::new(Mutex::new(None));
        let send_future = ResponseFuture::new(
            Some(ChannelHandle::pending("broker:10911", 1)),
            0,
            Some(RemotingCommand::default()),
            1000,
            Some(Arc::new(FailureCallback(send_error.clone()))),
        );
        send_future.set_send_request_ok(false);
        send_future.execute_invoke_callback();
        assert_eq!(
            *send_error.lock().unwrap(),
            Some(TransportError::SendRequest {
                remote_addr: "broker:10911".into(),
                cause: None,
            })
        );

        let timeout_error = Arc::new(Mutex::new(None));
        let timeout_future = ResponseFuture::new(
            Some(ChannelHandle::pending("broker:10911", 2)),
            0,
            Some(RemotingCommand::default()),
            -1,
            Some(Arc::new(FailureCallback(timeout_error.clone()))),
        );
        timeout_future.execute_invoke_callback();
        assert!(matches!(
            timeout_error.lock().unwrap().as_ref(),
            Some(TransportError::TimeoutWithContext {
                phase: crate::error::TimeoutPhase::Response,
                remote_addr: Some(remote_addr),
                ..
            }) if remote_addr == "broker:10911"
        ));

        let generic_error = Arc::new(Mutex::new(None));
        let generic_future = ResponseFuture::new(
            None,
            0,
            Some(RemotingCommand {
                code: 42,
                ..Default::default()
            }),
            i64::MAX,
            Some(Arc::new(FailureCallback(generic_error.clone()))),
        );
        generic_future.execute_invoke_callback();
        assert!(matches!(
            generic_error.lock().unwrap().as_ref(),
            Some(TransportError::Remoting(request)) if request.contains("code: 42")
        ));
    }

    #[test]
    fn channel_handle_exposes_address_generation_state_and_close() {
        let channel = ChannelHandle::pending("tls://user:secret@broker:10911", 7);
        assert_eq!(channel, channel.clone());
        assert_ne!(
            channel,
            ChannelHandle::pending("tls://user:secret@broker:10911", 7)
        );
        assert_eq!(channel.remote_addr(), "tls://[REDACTED]@broker:10911");
        assert_eq!(channel.generation(), 7);
        assert!(channel.is_open());
        assert!(!channel.is_active());
        assert!(!channel.is_writable());
        assert!(channel.close());
        assert!(!channel.close());
        assert!(!channel.is_open());
    }
}
