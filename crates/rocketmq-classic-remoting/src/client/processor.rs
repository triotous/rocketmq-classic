use std::{
    future::Future,
    sync::{Arc, Weak},
};

use rocketmq_classic_protocol::codes::response;
use tokio::sync::mpsc;

use crate::{command::RemotingCommand, error::TransportError};

use super::{ChannelHandle, ClientInner, ProcessorRegistration};

/// Request-scoped access to the channel that delivered an inbound command.
///
/// The channel handle preserves connection identity and supports address inspection, closing,
/// and outbound command writes. `write_and_flush` enqueues immediately and returns a future for
/// the local write result; it has no operation deadline and does not imply remote receipt.
#[derive(Clone, Debug)]
pub struct RequestContext {
    channel: ChannelHandle,
}

impl RequestContext {
    pub(super) fn new(channel: ChannelHandle) -> Self {
        Self { channel }
    }

    /// Returns the identity-preserving channel handle for the inbound connection.
    pub fn channel(&self) -> &ChannelHandle {
        &self.channel
    }

    /// Returns the peer address recorded for this channel, with URI credentials redacted.
    pub fn remote_addr(&self) -> &str {
        self.channel.remote_addr()
    }

    /// Closes this channel and lets the connection owner complete pending requests.
    pub fn close(&self) -> bool {
        self.channel.close()
    }

    /// Enqueues a command on this channel without an operation deadline.
    ///
    /// The returned future reports the local socket write result. Dropping it does not cancel the
    /// already-enqueued write, matching the fire-and-forget use of Java's `writeAndFlush` future.
    pub fn write_and_flush(
        &self,
        command: &RemotingCommand,
    ) -> Result<impl Future<Output = Result<(), TransportError>> + Send + 'static, TransportError>
    {
        self.channel.enqueue_and_flush(command)
    }
}

pub trait RequestProcessor: Send + Sync + 'static {
    fn process(&self, request: RemotingCommand) -> Result<Option<RemotingCommand>, String>;

    /// Channel identity needed by transaction checks and other broker callbacks.
    fn process_with_context(
        &self,
        _remote_addr: &str,
        request: RemotingCommand,
    ) -> Result<Option<RemotingCommand>, String> {
        self.process(request)
    }

    /// Processes a command with its inbound channel context.
    ///
    /// The default preserves implementations of `process_with_context` by forwarding the peer
    /// address. Implementations that need channel operations can override this method.
    fn process_with_request_context(
        &self,
        context: &RequestContext,
        request: RemotingCommand,
    ) -> Result<Option<RemotingCommand>, String> {
        self.process_with_context(context.remote_addr(), request)
    }

    fn reject_request(&self) -> bool {
        false
    }
}

impl<F> RequestProcessor for F
where
    F: Fn(RemotingCommand) -> Result<Option<RemotingCommand>, String> + Send + Sync + 'static,
{
    fn process(&self, request: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
        self(request)
    }
}

#[derive(Clone)]
pub(super) struct InboundRequest {
    pub generation: u64,
    pub remote_addr: String,
    pub channel: ChannelHandle,
    pub command: RemotingCommand,
}

pub(super) fn start(
    inner: Weak<ClientInner>,
    mut receiver: mpsc::UnboundedReceiver<InboundRequest>,
    worker_count: usize,
) -> Vec<tokio::task::JoinHandle<()>> {
    let Some(client) = inner.upgrade() else {
        return Vec::new();
    };
    let shutdown_sender = client.shutdown.clone();
    let (work, worker_rx) = mpsc::unbounded_channel();
    let worker_rx = Arc::new(tokio::sync::Mutex::new(worker_rx));
    let mut tasks = Vec::with_capacity(worker_count + 1);
    let mut shutdown = shutdown_sender.subscribe();
    tasks.push(tokio::spawn(async move {
        loop {
            tokio::select! {
                request = receiver.recv() => match request {
                    Some(request) => {
                        let _ = work.send(request);
                    }
                    None => return,
                },
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        receiver.close();
                        while let Some(request) = receiver.recv().await {
                            if work.send(request).is_err() {
                                return;
                            }
                        }
                        return;
                    }
                },
            }
        }
    }));
    for _ in 0..worker_count {
        let inner = inner.clone();
        let receiver = worker_rx.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let request = receiver.lock().await.recv().await;
                match request {
                    Some(request) => process_one(&inner, request),
                    None => return,
                }
            }
        }));
    }
    tasks
}

pub(super) fn process_one(inner: &Weak<ClientInner>, request: InboundRequest) {
    let Some(client) = inner.upgrade() else {
        return;
    };
    let registration = client
        .processors
        .lock()
        .unwrap()
        .get(&request.command.code)
        .cloned()
        .or_else(|| client.default_processor.lock().unwrap().clone());
    let Some(registration) = registration else {
        let remark = format!(" request type {} not supported", request.command.code);
        send_response(
            inner,
            request,
            error_response(response::REQUEST_CODE_NOT_SUPPORTED, &remark),
        );
        return;
    };
    if registration.processor.reject_request() {
        send_response(
            inner,
            request,
            error_response(
                response::SYSTEM_BUSY,
                "[REJECTREQUEST]system busy, start flow control for a while",
            ),
        );
        return;
    }

    let executor = registration
        .executor
        .clone()
        .unwrap_or_else(|| client.public_executor.clone());
    let request_for_rejection = request.clone();
    let task_inner = inner.clone();
    let task: Box<dyn FnOnce() + Send + 'static> = Box::new(move || {
        process_registered(&task_inner, request, registration);
    });
    {
        let task = Arc::new(std::sync::Mutex::new(Some(task)));
        let task_for_executor = task.clone();
        let dispatched: Box<dyn FnOnce() + Send + 'static> = Box::new(move || {
            if let Some(task) = task_for_executor.lock().unwrap().take() {
                task();
            }
        });
        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            executor.execute(dispatched)
        }))
        .map_or(true, |result| result.is_err());
        if rejected && task.lock().unwrap().take().is_some() {
            send_response(
                inner,
                request_for_rejection,
                error_response(
                    response::SYSTEM_BUSY,
                    "[OVERLOAD]system busy, start flow control for a while",
                ),
            );
        }
    }
}

fn process_registered(
    inner: &Weak<ClientInner>,
    mut request: InboundRequest,
    registration: ProcessorRegistration,
) {
    let request_context = RequestContext::new(request.channel.clone());
    let mut response = None;
    let mut run_after_hooks = false;
    let mut request_for_hooks = request.command.clone();
    if let Some(client) = inner.upgrade() {
        let hooks = client.rpc_hooks.lock().unwrap().clone();
        let before_result = hooks.iter().try_for_each(|hook| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hook.before_request_mut_result(&request.remote_addr, &mut request_for_hooks)
            }))
            .unwrap_or_else(|payload| Err(super::RpcHookError::Failed(panic_message(payload))))
        });
        let before_failure = match before_result {
            Err(super::RpcHookError::Abort {
                response_code,
                remark,
            }) => {
                response = Some(error_response(
                    response_code,
                    remark.as_deref().unwrap_or("RPC hook aborted request"),
                ));
                None
            }
            Err(super::RpcHookError::Failed(message)) => Some(message),
            Ok(()) => None,
        };
        if response.is_none() {
            let pipeline = client.request_pipeline.lock().unwrap().clone();
            let pipeline_result = if let Some(pipeline) = pipeline {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    pipeline.execute_detailed_with_context(&request_context, &request_for_hooks)
                }))
                .unwrap_or_else(|payload| {
                    Err(super::RequestPipelineError::Failed(panic_message(payload)))
                })
            } else {
                Ok(())
            };
            match pipeline_result {
                Err(super::RequestPipelineError::Abort {
                    response_code,
                    remark,
                }) => {
                    response = Some(error_response(
                        response_code,
                        remark.as_deref().unwrap_or("request pipeline aborted"),
                    ));
                }
                Err(super::RequestPipelineError::Failed(message)) => {
                    response = Some(error_response(response::SYSTEM_ERROR, &message));
                }
                Ok(()) if before_failure.is_some() => {
                    response = Some(error_response(
                        response::SYSTEM_ERROR,
                        before_failure.as_deref().unwrap_or_default(),
                    ));
                    run_after_hooks = true;
                }
                Ok(()) => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        registration.processor.process_with_request_context(
                            &request_context,
                            request_for_hooks.clone(),
                        )
                    }));
                    match result {
                        Ok(Ok(processed_response)) => {
                            response = processed_response;
                            run_after_hooks = true;
                        }
                        Ok(Err(message)) => {
                            response = Some(error_response(response::SYSTEM_ERROR, &message));
                        }
                        Err(payload) => {
                            response = Some(error_response(
                                response::SYSTEM_ERROR,
                                &panic_message(payload),
                            ));
                        }
                    }
                }
            }
        }
    }
    if run_after_hooks {
        if let Some(client) = inner.upgrade() {
            for hook in client.rpc_hooks.lock().unwrap().clone() {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    hook.after_response_optional_result(
                        &request.remote_addr,
                        &request_for_hooks,
                        response.as_ref(),
                    )
                }))
                .unwrap_or_else(|payload| Err(super::RpcHookError::Failed(panic_message(payload))));
                match result {
                    Ok(()) => {}
                    Err(super::RpcHookError::Abort {
                        response_code,
                        remark,
                    }) => {
                        response = Some(error_response(
                            response_code,
                            remark.as_deref().unwrap_or("RPC hook aborted response"),
                        ));
                        break;
                    }
                    Err(super::RpcHookError::Failed(message)) => {
                        response = Some(error_response(response::SYSTEM_ERROR, &message));
                        break;
                    }
                }
            }
        }
    }
    request.command = request_for_hooks;
    if let Some(response) = response {
        send_response(inner, request, response);
    }
}

fn send_response(
    inner: &Weak<ClientInner>,
    request: InboundRequest,
    mut response: RemotingCommand,
) {
    if request.command.is_oneway() {
        return;
    }
    let Some(client) = inner.upgrade() else {
        return;
    };
    response.opaque = request.command.opaque;
    response.mark_response();
    let connection = client
        .connections
        .lock()
        .unwrap()
        .values()
        .find(|connection| connection.generation == request.generation)
        .cloned();
    if let Some(connection) = connection {
        match connection.encode_command(&response, crate::codec::frame::JAVA_ENCODER_MAX_FRAME_SIZE)
        {
            Ok(encoded) => {
                let Some(runtime) = client.runtime.lock().unwrap().clone() else {
                    return;
                };
                let task = runtime.spawn(async move {
                    let _ = connection.send_without_deadline(encoded).await;
                });
                client.spawn(task);
            }
            Err(error) => client.connection_closed(request.generation, Some(error)),
        }
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).into()
    } else {
        "request processing panicked".into()
    }
}

fn error_response(code: i32, remark: &str) -> RemotingCommand {
    RemotingCommand {
        code,
        remark: Some(remark.into()),
        ..RemotingCommand::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LegacyProcessor;

    impl RequestProcessor for LegacyProcessor {
        fn process(&self, _: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
            panic!("the legacy context method should handle this call")
        }

        fn process_with_context(
            &self,
            remote_addr: &str,
            _: RemotingCommand,
        ) -> Result<Option<RemotingCommand>, String> {
            Ok(Some(RemotingCommand {
                remark: Some(remote_addr.to_owned()),
                ..RemotingCommand::default()
            }))
        }
    }

    #[test]
    fn request_context_adapts_existing_remote_address_processors() {
        let context = RequestContext::new(ChannelHandle::pending("127.0.0.1:10911", 7));
        let response = LegacyProcessor
            .process_with_request_context(&context, RemotingCommand::default())
            .unwrap()
            .unwrap();

        assert_eq!(response.remark.as_deref(), Some("127.0.0.1:10911"));
    }
}
