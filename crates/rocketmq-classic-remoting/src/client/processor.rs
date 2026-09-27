use std::sync::{Arc, Weak};

use tokio::sync::mpsc;

use crate::command::RemotingCommand;

use super::{ClientInner, ProcessorRegistration};

pub trait RequestProcessor: Send + Sync + 'static {
    fn process(&self, request: RemotingCommand) -> Result<Option<RemotingCommand>, String>;

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
    pub command: RemotingCommand,
}

pub(super) fn start(
    inner: Weak<ClientInner>,
    mut receiver: mpsc::UnboundedReceiver<InboundRequest>,
    worker_count: usize,
    disable_worker_group: bool,
) -> Vec<tokio::task::JoinHandle<()>> {
    let Some(client) = inner.upgrade() else {
        return Vec::new();
    };
    let shutdown_sender = client.shutdown.clone();
    if disable_worker_group {
        return Vec::new();
    }
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
        send_response(
            inner,
            request,
            error_response(
                response::REQUEST_CODE_NOT_SUPPORTED,
                "request code not supported",
            ),
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
    request: InboundRequest,
    registration: ProcessorRegistration,
) {
    let request_command = request.command.clone();
    let mut response = None;
    let mut run_after_hooks = false;
    let request_for_hooks = request_command.clone();
    if let Some(client) = inner.upgrade() {
        let hooks = client.rpc_hooks.lock().unwrap().clone();
        let before_result = hooks.iter().try_for_each(|hook| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hook.before_request_result(&request.remote_addr, &request_for_hooks)
            }))
            .unwrap_or_else(|_| {
                Err(super::RpcHookError::Failed(
                    "RPC before hook panicked".into(),
                ))
            })
        });
        let before_failed = match before_result {
            Err(super::RpcHookError::Abort {
                response_code,
                remark,
            }) => {
                response = Some(error_response(
                    response_code,
                    remark.as_deref().unwrap_or("RPC hook aborted request"),
                ));
                false
            }
            Err(super::RpcHookError::Failed(_)) => true,
            Ok(()) => false,
        };
        if response.is_none() {
            let pipeline = client.request_pipeline.lock().unwrap().clone();
            let pipeline_result = if let Some(pipeline) = pipeline {
                let context = super::RequestContext {
                    remote_addr: request.remote_addr.clone(),
                    connection_generation: request.generation,
                };
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    pipeline.execute_detailed(&context, &request_for_hooks)
                }))
                .unwrap_or_else(|_| {
                    Err(super::RequestPipelineError::Failed(
                        "request pipeline panicked".into(),
                    ))
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
                Err(super::RequestPipelineError::Failed(_)) => {
                    response = Some(error_response(response::SYSTEM_ERROR, "system error"));
                }
                Ok(()) if before_failed => {
                    response = Some(error_response(response::SYSTEM_ERROR, "system error"));
                    run_after_hooks = true;
                }
                Ok(()) => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        registration.processor.process(request_command.clone())
                    }));
                    match result {
                        Ok(Ok(processed_response)) => {
                            response = processed_response;
                            run_after_hooks = true;
                        }
                        Ok(Err(_)) | Err(_) => {
                            response = Some(error_response(response::SYSTEM_ERROR, "system error"));
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
                        &request_command,
                        response.as_ref(),
                    )
                }))
                .unwrap_or_else(|_| {
                    Err(super::RpcHookError::Failed(
                        "RPC after hook panicked".into(),
                    ))
                });
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
                    Err(super::RpcHookError::Failed(_)) => {
                        response = Some(error_response(response::SYSTEM_ERROR, "system error"));
                        break;
                    }
                }
            }
        }
    }
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
        match connection.encode_command(&response, client.max_frame_size) {
            Ok(encoded) => {
                if let Err(error) = connection.try_send(
                    encoded,
                    tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                ) {
                    client.connection_closed(request.generation, Some(error));
                }
            }
            Err(error) => client.connection_closed(request.generation, Some(error)),
        }
    }
}

fn error_response(code: i32, remark: &str) -> RemotingCommand {
    RemotingCommand {
        code,
        remark: Some(remark.into()),
        ..RemotingCommand::default()
    }
}

use rocketmq_classic_protocol::codes::response;
