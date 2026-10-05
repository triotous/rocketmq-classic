use super::{
    ClientRuntime, ConsumerRuntime, GroupLease, ProducerRuntime, RuntimeError, registry::GroupKind,
};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicU8, Ordering},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Duration, Instant},
};

const INNER_PRODUCER: &str = "CLIENT_INNER_PRODUCER";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RuntimeState {
    Created,
    Starting,
    Running,
    ShuttingDown,
    Stopped,
    Failed,
}

pub(super) enum Command {
    RegisterProducer {
        group: String,
        handle: Arc<dyn ProducerRuntime>,
        reply: oneshot::Sender<Result<PendingLease, RuntimeError>>,
    },
    RegisterConsumer {
        group: String,
        handle: Arc<dyn ConsumerRuntime>,
        reply: oneshot::Sender<Result<PendingLease, RuntimeError>>,
    },
    RegisterAdmin {
        group: String,
        reply: oneshot::Sender<Result<PendingLease, RuntimeError>>,
    },
    Unregister {
        lease: GroupLease,
        kind: GroupKind,
        deadline: Instant,
        reply: Option<oneshot::Sender<Result<bool, RuntimeError>>>,
    },
    Start {
        reply: oneshot::Sender<Result<(), RuntimeError>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<bool, RuntimeError>>,
    },
}

/// A response cancelled before the receiving future can accept the lease must
/// undo its registration. Drop enqueues cleanup into the existing owned worker.
pub(super) struct PendingLease {
    lease: Option<GroupLease>,
    sender: mpsc::UnboundedSender<Command>,
    timeout: Duration,
}

impl PendingLease {
    pub fn accept(mut self) -> Result<GroupLease, RuntimeError> {
        self.lease.take().ok_or(RuntimeError::RegistryPoisoned)
    }
}

impl Drop for PendingLease {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            let kind = lease.kind;
            let _ = self.sender.send(Command::Unregister {
                lease,
                kind,
                deadline: Instant::now() + self.timeout,
                reply: None,
            });
        }
    }
}

pub(super) struct ControlPlane {
    state: AtomicU8,
    sender: Mutex<Option<mpsc::UnboundedSender<Command>>>,
    task: Mutex<Option<JoinHandle<()>>>,
    join_gate: tokio::sync::Mutex<()>,
    #[cfg(test)]
    pub fault: AtomicU8,
}

impl Default for ControlPlane {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(RuntimeState::Created as u8),
            sender: Mutex::new(None),
            task: Mutex::new(None),
            join_gate: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            fault: AtomicU8::new(0),
        }
    }
}

impl Drop for ControlPlane {
    fn drop(&mut self) {
        if let Ok(task) = self.task.get_mut() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
    }
}

impl ControlPlane {
    pub fn state(&self) -> RuntimeState {
        match self.state.load(Ordering::Acquire) {
            0 => RuntimeState::Created,
            1 => RuntimeState::Starting,
            2 => RuntimeState::Running,
            3 => RuntimeState::ShuttingDown,
            4 => RuntimeState::Stopped,
            _ => RuntimeState::Failed,
        }
    }

    fn set_state(&self, state: RuntimeState) {
        self.state.store(state as u8, Ordering::Release);
    }

    pub fn sender(
        &self,
        runtime: &Arc<ClientRuntime>,
    ) -> Result<mpsc::UnboundedSender<Command>, RuntimeError> {
        let mut sender = self
            .sender
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if let Some(sender) = sender.as_ref() {
            return Ok(sender.clone());
        }
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            rocketmq_classic_remoting::TransportError::InvalidConfiguration(
                "runtime lifecycle requires an active Tokio runtime".into(),
            )
        })?;
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = handle.spawn(run(Arc::downgrade(runtime), tx.downgrade(), rx));
        *self
            .task
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)? = Some(worker);
        *sender = Some(tx.clone());
        Ok(tx)
    }

    pub async fn join_terminal(&self) -> Result<(), RuntimeError> {
        if !matches!(self.state(), RuntimeState::Stopped | RuntimeState::Failed) {
            return Ok(());
        }
        // Poll the owned handle in place. Cancelling a joining caller must not
        // take/drop (detach) the only handle. The async gate serializes wakers.
        let _join = self.join_gate.lock().await;
        std::future::poll_fn(|context| {
            use std::{future::Future, pin::Pin, task::Poll};
            let mut owned = match self.task.lock() {
                Ok(owned) => owned,
                Err(_) => return Poll::Ready(Err(RuntimeError::RegistryPoisoned)),
            };
            let Some(task) = owned.as_mut() else {
                return Poll::Ready(Ok(()));
            };
            match Pin::new(task).poll(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    owned.take();
                    Poll::Ready(result.map_err(|_| RuntimeError::ControlTaskFailed))
                }
            }
        })
        .await
    }

    fn checkpoint(&self, _step: u8) -> Result<(), RuntimeError> {
        #[cfg(test)]
        if self.fault.load(Ordering::Acquire) == _step {
            return Err(RuntimeError::StartupFailure);
        }
        Ok(())
    }
}

struct InnerProducer;
impl ProducerRuntime for InnerProducer {}

async fn run(
    runtime: Weak<ClientRuntime>,
    sender: mpsc::WeakUnboundedSender<Command>,
    mut receiver: mpsc::UnboundedReceiver<Command>,
) {
    let mut inner_producer: Option<Arc<dyn ProducerRuntime>> = None;
    let mut inner_lease: Option<GroupLease> = None;
    while let Some(command) = receiver.recv().await {
        let Some(runtime) = runtime.upgrade() else {
            return;
        };
        let state = runtime.control.state();
        let allowed = matches!(state, RuntimeState::Created | RuntimeState::Running);
        match command {
            Command::RegisterProducer {
                group,
                handle,
                reply,
            } => {
                if reply.is_closed() {
                    continue;
                }
                let result = if allowed {
                    runtime.groups.register_producer(&group, handle)
                } else {
                    Err(RuntimeError::InvalidState(state))
                };
                deliver(result, reply, &sender, &runtime);
            }
            Command::RegisterConsumer {
                group,
                handle,
                reply,
            } => {
                if reply.is_closed() {
                    continue;
                }
                let result = if allowed {
                    runtime.groups.register_consumer(&group, handle)
                } else {
                    Err(RuntimeError::InvalidState(state))
                };
                deliver(result, reply, &sender, &runtime);
            }
            Command::RegisterAdmin { group, reply } => {
                if reply.is_closed() {
                    continue;
                }
                let result = if allowed {
                    runtime.groups.register_admin(&group)
                } else {
                    Err(RuntimeError::InvalidState(state))
                };
                deliver(result, reply, &sender, &runtime);
            }
            Command::Unregister {
                lease,
                kind,
                deadline,
                reply,
            } => {
                let result = runtime.unregister_group(&lease, kind, deadline).await;
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
            }
            Command::Start { reply } => {
                if reply.is_closed() {
                    continue;
                }
                if state == RuntimeState::Running {
                    let _ = reply.send(Ok(()));
                    continue;
                }
                if state != RuntimeState::Created {
                    let _ = reply.send(Err(RuntimeError::InvalidState(state)));
                    continue;
                }
                runtime.control.set_state(RuntimeState::Starting);
                let producer: Arc<dyn ProducerRuntime> = Arc::new(InnerProducer);
                let result = (|| {
                    inner_lease = Some(
                        runtime
                            .groups
                            .register_producer(INNER_PRODUCER, producer.clone())?,
                    );
                    runtime.control.checkpoint(1)?;
                    runtime.remoting().start()?;
                    runtime.control.checkpoint(2)?;
                    Ok(())
                })();
                match result {
                    Ok(()) => {
                        inner_producer = Some(producer);
                        runtime.control.set_state(RuntimeState::Running);
                        let _ = reply.send(Ok(()));
                    }
                    Err(error) => {
                        runtime.remoting().shutdown().await;
                        let _ = runtime.groups.clear();
                        runtime.control.set_state(RuntimeState::Failed);
                        let _ = runtime.remove_from_manager();
                        let _ = reply.send(Err(error));
                        break;
                    }
                }
            }
            Command::Shutdown { reply } => {
                if runtime
                    .groups
                    .has_active_users(INNER_PRODUCER)
                    .unwrap_or(true)
                    || state != RuntimeState::Running
                {
                    let _ = reply.send(Ok(false));
                    continue;
                }
                runtime.control.set_state(RuntimeState::ShuttingDown);
                if let Some(lease) = inner_lease.take() {
                    let deadline = Instant::now() + runtime.rpc_timeout();
                    let _ = runtime
                        .unregister_group(&lease, GroupKind::Producer, deadline)
                        .await;
                }
                inner_producer.take();
                runtime.remoting().shutdown().await;
                runtime.control.set_state(RuntimeState::Stopped);
                let result = runtime.remove_from_manager().map(|_| true);
                let _ = reply.send(result);
                break;
            }
        }
    }
}

fn deliver(
    result: Result<GroupLease, RuntimeError>,
    reply: oneshot::Sender<Result<PendingLease, RuntimeError>>,
    sender: &mpsc::WeakUnboundedSender<Command>,
    runtime: &ClientRuntime,
) {
    let result = result.and_then(|lease| {
        let sender = sender.upgrade().ok_or(RuntimeError::ControlTaskFailed)?;
        Ok(PendingLease {
            lease: Some(lease),
            sender,
            timeout: runtime.rpc_timeout(),
        })
    });
    let _ = reply.send(result); // Undelivered PendingLease enqueues identity cleanup.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::ClientConfig, runtime::ClientRuntimeManager};

    #[tokio::test]
    async fn every_initial_startup_failure_cleans_transport_groups_control_and_manager_identity() {
        for point in [1, 2] {
            let manager = ClientRuntimeManager::new();
            let config = ClientConfig::new(format!("failure-{point}"));
            let runtime = manager.get_or_create(&config).await.unwrap();
            let producer: Arc<dyn ProducerRuntime> = Arc::new(InnerProducer);
            let lease = runtime
                .register_producer("producer", producer.clone())
                .await
                .unwrap();
            runtime.control.fault.store(point, Ordering::Release);
            assert!(matches!(
                runtime.start().await,
                Err(RuntimeError::StartupFailure)
            ));
            assert_eq!(runtime.state(), RuntimeState::Failed);
            assert!(runtime.control.task.lock().unwrap().is_none());
            assert!(
                runtime
                    .prepare_heartbeat_data(false)
                    .unwrap()
                    .producer_data_set
                    .is_empty()
            );
            assert!(
                !runtime
                    .unregister_producer(&lease, Instant::now())
                    .await
                    .unwrap()
            );
            assert!(matches!(
                runtime.start().await,
                Err(RuntimeError::InvalidState(RuntimeState::Failed))
            ));
            let replacement = manager.get_or_create(&config).await.unwrap();
            assert!(!Arc::ptr_eq(&runtime, &replacement));
        }
    }

    #[tokio::test]
    async fn abandoned_registration_delivery_rolls_back_through_the_owned_worker() {
        let manager = ClientRuntimeManager::new();
        let config = ClientConfig::new("delivery-cancellation");
        let runtime = manager.get_or_create(&config).await.unwrap();
        let sender = runtime.control.sender(&runtime).unwrap();
        let producer: Arc<dyn ProducerRuntime> = Arc::new(InnerProducer);
        let (reply, response) = oneshot::channel();
        sender
            .send(Command::RegisterProducer {
                group: "producer".into(),
                handle: producer.clone(),
                reply,
            })
            .unwrap_or_else(|_| panic!("owned worker must be available"));
        let delivery = response.await.unwrap().unwrap();
        assert!(runtime.select_producer("producer").unwrap().is_some());
        drop(delivery); // Caller never accepts the delivered identity.
        let replacement = runtime
            .register_producer("producer", producer.clone())
            .await
            .unwrap();
        assert_eq!(replacement.group(), "producer");
        runtime
            .unregister_producer(&replacement, Instant::now())
            .await
            .unwrap();
        runtime.start().await.unwrap();
        assert!(runtime.shutdown().await.unwrap());
        assert!(runtime.control.task.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn cancelled_terminal_join_keeps_the_owned_handle_for_another_joiner() {
        let control = Arc::new(ControlPlane::default());
        control.set_state(RuntimeState::Stopped);
        let (release, ready) = oneshot::channel();
        *control.task.lock().unwrap() = Some(tokio::spawn(async move {
            let _ = ready.await;
        }));
        let joining = control.clone();
        let first = tokio::spawn(async move { joining.join_terminal().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while control.join_gate.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        first.abort();
        assert!(matches!(first.await, Err(error) if error.is_cancelled()));
        assert!(control.task.lock().unwrap().is_some());
        let joining = control.clone();
        let mut second = tokio::spawn(async move { joining.join_terminal().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut second)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        second.await.unwrap().unwrap();
        assert!(control.task.lock().unwrap().is_none());
    }
}
