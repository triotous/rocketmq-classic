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
    StartPullScheduler,
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
    let mut periodic: Option<super::tasks::OwnedTasks> = None;
    let mut inbound = Vec::new();
    let mut rebalance: Option<super::rebalance::OwnedRebalance> = None;
    let mut pull_workers: [Option<super::pull::OwnedPullWorker>; 2] = [None, None];
    let mut pull_order = Vec::new();
    while let Some(command) = receiver.recv().await {
        let Some(runtime) = runtime.upgrade() else {
            return;
        };
        let state = runtime.control.state();
        let allowed = matches!(state, RuntimeState::Created | RuntimeState::Running);
        match command {
            Command::StartPullScheduler => {
                if allowed && pull_workers[1].is_none() {
                    let worker = super::pull::OwnedPullWorker::new(
                        Arc::downgrade(&runtime),
                        runtime.pull.clone(),
                        true,
                    );
                    worker.activate();
                    pull_workers[1] = Some(worker);
                    pull_order.push(1);
                }
            }
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
                let result = async {
                    inner_lease = Some(
                        runtime
                            .groups
                            .register_producer(INNER_PRODUCER, producer.clone())?,
                    );
                    runtime.control.checkpoint(1)?;
                    for (index, code) in super::inbound::CODES.into_iter().enumerate() {
                        inbound.push(
                            runtime
                                .remoting()
                                .register_owned_processor(code, runtime.inbound.clone()),
                        );
                        runtime.control.checkpoint(10 + index as u8)?;
                    }
                    if runtime.config().namesrv_addr.is_none() {
                        runtime.fetch_name_server_addr().await;
                    }
                    runtime.remoting().start()?;
                    runtime.control.checkpoint(2)?;
                    let specs = super::tasks::schedules(runtime.config())?;
                    periodic = Some(super::tasks::OwnedTasks::new(Arc::downgrade(&runtime)));
                    for (index, schedule) in specs.into_iter().enumerate() {
                        if let Some(tasks) = periodic.as_mut() {
                            tasks.register(schedule)?;
                        }
                        runtime.control.checkpoint(3 + index as u8)?;
                    }
                    for (scheduled, checkpoint) in [(false, 21), (true, 22)] {
                        let slot = &mut pull_workers[usize::from(scheduled)];
                        if slot.is_none() {
                            *slot = Some(super::pull::OwnedPullWorker::new(
                                Arc::downgrade(&runtime),
                                runtime.pull.clone(),
                                scheduled,
                            ));
                            pull_order.push(usize::from(scheduled));
                        }
                        runtime.control.checkpoint(checkpoint)?;
                    }
                    rebalance = Some(super::rebalance::OwnedRebalance::new(
                        Arc::downgrade(&runtime),
                        runtime.rebalance.clone(),
                    ));
                    runtime.control.checkpoint(20)?;
                    Ok(())
                }
                .await;
                match result {
                    Ok(()) => {
                        inner_producer = Some(producer);
                        runtime.control.set_state(RuntimeState::Running);
                        if let Some(tasks) = periodic.as_ref() {
                            tasks.activate();
                        }
                        if let Some(worker) = rebalance.as_ref() {
                            worker.activate();
                        }
                        for worker in pull_workers.iter().flatten() {
                            worker.activate();
                        }
                        let _ = reply.send(Ok(()));
                    }
                    Err(error) => {
                        runtime.pull.close();
                        runtime.rebalance.close_delayed();
                        if let Some(worker) = rebalance.as_mut() {
                            worker.stop().await;
                        }
                        for index in pull_order.iter().rev() {
                            if let Some(worker) = pull_workers[*index].as_mut() {
                                worker.stop().await;
                            }
                        }
                        if let Some(tasks) = periodic.as_mut() {
                            tasks.stop().await;
                        }
                        runtime.remoting().shutdown().await;
                        inbound.clear();
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
                runtime.rebalance.close_delayed();
                runtime.pull.stop_accepting();
                if let Some(worker) = rebalance.as_mut() {
                    worker.stop().await;
                }
                for (index, worker) in pull_workers.iter_mut().enumerate() {
                    let Some(worker) = worker else {
                        continue;
                    };
                    if index == 0 {
                        worker.stop().await;
                    } else {
                        worker.finish_gracefully().await;
                    }
                }
                runtime.pull.close();
                if let Some(tasks) = periodic.as_mut() {
                    tasks.stop().await;
                }
                if let Some(lease) = inner_lease.take() {
                    let deadline = Instant::now() + runtime.rpc_timeout();
                    let _ = runtime
                        .unregister_group(&lease, GroupKind::Producer, deadline)
                        .await;
                }
                inner_producer.take();
                runtime.remoting().shutdown().await;
                inbound.clear();
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

    struct BlankAddressing;
    impl crate::namesrv::TopAddressing for BlankAddressing {
        fn fetch_ns_addr(
            &self,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send + '_>>
        {
            Box::pin(async { Some(" ".into()) })
        }
    }

    #[tokio::test]
    async fn discovery_schedule_failure_joins_all_owned_work() {
        for point in [
            1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22,
        ] {
            let manager = ClientRuntimeManager::with_top_addressing(Arc::new(BlankAddressing));
            let config = ClientConfig::new("discovery-failure");
            let runtime = manager.get_or_create(&config).await.unwrap();
            runtime.rebalance_later(60_000).unwrap();
            runtime.execute_task_later(async {}, 60_000).unwrap();
            runtime.control.fault.store(point, Ordering::SeqCst);
            assert!(matches!(
                runtime.start().await,
                Err(RuntimeError::StartupFailure)
            ));
            assert_eq!(runtime.state(), RuntimeState::Failed);
            assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
            assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 0);
            assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 0);
            assert!(runtime.pull.is_empty());
            assert!(runtime.rebalance.next_due().is_none());
            assert!(runtime.control.task.lock().unwrap().is_none());
            assert_eq!(
                runtime.remoting().with_processor_table(|table| table.len()),
                0
            );
            assert!(
                runtime
                    .prepare_heartbeat_data(false)
                    .unwrap()
                    .producer_data_set
                    .is_empty()
            );
            let replacement = manager.get_or_create(&config).await.unwrap();
            assert!(!Arc::ptr_eq(&runtime, &replacement));
        }
    }

    #[tokio::test]
    async fn concurrent_starters_own_exactly_one_periodic_worker_and_join_it() {
        let manager = ClientRuntimeManager::new();
        let mut config = ClientConfig::new("one-task-set");
        config.namesrv_addr = Some(String::new());
        let runtime = manager.get_or_create(&config).await.unwrap();
        let mut calls = Vec::new();
        for _ in 0..64 {
            let runtime = runtime.clone();
            calls.push(tokio::spawn(async move {
                runtime.start().await.unwrap();
            }));
        }
        for call in calls {
            call.await.unwrap();
        }
        assert_eq!(runtime.periodic_probe.created.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.rebalance_probe.created.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.pull_probe.created.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 2);
        assert!(runtime.shutdown().await.unwrap());
        assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn every_initial_startup_failure_cleans_transport_groups_control_and_manager_identity() {
        for point in [1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22] {
            let manager = ClientRuntimeManager::new();
            let mut config = ClientConfig::new(format!("failure-{point}"));
            config.namesrv_addr = Some(String::new());
            let runtime = manager.get_or_create(&config).await.unwrap();
            let producer: Arc<dyn ProducerRuntime> = Arc::new(InnerProducer);
            let lease = runtime
                .register_producer("producer", producer.clone())
                .await
                .unwrap();
            runtime.control.fault.store(point, Ordering::Release);
            runtime.rebalance_later(60_000).unwrap();
            runtime.execute_task_later(async {}, 60_000).unwrap();
            assert!(matches!(
                runtime.start().await,
                Err(RuntimeError::StartupFailure)
            ));
            assert_eq!(runtime.state(), RuntimeState::Failed);
            assert!(runtime.control.task.lock().unwrap().is_none());
            assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
            assert_eq!(runtime.rebalance_probe.live.load(Ordering::SeqCst), 0);
            assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 0);
            assert!(runtime.pull.is_empty());
            assert!(runtime.rebalance.next_due().is_none());
            assert_eq!(
                runtime.remoting().with_processor_table(|table| table.len()),
                0
            );
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
        let mut config = ClientConfig::new("delivery-cancellation");
        config.namesrv_addr = Some(String::new());
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
