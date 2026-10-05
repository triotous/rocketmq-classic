use super::{ClientRuntime, RuntimeError, RuntimeState};
use rocketmq_classic_protocol::body::MessageRequestMode;
use std::{
    any::Any,
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::Poll,
};
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
    time::{Duration, Instant},
};

/// Routing-only contract; concrete PullRequest/PopRequest state belongs to
/// consumer phases, not a second Phase 6 consumer scheduler.
pub trait MessageRequest: Any + Send + Sync {
    fn consumer_group(&self) -> &str;
    fn mode(&self) -> MessageRequestMode;
    fn as_any(&self) -> &dyn Any;
}
type Task = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
enum Job {
    Request(Arc<dyn MessageRequest>),
    Task(Task),
}
#[derive(Default)]
struct Queues {
    stopped: bool,
    immediate: VecDeque<Arc<dyn MessageRequest>>,
    scheduled: BTreeMap<(Instant, u64), Job>,
    sequence: u64,
}
#[derive(Default)]
pub(super) struct PullService {
    queues: Mutex<Queues>,
    immediate: Notify,
    scheduled: Notify,
}
impl PullService {
    pub fn enqueue(&self, request: Arc<dyn MessageRequest>) -> Result<(), RuntimeError> {
        let mut queues = self
            .queues
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if queues.stopped {
            return Err(RuntimeError::InvalidState(RuntimeState::Stopped));
        }
        queues.immediate.push_back(request);
        self.immediate.notify_one();
        Ok(())
    }
    fn schedule(&self, job: Job, delay: i64) -> Result<(), RuntimeError> {
        let mut queues = self
            .queues
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?;
        if queues.stopped {
            return Err(RuntimeError::InvalidState(RuntimeState::Stopped));
        }
        let key = (
            Instant::now() + Duration::from_millis(delay.max(0) as u64),
            queues.sequence,
        );
        queues.sequence = queues.sequence.wrapping_add(1);
        queues.scheduled.insert(key, job);
        self.scheduled.notify_one();
        Ok(())
    }
    pub fn request_later(
        &self,
        request: Arc<dyn MessageRequest>,
        delay: i64,
    ) -> Result<(), RuntimeError> {
        self.schedule(Job::Request(request), delay)
    }
    pub fn task_later(&self, task: Task, delay: i64) -> Result<(), RuntimeError> {
        self.schedule(Job::Task(task), delay)
    }
    pub fn close(&self) {
        // User destructors/futures must drop outside the queue mutex.
        let old = self.queues.lock().ok().map(|mut queues| {
            std::mem::replace(
                &mut *queues,
                Queues {
                    stopped: true,
                    ..Queues::default()
                },
            )
        });
        drop(old);
    }
    pub fn stop_accepting(&self) {
        let pending = self.queues.lock().ok().map(|mut queues| {
            queues.stopped = true;
            std::mem::take(&mut queues.immediate)
        });
        drop(pending);
        self.scheduled.notify_one();
    }
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        let queues = self.queues.lock().unwrap();
        queues.immediate.is_empty() && queues.scheduled.is_empty()
    }
}
pub(super) struct OwnedPullWorker {
    active: watch::Sender<bool>,
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}
impl OwnedPullWorker {
    pub fn new(runtime: Weak<ClientRuntime>, service: Arc<PullService>, scheduled: bool) -> Self {
        #[cfg(test)]
        let live = runtime.upgrade().map(|runtime| {
            let probe = runtime.pull_probe.clone();
            probe
                .created
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            probe.live.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            super::tasks::LiveTask(probe)
        });
        let (active, mut activation) = watch::channel(false);
        let (cancel, mut cancellation) = watch::channel(false);
        let task = tokio::spawn(async move {
            #[cfg(test)]
            let _live = live;
            tokio::select! { biased; _ = cancellation.changed() => return, _ = activation.changed() => {}, }
            loop {
                if scheduled
                    && service
                        .queues
                        .lock()
                        .ok()
                        .is_some_and(|queues| queues.stopped && queues.scheduled.is_empty())
                {
                    return;
                }
                let work = if scheduled {
                    let due = service.queues.lock().ok().and_then(|queues| {
                        queues.scheduled.first_key_value().map(|(key, _)| key.0)
                    });
                    tokio::select! { biased;
                        _ = cancellation.changed() => return,
                        _ = service.scheduled.notified() => continue,
                        _ = async { if let Some(due) = due { tokio::time::sleep_until(due).await; } else { std::future::pending::<()>().await; } } => {},
                    }
                    service.queues.lock().ok().and_then(|mut queues| {
                        if queues
                            .scheduled
                            .first_key_value()
                            .is_some_and(|(key, _)| key.0 <= Instant::now())
                        {
                            queues.scheduled.pop_first().map(|(_, job)| job)
                        } else {
                            None
                        }
                    })
                } else {
                    let request = service
                        .queues
                        .lock()
                        .ok()
                        .and_then(|mut queues| queues.immediate.pop_front());
                    if request.is_none() {
                        tokio::select! { biased; _ = cancellation.changed() => return, _ = service.immediate.notified() => continue, }
                    }
                    request.map(Job::Request)
                };
                let Some(work) = work else {
                    continue;
                };
                let operation = async {
                    match work {
                        Job::Request(request) if scheduled => {
                            let _ = service.enqueue(request);
                        }
                        Job::Request(request) => {
                            let Some(runtime) = runtime.upgrade() else {
                                return;
                            };
                            if let Ok(Some(consumer)) =
                                runtime.select_consumer(request.consumer_group())
                            {
                                let operation = match request.mode() {
                                    MessageRequestMode::Pull => consumer.pull_message(request),
                                    MessageRequestMode::Pop => consumer.pop_message(request),
                                };
                                if let Some(operation) = operation {
                                    operation.await;
                                }
                            }
                        }
                        Job::Task(task) => task.await,
                    }
                };
                let mut operation = Box::pin(operation);
                let caught = std::future::poll_fn(|context| {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        operation.as_mut().poll(context)
                    }))
                    .unwrap_or(Poll::Ready(()))
                });
                tokio::select! { biased; _ = cancellation.changed() => return, _ = caught => {}, }
                tokio::task::yield_now().await;
            }
        });
        Self {
            active,
            cancel,
            task: Some(task),
        }
    }
    pub fn activate(&self) {
        self.active.send_replace(true);
    }
    pub async fn stop(&mut self) {
        self.cancel.send_replace(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
    pub async fn finish_gracefully(&mut self) {
        // PullMessageService's executor gets a 1000ms graceful window. Rust
        // cancellation then drops/joins pending work, rather than detaching it.
        if let Some(task) = self.task.as_mut() {
            if tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .is_ok()
            {
                self.task.take();
                return;
            }
        }
        self.stop().await;
    }
}
impl Drop for OwnedPullWorker {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Request(Arc<AtomicUsize>);
    impl MessageRequest for Request {
        fn consumer_group(&self) -> &str {
            "unactivated"
        }
        fn mode(&self) -> MessageRequestMode {
            MessageRequestMode::Pull
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }
    impl Drop for Request {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Reenter(Weak<PullService>, Arc<AtomicUsize>);
    impl Drop for Reenter {
        fn drop(&mut self) {
            let service = self.0.upgrade().unwrap();
            assert!(
                service.queues.try_lock().is_ok(),
                "user destructor ran under queue lock"
            );
            self.1.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn close_drops_queue_and_scheduled_owners_outside_mutex_and_rejects_late_work() {
        let service = Arc::new(PullService::default());
        let dropped = Arc::new(AtomicUsize::new(0));
        service.enqueue(Arc::new(Request(dropped.clone()))).unwrap();
        let guard = Reenter(Arc::downgrade(&service), dropped.clone());
        service
            .task_later(
                Box::pin(async move {
                    let _guard = guard;
                }),
                1_000,
            )
            .unwrap();
        service.close();
        assert!(service.is_empty());
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
        assert!(service.enqueue(Arc::new(Request(dropped.clone()))).is_err());
        assert!(service.task_later(Box::pin(async {}), 0).is_err());
    }

    #[tokio::test]
    async fn missing_capability_drops_request_without_claiming_consumer_execution() {
        struct Consumer;
        impl super::super::ConsumerRuntime for Consumer {
            fn heartbeat_data(&self) -> rocketmq_classic_protocol::heartbeat::ConsumerData {
                Default::default()
            }
        }
        let mut config = crate::config::ClientConfig::new("unactivated-pull");
        config.namesrv_addr = Some(String::new());
        let runtime = super::super::ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        let owner = Arc::new(Consumer);
        let lease = runtime
            .register_consumer("unactivated", owner.clone())
            .await
            .unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        runtime
            .execute_pull_request_immediately(Arc::new(Request(dropped.clone())))
            .unwrap();
        runtime.start().await.unwrap();
        for _ in 0..30 {
            tokio::task::yield_now().await;
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        runtime
            .unregister_consumer(&lease, Instant::now())
            .await
            .unwrap();
        runtime.shutdown().await.unwrap();
        assert_eq!(runtime.pull_probe.live.load(Ordering::SeqCst), 0);
    }
}
