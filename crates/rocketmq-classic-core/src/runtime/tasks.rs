use super::{ClientRuntime, RuntimeError, RuntimeState};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicU8, Ordering},
    sync::{Arc, Mutex, Weak},
    task::Poll,
};
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
    time::{Duration, Instant},
};

#[cfg(test)]
#[derive(Default)]
pub(super) struct TaskProbe {
    pub created: std::sync::atomic::AtomicUsize,
    pub live: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
pub(super) struct LiveTask(pub(super) Arc<TaskProbe>);

#[cfg(test)]
impl Drop for LiveTask {
    fn drop(&mut self) {
        self.0
            .live
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum TaskKind {
    Discovery,
    Route,
    Heartbeat,
    Persist,
    Adjust,
}

#[derive(Clone, Copy)]
pub(super) struct Schedule {
    kind: TaskKind,
    delay: Duration,
    period: Duration,
}

type UserFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type UserTaskFactory = Arc<dyn Fn() -> UserFuture + Send + Sync + 'static>;

const TASK_PENDING: u8 = 0;
const TASK_RUNNING: u8 = 1;
const TASK_FINISHED: u8 = 2;
const TASK_CANCELLED: u8 = 3;

/// A handle to one task accepted by a runtime's client scheduler.
///
/// Dropping the handle leaves the scheduled task running. Call [`cancel`](Self::cancel)
/// to cancel it explicitly.
#[derive(Clone)]
pub struct RuntimeScheduledTaskHandle {
    state: Arc<AtomicU8>,
    cancel: Arc<Notify>,
    changed: Arc<Notify>,
}

impl RuntimeScheduledTaskHandle {
    /// Cancels a task that has not already finished or been cancelled.
    pub fn cancel(&self) -> bool {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if matches!(current, TASK_FINISHED | TASK_CANCELLED) {
                return false;
            }
            match self.state.compare_exchange_weak(
                current,
                TASK_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.cancel.notify_one();
                    self.changed.notify_one();
                    return true;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Returns whether the task completed or was cancelled.
    pub fn is_finished(&self) -> bool {
        matches!(
            self.state.load(Ordering::Acquire),
            TASK_FINISHED | TASK_CANCELLED
        )
    }
}

#[derive(Clone)]
pub(super) struct UserSchedule {
    delay: Duration,
    period: Option<Duration>,
    fixed_rate: bool,
    factory: UserTaskFactory,
    state: Arc<AtomicU8>,
    cancel: Arc<Notify>,
    changed: Arc<Notify>,
}

impl UserSchedule {
    fn new(
        delay: Duration,
        period: Option<Duration>,
        fixed_rate: bool,
        factory: UserTaskFactory,
        changed: Arc<Notify>,
    ) -> (Self, RuntimeScheduledTaskHandle) {
        let state = Arc::new(AtomicU8::new(TASK_PENDING));
        let cancel = Arc::new(Notify::new());
        (
            Self {
                delay,
                period,
                fixed_rate,
                factory,
                state: state.clone(),
                cancel: cancel.clone(),
                changed: changed.clone(),
            },
            RuntimeScheduledTaskHandle {
                state,
                cancel,
                changed,
            },
        )
    }

    pub(super) fn reject(&self) {
        let mut current = self.state.load(Ordering::Acquire);
        while matches!(current, TASK_PENDING | TASK_RUNNING) {
            match self.state.compare_exchange_weak(
                current,
                TASK_FINISHED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        self.changed.notify_one();
    }
}

/// Rust-native access to the single-threaded scheduler owned by `MQClientInstance`.
///
/// The handle schedules async work on the runtime's client scheduler, whose
/// lifetime remains owned by `ClientRuntime`.
pub struct RuntimeScheduler {
    runtime: Weak<ClientRuntime>,
}

impl RuntimeScheduler {
    pub(super) fn new(runtime: Weak<ClientRuntime>) -> Self {
        Self { runtime }
    }

    /// Queues a task for immediate execution on the client scheduler.
    pub fn execute(
        &self,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> Result<RuntimeScheduledTaskHandle, RuntimeError> {
        self.schedule_after(Duration::ZERO, task)
    }

    /// Queues a task for execution after `delay`.
    pub fn schedule_after(
        &self,
        delay: Duration,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> Result<RuntimeScheduledTaskHandle, RuntimeError> {
        let pending = Arc::new(Mutex::new(Some(Box::pin(task) as UserFuture)));
        self.enqueue(
            delay,
            None,
            true,
            Arc::new(move || {
                pending
                    .lock()
                    .expect("one-shot scheduled task future lock poisoned")
                    .take()
                    .expect("one-shot scheduled task future already consumed")
            }),
        )
    }

    /// Queues a callback at a fixed rate. Calls run serially on the client
    /// scheduler and retain the prior-deadline catch-up behavior of Java's
    /// `scheduleAtFixedRate`.
    pub fn schedule_at_fixed_rate<F, Fut>(
        &self,
        initial_delay: Duration,
        period: Duration,
        callback: F,
    ) -> Result<RuntimeScheduledTaskHandle, RuntimeError>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if period.is_zero() {
            return Err(RuntimeError::InvalidSchedule);
        }
        let callback = Arc::new(callback);
        self.enqueue(
            initial_delay,
            Some(period),
            true,
            Arc::new(move || Box::pin(callback())),
        )
    }

    /// Queues a callback with a delay measured from the previous completion.
    /// Calls run serially on the client scheduler.
    pub fn schedule_with_fixed_delay<F, Fut>(
        &self,
        initial_delay: Duration,
        delay: Duration,
        callback: F,
    ) -> Result<RuntimeScheduledTaskHandle, RuntimeError>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if delay.is_zero() {
            return Err(RuntimeError::InvalidSchedule);
        }
        let callback = Arc::new(callback);
        self.enqueue(
            initial_delay,
            Some(delay),
            false,
            Arc::new(move || Box::pin(callback())),
        )
    }

    fn enqueue(
        &self,
        delay: Duration,
        period: Option<Duration>,
        fixed_rate: bool,
        factory: UserTaskFactory,
    ) -> Result<RuntimeScheduledTaskHandle, RuntimeError> {
        let runtime = self
            .runtime
            .upgrade()
            .ok_or(RuntimeError::InvalidState(RuntimeState::Stopped))?;
        let (schedule, handle) = UserSchedule::new(
            delay,
            period,
            fixed_rate,
            factory,
            runtime.task_scheduler_changed.clone(),
        );
        runtime.enqueue_scheduled_task(schedule)?;
        Ok(handle)
    }
}

#[derive(Clone)]
enum ScheduledJob {
    Periodic(Schedule),
    User(UserSchedule),
}

#[derive(Default)]
struct SchedulerQueue {
    jobs: BTreeMap<(Instant, u64), ScheduledJob>,
    next_order: u64,
}

impl SchedulerQueue {
    fn insert(&mut self, due: Instant, job: ScheduledJob) -> u64 {
        let order = self.next_order;
        self.next_order = self.next_order.wrapping_add(1);
        self.jobs.insert((due, order), job);
        order
    }

    fn remove_cancelled_users(&mut self) {
        self.jobs.retain(|_, job| {
            !matches!(job, ScheduledJob::User(schedule) if schedule.state.load(Ordering::Acquire) == TASK_CANCELLED)
        });
    }
}

pub(super) fn schedules(
    config: &crate::config::ClientConfig,
) -> Result<Vec<Schedule>, RuntimeError> {
    let mut result = Vec::new();
    if config.namesrv_addr.is_none() {
        result.push(Schedule {
            kind: TaskKind::Discovery,
            delay: Duration::from_secs(10),
            period: Duration::from_secs(120),
        });
    }
    for (kind, delay, period) in [
        (TaskKind::Route, 10, config.poll_name_server_interval),
        (TaskKind::Heartbeat, 1_000, config.heartbeat_broker_interval),
        (
            TaskKind::Persist,
            10_000,
            config.persist_consumer_offset_interval,
        ),
        (TaskKind::Adjust, 60_000, 60_000),
    ] {
        if period <= 0 {
            return Err(RuntimeError::InvalidSchedule);
        }
        result.push(Schedule {
            kind,
            delay: Duration::from_millis(delay),
            period: Duration::from_millis(period as u64),
        });
    }
    Ok(result)
}

/// Java's single scheduled executor: fixed-rate deadlines, no overlapping jobs,
/// catch-up after slow work, and one cancellation/join owner.
pub(super) struct OwnedTasks {
    schedules: Arc<Mutex<SchedulerQueue>>,
    changed: Arc<Notify>,
    active: watch::Sender<bool>,
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl OwnedTasks {
    pub fn new(runtime: Weak<ClientRuntime>) -> Self {
        #[cfg(test)]
        let live = runtime.upgrade().map(|runtime| {
            let probe = runtime.periodic_probe.clone();
            probe
                .created
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            probe.live.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            LiveTask(probe)
        });
        let (active, mut activation) = watch::channel(false);
        let (cancel, mut cancellation) = watch::channel(false);
        let schedules = Arc::new(Mutex::new(SchedulerQueue::default()));
        let changed = runtime
            .upgrade()
            .map(|runtime| runtime.task_scheduler_changed.clone())
            .unwrap_or_else(|| Arc::new(Notify::new()));
        let worker_schedules = schedules.clone();
        let worker_changed = changed.clone();
        let task = tokio::spawn(async move {
            #[cfg(test)]
            let _live = live;
            let Some(rebalance) = runtime.upgrade().map(|runtime| runtime.rebalance.clone()) else {
                return;
            };
            tokio::select! { biased;
                _ = cancellation.changed() => return,
                _ = activation.changed() => {},
            }
            loop {
                let next = match worker_schedules.lock() {
                    Ok(mut queue) => {
                        queue.remove_cancelled_users();
                        queue.jobs.first_key_value().map(|(key, _)| key.0)
                    }
                    Err(_) => return,
                };
                let Some(due) = next else {
                    tokio::select! { biased;
                        _ = cancellation.changed() => return,
                        _ = worker_changed.notified() => continue,
                        _ = rebalance.scheduled.notified() => {
                            rebalance.fire_due();
                            continue;
                        },
                    }
                };
                let wake_due = rebalance.next_due().filter(|wake| *wake < due);
                let next_wake = wake_due.unwrap_or(due);
                tokio::select! { biased;
                    _ = cancellation.changed() => return,
                    _ = worker_changed.notified() => continue,
                    _ = rebalance.scheduled.notified() => {
                        rebalance.fire_due();
                        continue;
                    },
                    _ = tokio::time::sleep_until(next_wake) => {},
                }
                if wake_due.is_some() {
                    rebalance.fire_due();
                    continue;
                }
                let (key, job) = match worker_schedules.lock() {
                    Ok(mut queue) => {
                        queue.remove_cancelled_users();
                        if queue
                            .jobs
                            .first_key_value()
                            .is_some_and(|(key, _)| key.0 <= Instant::now())
                        {
                            queue
                                .jobs
                                .pop_first()
                                .expect("first scheduled job was checked")
                        } else {
                            continue;
                        }
                    }
                    Err(_) => return,
                };
                let Some(runtime) = runtime.upgrade() else {
                    return;
                };
                match job {
                    ScheduledJob::Periodic(schedule) => {
                        let mut operation = Box::pin(runtime.run_periodic(schedule.kind));
                        let caught = std::future::poll_fn(|context| {
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                operation.as_mut().poll(context)
                            }))
                            .unwrap_or(Poll::Ready(Err(RuntimeError::TaskCallbackFailed)))
                        });
                        tokio::select! { biased;
                            _ = cancellation.changed() => return,
                            _ = caught => {},
                        }
                        if let Ok(mut queue) = worker_schedules.lock() {
                            queue.jobs.insert(
                                ((key.0 + schedule.period), key.1),
                                ScheduledJob::Periodic(schedule),
                            );
                        } else {
                            return;
                        }
                    }
                    ScheduledJob::User(schedule) => {
                        if schedule
                            .state
                            .compare_exchange(
                                TASK_PENDING,
                                TASK_RUNNING,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_err()
                        {
                            continue;
                        }
                        let mut operation =
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                (schedule.factory)()
                            })) {
                                Ok(operation) => operation,
                                Err(_) => {
                                    schedule.state.store(TASK_FINISHED, Ordering::Release);
                                    schedule.changed.notify_one();
                                    continue;
                                }
                            };
                        let caught = std::future::poll_fn(|context| {
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                operation.as_mut().poll(context)
                            }))
                            .unwrap_or(Poll::Ready(()))
                        });
                        tokio::select! { biased;
                            _ = cancellation.changed() => {
                                schedule.reject();
                                return;
                            },
                            _ = schedule.cancel.notified() => {},
                            _ = caught => {},
                        }
                        let next_state = if schedule.period.is_some() {
                            TASK_PENDING
                        } else {
                            TASK_FINISHED
                        };
                        if schedule
                            .state
                            .compare_exchange(
                                TASK_RUNNING,
                                next_state,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            if let Some(period) = schedule.period {
                                if let Ok(mut queue) = worker_schedules.lock() {
                                    let next_due = if schedule.fixed_rate {
                                        key.0 + period
                                    } else {
                                        Instant::now() + period
                                    };
                                    queue
                                        .jobs
                                        .insert((next_due, key.1), ScheduledJob::User(schedule));
                                    worker_changed.notify_one();
                                } else {
                                    return;
                                }
                            } else {
                                schedule.changed.notify_one();
                            }
                        } else {
                            schedule.changed.notify_one();
                        }
                    }
                }
            }
        });
        Self {
            schedules,
            changed,
            active,
            cancel,
            task: Some(task),
        }
    }

    pub fn register(&mut self, schedule: Schedule) -> Result<(), RuntimeError> {
        self.schedules
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?
            .insert(
                Instant::now() + schedule.delay,
                ScheduledJob::Periodic(schedule),
            );
        self.changed.notify_one();
        Ok(())
    }

    pub fn register_user(&mut self, schedule: UserSchedule) -> Result<(), RuntimeError> {
        let due = Instant::now() + schedule.delay;
        self.schedules
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?
            .insert(due, ScheduledJob::User(schedule));
        self.changed.notify_one();
        Ok(())
    }

    pub fn activate(&self) {
        self.active.send_replace(true);
    }

    pub async fn stop(&mut self) {
        self.cancel.send_replace(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        if let Ok(mut queue) = self.schedules.lock() {
            for (_, job) in std::mem::take(&mut queue.jobs) {
                if let ScheduledJob::User(schedule) = job {
                    schedule.reject();
                }
            }
        }
    }
}

impl Drop for OwnedTasks {
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
    use crate::{
        config::ClientConfig,
        runtime::{ConsumerRuntime, ProducerRuntime},
    };
    use rocketmq_classic_protocol::heartbeat::ConsumerData;
    use std::{
        collections::{BTreeSet, HashMap},
        pin::Pin,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct Consumer {
        persistence: AtomicUsize,
        adjustment: AtomicUsize,
        heartbeat: AtomicUsize,
        active: AtomicUsize,
        block: AtomicBool,
        panic: AtomicBool,
        release: tokio::sync::Notify,
    }

    struct Active<'a>(&'a AtomicUsize);
    impl Drop for Active<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl ConsumerRuntime for Consumer {
        fn heartbeat_data(&self) -> ConsumerData {
            self.heartbeat.fetch_add(1, Ordering::SeqCst);
            ConsumerData {
                group_name: Some("consumer".into()),
                ..Default::default()
            }
        }
        fn persist_consumer_offset(&self) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
            Some(Box::pin(async move {
                self.persistence.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    self.active.fetch_add(1, Ordering::SeqCst),
                    0,
                    "overlapping callback"
                );
                let _active = Active(&self.active);
                assert!(
                    !self.panic.swap(false, Ordering::SeqCst),
                    "scripted callback panic"
                );
                if self.block.load(Ordering::SeqCst) {
                    self.release.notified().await;
                }
            }))
        }
        fn adjust_thread_pool(&self) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
            Some(Box::pin(async move {
                self.adjustment.fetch_add(1, Ordering::SeqCst);
            }))
        }
    }

    #[derive(Default)]
    struct Producer(AtomicUsize);
    impl ProducerRuntime for Producer {
        fn publish_topics(&self) -> BTreeSet<String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            BTreeSet::new()
        }
    }

    fn runtime() -> Arc<ClientRuntime> {
        let mut config = ClientConfig::new("timer");
        config.namesrv_addr = Some(String::new());
        Arc::new(ClientRuntime::construct(config, &HashMap::new(), None).unwrap())
    }

    #[tokio::test]
    async fn public_and_periodic_adjustment_isolate_each_owner_failure() {
        struct FailingConsumer(bool);
        impl ConsumerRuntime for FailingConsumer {
            fn heartbeat_data(&self) -> ConsumerData {
                ConsumerData::default()
            }
            fn adjust_thread_pool(&self) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
                assert!(!self.0, "capability construction failure");
                Some(Box::pin(async {
                    tokio::task::yield_now().await;
                    panic!("capability polling failure");
                }))
            }
        }
        struct NoAdjustment;
        impl ConsumerRuntime for NoAdjustment {
            fn heartbeat_data(&self) -> ConsumerData {
                ConsumerData::default()
            }
        }
        let runtime = runtime();
        let construct_failure = Arc::new(FailingConsumer(true));
        let poll_failure = Arc::new(FailingConsumer(false));
        let unsupported = Arc::new(NoAdjustment);
        let healthy = Arc::new(Consumer::default());
        runtime
            .groups
            .register_consumer("a", construct_failure.clone())
            .unwrap();
        runtime
            .groups
            .register_consumer("b", poll_failure.clone())
            .unwrap();
        runtime
            .groups
            .register_consumer("c", unsupported.clone())
            .unwrap();
        runtime
            .groups
            .register_consumer("z", healthy.clone())
            .unwrap();
        runtime.adjust_thread_pool().await.unwrap();
        assert_eq!(healthy.adjustment.load(Ordering::SeqCst), 1);
        runtime.run_periodic(TaskKind::Adjust).await.unwrap();
        assert_eq!(healthy.adjustment.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.periodic_probe.created.load(Ordering::SeqCst), 0);
    }

    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }
    async fn advance(milliseconds: u64) {
        tokio::time::advance(Duration::from_millis(milliseconds)).await;
        settle().await;
    }

    #[tokio::test(start_paused = true)]
    async fn public_client_scheduler_runs_before_start_and_cancels_fixed_rate_tasks() {
        let runtime = runtime();
        let scheduler = runtime.scheduled_executor_service();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        let one_shot = scheduler
            .schedule_after(Duration::from_secs(1), async move {
                let _ = completed_tx.send(());
            })
            .unwrap();

        settle().await;
        advance(999).await;
        assert!(!one_shot.is_finished());
        advance(1).await;
        completed_rx.await.unwrap();
        settle().await;
        assert!(one_shot.is_finished());

        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = calls.clone();
        let repeating = scheduler
            .schedule_at_fixed_rate(Duration::ZERO, Duration::from_secs(1), move || {
                let callback_calls = callback_calls.clone();
                async move {
                    callback_calls.fetch_add(1, Ordering::SeqCst);
                }
            })
            .unwrap();
        settle().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(repeating.cancel());
        settle().await;
        advance(5_000).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(repeating.is_finished());

        let delayed_calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = delayed_calls.clone();
        let fixed_delay = scheduler
            .schedule_with_fixed_delay(Duration::from_secs(2), Duration::from_secs(2), move || {
                let callback_calls = callback_calls.clone();
                async move {
                    callback_calls.fetch_add(1, Ordering::SeqCst);
                }
            })
            .unwrap();
        settle().await;
        advance(2_000).await;
        assert_eq!(delayed_calls.load(Ordering::SeqCst), 1);
        advance(1_999).await;
        assert_eq!(delayed_calls.load(Ordering::SeqCst), 1);
        advance(1).await;
        assert_eq!(delayed_calls.load(Ordering::SeqCst), 2);
        assert!(fixed_delay.cancel());

        assert!(matches!(
            scheduler.schedule_with_fixed_delay(Duration::ZERO, Duration::ZERO, || async {}),
            Err(RuntimeError::InvalidSchedule)
        ));

        runtime.start().await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    fn tasks(runtime: &Arc<ClientRuntime>) -> OwnedTasks {
        let mut tasks = OwnedTasks::new(Arc::downgrade(runtime));
        for schedule in schedules(&runtime.config_snapshot()).unwrap() {
            tasks.register(schedule).unwrap();
        }
        tasks.activate();
        tasks
    }

    #[tokio::test(start_paused = true)]
    async fn defaults_have_java_initial_delays_periods_and_no_ticks_after_stop() {
        let runtime = runtime();
        let producer = Arc::new(Producer::default());
        let consumer = Arc::new(Consumer::default());
        runtime
            .groups
            .register_producer("producer", producer.clone())
            .unwrap();
        runtime
            .groups
            .register_consumer("consumer", consumer.clone())
            .unwrap();
        let mut tasks = tasks(&runtime);
        settle().await;
        advance(9).await;
        assert_eq!(producer.0.load(Ordering::SeqCst), 0);
        advance(1).await;
        assert_eq!(producer.0.load(Ordering::SeqCst), 1);
        assert_eq!(consumer.heartbeat.load(Ordering::SeqCst), 1); // route topic collection
        advance(989).await;
        assert_eq!(consumer.heartbeat.load(Ordering::SeqCst), 1);
        advance(1).await;
        assert_eq!(consumer.heartbeat.load(Ordering::SeqCst), 2);
        advance(8_999).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 0);
        advance(1).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 1);
        advance(5_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 2);
        advance(45_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 11);
        assert_eq!(consumer.adjustment.load(Ordering::SeqCst), 1);
        assert_eq!(producer.0.load(Ordering::SeqCst), 2);
        assert_eq!(consumer.heartbeat.load(Ordering::SeqCst), 4);
        tasks.stop().await;
        assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
        advance(120_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 11);
        assert_eq!(producer.0.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn offset_persistence_skips_missing_capabilities_and_continues_ticking() {
        struct NoPersistence(AtomicUsize);
        impl ConsumerRuntime for NoPersistence {
            fn heartbeat_data(&self) -> ConsumerData {
                ConsumerData::default()
            }
            fn persist_consumer_offset(
                &self,
            ) -> Option<Pin<Box<dyn Future<Output = ()> + Send + '_>>> {
                self.0.fetch_add(1, Ordering::SeqCst);
                None
            }
        }

        let runtime = runtime();
        let no_capability = Arc::new(NoPersistence(AtomicUsize::new(0)));
        let capable = Arc::new(Consumer::default());
        runtime
            .groups
            .register_consumer("a-no-persistence", no_capability.clone())
            .unwrap();
        runtime
            .groups
            .register_consumer("z-persistence", capable.clone())
            .unwrap();
        let mut tasks = tasks(&runtime);
        settle().await;

        advance(10_000).await;
        assert_eq!(no_capability.0.load(Ordering::SeqCst), 1);
        assert_eq!(capable.persistence.load(Ordering::SeqCst), 1);

        advance(5_000).await;
        assert_eq!(no_capability.0.load(Ordering::SeqCst), 2);
        assert_eq!(capable.persistence.load(Ordering::SeqCst), 2);
        tasks.stop().await;
    }

    #[test]
    fn scheduled_task_delays_and_periods_match_invoked_java_registration() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/lookup.json")).unwrap();
        for case in fixture["runtimeScheduledTaskCases"].as_array().unwrap() {
            let mut config = ClientConfig::new("scheduled-task-oracle");
            let mutated = match case["name"].as_str().unwrap() {
                "nameserver-absent" => {
                    config.namesrv_addr = None;
                    false
                }
                "nameserver-configured" => {
                    config.namesrv_addr = Some("127.0.0.1:9876".into());
                    false
                }
                "nameserver-absent-mutated-configured" => {
                    config.namesrv_addr = Some("late-after-construction:9876".into());
                    true
                }
                "nameserver-configured-mutated-absent" => {
                    config.namesrv_addr = None;
                    true
                }
                other => panic!("unexpected Java scheduler case {other}"),
            };
            config.poll_name_server_interval = if mutated { 71 } else { 23 };
            config.heartbeat_broker_interval = if mutated { 83 } else { 47 };
            config.persist_consumer_offset_interval = if mutated { 97 } else { 59 };

            let actual: Vec<_> = schedules(&config)
                .unwrap()
                .iter()
                .map(|schedule| {
                    serde_json::json!({
                        "initialDelayMillis": schedule.delay.as_millis(),
                        "periodMillis": schedule.period.as_millis(),
                    })
                })
                .collect();
            let expected: Vec<_> = case["scheduleAtFixedRateCalls"]
                .as_array()
                .unwrap()
                .iter()
                .map(|call| {
                    let multiplier = match call["unit"].as_str().unwrap() {
                        "MILLISECONDS" => 1,
                        "MINUTES" => 60_000,
                        unit => panic!("unexpected Java scheduler unit {unit}"),
                    };
                    serde_json::json!({
                        "initialDelayMillis": call["initialDelay"].as_u64().unwrap() * multiplier,
                        "periodMillis": call["period"].as_u64().unwrap() * multiplier,
                    })
                })
                .collect();
            assert_eq!(actual, expected, "{}", case["name"]);
        }
    }

    #[test]
    fn empty_nameserver_start_schedule_count_matches_java_probe() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/start.json")).unwrap();
        let java_case = &fixture["configuredToEmpty"];
        let mut config = ClientConfig::new("empty-nameserver-start");
        config.namesrv_addr = Some(String::new());

        assert_eq!(java_case["discoveryCalls"], 0);
        assert_eq!(
            schedules(&config).unwrap().len(),
            java_case["scheduledTaskCount"].as_u64().unwrap() as usize
        );
    }

    #[tokio::test(start_paused = true)]
    async fn slow_callback_serializes_jobs_then_catches_up_from_prior_deadlines() {
        let runtime = runtime();
        let consumer = Arc::new(Consumer::default());
        consumer.block.store(true, Ordering::SeqCst);
        runtime
            .groups
            .register_consumer("consumer", consumer.clone())
            .unwrap();
        let mut tasks = tasks(&runtime);
        settle().await;
        advance(10_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 1);
        advance(50_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 1);
        assert_eq!(consumer.active.load(Ordering::SeqCst), 1);
        assert_eq!(consumer.adjustment.load(Ordering::SeqCst), 0);
        consumer.block.store(false, Ordering::SeqCst);
        consumer.release.notify_one();
        settle().await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 11);
        assert_eq!(consumer.adjustment.load(Ordering::SeqCst), 1);
        assert_eq!(consumer.active.load(Ordering::SeqCst), 0);
        tasks.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_and_joins_pending_callback() {
        let runtime = runtime();
        let consumer = Arc::new(Consumer::default());
        consumer.block.store(true, Ordering::SeqCst);
        runtime
            .groups
            .register_consumer("consumer", consumer.clone())
            .unwrap();
        let mut tasks = tasks(&runtime);
        settle().await;
        advance(10_000).await;
        assert_eq!(consumer.active.load(Ordering::SeqCst), 1);
        tasks.stop().await;
        assert_eq!(consumer.active.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.periodic_probe.live.load(Ordering::SeqCst), 0);
        advance(120_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_rebalance_wakeup_waits_behind_the_single_scheduled_callback() {
        let runtime = runtime();
        let consumer = Arc::new(Consumer::default());
        consumer.block.store(true, Ordering::SeqCst);
        runtime
            .groups
            .register_consumer("consumer", consumer.clone())
            .unwrap();
        let mut tasks = tasks(&runtime);
        settle().await;
        advance(10_000).await;
        assert_eq!(consumer.active.load(Ordering::SeqCst), 1);
        runtime.rebalance_later(1_000).unwrap();
        advance(1_000).await;
        assert!(runtime.rebalance.next_due().is_some());
        consumer.block.store(false, Ordering::SeqCst);
        consumer.release.notify_one();
        settle().await;
        assert!(runtime.rebalance.next_due().is_none());
        tasks.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn panicking_callback_does_not_kill_later_ticks() {
        let runtime = runtime();
        let consumer = Arc::new(Consumer::default());
        consumer.panic.store(true, Ordering::SeqCst);
        runtime
            .groups
            .register_consumer("consumer", consumer.clone())
            .unwrap();
        let mut tasks = tasks(&runtime);
        settle().await;
        advance(10_000).await;
        advance(50_000).await;
        assert_eq!(consumer.persistence.load(Ordering::SeqCst), 11);
        assert_eq!(consumer.adjustment.load(Ordering::SeqCst), 1);
        tasks.stop().await;
    }

    #[test]
    fn nonpositive_period_is_rejected_before_task_creation() {
        for period in [0, -1] {
            let mut config = ClientConfig::new("timer");
            config.poll_name_server_interval = period;
            assert!(matches!(
                schedules(&config),
                Err(RuntimeError::InvalidSchedule)
            ));
        }
    }
}
