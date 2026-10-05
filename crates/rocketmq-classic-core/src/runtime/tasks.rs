use super::{ClientRuntime, RuntimeError};
use std::{
    future::Future,
    sync::{Arc, Mutex, Weak},
    task::Poll,
};
use tokio::{
    sync::watch,
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
struct LiveTask(Arc<TaskProbe>);

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

pub(super) fn schedules(
    config: &crate::config::ClientConfig,
) -> Result<Vec<Schedule>, RuntimeError> {
    let mut result = Vec::new();
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
    schedules: Arc<Mutex<Vec<(Schedule, Instant)>>>,
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
        let schedules = Arc::new(Mutex::new(Vec::new()));
        let worker_schedules = schedules.clone();
        let task = tokio::spawn(async move {
            #[cfg(test)]
            let _live = live;
            tokio::select! { biased;
                _ = cancellation.changed() => return,
                _ = activation.changed() => {},
            }
            let mut jobs: Vec<(Schedule, Instant)> = match worker_schedules.lock() {
                Ok(value) => value.clone(),
                Err(_) => return,
            };
            while !jobs.is_empty() {
                let index = jobs
                    .iter()
                    .enumerate()
                    .min_by_key(|(index, (_, due))| (*due, *index))
                    .map(|(index, _)| index)
                    .unwrap_or(0);
                let (schedule, due) = jobs[index];
                tokio::select! { biased;
                    _ = cancellation.changed() => return,
                    _ = tokio::time::sleep_until(due) => {},
                }
                let Some(runtime) = runtime.upgrade() else {
                    return;
                };
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
                // Fixed rate is anchored to the prior deadline, never to completion.
                jobs[index].1 += schedule.period;
            }
        });
        Self {
            schedules,
            active,
            cancel,
            task: Some(task),
        }
    }

    pub fn register(&mut self, schedule: Schedule) -> Result<(), RuntimeError> {
        self.schedules
            .lock()
            .map_err(|_| RuntimeError::RegistryPoisoned)?
            .push((schedule, Instant::now() + schedule.delay));
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
                group_name: "consumer".into(),
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
        Arc::new(
            ClientRuntime::construct(ClientConfig::new("timer"), &HashMap::new(), None).unwrap(),
        )
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

    fn tasks(runtime: &Arc<ClientRuntime>) -> OwnedTasks {
        let mut tasks = OwnedTasks::new(Arc::downgrade(runtime));
        for schedule in schedules(runtime.config()).unwrap() {
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
