use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
    time::{Duration, Instant},
};

use rocketmq_classic_protocol::body::ConsumerConsumeStatus;

const MINUTE_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
const HOUR_SAMPLE_INTERVAL: Duration = Duration::from_secs(10 * 60);
const MINUTE_SAMPLE_COUNT: usize = 7;
const HOUR_SAMPLE_COUNT: usize = 7;
const STATS_IDLE_TTL: Duration = Duration::from_secs(2 * 60 * 60);

type StatsKey = (String, String, String);

/// Per-PushConsumer metrics for its Running Info snapshot.
#[derive(Default)]
pub(crate) struct ConsumerStatsManager {
    topics: Mutex<HashMap<StatsKey, TopicStats>>,
}

impl ConsumerStatsManager {
    pub(crate) fn record_pull(
        &self,
        client_id: &str,
        group: &str,
        topic: &str,
        elapsed: Duration,
        message_count: usize,
    ) {
        self.update_topic(client_id, group, topic, |stats, now| {
            stats.pull_rt.record(now, millis(elapsed), 1);
            if message_count > 0 {
                stats.pull_tps.record(now, message_count as u64, 1);
            }
        });
    }

    pub(crate) fn record_consume_rt(
        &self,
        client_id: &str,
        group: &str,
        topic: &str,
        elapsed: Duration,
    ) {
        self.update_topic(client_id, group, topic, |stats, now| {
            stats.consume_rt.record(now, millis(elapsed), 1);
        });
    }

    pub(crate) fn record_concurrent_result(
        &self,
        client_id: &str,
        group: &str,
        topic: &str,
        acknowledged: Option<usize>,
        failed: usize,
    ) {
        self.update_topic(client_id, group, topic, |stats, now| {
            if let Some(acknowledged) = acknowledged {
                stats.consume_ok_tps.record(now, acknowledged as u64, 1);
            }
            stats.consume_failed_tps.record(now, failed as u64, 1);
        });
    }

    pub(crate) fn record_orderly_result(
        &self,
        client_id: &str,
        group: &str,
        topic: &str,
        acknowledged: Option<usize>,
        failed: Option<usize>,
    ) {
        self.update_topic(client_id, group, topic, |stats, now| {
            if let Some(acknowledged) = acknowledged {
                stats.consume_ok_tps.record(now, acknowledged as u64, 1);
            }
            if let Some(failed) = failed {
                stats.consume_failed_tps.record(now, failed as u64, 1);
            }
        });
    }

    pub(crate) fn status(
        &self,
        client_id: &str,
        group: &str,
        topic: &str,
    ) -> ConsumerConsumeStatus {
        self.status_at(client_id, group, topic, Instant::now())
    }

    #[cfg(test)]
    pub(crate) fn contains_topic(&self, client_id: &str, group: &str, topic: &str) -> bool {
        self.lock_topics()
            .contains_key(&(client_id.to_owned(), group.to_owned(), topic.to_owned()))
    }

    fn status_at(
        &self,
        client_id: &str,
        group: &str,
        topic: &str,
        now: Instant,
    ) -> ConsumerConsumeStatus {
        let mut topics = self.lock_topics();
        topics.retain(|_, stats| !stats.is_expired(now));
        topics
            .get_mut(&(client_id.to_owned(), group.to_owned(), topic.to_owned()))
            .map(|stats| stats.status(now))
            .unwrap_or_default()
    }

    fn update_topic(
        &self,
        client_id: &str,
        group: &str,
        topic: &str,
        update: impl FnOnce(&mut TopicStats, Instant),
    ) {
        let now = Instant::now();
        let mut topics = self.lock_topics();
        topics.retain(|_, stats| !stats.is_expired(now));
        let stats = topics
            .entry((client_id.to_owned(), group.to_owned(), topic.to_owned()))
            .or_default();
        update(stats, now);
        stats.last_update = Some(now);
    }

    fn lock_topics(&self) -> std::sync::MutexGuard<'_, HashMap<StatsKey, TopicStats>> {
        self.topics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Default)]
struct TopicStats {
    pull_rt: MetricSeries,
    pull_tps: MetricSeries,
    consume_rt: MetricSeries,
    consume_ok_tps: MetricSeries,
    consume_failed_tps: MetricSeries,
    last_update: Option<Instant>,
}

impl TopicStats {
    fn status(&mut self, now: Instant) -> ConsumerConsumeStatus {
        self.advance(now);
        let minute_consume_rt = self.consume_rt.minute.average();
        let consume_rt = if minute_consume_rt.1 == 0 {
            self.consume_rt.hour.average().0
        } else {
            minute_consume_rt.0
        };
        ConsumerConsumeStatus {
            pull_rt: self.pull_rt.minute.average().0,
            pull_tps: self.pull_tps.minute.rate(),
            consume_rt,
            consume_ok_tps: self.consume_ok_tps.minute.rate(),
            consume_failed_tps: self.consume_failed_tps.minute.rate(),
            consume_failed_msgs: i64::try_from(self.consume_failed_tps.hour.sum())
                .unwrap_or(i64::MAX),
        }
    }

    fn advance(&mut self, now: Instant) {
        self.pull_rt.advance(now);
        self.pull_tps.advance(now);
        self.consume_rt.advance(now);
        self.consume_ok_tps.advance(now);
        self.consume_failed_tps.advance(now);
    }

    fn is_expired(&self, now: Instant) -> bool {
        self.last_update
            .is_none_or(|last_update| now.saturating_duration_since(last_update) > STATS_IDLE_TTL)
    }
}

struct MetricSeries {
    value: u64,
    calls: u64,
    minute: SampleWindow,
    hour: SampleWindow,
}

impl Default for MetricSeries {
    fn default() -> Self {
        Self {
            value: 0,
            calls: 0,
            minute: SampleWindow::minute(),
            hour: SampleWindow::hour(),
        }
    }
}

impl MetricSeries {
    fn record(&mut self, now: Instant, value: u64, calls: u64) {
        self.advance(now);
        self.value = self.value.saturating_add(value);
        self.calls = self.calls.saturating_add(calls);
        self.minute
            .update_if_sampled_at(now, self.value, self.calls);
        self.hour.update_if_sampled_at(now, self.value, self.calls);
    }

    fn advance(&mut self, now: Instant) {
        self.minute.sample_until(now, self.value, self.calls);
        self.hour.sample_until(now, self.value, self.calls);
    }
}

struct SampleWindow {
    interval: Duration,
    capacity: usize,
    samples: VecDeque<Snapshot>,
    last_sample: Option<Instant>,
}

impl SampleWindow {
    fn minute() -> Self {
        Self::new(MINUTE_SAMPLE_INTERVAL, MINUTE_SAMPLE_COUNT)
    }

    fn hour() -> Self {
        Self::new(HOUR_SAMPLE_INTERVAL, HOUR_SAMPLE_COUNT)
    }

    fn new(interval: Duration, capacity: usize) -> Self {
        Self {
            interval,
            capacity,
            samples: VecDeque::with_capacity(capacity),
            last_sample: None,
        }
    }

    fn initialize(&mut self, now: Instant) {
        self.samples.push_back(Snapshot {
            at: now.checked_sub(self.interval).unwrap_or(now),
            value: 0,
            calls: 0,
        });
        self.samples.push_back(Snapshot {
            at: now,
            value: 0,
            calls: 0,
        });
        self.last_sample = Some(now);
    }

    fn sample_until(&mut self, now: Instant, value: u64, calls: u64) {
        if self.capacity == 0 || self.interval.is_zero() {
            return;
        }
        let Some(last_sample) = self.last_sample else {
            self.initialize(now);
            return;
        };
        let elapsed = now.saturating_duration_since(last_sample);
        let due = elapsed.as_nanos() / self.interval.as_nanos();
        if due == 0 {
            return;
        }

        let interval_nanos = self.interval.as_nanos();
        let skip = due.saturating_sub(self.capacity.saturating_sub(1) as u128);
        let mut sample_at = last_sample;
        if skip > 0 {
            sample_at = sample_at
                .checked_add(duration_from_nanos(interval_nanos.saturating_mul(skip)))
                .unwrap_or(now);
            self.samples.clear();
            self.push(Snapshot {
                at: sample_at,
                value,
                calls,
            });
        }

        let remaining = due.saturating_sub(skip);
        let remaining = usize::try_from(remaining).unwrap_or(self.capacity);
        for _ in 0..remaining {
            sample_at = sample_at.checked_add(self.interval).unwrap_or(now);
            self.push(Snapshot {
                at: sample_at,
                value,
                calls,
            });
        }
        self.last_sample = Some(sample_at);
    }

    fn push(&mut self, sample: Snapshot) {
        self.samples.push_back(sample);
        while self.samples.len() > self.capacity {
            self.samples.pop_front();
        }
    }

    fn update_if_sampled_at(&mut self, at: Instant, value: u64, calls: u64) {
        if let Some(sample) = self.samples.back_mut().filter(|sample| sample.at == at) {
            sample.value = value;
            sample.calls = calls;
        }
    }

    fn rate(&self) -> f64 {
        let Some((value, _)) = self.delta() else {
            return 0.0;
        };
        let first = self.samples.front().expect("delta requires two samples");
        let last = self.samples.back().expect("delta requires two samples");
        let elapsed = last.at.saturating_duration_since(first.at).as_secs_f64();
        if elapsed == 0.0 {
            0.0
        } else {
            value as f64 / elapsed
        }
    }

    fn average(&self) -> (f64, u64) {
        let Some((value, calls)) = self.delta() else {
            return (0.0, 0);
        };
        if calls == 0 {
            (0.0, 0)
        } else {
            (value as f64 / calls as f64, value)
        }
    }

    fn sum(&self) -> u64 {
        self.delta().map_or(0, |(value, _)| value)
    }

    fn delta(&self) -> Option<(u64, u64)> {
        let (Some(first), Some(last)) = (self.samples.front(), self.samples.back()) else {
            return None;
        };
        Some((
            last.value.saturating_sub(first.value),
            last.calls.saturating_sub(first.calls),
        ))
    }
}

#[derive(Clone, Copy)]
struct Snapshot {
    at: Instant,
    value: u64,
    calls: u64,
}

fn duration_from_nanos(nanos: u128) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{ConsumerConsumeStatus, ConsumerStatsManager, MetricSeries, SampleWindow};
    use std::time::{Duration, Instant};

    #[test]
    fn sampled_windows_match_java_rate_and_average_calculations() {
        let origin = Instant::now();
        let mut series = MetricSeries {
            minute: SampleWindow::minute(),
            hour: SampleWindow::hour(),
            ..MetricSeries::default()
        };
        series.record(origin, 200, 1);
        series.advance(origin + Duration::from_secs(20));
        series.record(origin + Duration::from_secs(20), 400, 1);
        series.advance(origin + Duration::from_secs(30));

        assert_eq!(series.minute.average(), (300.0, 600));
        assert_eq!(series.minute.rate(), 15.0);
        assert_eq!(series.hour.sum(), 200);

        series.advance(origin + Duration::from_secs(61));
        assert_eq!(series.minute.average(), (400.0, 400));
        assert!((series.minute.rate() - (400.0 / 60.0)).abs() < f64::EPSILON);
        assert_eq!(series.hour.sum(), 200);

        series.advance(origin + Duration::from_secs(81));
        assert_eq!(series.minute.average(), (0.0, 0));
        assert_eq!(series.minute.rate(), 0.0);
        assert_eq!(series.hour.sum(), 200);
    }

    #[test]
    fn consume_status_is_partitioned_and_failed_results_do_not_count_as_success_calls() {
        let stats = ConsumerStatsManager::default();
        stats.record_pull(
            "client-a",
            "workers",
            "orders",
            Duration::from_millis(500),
            2,
        );
        stats.record_consume_rt("client-a", "workers", "orders", Duration::from_millis(8));
        stats.record_concurrent_result("client-a", "workers", "orders", Some(3), 1);
        stats.record_concurrent_result("client-a", "workers", "orders", None, 2);

        let minute_target = Instant::now() + Duration::from_secs(10);
        let mut topics = stats.lock_topics();
        let topic = topics
            .get_mut(&("client-a".into(), "workers".into(), "orders".into()))
            .unwrap();
        topic.advance(minute_target);
        let status = topic.status(minute_target);

        assert_eq!(status.pull_rt, 500.0);
        assert_eq!(status.consume_rt, 8.0);
        assert!(status.pull_tps > 0.0);
        assert!(status.consume_ok_tps > 0.0);
        assert!(status.consume_failed_tps > 0.0);
        assert_eq!(status.consume_failed_msgs, 1);
        let hour_target = minute_target + Duration::from_secs(10 * 60);
        let hour_status = topic.status(hour_target);
        assert_eq!(hour_status.consume_failed_msgs, 3);
        drop(topics);
        assert_eq!(
            serde_json::to_value(stats.status_at("client-b", "workers", "orders", hour_target))
                .unwrap(),
            serde_json::to_value(ConsumerConsumeStatus::default()).unwrap()
        );
        assert_eq!(
            serde_json::to_value(stats.status_at("client-a", "other-group", "orders", hour_target))
                .unwrap(),
            serde_json::to_value(ConsumerConsumeStatus::default()).unwrap()
        );
    }
}
