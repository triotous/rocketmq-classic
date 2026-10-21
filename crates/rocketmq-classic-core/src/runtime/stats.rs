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

type StatsKey = String;

/// Java 5.5.1 consumer metrics shared by one `MQClientInstance` equivalent.
#[derive(Default)]
pub struct ConsumerStatsManager {
    topics: Mutex<HashMap<StatsKey, TopicStats>>,
}

impl ConsumerStatsManager {
    /// Creates an empty manager for one client runtime.
    pub fn new() -> Self {
        Self::default()
    }

    /// Java's lifecycle hooks are no-ops; sampling is evaluated on reads.
    pub fn start(&self) {}

    /// Java's lifecycle hooks are no-ops; the runtime owns this manager.
    pub fn shutdown(&self) {}

    pub fn inc_pull_rt(&self, group: &str, topic: &str, rt: i64) {
        self.update_topic(group, topic, |stats, now| {
            stats.pull_rt.record(now, java_int(rt), 1);
        });
    }

    pub fn inc_pull_tps(&self, group: &str, topic: &str, messages: i64) {
        self.update_topic(group, topic, |stats, now| {
            stats.pull_tps.record(now, java_int(messages), 1);
        });
    }

    pub fn inc_consume_rt(&self, group: &str, topic: &str, rt: i64) {
        self.update_topic(group, topic, |stats, now| {
            stats.consume_rt.record(now, java_int(rt), 1);
        });
    }

    pub fn inc_consume_ok_tps(&self, group: &str, topic: &str, messages: i64) {
        self.update_topic(group, topic, |stats, now| {
            stats.consume_ok_tps.record(now, java_int(messages), 1);
        });
    }

    pub fn inc_consume_failed_tps(&self, group: &str, topic: &str, messages: i64) {
        self.update_topic(group, topic, |stats, now| {
            stats.consume_failed_tps.record(now, java_int(messages), 1);
        });
    }

    pub fn consume_status(&self, group: &str, topic: &str) -> ConsumerConsumeStatus {
        self.status_at(group, topic, Instant::now())
    }

    fn status_at(&self, group: &str, topic: &str, now: Instant) -> ConsumerConsumeStatus {
        let mut topics = self.lock_topics();
        topics
            .get_mut(&java_stats_key(group, topic))
            .map(|stats| stats.status(now))
            .unwrap_or_default()
    }

    fn update_topic(
        &self,
        group: &str,
        topic: &str,
        update: impl FnOnce(&mut TopicStats, Instant),
    ) {
        let now = Instant::now();
        let mut topics = self.lock_topics();
        let stats = topics.entry(java_stats_key(group, topic)).or_default();
        update(stats, now);
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
        let mut status = ConsumerConsumeStatus::default();
        status.set_pull_rt(self.pull_rt.minute.average().0);
        status.set_pull_tps(self.pull_tps.minute.rate());
        status.set_consume_rt(consume_rt);
        status.set_consume_ok_tps(self.consume_ok_tps.minute.rate());
        status.set_consume_failed_tps(self.consume_failed_tps.minute.rate());
        status.set_consume_failed_msgs(self.consume_failed_tps.hour.sum());
        status
    }

    fn advance(&mut self, now: Instant) {
        self.pull_rt.advance(now);
        self.pull_tps.advance(now);
        self.consume_rt.advance(now);
        self.consume_ok_tps.advance(now);
        self.consume_failed_tps.advance(now);
    }
}

struct MetricSeries {
    value: i64,
    calls: i64,
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
    fn record(&mut self, now: Instant, value: i64, calls: i64) {
        self.advance(now);
        self.value = self.value.wrapping_add(value);
        self.calls = self.calls.wrapping_add(calls);
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

    fn sample_until(&mut self, now: Instant, value: i64, calls: i64) {
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

    fn update_if_sampled_at(&mut self, at: Instant, value: i64, calls: i64) {
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

    fn average(&self) -> (f64, i64) {
        let Some((value, calls)) = self.delta() else {
            return (0.0, 0);
        };
        if calls == 0 {
            (0.0, 0)
        } else {
            (value as f64 / calls as f64, value)
        }
    }

    fn sum(&self) -> i64 {
        self.delta().map_or(0, |(value, _)| value)
    }

    fn delta(&self) -> Option<(i64, i64)> {
        let (Some(first), Some(last)) = (self.samples.front(), self.samples.back()) else {
            return None;
        };
        Some((
            last.value.wrapping_sub(first.value),
            last.calls.wrapping_sub(first.calls),
        ))
    }
}

#[derive(Clone, Copy)]
struct Snapshot {
    at: Instant,
    value: i64,
    calls: i64,
}

fn duration_from_nanos(nanos: u128) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn java_int(value: i64) -> i64 {
    i64::from(value as i32)
}

fn java_stats_key(group: &str, topic: &str) -> String {
    format!("{topic}@{group}")
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
        stats.inc_pull_rt("workers", "orders", 500);
        stats.inc_pull_tps("workers", "orders", 2);
        stats.inc_consume_rt("workers", "orders", 8);
        stats.inc_consume_ok_tps("workers", "orders", 3);
        stats.inc_consume_failed_tps("workers", "orders", 1);
        stats.inc_consume_failed_tps("workers", "orders", 2);

        let minute_target = Instant::now() + Duration::from_secs(10);
        let mut topics = stats.lock_topics();
        let topic = topics.get_mut("orders@workers").unwrap();
        topic.advance(minute_target);
        let status = topic.status(minute_target);

        assert_eq!(status.get_pull_rt(), 500.0);
        assert_eq!(status.get_consume_rt(), 8.0);
        assert!(status.get_pull_tps() > 0.0);
        assert!(status.get_consume_ok_tps() > 0.0);
        assert!(status.get_consume_failed_tps() > 0.0);
        assert_eq!(status.get_consume_failed_msgs(), 1);
        let hour_target = minute_target + Duration::from_secs(10 * 60);
        let hour_status = topic.status(hour_target);
        assert_eq!(hour_status.get_consume_failed_msgs(), 3);
        drop(topics);
        assert_eq!(
            serde_json::to_value(stats.status_at("workers", "other-topic", hour_target)).unwrap(),
            serde_json::to_value(ConsumerConsumeStatus::default()).unwrap()
        );
        assert_eq!(
            serde_json::to_value(stats.status_at("other-group", "orders", hour_target)).unwrap(),
            serde_json::to_value(ConsumerConsumeStatus::default()).unwrap()
        );
    }

    #[test]
    fn public_long_inputs_narrow_like_java_int_arguments() {
        assert_eq!(
            super::java_int(i64::from(i32::MAX) + 1),
            i64::from(i32::MIN)
        );
        assert_eq!(super::java_int(-1), -1);
    }

    #[test]
    fn stats_keys_match_java_topic_group_concatenation() {
        let stats = ConsumerStatsManager::new();
        stats.inc_pull_rt("workers@blue", "orders", 3);
        stats.inc_pull_rt("blue", "orders@workers", 5);

        let first = stats.consume_status("workers@blue", "orders").get_pull_rt();
        let colliding = stats.consume_status("blue", "orders@workers").get_pull_rt();
        assert!(first > 0.0);
        assert_eq!(first, colliding);
        assert_eq!(stats.lock_topics().len(), 1);
    }
}
