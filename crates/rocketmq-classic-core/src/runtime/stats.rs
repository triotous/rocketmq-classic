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
pub struct ConsumerStatsManager {
    sampling_origin: Instant,
    topics: Mutex<HashMap<StatsKey, TopicStats>>,
}

impl Default for ConsumerStatsManager {
    fn default() -> Self {
        Self::with_sampling_origin(Instant::now())
    }
}

impl ConsumerStatsManager {
    /// Creates an empty manager for one client runtime.
    pub fn new() -> Self {
        Self::default()
    }

    fn with_sampling_origin(sampling_origin: Instant) -> Self {
        Self {
            sampling_origin,
            topics: Mutex::new(HashMap::new()),
        }
    }

    /// Java's lifecycle hooks are no-ops; scheduled samples are evaluated lazily.
    pub fn start(&self) {}

    /// Java's lifecycle hooks are no-ops; the runtime owns this manager.
    pub fn shutdown(&self) {}

    pub(super) fn sample(&self) {
        let now = Instant::now();
        for stats in self.lock_topics().values_mut() {
            stats.advance(now);
        }
    }

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
        self.update_topic_at(group, topic, Instant::now(), update);
    }

    fn update_topic_at(
        &self,
        group: &str,
        topic: &str,
        now: Instant,
        update: impl FnOnce(&mut TopicStats, Instant),
    ) {
        let mut topics = self.lock_topics();
        let stats = topics
            .entry(java_stats_key(group, topic))
            .or_insert_with(|| TopicStats::new(self.sampling_origin));
        update(stats, now);
    }

    fn lock_topics(&self) -> std::sync::MutexGuard<'_, HashMap<StatsKey, TopicStats>> {
        self.topics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct TopicStats {
    pull_rt: MetricSeries,
    pull_tps: MetricSeries,
    consume_rt: MetricSeries,
    consume_ok_tps: MetricSeries,
    consume_failed_tps: MetricSeries,
}

impl TopicStats {
    fn new(sampling_origin: Instant) -> Self {
        Self {
            pull_rt: MetricSeries::new(sampling_origin),
            pull_tps: MetricSeries::new(sampling_origin),
            consume_rt: MetricSeries::new(sampling_origin),
            consume_ok_tps: MetricSeries::new(sampling_origin),
            consume_failed_tps: MetricSeries::new(sampling_origin),
        }
    }
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

impl MetricSeries {
    fn new(sampling_origin: Instant) -> Self {
        Self {
            value: 0,
            calls: 0,
            minute: SampleWindow::minute(sampling_origin),
            hour: SampleWindow::hour(sampling_origin),
        }
    }

    fn record(&mut self, now: Instant, value: i64, calls: i64) {
        self.advance(now);
        self.minute.schedule_after(now);
        self.hour.schedule_after(now);
        self.value = self.value.wrapping_add(value);
        self.calls = self.calls.wrapping_add(calls);
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
    sampling_origin: Instant,
    next_sample: Option<Instant>,
}

impl SampleWindow {
    fn minute(sampling_origin: Instant) -> Self {
        Self::new(MINUTE_SAMPLE_INTERVAL, MINUTE_SAMPLE_COUNT, sampling_origin)
    }

    fn hour(sampling_origin: Instant) -> Self {
        Self::new(HOUR_SAMPLE_INTERVAL, HOUR_SAMPLE_COUNT, sampling_origin)
    }

    fn new(interval: Duration, capacity: usize, sampling_origin: Instant) -> Self {
        Self {
            interval,
            capacity,
            samples: VecDeque::with_capacity(capacity),
            sampling_origin,
            next_sample: None,
        }
    }

    fn schedule_after(&mut self, now: Instant) {
        if self.next_sample.is_some() || self.interval.is_zero() {
            return;
        }
        let elapsed = now.saturating_duration_since(self.sampling_origin);
        let interval_nanos = self.interval.as_nanos();
        let ticks = elapsed.as_nanos() / interval_nanos + 1;
        self.next_sample = Some(
            self.sampling_origin
                .checked_add(duration_from_nanos(interval_nanos.saturating_mul(ticks)))
                .unwrap_or_else(|| now.checked_add(self.interval).unwrap_or(now)),
        );
    }

    fn sample_until(&mut self, now: Instant, value: i64, calls: i64) {
        if self.capacity == 0 || self.interval.is_zero() {
            return;
        }
        let Some(next_sample) = self.next_sample else {
            return;
        };
        if next_sample > now {
            return;
        }

        let interval_nanos = self.interval.as_nanos();
        let due = now.saturating_duration_since(next_sample).as_nanos() / interval_nanos + 1;
        let retained = due.min(self.capacity as u128);
        let skipped = due - retained;
        let first_retained = next_sample
            .checked_add(duration_from_nanos(interval_nanos.saturating_mul(skipped)))
            .unwrap_or(now);

        if self.samples.is_empty() {
            self.push(Snapshot {
                at: next_sample
                    .checked_sub(self.interval)
                    .unwrap_or(next_sample),
                value: 0,
                calls: 0,
            });
        }

        let retained = usize::try_from(retained).unwrap_or(self.capacity);
        let mut sample_at = first_retained;
        for index in 0..retained {
            self.push(Snapshot {
                at: sample_at,
                value,
                calls,
            });
            if index + 1 < retained {
                sample_at = sample_at.checked_add(self.interval).unwrap_or(now);
            }
        }
        let last_sample = next_sample
            .checked_add(duration_from_nanos(
                interval_nanos.saturating_mul(due.saturating_sub(1)),
            ))
            .unwrap_or(now);
        self.next_sample = last_sample.checked_add(self.interval);
    }

    fn push(&mut self, sample: Snapshot) {
        self.samples.push_back(sample);
        while self.samples.len() > self.capacity {
            self.samples.pop_front();
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
    use super::{ConsumerConsumeStatus, ConsumerStatsManager, MetricSeries};
    use std::time::{Duration, Instant};

    #[test]
    fn metrics_remain_zero_until_the_first_java_periodic_sample() {
        let origin = Instant::now();
        let mut series = MetricSeries::new(origin);
        series.record(origin + Duration::from_secs(1), 20, 1);
        series.advance(origin + Duration::from_secs(2));

        assert!(series.minute.samples.is_empty());
        assert_eq!(series.minute.rate(), 0.0);
        assert_eq!(series.minute.average(), (0.0, 0));

        series.advance(origin + Duration::from_secs(10));
        assert_eq!(series.minute.rate(), 2.0);
        assert_eq!(series.minute.average(), (20.0, 20));

        series.record(origin + Duration::from_secs(11), 5, 1);
        series.advance(origin + Duration::from_secs(19));
        assert_eq!(series.minute.rate(), 2.0);
        assert_eq!(series.minute.average(), (20.0, 20));

        series.advance(origin + Duration::from_secs(20));
        assert_eq!(series.minute.average(), (12.5, 25));
        assert_eq!(series.minute.rate(), 1.25);

        series.advance(origin + Duration::from_secs(70));
        assert_eq!(series.minute.average(), (5.0, 5));
        assert!((series.minute.rate() - (5.0 / 60.0)).abs() < f64::EPSILON);
        assert_eq!(series.hour.sum(), 0);

        series.advance(origin + Duration::from_secs(80));
        assert_eq!(series.minute.average(), (0.0, 0));
        assert_eq!(series.minute.rate(), 0.0);
        assert_eq!(series.hour.sum(), 0);

        series.advance(origin + Duration::from_secs(600));
        assert_eq!(series.hour.sum(), 25);
    }

    #[test]
    fn consume_status_is_partitioned_and_failed_results_do_not_count_as_success_calls() {
        let origin = Instant::now();
        let stats = ConsumerStatsManager::with_sampling_origin(origin);
        stats.update_topic_at(
            "workers",
            "orders",
            origin + Duration::from_secs(1),
            |topic, now| {
                topic.pull_rt.record(now, 500, 1);
                topic.pull_tps.record(now, 2, 1);
                topic.consume_rt.record(now, 8, 1);
                topic.consume_ok_tps.record(now, 3, 1);
                topic.consume_failed_tps.record(now, 1, 1);
                topic.consume_failed_tps.record(now, 2, 1);
            },
        );

        let before_sample = stats.status_at("workers", "orders", origin + Duration::from_secs(2));
        assert_eq!(before_sample.get_pull_rt(), 0.0);
        assert_eq!(before_sample.get_pull_tps(), 0.0);
        assert_eq!(before_sample.get_consume_rt(), 0.0);

        let minute_target = origin + Duration::from_secs(10);
        let status = stats.status_at("workers", "orders", minute_target);

        assert_eq!(status.get_pull_rt(), 500.0);
        assert_eq!(status.get_consume_rt(), 8.0);
        assert_eq!(status.get_pull_tps(), 0.2);
        assert_eq!(status.get_consume_ok_tps(), 0.3);
        assert_eq!(status.get_consume_failed_tps(), 0.3);
        assert_eq!(status.get_consume_failed_msgs(), 0);
        let hour_target = origin + Duration::from_secs(10 * 60);
        let hour_status = stats.status_at("workers", "orders", hour_target);
        assert_eq!(hour_status.get_consume_failed_msgs(), 3);
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
        let origin = Instant::now();
        let stats = ConsumerStatsManager::with_sampling_origin(origin);
        stats.update_topic_at(
            "workers@blue",
            "orders",
            origin + Duration::from_secs(1),
            |topic, now| {
                topic.pull_rt.record(now, 3, 1);
            },
        );
        stats.update_topic_at(
            "blue",
            "orders@workers",
            origin + Duration::from_secs(1),
            |topic, now| {
                topic.pull_rt.record(now, 5, 1);
            },
        );

        let first = stats
            .status_at("workers@blue", "orders", origin + Duration::from_secs(10))
            .get_pull_rt();
        let colliding = stats
            .status_at("blue", "orders@workers", origin + Duration::from_secs(10))
            .get_pull_rt();
        assert_eq!(first, 4.0);
        assert_eq!(first, colliding);
        assert_eq!(stats.lock_topics().len(), 1);
    }

    #[test]
    fn sampling_matches_the_pinned_java_consumer_stats_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/consumer/push-implementation-accessor.json"
        ))
        .unwrap();
        let origin = Instant::now();
        let stats = ConsumerStatsManager::with_sampling_origin(origin);
        stats.update_topic_at(
            "workers",
            "orders",
            origin + Duration::from_secs(1),
            |topic, now| {
                topic.pull_tps.record(now, 20, 1);
            },
        );

        let before_sample = stats
            .status_at("workers", "orders", origin + Duration::from_secs(2))
            .get_pull_tps();
        let after_sample = stats
            .status_at("workers", "orders", origin + Duration::from_secs(10))
            .get_pull_tps();
        stats.update_topic_at(
            "workers",
            "orders",
            origin + Duration::from_secs(11),
            |topic, now| {
                topic.pull_tps.record(now, 5, 1);
            },
        );
        let before_next_sample = stats
            .status_at("workers", "orders", origin + Duration::from_secs(19))
            .get_pull_tps();

        assert_eq!(
            before_sample,
            fixture["consumerStatsSamplingBeforeFirstSamplePullTPS"]
                .as_f64()
                .unwrap()
        );
        assert_eq!(
            after_sample,
            fixture["consumerStatsSamplingAfterFirstSamplePullTPS"]
                .as_f64()
                .unwrap()
        );
        assert_eq!(
            before_next_sample,
            fixture["consumerStatsSamplingAfterPendingUpdatePullTPS"]
                .as_f64()
                .unwrap()
        );
    }
}
