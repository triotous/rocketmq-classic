use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocketmq_classic_core::config::ClientConfig;
use rocketmq_classic_protocol::heartbeat::{ConsumeFromWhere, MessageModel};

use crate::{config::validate_group, error::PushConsumerError};

pub const DEFAULT_PUSH_CONSUMER_GROUP: &str = "DEFAULT_CONSUMER";
pub const DEFAULT_CONSUME_THREAD_MIN: i32 = 20;
pub const DEFAULT_CONSUME_THREAD_MAX: i32 = 20;
pub const DEFAULT_ADJUST_THREAD_POOL_NUMS_THRESHOLD: i64 = 100_000;
pub const DEFAULT_CONSUME_CONCURRENTLY_MAX_SPAN: i32 = 2_000;
pub const DEFAULT_PULL_THRESHOLD_FOR_QUEUE: i32 = 1_000;
pub const DEFAULT_PULL_THRESHOLD_SIZE_FOR_QUEUE_MIB: i32 = 100;
pub const DEFAULT_PULL_THRESHOLD_FOR_TOPIC: i32 = -1;
pub const DEFAULT_PULL_THRESHOLD_SIZE_FOR_TOPIC_MIB: i32 = -1;
pub const DEFAULT_PULL_INTERVAL_MILLIS: i64 = 0;
pub const DEFAULT_CONSUME_MESSAGE_BATCH_MAX_SIZE: i32 = 1;
pub const DEFAULT_PULL_BATCH_SIZE: i32 = 32;
pub const DEFAULT_PULL_BATCH_SIZE_IN_BYTES: i32 = 256 * 1024;
pub const DEFAULT_MAX_RECONSUME_TIMES: i32 = -1;
pub const DEFAULT_CONSUME_TIMEOUT_MINUTES: i64 = 15;
pub const DEFAULT_AWAIT_TERMINATION_MILLIS_WHEN_SHUTDOWN: i64 = 0;

/// Java 5.5.1 `DefaultMQPushConsumer` settings owned by Phase 10.
///
/// POP, queue allocation, orderly consumption, tracing and public consume
/// hooks remain owned by their later phases.
pub struct PushConsumerConfig {
    client: ClientConfig,
    consumer_group: String,
    message_model: MessageModel,
    consume_from_where: ConsumeFromWhere,
    consume_timestamp: String,
    consume_thread_min: i32,
    consume_thread_max: i32,
    adjust_thread_pool_nums_threshold: i64,
    consume_concurrently_max_span: i32,
    pull_threshold_for_queue: i32,
    pull_threshold_size_for_queue_mib: i32,
    pull_threshold_for_topic: i32,
    pull_threshold_size_for_topic_mib: i32,
    pull_interval_millis: i64,
    consume_message_batch_max_size: i32,
    pull_batch_size: i32,
    pull_batch_size_in_bytes: i32,
    post_subscription_when_pull: bool,
    unit_mode: bool,
    max_reconsume_times: i32,
    consume_timeout_minutes: i64,
    await_termination_millis_when_shutdown: i64,
}

impl PushConsumerConfig {
    pub fn new(consumer_group: impl Into<String>) -> Result<Self, PushConsumerError> {
        Ok(Self {
            client: ClientConfig::new_auto()?,
            consumer_group: consumer_group.into(),
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            consume_timestamp: default_consume_timestamp(),
            consume_thread_min: DEFAULT_CONSUME_THREAD_MIN,
            consume_thread_max: DEFAULT_CONSUME_THREAD_MAX,
            adjust_thread_pool_nums_threshold: DEFAULT_ADJUST_THREAD_POOL_NUMS_THRESHOLD,
            consume_concurrently_max_span: DEFAULT_CONSUME_CONCURRENTLY_MAX_SPAN,
            pull_threshold_for_queue: DEFAULT_PULL_THRESHOLD_FOR_QUEUE,
            pull_threshold_size_for_queue_mib: DEFAULT_PULL_THRESHOLD_SIZE_FOR_QUEUE_MIB,
            pull_threshold_for_topic: DEFAULT_PULL_THRESHOLD_FOR_TOPIC,
            pull_threshold_size_for_topic_mib: DEFAULT_PULL_THRESHOLD_SIZE_FOR_TOPIC_MIB,
            pull_interval_millis: DEFAULT_PULL_INTERVAL_MILLIS,
            consume_message_batch_max_size: DEFAULT_CONSUME_MESSAGE_BATCH_MAX_SIZE,
            pull_batch_size: DEFAULT_PULL_BATCH_SIZE,
            pull_batch_size_in_bytes: DEFAULT_PULL_BATCH_SIZE_IN_BYTES,
            post_subscription_when_pull: false,
            unit_mode: false,
            max_reconsume_times: DEFAULT_MAX_RECONSUME_TIMES,
            consume_timeout_minutes: DEFAULT_CONSUME_TIMEOUT_MINUTES,
            await_termination_millis_when_shutdown: DEFAULT_AWAIT_TERMINATION_MILLIS_WHEN_SHUTDOWN,
        })
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    pub fn client_config_mut(&mut self) -> &mut ClientConfig {
        &mut self.client
    }

    pub fn consumer_group(&self) -> &str {
        &self.consumer_group
    }

    pub fn set_consumer_group(&mut self, value: impl Into<String>) {
        self.consumer_group = value.into();
    }

    pub const fn message_model(&self) -> MessageModel {
        self.message_model
    }

    pub fn set_message_model(&mut self, value: MessageModel) {
        self.message_model = value;
    }

    pub const fn consume_from_where(&self) -> ConsumeFromWhere {
        self.consume_from_where
    }

    pub fn set_consume_from_where(&mut self, value: ConsumeFromWhere) {
        self.consume_from_where = value;
    }

    pub fn consume_timestamp(&self) -> &str {
        &self.consume_timestamp
    }

    pub fn set_consume_timestamp(&mut self, value: impl Into<String>) {
        self.consume_timestamp = value.into();
    }

    pub const fn consume_thread_min(&self) -> i32 {
        self.consume_thread_min
    }

    pub fn set_consume_thread_min(&mut self, value: i32) {
        self.consume_thread_min = value;
    }

    pub const fn consume_thread_max(&self) -> i32 {
        self.consume_thread_max
    }

    pub fn set_consume_thread_max(&mut self, value: i32) {
        self.consume_thread_max = value;
    }

    pub const fn adjust_thread_pool_nums_threshold(&self) -> i64 {
        self.adjust_thread_pool_nums_threshold
    }

    pub fn set_adjust_thread_pool_nums_threshold(&mut self, value: i64) {
        self.adjust_thread_pool_nums_threshold = value;
    }

    pub const fn consume_concurrently_max_span(&self) -> i32 {
        self.consume_concurrently_max_span
    }

    pub fn set_consume_concurrently_max_span(&mut self, value: i32) {
        self.consume_concurrently_max_span = value;
    }

    pub const fn pull_threshold_for_queue(&self) -> i32 {
        self.pull_threshold_for_queue
    }

    pub fn set_pull_threshold_for_queue(&mut self, value: i32) {
        self.pull_threshold_for_queue = value;
    }

    pub const fn pull_threshold_size_for_queue_mib(&self) -> i32 {
        self.pull_threshold_size_for_queue_mib
    }

    pub fn set_pull_threshold_size_for_queue_mib(&mut self, value: i32) {
        self.pull_threshold_size_for_queue_mib = value;
    }

    pub const fn pull_threshold_for_topic(&self) -> i32 {
        self.pull_threshold_for_topic
    }

    pub fn set_pull_threshold_for_topic(&mut self, value: i32) {
        self.pull_threshold_for_topic = value;
    }

    pub const fn pull_threshold_size_for_topic_mib(&self) -> i32 {
        self.pull_threshold_size_for_topic_mib
    }

    pub fn set_pull_threshold_size_for_topic_mib(&mut self, value: i32) {
        self.pull_threshold_size_for_topic_mib = value;
    }

    pub const fn pull_interval_millis(&self) -> i64 {
        self.pull_interval_millis
    }

    pub fn set_pull_interval_millis(&mut self, value: i64) {
        self.pull_interval_millis = value;
    }

    pub const fn consume_message_batch_max_size(&self) -> i32 {
        self.consume_message_batch_max_size
    }

    pub fn set_consume_message_batch_max_size(&mut self, value: i32) {
        self.consume_message_batch_max_size = value;
    }

    pub const fn pull_batch_size(&self) -> i32 {
        self.pull_batch_size
    }

    pub fn set_pull_batch_size(&mut self, value: i32) {
        self.pull_batch_size = value;
    }

    pub const fn pull_batch_size_in_bytes(&self) -> i32 {
        self.pull_batch_size_in_bytes
    }

    pub fn set_pull_batch_size_in_bytes(&mut self, value: i32) {
        self.pull_batch_size_in_bytes = value;
    }

    pub const fn post_subscription_when_pull(&self) -> bool {
        self.post_subscription_when_pull
    }

    pub fn set_post_subscription_when_pull(&mut self, value: bool) {
        self.post_subscription_when_pull = value;
    }

    pub const fn unit_mode(&self) -> bool {
        self.unit_mode
    }

    pub fn set_unit_mode(&mut self, value: bool) {
        self.unit_mode = value;
    }

    pub const fn max_reconsume_times(&self) -> i32 {
        self.max_reconsume_times
    }

    /// Java `DefaultMQPushConsumerImpl.getMaxReconsumeTimes()` resolves only
    /// the `-1` default to 16 in concurrent mode.
    pub const fn effective_max_reconsume_times(&self) -> i32 {
        if self.max_reconsume_times == -1 {
            16
        } else {
            self.max_reconsume_times
        }
    }

    pub fn set_max_reconsume_times(&mut self, value: i32) {
        self.max_reconsume_times = value;
    }

    pub const fn consume_timeout_minutes(&self) -> i64 {
        self.consume_timeout_minutes
    }

    pub fn set_consume_timeout_minutes(&mut self, value: i64) {
        self.consume_timeout_minutes = value;
    }

    pub const fn await_termination_millis_when_shutdown(&self) -> i64 {
        self.await_termination_millis_when_shutdown
    }

    pub fn set_await_termination_millis_when_shutdown(&mut self, value: i64) {
        self.await_termination_millis_when_shutdown = value;
    }

    pub(crate) fn validate(&self, effective_consumer_group: &str) -> Result<(), PushConsumerError> {
        validate_group(effective_consumer_group)
            .map_err(|_| PushConsumerError::InvalidConsumerGroup)?;
        if effective_consumer_group == DEFAULT_PUSH_CONSUMER_GROUP {
            return Err(PushConsumerError::DefaultConsumerGroup);
        }
        if !valid_consume_timestamp(&self.consume_timestamp) {
            return Err(PushConsumerError::InvalidConsumeTimestamp);
        }
        for (name, value) in [
            ("consumeThreadMin", self.consume_thread_min),
            ("consumeThreadMax", self.consume_thread_max),
        ] {
            validate_range(name, i64::from(value), 1, 1_000)?;
        }
        if self.consume_thread_min > self.consume_thread_max {
            return Err(PushConsumerError::InvalidThreadBounds {
                minimum: self.consume_thread_min,
                maximum: self.consume_thread_max,
            });
        }
        validate_range(
            "consumeConcurrentlyMaxSpan",
            i64::from(self.consume_concurrently_max_span),
            1,
            65_535,
        )?;
        validate_range(
            "pullThresholdForQueue",
            i64::from(self.pull_threshold_for_queue),
            1,
            65_535,
        )?;
        validate_optional_range(
            "pullThresholdForTopic",
            self.pull_threshold_for_topic,
            6_553_500,
        )?;
        validate_range(
            "pullThresholdSizeForQueue",
            i64::from(self.pull_threshold_size_for_queue_mib),
            1,
            1_024,
        )?;
        validate_optional_range(
            "pullThresholdSizeForTopic",
            self.pull_threshold_size_for_topic_mib,
            102_400,
        )?;
        validate_range("pullInterval", self.pull_interval_millis, 0, 65_535)?;
        validate_range(
            "consumeMessageBatchMaxSize",
            i64::from(self.consume_message_batch_max_size),
            1,
            1_024,
        )?;
        validate_range("pullBatchSize", i64::from(self.pull_batch_size), 1, 1_024)?;
        Ok(())
    }
}

fn validate_range(
    name: &'static str,
    value: i64,
    minimum: i64,
    maximum: i64,
) -> Result<(), PushConsumerError> {
    if !(minimum..=maximum).contains(&value) {
        return Err(PushConsumerError::InvalidSetting {
            name,
            value,
            minimum,
            maximum,
        });
    }
    Ok(())
}

fn validate_optional_range(
    name: &'static str,
    value: i32,
    maximum: i64,
) -> Result<(), PushConsumerError> {
    if value == -1 {
        return Ok(());
    }
    validate_range(name, i64::from(value), 1, maximum)
}

fn valid_consume_timestamp(value: &str) -> bool {
    if value.len() != 14 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    let number = |range: std::ops::Range<usize>| -> Option<u32> { value.get(range)?.parse().ok() };
    let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) = (
        number(0..4),
        number(4..6),
        number(6..8),
        number(8..10),
        number(10..12),
        number(12..14),
    ) else {
        return false;
    };
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    (1..=days_in_month).contains(&day) && hour <= 23 && minute <= 59 && second <= 59
}

fn default_consume_timestamp() -> String {
    let target = SystemTime::now()
        .checked_sub(Duration::from_secs(30 * 60))
        .unwrap_or(UNIX_EPOCH);
    let seconds = target
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());

    #[cfg(unix)]
    if let Ok(seconds) = libc::time_t::try_from(seconds) {
        let mut local = std::mem::MaybeUninit::<libc::tm>::uninit();
        // `localtime_r` writes the result into the supplied, uniquely owned buffer.
        let result = unsafe { libc::localtime_r(&seconds, local.as_mut_ptr()) };
        if !result.is_null() {
            // SAFETY: a non-null return means `localtime_r` initialized `local`.
            let local = unsafe { local.assume_init() };
            return format!(
                "{:04}{:02}{:02}{:02}{:02}{:02}",
                local.tm_year + 1900,
                local.tm_mon + 1,
                local.tm_mday,
                local.tm_hour,
                local.tm_min,
                local.tm_sec,
            );
        }
    }

    format_utc_timestamp(seconds)
}

#[cfg_attr(unix, allow(dead_code))]
fn format_utc_timestamp(seconds: u64) -> String {
    let days = (seconds / 86_400) as i64;
    let seconds_in_day = seconds % 86_400;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    let hour = seconds_in_day / 3_600;
    let minute = seconds_in_day % 3_600 / 60;
    let second = seconds_in_day % 60;
    format!("{year:04}{month:02}{day:02}{hour:02}{minute:02}{second:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java_push_consumer() {
        let config = PushConsumerConfig::new("orders").unwrap();
        assert_eq!(config.message_model(), MessageModel::Clustering);
        assert_eq!(
            config.consume_from_where(),
            ConsumeFromWhere::ConsumeFromLastOffset
        );
        assert_eq!(config.consume_thread_min(), 20);
        assert_eq!(config.consume_thread_max(), 20);
        assert_eq!(config.adjust_thread_pool_nums_threshold(), 100_000);
        assert_eq!(config.consume_concurrently_max_span(), 2_000);
        assert!(!config.post_subscription_when_pull());
        assert!(!config.unit_mode());
        assert_eq!(config.pull_threshold_for_queue(), 1_000);
        assert_eq!(config.pull_threshold_size_for_queue_mib(), 100);
        assert_eq!(config.pull_threshold_for_topic(), -1);
        assert_eq!(config.pull_threshold_size_for_topic_mib(), -1);
        assert_eq!(config.pull_interval_millis(), 0);
        assert_eq!(config.consume_message_batch_max_size(), 1);
        assert_eq!(config.pull_batch_size(), 32);
        assert_eq!(config.pull_batch_size_in_bytes(), 256 * 1024);
        assert_eq!(config.max_reconsume_times(), -1);
        assert_eq!(config.effective_max_reconsume_times(), 16);
        let mut config = config;
        config.set_max_reconsume_times(-2);
        assert_eq!(config.effective_max_reconsume_times(), -2);
        assert_eq!(config.consume_timeout_minutes(), 15);
        assert_eq!(config.await_termination_millis_when_shutdown(), 0);
        assert!(valid_consume_timestamp(config.consume_timestamp()));
    }

    #[test]
    fn validation_matches_java_push_ranges_and_topic_unlimited_sentinel() {
        let mut config = PushConsumerConfig::new("orders").unwrap();
        assert!(config.validate("orders").is_ok());

        config.set_consume_thread_min(0);
        assert!(config.validate("orders").is_err());
        config.set_consume_thread_min(1);
        config.set_consume_thread_max(1);
        assert!(config.validate("orders").is_ok());
        config.set_consume_thread_min(1_001);
        assert!(config.validate("orders").is_err());
        config.set_consume_thread_min(20);
        config.set_consume_thread_max(1_001);
        assert!(config.validate("orders").is_err());
        config.set_consume_thread_max(19);
        assert!(matches!(
            config.validate("orders"),
            Err(PushConsumerError::InvalidThreadBounds { .. })
        ));
        config.set_consume_thread_max(20);

        config.set_consume_concurrently_max_span(0);
        assert!(config.validate("orders").is_err());
        config.set_consume_concurrently_max_span(65_536);
        assert!(config.validate("orders").is_err());
        config.set_consume_concurrently_max_span(65_535);
        assert!(config.validate("orders").is_ok());

        config.set_pull_threshold_for_queue(0);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_for_queue(65_536);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_for_queue(65_535);
        assert!(config.validate("orders").is_ok());

        config.set_pull_threshold_for_topic(0);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_for_topic(6_553_500);
        assert!(config.validate("orders").is_ok());
        config.set_pull_threshold_for_topic(6_553_501);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_for_topic(-1);
        assert!(config.validate("orders").is_ok());

        config.set_pull_threshold_size_for_queue_mib(0);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_size_for_queue_mib(1_025);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_size_for_queue_mib(1_024);
        assert!(config.validate("orders").is_ok());

        config.set_pull_threshold_size_for_topic_mib(0);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_size_for_topic_mib(102_400);
        assert!(config.validate("orders").is_ok());
        config.set_pull_threshold_size_for_topic_mib(102_401);
        assert!(config.validate("orders").is_err());
        config.set_pull_threshold_size_for_topic_mib(-1);

        config.set_pull_interval_millis(-1);
        assert!(config.validate("orders").is_err());
        config.set_pull_interval_millis(65_535);
        assert!(config.validate("orders").is_ok());
        config.set_pull_interval_millis(65_536);
        assert!(config.validate("orders").is_err());
        config.set_pull_interval_millis(0);

        config.set_consume_message_batch_max_size(0);
        assert!(config.validate("orders").is_err());
        config.set_consume_message_batch_max_size(1_024);
        assert!(config.validate("orders").is_ok());
        config.set_consume_message_batch_max_size(1_025);
        assert!(config.validate("orders").is_err());
        config.set_consume_message_batch_max_size(1);

        config.set_pull_batch_size(0);
        assert!(config.validate("orders").is_err());
        config.set_pull_batch_size(1_024);
        assert!(config.validate("orders").is_ok());
        config.set_pull_batch_size(1_025);
        assert!(config.validate("orders").is_err());
    }

    #[test]
    fn group_and_timestamp_validation_fail_before_startup() {
        let config = PushConsumerConfig::new("orders").unwrap();
        assert!(matches!(
            config.validate(DEFAULT_PUSH_CONSUMER_GROUP),
            Err(PushConsumerError::DefaultConsumerGroup)
        ));
        assert!(matches!(
            config.validate("bad/group"),
            Err(PushConsumerError::InvalidConsumerGroup)
        ));

        let mut config = PushConsumerConfig::new("orders").unwrap();
        config.set_consume_timestamp("20260230010101");
        assert!(matches!(
            config.validate("orders"),
            Err(PushConsumerError::InvalidConsumeTimestamp)
        ));
    }

    #[test]
    fn timestamp_validation_checks_calendar_and_time_fields() {
        assert!(valid_consume_timestamp("20240229010203"));
        assert!(!valid_consume_timestamp("20230229010203"));
        assert!(!valid_consume_timestamp("20260101246000"));
        assert!(!valid_consume_timestamp("2026010112000"));
        assert!(!valid_consume_timestamp("2026010112000x"));
    }

    #[test]
    fn utc_fallback_formatter_matches_java_timestamp_shape() {
        assert_eq!(format_utc_timestamp(0), "19700101000000");
        assert_eq!(format_utc_timestamp(1_738_368_000), "20250201000000");
    }
}
