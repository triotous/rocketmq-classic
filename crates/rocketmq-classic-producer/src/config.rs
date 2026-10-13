use std::collections::BTreeSet;

use rocketmq_classic_common::sysflag::message::CompressionType;
use rocketmq_classic_core::config::ClientConfig;
use rocketmq_classic_remoting::client::TlsConfig;

use crate::ProducerError;

pub const DEFAULT_PRODUCER_GROUP: &str = "DEFAULT_PRODUCER";
pub const DEFAULT_CREATE_TOPIC_KEY: &str = "TBW102";
pub const DEFAULT_SEND_TIMEOUT_MILLIS: i32 = 3_000;
pub const DEFAULT_COMPRESS_THRESHOLD: i32 = 4_096;
pub const DEFAULT_SYNC_RETRIES: i32 = 2;
pub const DEFAULT_ASYNC_RETRIES: i32 = 2;
pub const DEFAULT_MAX_MESSAGE_SIZE: i32 = 4 * 1024 * 1024;
pub const DEFAULT_TOPIC_QUEUE_COUNT: i32 = 4;
pub const DEFAULT_BACKPRESSURE_ASYNC_SEND_NUM: i32 = 1_024;
pub const DEFAULT_BACKPRESSURE_ASYNC_SEND_SIZE: i32 = 100 * 1024 * 1024;
pub const DEFAULT_BATCH_MAX_DELAY_MILLIS: i32 = 10;
pub const DEFAULT_BATCH_MAX_BYTES: i64 = 32 * 1024;
pub const DEFAULT_TOTAL_BATCH_MAX_BYTES: i64 = 32 * 1024 * 1024;
pub const DEFAULT_LATENCY_MAX_MILLIS: [i64; 7] = [50, 100, 550, 1_800, 3_000, 5_000, 15_000];
pub const DEFAULT_NOT_AVAILABLE_DURATION_MILLIS: [i64; 7] =
    [0, 0, 2_000, 5_000, 6_000, 10_000, 30_000];
pub const MESSAGE_COMPRESS_TYPE_PROPERTY: &str = "rocketmq.message.compressType";
pub const MESSAGE_COMPRESS_LEVEL_PROPERTY: &str = "rocketmq.message.compressLevel";

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccumulatorSettings {
    pub(crate) max_delay_millis: i32,
    pub(crate) max_bytes: usize,
    pub(crate) total_max_bytes: usize,
}

/// Producer settings from Java 5.5.1. Client-level transport settings stay
/// in the embedded `ClientConfig`.
#[derive(Clone)]
pub struct ProducerConfig {
    pub(crate) client: ClientConfig,
    tls_config: Option<TlsConfig>,
    producer_group: String,
    create_topic_key: String,
    default_topic_queue_nums: i32,
    send_timeout_millis: i32,
    send_msg_max_timeout_per_request: i32,
    compress_threshold: i32,
    compress_type: CompressionType,
    compress_level: i32,
    retry_times_when_send_failed: i32,
    retry_times_when_send_async_failed: i32,
    retry_response_codes: BTreeSet<i32>,
    retry_another_broker_when_not_store_ok: bool,
    max_message_size: i32,
    auto_batch: bool,
    batch_max_delay_millis: Option<i32>,
    batch_max_bytes: Option<i64>,
    total_batch_max_bytes: Option<i64>,
    enable_backpressure_for_async_mode: bool,
    backpressure_async_send_num: i32,
    backpressure_async_send_size: i32,
    latency_max_millis: Vec<i64>,
    not_available_duration_millis: Vec<i64>,
    topics: Option<Vec<String>>,
}

impl ProducerConfig {
    pub fn new(group: impl Into<String>) -> Result<Self, ProducerError> {
        let (compress_type, compress_level) =
            Self::compression_settings(|key| std::env::var(key).ok())?;
        Ok(Self {
            client: ClientConfig::new_auto()?,
            tls_config: None,
            producer_group: group.into(),
            create_topic_key: DEFAULT_CREATE_TOPIC_KEY.into(),
            default_topic_queue_nums: DEFAULT_TOPIC_QUEUE_COUNT,
            send_timeout_millis: DEFAULT_SEND_TIMEOUT_MILLIS,
            send_msg_max_timeout_per_request: -1,
            compress_threshold: DEFAULT_COMPRESS_THRESHOLD,
            compress_type,
            compress_level,
            retry_times_when_send_failed: DEFAULT_SYNC_RETRIES,
            retry_times_when_send_async_failed: DEFAULT_ASYNC_RETRIES,
            retry_response_codes: [17, 14, 1, 2, 16, 204, 205, 1500].into(),
            retry_another_broker_when_not_store_ok: false,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            auto_batch: false,
            batch_max_delay_millis: None,
            batch_max_bytes: None,
            total_batch_max_bytes: None,
            enable_backpressure_for_async_mode: false,
            backpressure_async_send_num: DEFAULT_BACKPRESSURE_ASYNC_SEND_NUM,
            backpressure_async_send_size: DEFAULT_BACKPRESSURE_ASYNC_SEND_SIZE,
            latency_max_millis: DEFAULT_LATENCY_MAX_MILLIS.to_vec(),
            not_available_duration_millis: DEFAULT_NOT_AVAILABLE_DURATION_MILLIS.to_vec(),
            topics: None,
        })
    }

    fn compression_settings(
        mut property: impl FnMut(&str) -> Option<String>,
    ) -> Result<(CompressionType, i32), ProducerError> {
        let compress_type = property(MESSAGE_COMPRESS_TYPE_PROPERTY)
            .map(|value| CompressionType::of(Some(&value)))
            .transpose()?;
        let compress_level = property(MESSAGE_COMPRESS_LEVEL_PROPERTY)
            .map(|value| {
                value
                    .parse::<i32>()
                    .map_err(|_| ProducerError::InvalidCompressionLevel(value))
            })
            .transpose()?;
        Ok((
            compress_type.unwrap_or(CompressionType::Zlib),
            compress_level.unwrap_or(5),
        ))
    }

    pub(crate) fn accumulator_settings(&self) -> Result<AccumulatorSettings, ProducerError> {
        let max_delay_millis = match self.batch_max_delay_millis {
            Some(value) if value > -1 => {
                if !(1..=30_000).contains(&value) {
                    return Err(ProducerError::InvalidBatchMaxDelay(value));
                }
                value
            }
            _ => DEFAULT_BATCH_MAX_DELAY_MILLIS,
        };
        let max_bytes = match self.batch_max_bytes {
            Some(value) if value > -1 => {
                if !(1..=2 * 1024 * 1024).contains(&value) {
                    return Err(ProducerError::InvalidBatchMaxBytes(value));
                }
                value as usize
            }
            _ => DEFAULT_BATCH_MAX_BYTES as usize,
        };
        let total_max_bytes = match self.total_batch_max_bytes {
            Some(value) if value > -1 => {
                if value <= 0 {
                    return Err(ProducerError::InvalidTotalBatchMaxBytes(value));
                }
                usize::try_from(value)
                    .map_err(|_| ProducerError::InvalidTotalBatchMaxBytes(value))?
            }
            _ => DEFAULT_TOTAL_BATCH_MAX_BYTES as usize,
        };
        Ok(AccumulatorSettings {
            max_delay_millis,
            max_bytes,
            total_max_bytes,
        })
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }
    pub fn client_config_mut(&mut self) -> &mut ClientConfig {
        &mut self.client
    }
    /// Returns the explicit transport TLS configuration, when one was set.
    pub fn tls_config(&self) -> Option<&TlsConfig> {
        self.tls_config.as_ref()
    }
    /// Sets an explicit TLS configuration for the shared client runtime.
    /// A configured TLS value also enables TLS on the embedded `ClientConfig`.
    pub fn set_tls_config(&mut self, value: Option<TlsConfig>) {
        if value.is_some() {
            self.client.set_use_tls(true);
        }
        self.tls_config = value;
    }
    pub fn send_message_with_vip_channel(&self) -> bool {
        self.client.vip_channel_enabled()
    }
    pub fn set_send_message_with_vip_channel(&mut self, value: bool) {
        self.client.set_vip_channel_enabled(value);
    }
    pub fn send_latency_fault_enable(&self) -> bool {
        self.client.send_latency_enable()
    }
    pub fn set_send_latency_fault_enable(&mut self, value: bool) {
        self.client.set_send_latency_enable(value);
    }
    pub fn start_detector_enable(&self) -> bool {
        self.client.start_detector_enable()
    }
    pub fn set_start_detector_enable(&mut self, value: bool) {
        self.client.set_start_detector_enable(value);
    }
    pub fn detect_timeout_millis(&self) -> i32 {
        self.client.detect_timeout()
    }
    pub fn set_detect_timeout_millis(&mut self, value: i32) {
        self.client.set_detect_timeout(value);
    }
    pub fn detect_interval_millis(&self) -> i32 {
        self.client.detect_interval()
    }
    pub fn set_detect_interval_millis(&mut self, value: i32) {
        self.client.set_detect_interval(value);
    }
    pub fn producer_group(&self) -> &str {
        &self.producer_group
    }
    pub fn set_producer_group(&mut self, value: impl Into<String>) {
        self.producer_group = value.into();
    }
    pub fn create_topic_key(&self) -> &str {
        &self.create_topic_key
    }
    pub fn set_create_topic_key(&mut self, value: impl Into<String>) {
        self.create_topic_key = value.into();
    }
    pub fn default_topic_queue_nums(&self) -> i32 {
        self.default_topic_queue_nums
    }
    pub fn set_default_topic_queue_nums(&mut self, value: i32) {
        self.default_topic_queue_nums = value;
    }
    pub fn send_timeout_millis(&self) -> i32 {
        self.send_timeout_millis
    }
    pub fn set_send_timeout_millis(&mut self, value: i32) {
        self.send_timeout_millis = value;
    }
    pub fn send_msg_max_timeout_per_request(&self) -> i32 {
        self.send_msg_max_timeout_per_request
    }
    pub fn set_send_msg_max_timeout_per_request(&mut self, value: i32) {
        self.send_msg_max_timeout_per_request = value;
    }
    pub fn compress_threshold(&self) -> i32 {
        self.compress_threshold
    }
    pub fn set_compress_threshold(&mut self, value: i32) {
        self.compress_threshold = value;
    }
    pub fn compress_type(&self) -> CompressionType {
        self.compress_type
    }
    pub fn set_compress_type(&mut self, value: CompressionType) {
        self.compress_type = value;
    }
    pub fn compress_level(&self) -> i32 {
        self.compress_level
    }
    pub fn set_compress_level(&mut self, value: i32) {
        self.compress_level = value;
    }
    pub fn retry_times_when_send_failed(&self) -> i32 {
        self.retry_times_when_send_failed
    }
    pub fn set_retry_times_when_send_failed(&mut self, value: i32) {
        self.retry_times_when_send_failed = value;
    }
    pub fn retry_times_when_send_async_failed(&self) -> i32 {
        self.retry_times_when_send_async_failed
    }
    pub fn set_retry_times_when_send_async_failed(&mut self, value: i32) {
        self.retry_times_when_send_async_failed = value;
    }
    pub fn retry_response_codes(&self) -> &BTreeSet<i32> {
        &self.retry_response_codes
    }
    pub fn add_retry_response_code(&mut self, value: i32) {
        self.retry_response_codes.insert(value);
    }
    pub fn remove_retry_response_code(&mut self, value: i32) {
        self.retry_response_codes.remove(&value);
    }
    pub fn retry_another_broker_when_not_store_ok(&self) -> bool {
        self.retry_another_broker_when_not_store_ok
    }
    pub fn set_retry_another_broker_when_not_store_ok(&mut self, value: bool) {
        self.retry_another_broker_when_not_store_ok = value;
    }
    pub fn max_message_size(&self) -> i32 {
        self.max_message_size
    }
    pub fn set_max_message_size(&mut self, value: i32) {
        self.max_message_size = value;
    }
    pub fn auto_batch(&self) -> bool {
        self.auto_batch
    }
    pub fn set_auto_batch(&mut self, value: bool) {
        self.auto_batch = value;
    }
    pub fn batch_max_delay_millis(&self) -> Option<i32> {
        self.batch_max_delay_millis
    }
    pub fn set_batch_max_delay_millis(&mut self, value: i32) {
        self.batch_max_delay_millis = Some(value);
    }
    pub fn batch_max_bytes(&self) -> Option<i64> {
        self.batch_max_bytes
    }
    pub fn set_batch_max_bytes(&mut self, value: i64) {
        self.batch_max_bytes = Some(value);
    }
    pub fn total_batch_max_bytes(&self) -> Option<i64> {
        self.total_batch_max_bytes
    }
    pub fn set_total_batch_max_bytes(&mut self, value: i64) {
        self.total_batch_max_bytes = Some(value);
    }
    pub fn enable_backpressure_for_async_mode(&self) -> bool {
        self.enable_backpressure_for_async_mode
    }
    pub fn set_enable_backpressure_for_async_mode(&mut self, value: bool) {
        self.enable_backpressure_for_async_mode = value;
    }
    pub fn backpressure_async_send_num(&self) -> i32 {
        self.backpressure_async_send_num
    }
    pub fn set_backpressure_async_send_num(&mut self, value: i32) {
        self.backpressure_async_send_num = value.max(10);
    }
    pub fn backpressure_async_send_size(&self) -> i32 {
        self.backpressure_async_send_size
    }
    pub fn set_backpressure_async_send_size(&mut self, value: i32) {
        self.backpressure_async_send_size = value.max(1024 * 1024);
    }
    pub fn latency_max_millis(&self) -> &[i64] {
        &self.latency_max_millis
    }
    pub fn set_latency_max_millis(&mut self, value: Vec<i64>) {
        self.latency_max_millis = value;
    }
    pub fn not_available_duration_millis(&self) -> &[i64] {
        &self.not_available_duration_millis
    }
    pub fn set_not_available_duration_millis(&mut self, value: Vec<i64>) {
        self.not_available_duration_millis = value;
    }
    pub fn topics(&self) -> Option<&[String]> {
        self.topics.as_deref()
    }
    pub fn set_topics(&mut self, value: Option<Vec<String>>) {
        self.topics = value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_defaults_match_pinned_java_551() {
        let config = ProducerConfig::new(DEFAULT_PRODUCER_GROUP).unwrap();
        assert_eq!(config.create_topic_key(), "TBW102");
        assert_eq!(config.default_topic_queue_nums(), 4);
        assert_eq!(config.send_timeout_millis(), 3_000);
        assert_eq!(config.send_msg_max_timeout_per_request(), -1);
        assert_eq!(config.compress_threshold(), 4_096);
        assert_eq!(config.compress_type(), CompressionType::Zlib);
        assert_eq!(config.compress_level(), 5);
        assert_eq!(config.retry_times_when_send_failed(), 2);
        assert_eq!(config.retry_times_when_send_async_failed(), 2);
        assert_eq!(
            config.retry_response_codes(),
            &[17, 14, 1, 2, 16, 204, 205, 1500].into()
        );
        assert!(!config.retry_another_broker_when_not_store_ok());
        assert_eq!(config.max_message_size(), 4 * 1024 * 1024);
        assert!(!config.auto_batch());
        assert_eq!(config.batch_max_delay_millis(), None);
        assert_eq!(config.batch_max_bytes(), None);
        assert_eq!(config.total_batch_max_bytes(), None);
        assert!(!config.enable_backpressure_for_async_mode());
        assert_eq!(
            config.backpressure_async_send_num(),
            DEFAULT_BACKPRESSURE_ASYNC_SEND_NUM
        );
        assert_eq!(
            config.backpressure_async_send_size(),
            DEFAULT_BACKPRESSURE_ASYNC_SEND_SIZE
        );
        assert_eq!(config.topics(), None);
        assert_eq!(config.latency_max_millis(), &DEFAULT_LATENCY_MAX_MILLIS);
        assert_eq!(
            config.not_available_duration_millis(),
            &DEFAULT_NOT_AVAILABLE_DURATION_MILLIS
        );
        assert!(!config.send_message_with_vip_channel());
        assert!(!config.send_latency_fault_enable());
        assert!(!config.start_detector_enable());
        assert_eq!(config.detect_timeout_millis(), 200);
        assert_eq!(config.detect_interval_millis(), 2_000);
        assert_eq!(
            config.accumulator_settings().unwrap().max_delay_millis,
            DEFAULT_BATCH_MAX_DELAY_MILLIS
        );
        assert_eq!(
            config.accumulator_settings().unwrap().max_bytes,
            DEFAULT_BATCH_MAX_BYTES as usize
        );
        assert_eq!(
            config.accumulator_settings().unwrap().total_max_bytes,
            DEFAULT_TOTAL_BATCH_MAX_BYTES as usize
        );
    }

    #[test]
    fn explicit_tls_config_enables_client_transport() {
        let mut config = ProducerConfig::new("tls-producer").unwrap();
        assert!(!config.client_config().use_tls());
        let tls = TlsConfig {
            test_mode: false,
            ..TlsConfig::default()
        };
        config.set_tls_config(Some(tls));
        assert!(config.client_config().use_tls());
        assert!(!config.tls_config().unwrap().test_mode);
    }

    #[test]
    fn remaining_producer_settings_preserve_java_values_and_clamps() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let defaults = &fixture["defaults"];
        let mut config = ProducerConfig::new("producer").unwrap();
        assert_eq!(
            config.auto_batch(),
            defaults["autoBatch"].as_bool().unwrap()
        );
        assert_eq!(config.batch_max_delay_millis(), None);
        assert_eq!(config.batch_max_bytes(), None);
        assert_eq!(config.total_batch_max_bytes(), None);
        assert_eq!(
            defaults["batchMaxDelayMs"].as_i64(),
            Some(0),
            "Java reports no accumulator's delay as zero"
        );
        assert_eq!(
            defaults["batchMaxBytes"].as_i64(),
            Some(0),
            "Java reports no accumulator's byte limit as zero"
        );
        assert_eq!(defaults["totalBatchMaxBytes"].as_i64(), Some(0));
        let accumulator_defaults = &fixture["accumulatorDefaults"];
        let effective = config.accumulator_settings().unwrap();
        assert_eq!(
            effective.max_delay_millis,
            accumulator_defaults["batchMaxDelayMs"].as_i64().unwrap() as i32
        );
        assert_eq!(
            effective.max_bytes,
            accumulator_defaults["batchMaxBytes"].as_i64().unwrap() as usize
        );
        assert_eq!(
            effective.total_max_bytes,
            accumulator_defaults["totalBatchHoldBytes"]
                .as_i64()
                .unwrap() as usize
        );
        assert!(
            !fixture["autoBatchLifecycle"]["enabledBeforeAccumulator"]
                .as_bool()
                .unwrap()
        );
        assert!(
            fixture["autoBatchLifecycle"]["enabledAfterAccumulator"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            config.enable_backpressure_for_async_mode(),
            defaults["enableBackpressureForAsyncMode"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            config.backpressure_async_send_num(),
            defaults["backPressureForAsyncSendNum"].as_i64().unwrap() as i32
        );
        assert_eq!(
            config.backpressure_async_send_size(),
            defaults["backPressureForAsyncSendSize"].as_i64().unwrap() as i32
        );

        config.set_auto_batch(true);
        config.set_batch_max_delay_millis(25);
        config.set_batch_max_bytes(65_536);
        config.set_total_batch_max_bytes(1_048_576);
        config.set_enable_backpressure_for_async_mode(true);
        config.set_backpressure_async_send_num(1);
        config.set_backpressure_async_send_size(1);
        config.set_topics(Some(vec!["orders".into()]));
        assert!(config.auto_batch());
        assert_eq!(config.batch_max_delay_millis(), Some(25));
        assert_eq!(config.batch_max_bytes(), Some(65_536));
        assert_eq!(config.total_batch_max_bytes(), Some(1_048_576));
        assert_eq!(
            config.accumulator_settings().unwrap().total_max_bytes,
            1_048_576
        );
        assert_eq!(
            fixture["batchSettings"]["configuredTotalBatchHoldBytes"]
                .as_i64()
                .unwrap(),
            1_048_576
        );
        assert_eq!(
            fixture["batchSettings"]["configuredTotalBatchMaxBytes"].as_i64(),
            fixture["batchSettings"]["configuredBatchMaxBytes"].as_i64(),
            "Java's getTotalBatchMaxBytes reports the per-batch hold size; its same-named setter configures the accumulator-wide hold cap"
        );
        assert!(config.enable_backpressure_for_async_mode());
        assert_eq!(
            config.backpressure_async_send_num(),
            fixture["backpressureSettings"]["clampedSendNum"]
                .as_i64()
                .unwrap() as i32
        );
        assert_eq!(
            config.backpressure_async_send_size(),
            fixture["backpressureSettings"]["clampedSendSize"]
                .as_i64()
                .unwrap() as i32
        );
        assert_eq!(
            config.topics().map(<[String]>::to_vec),
            Some(vec!["orders".to_owned()])
        );

        config.set_send_message_with_vip_channel(true);
        config.set_send_latency_fault_enable(true);
        config.set_start_detector_enable(true);
        config.set_detect_timeout_millis(321);
        config.set_detect_interval_millis(4_321);
        config.set_latency_max_millis(vec![25, 250]);
        config.set_not_available_duration_millis(vec![0, 1_000]);
        assert!(config.send_message_with_vip_channel());
        assert!(config.send_latency_fault_enable());
        assert!(config.start_detector_enable());
        assert_eq!(config.detect_timeout_millis(), 321);
        assert_eq!(config.detect_interval_millis(), 4_321);
        assert_eq!(config.latency_max_millis(), [25, 250]);
        assert_eq!(config.not_available_duration_millis(), [0, 1_000]);
    }

    #[test]
    fn accumulator_setting_validation_matches_java_ranges_and_sentinels() {
        let mut config = ProducerConfig::new("producer").unwrap();
        config.set_batch_max_delay_millis(-1);
        config.set_batch_max_bytes(-1);
        config.set_total_batch_max_bytes(-1);
        assert_eq!(
            config.accumulator_settings().unwrap().max_delay_millis,
            DEFAULT_BATCH_MAX_DELAY_MILLIS
        );

        config.set_batch_max_delay_millis(0);
        assert!(matches!(
            config.accumulator_settings(),
            Err(ProducerError::InvalidBatchMaxDelay(0))
        ));
        config.set_batch_max_delay_millis(30_000);
        assert!(config.accumulator_settings().is_ok());
        config.set_batch_max_delay_millis(30_001);
        assert!(matches!(
            config.accumulator_settings(),
            Err(ProducerError::InvalidBatchMaxDelay(30_001))
        ));

        config.set_batch_max_delay_millis(10);
        config.set_batch_max_bytes(2 * 1024 * 1024);
        assert!(config.accumulator_settings().is_ok());
        config.set_batch_max_bytes(2 * 1024 * 1024 + 1);
        assert!(matches!(
            config.accumulator_settings(),
            Err(ProducerError::InvalidBatchMaxBytes(_))
        ));
        config.set_batch_max_bytes(32 * 1024);
        config.set_total_batch_max_bytes(0);
        assert!(matches!(
            config.accumulator_settings(),
            Err(ProducerError::InvalidTotalBatchMaxBytes(0))
        ));
    }

    #[test]
    fn compression_system_properties_match_java_defaults_and_parsing() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/producer/core.json")).unwrap();
        let java = &fixture["compressionProperties"];
        let mut empty = |_key: &str| None;
        assert_eq!(
            ProducerConfig::compression_settings(&mut empty).unwrap(),
            (CompressionType::Zlib, 5)
        );
        let mut properties = |key: &str| match key {
            MESSAGE_COMPRESS_TYPE_PROPERTY => java["inputType"].as_str().map(str::to_owned),
            MESSAGE_COMPRESS_LEVEL_PROPERTY => java["inputLevel"].as_str().map(str::to_owned),
            _ => None,
        };
        assert_eq!(
            ProducerConfig::compression_settings(&mut properties).unwrap(),
            (
                CompressionType::of(java["parsedType"].as_str()).unwrap(),
                java["parsedLevel"].as_i64().unwrap() as i32
            )
        );
        let mut invalid =
            |key: &str| (key == MESSAGE_COMPRESS_TYPE_PROPERTY).then(|| "invalid".into());
        assert_eq!(
            ProducerConfig::compression_settings(&mut invalid).is_ok(),
            java["invalidTypeAccepted"].as_bool().unwrap()
        );
        let mut invalid_level =
            |key: &str| (key == MESSAGE_COMPRESS_LEVEL_PROPERTY).then(|| "invalid".into());
        assert_eq!(
            ProducerConfig::compression_settings(&mut invalid_level).is_ok(),
            java["invalidLevelAccepted"].as_bool().unwrap()
        );
    }
}
