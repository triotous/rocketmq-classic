use rocketmq_classic_core::config::ClientConfig;
use rocketmq_classic_protocol::heartbeat::{ConsumeFromWhere, MessageModel};
use rocketmq_classic_remoting::client::TlsConfig;

use crate::error::PullConsumerError;

pub const DEFAULT_CONSUMER_GROUP: &str = "DEFAULT_CONSUMER";
pub const DEFAULT_BROKER_SUSPEND_MAX_TIME_MILLIS: i64 = 20_000;
pub const DEFAULT_CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND: i64 = 30_000;
pub const DEFAULT_CONSUMER_PULL_TIMEOUT_MILLIS: i64 = 10_000;
pub const DEFAULT_PULL_MAX_RECONSUME_TIMES: i32 = 16;
pub const DEFAULT_PULL_CONSUMER_ENABLE_REBALANCE: bool = true;

/// Settings implemented by Java 5.5.1 `DefaultMQPullConsumer` for explicit pull.
pub struct PullConsumerConfig {
    client: ClientConfig,
    tls_config: Option<TlsConfig>,
    consumer_group: String,
    broker_suspend_max_time_millis: i64,
    consumer_timeout_millis_when_suspend: i64,
    consumer_pull_timeout_millis: i64,
    max_reconsume_times: i32,
    message_model: MessageModel,
    consume_from_where: ConsumeFromWhere,
    unit_mode: bool,
    enable_rebalance: bool,
    allocate_message_queue_strategy:
        std::sync::Arc<dyn crate::rebalance::AllocateMessageQueueStrategy>,
    message_queue_listener: Option<crate::push::MessageQueueListener>,
}

impl PullConsumerConfig {
    pub fn new(consumer_group: impl Into<String>) -> Result<Self, PullConsumerError> {
        Ok(Self {
            client: ClientConfig::new_auto()?,
            tls_config: None,
            consumer_group: consumer_group.into(),
            broker_suspend_max_time_millis: DEFAULT_BROKER_SUSPEND_MAX_TIME_MILLIS,
            consumer_timeout_millis_when_suspend: DEFAULT_CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
            consumer_pull_timeout_millis: DEFAULT_CONSUMER_PULL_TIMEOUT_MILLIS,
            max_reconsume_times: DEFAULT_PULL_MAX_RECONSUME_TIMES,
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            unit_mode: false,
            enable_rebalance: DEFAULT_PULL_CONSUMER_ENABLE_REBALANCE,
            allocate_message_queue_strategy: std::sync::Arc::new(
                crate::rebalance::AllocateMessageQueueAveragely,
            ),
            message_queue_listener: None,
        })
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    pub fn client_config_mut(&mut self) -> &mut ClientConfig {
        &mut self.client
    }

    pub fn tls_config(&self) -> Option<&TlsConfig> {
        self.tls_config.as_ref()
    }

    pub fn set_tls_config(&mut self, value: Option<TlsConfig>) {
        if value.is_some() {
            self.client.set_use_tls(true);
        }
        self.tls_config = value;
    }

    pub fn consumer_group(&self) -> &str {
        &self.consumer_group
    }

    pub fn set_consumer_group(&mut self, value: impl Into<String>) {
        self.consumer_group = value.into();
    }

    pub const fn broker_suspend_max_time_millis(&self) -> i64 {
        self.broker_suspend_max_time_millis
    }

    pub fn set_broker_suspend_max_time_millis(&mut self, value: i64) {
        self.broker_suspend_max_time_millis = value;
    }

    pub const fn consumer_timeout_millis_when_suspend(&self) -> i64 {
        self.consumer_timeout_millis_when_suspend
    }

    pub fn set_consumer_timeout_millis_when_suspend(&mut self, value: i64) {
        self.consumer_timeout_millis_when_suspend = value;
    }

    pub const fn consumer_pull_timeout_millis(&self) -> i64 {
        self.consumer_pull_timeout_millis
    }

    pub fn set_consumer_pull_timeout_millis(&mut self, value: i64) {
        self.consumer_pull_timeout_millis = value;
    }

    pub const fn max_reconsume_times(&self) -> i32 {
        self.max_reconsume_times
    }

    pub fn set_max_reconsume_times(&mut self, value: i32) {
        self.max_reconsume_times = value;
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

    pub const fn unit_mode(&self) -> bool {
        self.unit_mode
    }

    pub fn set_unit_mode(&mut self, value: bool) {
        self.unit_mode = value;
    }

    pub const fn enable_rebalance(&self) -> bool {
        self.enable_rebalance
    }

    pub fn set_enable_rebalance(&mut self, value: bool) {
        self.enable_rebalance = value;
    }

    pub fn allocate_message_queue_strategy(
        &self,
    ) -> std::sync::Arc<dyn crate::rebalance::AllocateMessageQueueStrategy> {
        self.allocate_message_queue_strategy.clone()
    }

    pub fn set_allocate_message_queue_strategy(
        &mut self,
        value: std::sync::Arc<dyn crate::rebalance::AllocateMessageQueueStrategy>,
    ) {
        self.allocate_message_queue_strategy = value;
    }

    pub fn message_queue_listener(&self) -> Option<crate::push::MessageQueueListener> {
        self.message_queue_listener.clone()
    }

    pub fn set_message_queue_listener(&mut self, value: Option<crate::push::MessageQueueListener>) {
        self.message_queue_listener = value;
    }

    pub(crate) fn validate(&self, effective_consumer_group: &str) -> Result<(), PullConsumerError> {
        validate_group(effective_consumer_group)?;
        if effective_consumer_group == DEFAULT_CONSUMER_GROUP {
            return Err(PullConsumerError::DefaultConsumerGroup);
        }
        if self.consumer_timeout_millis_when_suspend < self.broker_suspend_max_time_millis {
            return Err(PullConsumerError::InvalidSuspendTimeout {
                consumer_timeout_millis: self.consumer_timeout_millis_when_suspend,
                broker_suspend_millis: self.broker_suspend_max_time_millis,
            });
        }
        Ok(())
    }
}

pub(crate) fn validate_group(group: &str) -> Result<(), PullConsumerError> {
    let blank = group.is_empty()
        || group.chars().all(|character| {
            (character.is_whitespace() || matches!(character, '\u{001c}'..='\u{001f}'))
                && !matches!(character, '\u{00a0}' | '\u{2007}' | '\u{202f}')
        });
    if blank
        || group.encode_utf16().count() > 120
        || group.chars().any(|character| {
            !matches!(character, '%' | '|' | '-' | '_') && !character.is_ascii_alphanumeric()
        })
    {
        return Err(PullConsumerError::InvalidConsumerGroup);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_tls_config_enables_client_transport() {
        let mut config = PullConsumerConfig::new("tls-consumer").unwrap();
        let tls = TlsConfig {
            test_mode: false,
            ..TlsConfig::default()
        };
        config.set_tls_config(Some(tls));
        assert!(config.client_config().use_tls());
        assert!(!config.tls_config().unwrap().test_mode);
    }

    #[test]
    fn pull_max_reconsume_times_matches_java_default_and_accepts_mutation() {
        let mut config = PullConsumerConfig::new("pull-reconsume-config").unwrap();
        assert_eq!(
            config.max_reconsume_times(),
            DEFAULT_PULL_MAX_RECONSUME_TIMES
        );
        config.set_max_reconsume_times(-3);
        assert_eq!(config.max_reconsume_times(), -3);
    }

    #[test]
    fn consumer_group_validation_matches_java_allowed_characters_and_utf16_limit() {
        assert!(validate_group("orders-01_%|group").is_ok());
        assert!(matches!(
            validate_group("group/invalid"),
            Err(PullConsumerError::InvalidConsumerGroup)
        ));
        assert!(matches!(
            validate_group(&"g".repeat(121)),
            Err(PullConsumerError::InvalidConsumerGroup)
        ));
        assert!(matches!(
            validate_group(&"界".repeat(61)),
            Err(PullConsumerError::InvalidConsumerGroup)
        ));
    }

    #[test]
    fn consumer_config_rejects_default_group_and_invalid_suspend_relationship() {
        let mut config = PullConsumerConfig::new("orders").unwrap();
        assert!(matches!(
            config.validate(DEFAULT_CONSUMER_GROUP),
            Err(PullConsumerError::DefaultConsumerGroup)
        ));

        config.set_broker_suspend_max_time_millis(30_001);
        assert!(matches!(
            config.validate("orders"),
            Err(PullConsumerError::InvalidSuspendTimeout { .. })
        ));
        config.set_consumer_timeout_millis_when_suspend(30_001);
        assert!(config.validate("orders").is_ok());
    }
}
