use rocketmq_classic_core::{config::ConfigError, error::RpcError, runtime::RuntimeError};

use crate::{offset::OffsetStoreError, pull::ProcessPullError};

#[derive(Debug, thiserror::Error)]
pub enum PullConsumerError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error(transparent)]
    OffsetStore(#[from] OffsetStoreError),
    #[error(transparent)]
    PullProcessing(#[from] ProcessPullError),
    #[error("consumer group is blank or contains characters not accepted by RocketMQ")]
    InvalidConsumerGroup,
    #[error("consumer group must not be DEFAULT_CONSUMER")]
    DefaultConsumerGroup,
    #[error(
        "consumer timeout while suspended ({consumer_timeout_millis} ms) must be at least the broker suspend timeout ({broker_suspend_millis} ms)"
    )]
    InvalidSuspendTimeout {
        consumer_timeout_millis: i64,
        broker_suspend_millis: i64,
    },
    #[error("pull consumer has not been started")]
    NotStarted,
    #[error("pull consumer has already been started")]
    AlreadyStarted,
    #[error("pull consumer has been shut down")]
    Shutdown,
    #[error("offset store must be configured before startup")]
    OffsetStoreAfterStartup,
    #[error("message queue must have a topic and broker name")]
    InvalidQueue,
    #[error("pull offset must be nonnegative")]
    InvalidOffset,
    #[error("max message count must be positive")]
    InvalidMaxMessageCount,
    #[error("max message bytes must be positive")]
    InvalidMaxMessageBytes,
    #[error("pull timeout must be nonnegative and representable")]
    InvalidTimeout,
    #[error("SQL92 pull expression must not be empty")]
    EmptySqlExpression,
    #[error("broker `{broker}` was not found in the current route")]
    BrokerNotFound { broker: String },
    #[error(
        "broker `{broker}` version {version} does not support message filtering by {expression_type}"
    )]
    UnsupportedExpressionBroker {
        broker: String,
        version: i32,
        expression_type: String,
    },
    #[error(
        "class-filter pull has no filter server for broker address `{broker_address}` and topic `{topic}`"
    )]
    FilterServerNotFound {
        broker_address: String,
        topic: String,
    },
}
