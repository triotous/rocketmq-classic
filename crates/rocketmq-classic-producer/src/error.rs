use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProducerError {
    #[error("producer group is blank")]
    BlankGroup,
    #[error("producer group exceeds Java's 120-character limit")]
    GroupTooLong,
    #[error("producer group contains illegal characters")]
    IllegalGroup,
    #[error("message is null")]
    NullMessage,
    #[error("invalid compression level: {0}")]
    InvalidCompressionLevel(String),
    #[error("{0}")]
    CompressionType(#[from] rocketmq_classic_common::sysflag::message::CompressionTypeError),
    #[error("message topic is blank")]
    BlankTopic,
    #[error("message topic exceeds Java's topic length limit")]
    TopicTooLong,
    #[error("message topic contains illegal characters")]
    IllegalTopic,
    #[error("message topic conflicts with a system topic")]
    SystemTopic,
    #[error("topic is not supported")]
    UnsupportedRecallTopic,
    #[error("recall handle is invalid")]
    InvalidRecallHandle,
    #[error("The broker service address not found")]
    RecallBrokerAddressNotFound,
    #[error("sending to this system topic is forbidden")]
    ForbiddenTopic,
    #[error("message body is empty")]
    EmptyBody,
    #[error("message body size {actual} exceeds configured maximum {maximum}")]
    BodyTooLarge { actual: usize, maximum: i32 },
    #[error("INNER_MULTI_DISPATCH contains the platform path separator")]
    InvalidMultiDispatch,
    #[error("producer is not started")]
    NotStarted,
    #[error("producer has already been started")]
    AlreadyStarted,
    #[error("producer is already shut down")]
    Shutdown,
    #[error("no writable route for topic {0}")]
    NoRoute(String),
    #[error("selected queue does not belong to the topic writable queues")]
    QueueNotInRoute,
    #[error("queue selector did not choose a writable queue")]
    NoQueueSelected,
    #[error("send deadline expired")]
    Timeout,
    #[error(
        "send request message to <{topic}> OK, but wait reply message timeout, {timeout_millis} ms."
    )]
    RequestReplyTimeout { topic: String, timeout_millis: u128 },
    #[error("send request message to <{topic}> fail: {cause}")]
    RequestSendFailed { topic: String, cause: String },
    #[error("producer shut down before the request received a reply")]
    RequestReplyShutdown,
    #[error("request/reply waiter was cancelled")]
    RequestReplyCancelled,
    #[error("request timeout exceeds Java's signed long millisecond range")]
    InvalidRequestTimeout,
    #[error("TransactionListener is null")]
    TransactionListenerMissing,
    #[error("Transactional messages do not support delayed delivery")]
    DelayedTransactionalMessage,
    #[error("transaction check pool settings are invalid: min={min}, max={max}, hold={hold}")]
    InvalidTransactionPoolSettings { min: i32, max: i32, hold: i32 },
    #[error("transaction check queue is full or shut down")]
    TransactionCheckQueueUnavailable,
    #[error("transaction message ID is malformed: {0}")]
    TransactionMetadata(String),
    #[error("send message Exception: {0}")]
    TransactionSend(Box<ProducerError>),
    #[error("configured synchronous retry count results in no send attempts")]
    NoSendAttempt,
    #[error("retry metadata is not a valid Java integer")]
    InvalidRetryMetadata,
    #[error("batch must contain at least one message")]
    EmptyBatch,
    #[error("messages in a batch must have the same topic")]
    BatchTopicMismatch,
    #[error("messages in a batch must have the same waitStoreMsgOK setting")]
    BatchWaitStoreMismatch,
    #[error("delayed messages are not supported in batches")]
    DelayedBatchMessage,
    #[error("batch message contains an invalid delay value")]
    InvalidBatchDelay,
    #[error("retry-group messages are not supported in batches")]
    RetryBatchMessage,
    #[error("batch message property encoding exceeds Java's signed-short limit")]
    BatchPropertiesTooLarge,
    #[error("encoded batch size {actual} exceeds configured maximum {maximum}")]
    BatchTooLarge { actual: usize, maximum: i32 },
    #[error("batchMaxDelayMs must be between 1ms and 30s, got {0}")]
    InvalidBatchMaxDelay(i32),
    #[error("batchMaxBytes must be between 1B and 2MB, got {0}")]
    InvalidBatchMaxBytes(i64),
    #[error("totalBatchMaxBytes must be greater than zero, got {0}")]
    InvalidTotalBatchMaxBytes(i64),
    #[error("auto-batch send failed: {0}")]
    AutoBatchSend(String),
    #[error("Java returned a batch result that cannot be split by message")]
    InvalidAutoBatchResult,
    #[error("{0}")]
    Runtime(#[from] rocketmq_classic_core::runtime::RuntimeError),
    #[error("{0}")]
    Rpc(#[from] rocketmq_classic_core::error::RpcError),
    #[error("{0}")]
    Config(#[from] rocketmq_classic_core::config::ConfigError),
}
