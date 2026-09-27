use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CodecError {
    #[error("frame length {0} is smaller than the required length prefix")]
    InvalidFrameLength(i32),
    #[error("frame length {length} exceeds configured limit {limit}")]
    FrameTooLarge { length: usize, limit: usize },
    #[error("header length {header} exceeds frame payload length {payload}")]
    HeaderTooLarge { header: usize, payload: usize },
    #[error("unsupported serialization type {0}")]
    UnsupportedSerialization(u8),
    #[error("unsupported language code {0}")]
    UnsupportedLanguage(u8),
    #[error("malformed {0} header: {1}")]
    MalformedHeader(&'static str, String),
    #[error("field `{0}` does not fit Java wire width")]
    FieldTooWide(&'static str),
    #[error("JSON header error: {0}")]
    Json(String),
    #[error("invalid UTF-8 in RocketMQ header")]
    InvalidUtf8,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransportError {
    #[error("connection failed: {0}")]
    Connect(String),
    #[error("connect to {remote_addr} failed: {cause:?}")]
    ConnectRequest {
        remote_addr: String,
        cause: Option<String>,
    },
    #[error("request send failed: {0}")]
    Send(String),
    #[error("operation deadline expired")]
    Timeout,
    #[error("invalid client configuration: {0}")]
    InvalidConfiguration(String),
    #[error("request admission limit reached")]
    TooManyRequests,
    #[error("request send failed for {remote_addr}: {cause:?}")]
    SendRequest {
        remote_addr: String,
        cause: Option<String>,
    },
    #[error("connection is closed")]
    Closed,
    #[error("request was interrupted because its channel was closed")]
    Interrupted,
    #[error("RPC hook failed: {0}")]
    RpcHook(String),
    #[error("TLS setup or handshake failed: {0}")]
    Tls(String),
    #[error("malformed frame: {0}")]
    MalformedFrame(CodecError),
    #[error("codec: {0}")]
    Codec(CodecError),
    #[error("inbound request processor is overloaded")]
    InboundOverloaded,
    #[error("client has not been started")]
    NotStarted,
    #[error("client has shut down")]
    Shutdown,
    #[error("opaque identifiers are exhausted")]
    OpaqueExhausted,
}

impl From<CodecError> for TransportError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}
