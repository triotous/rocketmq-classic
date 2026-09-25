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
