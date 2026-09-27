use std::{error::Error, fmt, sync::Arc, time::Duration};

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

/// A cloneable, typed error source retained by transport errors and callback results.
///
/// Equality compares the displayed source chain so transport errors retain value-comparison
/// behavior without requiring arbitrary wrapped error types to implement `Eq`.
#[derive(Clone)]
pub struct ErrorSource(Arc<dyn Error + Send + Sync + 'static>);

impl ErrorSource {
    pub fn new(error: impl Error + Send + Sync + 'static) -> Self {
        Self(Arc::new(error))
    }

    pub fn with_context(
        message: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::new(ContextError {
            message: message.into(),
            source: Box::new(source),
        })
    }

    pub fn message(message: impl Into<String>) -> Self {
        Self::new(MessageError(message.into()))
    }

    pub fn downcast_ref<T: Error + 'static>(&self) -> Option<&T> {
        let mut current: &(dyn Error + 'static) = self.0.as_ref();
        loop {
            if let Some(error) = current.downcast_ref() {
                return Some(error);
            }
            current = current.source()?;
        }
    }
}

impl fmt::Debug for ErrorSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ErrorSource")
            .field(&self.to_string())
            .finish()
    }
}

impl fmt::Display for ErrorSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Error for ErrorSource {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl PartialEq for ErrorSource {
    fn eq(&self, other: &Self) -> bool {
        fn chain(error: &(dyn Error + 'static)) -> Vec<String> {
            let mut result = Vec::new();
            let mut current = Some(error);
            while let Some(error) = current {
                result.push(error.to_string());
                current = error.source();
            }
            result
        }
        chain(self.0.as_ref()) == chain(other.0.as_ref())
    }
}

impl Eq for ErrorSource {}

impl From<String> for ErrorSource {
    fn from(message: String) -> Self {
        Self::message(message)
    }
}

impl From<&str> for ErrorSource {
    fn from(message: &str) -> Self {
        Self::message(message)
    }
}

#[derive(Debug)]
struct MessageError(String);

impl fmt::Display for MessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for MessageError {}

#[derive(Debug)]
struct ContextError {
    message: String,
    source: Box<dyn Error + Send + Sync>,
}

impl fmt::Display for ContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.message, self.source)
    }
}

impl Error for ContextError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoAwayCause {
    pub connection_generation: u64,
    pub remote_addr: String,
    pub retried: bool,
}

impl fmt::Display for GoAwayCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.retried {
            write!(
                formatter,
                "Receive GO_AWAY twice in request from channelId={} (generation={})",
                self.remote_addr, self.connection_generation
            )
        } else {
            write!(
                formatter,
                "Receive GO_AWAY from channelId={} (generation={})",
                self.remote_addr, self.connection_generation
            )
        }
    }
}

impl Error for GoAwayCause {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TimeoutPhase {
    #[error("operation")]
    Operation,
    #[error("connection acquisition")]
    Connection,
    #[error("request admission")]
    Admission,
    #[error("socket write")]
    Write,
    #[error("response wait")]
    Response,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransportError {
    #[error("connection failed: {0}")]
    Connect(String),
    #[error("connection failed: {0}")]
    ConnectSource(#[source] ErrorSource),
    #[error("connect to {remote_addr} failed: {cause:?}")]
    ConnectRequest {
        remote_addr: String,
        #[source]
        cause: Option<ErrorSource>,
    },
    #[error("request send failed: {0}")]
    Send(String),
    #[error("request send failed: {0}")]
    SendSource(#[source] ErrorSource),
    #[error("operation deadline expired")]
    Timeout,
    #[error("{phase} deadline expired for {remote_addr:?} after {timeout:?}")]
    TimeoutWithContext {
        phase: TimeoutPhase,
        remote_addr: Option<String>,
        timeout: Option<Duration>,
    },
    #[error("invalid client configuration: {0}")]
    InvalidConfiguration(String),
    #[error("request admission limit reached")]
    TooManyRequests,
    #[error("request send failed for {remote_addr}: {cause:?}")]
    SendRequest {
        remote_addr: String,
        #[source]
        cause: Option<ErrorSource>,
    },
    #[error("connection is closed")]
    Closed,
    #[error("request was interrupted because its channel was closed")]
    Interrupted,
    #[error("RPC hook failed: {0}")]
    RpcHook(String),
    #[error("TLS setup or handshake failed: {0}")]
    Tls(String),
    #[error("TLS setup or handshake failed: {0}")]
    TlsSource(#[source] ErrorSource),
    #[error("malformed frame: {0}")]
    MalformedFrame(CodecError),
    #[error("codec: {0}")]
    Codec(CodecError),
    #[error("client has not been started")]
    NotStarted,
    #[error("client has shut down")]
    Shutdown,
    #[error("opaque identifiers are exhausted")]
    OpaqueExhausted,
}

impl TransportError {
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout | Self::TimeoutWithContext { .. })
    }

    pub fn timeout(
        phase: TimeoutPhase,
        remote_addr: Option<String>,
        timeout: Option<Duration>,
    ) -> Self {
        Self::TimeoutWithContext {
            phase,
            remote_addr,
            timeout,
        }
    }

    pub fn connect_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::ConnectSource(ErrorSource::new(error))
    }

    pub fn send_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::SendSource(ErrorSource::new(error))
    }

    pub fn tls_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::TlsSource(ErrorSource::new(error))
    }
}

impl From<CodecError> for TransportError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_source_preserves_typed_cause_and_context_chain() {
        let source = ErrorSource::with_context(
            "dial context",
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused"),
        );
        assert_eq!(source.to_string(), "dial context: refused");
        assert_eq!(
            source
                .downcast_ref::<std::io::Error>()
                .map(std::io::Error::kind),
            Some(std::io::ErrorKind::ConnectionRefused)
        );
    }

    #[test]
    fn go_away_cause_retains_address_generation_and_retry_state() {
        let cause = ErrorSource::new(GoAwayCause {
            connection_generation: 17,
            remote_addr: "127.0.0.1:10911".into(),
            retried: true,
        });
        let go_away = cause.downcast_ref::<GoAwayCause>().unwrap();
        assert_eq!(go_away.connection_generation, 17);
        assert_eq!(go_away.remote_addr, "127.0.0.1:10911");
        assert!(go_away.retried);
        assert!(
            cause
                .to_string()
                .contains("channelId=127.0.0.1:10911 (generation=17)")
        );
    }
}
