use rocketmq_classic_protocol::header::ProtocolError;
use rocketmq_classic_remoting::TransportError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("transport failure: {0}")]
    Transport(#[from] TransportError),
    #[error("invalid RPC header: {0}")]
    Header(#[from] ProtocolError),
    #[error("unsupported RPC feature: {0}")]
    Unsupported(&'static str),
    #[error("invalid response for request {request_code} from {peer}: {reason}")]
    MalformedResponse {
        request_code: i32,
        peer: String,
        reason: String,
    },
    #[error("request {request_code} rejected by {peer} with code {code}: {remark:?}")]
    Response {
        request_code: i32,
        peer: String,
        code: i32,
        remark: Option<String>,
    },
}
