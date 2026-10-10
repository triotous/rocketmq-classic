use rocketmq_classic_protocol::{
    codes::{request, response},
    header::{
        CommandHeader, EndTransactionRequestHeader, HeaderMap, SendMessageRequestHeaderV1,
        SendMessageRequestHeaderV2, SendMessageResponseHeader,
    },
};
use rocketmq_classic_remoting::RemotingCommand;
use tokio::time::Instant;

use super::{MQClientApi, decode_header};
use crate::error::RpcError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendKind {
    V1,
    V2,
    Batch,
    ReplyV1,
    ReplyV2,
}

impl SendKind {
    fn code(self) -> i32 {
        match self {
            Self::V1 => request::SEND_MESSAGE,
            Self::V2 => request::SEND_MESSAGE_V2,
            Self::Batch => request::SEND_BATCH_MESSAGE,
            Self::ReplyV1 => request::SEND_REPLY_MESSAGE,
            Self::ReplyV2 => request::SEND_REPLY_MESSAGE_V2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendMode {
    Sync,
    Future,
    Oneway,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendStatus {
    SendOk,
    FlushDiskTimeout,
    FlushSlaveTimeout,
    SlaveNotAvailable,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SendStatusLookupError {
    #[error("Name is null")]
    NullName,
    #[error("No enum constant org.apache.rocketmq.client.producer.SendStatus.{0}")]
    UnknownName(String),
}

impl SendStatus {
    pub const fn values() -> [Self; 4] {
        [
            Self::SendOk,
            Self::FlushDiskTimeout,
            Self::FlushSlaveTimeout,
            Self::SlaveNotAvailable,
        ]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, SendStatusLookupError> {
        match name {
            None => Err(SendStatusLookupError::NullName),
            Some("SEND_OK") => Ok(Self::SendOk),
            Some("FLUSH_DISK_TIMEOUT") => Ok(Self::FlushDiskTimeout),
            Some("FLUSH_SLAVE_TIMEOUT") => Ok(Self::FlushSlaveTimeout),
            Some("SLAVE_NOT_AVAILABLE") => Ok(Self::SlaveNotAvailable),
            Some(other) => Err(SendStatusLookupError::UnknownName(other.to_owned())),
        }
    }

    pub const fn name(&self) -> &'static str {
        match self {
            Self::SendOk => "SEND_OK",
            Self::FlushDiskTimeout => "FLUSH_DISK_TIMEOUT",
            Self::FlushSlaveTimeout => "FLUSH_SLAVE_TIMEOUT",
            Self::SlaveNotAvailable => "SLAVE_NOT_AVAILABLE",
        }
    }

    pub const fn ordinal(&self) -> i32 {
        match self {
            Self::SendOk => 0,
            Self::FlushDiskTimeout => 1,
            Self::FlushSlaveTimeout => 2,
            Self::SlaveNotAvailable => 3,
        }
    }

    pub const fn java_compare_to(&self, other: &Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl std::fmt::Display for SendStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendResult {
    pub status: SendStatus,
    pub msg_id: String,
    pub offset_msg_id: String,
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub transaction_id: Option<String>,
    pub recall_handle: Option<String>,
    pub raw_resp_body: Option<Vec<u8>>,
    pub region_id: String,
    pub trace_on: bool,
}

impl MQClientApi {
    /// Performs exactly one Broker attempt; queue selection and retries belong to the producer.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_message(
        &self,
        addr: &str,
        broker_name: &str,
        header: &SendMessageRequestHeaderV1,
        body: Vec<u8>,
        uniq_msg_id: &str,
        kind: SendKind,
        mode: SendMode,
        deadline: Instant,
    ) -> Result<Option<SendResult>, RpcError> {
        let code = kind.code();
        if mode == SendMode::Oneway {
            match kind {
                SendKind::V1 | SendKind::ReplyV1 => {
                    self.oneway(addr, code, header, Some(body), deadline)
                        .await?
                }
                _ => {
                    self.oneway(
                        addr,
                        code,
                        &SendMessageRequestHeaderV2::from_v1(header),
                        Some(body),
                        deadline,
                    )
                    .await?
                }
            }
            return Ok(None);
        }
        let reply = match kind {
            SendKind::V1 | SendKind::ReplyV1 => {
                self.request_with_mode(
                    addr,
                    code,
                    header,
                    Some(body),
                    deadline,
                    mode == SendMode::Sync,
                )
                .await?
            }
            _ => {
                self.request_with_mode(
                    addr,
                    code,
                    &SendMessageRequestHeaderV2::from_v1(header),
                    Some(body),
                    deadline,
                    mode == SendMode::Sync,
                )
                .await?
            }
        };
        let status = match reply.code {
            response::SUCCESS => SendStatus::SendOk,
            response::FLUSH_DISK_TIMEOUT => SendStatus::FlushDiskTimeout,
            response::FLUSH_SLAVE_TIMEOUT => SendStatus::FlushSlaveTimeout,
            response::SLAVE_NOT_AVAILABLE => SendStatus::SlaveNotAvailable,
            code => {
                return Err(RpcError::Response {
                    request_code: kind.code(),
                    peer: addr.to_owned(),
                    code,
                    remark: reply.remark,
                });
            }
        };
        let result: SendMessageResponseHeader = decode_header(&reply, code, addr)?;
        let ext = reply
            .ext_fields
            .as_ref()
            .expect("decoded response header exists");
        Ok(Some(SendResult {
            status,
            msg_id: uniq_msg_id.to_owned(),
            offset_msg_id: result.msg_id,
            topic: header.topic.clone(),
            broker_name: broker_name.to_owned(),
            queue_id: result.queue_id,
            queue_offset: result.queue_offset,
            transaction_id: result.transaction_id,
            recall_handle: result.recall_handle,
            raw_resp_body: None,
            region_id: ext
                .get("MSG_REGION")
                .filter(|s| !s.is_empty())
                .cloned()
                .unwrap_or_else(|| "DefaultRegion".into()),
            trace_on: ext.get("TRACE_ON").is_none_or(|value| value != "false"),
        }))
    }

    /// Java's endTransactionOneway sends a remark but never waits for a Broker reply.
    pub async fn end_transaction_oneway(
        &self,
        addr: &str,
        header: &EndTransactionRequestHeader,
        remark: Option<String>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        self.remoting
            .invoke_oneway(
                addr,
                RemotingCommand {
                    code: request::END_TRANSACTION,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ext_fields: Some(fields),
                    remark,
                    ..RemotingCommand::default()
                },
                deadline,
            )?
            .await?;
        Ok(())
    }
}
