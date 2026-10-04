pub mod route;

use crate::error::RpcError;
use rocketmq_classic_protocol::codes::response;
use rocketmq_classic_protocol::header::{CommandHeader, HeaderMap};
use rocketmq_classic_remoting::RemotingCommand;
use rocketmq_classic_remoting::client::RemotingClient;
use serde::de::DeserializeOwned;
use tokio::time::Instant;

#[derive(Clone)]
pub struct MQClientApi {
    remoting: RemotingClient,
}

impl MQClientApi {
    pub fn new(remoting: RemotingClient) -> Self {
        Self { remoting }
    }

    pub async fn request<H: CommandHeader>(
        &self,
        addr: &str,
        code: i32,
        header: &H,
        body: Option<Vec<u8>>,
        deadline: Instant,
    ) -> Result<RemotingCommand, RpcError> {
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        let reply = self
            .remoting
            .invoke(
                addr,
                RemotingCommand {
                    code,
                    ext_fields: Some(fields),
                    body,
                    ..RemotingCommand::default()
                },
                deadline,
            )
            .await?;
        Ok(reply)
    }

    pub async fn oneway<H: CommandHeader>(
        &self,
        addr: &str,
        code: i32,
        header: &H,
        body: Option<Vec<u8>>,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        self.remoting
            .invoke_oneway(
                addr,
                RemotingCommand {
                    code,
                    ext_fields: Some(fields),
                    body,
                    ..RemotingCommand::default()
                },
                deadline,
            )?
            .await?;
        Ok(())
    }
}

pub fn classify(
    reply: RemotingCommand,
    code: i32,
    peer: &str,
) -> Result<RemotingCommand, RpcError> {
    if reply.code != response::SUCCESS {
        return Err(RpcError::Response {
            request_code: code,
            peer: peer.to_owned(),
            code: reply.code,
            remark: reply.remark,
        });
    }
    Ok(reply)
}

pub fn decode_header<T: CommandHeader>(
    reply: &RemotingCommand,
    code: i32,
    peer: &str,
) -> Result<T, RpcError> {
    T::decode_fields(
        reply
            .ext_fields
            .as_ref()
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: peer.to_owned(),
                reason: "missing response header".into(),
            })?,
    )
    .map_err(|error| RpcError::MalformedResponse {
        request_code: code,
        peer: peer.to_owned(),
        reason: error.to_string(),
    })
}

pub fn decode_body<T: DeserializeOwned>(
    reply: &RemotingCommand,
    code: i32,
    peer: &str,
) -> Result<T, RpcError> {
    let body = reply
        .body
        .as_ref()
        .ok_or_else(|| RpcError::MalformedResponse {
            request_code: code,
            peer: peer.to_owned(),
            reason: "missing response body".into(),
        })?;
    serde_json::from_slice(body).map_err(|error| RpcError::MalformedResponse {
        request_code: code,
        peer: peer.to_owned(),
        reason: error.to_string(),
    })
}
