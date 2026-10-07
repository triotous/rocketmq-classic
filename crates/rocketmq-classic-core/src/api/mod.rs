pub mod consumer;
pub mod extensions;
pub mod producer;
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
    language: Option<rocketmq_classic_protocol::codes::LanguageCode>,
}

impl MQClientApi {
    pub fn new(remoting: RemotingClient) -> Self {
        Self::with_language(
            remoting,
            Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
        )
    }

    pub(crate) fn with_language(
        remoting: RemotingClient,
        language: Option<rocketmq_classic_protocol::codes::LanguageCode>,
    ) -> Self {
        Self { remoting, language }
    }

    async fn request<H: CommandHeader>(
        &self,
        addr: &str,
        code: i32,
        header: &H,
        body: Option<Vec<u8>>,
        deadline: Instant,
    ) -> Result<RemotingCommand, RpcError> {
        self.request_with_mode(addr, code, header, body, deadline, true)
            .await
    }

    async fn request_future<H: CommandHeader>(
        &self,
        addr: &str,
        code: i32,
        header: &H,
        body: Option<Vec<u8>>,
        deadline: Instant,
    ) -> Result<RemotingCommand, RpcError> {
        self.request_with_mode(addr, code, header, body, deadline, false)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn request_with_mode<H: CommandHeader>(
        &self,
        addr: &str,
        code: i32,
        header: &H,
        body: Option<Vec<u8>>,
        deadline: Instant,
        sync: bool,
    ) -> Result<RemotingCommand, RpcError> {
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        let command = RemotingCommand {
            code,
            // Pinned Java sets clientConfig.language only on heartbeat V1/V2.
            language: if code == rocketmq_classic_protocol::codes::request::HEART_BEAT {
                self.language
            } else {
                Some(rocketmq_classic_protocol::codes::LanguageCode::Java)
            },
            ext_fields: Some(fields),
            body,
            ..Default::default()
        };
        Ok(if sync {
            self.remoting
                .invoke_sync_with_deadline(addr, command, deadline)
                .await?
        } else {
            self.remoting.invoke(addr, command, deadline).await?
        })
    }

    async fn oneway<H: CommandHeader>(
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
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
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

fn classify(reply: RemotingCommand, code: i32, peer: &str) -> Result<RemotingCommand, RpcError> {
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

fn decode_header<T: CommandHeader>(
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

fn decode_body<T: DeserializeOwned>(
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
