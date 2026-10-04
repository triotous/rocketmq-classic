use rocketmq_classic_protocol::{
    codes::{request, response},
    header::{CommandHeader, GetRouteInfoRequestHeader, HeaderMap},
    route::TopicRouteData,
};
use rocketmq_classic_remoting::RemotingCommand;
use tokio::time::Instant;

use super::MQClientApi;
use crate::error::RpcError;

impl MQClientApi {
    pub async fn get_topic_route(
        &self,
        topic: &str,
        deadline: Instant,
    ) -> Result<TopicRouteData, RpcError> {
        let mut fields = HeaderMap::new();
        GetRouteInfoRequestHeader {
            topic: topic.to_owned(),
            accept_standard_json_only: None,
        }
        .encode_fields(&mut fields);
        let (reply, peer) = self
            .remoting
            .invoke_name_server_with_peer(
                RemotingCommand {
                    code: request::GET_ROUTEINFO_BY_TOPIC,
                    ext_fields: Some(fields),
                    ..RemotingCommand::default()
                },
                deadline,
            )
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: request::GET_ROUTEINFO_BY_TOPIC,
                peer,
                code: reply.code,
                remark: reply.remark,
            });
        }
        let body = reply.body.ok_or_else(|| RpcError::MalformedResponse {
            request_code: request::GET_ROUTEINFO_BY_TOPIC,
            peer: peer.clone(),
            reason: "missing route body".into(),
        })?;
        serde_json::from_slice(&body).map_err(|error| RpcError::MalformedResponse {
            request_code: request::GET_ROUTEINFO_BY_TOPIC,
            peer,
            reason: error.to_string(),
        })
    }
}
