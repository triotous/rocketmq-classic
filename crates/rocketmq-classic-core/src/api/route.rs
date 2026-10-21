use rocketmq_classic_protocol::{
    body::TopicList,
    codes::{request, response},
    header::{CommandHeader, GetRouteInfoRequestHeader, HeaderMap},
    route::TopicRouteData,
};
use rocketmq_classic_remoting::RemotingCommand;
use tokio::time::Instant;

use super::MQClientApi;
use crate::error::RpcError;

const MISSING_ROUTE_BODY: &str = "missing route body";

impl MQClientApi {
    pub(crate) fn route_error_is_client_exception(error: &RpcError) -> bool {
        matches!(error, RpcError::Response { .. })
            || matches!(error, RpcError::MalformedResponse { request_code: request::GET_ROUTEINFO_BY_TOPIC, reason, .. } if reason == MISSING_ROUTE_BODY)
    }

    /// Java's default-route method queries the fixed auto-create topic.
    /// allowTopicNotExist changes diagnostics only, not the wire or error.
    pub async fn get_default_topic_route(
        &self,
        deadline: Instant,
    ) -> Result<Option<TopicRouteData>, RpcError> {
        self.get_topic_route("TBW102", deadline).await
    }

    pub async fn get_topic_route(
        &self,
        topic: &str,
        deadline: Instant,
    ) -> Result<Option<TopicRouteData>, RpcError> {
        self.get_topic_route_nullable(Some(topic), deadline).await
    }

    /// Retrieves the NameServer topic list through Java
    /// `MQClientAPIImpl.getTopicListFromNameServer`.
    pub async fn get_topic_list_from_nameserver(
        &self,
        deadline: Instant,
    ) -> Result<TopicList, RpcError> {
        let code = request::GET_ALL_TOPIC_LIST_FROM_NAMESERVER;
        let (reply, peer) = self
            .remoting
            .invoke_name_server_sync_with_peer(
                RemotingCommand {
                    code,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ..RemotingCommand::default()
                },
                deadline,
            )
            .await?;
        if reply.code != response::SUCCESS {
            return Err(RpcError::Response {
                request_code: code,
                peer,
                code: reply.code,
                remark: reply.remark,
            });
        }
        let body = reply.body.ok_or_else(|| RpcError::MalformedResponse {
            request_code: code,
            peer: peer.clone(),
            reason: "successful topic-list reply has no body".into(),
        })?;
        TopicList::java_decode(&body).map_err(|error| RpcError::MalformedResponse {
            request_code: code,
            peer,
            reason: error.to_string(),
        })
    }

    pub(crate) async fn get_topic_route_nullable(
        &self,
        topic: Option<&str>,
        deadline: Instant,
    ) -> Result<Option<TopicRouteData>, RpcError> {
        let mut fields = HeaderMap::new();
        if let Some(topic) = topic {
            {
                let mut __phase18_header_0 = GetRouteInfoRequestHeader::default();
                __phase18_header_0.set_topic(topic.to_owned());
                __phase18_header_0.set_accept_standard_json_only(None);
                __phase18_header_0
            }
            .encode_fields(&mut fields);
        }
        let (reply, peer) = self
            .remoting
            .invoke_name_server_sync_with_peer(
                RemotingCommand {
                    code: request::GET_ROUTEINFO_BY_TOPIC,
                    language: Some(rocketmq_classic_protocol::codes::LanguageCode::Java),
                    ext_fields: Some(fields.into()),
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
            reason: MISSING_ROUTE_BODY.into(),
        })?;
        TopicRouteData::java_decode_nullable(Some(&body)).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: request::GET_ROUTEINFO_BY_TOPIC,
                peer,
                reason: error.to_string(),
            }
        })
    }
}
