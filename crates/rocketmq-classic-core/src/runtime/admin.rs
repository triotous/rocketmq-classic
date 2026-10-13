use crate::{
    api::admin::{AdminQueryError, AdminQueryRequest, QueryResult},
    error::RpcError,
};
use rocketmq_classic_protocol::codes::{request, response};
use tokio::time::Instant;

use super::ClientRuntime;

impl ClientRuntime {
    /// Queries the route's Brokers with one shared deadline, using Java's
    /// MQAdmin filtering and partial-failure behavior.
    pub async fn query_message(
        &self,
        query: &AdminQueryRequest,
        deadline: Instant,
    ) -> Result<QueryResult, AdminQueryError> {
        let route = match self.get_existing_topic_route(&query.route_topic)? {
            Some(route) => Some(route),
            None => {
                self.update_topic_route(&query.route_topic, deadline)
                    .await?;
                self.get_existing_topic_route(&query.route_topic)?
            }
        };
        let Some(route) = route else {
            return Err(RpcError::Response {
                request_code: request::QUERY_MESSAGE,
                peer: query.topic.clone(),
                code: response::NO_MESSAGE,
                remark: Some("query message by key finished, but no message.".into()),
            }
            .into());
        };
        let route = route.snapshot()?;
        self.api()
            .query_message_across_brokers(&route, query, deadline)
            .await
            .map_err(Into::into)
    }
}
