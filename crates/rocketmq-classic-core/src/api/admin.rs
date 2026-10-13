use rocketmq_classic_common::message::{
    DecodeLimits, DecodeOptions, MessageExt, decode_many, decode_one,
};
use rocketmq_classic_protocol::{
    codes::request,
    header::{
        CommandHeader, CreateTopicRequestHeader, QueryMessageRequestHeader,
        QueryMessageResponseHeader, ViewMessageRequestHeader,
    },
};
use tokio::time::Instant;

use super::{MQClientApi, classify, decode_header};
use crate::error::RpcError;

/// The index watermark and matching messages returned by a Broker query.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueryResult {
    pub index_last_update_timestamp: i64,
    pub message_list: Vec<MessageExt>,
}

impl QueryResult {
    pub fn get_index_last_update_timestamp(&self) -> i64 {
        self.index_last_update_timestamp
    }

    pub fn get_message_list(&self) -> &Vec<MessageExt> {
        &self.message_list
    }

    pub fn get_message_list_mut(&mut self) -> &mut Vec<MessageExt> {
        &mut self.message_list
    }
}

impl MQClientApi {
    /// Sends Java `MQClientAPIImpl.createTopic` to one Broker.
    pub async fn create_topic(
        &self,
        addr: &str,
        header: &CreateTopicRequestHeader,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        let code = request::UPDATE_AND_CREATE_TOPIC;
        let reply = self.request(addr, code, header, None, deadline).await?;
        classify(reply, code, addr)?;
        Ok(())
    }

    /// Queries one Broker using Java's `_UNIQUE_KEY_QUERY` extension field.
    /// The admin facade is responsible for route selection, fan-out and result
    /// merge; this method performs exactly one Broker request.
    pub async fn query_message(
        &self,
        addr: &str,
        header: &QueryMessageRequestHeader,
        unique_key: bool,
        deadline: Instant,
    ) -> Result<QueryResult, RpcError> {
        let code = request::QUERY_MESSAGE;
        let mut fields = rocketmq_classic_protocol::header::HeaderMap::new();
        header.encode_fields(&mut fields);
        fields.insert("_UNIQUE_KEY_QUERY".into(), unique_key.to_string());
        let reply = self
            .request_fields_with_mode(addr, code, fields, None, deadline, true)
            .await?;
        let reply = classify(reply, code, addr)?;
        let response_header: QueryMessageResponseHeader = decode_header(&reply, code, addr)?;
        let body = reply
            .body
            .as_ref()
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: "missing response body".into(),
            })?;
        let message_list = decode_many(body, DecodeOptions::default(), DecodeLimits::default())
            .map_err(|error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            })?;
        Ok(QueryResult {
            index_last_update_timestamp: response_header.index_last_update_timestamp,
            message_list,
        })
    }

    /// Views one physical message by the Broker's commit-log offset.
    pub async fn view_message(
        &self,
        addr: &str,
        header: &ViewMessageRequestHeader,
        deadline: Instant,
    ) -> Result<MessageExt, RpcError> {
        let code = request::VIEW_MESSAGE_BY_ID;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        let body = reply
            .body
            .as_ref()
            .ok_or_else(|| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: "missing response body".into(),
            })?;
        decode_one(body, DecodeOptions::default(), DecodeLimits::default())
            .map(|(message, _)| message)
            .map_err(|error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{MessageExt, QueryResult};

    #[test]
    fn query_result_java_getters_expose_the_stored_values() {
        let mut result = QueryResult {
            index_last_update_timestamp: 17,
            message_list: Vec::new(),
        };
        assert_eq!(result.get_index_last_update_timestamp(), 17);
        assert!(result.get_message_list().is_empty());
        result.get_message_list_mut().push(MessageExt::default());
        assert_eq!(result.get_message_list().len(), 1);
    }
}
