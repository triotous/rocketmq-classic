use rocketmq_classic_common::message::{
    DecodeOptions, MessageExt, constants, decode_many, decode_one,
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::{GroupList, QuerySubscriptionResponseBody, TopicList, TopicStatsTable},
    codes::{request, response},
    header::{
        CommandHeader, CreateTopicRequestHeader, GetTopicStatsInfoRequestHeader,
        QueryMessageRequestHeader, QueryMessageResponseHeader,
        QuerySubscriptionByConsumerRequestHeader, QueryTopicConsumeByWhoRequestHeader,
        QueryTopicsByConsumerRequestHeader, ViewMessageRequestHeader,
    },
    heartbeat::SubscriptionData,
    route::TopicRouteData,
};
use tokio::{
    task::JoinSet,
    time::{Instant, timeout_at},
};

use super::{MQClientApi, classify, decode_body, decode_header};
use crate::error::RpcError;

/// The index watermark and matching messages returned by a Broker query.
/// Its read-only result fields match Java's constructor/getter surface; the
/// optional list and shared element handles preserve Java null and alias
/// behavior.
///
/// ```compile_fail
/// use rocketmq_classic_core::api::admin::QueryResult;
/// let mut result = QueryResult::new(17, None);
/// result.index_last_update_timestamp = 18;
/// ```
#[derive(Debug)]
pub struct QueryResult {
    pub(crate) index_last_update_timestamp: i64,
    pub(crate) message_list: Option<QueryMessageList>,
}

/// A mutable list handle corresponding to Java's retained `List<MessageExt>`.
pub type QueryMessageList = SharedValue<Vec<Option<SharedValue<MessageExt>>>>;

/// One Java MQAdmin key or unique-key query across a topic route.
/// The route topic and logical-topic flag are derived from Java's query inputs
/// and stay internal. Builder methods retain the nullable cluster, index-type,
/// and pagination inputs of Java's full `MQAdminImpl.queryMessage` overload:
///
/// ```compile_fail
/// use rocketmq_classic_core::api::admin::AdminQueryRequest;
/// let mut request = AdminQueryRequest::by_key("orders", "k", 32, 0, 1);
/// request.logical_message_queue = true;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminQueryRequest {
    pub(crate) route_topic: String,
    pub(crate) topic: String,
    pub(crate) key: String,
    pub(crate) max_num: i32,
    pub(crate) begin_timestamp: i64,
    pub(crate) end_timestamp: i64,
    pub(crate) index_type: Option<String>,
    pub(crate) last_key: Option<String>,
    pub(crate) unique_key: bool,
    pub(crate) cluster_name: Option<String>,
    pub(crate) logical_message_queue: bool,
}

impl AdminQueryRequest {
    pub fn by_key(
        topic: impl Into<String>,
        key: impl Into<String>,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Self {
        let topic = topic.into();
        Self {
            route_topic: topic.clone(),
            logical_message_queue: topic.starts_with("%LMQ%"),
            topic,
            key: key.into(),
            max_num,
            begin_timestamp,
            end_timestamp,
            index_type: Some(constants::INDEX_KEY_TYPE.to_owned()),
            last_key: None,
            unique_key: false,
            cluster_name: None,
        }
    }

    pub fn by_unique_key(
        topic: impl Into<String>,
        unique_key: impl Into<String>,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Self {
        let topic = topic.into();
        Self {
            route_topic: topic.clone(),
            logical_message_queue: topic.starts_with("%LMQ%"),
            topic,
            key: unique_key.into(),
            max_num,
            begin_timestamp,
            end_timestamp,
            index_type: Some(constants::INDEX_UNIQUE_TYPE.to_owned()),
            last_key: None,
            unique_key: true,
            cluster_name: None,
        }
    }

    /// Applies Java's optional cluster selector. Logical topics and the wheel
    /// timer use the supplied cluster as their route topic; ordinary topics
    /// keep their own route and use the cluster only to filter Brokers.
    pub fn in_cluster(self, cluster_name: impl Into<String>) -> Self {
        self.with_cluster_name(Some(cluster_name.into()))
    }

    /// Applies Java's nullable `clusterName` query input.
    pub fn with_cluster_name(mut self, cluster_name: Option<String>) -> Self {
        self.route_topic = self.topic.clone();
        if self.logical_message_queue || self.topic == "rmq_sys_wheel_timer" {
            if let Some(cluster) = cluster_name
                .as_deref()
                .filter(|cluster| !cluster.is_empty())
            {
                self.route_topic = cluster.to_owned();
            }
        }
        self.cluster_name = cluster_name;
        self
    }

    /// Applies Java's nullable `indexType` query input. A null or empty value
    /// causes the Java query-result filter to return no key matches.
    pub fn with_index_type(mut self, index_type: Option<String>) -> Self {
        self.index_type = index_type;
        self
    }

    /// Applies Java's nullable `lastKey` pagination input.
    pub fn with_last_key(mut self, last_key: Option<String>) -> Self {
        self.last_key = last_key;
        self
    }
}

/// Errors from the route lookup or single-Broker calls used by an admin query.
#[derive(Debug, thiserror::Error)]
pub enum AdminQueryError {
    #[error(transparent)]
    Runtime(#[from] crate::runtime::RuntimeError),
    #[error(transparent)]
    Rpc(#[from] RpcError),
}

impl QueryResult {
    /// Constructs a query result from Java's nullable message-list input.
    pub fn new(index_last_update_timestamp: i64, message_list: Option<QueryMessageList>) -> Self {
        Self {
            index_last_update_timestamp,
            message_list,
        }
    }

    pub fn get_index_last_update_timestamp(&self) -> i64 {
        self.index_last_update_timestamp
    }

    /// Returns a shared mutable list handle, preserving Java's getter alias.
    pub fn get_message_list(&self) -> Option<QueryMessageList> {
        self.message_list.clone()
    }
}

impl std::fmt::Display for QueryResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "QueryResult [indexLastUpdateTimestamp={}, messageList=",
            self.index_last_update_timestamp
        )?;
        match &self.message_list {
            None => formatter.write_str("null")?,
            Some(messages) => {
                formatter.write_str("[")?;
                for (index, message) in messages.snapshot().iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    match message {
                        Some(message) => message.with(|message| write!(formatter, "{message}"))?,
                        None => formatter.write_str("null")?,
                    }
                }
                formatter.write_str("]")?;
            }
        }
        formatter.write_str("]")
    }
}

impl MQClientApi {
    /// Gets one Broker's topic statistics and offset table.
    pub async fn get_topic_stats_info(
        &self,
        addr: &str,
        header: &GetTopicStatsInfoRequestHeader,
        deadline: Instant,
    ) -> Result<Option<TopicStatsTable>, RpcError> {
        let code = request::GET_TOPIC_STATS_INFO;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        TopicStatsTable::java_decode_nullable(reply.body.as_deref()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })
    }

    /// Queries one Broker for the consumer groups subscribed to a topic.
    pub async fn query_topic_consume_by_who(
        &self,
        addr: &str,
        header: &QueryTopicConsumeByWhoRequestHeader,
        deadline: Instant,
    ) -> Result<GroupList, RpcError> {
        let code = request::QUERY_TOPIC_CONSUME_BY_WHO;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        decode_body(&reply, code, addr)
    }

    /// Queries one Broker for a group's subscription to a topic.
    pub async fn query_subscription_by_consumer(
        &self,
        addr: &str,
        header: &QuerySubscriptionByConsumerRequestHeader,
        deadline: Instant,
    ) -> Result<Option<SharedValue<SubscriptionData>>, RpcError> {
        let code = request::QUERY_SUBSCRIPTION_BY_CONSUMER;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        let body: QuerySubscriptionResponseBody = decode_body(&reply, code, addr)?;
        Ok(body.get_subscription_data())
    }

    /// Queries one Broker for the topics consumed by a group.
    pub async fn query_topics_by_consumer(
        &self,
        addr: &str,
        header: &QueryTopicsByConsumerRequestHeader,
        deadline: Instant,
    ) -> Result<TopicList, RpcError> {
        let code = request::QUERY_TOPICS_BY_CONSUMER;
        let reply = self.request(addr, code, header, None, deadline).await?;
        let reply = classify(reply, code, addr)?;
        decode_body(&reply, code, addr)
    }

    /// Fans one Java MQAdmin query out to the selected Brokers on a route.
    /// Individual Broker errors and malformed responses are ignored when
    /// another Broker returns matching messages, as in Java's callback path.
    pub async fn query_message_across_brokers(
        &self,
        route: &TopicRouteData,
        query: &AdminQueryRequest,
        deadline: Instant,
    ) -> Result<QueryResult, RpcError> {
        let mut addresses = Vec::new();
        for broker in route.broker_datas_snapshot().unwrap_or_default() {
            let Some(broker) = broker else {
                continue;
            };
            if !query.logical_message_queue
                && query.cluster_name.as_deref().is_some_and(|cluster| {
                    !cluster.is_empty() && broker.cluster.as_deref() != Some(cluster)
                })
            {
                continue;
            }
            match broker.java_select_broker_addr() {
                Ok(Some(address)) => addresses.push(address),
                Ok(None) => {}
                Err(error) => {
                    return Err(RpcError::MalformedResponse {
                        request_code: request::QUERY_MESSAGE,
                        peer: query.route_topic.clone(),
                        reason: error.to_string(),
                    });
                }
            }
        }
        if addresses.is_empty() {
            return Err(no_message_error(&query.topic));
        }

        let request_topic = if query.logical_message_queue {
            query
                .cluster_name
                .as_deref()
                .filter(|cluster| !cluster.is_empty())
                .unwrap_or(&query.topic)
        } else {
            &query.topic
        };
        let header = QueryMessageRequestHeader {
            topic: Some(request_topic.to_owned()),
            key: Some(query.key.clone()),
            max_num: Some(query.max_num),
            begin_timestamp: Some(query.begin_timestamp),
            end_timestamp: Some(query.end_timestamp),
            index_type: query.index_type.clone(),
            last_key: query.last_key.clone(),
            ..QueryMessageRequestHeader::default()
        };
        let mut tasks = JoinSet::new();
        for address in addresses {
            let api = self.clone();
            let header = header.clone();
            let query_is_unique = query.unique_key;
            tasks.spawn(async move {
                api.query_message(&address, &header, query_is_unique, deadline)
                    .await
            });
        }

        let mut watermark = 0;
        let mut messages = Vec::new();
        loop {
            let joined = match timeout_at(deadline, tasks.join_next()).await {
                Ok(joined) => joined,
                Err(_) => {
                    tasks.abort_all();
                    break;
                }
            };
            match joined {
                None => break,
                Some(Ok(Ok(result))) => {
                    watermark = watermark.max(result.index_last_update_timestamp);
                    if let Some(result_messages) = result.message_list {
                        for message in result_messages.snapshot().into_iter().flatten() {
                            if message.with(|message| query_message_matches(message, query)) {
                                messages.push(Some(message));
                            }
                        }
                    }
                }
                Some(_) => continue,
            }
        }

        if messages.is_empty() {
            return Err(RpcError::Response {
                request_code: request::QUERY_MESSAGE,
                peer: query.topic.clone(),
                code: response::NO_MESSAGE,
                remark: Some("query message by key finished, but no message.".into()),
            });
        }
        Ok(QueryResult::new(
            watermark,
            Some(SharedValue::from(messages)),
        ))
    }

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
        let mut message_list = decode_many(body, DecodeOptions::default()).map_err(|error| {
            RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            }
        })?;
        for message in &mut message_list {
            client_decode_message(message);
        }
        let index_last_update_timestamp =
            response_header.index_last_update_timestamp.ok_or_else(|| {
                RpcError::MalformedResponse {
                    request_code: code,
                    peer: addr.to_owned(),
                    reason: "missing or invalid response header field `indexLastUpdateTimestamp`"
                        .into(),
                }
            })?;
        Ok(QueryResult::new(
            index_last_update_timestamp,
            Some(SharedValue::from(
                message_list
                    .into_iter()
                    .map(|message| Some(SharedValue::from(message)))
                    .collect::<Vec<_>>(),
            )),
        ))
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
        decode_one(body, DecodeOptions::default())
            .map(|(mut message, _)| {
                client_decode_message(&mut message);
                message
            })
            .map_err(|error| RpcError::MalformedResponse {
                request_code: code,
                peer: addr.to_owned(),
                reason: error.to_string(),
            })
    }
}

fn no_message_error(topic: &str) -> RpcError {
    RpcError::Response {
        request_code: request::QUERY_MESSAGE,
        peer: topic.to_owned(),
        code: response::NO_MESSAGE,
        remark: Some("query message by key finished, but no message.".into()),
    }
}

fn query_message_matches(message: &MessageExt, query: &AdminQueryRequest) -> bool {
    if query.unique_key {
        return message.message_id() == Some(query.key.as_str());
    }

    let topic_matches = query.logical_message_queue || message.topic() == query.topic.as_str();
    if !topic_matches {
        return false;
    }
    match query.index_type.as_deref() {
        Some(constants::INDEX_KEY_TYPE) => message.keys().is_some_and(|keys| {
            keys.split(constants::KEY_SEPARATOR)
                .any(|key| key == query.key)
        }),
        Some(constants::INDEX_TAG_TYPE) => message.tags().is_some_and(|tags| tags == query.key),
        _ => false,
    }
}

/// Applies the ID fields exposed by Java's `MessageDecoder.clientDecode`.
/// The record decoder creates the physical ID; client decode exposes that as
/// `offsetMsgId` and prefers the `UNIQ_KEY` property for `msgId`.
fn client_decode_message(message: &mut MessageExt) {
    let offset_message_id = message.message_id().map(str::to_owned);
    let client_message_id = message
        .properties()
        .get(constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        .cloned()
        .or_else(|| offset_message_id.clone());
    message.set_offset_message_id(offset_message_id);
    message.set_message_id(client_message_id);
}

#[cfg(test)]
mod tests {
    use super::{AdminQueryRequest, MessageExt, QueryResult, SharedValue};

    #[test]
    fn query_result_java_getters_expose_the_stored_values() {
        let messages = SharedValue::from(Vec::new());
        let result = QueryResult::new(17, Some(messages.clone()));
        assert_eq!(result.get_index_last_update_timestamp(), 17);
        let returned = result.get_message_list().unwrap();
        assert!(returned.ptr_eq(&messages));
        returned.with_mut(|messages| messages.push(Some(SharedValue::from(MessageExt::default()))));
        assert_eq!(messages.snapshot().len(), 1);
    }

    #[test]
    fn query_result_preserves_java_null_list_elements_and_to_string() {
        let null_list = QueryResult::new(17, None);
        assert!(null_list.get_message_list().is_none());
        assert_eq!(
            null_list.to_string(),
            "QueryResult [indexLastUpdateTimestamp=17, messageList=null]"
        );

        let null_element = QueryResult::new(18, Some(SharedValue::from(vec![None])));
        assert_eq!(
            null_element.to_string(),
            "QueryResult [indexLastUpdateTimestamp=18, messageList=[null]]"
        );
    }

    #[test]
    fn query_cluster_inputs_derive_java_route_topic() {
        let logical = AdminQueryRequest::by_key("%LMQ%child", "k", 32, 0, 1)
            .with_cluster_name(Some("parent".into()));
        assert!(logical.logical_message_queue);
        assert_eq!(logical.route_topic, "parent");

        let logical_without_cluster = logical.with_cluster_name(None);
        assert_eq!(logical_without_cluster.route_topic, "%LMQ%child");

        let wheel_timer = AdminQueryRequest::by_key("rmq_sys_wheel_timer", "k", 32, 0, 1)
            .with_cluster_name(Some("cluster-a".into()));
        assert!(!wheel_timer.logical_message_queue);
        assert_eq!(wheel_timer.route_topic, "cluster-a");

        let ordinary = AdminQueryRequest::by_key("orders", "k", 32, 0, 1)
            .with_cluster_name(Some("cluster-a".into()));
        assert_eq!(ordinary.route_topic, "orders");
    }

    #[tokio::test]
    async fn query_topics_by_consumer_sends_java_header_and_decodes_topic_list() {
        use crate::api::MQClientApi;
        use rocketmq_classic_protocol::{
            codes::{request, response},
            header::{CommandHeader, HeaderMap, QueryTopicsByConsumerRequestHeader},
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::time::Duration;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request.code, request::QUERY_TOPICS_BY_CONSUMER);
            assert_eq!(
                request
                    .ext_fields
                    .as_ref()
                    .and_then(|fields| fields.get("group"))
                    .map(String::as_str),
                Some("workers")
            );
            let mut expected_fields = HeaderMap::new();
            CommandHeader::encode_fields(
                &QueryTopicsByConsumerRequestHeader {
                    group: Some("workers".into()),
                    namespace: Some("tenant".into()),
                    namespaced: Some(true),
                    broker_name: Some("broker-a".into()),
                    oneway: Some(false),
                },
                &mut expected_fields,
            );
            assert_eq!(
                request
                    .ext_fields
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|(key, value)| { (key.clone(), value.clone()) })
                    .collect::<HeaderMap>(),
                expected_fields
            );

            let mut reply = RemotingCommand {
                code: response::SUCCESS,
                opaque: request.opaque,
                body: Some(
                    br#"{"topicList":["orders","billing"],"brokerAddr":"broker-a"}"#.to_vec(),
                ),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let api = MQClientApi::new(remoting.clone());
        let result = api
            .query_topics_by_consumer(
                &address,
                &QueryTopicsByConsumerRequestHeader {
                    group: Some("workers".into()),
                    namespace: Some("tenant".into()),
                    namespaced: Some(true),
                    broker_name: Some("broker-a".into()),
                    oneway: Some(false),
                },
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert_eq!(result.get_broker_addr(), Some("broker-a"));
        assert_eq!(
            result.get_topic_list().unwrap().snapshot(),
            std::collections::HashSet::from(["orders".to_owned(), "billing".to_owned()])
        );
        peer.await.unwrap();
        remoting.shutdown().await;
    }

    #[tokio::test]
    async fn query_topics_by_consumer_preserves_broker_error() {
        use crate::{api::MQClientApi, error::RpcError};
        use rocketmq_classic_protocol::{
            codes::{request, response},
            header::QueryTopicsByConsumerRequestHeader,
        };
        use rocketmq_classic_remoting::{
            RemotingCommand,
            client::{RemotingClient, TransportMode},
            codec::frame,
        };
        use std::time::Duration;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut encoded = bytes::BytesMut::from(&length[..]);
            encoded.resize(u32::from_be_bytes(length) as usize + 4, 0);
            stream.read_exact(&mut encoded[4..]).await.unwrap();
            let request = frame::decode_next(&mut encoded).unwrap().unwrap();
            assert_eq!(request.code, request::QUERY_TOPICS_BY_CONSUMER);

            let mut reply = RemotingCommand {
                code: response::TOPIC_NOT_EXIST,
                opaque: request.opaque,
                remark: Some("topic not exist".into()),
                ..Default::default()
            };
            reply.mark_response();
            stream
                .write_all(&frame::encode(&reply).unwrap())
                .await
                .unwrap();
        });

        let remoting = RemotingClient::new(TransportMode::Plain);
        remoting.start().unwrap();
        let api = MQClientApi::new(remoting.clone());
        let error = api
            .query_topics_by_consumer(
                &address,
                &QueryTopicsByConsumerRequestHeader {
                    group: Some("workers".into()),
                    ..QueryTopicsByConsumerRequestHeader::default()
                },
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RpcError::Response {
                request_code: request::QUERY_TOPICS_BY_CONSUMER,
                peer,
                code: response::TOPIC_NOT_EXIST,
                remark: Some(remark),
            } if peer == address && remark == "topic not exist"
        ));
        peer.await.unwrap();
        remoting.shutdown().await;
    }
}
