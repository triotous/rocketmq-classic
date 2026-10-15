use rocketmq_classic_common::message::{
    DecodeOptions, MessageExt, constants, decode_many, decode_one,
};
use rocketmq_classic_protocol::{
    SharedValue,
    codes::{request, response},
    header::{
        CommandHeader, CreateTopicRequestHeader, QueryMessageRequestHeader,
        QueryMessageResponseHeader, ViewMessageRequestHeader,
    },
    route::TopicRouteData,
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use tokio::{
    task::JoinSet,
    time::{Instant, timeout_at},
};

use super::{MQClientApi, classify, decode_header};
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

/// A decoded Java RocketMQ physical message ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedMessageId {
    pub address: SocketAddr,
    pub offset: i64,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum MessageIdDecodeError {
    #[error("message ID contains an odd number of hexadecimal digits")]
    OddLength,
    #[error("message ID contains a non-hexadecimal digit at byte {index}")]
    InvalidHex { index: usize },
    #[error("message ID is too short for an address, port and offset")]
    TooShort,
    #[error("message ID contains an invalid port")]
    InvalidPort,
}

/// Decodes Java's offset message ID format: address bytes, network-order port,
/// then a signed network-order commit-log offset.
pub fn decode_message_id(value: &str) -> Result<DecodedMessageId, MessageIdDecodeError> {
    if value.len() % 2 != 0 {
        return Err(MessageIdDecodeError::OddLength);
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high =
            hex_nibble(pair[0]).ok_or(MessageIdDecodeError::InvalidHex { index: index * 2 })?;
        let low = hex_nibble(pair[1]).ok_or(MessageIdDecodeError::InvalidHex {
            index: index * 2 + 1,
        })?;
        bytes.push((high << 4) | low);
    }
    let address_length = if value.len() == 32 { 4 } else { 16 };
    if bytes.len() < address_length + 12 {
        return Err(MessageIdDecodeError::TooShort);
    }
    let address = if address_length == 4 {
        IpAddr::V4(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]))
    } else {
        IpAddr::V6(Ipv6Addr::new(
            u16::from_be_bytes([bytes[0], bytes[1]]),
            u16::from_be_bytes([bytes[2], bytes[3]]),
            u16::from_be_bytes([bytes[4], bytes[5]]),
            u16::from_be_bytes([bytes[6], bytes[7]]),
            u16::from_be_bytes([bytes[8], bytes[9]]),
            u16::from_be_bytes([bytes[10], bytes[11]]),
            u16::from_be_bytes([bytes[12], bytes[13]]),
            u16::from_be_bytes([bytes[14], bytes[15]]),
        ))
    };
    let port_offset = address_length;
    let port = u32::from_be_bytes([
        bytes[port_offset],
        bytes[port_offset + 1],
        bytes[port_offset + 2],
        bytes[port_offset + 3],
    ]);
    let port = u16::try_from(port).map_err(|_| MessageIdDecodeError::InvalidPort)?;
    let offset = i64::from_be_bytes([
        bytes[port_offset + 4],
        bytes[port_offset + 5],
        bytes[port_offset + 6],
        bytes[port_offset + 7],
        bytes[port_offset + 8],
        bytes[port_offset + 9],
        bytes[port_offset + 10],
        bytes[port_offset + 11],
    ]);
    Ok(DecodedMessageId {
        address: SocketAddr::new(address, port),
        offset,
    })
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

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
            topic: request_topic.to_owned(),
            key: query.key.clone(),
            max_num: query.max_num,
            begin_timestamp: query.begin_timestamp,
            end_timestamp: query.end_timestamp,
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
        Ok(QueryResult::new(
            response_header.index_last_update_timestamp,
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
    use super::{
        AdminQueryRequest, DecodedMessageId, MessageExt, MessageIdDecodeError, QueryResult,
        SharedValue, decode_message_id,
    };
    use std::{net::SocketAddr, str::FromStr};

    #[test]
    fn java_message_id_decoder_handles_ipv4_ipv6_and_malformed_values() {
        assert_eq!(
            decode_message_id("C633641400002A9F0000018BCFE56829").unwrap(),
            DecodedMessageId {
                address: SocketAddr::from_str("198.51.100.20:10911").unwrap(),
                offset: 1_700_000_000_041,
            }
        );
        assert_eq!(
            decode_message_id("20010db800000000000000000000000000002a9f0000000000000000").unwrap(),
            DecodedMessageId {
                address: SocketAddr::from_str("[2001:db8::]:10911").unwrap(),
                offset: 0,
            }
        );
        assert_eq!(decode_message_id("a"), Err(MessageIdDecodeError::OddLength));
        assert_eq!(
            decode_message_id("zz"),
            Err(MessageIdDecodeError::InvalidHex { index: 0 })
        );
        assert_eq!(
            decode_message_id("00000000000100000000000000000000"),
            Err(MessageIdDecodeError::InvalidPort)
        );
    }

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
}
