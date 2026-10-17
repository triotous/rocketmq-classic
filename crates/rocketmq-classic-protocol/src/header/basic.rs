use std::hash::{Hash, Hasher};

use super::{
    CommandHeader, HeaderMap, ProtocolError, java_boolean, optional_i32, optional_i32_with_mode,
    optional_i64, optional_i64_with_mode, optional_java_boolean, optional_string, required_i32,
    required_i64, required_string,
};
use crate::codes::JavaEnumLookupError;

macro_rules! impl_rpc_request_header_semantics {
    ($($header:ty),+ $(,)?) => {
        $(
            impl PartialEq for $header {
                fn eq(&self, other: &Self) -> bool {
                    self.namespace == other.namespace
                        && self.namespaced == other.namespaced
                        && self.broker_name == other.broker_name
                        && self.oneway == other.oneway
                }
            }

            impl Eq for $header {}

            impl Hash for $header {
                fn hash<H: Hasher>(&self, state: &mut H) {
                    self.namespace.hash(state);
                    self.namespaced.hash(state);
                    self.broker_name.hash(state);
                    self.oneway.hash(state);
                }
            }
        )+
    };
}

impl_rpc_request_header_semantics!(
    QueryConsumerOffsetRequestHeader,
    UpdateConsumerOffsetRequestHeader,
    QueryConsumeTimeSpanRequestHeader,
    SearchOffsetRequestHeader,
    GetMaxOffsetRequestHeader,
    GetMinOffsetRequestHeader,
    GetEarliestMsgStoretimeRequestHeader,
    PullMessageRequestHeader,
    UnregisterClientRequestHeader,
    GetRouteInfoRequestHeader,
    CreateTopicRequestHeader,
    QueryMessageRequestHeader,
    GetConsumerListByGroupRequestHeader,
    LockBatchMqRequestHeader,
    UnlockBatchMqRequestHeader,
    ConsumerSendMsgBackRequestHeader,
    RecallMessageRequestHeader,
    EndTransactionRequestHeader,
    CheckTransactionStateRequestHeader,
    ReplyMessageRequestHeader,
    NotificationRequestHeader,
    ResetOffsetRequestHeader,
    GetConsumerStatusRequestHeader,
    GetConsumerRunningInfoRequestHeader,
    PopMessageRequestHeader,
    AckMessageRequestHeader,
    ChangeInvisibleTimeRequestHeader,
    PollingInfoRequestHeader,
    SendMessageRequestHeaderV1,
    SendMessageRequestHeaderV2,
);

impl PartialEq for ConsumeMessageDirectlyResultRequestHeader {
    fn eq(&self, other: &Self) -> bool {
        self.namespace == other.namespace
            && self.namespaced == other.namespaced
            && self.rpc_broker_name == other.rpc_broker_name
            && self.oneway == other.oneway
    }
}

impl Eq for ConsumeMessageDirectlyResultRequestHeader {}

impl Hash for ConsumeMessageDirectlyResultRequestHeader {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.namespace.hash(state);
        self.namespaced.hash(state);
        self.rpc_broker_name.hash(state);
        self.oneway.hash(state);
    }
}

fn put_optional<T: ToString>(fields: &mut HeaderMap, name: &str, value: Option<T>) {
    if let Some(value) = value {
        fields.insert(name.to_owned(), value.to_string());
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryConsumerOffsetRequestHeader {
    pub consumer_group: Option<String>,
    pub topic: Option<String>,
    pub queue_id: Option<i32>,
    pub set_zero_if_not_found: Option<bool>,
    pub broker_name: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub logical: Option<bool>,
    pub oneway: Option<bool>,
}

impl CommandHeader for QueryConsumerOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        put_optional(fields, "setZeroIfNotFound", self.set_zero_if_not_found);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            logical: optional_java_boolean(fields, "lo"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: Some(required_string(fields, "consumerGroup")?.to_owned()),
            topic: Some(required_string(fields, "topic")?.to_owned()),
            queue_id: Some(required_i32(fields, "queueId")?),
            set_zero_if_not_found: optional_java_boolean(fields, "setZeroIfNotFound"),
            broker_name: optional_string(fields, "bname"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            logical: optional_java_boolean(fields, "lo"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: optional_string(fields, "consumerGroup"),
            topic: optional_string(fields, "topic"),
            queue_id: optional_i32_with_mode(fields, "queueId", false)?,
            set_zero_if_not_found: optional_java_boolean(fields, "setZeroIfNotFound"),
            broker_name: optional_string(fields, "bname"),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct UpdateConsumerOffsetRequestHeader {
    pub consumer_group: Option<String>,
    pub topic: Option<String>,
    pub queue_id: Option<i32>,
    pub commit_offset: Option<i64>,
    pub broker_name: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub oneway: Option<bool>,
}

impl CommandHeader for UpdateConsumerOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        put_optional(fields, "commitOffset", self.commit_offset);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: Some(required_string(fields, "consumerGroup")?.to_owned()),
            topic: Some(required_string(fields, "topic")?.to_owned()),
            queue_id: Some(required_i32(fields, "queueId")?),
            commit_offset: Some(required_i64(fields, "commitOffset")?),
            broker_name: optional_string(fields, "bname"),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: optional_string(fields, "consumerGroup"),
            topic: optional_string(fields, "topic"),
            queue_id: optional_i32_with_mode(fields, "queueId", false)?,
            commit_offset: optional_i64_with_mode(fields, "commitOffset", false)?,
            broker_name: optional_string(fields, "bname"),
        })
    }
}

/// Pinned Java `QueryConsumerOffsetResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::QueryConsumerOffsetResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<QueryConsumerOffsetResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct QueryConsumerOffsetResponseHeader {
    pub offset: Option<i64>,
}

impl CommandHeader for QueryConsumerOffsetResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "offset", self.offset);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: Some(required_i64(fields, "offset")?),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: optional_i64_with_mode(fields, "offset", false)?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryConsumeTimeSpanRequestHeader {
    pub topic: Option<String>,
    pub group: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for QueryConsumeTimeSpanRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "group", self.group.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: optional_string(fields, "topic"),
            group: optional_string(fields, "group"),
        })
    }
}

/// Pinned Java `org.apache.rocketmq.common.BoundaryType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryType {
    Lower,
    Upper,
}

impl BoundaryType {
    pub const fn values() -> [Self; 2] {
        [Self::Lower, Self::Upper]
    }

    pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
        match name {
            None => Err(JavaEnumLookupError::NullName),
            Some("LOWER") => Ok(Self::Lower),
            Some("UPPER") => Ok(Self::Upper),
            Some(other) => Err(JavaEnumLookupError::UnknownName {
                enum_class: "org.apache.rocketmq.common.BoundaryType",
                name: other.to_owned(),
            }),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Lower => "LOWER",
            Self::Upper => "UPPER",
        }
    }

    /// Java `BoundaryType.getName()` uses lowercase values for broker semantics.
    pub const fn get_name(self) -> &'static str {
        match self {
            Self::Lower => "lower",
            Self::Upper => "upper",
        }
    }

    /// Java `BoundaryType.getType` recognizes the case-insensitive `upper`
    /// name and falls back to LOWER for null or any other input.
    pub fn get_type(name: Option<&str>) -> Self {
        if name.is_some_and(|name| name.eq_ignore_ascii_case("upper")) {
            Self::Upper
        } else {
            Self::Lower
        }
    }

    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Lower => 0,
            Self::Upper => 1,
        }
    }

    pub const fn java_compare_to(self, other: Self) -> i32 {
        self.ordinal() - other.ordinal()
    }
}

impl Default for BoundaryType {
    fn default() -> Self {
        Self::Lower
    }
}

impl std::fmt::Display for BoundaryType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Debug, Clone, Default)]
pub struct SearchOffsetRequestHeader {
    pub topic: String,
    pub lite_topic: Option<String>,
    pub queue_id: i32,
    pub timestamp: i64,
    pub boundary_type: Option<BoundaryType>,
    pub broker_name: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub oneway: Option<bool>,
}

impl CommandHeader for SearchOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("topic".into(), self.topic.clone());
        put_optional(fields, "liteTopic", self.lite_topic.as_deref());
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("timestamp".into(), self.timestamp.to_string());
        put_optional(
            fields,
            "boundaryType",
            self.boundary_type.map(BoundaryType::name),
        );
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        let boundary_type = fields
            .get("boundaryType")
            .map(|value| {
                BoundaryType::value_of_name(Some(value)).map_err(|error| {
                    ProtocolError::InvalidField {
                        field: "boundaryType",
                        reason: error.to_string(),
                    }
                })
            })
            .transpose()?;
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: required_string(fields, "topic")?.to_owned(),
            lite_topic: optional_string(fields, "liteTopic"),
            queue_id: required_i32(fields, "queueId")?,
            timestamp: required_i64(fields, "timestamp")?,
            boundary_type,
            broker_name: optional_string(fields, "bname"),
        })
    }
}

#[derive(Debug, Clone)]
pub struct GetMaxOffsetRequestHeader {
    pub topic: String,
    pub queue_id: i32,
    pub committed: bool,
    pub broker_name: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub oneway: Option<bool>,
}

impl Default for GetMaxOffsetRequestHeader {
    fn default() -> Self {
        Self {
            topic: String::new(),
            queue_id: 0,
            committed: true,
            broker_name: None,
            namespace: None,
            namespaced: None,
            oneway: None,
        }
    }
}

impl CommandHeader for GetMaxOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("committed".into(), self.committed.to_string());
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: required_i32(fields, "queueId")?,
            committed: optional_java_boolean(fields, "committed").unwrap_or(true),
            broker_name: optional_string(fields, "bname"),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetMinOffsetRequestHeader {
    pub topic: String,
    pub queue_id: i32,
    pub broker_name: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub oneway: Option<bool>,
}

impl CommandHeader for GetMinOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: required_i32(fields, "queueId")?,
            broker_name: optional_string(fields, "bname"),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetEarliestMsgStoretimeRequestHeader {
    pub topic: String,
    pub queue_id: i32,
    pub broker_name: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub oneway: Option<bool>,
}

impl CommandHeader for GetEarliestMsgStoretimeRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: required_i32(fields, "queueId")?,
            broker_name: optional_string(fields, "bname"),
        })
    }
}

/// Pinned Java `SearchOffsetResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::SearchOffsetResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<SearchOffsetResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchOffsetResponseHeader {
    pub offset: Option<i64>,
}

impl CommandHeader for SearchOffsetResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "offset", self.offset);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: Some(required_i64(fields, "offset")?),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: optional_i64_with_mode(fields, "offset", false)?,
        })
    }
}

/// Pinned Java `GetMaxOffsetResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetMaxOffsetResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<GetMaxOffsetResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct GetMaxOffsetResponseHeader {
    pub offset: Option<i64>,
}

impl CommandHeader for GetMaxOffsetResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "offset", self.offset);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: Some(required_i64(fields, "offset")?),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: optional_i64_with_mode(fields, "offset", false)?,
        })
    }
}

/// Pinned Java `GetMinOffsetResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetMinOffsetResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<GetMinOffsetResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct GetMinOffsetResponseHeader {
    pub offset: Option<i64>,
}

impl CommandHeader for GetMinOffsetResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "offset", self.offset);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: Some(required_i64(fields, "offset")?),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            offset: optional_i64_with_mode(fields, "offset", false)?,
        })
    }
}

/// Pinned Java `GetEarliestMsgStoretimeResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::GetEarliestMsgStoretimeResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<GetEarliestMsgStoretimeResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct GetEarliestMsgStoretimeResponseHeader {
    pub timestamp: Option<i64>,
}

impl CommandHeader for GetEarliestMsgStoretimeResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "timestamp", self.timestamp);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            timestamp: Some(required_i64(fields, "timestamp")?),
        })
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        _use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            timestamp: optional_i64_with_mode(fields, "timestamp", false)?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct PullMessageRequestHeader {
    pub consumer_group: Option<String>,
    pub topic: Option<String>,
    pub queue_id: Option<i32>,
    pub queue_offset: Option<i64>,
    pub max_msg_nums: Option<i32>,
    pub sys_flag: Option<i32>,
    pub commit_offset: Option<i64>,
    pub suspend_timeout_millis: Option<i64>,
    pub subscription: Option<String>,
    pub sub_version: Option<i64>,
    pub expression_type: Option<String>,
    pub max_msg_bytes: Option<i32>,
    pub request_source: Option<i32>,
    pub proxy_forward_client_id: Option<String>,
    pub broker_name: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub logical: Option<bool>,
    pub oneway: Option<bool>,
}

impl CommandHeader for PullMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        put_optional(fields, "queueOffset", self.queue_offset);
        put_optional(fields, "maxMsgNums", self.max_msg_nums);
        put_optional(fields, "sysFlag", self.sys_flag);
        put_optional(fields, "commitOffset", self.commit_offset);
        put_optional(fields, "suspendTimeoutMillis", self.suspend_timeout_millis);
        put_optional(fields, "subscription", self.subscription.as_deref());
        put_optional(fields, "subVersion", self.sub_version);
        put_optional(fields, "expressionType", self.expression_type.as_deref());
        put_optional(fields, "maxMsgBytes", self.max_msg_bytes);
        put_optional(fields, "requestSource", self.request_source);
        put_optional(
            fields,
            "proxyFrowardClientId",
            self.proxy_forward_client_id.as_deref(),
        );
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, true)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        let header = Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            logical: optional_java_boolean(fields, "lo"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: optional_string(fields, "consumerGroup"),
            topic: optional_string(fields, "topic"),
            queue_id: optional_i32_with_mode(fields, "queueId", use_fast_encode)?,
            queue_offset: optional_i64_with_mode(fields, "queueOffset", use_fast_encode)?,
            max_msg_nums: optional_i32_with_mode(fields, "maxMsgNums", use_fast_encode)?,
            sys_flag: optional_i32_with_mode(fields, "sysFlag", use_fast_encode)?,
            commit_offset: optional_i64_with_mode(fields, "commitOffset", use_fast_encode)?,
            suspend_timeout_millis: optional_i64_with_mode(
                fields,
                "suspendTimeoutMillis",
                use_fast_encode,
            )?,
            subscription: optional_string(fields, "subscription"),
            sub_version: optional_i64_with_mode(fields, "subVersion", use_fast_encode)?,
            expression_type: optional_string(fields, "expressionType"),
            max_msg_bytes: optional_i32_with_mode(fields, "maxMsgBytes", use_fast_encode)?,
            request_source: optional_i32_with_mode(fields, "requestSource", use_fast_encode)?,
            proxy_forward_client_id: optional_string(fields, "proxyFrowardClientId"),
            broker_name: optional_string(fields, "bname"),
        };
        header.check_fields()?;
        Ok(header)
    }
}

/// Pinned Java `PullMessageResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PullMessageResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<PullMessageResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct PullMessageResponseHeader {
    pub suggest_which_broker_id: Option<i64>,
    pub next_begin_offset: Option<i64>,
    pub min_offset: Option<i64>,
    pub max_offset: Option<i64>,
    pub offset_delta: Option<i64>,
    pub topic_sys_flag: Option<i32>,
    pub group_sys_flag: Option<i32>,
    pub forbidden_type: Option<i32>,
}

impl CommandHeader for PullMessageResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "suggestWhichBrokerId", self.suggest_which_broker_id);
        put_optional(fields, "nextBeginOffset", self.next_begin_offset);
        put_optional(fields, "minOffset", self.min_offset);
        put_optional(fields, "maxOffset", self.max_offset);
        put_optional(fields, "offsetDelta", self.offset_delta);
        put_optional(fields, "topicSysFlag", self.topic_sys_flag);
        put_optional(fields, "groupSysFlag", self.group_sys_flag);
        put_optional(fields, "forbiddenType", self.forbidden_type);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, true)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            suggest_which_broker_id: optional_i64_with_mode(
                fields,
                "suggestWhichBrokerId",
                use_fast_encode,
            )?,
            next_begin_offset: optional_i64_with_mode(fields, "nextBeginOffset", use_fast_encode)?,
            min_offset: optional_i64_with_mode(fields, "minOffset", use_fast_encode)?,
            max_offset: optional_i64_with_mode(fields, "maxOffset", use_fast_encode)?,
            offset_delta: optional_i64_with_mode(fields, "offsetDelta", use_fast_encode)?,
            topic_sys_flag: optional_i32_with_mode(fields, "topicSysFlag", use_fast_encode)?,
            group_sys_flag: optional_i32_with_mode(fields, "groupSysFlag", use_fast_encode)?,
            forbidden_type: optional_i32_with_mode(fields, "forbiddenType", use_fast_encode)?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct UnregisterClientRequestHeader {
    pub client_id: String,
    pub producer_group: Option<String>,
    pub consumer_group: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for UnregisterClientRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("clientID".into(), self.client_id.clone());
        put_optional(fields, "producerGroup", self.producer_group.as_deref());
        put_optional(fields, "consumerGroup", self.consumer_group.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            client_id: required_string(fields, "clientID")?.to_owned(),
            producer_group: optional_string(fields, "producerGroup"),
            consumer_group: optional_string(fields, "consumerGroup"),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetRouteInfoRequestHeader {
    pub topic: String,
    pub accept_standard_json_only: Option<bool>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for GetRouteInfoRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("topic".into(), self.topic.clone());
        put_optional(
            fields,
            "acceptStandardJsonOnly",
            self.accept_standard_json_only,
        );
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: required_string(fields, "topic")?.to_owned(),
            accept_standard_json_only: optional_java_boolean(fields, "acceptStandardJsonOnly"),
        })
    }
}

/// Pinned Java `CreateTopicRequestHeader` used by `UPDATE_AND_CREATE_TOPIC`.
#[derive(Debug, Clone)]
pub struct CreateTopicRequestHeader {
    pub topic: String,
    pub default_topic: String,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
    pub topic_filter_type: String,
    pub topic_sys_flag: Option<i32>,
    pub order: Option<bool>,
    pub attributes: Option<String>,
    pub force: Option<bool>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
    pub logical: Option<bool>,
}

impl Default for CreateTopicRequestHeader {
    fn default() -> Self {
        Self {
            topic: String::new(),
            default_topic: String::new(),
            read_queue_nums: 0,
            write_queue_nums: 0,
            perm: 0,
            topic_filter_type: String::new(),
            topic_sys_flag: None,
            order: Some(false),
            attributes: None,
            force: Some(false),
            namespace: None,
            namespaced: None,
            broker_name: None,
            oneway: None,
            logical: None,
        }
    }
}

impl CommandHeader for CreateTopicRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("defaultTopic".into(), self.default_topic.clone());
        fields.insert("readQueueNums".into(), self.read_queue_nums.to_string());
        fields.insert("writeQueueNums".into(), self.write_queue_nums.to_string());
        fields.insert("perm".into(), self.perm.to_string());
        fields.insert("topicFilterType".into(), self.topic_filter_type.clone());
        put_optional(fields, "topicSysFlag", self.topic_sys_flag);
        put_optional(fields, "order", self.order);
        put_optional(fields, "attributes", self.attributes.as_deref());
        put_optional(fields, "force", self.force);
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        let topic_filter_type = required_string(fields, "topicFilterType")?.to_owned();
        let header = Self {
            topic: required_string(fields, "topic")?.to_owned(),
            default_topic: required_string(fields, "defaultTopic")?.to_owned(),
            read_queue_nums: required_i32(fields, "readQueueNums")?,
            write_queue_nums: required_i32(fields, "writeQueueNums")?,
            perm: required_i32(fields, "perm")?,
            topic_filter_type,
            topic_sys_flag: optional_i32(fields, "topicSysFlag")?,
            order: Some(optional_java_boolean(fields, "order").unwrap_or(false)),
            attributes: optional_string(fields, "attributes"),
            force: Some(optional_java_boolean(fields, "force").unwrap_or(false)),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
        };
        header.check_fields()?;
        Ok(header)
    }

    fn check_fields(&self) -> Result<(), ProtocolError> {
        if matches!(self.topic_filter_type.as_str(), "SINGLE_TAG" | "MULTI_TAG") {
            Ok(())
        } else {
            Err(ProtocolError::InvalidField {
                field: "topicFilterType",
                reason: format!("unknown Java TopicFilterType {}", self.topic_filter_type),
            })
        }
    }
}

/// Pinned Java `QueryMessageRequestHeader` used by `QUERY_MESSAGE`.
#[derive(Debug, Clone, Default)]
pub struct QueryMessageRequestHeader {
    pub topic: String,
    pub key: String,
    pub max_num: i32,
    pub begin_timestamp: i64,
    pub end_timestamp: i64,
    pub index_type: Option<String>,
    pub last_key: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
    pub logical: Option<bool>,
}

impl CommandHeader for QueryMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("key".into(), self.key.clone());
        fields.insert("maxNum".into(), self.max_num.to_string());
        fields.insert("beginTimestamp".into(), self.begin_timestamp.to_string());
        fields.insert("endTimestamp".into(), self.end_timestamp.to_string());
        put_optional(fields, "indexType", self.index_type.as_deref());
        put_optional(fields, "lastKey", self.last_key.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: required_string(fields, "topic")?.to_owned(),
            key: required_string(fields, "key")?.to_owned(),
            max_num: required_i32(fields, "maxNum")?,
            begin_timestamp: required_i64(fields, "beginTimestamp")?,
            end_timestamp: required_i64(fields, "endTimestamp")?,
            index_type: optional_string(fields, "indexType"),
            last_key: optional_string(fields, "lastKey"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
        })
    }
}

/// Pinned Java `QueryMessageResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::QueryMessageResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<QueryMessageResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct QueryMessageResponseHeader {
    pub index_last_update_timestamp: i64,
    pub index_last_update_phyoffset: i64,
}

impl CommandHeader for QueryMessageResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert(
            "indexLastUpdateTimestamp".into(),
            self.index_last_update_timestamp.to_string(),
        );
        fields.insert(
            "indexLastUpdatePhyoffset".into(),
            self.index_last_update_phyoffset.to_string(),
        );
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            index_last_update_timestamp: required_i64(fields, "indexLastUpdateTimestamp")?,
            index_last_update_phyoffset: required_i64(fields, "indexLastUpdatePhyoffset")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ViewMessageRequestHeader {
    pub topic: Option<String>,
    pub offset: i64,
}

impl CommandHeader for ViewMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        fields.insert("offset".into(), self.offset.to_string());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: optional_string(fields, "topic"),
            offset: required_i64(fields, "offset")?,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetConsumerListByGroupRequestHeader {
    pub consumer_group: String,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct LockBatchMqRequestHeader {
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for LockBatchMqRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct UnlockBatchMqRequestHeader {
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for UnlockBatchMqRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

impl CommandHeader for GetConsumerListByGroupRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConsumerSendMsgBackRequestHeader {
    pub group: String,
    pub broker_name: Option<String>,
    pub origin_topic: String,
    pub offset: i64,
    pub delay_level: i32,
    pub origin_msg_id: Option<String>,
    pub max_reconsume_times: Option<i32>,
    pub unit_mode: bool,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub oneway: Option<bool>,
}

impl CommandHeader for ConsumerSendMsgBackRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("group".into(), self.group.clone());
        put_optional(fields, "bname", self.broker_name.as_deref());
        fields.insert("originTopic".into(), self.origin_topic.clone());
        fields.insert("offset".into(), self.offset.to_string());
        fields.insert("delayLevel".into(), self.delay_level.to_string());
        put_optional(fields, "originMsgId", self.origin_msg_id.as_deref());
        put_optional(fields, "maxReconsumeTimes", self.max_reconsume_times);
        fields.insert("unitMode".into(), self.unit_mode.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            oneway: optional_java_boolean(fields, "oway"),
            group: required_string(fields, "group")?.to_owned(),
            broker_name: optional_string(fields, "bname"),
            origin_topic: required_string(fields, "originTopic")?.to_owned(),
            offset: required_i64(fields, "offset")?,
            delay_level: required_i32(fields, "delayLevel")?,
            origin_msg_id: optional_string(fields, "originMsgId"),
            max_reconsume_times: optional_i32(fields, "maxReconsumeTimes")?,
            unit_mode: fields
                .get("unitMode")
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
        })
    }
}

/// Java 5.5.1 `RecallMessageRequestHeader`, including nullable fields inherited
/// from `RpcRequestHeader` and `TopicRequestHeader`.
#[derive(Debug, Clone, Default)]
pub struct RecallMessageRequestHeader {
    pub producer_group: Option<String>,
    pub topic: String,
    pub recall_handle: String,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
    pub logical: Option<bool>,
}

impl CommandHeader for RecallMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "producerGroup", self.producer_group.as_deref());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("recallHandle".into(), self.recall_handle.clone());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
        put_optional(fields, "lo", self.logical);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            producer_group: optional_string(fields, "producerGroup"),
            topic: required_string(fields, "topic")?.to_owned(),
            recall_handle: required_string(fields, "recallHandle")?.to_owned(),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            logical: optional_java_boolean(fields, "lo"),
        })
    }
}

/// Pinned Java `RecallMessageResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::RecallMessageResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<RecallMessageResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct RecallMessageResponseHeader {
    pub msg_id: String,
}

impl CommandHeader for RecallMessageResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("msgId".into(), self.msg_id.clone());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            msg_id: required_string(fields, "msgId")?.to_owned(),
        })
    }
}

/// Pinned Java `SendMessageResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::SendMessageResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<SendMessageResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct SendMessageResponseHeader {
    pub msg_id: Option<String>,
    pub queue_id: Option<i32>,
    pub queue_offset: Option<i64>,
    pub transaction_id: Option<String>,
    pub batch_uniq_id: Option<String>,
    pub recall_handle: Option<String>,
}

/// Pinned Java `UnregisterClientResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::UnregisterClientResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<UnregisterClientResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct UnregisterClientResponseHeader;

impl CommandHeader for UnregisterClientResponseHeader {
    fn encode_fields(&self, _fields: &mut HeaderMap) {}

    fn decode_fields(_fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self)
    }
}

#[derive(Debug, Clone)]
pub struct EndTransactionRequestHeader {
    pub topic: Option<String>,
    pub producer_group: Option<String>,
    pub transaction_state_table_offset: i64,
    pub commit_log_offset: i64,
    pub commit_or_rollback: i32,
    pub from_transaction_check: Option<bool>,
    pub msg_id: String,
    pub transaction_id: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl Default for EndTransactionRequestHeader {
    fn default() -> Self {
        Self {
            topic: None,
            producer_group: None,
            transaction_state_table_offset: 0,
            commit_log_offset: 0,
            commit_or_rollback: 0,
            from_transaction_check: Some(false),
            msg_id: String::new(),
            transaction_id: None,
            namespace: None,
            namespaced: None,
            broker_name: None,
            oneway: None,
        }
    }
}

impl CommandHeader for EndTransactionRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "producerGroup", self.producer_group.as_deref());
        fields.insert(
            "tranStateTableOffset".into(),
            self.transaction_state_table_offset.to_string(),
        );
        fields.insert("commitLogOffset".into(), self.commit_log_offset.to_string());
        fields.insert(
            "commitOrRollback".into(),
            self.commit_or_rollback.to_string(),
        );
        put_optional(fields, "fromTransactionCheck", self.from_transaction_check);
        fields.insert("msgId".into(), self.msg_id.clone());
        put_optional(fields, "transactionId", self.transaction_id.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: optional_string(fields, "topic"),
            producer_group: optional_string(fields, "producerGroup"),
            transaction_state_table_offset: required_i64(fields, "tranStateTableOffset")?,
            commit_log_offset: required_i64(fields, "commitLogOffset")?,
            commit_or_rollback: required_i32(fields, "commitOrRollback")?,
            from_transaction_check: optional_java_boolean(fields, "fromTransactionCheck"),
            msg_id: required_string(fields, "msgId")?.to_owned(),
            transaction_id: optional_string(fields, "transactionId"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }

    fn check_fields(&self) -> Result<(), ProtocolError> {
        if matches!(self.commit_or_rollback, 0 | 8 | 12) {
            Ok(())
        } else {
            Err(ProtocolError::InvalidField {
                field: "commitOrRollback",
                reason: "value invalid".into(),
            })
        }
    }
}

/// Pinned Java `EndTransactionResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::EndTransactionResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<EndTransactionResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct EndTransactionResponseHeader;

impl CommandHeader for EndTransactionResponseHeader {
    fn encode_fields(&self, _fields: &mut HeaderMap) {}
    fn decode_fields(_fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CheckTransactionStateRequestHeader {
    pub topic: Option<String>,
    pub transaction_state_table_offset: i64,
    pub commit_log_offset: i64,
    pub msg_id: Option<String>,
    pub transaction_id: Option<String>,
    pub offset_msg_id: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for CheckTransactionStateRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "topic", self.topic.as_deref());
        fields.insert(
            "tranStateTableOffset".into(),
            self.transaction_state_table_offset.to_string(),
        );
        fields.insert("commitLogOffset".into(), self.commit_log_offset.to_string());
        put_optional(fields, "msgId", self.msg_id.as_deref());
        put_optional(fields, "transactionId", self.transaction_id.as_deref());
        put_optional(fields, "offsetMsgId", self.offset_msg_id.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            topic: optional_string(fields, "topic"),
            transaction_state_table_offset: required_i64(fields, "tranStateTableOffset")?,
            commit_log_offset: required_i64(fields, "commitLogOffset")?,
            msg_id: optional_string(fields, "msgId"),
            transaction_id: optional_string(fields, "transactionId"),
            offset_msg_id: optional_string(fields, "offsetMsgId"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

/// Pinned Java `CheckTransactionStateResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::CheckTransactionStateResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<CheckTransactionStateResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct CheckTransactionStateResponseHeader {
    pub producer_group: String,
    pub transaction_state_table_offset: i64,
    pub commit_log_offset: i64,
    pub commit_or_rollback: i32,
}

impl CommandHeader for CheckTransactionStateResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("producerGroup".into(), self.producer_group.clone());
        fields.insert(
            "tranStateTableOffset".into(),
            self.transaction_state_table_offset.to_string(),
        );
        fields.insert("commitLogOffset".into(), self.commit_log_offset.to_string());
        fields.insert(
            "commitOrRollback".into(),
            self.commit_or_rollback.to_string(),
        );
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            producer_group: required_string(fields, "producerGroup")?.to_owned(),
            transaction_state_table_offset: required_i64(fields, "tranStateTableOffset")?,
            commit_log_offset: required_i64(fields, "commitLogOffset")?,
            commit_or_rollback: required_i32(fields, "commitOrRollback")?,
        })
    }

    fn check_fields(&self) -> Result<(), ProtocolError> {
        if matches!(self.commit_or_rollback, 8 | 12) {
            Ok(())
        } else {
            Err(ProtocolError::InvalidField {
                field: "commitOrRollback",
                reason: "value invalid".into(),
            })
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReplyMessageRequestHeader {
    pub producer_group: String,
    pub topic: String,
    pub default_topic: String,
    pub default_topic_queue_nums: i32,
    pub queue_id: i32,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub flag: i32,
    pub properties: Option<String>,
    pub reconsume_times: Option<i32>,
    pub unit_mode: bool,
    pub born_host: String,
    pub store_host: String,
    pub store_timestamp: i64,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct NotificationRequestHeader {
    pub consumer_group: String,
    pub topic: String,
    pub queue_id: i32,
    pub poll_time: i64,
    pub born_time: i64,
    pub order: Option<bool>,
    pub attempt_id: Option<String>,
    pub lite_consumer: bool,
    pub client_id: Option<String>,
    pub expression_type: Option<String>,
    pub expression: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for NotificationRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("pollTime".into(), self.poll_time.to_string());
        fields.insert("bornTime".into(), self.born_time.to_string());
        put_optional(fields, "order", self.order);
        put_optional(fields, "attemptId", self.attempt_id.as_deref());
        fields.insert("isLiteConsumer".into(), self.lite_consumer.to_string());
        put_optional(fields, "clientId", self.client_id.as_deref());
        put_optional(fields, "expType", self.expression_type.as_deref());
        put_optional(fields, "exp", self.expression.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: required_i32(fields, "queueId")?,
            poll_time: required_i64(fields, "pollTime")?,
            born_time: required_i64(fields, "bornTime")?,
            order: optional_java_boolean(fields, "order"),
            attempt_id: optional_string(fields, "attemptId"),
            lite_consumer: java_boolean(fields, "isLiteConsumer"),
            client_id: optional_string(fields, "clientId"),
            expression_type: optional_string(fields, "expType"),
            expression: optional_string(fields, "exp"),
        })
    }
}

/// Pinned Java `NotificationResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::NotificationResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<NotificationResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct NotificationResponseHeader {
    pub has_msg: bool,
    pub polling_full: bool,
}

impl CommandHeader for NotificationResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("hasMsg".into(), self.has_msg.to_string());
        fields.insert("pollingFull".into(), self.polling_full.to_string());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            has_msg: java_boolean(fields, "hasMsg"),
            polling_full: java_boolean(fields, "pollingFull"),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ResetOffsetRequestHeader {
    pub group: String,
    pub topic: String,
    pub queue_id: i32,
    pub offset: Option<i64>,
    pub timestamp: i64,
    pub force: bool,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for ResetOffsetRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("group".into(), self.group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        put_optional(fields, "offset", self.offset);
        fields.insert("timestamp".into(), self.timestamp.to_string());
        fields.insert("isForce".into(), self.force.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            group: required_string(fields, "group")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: optional_i32(fields, "queueId")?.unwrap_or(-1),
            offset: optional_i64(fields, "offset")?,
            timestamp: required_i64(fields, "timestamp")?,
            force: java_boolean(fields, "isForce"),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetConsumerStatusRequestHeader {
    pub topic: String,
    pub group: String,
    pub client_addr: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for GetConsumerStatusRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("group".into(), self.group.clone());
        put_optional(fields, "clientAddr", self.client_addr.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            topic: required_string(fields, "topic")?.to_owned(),
            group: required_string(fields, "group")?.to_owned(),
            client_addr: optional_string(fields, "clientAddr"),
        })
    }
}

/// Pinned Java `GetConsumerRunningInfoRequestHeader`, including the inherited
/// `RpcRequestHeader` namespace and broker-routing fields.
#[derive(Debug, Clone, Default)]
pub struct GetConsumerRunningInfoRequestHeader {
    pub consumer_group: String,
    pub client_id: String,
    pub jstack_enable: bool,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for GetConsumerRunningInfoRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("clientId".into(), self.client_id.clone());
        fields.insert("jstackEnable".into(), self.jstack_enable.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            client_id: required_string(fields, "clientId")?.to_owned(),
            jstack_enable: java_boolean(fields, "jstackEnable"),
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConsumeMessageDirectlyResultRequestHeader {
    pub consumer_group: String,
    pub client_id: Option<String>,
    pub msg_id: Option<String>,
    pub broker_name: Option<String>,
    pub topic: Option<String>,
    pub topic_sys_flag: Option<i32>,
    pub group_sys_flag: Option<i32>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    /// The inherited `RpcRequestHeader.bname`; distinct from this class's
    /// `brokerName` payload field.
    pub rpc_broker_name: Option<String>,
    pub oneway: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct PopMessageRequestHeader {
    pub consumer_group: String,
    pub topic: String,
    pub queue_id: i32,
    pub max_msg_nums: i32,
    pub invisible_time: i64,
    pub poll_time: i64,
    pub born_time: i64,
    pub init_mode: i32,
    pub expression_type: Option<String>,
    pub expression: Option<String>,
    pub order: Option<bool>,
    pub attempt_id: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for PopMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("maxMsgNums".into(), self.max_msg_nums.to_string());
        fields.insert("invisibleTime".into(), self.invisible_time.to_string());
        fields.insert("pollTime".into(), self.poll_time.to_string());
        fields.insert("bornTime".into(), self.born_time.to_string());
        fields.insert("initMode".into(), self.init_mode.to_string());
        put_optional(fields, "expType", self.expression_type.as_deref());
        put_optional(fields, "exp", self.expression.as_deref());
        put_optional(fields, "order", self.order);
        put_optional(fields, "attemptId", self.attempt_id.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: required_i32(fields, "queueId")?,
            max_msg_nums: required_i32(fields, "maxMsgNums")?,
            invisible_time: required_i64(fields, "invisibleTime")?,
            poll_time: required_i64(fields, "pollTime")?,
            born_time: required_i64(fields, "bornTime")?,
            init_mode: required_i32(fields, "initMode")?,
            expression_type: optional_string(fields, "expType"),
            expression: optional_string(fields, "exp"),
            order: optional_java_boolean(fields, "order"),
            attempt_id: optional_string(fields, "attemptId"),
        })
    }
}

/// Pinned Java `PopMessageResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PopMessageResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<PopMessageResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct PopMessageResponseHeader {
    pub pop_time: i64,
    pub invisible_time: i64,
    pub revive_queue_id: i32,
    pub rest_num: i64,
    pub start_offset_info: Option<String>,
    pub msg_offset_info: Option<String>,
    pub order_count_info: Option<String>,
}

impl CommandHeader for PopMessageResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("popTime".into(), self.pop_time.to_string());
        fields.insert("invisibleTime".into(), self.invisible_time.to_string());
        fields.insert("reviveQid".into(), self.revive_queue_id.to_string());
        fields.insert("restNum".into(), self.rest_num.to_string());
        put_optional(fields, "startOffsetInfo", self.start_offset_info.as_deref());
        put_optional(fields, "msgOffsetInfo", self.msg_offset_info.as_deref());
        put_optional(fields, "orderCountInfo", self.order_count_info.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            pop_time: required_i64(fields, "popTime")?,
            invisible_time: required_i64(fields, "invisibleTime")?,
            revive_queue_id: required_i32(fields, "reviveQid")?,
            rest_num: required_i64(fields, "restNum")?,
            start_offset_info: optional_string(fields, "startOffsetInfo"),
            msg_offset_info: optional_string(fields, "msgOffsetInfo"),
            order_count_info: optional_string(fields, "orderCountInfo"),
        })
    }
}

/// Pinned Java `PopLiteMessageResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PopLiteMessageResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<PopLiteMessageResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct PopLiteMessageResponseHeader {
    pub pop_time: i64,
    pub invisible_time: i64,
    pub revive_queue_id: i32,
    pub start_offset_info: Option<String>,
    pub msg_offset_info: Option<String>,
    pub order_count_info: Option<String>,
}

impl CommandHeader for PopLiteMessageResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("popTime".into(), self.pop_time.to_string());
        fields.insert("invisibleTime".into(), self.invisible_time.to_string());
        fields.insert("reviveQid".into(), self.revive_queue_id.to_string());
        put_optional(fields, "startOffsetInfo", self.start_offset_info.as_deref());
        put_optional(fields, "msgOffsetInfo", self.msg_offset_info.as_deref());
        put_optional(fields, "orderCountInfo", self.order_count_info.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            pop_time: required_i64(fields, "popTime")?,
            invisible_time: required_i64(fields, "invisibleTime")?,
            revive_queue_id: required_i32(fields, "reviveQid")?,
            start_offset_info: optional_string(fields, "startOffsetInfo"),
            msg_offset_info: optional_string(fields, "msgOffsetInfo"),
            order_count_info: optional_string(fields, "orderCountInfo"),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct AckMessageRequestHeader {
    pub consumer_group: String,
    pub topic: String,
    pub queue_id: i32,
    pub extra_info: String,
    pub offset: i64,
    pub lite_topic: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for AckMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("extraInfo".into(), self.extra_info.clone());
        fields.insert("offset".into(), self.offset.to_string());
        put_optional(fields, "liteTopic", self.lite_topic.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: required_i32(fields, "queueId")?,
            extra_info: required_string(fields, "extraInfo")?.to_owned(),
            offset: required_i64(fields, "offset")?,
            lite_topic: optional_string(fields, "liteTopic"),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ChangeInvisibleTimeRequestHeader {
    pub consumer_group: String,
    pub topic: String,
    pub queue_id: i32,
    pub extra_info: String,
    pub offset: i64,
    pub invisible_time: i64,
    pub lite_topic: Option<String>,
    pub suspend: bool,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct PollingInfoRequestHeader {
    pub consumer_group: String,
    pub topic: String,
    pub queue_id: i32,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for PollingInfoRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: required_i32(fields, "queueId")?,
        })
    }
}

/// Pinned Java `PollingInfoResponseHeader` inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::header::PollingInfoResponseHeader;
/// fn requires_value_equality<T: PartialEq + Eq>() {}
/// fn main() { requires_value_equality::<PollingInfoResponseHeader>(); }
/// ```
#[derive(Debug, Clone, Default)]
pub struct PollingInfoResponseHeader {
    pub polling_num: i32,
}

impl CommandHeader for PollingInfoResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("pollingNum".into(), self.polling_num.to_string());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            polling_num: required_i32(fields, "pollingNum")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PopLiteMessageRequestHeader {
    pub client_id: String,
    pub consumer_group: String,
    pub topic: String,
    pub max_msg_num: i32,
    pub invisible_time: i64,
    pub poll_time: i64,
    pub born_time: i64,
    pub attempt_id: Option<String>,
}

impl CommandHeader for PopLiteMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("clientId".into(), self.client_id.clone());
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("maxMsgNum".into(), self.max_msg_num.to_string());
        fields.insert("invisibleTime".into(), self.invisible_time.to_string());
        fields.insert("pollTime".into(), self.poll_time.to_string());
        fields.insert("bornTime".into(), self.born_time.to_string());
        put_optional(fields, "attemptId", self.attempt_id.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            client_id: required_string(fields, "clientId")?.to_owned(),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            max_msg_num: required_i32(fields, "maxMsgNum")?,
            invisible_time: required_i64(fields, "invisibleTime")?,
            poll_time: required_i64(fields, "pollTime")?,
            born_time: required_i64(fields, "bornTime")?,
            attempt_id: optional_string(fields, "attemptId"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LiteSubscriptionCtlRequestHeader;

impl CommandHeader for LiteSubscriptionCtlRequestHeader {
    fn encode_fields(&self, _fields: &mut HeaderMap) {}
    fn decode_fields(_fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NotifyUnsubscribeLiteRequestHeader {
    pub lite_topic: String,
    pub consumer_group: String,
    pub client_id: String,
}

impl CommandHeader for NotifyUnsubscribeLiteRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("liteTopic".into(), self.lite_topic.clone());
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("clientId".into(), self.client_id.clone());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            lite_topic: required_string(fields, "liteTopic")?.to_owned(),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            client_id: required_string(fields, "clientId")?.to_owned(),
        })
    }
}

impl CommandHeader for ChangeInvisibleTimeRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("extraInfo".into(), self.extra_info.clone());
        fields.insert("offset".into(), self.offset.to_string());
        fields.insert("invisibleTime".into(), self.invisible_time.to_string());
        put_optional(fields, "liteTopic", self.lite_topic.as_deref());
        fields.insert("suspend".into(), self.suspend.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            queue_id: required_i32(fields, "queueId")?,
            extra_info: required_string(fields, "extraInfo")?.to_owned(),
            offset: required_i64(fields, "offset")?,
            invisible_time: required_i64(fields, "invisibleTime")?,
            lite_topic: optional_string(fields, "liteTopic"),
            suspend: java_boolean(fields, "suspend"),
        })
    }
}

impl CommandHeader for ConsumeMessageDirectlyResultRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("consumerGroup".into(), self.consumer_group.clone());
        put_optional(fields, "clientId", self.client_id.as_deref());
        put_optional(fields, "msgId", self.msg_id.as_deref());
        put_optional(fields, "brokerName", self.broker_name.as_deref());
        put_optional(fields, "topic", self.topic.as_deref());
        put_optional(fields, "topicSysFlag", self.topic_sys_flag);
        put_optional(fields, "groupSysFlag", self.group_sys_flag);
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.rpc_broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            rpc_broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            consumer_group: required_string(fields, "consumerGroup")?.to_owned(),
            client_id: optional_string(fields, "clientId"),
            msg_id: optional_string(fields, "msgId"),
            broker_name: optional_string(fields, "brokerName"),
            topic: optional_string(fields, "topic"),
            topic_sys_flag: optional_i32(fields, "topicSysFlag")?,
            group_sys_flag: optional_i32(fields, "groupSysFlag")?,
        })
    }
}

impl CommandHeader for ReplyMessageRequestHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        fields.insert("producerGroup".into(), self.producer_group.clone());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("defaultTopic".into(), self.default_topic.clone());
        fields.insert(
            "defaultTopicQueueNums".into(),
            self.default_topic_queue_nums.to_string(),
        );
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("sysFlag".into(), self.sys_flag.to_string());
        fields.insert("bornTimestamp".into(), self.born_timestamp.to_string());
        fields.insert("flag".into(), self.flag.to_string());
        put_optional(fields, "properties", self.properties.as_deref());
        put_optional(fields, "reconsumeTimes", self.reconsume_times);
        fields.insert("unitMode".into(), self.unit_mode.to_string());
        fields.insert("bornHost".into(), self.born_host.clone());
        fields.insert("storeHost".into(), self.store_host.clone());
        fields.insert("storeTimestamp".into(), self.store_timestamp.to_string());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            producer_group: required_string(fields, "producerGroup")?.to_owned(),
            topic: required_string(fields, "topic")?.to_owned(),
            default_topic: required_string(fields, "defaultTopic")?.to_owned(),
            default_topic_queue_nums: required_i32(fields, "defaultTopicQueueNums")?,
            queue_id: required_i32(fields, "queueId")?,
            sys_flag: required_i32(fields, "sysFlag")?,
            born_timestamp: required_i64(fields, "bornTimestamp")?,
            flag: required_i32(fields, "flag")?,
            properties: optional_string(fields, "properties"),
            reconsume_times: optional_i32(fields, "reconsumeTimes")?,
            unit_mode: java_boolean(fields, "unitMode"),
            born_host: required_string(fields, "bornHost")?.to_owned(),
            store_host: required_string(fields, "storeHost")?.to_owned(),
            store_timestamp: required_i64(fields, "storeTimestamp")?,
        })
    }
}

impl CommandHeader for SendMessageResponseHeader {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "msgId", self.msg_id.as_deref());
        put_optional(fields, "queueId", self.queue_id);
        put_optional(fields, "queueOffset", self.queue_offset);
        put_optional(fields, "transactionId", self.transaction_id.as_deref());
        put_optional(fields, "batchUniqId", self.batch_uniq_id.as_deref());
        put_optional(fields, "recallHandle", self.recall_handle.as_deref());
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, true)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        let header = Self {
            msg_id: optional_string(fields, "msgId"),
            queue_id: optional_i32_with_mode(fields, "queueId", use_fast_encode)?,
            queue_offset: optional_i64_with_mode(fields, "queueOffset", use_fast_encode)?,
            transaction_id: optional_string(fields, "transactionId"),
            batch_uniq_id: optional_string(fields, "batchUniqId"),
            recall_handle: optional_string(fields, "recallHandle"),
        };
        header.check_fields()?;
        Ok(header)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SendMessageRequestHeaderV2 {
    pub a: Option<String>,
    pub b: Option<String>,
    pub c: Option<String>,
    pub d: Option<i32>,
    pub e: Option<i32>,
    pub f: Option<i32>,
    pub g: Option<i64>,
    pub h: Option<i32>,
    pub i: Option<String>,
    pub j: Option<i32>,
    pub k: Option<bool>,
    pub l: Option<i32>,
    pub m: Option<bool>,
    pub n: Option<String>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub logical: Option<bool>,
    pub broker_name: Option<String>,
    pub oneway: Option<bool>,
}

impl CommandHeader for SendMessageRequestHeaderV2 {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "a", self.a.as_deref());
        put_optional(fields, "b", self.b.as_deref());
        put_optional(fields, "c", self.c.as_deref());
        put_optional(fields, "d", self.d);
        put_optional(fields, "e", self.e);
        put_optional(fields, "f", self.f);
        put_optional(fields, "g", self.g);
        put_optional(fields, "h", self.h);
        put_optional(fields, "i", self.i.as_deref());
        put_optional(fields, "j", self.j);
        put_optional(fields, "k", self.k);
        put_optional(fields, "l", self.l);
        put_optional(fields, "m", self.m);
        put_optional(fields, "n", self.n.as_deref());
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Self::decode_fields_with_mode(fields, true)
    }

    fn decode_fields_with_mode(
        fields: &HeaderMap,
        use_fast_encode: bool,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: (!use_fast_encode)
                .then(|| optional_string(fields, "ns"))
                .flatten(),
            namespaced: (!use_fast_encode)
                .then(|| optional_java_boolean(fields, "nsd"))
                .flatten(),
            logical: (!use_fast_encode)
                .then(|| optional_java_boolean(fields, "lo"))
                .flatten(),
            broker_name: (!use_fast_encode)
                .then(|| optional_string(fields, "bname"))
                .flatten(),
            oneway: (!use_fast_encode)
                .then(|| optional_java_boolean(fields, "oway"))
                .flatten(),
            a: optional_string(fields, "a"),
            b: optional_string(fields, "b"),
            c: optional_string(fields, "c"),
            d: optional_i32_with_mode(fields, "d", use_fast_encode)?,
            e: optional_i32_with_mode(fields, "e", use_fast_encode)?,
            f: optional_i32_with_mode(fields, "f", use_fast_encode)?,
            g: optional_i64_with_mode(fields, "g", use_fast_encode)?,
            h: optional_i32_with_mode(fields, "h", use_fast_encode)?,
            i: optional_string(fields, "i"),
            j: optional_i32_with_mode(fields, "j", use_fast_encode)?,
            k: optional_java_boolean(fields, "k"),
            l: optional_i32_with_mode(fields, "l", use_fast_encode)?,
            m: optional_java_boolean(fields, "m"),
            n: optional_string(fields, "n"),
        })
    }
}

impl SendMessageRequestHeaderV2 {
    pub fn from_v1(header: &SendMessageRequestHeaderV1) -> Self {
        Self {
            a: header.producer_group.clone(),
            b: Some(header.topic.clone()),
            c: Some(header.default_topic.clone()),
            d: Some(header.default_topic_queue_nums),
            e: Some(header.queue_id),
            f: Some(header.sys_flag),
            g: Some(header.born_timestamp),
            h: Some(header.flag),
            i: header.properties.clone(),
            j: header.reconsume_times,
            k: header.unit_mode,
            l: header.max_reconsume_times,
            m: header.batch,
            n: header.broker_name.clone(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SendMessageRequestHeaderV1 {
    pub producer_group: Option<String>,
    pub topic: String,
    pub default_topic: String,
    pub default_topic_queue_nums: i32,
    pub queue_id: i32,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub flag: i32,
    pub properties: Option<String>,
    pub reconsume_times: Option<i32>,
    pub unit_mode: Option<bool>,
    pub max_reconsume_times: Option<i32>,
    pub batch: Option<bool>,
    pub namespace: Option<String>,
    pub namespaced: Option<bool>,
    pub broker_name: Option<String>,
    pub logical: Option<bool>,
    pub oneway: Option<bool>,
}

impl CommandHeader for SendMessageRequestHeaderV1 {
    fn encode_fields(&self, fields: &mut HeaderMap) {
        put_optional(fields, "producerGroup", self.producer_group.as_deref());
        fields.insert("topic".into(), self.topic.clone());
        fields.insert("defaultTopic".into(), self.default_topic.clone());
        fields.insert(
            "defaultTopicQueueNums".into(),
            self.default_topic_queue_nums.to_string(),
        );
        fields.insert("queueId".into(), self.queue_id.to_string());
        fields.insert("sysFlag".into(), self.sys_flag.to_string());
        fields.insert("bornTimestamp".into(), self.born_timestamp.to_string());
        fields.insert("flag".into(), self.flag.to_string());
        put_optional(fields, "properties", self.properties.as_deref());
        put_optional(fields, "reconsumeTimes", self.reconsume_times);
        put_optional(fields, "unitMode", self.unit_mode);
        put_optional(fields, "maxReconsumeTimes", self.max_reconsume_times);
        put_optional(fields, "batch", self.batch);
        put_optional(fields, "ns", self.namespace.as_deref());
        put_optional(fields, "nsd", self.namespaced);
        put_optional(fields, "bname", self.broker_name.as_deref());
        put_optional(fields, "lo", self.logical);
        put_optional(fields, "oway", self.oneway);
    }

    fn decode_fields(fields: &HeaderMap) -> Result<Self, ProtocolError> {
        Ok(Self {
            namespace: optional_string(fields, "ns"),
            namespaced: optional_java_boolean(fields, "nsd"),
            logical: optional_java_boolean(fields, "lo"),
            broker_name: optional_string(fields, "bname"),
            oneway: optional_java_boolean(fields, "oway"),
            producer_group: optional_string(fields, "producerGroup"),
            topic: required_string(fields, "topic")?.to_owned(),
            default_topic: required_string(fields, "defaultTopic")?.to_owned(),
            default_topic_queue_nums: required_i32(fields, "defaultTopicQueueNums")?,
            queue_id: required_i32(fields, "queueId")?,
            sys_flag: required_i32(fields, "sysFlag")?,
            born_timestamp: required_i64(fields, "bornTimestamp")?,
            flag: required_i32(fields, "flag")?,
            properties: optional_string(fields, "properties"),
            reconsume_times: optional_i32(fields, "reconsumeTimes")?,
            unit_mode: optional_java_boolean(fields, "unitMode"),
            max_reconsume_times: optional_i32(fields, "maxReconsumeTimes")?,
            batch: optional_java_boolean(fields, "batch"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_header_identity_matches_invoked_java() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/protocol/header-response-identity.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["clientArtifact"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(
            fixture["sourceCommit"],
            "efa1be59d6fc747ad220f036c86030f05f0419f9"
        );
        assert_eq!(fixture["serverContacted"], false);
        let cases = fixture["identityCases"].as_object().unwrap();
        assert_eq!(cases.len(), 16);
        for (rust_name, identity) in cases {
            assert_eq!(
                identity["javaClass"],
                format!("org.apache.rocketmq.remoting.protocol.header.{rust_name}")
            );
            assert_eq!(
                identity["equalsDeclaredBy"], "java.lang.Object",
                "{rust_name}"
            );
            assert_eq!(
                identity["hashCodeDeclaredBy"], "java.lang.Object",
                "{rust_name}"
            );
            assert_eq!(identity["sameObjectEqualsItself"], true, "{rust_name}");
            assert_eq!(
                identity["distinctDefaultObjectsEquals"], false,
                "{rust_name}"
            );
        }
    }

    fn request_header_hash<T: std::hash::Hash>(value: &T) -> u64 {
        use std::hash::Hasher;

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    macro_rules! check_rpc_request_header {
        ($header:ty, $broker_field:ident, $prepare:expr, $payload_change:expr, $decode_fast:expr) => {{
            let mut header = <$header>::default();
            header.namespace = Some("namespace".into());
            header.namespaced = Some(true);
            header.$broker_field = Some("broker".into());
            header.oneway = Some(false);
            ($prepare)(&mut header);

            let mut payload_change = header.clone();
            ($payload_change)(&mut payload_change);
            assert_eq!(
                header,
                payload_change,
                "{} payload equality",
                stringify!($header)
            );
            assert_eq!(
                request_header_hash(&header),
                request_header_hash(&payload_change),
                "{} payload hash",
                stringify!($header)
            );

            let mut fields = HeaderMap::new();
            header.encode_fields(&mut fields);
            assert_eq!(fields.get("ns").map(String::as_str), Some("namespace"));
            assert_eq!(fields.get("nsd").map(String::as_str), Some("true"));
            assert_eq!(fields.get("bname").map(String::as_str), Some("broker"));
            assert_eq!(fields.get("oway").map(String::as_str), Some("false"));
            let decoded = <$header>::decode_fields_with_mode(&fields, $decode_fast).unwrap();
            assert_eq!(
                header,
                decoded,
                "{} base-field round trip",
                stringify!($header)
            );

            let mut changed_namespace = header.clone();
            changed_namespace.namespace = Some("other-namespace".into());
            assert_ne!(header, changed_namespace, "{} ns", stringify!($header));
            let mut changed_namespaced = header.clone();
            changed_namespaced.namespaced = Some(false);
            assert_ne!(header, changed_namespaced, "{} nsd", stringify!($header));
            let mut changed_broker = header.clone();
            changed_broker.$broker_field = Some("other-broker".into());
            assert_ne!(header, changed_broker, "{} bname", stringify!($header));
            let mut changed_oneway = header.clone();
            changed_oneway.oneway = Some(true);
            assert_ne!(header, changed_oneway, "{} oway", stringify!($header));
        }};
        ($header:ty, $broker_field:ident, $prepare:expr, $payload_change:expr) => {
            check_rpc_request_header!($header, $broker_field, $prepare, $payload_change, true);
        };
    }

    #[test]
    fn request_header_equality_and_hash_match_rpc_base_fields() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/protocol/request-header-identity.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["clientArtifact"],
            "org.apache.rocketmq:rocketmq-client:5.5.1"
        );
        assert_eq!(
            fixture["sourceCommit"],
            "efa1be59d6fc747ad220f036c86030f05f0419f9"
        );
        assert_eq!(fixture["serverContacted"], false);
        let cases = fixture["identityCases"].as_object().unwrap();
        assert_eq!(cases.len(), 31);
        for (rust_name, identity) in cases {
            let java_class = if rust_name == "GetRouteInfoRequestHeader" {
                "org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader"
                    .to_owned()
            } else if rust_name == "SendMessageRequestHeaderV1" {
                "org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader".to_owned()
            } else {
                format!("org.apache.rocketmq.remoting.protocol.header.{rust_name}")
            };
            assert_eq!(identity["javaClass"], java_class, "{rust_name}");
            assert_eq!(
                identity["equalsDeclaredBy"], "org.apache.rocketmq.remoting.rpc.RpcRequestHeader",
                "{rust_name}"
            );
            assert_eq!(
                identity["hashCodeDeclaredBy"], "org.apache.rocketmq.remoting.rpc.RpcRequestHeader",
                "{rust_name}"
            );
            assert_eq!(identity["sameObjectEqualsItself"], true, "{rust_name}");
            assert_eq!(
                identity["distinctDefaultObjectsEquals"], true,
                "{rust_name}"
            );
            assert_eq!(identity["equalBaseFieldsEquals"], true, "{rust_name}");
            assert_eq!(identity["equalBaseFieldsHashEqual"], true, "{rust_name}");
            assert_eq!(identity["payloadDifferenceIgnored"], true, "{rust_name}");
            assert_eq!(
                identity["payloadDifferenceTested"],
                !matches!(
                    rust_name.as_str(),
                    "LockBatchMqRequestHeader" | "UnlockBatchMqRequestHeader"
                ),
                "{rust_name}"
            );
            for field in ["ns", "nsd", "bname", "oway"] {
                assert_eq!(
                    identity["baseFieldDifferenceEquals"][field], false,
                    "{rust_name}.{field}"
                );
            }
        }

        check_rpc_request_header!(
            QueryConsumerOffsetRequestHeader,
            broker_name,
            |_| {},
            |h: &mut QueryConsumerOffsetRequestHeader| {
                h.consumer_group = Some("payload".into())
            }
        );
        check_rpc_request_header!(
            UpdateConsumerOffsetRequestHeader,
            broker_name,
            |_| {},
            |h: &mut UpdateConsumerOffsetRequestHeader| h.commit_offset = Some(17)
        );
        check_rpc_request_header!(
            QueryConsumeTimeSpanRequestHeader,
            broker_name,
            |_| {},
            |h: &mut QueryConsumeTimeSpanRequestHeader| h.topic = Some("payload".into())
        );
        check_rpc_request_header!(
            SearchOffsetRequestHeader,
            broker_name,
            |_| {},
            |h: &mut SearchOffsetRequestHeader| h.timestamp = 17
        );
        check_rpc_request_header!(
            GetMaxOffsetRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetMaxOffsetRequestHeader| h.committed = false
        );
        check_rpc_request_header!(
            GetMinOffsetRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetMinOffsetRequestHeader| h.queue_id = 17
        );
        check_rpc_request_header!(
            GetEarliestMsgStoretimeRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetEarliestMsgStoretimeRequestHeader| h.queue_id = 17
        );
        check_rpc_request_header!(
            PullMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut PullMessageRequestHeader| h.consumer_group = Some("payload".into())
        );
        check_rpc_request_header!(
            UnregisterClientRequestHeader,
            broker_name,
            |_| {},
            |h: &mut UnregisterClientRequestHeader| h.client_id = "payload".into()
        );
        check_rpc_request_header!(
            GetRouteInfoRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetRouteInfoRequestHeader| h.topic = "payload".into()
        );
        check_rpc_request_header!(
            CreateTopicRequestHeader,
            broker_name,
            |h: &mut CreateTopicRequestHeader| h.topic_filter_type = "SINGLE_TAG".into(),
            |h: &mut CreateTopicRequestHeader| h.topic = "payload".into()
        );
        check_rpc_request_header!(
            QueryMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut QueryMessageRequestHeader| h.key = "payload".into()
        );
        check_rpc_request_header!(
            GetConsumerListByGroupRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetConsumerListByGroupRequestHeader| h.consumer_group = "payload".into()
        );
        check_rpc_request_header!(
            LockBatchMqRequestHeader,
            broker_name,
            |_| {},
            |_: &mut LockBatchMqRequestHeader| {}
        );
        check_rpc_request_header!(
            UnlockBatchMqRequestHeader,
            broker_name,
            |_| {},
            |_: &mut UnlockBatchMqRequestHeader| {}
        );
        check_rpc_request_header!(
            ConsumerSendMsgBackRequestHeader,
            broker_name,
            |_| {},
            |h: &mut ConsumerSendMsgBackRequestHeader| h.group = "payload".into()
        );
        check_rpc_request_header!(
            RecallMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut RecallMessageRequestHeader| h.recall_handle = "payload".into()
        );
        check_rpc_request_header!(
            EndTransactionRequestHeader,
            broker_name,
            |_| {},
            |h: &mut EndTransactionRequestHeader| h.transaction_id = Some("payload".into())
        );
        check_rpc_request_header!(
            CheckTransactionStateRequestHeader,
            broker_name,
            |_| {},
            |h: &mut CheckTransactionStateRequestHeader| h.transaction_id = Some("payload".into())
        );
        check_rpc_request_header!(
            ReplyMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut ReplyMessageRequestHeader| h.producer_group = "payload".into()
        );
        check_rpc_request_header!(
            NotificationRequestHeader,
            broker_name,
            |_| {},
            |h: &mut NotificationRequestHeader| h.attempt_id = Some("payload".into())
        );
        check_rpc_request_header!(
            ResetOffsetRequestHeader,
            broker_name,
            |_| {},
            |h: &mut ResetOffsetRequestHeader| h.group = "payload".into()
        );
        check_rpc_request_header!(
            GetConsumerStatusRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetConsumerStatusRequestHeader| h.topic = "payload".into()
        );
        check_rpc_request_header!(
            GetConsumerRunningInfoRequestHeader,
            broker_name,
            |_| {},
            |h: &mut GetConsumerRunningInfoRequestHeader| h.consumer_group = "payload".into()
        );
        check_rpc_request_header!(
            ConsumeMessageDirectlyResultRequestHeader,
            rpc_broker_name,
            |_| {},
            |h: &mut ConsumeMessageDirectlyResultRequestHeader| h.broker_name =
                Some("payload".into())
        );
        check_rpc_request_header!(
            PopMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut PopMessageRequestHeader| h.consumer_group = "payload".into()
        );
        check_rpc_request_header!(
            AckMessageRequestHeader,
            broker_name,
            |_| {},
            |h: &mut AckMessageRequestHeader| h.consumer_group = "payload".into()
        );
        check_rpc_request_header!(
            ChangeInvisibleTimeRequestHeader,
            broker_name,
            |_| {},
            |h: &mut ChangeInvisibleTimeRequestHeader| h.consumer_group = "payload".into()
        );
        check_rpc_request_header!(
            PollingInfoRequestHeader,
            broker_name,
            |_| {},
            |h: &mut PollingInfoRequestHeader| h.consumer_group = "payload".into()
        );
        check_rpc_request_header!(
            SendMessageRequestHeaderV1,
            broker_name,
            |_| {},
            |h: &mut SendMessageRequestHeaderV1| h.topic = "payload".into()
        );
        check_rpc_request_header!(
            SendMessageRequestHeaderV2,
            broker_name,
            |_| {},
            |h: &mut SendMessageRequestHeaderV2| h.b = Some("payload".into()),
            false
        );
    }

    #[test]
    fn query_offset_preserves_unknown_fields_and_optional_false() {
        let raw = HeaderMap::from([
            ("consumerGroup".into(), "workers".into()),
            ("topic".into(), "orders".into()),
            ("queueId".into(), "0".into()),
            ("setZeroIfNotFound".into(), "false".into()),
            ("futureField".into(), "keep".into()),
        ]);
        let mut updated = raw.clone();
        let mut typed = QueryConsumerOffsetRequestHeader::decode_fields(&raw).unwrap();
        typed.set_zero_if_not_found = Some(true);
        typed.encode_fields(&mut updated);

        assert_eq!(updated["setZeroIfNotFound"], "true");
        assert_eq!(updated["futureField"], "keep");
        assert_eq!(raw["setZeroIfNotFound"], "false");
    }

    #[test]
    fn send_v1_and_v2_round_trip_compact_fields() {
        let v1 = SendMessageRequestHeaderV1 {
            producer_group: Some("producer".into()),
            topic: "orders".into(),
            default_topic: "TBW102".into(),
            default_topic_queue_nums: 4,
            queue_id: 2,
            sys_flag: 1,
            born_timestamp: i64::MAX,
            flag: 7,
            properties: Some("TAGS\u{1}blue".into()),
            reconsume_times: Some(0),
            unit_mode: Some(true),
            max_reconsume_times: Some(16),
            batch: Some(false),
            ..SendMessageRequestHeaderV1::default()
        };
        let v2 = SendMessageRequestHeaderV2::from_v1(&v1);
        let mut v2_fields = HeaderMap::new();
        v2.encode_fields(&mut v2_fields);
        let decoded = SendMessageRequestHeaderV2::decode_fields(&v2_fields).unwrap();
        assert_eq!(decoded, v2);
        assert_eq!(v2_fields["a"], "producer");
        assert!(!v2_fields.contains_key("n"));
    }

    #[test]
    fn nullable_producer_group_is_omitted_from_send_headers() {
        let header = SendMessageRequestHeaderV1 {
            producer_group: None,
            broker_name: Some("short-broker".into()),
            namespace: Some("tenant".into()),
            namespaced: Some(true),
            logical: Some(true),
            oneway: Some(false),
            ..SendMessageRequestHeaderV1::default()
        };
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        assert!(!fields.contains_key("producerGroup"));
        assert_eq!(
            SendMessageRequestHeaderV1::decode_fields(&fields),
            Ok(header.clone())
        );

        let compact = SendMessageRequestHeaderV2::from_v1(&header);
        assert_eq!(compact.n.as_deref(), Some("short-broker"));
        assert_eq!(compact.namespace, None);
        assert_eq!(compact.namespaced, None);
        assert_eq!(compact.logical, None);
        assert_eq!(compact.broker_name, None);
        assert_eq!(compact.oneway, None);
        let mut compact_fields = HeaderMap::new();
        compact.encode_fields(&mut compact_fields);
        assert!(!compact_fields.contains_key("a"));
        assert_eq!(
            compact_fields.get("n").map(String::as_str),
            Some("short-broker")
        );
        assert!(!compact_fields.contains_key("bname"));
        assert!(!compact_fields.contains_key("ns"));
        assert!(!compact_fields.contains_key("nsd"));
        assert!(!compact_fields.contains_key("lo"));
        assert!(!compact_fields.contains_key("oway"));
        assert_eq!(
            SendMessageRequestHeaderV2::decode_fields(&compact_fields).unwrap(),
            compact
        );
    }

    #[test]
    fn pull_headers_round_trip_required_and_optional_fields() {
        let header = PullMessageRequestHeader {
            consumer_group: Some("workers".into()),
            topic: Some("orders".into()),
            queue_id: Some(-1),
            queue_offset: Some(i64::MIN),
            max_msg_nums: Some(32),
            sys_flag: Some(0),
            commit_offset: Some(0),
            suspend_timeout_millis: Some(15_000),
            sub_version: Some(i64::MAX),
            proxy_forward_client_id: Some("proxy".into()),
            broker_name: Some("broker-a".into()),
            ..PullMessageRequestHeader::default()
        };
        let mut fields = HeaderMap::new();
        header.encode_fields(&mut fields);
        assert_eq!(PullMessageRequestHeader::decode_fields(&fields), Ok(header));
        assert!(!fields.contains_key("subscription"));
        assert_eq!(fields["proxyFrowardClientId"], "proxy");
        assert_eq!(fields["bname"], "broker-a");
    }

    #[test]
    fn pull_request_header_does_not_export_java_absent_lite_topic_field() {
        let mut fields = HeaderMap::new();
        fields.insert("consumerGroup".into(), "workers".into());
        fields.insert("topic".into(), "orders".into());
        fields.insert("queueId".into(), "1".into());
        fields.insert("queueOffset".into(), "9".into());
        fields.insert("maxMsgNums".into(), "32".into());
        fields.insert("sysFlag".into(), "0".into());
        fields.insert("commitOffset".into(), "8".into());
        fields.insert("suspendTimeoutMillis".into(), "15000".into());
        fields.insert("subVersion".into(), "1".into());
        fields.insert("liteTopic".into(), "not-a-pull-header-field".into());

        let decoded = PullMessageRequestHeader::decode_fields(&fields).unwrap();
        let mut encoded = HeaderMap::new();
        decoded.encode_fields(&mut encoded);
        assert!(!encoded.contains_key("liteTopic"));
    }

    #[test]
    fn missing_and_invalid_response_header_values_name_the_field() {
        assert!(matches!(
            QueryConsumerOffsetResponseHeader::decode_fields(&HeaderMap::new()),
            Err(ProtocolError::MissingField("offset"))
        ));
        let decoded = QueryConsumerOffsetResponseHeader::decode_fields(&HeaderMap::from([(
            "offset".into(),
            "12".into(),
        )]))
        .unwrap();
        assert_eq!(decoded.offset, Some(12));
        assert!(matches!(
            QueryConsumerOffsetResponseHeader::decode_fields(&HeaderMap::from([(
                "offset".into(),
                "9223372036854775808".into()
            )])),
            Err(ProtocolError::InvalidField {
                field: "offset",
                ..
            })
        ));
    }

    #[test]
    fn recall_headers_match_java_field_names_and_required_response_msg_id() {
        let request = RecallMessageRequestHeader {
            producer_group: Some("recall-group".into()),
            topic: "orders".into(),
            recall_handle: "handle".into(),
            broker_name: Some("broker-a".into()),
            ..RecallMessageRequestHeader::default()
        };
        let mut fields = HeaderMap::new();
        request.encode_fields(&mut fields);
        assert_eq!(
            fields,
            HeaderMap::from([
                ("producerGroup".into(), "recall-group".into()),
                ("topic".into(), "orders".into()),
                ("recallHandle".into(), "handle".into()),
                ("bname".into(), "broker-a".into()),
            ])
        );
        assert_eq!(
            RecallMessageRequestHeader::decode_fields(&fields),
            Ok(request)
        );
        assert!(matches!(
            RecallMessageResponseHeader::decode_fields(&HeaderMap::new()),
            Err(ProtocolError::MissingField("msgId"))
        ));
        let decoded = RecallMessageResponseHeader::decode_fields(&HeaderMap::from([(
            "msgId".into(),
            "recalled-id".into(),
        )]))
        .unwrap();
        assert_eq!(decoded.msg_id, "recalled-id");
    }
}
