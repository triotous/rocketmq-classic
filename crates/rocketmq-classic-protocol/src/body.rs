use crate::codes::JavaEnumLookupError;
use rocketmq_classic_common::{message::MessageQueue, serde_fastjson};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

macro_rules! impl_java_enum_api {
    ($enum_type:ident, $java_class:literal, $count:literal, $($variant:ident => $name:literal = $ordinal:literal),+ $(,)?) => {
        impl $enum_type {
            pub const fn values() -> [Self; $count] {
                [$(Self::$variant),+]
            }

            pub fn value_of_name(name: Option<&str>) -> Result<Self, JavaEnumLookupError> {
                match name {
                    None => Err(JavaEnumLookupError::NullName),
                    $(Some($name) => Ok(Self::$variant),)+
                    Some(other) => Err(JavaEnumLookupError::UnknownName {
                        enum_class: $java_class,
                        name: other.to_owned(),
                    }),
                }
            }

            pub const fn name(&self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)+
                }
            }

            pub const fn ordinal(&self) -> i32 {
                match self {
                    $(Self::$variant => $ordinal,)+
                }
            }

            pub const fn java_compare_to(&self, other: &Self) -> i32 {
                self.ordinal() - other.ordinal()
            }
        }

        impl std::fmt::Display for $enum_type {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.name())
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CheckClientRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_data: Option<super::heartbeat::SubscriptionData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageRequestMode {
    #[default]
    Pull,
    Pop,
}

impl_java_enum_api!(
    MessageRequestMode,
    "org.apache.rocketmq.common.message.MessageRequestMode",
    2,
    Pull => "PULL" = 0,
    Pop => "POP" = 1,
);

impl super::heartbeat::JavaOrdinalEnum for MessageRequestMode {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::Pull),
            1 => Some(Self::Pop),
            _ => None,
        }
    }
}

fn default_assignment_mode() -> Option<MessageRequestMode> {
    Some(MessageRequestMode::Pull)
}

fn deserialize_optional_assignment_set<'de, D>(
    deserializer: D,
) -> Result<Option<std::collections::HashSet<Option<MessageQueueAssignment>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let raw = raw.get().trim();
    if raw == "null" {
        return Ok(None);
    }
    let values = if raw.starts_with('[') {
        serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(raw)
            .map_err(de::Error::custom)?
    } else if raw.starts_with('{') {
        vec![
            serde_json::from_str::<Box<serde_json::value::RawValue>>(raw)
                .map_err(de::Error::custom)?,
        ]
    } else {
        return Err(de::Error::custom(
            "expected a JSON assignment array, object, or null",
        ));
    };
    let mut assignments = std::collections::HashSet::with_capacity(values.len());
    for value in values {
        let assignment = if value.get().trim() == "null" {
            None
        } else {
            Some(serde_json::from_str(value.get()).map_err(de::Error::custom)?)
        };
        assignments.insert(assignment);
    }
    Ok(Some(assignments))
}

/// Java's default JSON writer omits null-valued entries in this map.
fn serialize_assignment_attachments<S>(
    attachments: &Option<std::collections::BTreeMap<String, Option<String>>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use serde::ser::SerializeMap;

    let Some(attachments) = attachments else {
        return serializer.serialize_none();
    };
    let non_null_count = attachments.values().filter(|value| value.is_some()).count();
    let mut map = serializer.serialize_map(Some(non_null_count))?;
    for (key, value) in attachments {
        if let Some(value) = value {
            map.serialize_entry(key, value)?;
        }
    }
    map.end()
}

struct FastjsonI32(i32);

impl<'de> Deserialize<'de> for FastjsonI32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_fastjson::deserialize_i32(deserializer).map(Self)
    }
}

fn deserialize_assignment_mode<'de, D>(
    deserializer: D,
) -> Result<Option<MessageRequestMode>, D::Error>
where
    D: Deserializer<'de>,
{
    fn from_ordinal<E: de::Error>(ordinal: i64) -> Result<Option<MessageRequestMode>, E> {
        match ordinal {
            0 => Ok(Some(MessageRequestMode::Pull)),
            1 => Ok(Some(MessageRequestMode::Pop)),
            _ => Err(E::custom(format!(
                "Fastjson MessageRequestMode ordinal {ordinal} is out of range"
            ))),
        }
    }

    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let raw = raw.get().trim();
    if raw == "null" {
        return Ok(None);
    }
    if raw.starts_with('"') {
        let value = serde_json::from_str::<String>(raw).map_err(de::Error::custom)?;
        return match value.as_str() {
            value if value.eq_ignore_ascii_case("PULL") => Ok(Some(MessageRequestMode::Pull)),
            value if value.eq_ignore_ascii_case("POP") => Ok(Some(MessageRequestMode::Pop)),
            _ => match value.parse::<i64>() {
                Ok(ordinal) => from_ordinal::<D::Error>(ordinal),
                Err(_) => Ok(None),
            },
        };
    }
    if raw == "true" || raw == "false" || raw.starts_with('{') || raw.starts_with('[') {
        return Err(de::Error::custom(
            "Fastjson cannot coerce this value to MessageRequestMode",
        ));
    }
    let FastjsonI32(ordinal) = serde_json::from_str(raw).map_err(de::Error::custom)?;
    from_ordinal::<D::Error>(ordinal as i64)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LiteSubscriptionAction {
    PartialAdd,
    PartialRemove,
    CompleteAdd,
    CompleteRemove,
}

impl_java_enum_api!(
    LiteSubscriptionAction,
    "org.apache.rocketmq.common.lite.LiteSubscriptionAction",
    4,
    PartialAdd => "PARTIAL_ADD" = 0,
    PartialRemove => "PARTIAL_REMOVE" = 1,
    CompleteAdd => "COMPLETE_ADD" = 2,
    CompleteRemove => "COMPLETE_REMOVE" = 3,
);

impl super::heartbeat::JavaOrdinalEnum for LiteSubscriptionAction {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::PartialAdd),
            1 => Some(Self::PartialRemove),
            2 => Some(Self::CompleteAdd),
            3 => Some(Self::CompleteRemove),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OffsetOptionType {
    Policy,
    Offset,
    TailN,
    Timestamp,
}

impl_java_enum_api!(
    OffsetOptionType,
    "org.apache.rocketmq.common.lite.OffsetOption.Type",
    4,
    Policy => "POLICY" = 0,
    Offset => "OFFSET" = 1,
    TailN => "TAIL_N" = 2,
    Timestamp => "TIMESTAMP" = 3,
);

impl super::heartbeat::JavaOrdinalEnum for OffsetOptionType {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::Policy),
            1 => Some(Self::Offset),
            2 => Some(Self::TailN),
            3 => Some(Self::Timestamp),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct OffsetOption {
    #[serde(
        rename = "type",
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub option_type: Option<OffsetOptionType>,
    #[serde(default)]
    pub value: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LiteSubscriptionDto {
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub action: Option<LiteSubscriptionAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lite_topic_set: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_option: Option<OffsetOption>,
    #[serde(default)]
    pub version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LiteSubscriptionCtlRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_set: Option<Vec<LiteSubscriptionDto>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LiteSubscription {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lite_topic_set: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryAssignmentRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy_name: Option<String>,
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_model: Option<super::heartbeat::MessageModel>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryAssignmentRequestBodyFields {
    topic: Option<String>,
    consumer_group: Option<String>,
    client_id: Option<String>,
    strategy_name: Option<String>,
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    message_model: Option<super::heartbeat::MessageModel>,
}

impl<'de> Deserialize<'de> for QueryAssignmentRequestBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: QueryAssignmentRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            topic: fields.topic,
            consumer_group: fields.consumer_group,
            client_id: fields.client_id,
            strategy_name: fields.strategy_name,
            message_model: fields.message_model,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageQueueAssignment {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_queue: Option<MessageQueue>,
    /// Java defaults this field to PULL but Fastjson may set it to null.
    #[serde(
        default = "default_assignment_mode",
        deserialize_with = "deserialize_assignment_mode",
        skip_serializing_if = "Option::is_none"
    )]
    pub mode: Option<MessageRequestMode>,
    /// Java retains null values on decode and omits them during default JSON encoding.
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_string_map",
        serialize_with = "serialize_assignment_attachments",
        skip_serializing_if = "Option::is_none"
    )]
    pub attachments: Option<std::collections::BTreeMap<String, Option<String>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageQueueAssignmentFields {
    message_queue: Option<MessageQueue>,
    #[serde(
        default = "default_assignment_mode",
        deserialize_with = "deserialize_assignment_mode"
    )]
    mode: Option<MessageRequestMode>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_string_map"
    )]
    attachments: Option<std::collections::BTreeMap<String, Option<String>>>,
}

impl<'de> Deserialize<'de> for MessageQueueAssignment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: MessageQueueAssignmentFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            message_queue: fields.message_queue,
            mode: fields.mode,
            attachments: fields.attachments,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MessageQueueAssignmentEqualsError {
    #[error("Java MessageQueueAssignment.equals dereferences a null messageQueue")]
    NullMessageQueue,
}

impl MessageQueueAssignment {
    pub fn message_queue(&self) -> Option<&MessageQueue> {
        self.message_queue.as_ref()
    }

    pub fn set_message_queue(&mut self, message_queue: Option<MessageQueue>) {
        self.message_queue = message_queue;
    }

    pub fn mode(&self) -> Option<MessageRequestMode> {
        self.mode
    }

    pub fn set_mode(&mut self, mode: Option<MessageRequestMode>) {
        self.mode = mode;
    }

    pub fn attachments(&self) -> Option<&std::collections::BTreeMap<String, Option<String>>> {
        self.attachments.as_ref()
    }

    pub fn attachments_mut(
        &mut self,
    ) -> Option<&mut std::collections::BTreeMap<String, Option<String>>> {
        self.attachments.as_mut()
    }

    pub fn set_attachments(
        &mut self,
        attachments: Option<std::collections::BTreeMap<String, Option<String>>>,
    ) {
        self.attachments = attachments;
    }

    /// Matches Java's public `equals` method, including its null-queue failure.
    pub fn java_equals(
        &self,
        other: Option<&Self>,
    ) -> Result<bool, MessageQueueAssignmentEqualsError> {
        let Some(other) = other else {
            return Ok(false);
        };
        if std::ptr::eq(self, other) {
            return Ok(true);
        }
        let Some(message_queue) = self.message_queue.as_ref() else {
            return Err(MessageQueueAssignmentEqualsError::NullMessageQueue);
        };
        Ok(other
            .message_queue
            .as_ref()
            .is_some_and(|other_queue| message_queue == other_queue))
    }

    pub fn java_to_string(&self) -> String {
        self.to_string()
    }
}

impl std::fmt::Display for MessageQueueAssignment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message_queue = self
            .message_queue
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "null".into());
        let mode = match self.mode {
            Some(MessageRequestMode::Pull) => "PULL",
            Some(MessageRequestMode::Pop) => "POP",
            None => "null",
        };
        write!(
            formatter,
            "MessageQueueAssignment [MessageQueue={message_queue}, Mode={mode}]"
        )
    }
}

impl Default for MessageQueueAssignment {
    fn default() -> Self {
        Self {
            message_queue: None,
            mode: default_assignment_mode(),
            attachments: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryAssignmentResponseBody {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_assignment_set",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_queue_assignments:
        Option<std::collections::HashSet<Option<MessageQueueAssignment>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryAssignmentResponseBodyFields {
    #[serde(default, deserialize_with = "deserialize_optional_assignment_set")]
    message_queue_assignments: Option<std::collections::HashSet<Option<MessageQueueAssignment>>>,
}

impl<'de> Deserialize<'de> for QueryAssignmentResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: QueryAssignmentResponseBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            message_queue_assignments: fields.message_queue_assignments,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SetMessageRequestModeRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    #[serde(
        default = "default_assignment_mode",
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub mode: Option<MessageRequestMode>,
    #[serde(default)]
    pub pop_share_queue_num: i32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SetMessageRequestModeRequestBodyFields {
    topic: Option<String>,
    consumer_group: Option<String>,
    #[serde(
        default = "default_assignment_mode",
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    mode: Option<MessageRequestMode>,
    #[serde(default)]
    pop_share_queue_num: i32,
}

impl<'de> Deserialize<'de> for SetMessageRequestModeRequestBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: SetMessageRequestModeRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            topic: fields.topic,
            consumer_group: fields.consumer_group,
            mode: fields.mode,
            pop_share_queue_num: fields.pop_share_queue_num,
        })
    }
}

impl Default for SetMessageRequestModeRequestBody {
    fn default() -> Self {
        Self {
            topic: None,
            consumer_group: None,
            mode: default_assignment_mode(),
            pop_share_queue_num: 0,
        }
    }
}

mod bitset_base64 {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match bytes {
            Some(bytes) => serializer.serialize_str(&STANDARD.encode(bytes)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<String>::deserialize(deserializer)?;
        value
            .map(|text| STANDARD.decode(text).map_err(serde::de::Error::custom))
            .transpose()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BatchAckMessageRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acks: Option<Vec<BatchAck>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchAck {
    #[serde(
        rename = "c",
        alias = "consumerGroup",
        skip_serializing_if = "Option::is_none"
    )]
    pub consumer_group: Option<String>,
    #[serde(rename = "t", alias = "topic", skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(rename = "r", alias = "retry", skip_serializing_if = "Option::is_none")]
    pub retry: Option<String>,
    #[serde(rename = "so", alias = "startOffset")]
    pub start_offset: i64,
    #[serde(rename = "q", alias = "queueId")]
    pub queue_id: i32,
    #[serde(rename = "rq", alias = "reviveQueueId")]
    pub revive_queue_id: i32,
    #[serde(rename = "pt", alias = "popTime")]
    pub pop_time: i64,
    #[serde(rename = "it", alias = "invisibleTime")]
    pub invisible_time: i64,
    #[serde(rename = "b", alias = "bitSet", with = "bitset_base64")]
    pub bit_set: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MessageRequestModeSerializeWrapper {
    pub message_request_mode_map: Option<
        std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<String, SetMessageRequestModeRequestBody>,
        >,
    >,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UnregisterClientRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LockBatchRequestBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default)]
    pub only_this_broker: bool,
    #[serde(default)]
    pub mq_set: Vec<MessageQueue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UnlockBatchRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default)]
    pub only_this_broker: bool,
    #[serde(default)]
    pub mq_set: Vec<MessageQueue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LockBatchResponseBody {
    #[serde(default, rename = "lockOKMQSet")]
    pub lock_okmq_set: Vec<MessageQueue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UnlockBatchResponseBody {
    #[serde(default)]
    pub unlock_okmq_set: Vec<MessageQueue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerListByGroupResponseBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_id_list: Option<Vec<Option<String>>>,
}

impl<'de> Deserialize<'de> for ConsumerListByGroupResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ConsumerListVisitor;

        impl<'de> de::Visitor<'de> for ConsumerListVisitor {
            type Value = ConsumerListByGroupResponseBody;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a consumer-list response object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut raw_consumer_id_list = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key == "consumerIdList" {
                        raw_consumer_id_list =
                            Some(map.next_value::<Box<serde_json::value::RawValue>>()?);
                    } else {
                        map.next_value::<de::IgnoredAny>()?;
                    }
                }

                let consumer_id_list = match raw_consumer_id_list {
                    Some(raw) => {
                        // Fastjson retains the last duplicate property before coercing its value.
                        let mut raw_deserializer = serde_json::Deserializer::from_str(raw.get());
                        let value = serde_fastjson::deserialize_optional_nullable_string_list(
                            &mut raw_deserializer,
                        )
                        .map_err(de::Error::custom)?;
                        raw_deserializer.end().map_err(de::Error::custom)?;
                        value
                    }
                    None => None,
                };

                Ok(ConsumerListByGroupResponseBody { consumer_id_list })
            }
        }

        deserializer.deserialize_map(ConsumerListVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetConsumerStatusBody {
    #[serde(default, with = "message_queue_offset_table")]
    pub message_queue_table: Vec<(MessageQueue, i64)>,
    #[serde(default, with = "nested_message_queue_offset_table")]
    pub consumer_table: std::collections::BTreeMap<String, Vec<(MessageQueue, i64)>>,
}

impl GetConsumerStatusBody {
    pub fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let queue_table = encode_java_queue_table_text(&self.message_queue_table)?;
        let mut consumer_entries = Vec::new();
        for (consumer, entries) in &self.consumer_table {
            consumer_entries.push(format!(
                "{}:{}",
                serde_json::to_string(consumer)?,
                encode_java_queue_table_text(entries)?
            ));
        }
        let consumer_table = format!("{{{}}}", consumer_entries.join(","));
        Ok(format!(
            "{{\"consumerTable\":{},\"messageQueueTable\":{}}}",
            consumer_table, queue_table
        )
        .into_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResetOffsetBody {
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "optional_message_queue_offset_table"
    )]
    pub offset_table: Option<Vec<(MessageQueue, i64)>>,
}

impl ResetOffsetBody {
    pub fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let entries = self.offset_table.as_deref().unwrap_or_default();
        let table = encode_java_queue_table_text(entries)?;
        Ok(format!("{{\"offsetTable\":{table}}}").into_bytes())
    }
}

fn encode_java_queue_table_text(
    entries: &[(MessageQueue, i64)],
) -> Result<String, serde_json::Error> {
    let mut parts = Vec::with_capacity(entries.len());
    for (queue, offset) in entries {
        let queue_key = format!(
            "{{\"brokerName\":{},\"queueId\":{},\"topic\":{}}}",
            serde_json::to_string(&queue.broker_name())?,
            serde_json::to_string(&queue.queue_id())?,
            serde_json::to_string(&queue.topic())?
        );
        parts.push(format!("{queue_key}:{}", serde_json::to_string(offset)?));
    }
    Ok(format!("{{{}}}", parts.join(",")))
}

type QueueOffsetEntries = Vec<(MessageQueue, i64)>;
type ConsumerQueueOffsets = std::collections::BTreeMap<String, QueueOffsetEntries>;

mod message_queue_offset_table {
    use super::encode_table;
    use super::{MessageQueue, QueueOffsetEntries};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(entries: &QueueOffsetEntries, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        encode_table(entries)
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<QueueOffsetEntries, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = std::collections::BTreeMap::<String, i64>::deserialize(deserializer)?;
        encoded
            .into_iter()
            .map(|(key, offset)| {
                let queue: MessageQueue =
                    serde_json::from_str(&key).map_err(serde::de::Error::custom)?;
                Ok((queue, offset))
            })
            .collect()
    }
}

fn encode_table(
    entries: &[(MessageQueue, i64)],
) -> Result<std::collections::BTreeMap<String, i64>, serde_json::Error> {
    entries
        .iter()
        .map(|(queue, offset)| Ok((serde_json::to_string(queue)?, *offset)))
        .collect()
}

mod nested_message_queue_offset_table {
    use super::ConsumerQueueOffsets;
    use super::encode_table;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(entries: &ConsumerQueueOffsets, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = entries
            .iter()
            .map(|(key, values)| {
                Ok((
                    key,
                    encode_table(values).map_err(serde::ser::Error::custom)?,
                ))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>, S::Error>>()?;
        encoded.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ConsumerQueueOffsets, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw =
            std::collections::BTreeMap::<String, serde_json::Value>::deserialize(deserializer)?;
        raw.into_iter()
            .map(|(key, value)| {
                let entries = super::decode_table(value).map_err(serde::de::Error::custom)?;
                Ok((key, entries))
            })
            .collect()
    }
}

fn decode_table(value: serde_json::Value) -> Result<Vec<(MessageQueue, i64)>, serde_json::Error> {
    value
        .as_object()
        .ok_or_else(|| serde::de::Error::custom("expected MessageQueue-keyed object"))?
        .iter()
        .map(|(key, value)| {
            let queue = serde_json::from_str(key)?;
            let offset = serde_json::from_value(value.clone())?;
            Ok((queue, offset))
        })
        .collect()
}

mod optional_message_queue_offset_table {
    use super::*;
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S>(
        entries: &Option<Vec<(MessageQueue, i64)>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match entries {
            Some(entries) => super::message_queue_offset_table::serialize(entries, serializer),
            None => serializer.serialize_none(),
        }
    }
    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<Vec<(MessageQueue, i64)>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<serde_json::Value>::deserialize(deserializer)?
            .map(|value| super::decode_table(value).map_err(serde::de::Error::custom))
            .transpose()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeMessageDirectlyResult {
    #[serde(default)]
    pub order: bool,
    #[serde(default = "default_auto_commit")]
    pub auto_commit: bool,
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub consume_result: Option<ConsumeMessageResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remark: Option<String>,
    #[serde(default)]
    pub spent_time_mills: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConsumeMessageResult {
    CrSuccess,
    CrLater,
    CrRollback,
    CrCommit,
    CrThrowException,
    CrReturnNull,
}

impl_java_enum_api!(
    ConsumeMessageResult,
    "org.apache.rocketmq.remoting.protocol.body.CMResult",
    6,
    CrSuccess => "CR_SUCCESS" = 0,
    CrLater => "CR_LATER" = 1,
    CrRollback => "CR_ROLLBACK" = 2,
    CrCommit => "CR_COMMIT" = 3,
    CrThrowException => "CR_THROW_EXCEPTION" = 4,
    CrReturnNull => "CR_RETURN_NULL" = 5,
);

impl super::heartbeat::JavaOrdinalEnum for ConsumeMessageResult {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::CrSuccess),
            1 => Some(Self::CrLater),
            2 => Some(Self::CrRollback),
            3 => Some(Self::CrCommit),
            4 => Some(Self::CrThrowException),
            5 => Some(Self::CrReturnNull),
            _ => None,
        }
    }
}

fn default_auto_commit() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UnregisterClientBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unregister_body_omits_absent_groups_but_keeps_empty_values() {
        let body = UnregisterClientRequestBody {
            client_id: Some("client".into()),
            producer_group: Some(String::new()),
            consumer_group: None,
        };
        let value = serde_json::to_value(body).unwrap();
        assert_eq!(value, json!({"clientId":"client", "producerGroup":""}));
    }

    #[test]
    fn lock_and_unlock_bodies_preserve_collection_and_boolean_semantics() {
        let lock = LockBatchRequestBody {
            consumer_group: Some("workers".into()),
            client_id: Some("client".into()),
            only_this_broker: false,
            mq_set: vec![MessageQueue::new("orders", "broker-a", 0)],
        };
        let unlock = UnlockBatchRequestBody {
            consumer_group: lock.consumer_group.clone(),
            client_id: lock.client_id.clone(),
            mq_set: lock.mq_set.clone(),
            only_this_broker: true,
        };
        assert_eq!(
            serde_json::from_value::<LockBatchRequestBody>(serde_json::to_value(&lock).unwrap())
                .unwrap()
                .mq_set,
            lock.mq_set
        );
        let value = serde_json::to_value(unlock).unwrap();
        assert_eq!(value["onlyThisBroker"], true);
        assert_eq!(value["mqSet"][0]["queueId"], 0);
    }

    #[test]
    fn consumer_list_body_keeps_empty_and_nonempty_lists_distinct() {
        assert_eq!(
            serde_json::to_value(ConsumerListByGroupResponseBody::default()).unwrap(),
            json!({})
        );
        let populated = serde_json::from_value::<ConsumerListByGroupResponseBody>(json!({
            "consumerIdList": ["client"]
        }))
        .unwrap();
        assert_eq!(
            populated.consumer_id_list,
            Some(vec![Some("client".into())])
        );
    }

    #[test]
    fn lock_request_uses_java_message_queue_field_names() {
        let body = LockBatchRequestBody {
            consumer_group: Some("workers".into()),
            client_id: Some("client".into()),
            mq_set: vec![MessageQueue::new("orders", "broker-a", 0)],
            ..LockBatchRequestBody::default()
        };
        let value = serde_json::to_value(body).unwrap();
        assert_eq!(value["mqSet"][0]["brokerName"], "broker-a");
        assert_eq!(value["mqSet"][0]["queueId"], 0);
    }

    #[test]
    fn runtime_inbound_bodies_keep_map_and_optional_shapes() {
        let queue = MessageQueue::new("orders", "broker-a", 1);
        let reset = ResetOffsetBody {
            offset_table: Some(vec![(queue.clone(), 7)]),
        };
        let value = serde_json::to_value(reset).unwrap();
        let offset_table = value["offsetTable"].as_object().unwrap();
        assert_eq!(offset_table.values().next().unwrap(), 7);
        let key: serde_json::Value =
            serde_json::from_str(offset_table.keys().next().unwrap()).unwrap();
        assert_eq!(key["queueId"], 1);

        let status = GetConsumerStatusBody {
            message_queue_table: vec![(queue.clone(), 7)],
            consumer_table: std::collections::BTreeMap::from([(
                "consumer-a".into(),
                vec![(queue, 8)],
            )]),
        };
        let value = serde_json::to_value(status).unwrap();
        let consumer_table = value["consumerTable"]["consumer-a"].as_object().unwrap();
        assert_eq!(consumer_table.values().next().unwrap(), 8);
        let queue_key: serde_json::Value =
            serde_json::from_str(consumer_table.keys().next().unwrap()).unwrap();
        assert_eq!(queue_key["queueId"], 1);

        let direct: ConsumeMessageDirectlyResult =
            serde_json::from_value(serde_json::json!({"order":false, "spentTimeMills":0})).unwrap();
        let value = serde_json::to_value(direct).unwrap();
        assert_eq!(value["autoCommit"], true);
        assert!(value.get("consumeResult").is_none());
        let direct = ConsumeMessageDirectlyResult {
            consume_result: Some(ConsumeMessageResult::CrSuccess),
            ..ConsumeMessageDirectlyResult::default()
        };
        assert_eq!(
            serde_json::to_value(direct).unwrap()["consumeResult"],
            "CR_SUCCESS"
        );
    }

    #[test]
    fn assignment_request_mode_lite_and_batch_ack_match_java_json_shapes() {
        let assignment = MessageQueueAssignment {
            message_queue: Some(MessageQueue::new("orders", "broker-a", 1)),
            mode: Some(MessageRequestMode::Pop),
            attachments: Some(std::collections::BTreeMap::from([(
                "zone".into(),
                Some("z1".into()),
            )])),
        };
        let value = serde_json::to_value(assignment).unwrap();
        assert_eq!(value["mode"], "POP");
        assert_eq!(value["messageQueue"]["brokerName"], "broker-a");

        let lite = LiteSubscriptionDto {
            action: Some(LiteSubscriptionAction::CompleteAdd),
            client_id: Some("client".into()),
            group: Some("workers".into()),
            topic: Some("orders".into()),
            lite_topic_set: Some(vec!["lite-orders".into()]),
            offset_option: Some(OffsetOption {
                option_type: Some(OffsetOptionType::Timestamp),
                value: 99,
            }),
            version: 2,
        };
        let value = serde_json::to_value(lite).unwrap();
        assert_eq!(value["action"], "COMPLETE_ADD");
        assert_eq!(value["offsetOption"]["type"], "TIMESTAMP");

        let body = BatchAckMessageRequestBody {
            broker_name: Some("broker-a".into()),
            acks: Some(vec![BatchAck {
                consumer_group: Some("workers".into()),
                topic: Some("orders".into()),
                retry: Some("0".into()),
                start_offset: 100,
                queue_id: 1,
                revive_queue_id: 0,
                pop_time: 10,
                invisible_time: 20,
                bit_set: Some(vec![0b0000_0101]),
            }]),
        };
        let value = serde_json::to_value(&body).unwrap();
        let ack = &value["acks"][0];
        assert_eq!(ack["c"], "workers");
        assert_eq!(ack["t"], "orders");
        assert_eq!(ack["b"], "BQ==");
        let round_trip: BatchAckMessageRequestBody = serde_json::from_value(value).unwrap();
        assert_eq!(round_trip.acks.unwrap()[0].bit_set, Some(vec![0b0000_0101]));
    }

    #[test]
    fn assignment_mode_inputs_match_invoked_java_fastjson() {
        fn normalize(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        normalize(value);
                    }
                    values.sort_by_key(|value| serde_json::to_string(value).unwrap());
                }
                serde_json::Value::Object(fields) => {
                    for value in fields.values_mut() {
                        normalize(value);
                    }
                }
                _ => {}
            }
        }

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["assignmentModeCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            20,
            "Java oracle includes decimal ordinal boundaries"
        );
        assert!(
            cases
                .iter()
                .any(|case| case["name"] == "fractional-ordinal-precision-boundary")
        );
        assert!(
            cases
                .iter()
                .any(|case| case["name"] == "decimal-ordinal-wraps-to-zero")
        );
        assert!(
            cases
                .iter()
                .any(|case| case["name"] == "integer-ordinal-wrap-boundary")
        );
        for case in cases {
            let decoded = serde_json::from_str::<QueryAssignmentResponseBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                let assignment = decoded
                    .message_queue_assignments
                    .as_ref()
                    .and_then(|assignments| assignments.iter().next())
                    .and_then(Option::as_ref)
                    .unwrap();
                let mode = assignment
                    .mode
                    .map(serde_json::to_value)
                    .transpose()
                    .unwrap()
                    .unwrap_or(serde_json::Value::Null);
                assert_eq!(mode, case["mode"], "{}", case["name"]);

                let mut actual = serde_json::to_value(decoded).unwrap();
                let mut expected = case["encoded"].clone();
                normalize(&mut actual);
                normalize(&mut expected);
                assert_eq!(actual, expected, "{}", case["name"]);
            }
        }
        assert_eq!(
            MessageQueueAssignment::default().mode,
            Some(MessageRequestMode::Pull)
        );
    }

    #[test]
    fn assignment_attachment_inputs_match_invoked_java_fastjson() {
        fn normalize(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        normalize(value);
                    }
                    values.sort_by_key(|value| serde_json::to_string(value).unwrap());
                }
                serde_json::Value::Object(fields) => {
                    for value in fields.values_mut() {
                        normalize(value);
                    }
                }
                _ => {}
            }
        }

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["assignmentAttachmentCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            7,
            "Java oracle includes numeric string coercions"
        );
        assert!(
            cases
                .iter()
                .any(|case| case["name"] == "numeric-string-values")
        );
        for case in cases {
            let decoded = serde_json::from_str::<QueryAssignmentResponseBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                let assignment = decoded
                    .message_queue_assignments
                    .as_ref()
                    .and_then(|assignments| assignments.iter().next())
                    .and_then(Option::as_ref)
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(&assignment.attachments).unwrap(),
                    case["attachments"],
                    "{}",
                    case["name"]
                );

                let mut actual = serde_json::to_value(decoded).unwrap();
                let mut expected = case["encoded"].clone();
                normalize(&mut actual);
                normalize(&mut expected);
                assert_eq!(actual, expected, "{}", case["name"]);
            }
        }
    }

    #[test]
    fn consumer_list_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["consumerListCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            16,
            "Java oracle includes high-precision numeric strings and duplicate-field last-wins input"
        );
        assert!(
            cases
                .iter()
                .any(|case| case["name"] == "high-precision-numeric-strings")
        );
        for case in cases {
            let decoded = serde_json::from_str::<ConsumerListByGroupResponseBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(&decoded.consumer_id_list).unwrap(),
                    case["ids"],
                    "{}",
                    case["name"]
                );
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn assignment_and_request_mode_json_preserve_optional_fields_and_defaults() {
        let query: QueryAssignmentRequestBody = serde_json::from_value(serde_json::json!({
            "topic":"orders", "consumerGroup":"workers", "clientId":"client",
            "strategyName":"AllocateMessageQueueAveragely", "messageModel":"CLUSTERING"
        }))
        .unwrap();
        assert_eq!(query.topic.as_deref(), Some("orders"));

        let mode: SetMessageRequestModeRequestBody = serde_json::from_value(serde_json::json!({
            "topic":"orders", "consumerGroup":"workers", "popShareQueueNum":0
        }))
        .unwrap();
        assert_eq!(mode.mode, Some(MessageRequestMode::Pull));
        assert_eq!(serde_json::to_value(mode).unwrap()["mode"], "PULL");

        let absent: LiteSubscriptionCtlRequestBody =
            serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(serde_json::to_value(absent).unwrap(), serde_json::json!({}));
    }

    #[test]
    fn assignment_body_inputs_match_invoked_java_fastjson() {
        fn normalize(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        normalize(value);
                    }
                    values.sort_by_key(|value| serde_json::to_string(value).unwrap());
                }
                serde_json::Value::Object(fields) => {
                    for value in fields.values_mut() {
                        normalize(value);
                    }
                }
                _ => {}
            }
        }

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["assignmentBodyCases"].as_array().unwrap();
        assert_eq!(cases.len(), 15);
        for case in cases {
            let decoded = serde_json::from_str::<QueryAssignmentResponseBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                let actual_assignments = decoded.message_queue_assignments.as_ref().map(|items| {
                    items
                        .iter()
                        .map(|item| match item {
                            Some(item) => serde_json::json!({
                                "attachments": &item.attachments,
                                "messageQueue": &item.message_queue,
                                "mode": item.mode,
                            }),
                            None => serde_json::Value::Null,
                        })
                        .collect::<Vec<_>>()
                });
                let mut actual_assignments = serde_json::to_value(actual_assignments).unwrap();
                let mut expected_assignments = case["assignments"].clone();
                normalize(&mut actual_assignments);
                normalize(&mut expected_assignments);
                assert_eq!(actual_assignments, expected_assignments, "{}", case["name"]);

                let mut actual = serde_json::to_value(decoded).unwrap();
                let mut expected = case["encoded"].clone();
                normalize(&mut actual);
                normalize(&mut expected);
                assert_eq!(actual, expected, "{}", case["name"]);
            }
        }
    }

    #[test]
    fn lite_subscription_enum_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionEnumCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            23,
            "Java oracle includes lite enum names and ordinals"
        );
        for required in [
            "action-duplicate-last-wins",
            "client-id-duplicate-last-wins",
            "group-duplicate-last-wins",
            "topic-duplicate-last-wins",
            "lite-topic-set-duplicate-last-wins",
            "offset-option-duplicate-last-wins",
            "version-duplicate-last-wins",
            "subscription-set-duplicate-last-wins",
            "offset-type-duplicate-last-wins",
            "offset-value-duplicate-last-wins",
        ] {
            assert!(cases.iter().any(|case| case["name"] == required), "missing {required}");
        }
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(raw_input);
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn query_assignment_request_enum_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["queryAssignmentEnumCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            10,
            "Java oracle includes assignment request enum and duplicate-field inputs"
        );
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let decoded = serde_json::from_str::<QueryAssignmentRequestBody>(raw_input);
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn set_request_mode_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["setRequestModeCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            14,
            "Java oracle includes request mode defaults, coercions, and duplicate fields"
        );
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let decoded = serde_json::from_str::<SetMessageRequestModeRequestBody>(raw_input);
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn consume_direct_result_enum_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["consumeDirectResultCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            19,
            "Java oracle includes every consume result enum value"
        );
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let decoded = serde_json::from_str::<ConsumeMessageDirectlyResult>(raw_input);
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(
                decoded.is_ok(),
                java_accepts,
                "{}: {decoded:?}",
                case["name"]
            );
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn assignment_set_hash_collision_oracle_records_the_java_boundary() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let collision = &fixture["assignmentHashCollisionCases"];

        // MessageQueueAssignment.equals ignores mode and attachments, while its Java
        // hash includes both. The fixture constructs an equal-hash pair using each
        // JVM's enum identity hash; that process-local collision is not portable to
        // a Rust HashSet with the lawful all-fields Eq/Hash contract.
        assert_eq!(collision["sameQueueEquals"], true);
        assert_eq!(collision["equalHash"], true);
        assert_eq!(collision["setSize"], 1);
        assert_eq!(collision["collisionDeduplicates"], true);
    }

    #[test]
    fn assignment_methods_match_invoked_java_public_api() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        for case in fixture["assignmentMethodCases"].as_array().unwrap() {
            let mut assignment = MessageQueueAssignment::default();
            assignment.set_message_queue(
                serde_json::from_value::<Option<MessageQueue>>(case["messageQueue"].clone())
                    .unwrap(),
            );
            assignment.set_mode(
                serde_json::from_value::<Option<MessageRequestMode>>(case["mode"].clone()).unwrap(),
            );
            assignment.set_attachments(
                serde_json::from_value::<Option<
                    std::collections::BTreeMap<String, Option<String>>,
                >>(case["attachments"].clone())
                .unwrap(),
            );

            assert_eq!(
                assignment.message_queue(),
                assignment.message_queue.as_ref(),
                "{}",
                case["name"]
            );
            assert_eq!(assignment.mode(), assignment.mode, "{}", case["name"]);
            assert_eq!(
                assignment.attachments(),
                assignment.attachments.as_ref(),
                "{}",
                case["name"]
            );
            assert_eq!(
                assignment.java_to_string(),
                case["text"].as_str().unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(assignment.to_string(), case["text"].as_str().unwrap());
            assert_eq!(assignment.java_equals(None), Ok(false));
            assert_eq!(assignment.java_equals(Some(&assignment)), Ok(true));

            if case["name"] == "default" {
                assert_eq!(
                    assignment.java_equals(Some(&MessageQueueAssignment::default())),
                    Err(MessageQueueAssignmentEqualsError::NullMessageQueue)
                );
            } else {
                let mut same_queue = assignment.clone();
                same_queue.set_mode(Some(MessageRequestMode::Pull));
                same_queue.set_attachments(None);
                assert_eq!(
                    assignment.java_equals(Some(&same_queue)),
                    Ok(case["equalsSameQueueDifferentMetadata"].as_bool().unwrap())
                );

                let mut different_queue = same_queue.clone();
                different_queue.set_message_queue(Some(MessageQueue::new("orders", "broker", 3)));
                assert_eq!(
                    assignment.java_equals(Some(&different_queue)),
                    Ok(case["equalsDifferentQueue"].as_bool().unwrap())
                );
            }
        }
    }

    #[test]
    fn message_request_mode_name_matches_invoked_java_public_api() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        for case in fixture["messageRequestModeMethodCases"].as_array().unwrap() {
            let mode: MessageRequestMode =
                serde_json::from_value(case["constant"].clone()).unwrap();
            assert_eq!(mode.name(), case["getName"].as_str().unwrap());
        }
    }

    #[test]
    fn lite_subscription_control_and_wrapper_preserve_nested_wire_shape() {
        let lite = LiteSubscriptionDto {
            action: Some(LiteSubscriptionAction::CompleteAdd),
            client_id: Some("client".into()),
            group: Some("workers".into()),
            topic: Some("orders".into()),
            lite_topic_set: Some(vec!["shard-a".into()]),
            offset_option: Some(OffsetOption {
                option_type: Some(OffsetOptionType::Policy),
                value: 1,
            }),
            version: 99,
        };
        let ctl = LiteSubscriptionCtlRequestBody {
            subscription_set: Some(vec![lite]),
        };
        let value = serde_json::to_value(&ctl).unwrap();
        assert_eq!(value["subscriptionSet"][0]["action"], "COMPLETE_ADD");
        assert_eq!(
            value["subscriptionSet"][0]["offsetOption"]["type"],
            "POLICY"
        );
        assert_eq!(
            serde_json::from_value::<LiteSubscriptionCtlRequestBody>(value).unwrap(),
            ctl
        );

        let wrapper = MessageRequestModeSerializeWrapper {
            message_request_mode_map: Some(std::collections::BTreeMap::from([(
                "orders".into(),
                std::collections::BTreeMap::from([(
                    "workers".into(),
                    SetMessageRequestModeRequestBody {
                        topic: Some("orders".into()),
                        consumer_group: Some("workers".into()),
                        mode: Some(MessageRequestMode::Pop),
                        pop_share_queue_num: 2,
                    },
                )]),
            )])),
        };
        let value = serde_json::to_value(wrapper).unwrap();
        assert_eq!(
            value["messageRequestModeMap"]["orders"]["workers"]["mode"],
            "POP"
        );
    }
}
