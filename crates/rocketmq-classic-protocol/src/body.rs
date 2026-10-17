use crate::{
    JavaJsonWriterFeature, SharedValue, codes::JavaEnumLookupError, heartbeat::SubscriptionData,
    serde_fastjson,
};
use rocketmq_classic_common::message::MessageQueue;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser::SerializeStruct};
use std::{collections::BTreeMap, hash::Hash};

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

macro_rules! impl_java_remoting_serializable_adapters {
    ($($dto:ty),+ $(,)?) => {
        $(
            impl $dto {
                pub fn java_encode_optional(
                    value: Option<&Self>,
                ) -> Result<Option<Vec<u8>>, serde_json::Error> {
                    value.map(Self::java_encode).transpose()
                }

                pub fn java_to_json_optional(
                    value: Option<&Self>,
                    pretty_format: bool,
                ) -> Result<String, serde_json::Error> {
                    value.map_or_else(
                        || Ok("null".to_owned()),
                        |value| value.java_to_json(pretty_format),
                    )
                }

                pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
                    crate::route::java_fastjson_json(self, pretty_format)
                }

                pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
                    Ok(self.java_to_json(false)?.into_bytes())
                }

                /// Encodes with the supported inherited Fastjson2 writer features.
                pub fn java_encode_with_features(
                    &self,
                    features: &[JavaJsonWriterFeature],
                ) -> Result<Vec<u8>, serde_json::Error> {
                    Ok(crate::route::java_fastjson_json_with_features(
                        self, false, features,
                    )?.into_bytes())
                }

                pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
                    serde_fastjson::from_fastjson_slice(data)
                }

                pub fn java_decode_nullable(
                    data: Option<&[u8]>,
                ) -> Result<Option<Self>, serde_json::Error> {
                    serde_fastjson::from_fastjson_slice_nullable(data)
                }

                pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
                    serde_fastjson::from_fastjson_slice(data)
                }

                pub fn java_decode_list_nullable(
                    data: Option<&[u8]>,
                ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
                    serde_fastjson::from_fastjson_list_nullable(data)
                }

                pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
                    serde_fastjson::from_fastjson_str(json)
                }

                pub fn java_from_json_nullable(
                    json: Option<&str>,
                ) -> Result<Option<Self>, serde_json::Error> {
                    serde_fastjson::from_fastjson_str_nullable(json)
                }
            }
        )+
    };
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::CheckClientRequestBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<CheckClientRequestBody>(); }
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CheckClientRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_data: Option<SharedValue<super::heartbeat::SubscriptionData>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckClientRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    group: Option<String>,
    subscription_data: Option<super::heartbeat::SubscriptionData>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    namespace: Option<String>,
}

impl<'de> Deserialize<'de> for CheckClientRequestBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: CheckClientRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            client_id: fields.client_id,
            group: fields.group,
            subscription_data: fields.subscription_data.map(SharedValue::new),
            namespace: fields.namespace,
        })
    }
}

impl CheckClientRequestBody {
    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, client_id: Option<String>) {
        self.client_id = client_id;
    }

    pub fn get_group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, group: Option<String>) {
        self.group = group;
    }

    pub fn get_subscription_data(&self) -> Option<SharedValue<super::heartbeat::SubscriptionData>> {
        self.subscription_data.clone()
    }

    /// Returns the same shared object used by the Java DTO setter/getter pair.
    pub fn get_subscription_data_mut(
        &self,
    ) -> Option<SharedValue<super::heartbeat::SubscriptionData>> {
        self.subscription_data.clone()
    }

    pub fn set_subscription_data(
        &mut self,
        subscription_data: Option<SharedValue<super::heartbeat::SubscriptionData>>,
    ) {
        self.subscription_data = subscription_data;
    }

    pub fn get_namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn set_namespace(&mut self, namespace: Option<String>) {
        self.namespace = namespace;
    }
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

pub type SharedQueryAssignment = SharedValue<MessageQueueAssignment>;
pub type QueryAssignmentSet = std::collections::HashSet<Option<SharedQueryAssignment>>;
pub type SharedQueryAssignmentSet = SharedValue<QueryAssignmentSet>;
pub type AssignmentAttachments = std::collections::BTreeMap<String, Option<String>>;
pub type SharedAssignmentAttachments = SharedValue<AssignmentAttachments>;
pub type SharedAssignmentMessageQueue = SharedValue<MessageQueue>;
pub type MessageQueueSet = std::collections::HashSet<Option<SharedAssignmentMessageQueue>>;
pub type SharedMessageQueueSet = SharedValue<MessageQueueSet>;

// Java MessageQueue implements value equality/hash and remains mutable when it
// is stored as a HashSet key. This handle keeps those Set semantics while
// retaining the exact nested MessageQueue object.
impl PartialEq for SharedValue<MessageQueue> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other) || self.snapshot() == other.snapshot()
    }
}

impl Eq for SharedValue<MessageQueue> {}

impl std::hash::Hash for SharedValue<MessageQueue> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.with(|message_queue| message_queue.hash(state));
    }
}

fn deserialize_optional_assignment_set<'de, D>(
    deserializer: D,
) -> Result<Option<SharedQueryAssignmentSet>, D::Error>
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
    // Java stores these mutable DTO keys in a HashSet; changing a nested queue or
    // attachment can likewise leave the Java entry in its original bucket.
    #[allow(clippy::mutable_key_type)]
    let mut assignments = QueryAssignmentSet::with_capacity(values.len());
    for value in values {
        let assignment = if value.get().trim() == "null" {
            None
        } else {
            Some(SharedValue::new(
                serde_json::from_str(value.get()).map_err(de::Error::custom)?,
            ))
        };
        assignments.insert(assignment);
    }
    Ok(Some(SharedValue::new(assignments)))
}

fn default_message_queue_set() -> Option<MessageQueueSet> {
    Some(std::collections::HashSet::new())
}

fn deserialize_optional_message_queue_set<'de, D>(
    deserializer: D,
) -> Result<Option<MessageQueueSet>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let raw = raw.get().trim();
    if raw == "null" || raw == "\"\"" {
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
            "expected a JSON message queue array, object, empty string, or null",
        ));
    };

    #[allow(clippy::mutable_key_type)]
    let queues = values
        .into_iter()
        .map(|value| {
            if value.get().trim() == "null" {
                Ok(None)
            } else {
                serde_json::from_str::<MessageQueue>(value.get())
                    .map(|message_queue| Some(SharedValue::new(message_queue)))
                    .map_err(de::Error::custom)
            }
        })
        .collect::<Result<std::collections::HashSet<_>, _>>()?;
    Ok(Some(queues))
}

/// Java's default JSON writer omits null-valued entries in this map.
fn serialize_assignment_attachments<S>(
    attachments: &Option<SharedAssignmentAttachments>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use serde::ser::SerializeMap;

    let Some(attachments) = attachments else {
        return serializer.serialize_none();
    };
    let attachments = attachments.snapshot();
    let non_null_count = attachments.values().filter(|value| value.is_some()).count();
    let mut map = serializer.serialize_map(Some(non_null_count))?;
    for (key, value) in &attachments {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Default)]
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

/// Shared handle for a Java `LiteSubscriptionDTO` lite topic set.
pub type SharedLiteSubscriptionTopicSet = SharedValue<std::collections::BTreeSet<Option<String>>>;

/// Shared handle for the mutable `OffsetOption` nested in a lite subscription DTO.
pub type SharedOffsetOption = SharedValue<OffsetOption>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OffsetOptionFields {
    #[serde(
        rename = "type",
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    option_type: Option<OffsetOptionType>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    value: i64,
}

impl<'de> Deserialize<'de> for OffsetOption {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: OffsetOptionFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            option_type: fields.option_type,
            value: fields.value,
        })
    }
}

impl OffsetOption {
    pub const POLICY_LAST_VALUE: i64 = 0;
    pub const POLICY_MIN_VALUE: i64 = 1;
    pub const POLICY_MAX_VALUE: i64 = 2;

    /// Mirrors Java `equals(Object)`, including null and wrong-type inputs.
    pub fn java_equals(&self, other: Option<&dyn std::any::Any>) -> bool {
        other
            .and_then(|other| other.downcast_ref::<Self>())
            .is_some_and(|other| self == other)
    }

    pub fn new(option_type: Option<OffsetOptionType>, value: i64) -> Self {
        Self { option_type, value }
    }

    pub fn option_type(&self) -> Option<OffsetOptionType> {
        self.option_type
    }

    pub fn set_option_type(&mut self, option_type: Option<OffsetOptionType>) {
        self.option_type = option_type;
    }

    pub fn value(&self) -> i64 {
        self.value
    }

    pub fn set_value(&mut self, value: i64) {
        self.value = value;
    }
}

impl std::fmt::Display for OffsetOption {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let option_type = self
            .option_type
            .map(|option_type| option_type.to_string())
            .unwrap_or_else(|| "null".to_owned());
        write!(
            formatter,
            "OffsetOption{{type={option_type}, value={}}}",
            self.value
        )
    }
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
/// The Java DTO inherits identity-based `Object.equals` and `hashCode`.
///
/// ```compile_fail
/// fn require_value_equality<T: PartialEq + Eq>() {}
/// fn main() {
///     require_value_equality::<rocketmq_classic_protocol::body::LiteSubscriptionDto>();
/// }
/// ```
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
    pub lite_topic_set: Option<SharedLiteSubscriptionTopicSet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_option: Option<SharedOffsetOption>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    pub version: i64,
}

#[allow(non_camel_case_types)]
pub type LiteSubscriptionDTO = LiteSubscriptionDto;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiteSubscriptionDtoFields {
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    action: Option<LiteSubscriptionAction>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    group: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_fastjson_string_set")]
    lite_topic_set: Option<std::collections::BTreeSet<Option<String>>>,
    #[serde(default)]
    offset_option: Option<OffsetOption>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    version: i64,
}

fn deserialize_optional_fastjson_string_set<'de, D>(
    deserializer: D,
) -> Result<Option<std::collections::BTreeSet<Option<String>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    let raw = raw.get().trim();
    if raw == "null" {
        return Ok(None);
    }

    fn parse_value<E>(raw: &str) -> Result<Option<String>, E>
    where
        E: de::Error,
    {
        let mut deserializer = serde_json::Deserializer::from_str(raw);
        let value =
            serde_fastjson::deserialize_nullable_string(&mut deserializer).map_err(E::custom)?;
        deserializer.end().map_err(E::custom)?;
        Ok(value)
    }

    if raw.starts_with('"') {
        let value = serde_json::from_str::<String>(raw).map_err(de::Error::custom)?;
        let mut values = std::collections::BTreeSet::new();
        if !value.is_empty() {
            values.insert(Some(value));
        }
        return Ok(Some(values));
    }
    if raw.starts_with('{') {
        return Ok(Some(std::collections::BTreeSet::from([parse_value(raw)?])));
    }
    if raw.starts_with('[') {
        let values = serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(raw)
            .map_err(de::Error::custom)?;
        return values
            .iter()
            .map(|value| parse_value(value.get()))
            .collect::<Result<std::collections::BTreeSet<_>, _>>()
            .map(Some);
    }
    Err(de::Error::custom(
        "expected a Fastjson string, object, array, or null for a Java Set field",
    ))
}

impl<'de> Deserialize<'de> for LiteSubscriptionDto {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: LiteSubscriptionDtoFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            action: fields.action,
            client_id: fields.client_id,
            group: fields.group,
            topic: fields.topic,
            lite_topic_set: fields.lite_topic_set.map(SharedValue::new),
            offset_option: fields.offset_option.map(SharedValue::new),
            version: fields.version,
        })
    }
}

impl LiteSubscriptionDto {
    pub fn action(&self) -> Option<LiteSubscriptionAction> {
        self.action
    }

    pub fn set_action(&mut self, action: Option<LiteSubscriptionAction>) -> &mut Self {
        self.action = action;
        self
    }

    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, client_id: Option<String>) -> &mut Self {
        self.client_id = client_id;
        self
    }

    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, group: Option<String>) -> &mut Self {
        self.group = group;
        self
    }

    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, topic: Option<String>) -> &mut Self {
        self.topic = topic;
        self
    }

    pub fn lite_topic_set(&self) -> Option<SharedLiteSubscriptionTopicSet> {
        self.lite_topic_set.clone()
    }

    pub fn lite_topic_set_mut(&mut self) -> Option<&mut SharedLiteSubscriptionTopicSet> {
        self.lite_topic_set.as_mut()
    }

    pub fn set_lite_topic_set(
        &mut self,
        lite_topic_set: Option<SharedLiteSubscriptionTopicSet>,
    ) -> &mut Self {
        self.lite_topic_set = lite_topic_set;
        self
    }

    pub fn offset_option(&self) -> Option<SharedOffsetOption> {
        self.offset_option.clone()
    }

    pub fn offset_option_mut(&mut self) -> Option<&mut SharedOffsetOption> {
        self.offset_option.as_mut()
    }

    pub fn set_offset_option(&mut self, offset_option: Option<SharedOffsetOption>) {
        self.offset_option = offset_option;
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    pub fn set_version(&mut self, version: i64) -> &mut Self {
        self.version = version;
        self
    }
}

impl std::fmt::Display for LiteSubscriptionDto {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let action = self
            .action
            .map(|action| action.to_string())
            .unwrap_or_else(|| "null".to_owned());
        let client_id = self.client_id.as_deref().unwrap_or("null");
        let group = self.group.as_deref().unwrap_or("null");
        let topic = self.topic.as_deref().unwrap_or("null");
        let lite_topic_set = self
            .lite_topic_set
            .as_ref()
            .map(|topics| {
                topics.with(|topics| {
                    format!(
                        "[{}]",
                        topics
                            .iter()
                            .map(|topic| topic.as_deref().unwrap_or("null"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
            })
            .unwrap_or_else(|| "null".to_owned());
        let offset_option = self
            .offset_option
            .as_ref()
            .map(|option| option.with(ToString::to_string))
            .unwrap_or_else(|| "null".to_owned());
        write!(
            formatter,
            "LiteSubscriptionDTO{{action={action}, clientId='{client_id}', group='{group}', topic='{topic}', liteTopicSet={lite_topic_set}, offsetOption={offset_option}, version={}}}",
            self.version
        )
    }
}

/// Nullable DTO elements in Java's identity-based subscription Set.
pub type LiteSubscriptionDtoSet =
    std::collections::HashSet<Option<SharedValue<LiteSubscriptionDto>>>;

/// Shared handle for the subscription Set stored by `LiteSubscriptionCtlRequestBody`.
pub type SharedLiteSubscriptionSet = SharedValue<LiteSubscriptionDtoSet>;

/// Shared mutable topic names returned by `LiteSubscription.getLiteTopicSet`.
pub type SharedLiteSubscriptionTopicNames = SharedValue<std::collections::BTreeSet<String>>;

impl PartialEq for SharedValue<LiteSubscriptionDto> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other)
    }
}

impl Eq for SharedValue<LiteSubscriptionDto> {}

impl std::hash::Hash for SharedValue<LiteSubscriptionDto> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash_identity(state);
    }
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
/// Java `LiteSubscriptionCtlRequestBody` inherits identity-based equality.
///
/// ```compile_fail
/// fn require_value_equality<T: PartialEq + Eq>() {}
/// fn main() {
///     require_value_equality::<rocketmq_classic_protocol::body::LiteSubscriptionCtlRequestBody>();
/// }
/// ```
pub struct LiteSubscriptionCtlRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_set: Option<SharedLiteSubscriptionSet>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiteSubscriptionCtlRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_vec_or_single"
    )]
    subscription_set: Option<Vec<Option<LiteSubscriptionDto>>>,
}

impl<'de> Deserialize<'de> for LiteSubscriptionCtlRequestBody {
    // The Set key hash uses the SharedValue allocation identity, which remains
    // stable when the Java DTO's mutable fields change.
    #[allow(clippy::mutable_key_type)]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: LiteSubscriptionCtlRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        let mut null_seen = false;
        let subscription_set = fields.subscription_set.map(|items| {
            let items = items
                .into_iter()
                .filter(|item| {
                    if item.is_some() {
                        true
                    } else if null_seen {
                        false
                    } else {
                        null_seen = true;
                        true
                    }
                })
                .map(|item| item.map(SharedValue::new))
                .collect::<LiteSubscriptionDtoSet>();
            SharedValue::new(items)
        });
        Ok(Self { subscription_set })
    }
}

impl LiteSubscriptionCtlRequestBody {
    pub fn subscription_set(&self) -> Option<SharedLiteSubscriptionSet> {
        self.subscription_set.clone()
    }

    pub fn subscription_set_mut(&mut self) -> Option<&mut SharedLiteSubscriptionSet> {
        self.subscription_set.as_mut()
    }

    pub fn set_subscription_set(&mut self, subscription_set: Option<SharedLiteSubscriptionSet>) {
        self.subscription_set = subscription_set;
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
/// Java `LiteSubscription` inherits identity-based equality.
///
/// ```compile_fail
/// fn require_value_equality<T: PartialEq + Eq>() {}
/// fn main() {
///     require_value_equality::<rocketmq_classic_protocol::body::LiteSubscription>();
/// }
/// ```
pub struct LiteSubscription {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    pub lite_topic_set: SharedLiteSubscriptionTopicNames,
    pub update_time: i64,
}

fn current_lite_subscription_time_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(i64::MAX as u128) as i64,
        Err(error) => -(error.duration().as_millis().min(i64::MAX as u128) as i64),
    }
}

impl Default for LiteSubscription {
    fn default() -> Self {
        Self {
            group: None,
            topic: None,
            lite_topic_set: SharedValue::new(std::collections::BTreeSet::new()),
            update_time: current_lite_subscription_time_millis(),
        }
    }
}

impl LiteSubscription {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }

    pub fn set_group(&mut self, group: Option<String>) -> &mut Self {
        self.group = group;
        self
    }

    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, topic: Option<String>) -> &mut Self {
        self.topic = topic;
        self
    }

    pub fn lite_topic_set(&self) -> SharedLiteSubscriptionTopicNames {
        self.lite_topic_set.clone()
    }

    pub fn lite_topic_set_mut(&mut self) -> &mut SharedLiteSubscriptionTopicNames {
        &mut self.lite_topic_set
    }

    pub fn set_lite_topic_set<I, S>(&mut self, topics: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.lite_topic_set.with_mut(|topic_set| {
            topic_set.extend(topics.into_iter().map(|topic| topic.as_ref().to_owned()));
        });
        self
    }

    pub fn update_time(&self) -> i64 {
        self.update_time
    }

    pub fn set_update_time(&mut self, update_time: i64) {
        self.update_time = update_time;
    }

    pub fn add_lite_topic(&mut self, topic: impl Into<String>) -> bool {
        self.update_time = current_lite_subscription_time_millis();
        self.lite_topic_set
            .with_mut(|topic_set| topic_set.insert(topic.into()))
    }

    pub fn add_lite_topics<I, S>(&mut self, topics: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.update_time = current_lite_subscription_time_millis();
        self.lite_topic_set.with_mut(|topic_set| {
            topic_set.extend(topics.into_iter().map(|topic| topic.as_ref().to_owned()));
        });
    }

    pub fn remove_lite_topic(&mut self, topic: impl AsRef<str>) -> bool {
        self.update_time = current_lite_subscription_time_millis();
        self.lite_topic_set
            .with_mut(|topic_set| topic_set.remove(topic.as_ref()))
    }

    pub fn remove_lite_topics<I, S>(&mut self, topics: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.update_time = current_lite_subscription_time_millis();
        for topic in topics {
            self.lite_topic_set
                .with_mut(|topic_set| topic_set.remove(topic.as_ref()));
        }
    }
}

impl std::fmt::Display for LiteSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let group = self.group.as_deref().unwrap_or("null");
        let topic = self.topic.as_deref().unwrap_or("null");
        let lite_topic_set = self.lite_topic_set.with(|topics| {
            topics
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        });
        write!(
            formatter,
            "LiteSubscription{{group='{group}', topic='{topic}', liteTopicSet=[{lite_topic_set}], updateTime={}}}",
            self.update_time
        )
    }
}

struct LiteSubscriptionRawEntries(Vec<(String, Box<serde_json::value::RawValue>)>);

impl<'de> Deserialize<'de> for LiteSubscriptionRawEntries {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EntriesVisitor;

        impl<'de> de::Visitor<'de> for EntriesVisitor {
            type Value = LiteSubscriptionRawEntries;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object containing LiteSubscription fields")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut entries = Vec::new();
                while let Some((name, value)) = map.next_entry()? {
                    entries.push((name, value));
                }
                Ok(LiteSubscriptionRawEntries(entries))
            }
        }

        deserializer.deserialize_map(EntriesVisitor)
    }
}

fn parse_lite_subscription_nullable_string(raw: &str) -> Result<Option<String>, String> {
    let mut deserializer = serde_json::Deserializer::from_str(raw);
    let value = serde_fastjson::deserialize_nullable_string(&mut deserializer)
        .map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;
    Ok(value)
}

fn parse_lite_subscription_update_time(raw: &str) -> Result<i64, String> {
    let mut deserializer = serde_json::Deserializer::from_str(raw);
    let value = serde_fastjson::deserialize_i64_reject_object(&mut deserializer)
        .map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;
    Ok(value)
}

fn parse_lite_subscription_topic_set(
    raw: &str,
) -> Result<std::collections::BTreeSet<String>, String> {
    let mut deserializer = serde_json::Deserializer::from_str(raw);
    let values = deserialize_optional_fastjson_string_set(&mut deserializer)
        .map_err(|error| error.to_string())?;
    deserializer.end().map_err(|error| error.to_string())?;
    let values =
        values.ok_or_else(|| "LiteSubscription cannot add a null lite topic set".to_owned())?;
    values
        .into_iter()
        .map(|value| {
            value.ok_or_else(|| "LiteSubscription cannot add a null lite topic".to_owned())
        })
        .collect()
}

impl<'de> Deserialize<'de> for LiteSubscription {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let LiteSubscriptionRawEntries(entries) =
            LiteSubscriptionRawEntries::deserialize(deserializer)?;
        let mut subscription = Self::default();
        for (name, raw) in entries {
            match name.as_str() {
                "group" => {
                    subscription.group = parse_lite_subscription_nullable_string(raw.get())
                        .map_err(de::Error::custom)?;
                }
                "topic" => {
                    subscription.topic = parse_lite_subscription_nullable_string(raw.get())
                        .map_err(de::Error::custom)?;
                }
                "liteTopicSet" => {
                    let topics =
                        parse_lite_subscription_topic_set(raw.get()).map_err(de::Error::custom)?;
                    subscription
                        .lite_topic_set
                        .with_mut(|topic_set| topic_set.extend(topics));
                }
                "updateTime" => {
                    subscription.update_time = parse_lite_subscription_update_time(raw.get())
                        .map_err(de::Error::custom)?;
                }
                _ => {}
            }
        }
        Ok(subscription)
    }
}

/// Java `QueryAssignmentRequestBody` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::QueryAssignmentRequestBody;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<QueryAssignmentRequestBody>();
/// ```
#[derive(Debug, Clone, Serialize, Default)]
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
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    consumer_group: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
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

impl QueryAssignmentRequestBody {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, topic: Option<String>) {
        self.topic = topic;
    }

    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, consumer_group: Option<String>) {
        self.consumer_group = consumer_group;
    }

    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, client_id: Option<String>) {
        self.client_id = client_id;
    }

    pub fn get_strategy_name(&self) -> Option<&str> {
        self.strategy_name.as_deref()
    }

    pub fn set_strategy_name(&mut self, strategy_name: Option<String>) {
        self.strategy_name = strategy_name;
    }

    pub fn get_message_model(&self) -> Option<super::heartbeat::MessageModel> {
        self.message_model
    }

    pub fn set_message_model(&mut self, message_model: Option<super::heartbeat::MessageModel>) {
        self.message_model = message_model;
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageQueueAssignment {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_queue: Option<SharedAssignmentMessageQueue>,
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
    pub attachments: Option<SharedAssignmentAttachments>,
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
    attachments: Option<AssignmentAttachments>,
}

impl<'de> Deserialize<'de> for MessageQueueAssignment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: MessageQueueAssignmentFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            message_queue: fields.message_queue.map(SharedValue::new),
            mode: fields.mode,
            attachments: fields.attachments.map(SharedValue::new),
        })
    }
}

impl PartialEq for MessageQueueAssignment {
    fn eq(&self, other: &Self) -> bool {
        self.message_queue.as_ref().map(SharedValue::snapshot)
            == other.message_queue.as_ref().map(SharedValue::snapshot)
            && self.mode == other.mode
            && self.attachments.as_ref().map(SharedValue::snapshot)
                == other.attachments.as_ref().map(SharedValue::snapshot)
    }
}

impl Eq for MessageQueueAssignment {}

impl std::hash::Hash for MessageQueueAssignment {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.message_queue
            .as_ref()
            .map(SharedValue::snapshot)
            .hash(state);
        self.mode.hash(state);
        self.attachments
            .as_ref()
            .map(SharedValue::snapshot)
            .hash(state);
    }
}

// Keep Java DTO object aliases while retaining the existing value-based Set
// membership used by this Rust response DTO.
impl PartialEq for SharedValue<MessageQueueAssignment> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other) || self.snapshot() == other.snapshot()
    }
}

impl Eq for SharedValue<MessageQueueAssignment> {}

impl std::hash::Hash for SharedValue<MessageQueueAssignment> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.snapshot().hash(state);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MessageQueueAssignmentEqualsError {
    #[error("Java MessageQueueAssignment.equals dereferences a null messageQueue")]
    NullMessageQueue,
}

impl MessageQueueAssignment {
    pub fn message_queue(&self) -> Option<SharedAssignmentMessageQueue> {
        self.message_queue.clone()
    }

    /// Provides direct access to the assigned queue handle for replacement.
    pub fn message_queue_mut(&mut self) -> Option<&mut SharedAssignmentMessageQueue> {
        self.message_queue.as_mut()
    }

    pub fn set_message_queue(&mut self, message_queue: Option<SharedAssignmentMessageQueue>) {
        self.message_queue = message_queue;
    }

    pub fn mode(&self) -> Option<MessageRequestMode> {
        self.mode
    }

    pub fn set_mode(&mut self, mode: Option<MessageRequestMode>) {
        self.mode = mode;
    }

    pub fn attachments(&self) -> Option<SharedAssignmentAttachments> {
        self.attachments.clone()
    }

    pub fn attachments_mut(&mut self) -> Option<&mut SharedAssignmentAttachments> {
        self.attachments.as_mut()
    }

    pub fn set_attachments(&mut self, attachments: Option<SharedAssignmentAttachments>) {
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
        Ok(other.message_queue.as_ref().is_some_and(|other_queue| {
            message_queue.ptr_eq(other_queue) || message_queue.snapshot() == other_queue.snapshot()
        }))
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
            .map(|message_queue| message_queue.with(ToString::to_string))
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

/// Java inherits identity-based equality and hashing for this response DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::QueryAssignmentResponseBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<QueryAssignmentResponseBody>(); }
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryAssignmentResponseBody {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_assignment_set",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_queue_assignments: Option<SharedQueryAssignmentSet>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryAssignmentResponseBodyFields {
    #[serde(default, deserialize_with = "deserialize_optional_assignment_set")]
    message_queue_assignments: Option<SharedQueryAssignmentSet>,
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

impl QueryAssignmentResponseBody {
    pub fn get_message_queue_assignments(&self) -> Option<SharedQueryAssignmentSet> {
        self.message_queue_assignments.clone()
    }

    /// Provides direct access to the assigned identity handle for replacement.
    pub fn get_message_queue_assignments_mut(&mut self) -> Option<&mut SharedQueryAssignmentSet> {
        self.message_queue_assignments.as_mut()
    }

    pub fn set_message_queue_assignments(
        &mut self,
        message_queue_assignments: Option<SharedQueryAssignmentSet>,
    ) {
        self.message_queue_assignments = message_queue_assignments;
    }
}

/// One queue's broker and consumer timestamp range, matching Java
/// `org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan`.
#[derive(Debug, Clone, Default)]
pub struct QueueTimeSpan {
    pub message_queue: Option<MessageQueue>,
    pub min_time_stamp: i64,
    pub max_time_stamp: i64,
    pub consume_time_stamp: i64,
    pub delay_time: i64,
}

impl Serialize for QueueTimeSpan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = serializer.serialize_struct(
            "QueueTimeSpan",
            7 + usize::from(self.message_queue.is_some()),
        )?;
        fields.serialize_field("consumeTimeStamp", &self.consume_time_stamp)?;
        fields.serialize_field("consumeTimeStampStr", &self.get_consume_time_stamp_str())?;
        fields.serialize_field("delayTime", &self.delay_time)?;
        fields.serialize_field("maxTimeStamp", &self.max_time_stamp)?;
        fields.serialize_field("maxTimeStampStr", &self.get_max_time_stamp_str())?;
        if let Some(message_queue) = &self.message_queue {
            fields.serialize_field("messageQueue", message_queue)?;
        }
        fields.serialize_field("minTimeStamp", &self.min_time_stamp)?;
        fields.serialize_field("minTimeStampStr", &self.get_min_time_stamp_str())?;
        fields.end()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueueTimeSpanFields {
    #[serde(default)]
    message_queue: Option<MessageQueue>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    min_time_stamp: i64,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    max_time_stamp: i64,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    consume_time_stamp: i64,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    delay_time: i64,
}

impl<'de> Deserialize<'de> for QueueTimeSpan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: QueueTimeSpanFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            message_queue: fields.message_queue,
            min_time_stamp: fields.min_time_stamp,
            max_time_stamp: fields.max_time_stamp,
            consume_time_stamp: fields.consume_time_stamp,
            delay_time: fields.delay_time,
        })
    }
}

impl QueueTimeSpan {
    pub fn get_min_time_stamp_str(&self) -> String {
        java_format_timestamp_millis(self.min_time_stamp)
    }

    pub fn get_max_time_stamp_str(&self) -> String {
        java_format_timestamp_millis(self.max_time_stamp)
    }

    pub fn get_consume_time_stamp_str(&self) -> String {
        java_format_timestamp_millis(self.consume_time_stamp)
    }

    pub fn get_message_queue(&self) -> Option<&MessageQueue> {
        self.message_queue.as_ref()
    }

    pub fn set_message_queue(&mut self, message_queue: Option<MessageQueue>) {
        self.message_queue = message_queue;
    }

    pub fn get_min_time_stamp(&self) -> i64 {
        self.min_time_stamp
    }

    pub fn set_min_time_stamp(&mut self, min_time_stamp: i64) {
        self.min_time_stamp = min_time_stamp;
    }

    pub fn get_max_time_stamp(&self) -> i64 {
        self.max_time_stamp
    }

    pub fn set_max_time_stamp(&mut self, max_time_stamp: i64) {
        self.max_time_stamp = max_time_stamp;
    }

    pub fn get_consume_time_stamp(&self) -> i64 {
        self.consume_time_stamp
    }

    pub fn set_consume_time_stamp(&mut self, consume_time_stamp: i64) {
        self.consume_time_stamp = consume_time_stamp;
    }

    pub fn get_delay_time(&self) -> i64 {
        self.delay_time
    }

    pub fn set_delay_time(&mut self, delay_time: i64) {
        self.delay_time = delay_time;
    }
}

fn java_format_timestamp_millis(timestamp_millis: i64) -> String {
    const MILLIS_PER_DAY: i128 = 86_400_000;
    let timestamp_millis = i128::from(timestamp_millis);
    let utc_days = timestamp_millis.div_euclid(MILLIS_PER_DAY);
    let millis_in_day = timestamp_millis.rem_euclid(MILLIS_PER_DAY);
    let (utc_year, utc_month, utc_day) = java_calendar_date_from_epoch_day(utc_days);
    let hour = millis_in_day / 3_600_000;
    let minute = (millis_in_day / 60_000) % 60;
    let second = (millis_in_day / 1_000) % 60;
    let millisecond = millis_in_day % 1_000;
    let offset_seconds = java_local_offset_seconds(
        timestamp_millis,
        utc_year,
        utc_month,
        utc_day,
        hour,
        minute,
        second,
    );
    let local_millis = timestamp_millis + i128::from(offset_seconds) * 1_000;
    let local_days = local_millis.div_euclid(MILLIS_PER_DAY);
    let millis_in_day = local_millis.rem_euclid(MILLIS_PER_DAY);
    let (year, month, day) = java_calendar_date_from_epoch_day(local_days);
    let year_of_era = if year > 0 { year } else { 1 - year };
    format!(
        "{year_of_era:04}-{month:02}-{day:02}#{:02}:{:02}:{:02}:{millisecond:03}",
        millis_in_day / 3_600_000,
        (millis_in_day / 60_000) % 60,
        (millis_in_day / 1_000) % 60,
    )
}

fn java_local_offset_seconds(
    timestamp_millis: i128,
    year: i128,
    month: i128,
    day: i128,
    hour: i128,
    minute: i128,
    second: i128,
) -> i32 {
    let exact_datetime =
        time::OffsetDateTime::from_unix_timestamp_nanos(timestamp_millis * 1_000_000);
    if let Ok(datetime) = exact_datetime {
        if let Ok(offset) = time::UtcOffset::local_offset_at(datetime) {
            return offset.whole_seconds();
        }
    }

    if year < -9_999 {
        let winter = local_offset_for_gregorian_date(2_000, 1, 15, 12, 0, 0);
        let summer = local_offset_for_gregorian_date(2_000, 7, 15, 12, 0, 0);
        return winter.min(summer);
    }

    let representative_year = if year > 9_999 {
        2_000 + (year - 2_000).rem_euclid(400)
    } else {
        year
    };
    let day = day.min(days_in_gregorian_month(representative_year, month));
    local_offset_for_gregorian_date(representative_year, month, day, hour, minute, second)
}

fn local_offset_for_gregorian_date(
    year: i128,
    month: i128,
    day: i128,
    hour: i128,
    minute: i128,
    second: i128,
) -> i32 {
    let representative_days = gregorian_days_from_civil(year, month, day);
    let representative_seconds = representative_days * 86_400 + hour * 3_600 + minute * 60 + second;
    time::OffsetDateTime::from_unix_timestamp(representative_seconds as i64)
        .ok()
        .and_then(|datetime| time::UtcOffset::local_offset_at(datetime).ok())
        .unwrap_or(time::UtcOffset::UTC)
        .whole_seconds()
}

fn java_calendar_date_from_epoch_day(days: i128) -> (i128, i128, i128) {
    // GregorianCalendar switches from the Julian calendar after 1582-10-04.
    if days >= -141_427 {
        let shifted = days + 719_468;
        let era = shifted.div_euclid(146_097);
        let day_of_era = shifted - era * 146_097;
        let year_of_era =
            (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let mut year = year_of_era + era * 400;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month_prime = (5 * day_of_year + 2) / 153;
        let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
        let month = month_prime + if month_prime < 10 { 3 } else { -9 };
        year += i128::from(month <= 2);
        (year, month, day)
    } else {
        let mut days_in_cycle = days + 719_530;
        let cycle = days_in_cycle.div_euclid(1_461);
        days_in_cycle = days_in_cycle.rem_euclid(1_461);
        let mut year = cycle * 4;
        loop {
            let year_length = if year.rem_euclid(4) == 0 { 366 } else { 365 };
            if days_in_cycle < year_length {
                break;
            }
            days_in_cycle -= year_length;
            year += 1;
        }
        let mut month = 1;
        while days_in_cycle >= julian_month_length(year, month) {
            days_in_cycle -= julian_month_length(year, month);
            month += 1;
        }
        (year, month, days_in_cycle + 1)
    }
}

fn julian_month_length(year: i128, month: i128) -> i128 {
    match month {
        2 if year.rem_euclid(4) == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn gregorian_days_from_civil(year: i128, month: i128, day: i128) -> i128 {
    let year = year - i128::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn days_in_gregorian_month(year: i128, month: i128) -> i128 {
    match month {
        2 if year.rem_euclid(4) == 0
            && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0) =>
        {
            29
        }
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Java response wrapper returned by `QUERY_CONSUME_TIME_SPAN`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryConsumeTimeSpanBody {
    #[serde(
        default = "default_consume_time_span_set",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_vec_or_single",
        skip_serializing_if = "Option::is_none"
    )]
    pub consume_time_span_set: Option<Vec<Option<QueueTimeSpan>>>,
}

fn default_consume_time_span_set() -> Option<Vec<Option<QueueTimeSpan>>> {
    Some(Vec::new())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryConsumeTimeSpanBodyFields {
    #[serde(
        default = "default_consume_time_span_set",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_vec_or_single"
    )]
    consume_time_span_set: Option<Vec<Option<QueueTimeSpan>>>,
}

impl<'de> Deserialize<'de> for QueryConsumeTimeSpanBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: QueryConsumeTimeSpanBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            consume_time_span_set: fields.consume_time_span_set,
        })
    }
}

impl Default for QueryConsumeTimeSpanBody {
    fn default() -> Self {
        Self {
            consume_time_span_set: default_consume_time_span_set(),
        }
    }
}

impl QueryConsumeTimeSpanBody {
    pub fn get_consume_time_span_set(&self) -> Option<&[Option<QueueTimeSpan>]> {
        self.consume_time_span_set.as_deref()
    }

    pub fn get_consume_time_span_set_mut(&mut self) -> Option<&mut Vec<Option<QueueTimeSpan>>> {
        self.consume_time_span_set.as_mut()
    }

    pub fn set_consume_time_span_set(
        &mut self,
        consume_time_span_set: Option<Vec<Option<QueueTimeSpan>>>,
    ) {
        self.consume_time_span_set = consume_time_span_set;
    }
}

/// Java `SetMessageRequestModeRequestBody` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::SetMessageRequestModeRequestBody;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<SetMessageRequestModeRequestBody>();
/// ```
#[derive(Debug, Clone, Serialize)]
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

impl SetMessageRequestModeRequestBody {
    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, topic: Option<String>) {
        self.topic = topic;
    }

    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, consumer_group: Option<String>) {
        self.consumer_group = consumer_group;
    }

    pub fn get_mode(&self) -> Option<MessageRequestMode> {
        self.mode
    }

    pub fn set_mode(&mut self, mode: Option<MessageRequestMode>) {
        self.mode = mode;
    }

    pub fn get_pop_share_queue_num(&self) -> i32 {
        self.pop_share_queue_num
    }

    pub fn set_pop_share_queue_num(&mut self, pop_share_queue_num: i32) {
        self.pop_share_queue_num = pop_share_queue_num;
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SetMessageRequestModeRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    consumer_group: Option<String>,
    #[serde(
        default = "default_assignment_mode",
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    mode: Option<MessageRequestMode>,
    #[serde(default, deserialize_with = "deserialize_fastjson_i32_object_as_zero")]
    pop_share_queue_num: i32,
}

fn deserialize_fastjson_i32_object_as_zero<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    if raw.get().trim_start().starts_with('{') {
        return Ok(0);
    }

    #[derive(Deserialize)]
    struct ValueField {
        #[serde(deserialize_with = "serde_fastjson::deserialize_i32")]
        value: i32,
    }

    let wrapped = format!("{{\"value\":{}}}", raw.get());
    serde_json::from_str::<ValueField>(&wrapped)
        .map(|field| field.value)
        .map_err(de::Error::custom)
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

/// A compact, Java-compatible bit set used by [`BatchAck`].
///
/// Bit indexes use the same little-endian byte ordering as
/// `java.util.BitSet.toByteArray()`.
#[derive(Debug, Clone)]
pub struct BitSet {
    words: Vec<u64>,
    java_capacity_words: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitSetError {
    NegativeIndex(i32),
    NegativeCapacity(i32),
    InvalidRange { from: i32, to: i32 },
    IndexOverflow(usize),
    AllocationFailed(i32),
}

impl std::fmt::Display for BitSetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NegativeIndex(index) => {
                write!(formatter, "bit index must be nonnegative: {index}")
            }
            Self::NegativeCapacity(capacity) => {
                write!(
                    formatter,
                    "bit-set capacity must be nonnegative: {capacity}"
                )
            }
            Self::InvalidRange { from, to } => {
                write!(formatter, "bit range must have from <= to: {from}..{to}")
            }
            Self::IndexOverflow(index) => {
                write!(formatter, "bit index does not fit Java int: {index}")
            }
            Self::AllocationFailed(index) => {
                write!(formatter, "could not allocate bit index: {index}")
            }
        }
    }
}

impl std::error::Error for BitSetError {}

impl Default for BitSet {
    fn default() -> Self {
        Self {
            words: Vec::new(),
            java_capacity_words: 1,
        }
    }
}

impl PartialEq for BitSet {
    fn eq(&self, other: &Self) -> bool {
        self.words == other.words
    }
}

impl Eq for BitSet {}

impl BitSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a bit set with Java's requested initial capacity in bits.
    pub fn try_with_capacity_bits(capacity: i32) -> Result<Self, BitSetError> {
        let capacity =
            usize::try_from(capacity).map_err(|_| BitSetError::NegativeCapacity(capacity))?;
        let capacity_words = capacity.div_ceil(64);
        let mut words = Vec::new();
        words.try_reserve_exact(capacity_words).map_err(|_| {
            BitSetError::AllocationFailed(capacity.saturating_sub(1).min(i32::MAX as usize) as i32)
        })?;
        Ok(Self {
            words,
            java_capacity_words: capacity_words,
        })
    }

    /// Creates a bit set from Java's little-endian `BitSet.toByteArray()` form.
    pub fn from_byte_array(bytes: &[u8]) -> Self {
        let mut words = bytes
            .chunks(8)
            .map(|chunk| {
                let mut word = [0; 8];
                word[..chunk.len()].copy_from_slice(chunk);
                u64::from_le_bytes(word)
            })
            .collect::<Vec<_>>();
        while words.last() == Some(&0) {
            words.pop();
        }
        let java_capacity_words = words.len();
        Self {
            words,
            java_capacity_words,
        }
    }

    /// Creates a bit set from Java's native signed-long `BitSet.valueOf(long[])` form.
    pub fn try_from_long_array(values: &[i64]) -> Result<Self, BitSetError> {
        let mut words = Vec::new();
        words
            .try_reserve_exact(values.len())
            .map_err(|_| BitSetError::AllocationFailed(i32::MAX))?;
        words.extend(values.iter().map(|value| *value as u64));
        while words.last() == Some(&0) {
            words.pop();
        }
        let java_capacity_words = words.len();
        Ok(Self {
            words,
            java_capacity_words,
        })
    }

    /// Returns the Java `BitSet.size()` capacity in bits.
    pub fn size(&self) -> usize {
        self.java_capacity_words.saturating_mul(64)
    }

    /// Returns the minimal little-endian byte array used by Java's `toByteArray()`.
    pub fn to_byte_array(&self) -> Result<Vec<u8>, BitSetError> {
        let bit_length = self.length();
        let byte_length = bit_length.div_ceil(8);
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(byte_length)
            .map_err(|_| BitSetError::AllocationFailed(bit_length.saturating_sub(1) as i32))?;
        bytes.resize(byte_length, 0);
        for (word_index, word) in self.words.iter().copied().enumerate() {
            let start = word_index * 8;
            if start >= byte_length {
                break;
            }
            let encoded = word.to_le_bytes();
            let count = (byte_length - start).min(encoded.len());
            bytes[start..start + count].copy_from_slice(&encoded[..count]);
        }
        Ok(bytes)
    }

    pub fn get(&self, index: i32) -> Result<bool, BitSetError> {
        let index = Self::checked_index(index)?;
        let word = index / 64;
        Ok(self
            .words
            .get(word)
            .is_some_and(|value| value & (1 << (index % 64)) != 0))
    }

    pub fn set(&mut self, index: i32) -> Result<(), BitSetError> {
        let index = Self::checked_index(index)?;
        let word_index = index / 64;
        self.ensure_words_len(word_index + 1, index as i32)?;
        self.words[word_index] |= 1 << (index % 64);
        Ok(())
    }

    /// Sets or clears one bit, matching Java's `set(index, value)` overload.
    pub fn set_with_value(&mut self, index: i32, value: bool) -> Result<(), BitSetError> {
        if value {
            self.set(index)
        } else {
            self.clear(index)
        }
    }

    pub fn clear(&mut self, index: i32) -> Result<(), BitSetError> {
        let index = Self::checked_index(index)?;
        let word_index = index / 64;
        if let Some(word) = self.words.get_mut(word_index) {
            *word &= !(1 << (index % 64));
            self.trim_trailing_zero_words();
        }
        Ok(())
    }

    pub fn flip(&mut self, index: i32) -> Result<(), BitSetError> {
        let index = Self::checked_index(index)?;
        if self.get(index as i32)? {
            self.clear(index as i32)
        } else {
            self.set(index as i32)
        }
    }

    /// Sets every bit in the half-open range `from..to`.
    pub fn set_range(&mut self, from: i32, to: i32) -> Result<(), BitSetError> {
        self.mutate_range(from, to, true, |word, mask| word | mask)
    }

    /// Clears every bit in the half-open range `from..to`.
    pub fn clear_range(&mut self, from: i32, to: i32) -> Result<(), BitSetError> {
        self.mutate_range(from, to, false, |word, mask| word & !mask)
    }

    /// Flips every bit in the half-open range `from..to`.
    pub fn flip_range(&mut self, from: i32, to: i32) -> Result<(), BitSetError> {
        self.mutate_range(from, to, true, |word, mask| word ^ mask)
    }

    /// Sets or clears every bit in the half-open range, matching Java's boolean overload.
    pub fn set_range_with_value(
        &mut self,
        from: i32,
        to: i32,
        value: bool,
    ) -> Result<(), BitSetError> {
        if value {
            self.set_range(from, to)
        } else {
            self.clear_range(from, to)
        }
    }

    /// Clears all bits while keeping this owned value reusable.
    pub fn clear_all(&mut self) {
        self.words.clear();
    }

    /// Returns a copy of the half-open range, shifted so `from` becomes bit zero.
    pub fn get_range(&self, from: i32, to: i32) -> Result<Self, BitSetError> {
        let (from, to) = Self::checked_range(from, to)?;
        let bit_count = to - from;
        if bit_count == 0 {
            return Ok(Self {
                words: Vec::new(),
                java_capacity_words: 0,
            });
        }

        let output_word_count = bit_count.div_ceil(64);
        let mut words = Vec::new();
        words
            .try_reserve_exact(output_word_count)
            .map_err(|_| BitSetError::AllocationFailed((to - 1) as i32))?;
        for output_word_index in 0..output_word_count {
            let source_bit_index = from + output_word_index * 64;
            let source_word_index = source_bit_index / 64;
            let shift = source_bit_index % 64;
            let lower = self.words.get(source_word_index).copied().unwrap_or(0) >> shift;
            let upper = if shift == 0 {
                0
            } else {
                self.words.get(source_word_index + 1).copied().unwrap_or(0) << (64 - shift)
            };
            let mut word = lower | upper;
            let remaining_bits = bit_count - output_word_index * 64;
            if remaining_bits < 64 {
                word &= (1_u64 << remaining_bits) - 1;
            }
            words.push(word);
        }
        while words.last() == Some(&0) {
            words.pop();
        }
        Ok(Self {
            words,
            java_capacity_words: output_word_count,
        })
    }

    /// Returns the next set bit at or after `from`, or `-1` when none exists.
    pub fn next_set_bit(&self, from: i32) -> Result<i32, BitSetError> {
        let from = Self::checked_index(from)?;
        let mut word_index = from / 64;
        let Some(first_word) = self.words.get(word_index).copied() else {
            return Ok(-1);
        };
        let first_candidate = first_word & (u64::MAX << (from % 64));
        if first_candidate != 0 {
            return Self::java_index(word_index * 64 + first_candidate.trailing_zeros() as usize);
        }
        word_index += 1;
        while let Some(word) = self.words.get(word_index).copied() {
            if word != 0 {
                return Self::java_index(word_index * 64 + word.trailing_zeros() as usize);
            }
            word_index += 1;
        }
        Ok(-1)
    }

    /// Returns the next clear bit at or after `from`.
    pub fn next_clear_bit(&self, from: i32) -> Result<i32, BitSetError> {
        let from = Self::checked_index(from)?;
        let mut word_index = from / 64;
        let Some(first_word) = self.words.get(word_index).copied() else {
            return Self::java_index(from);
        };
        let first_candidate = !first_word & (u64::MAX << (from % 64));
        if first_candidate != 0 {
            return Self::java_index(word_index * 64 + first_candidate.trailing_zeros() as usize);
        }
        word_index += 1;
        while let Some(word) = self.words.get(word_index).copied() {
            if word != u64::MAX {
                let candidate = !word;
                return Self::java_index(word_index * 64 + candidate.trailing_zeros() as usize);
            }
            word_index += 1;
        }
        Self::java_index(self.words.len() * 64)
    }

    /// Returns the previous set bit at or before `from`, or `-1` when none exists.
    /// Java defines `previousSetBit(-1)` as `-1` as a special case.
    pub fn previous_set_bit(&self, from: i32) -> Result<i32, BitSetError> {
        if from == -1 {
            return Ok(-1);
        }
        if from < -1 {
            return Err(BitSetError::NegativeIndex(from));
        }
        let from = from as usize;
        if self.words.is_empty() {
            return Ok(-1);
        }
        let mut word_index = (from / 64).min(self.words.len() - 1);
        let bit_offset = from.min(word_index * 64 + 63) % 64;
        let mask = if bit_offset == 63 {
            u64::MAX
        } else {
            (1_u64 << (bit_offset + 1)) - 1
        };
        let candidate = self.words[word_index] & mask;
        if candidate != 0 {
            return Self::java_index(word_index * 64 + (63 - candidate.leading_zeros() as usize));
        }
        while word_index > 0 {
            word_index -= 1;
            let candidate = self.words[word_index];
            if candidate != 0 {
                return Self::java_index(
                    word_index * 64 + (63 - candidate.leading_zeros() as usize),
                );
            }
        }
        Ok(-1)
    }

    /// Returns the previous clear bit at or before `from`, or `-1` when none exists.
    /// Java defines `previousClearBit(-1)` as `-1` as a special case.
    pub fn previous_clear_bit(&self, from: i32) -> Result<i32, BitSetError> {
        if from == -1 {
            return Ok(-1);
        }
        if from < -1 {
            return Err(BitSetError::NegativeIndex(from));
        }
        let from = from as usize;
        if from / 64 >= self.words.len() {
            return Self::java_index(from);
        }
        let mut word_index = from / 64;
        let bit_offset = from % 64;
        let mask = if bit_offset == 63 {
            u64::MAX
        } else {
            (1_u64 << (bit_offset + 1)) - 1
        };
        let candidate = !self.words[word_index] & mask;
        if candidate != 0 {
            return Self::java_index(word_index * 64 + (63 - candidate.leading_zeros() as usize));
        }
        while word_index > 0 {
            word_index -= 1;
            let candidate = !self.words[word_index];
            if candidate != 0 {
                return Self::java_index(
                    word_index * 64 + (63 - candidate.leading_zeros() as usize),
                );
            }
        }
        Ok(-1)
    }

    /// Returns true if this bit set and `other` share at least one set bit.
    pub fn intersects(&self, other: &Self) -> bool {
        self.words
            .iter()
            .zip(&other.words)
            .any(|(left, right)| left & right != 0)
    }

    /// Replaces this value with its bitwise intersection with `other`.
    pub fn and(&mut self, other: &Self) {
        for (index, word) in self.words.iter_mut().enumerate() {
            *word &= other.words.get(index).copied().unwrap_or(0);
        }
        self.trim_trailing_zero_words();
    }

    /// Adds every set bit from `other` to this value.
    pub fn or(&mut self, other: &Self) -> Result<(), BitSetError> {
        self.ensure_words_len(
            other.words.len(),
            other.length().saturating_sub(1).min(i32::MAX as usize) as i32,
        )?;
        for (word, other_word) in self.words.iter_mut().zip(&other.words) {
            *word |= other_word;
        }
        Ok(())
    }

    /// Replaces this value with its bitwise exclusive-or with `other`.
    pub fn xor(&mut self, other: &Self) -> Result<(), BitSetError> {
        self.ensure_words_len(
            other.words.len(),
            other.length().saturating_sub(1).min(i32::MAX as usize) as i32,
        )?;
        for (word, other_word) in self.words.iter_mut().zip(&other.words) {
            *word ^= other_word;
        }
        self.trim_trailing_zero_words();
        Ok(())
    }

    /// Clears every bit present in `other` from this value.
    pub fn and_not(&mut self, other: &Self) {
        for (word, other_word) in self.words.iter_mut().zip(&other.words) {
            *word &= !other_word;
        }
        self.trim_trailing_zero_words();
    }

    /// Returns the used 64-bit words in Java's `toLongArray()` order.
    pub fn to_long_array(&self) -> Result<Vec<i64>, BitSetError> {
        let mut words = Vec::new();
        words
            .try_reserve_exact(self.words.len())
            .map_err(|_| BitSetError::AllocationFailed(self.length().saturating_sub(1) as i32))?;
        words.extend(self.words.iter().map(|word| *word as i64));
        Ok(words)
    }

    /// Returns the Java `BitSet.hashCode()` value.
    pub fn java_hash_code(&self) -> i32 {
        let hash = self
            .words
            .iter()
            .enumerate()
            .fold(1234_u64, |hash, (index, word)| {
                hash ^ word.wrapping_mul(index as u64 + 1)
            });
        ((hash >> 32) ^ hash) as i32
    }

    /// Iterates over set-bit indexes in ascending order.
    pub fn iter_set_bits(&self) -> impl Iterator<Item = usize> + '_ {
        let mut next_word_index = 0;
        let mut active_word_index = 0;
        let mut remaining = 0_u64;
        std::iter::from_fn(move || {
            loop {
                if remaining != 0 {
                    let bit = remaining.trailing_zeros() as usize;
                    remaining &= remaining - 1;
                    return Some(active_word_index * 64 + bit);
                }
                let word = self.words.get(next_word_index).copied()?;
                active_word_index = next_word_index;
                next_word_index += 1;
                remaining = word;
            }
        })
    }

    pub fn cardinality(&self) -> usize {
        self.words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }

    /// Returns one plus the highest set-bit index, or zero for an empty set.
    pub fn length(&self) -> usize {
        self.words.last().map_or(0, |word| {
            (self.words.len() - 1) * 64 + (64 - word.leading_zeros() as usize)
        })
    }

    pub fn is_empty(&self) -> bool {
        self.words.is_empty()
    }

    fn checked_index(index: i32) -> Result<usize, BitSetError> {
        usize::try_from(index).map_err(|_| BitSetError::NegativeIndex(index))
    }

    fn checked_range(from: i32, to: i32) -> Result<(usize, usize), BitSetError> {
        let from_index = Self::checked_index(from)?;
        if to < 0 {
            return Err(BitSetError::NegativeIndex(to));
        }
        if from > to {
            return Err(BitSetError::InvalidRange { from, to });
        }
        Ok((from_index, to as usize))
    }

    fn mutate_range(
        &mut self,
        from: i32,
        to: i32,
        may_grow: bool,
        operation: fn(u64, u64) -> u64,
    ) -> Result<(), BitSetError> {
        let (from, to) = Self::checked_range(from, to)?;
        if from == to {
            return Ok(());
        }
        let first_word = from / 64;
        let mut last_word = (to - 1) / 64;
        if may_grow {
            self.ensure_words_len(last_word + 1, (to - 1) as i32)?;
        } else if first_word >= self.words.len() {
            return Ok(());
        } else {
            last_word = last_word.min(self.words.len() - 1);
        }
        for word_index in first_word..=last_word {
            let word_start = word_index * 64;
            let start_bit = from.saturating_sub(word_start).min(64);
            let end_bit = (to - word_start).min(64);
            let lower_mask = u64::MAX << start_bit;
            let upper_mask = if end_bit == 64 {
                u64::MAX
            } else {
                (1_u64 << end_bit) - 1
            };
            self.words[word_index] = operation(self.words[word_index], lower_mask & upper_mask);
        }
        self.trim_trailing_zero_words();
        Ok(())
    }

    fn java_index(index: usize) -> Result<i32, BitSetError> {
        i32::try_from(index).map_err(|_| BitSetError::IndexOverflow(index))
    }

    fn ensure_words_len(
        &mut self,
        words_len: usize,
        allocation_index: i32,
    ) -> Result<(), BitSetError> {
        let java_capacity_words = if words_len > self.java_capacity_words {
            self.java_capacity_words.saturating_mul(2).max(words_len)
        } else {
            self.java_capacity_words
        };
        if words_len > self.words.len() {
            self.words
                .try_reserve(words_len - self.words.len())
                .map_err(|_| BitSetError::AllocationFailed(allocation_index))?;
            self.words.resize(words_len, 0);
        }
        self.java_capacity_words = java_capacity_words;
        Ok(())
    }

    fn trim_trailing_zero_words(&mut self) {
        while self.words.last() == Some(&0) {
            self.words.pop();
        }
    }

    pub fn java_to_string(&self) -> String {
        let mut output = String::from("{");
        let mut first = true;
        for (word_index, word) in self.words.iter().copied().enumerate() {
            let mut remaining = word;
            while remaining != 0 {
                let bit = remaining.trailing_zeros() as usize;
                if !first {
                    output.push_str(", ");
                }
                first = false;
                output.push_str(&(word_index * 64 + bit).to_string());
                remaining &= remaining - 1;
            }
        }
        output.push('}');
        output
    }
}

mod bitset_base64 {
    use super::BitSet;
    use crate::SharedValue;
    use crate::serde_fastjson;
    use base64::{
        Engine as _, alphabet, engine::general_purpose::GeneralPurpose,
        engine::general_purpose::GeneralPurposeConfig, engine::general_purpose::NO_PAD,
        engine::general_purpose::STANDARD,
    };
    use serde::{Deserializer, Serializer};

    // Java's Basic decoder ignores unused low bits in the final Base64 symbol.
    const JAVA_STANDARD: GeneralPurpose = GeneralPurpose::new(
        &alphabet::STANDARD,
        GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true),
    );
    const JAVA_STANDARD_NO_PAD: GeneralPurpose = GeneralPurpose::new(
        &alphabet::STANDARD,
        NO_PAD.with_decode_allow_trailing_bits(true),
    );

    pub fn serialize<S>(
        bit_set: &Option<SharedValue<BitSet>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match bit_set {
            Some(bit_set) => {
                let bytes = bit_set
                    .with(BitSet::to_byte_array)
                    .map_err(serde::ser::Error::custom)?;
                serializer.serialize_str(&STANDARD.encode(bytes))
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<BitSet>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_fastjson::deserialize_nullable_string(deserializer)?;
        value
            .map(|text| {
                JAVA_STANDARD
                    .decode(&text)
                    .or_else(|_| JAVA_STANDARD_NO_PAD.decode(&text))
                    .map(|bytes| BitSet::from_byte_array(&bytes))
                    .map_err(serde::de::Error::custom)
            })
            .transpose()
    }
}

fn is_empty_bitset(value: &Option<SharedValue<BitSet>>) -> bool {
    value
        .as_ref()
        .is_none_or(|bit_set| bit_set.with(BitSet::is_empty))
}

fn deserialize_batch_ack_list<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<Option<BatchAck>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    match raw.get().trim() {
        // Fastjson's List converter treats a Boolean token as a singleton null row.
        "true" | "false" => Ok(Some(vec![None])),
        raw => {
            let mut deserializer = serde_json::Deserializer::from_str(raw);
            let rows =
                serde_fastjson::deserialize_optional_nullable_vec_or_single(&mut deserializer)
                    .map_err(de::Error::custom)?;
            deserializer.end().map_err(de::Error::custom)?;
            Ok(rows)
        }
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::BatchAckMessageRequestBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<BatchAckMessageRequestBody>(); }
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BatchAckMessageRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acks: Option<SharedValue<Vec<Option<SharedValue<BatchAck>>>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchAckMessageRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    broker_name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_batch_ack_list")]
    acks: Option<Vec<Option<BatchAck>>>,
}

impl<'de> Deserialize<'de> for BatchAckMessageRequestBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: BatchAckMessageRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            broker_name: fields.broker_name,
            acks: fields.acks.map(|acks| {
                SharedValue::new(
                    acks.into_iter()
                        .map(|ack| ack.map(SharedValue::new))
                        .collect(),
                )
            }),
        })
    }
}

impl BatchAckMessageRequestBody {
    /// Encodes the inherited Java static encode(Object) form, preserving its null result.
    pub fn java_encode_optional(
        value: Option<&Self>,
    ) -> Result<Option<Vec<u8>>, serde_json::Error> {
        value.map(Self::java_encode).transpose()
    }

    /// Encodes the inherited Java static toJson(Object, boolean) form.
    pub fn java_to_json_optional(
        value: Option<&Self>,
        pretty_format: bool,
    ) -> Result<String, serde_json::Error> {
        value.map_or_else(
            || Ok("null".to_owned()),
            |value| value.java_to_json(pretty_format),
        )
    }

    /// Encodes the inherited Java RemotingSerializable.toJson(boolean) form.
    pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
        crate::route::java_fastjson_json(self, pretty_format)
    }

    /// Encodes the inherited Java RemotingSerializable.encode() UTF-8 bytes.
    pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        Ok(self.java_to_json(false)?.into_bytes())
    }

    /// Encodes with the supported inherited Fastjson2 writer features.
    pub fn java_encode_with_features(
        &self,
        features: &[JavaJsonWriterFeature],
    ) -> Result<Vec<u8>, serde_json::Error> {
        Ok(crate::route::java_fastjson_json_with_features(self, false, features)?.into_bytes())
    }

    /// Decodes the inherited Java RemotingSerializable.decode form.
    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }

    /// Decodes the nullable inherited Java utility, including null bytes and a JSON null root.
    pub fn java_decode_nullable(data: Option<&[u8]>) -> Result<Option<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_slice_nullable(data)
    }

    /// Decodes the inherited Java RemotingSerializable.decodeList form.
    pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }

    /// Decodes a nullable Java list, preserving null roots and members.
    pub fn java_decode_list_nullable(
        data: Option<&[u8]>,
    ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
        serde_fastjson::from_fastjson_list_nullable(data)
    }

    /// Decodes the inherited Java RemotingSerializable.fromJson form.
    pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_str(json)
    }

    /// Decodes the nullable inherited Java fromJson utility form.
    pub fn java_from_json_nullable(json: Option<&str>) -> Result<Option<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_str_nullable(json)
    }

    pub fn get_broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }

    pub fn set_broker_name(&mut self, broker_name: Option<String>) {
        self.broker_name = broker_name;
    }

    /// Returns a handle to the same mutable List object assigned to the Java DTO.
    pub fn get_acks(&self) -> Option<SharedValue<Vec<Option<SharedValue<BatchAck>>>>> {
        self.acks.clone()
    }

    pub fn set_acks(&mut self, acks: Option<SharedValue<Vec<Option<SharedValue<BatchAck>>>>>) {
        self.acks = acks;
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::BatchAck;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<BatchAck>(); }
/// ```
#[derive(Debug, Clone, Serialize, Default)]
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
    #[serde(
        rename = "b",
        alias = "bitSet",
        with = "bitset_base64",
        skip_serializing_if = "is_empty_bitset"
    )]
    pub bit_set: Option<SharedValue<BitSet>>,
}

#[derive(Deserialize)]
struct BatchAckFields {
    #[serde(
        default,
        rename = "c",
        alias = "consumerGroup",
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    consumer_group: Option<String>,
    #[serde(
        default,
        rename = "t",
        alias = "topic",
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
    #[serde(
        default,
        rename = "r",
        alias = "retry",
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    retry: Option<String>,
    #[serde(
        default,
        rename = "so",
        alias = "startOffset",
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    start_offset: i64,
    #[serde(
        default,
        rename = "q",
        alias = "queueId",
        deserialize_with = "deserialize_fastjson_i32_object_as_zero"
    )]
    queue_id: i32,
    #[serde(
        default,
        rename = "rq",
        alias = "reviveQueueId",
        deserialize_with = "deserialize_fastjson_i32_object_as_zero"
    )]
    revive_queue_id: i32,
    #[serde(
        default,
        rename = "pt",
        alias = "popTime",
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    pop_time: i64,
    #[serde(
        default,
        rename = "it",
        alias = "invisibleTime",
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    invisible_time: i64,
    #[serde(default, rename = "b", alias = "bitSet", with = "bitset_base64")]
    bit_set: Option<BitSet>,
}

impl<'de> Deserialize<'de> for BatchAck {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: BatchAckFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            consumer_group: fields.consumer_group,
            topic: fields.topic,
            retry: fields.retry,
            start_offset: fields.start_offset,
            queue_id: fields.queue_id,
            revive_queue_id: fields.revive_queue_id,
            pop_time: fields.pop_time,
            invisible_time: fields.invisible_time,
            bit_set: fields.bit_set.map(SharedValue::new),
        })
    }
}

impl BatchAck {
    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, consumer_group: Option<String>) {
        self.consumer_group = consumer_group;
    }

    pub fn get_topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }

    pub fn set_topic(&mut self, topic: Option<String>) {
        self.topic = topic;
    }

    pub fn get_retry(&self) -> Option<&str> {
        self.retry.as_deref()
    }

    pub fn set_retry(&mut self, retry: Option<String>) {
        self.retry = retry;
    }

    pub fn get_start_offset(&self) -> i64 {
        self.start_offset
    }

    pub fn set_start_offset(&mut self, start_offset: i64) {
        self.start_offset = start_offset;
    }

    pub fn get_queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }

    pub fn get_revive_queue_id(&self) -> i32 {
        self.revive_queue_id
    }

    pub fn set_revive_queue_id(&mut self, revive_queue_id: i32) {
        self.revive_queue_id = revive_queue_id;
    }

    pub fn get_pop_time(&self) -> i64 {
        self.pop_time
    }

    pub fn set_pop_time(&mut self, pop_time: i64) {
        self.pop_time = pop_time;
    }

    pub fn get_invisible_time(&self) -> i64 {
        self.invisible_time
    }

    pub fn set_invisible_time(&mut self, invisible_time: i64) {
        self.invisible_time = invisible_time;
    }

    pub fn get_bit_set(&self) -> Option<SharedValue<BitSet>> {
        self.bit_set.clone()
    }

    pub fn set_bit_set(&mut self, bit_set: Option<SharedValue<BitSet>>) {
        self.bit_set = bit_set;
    }
}

impl std::fmt::Display for BatchAck {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let consumer_group = self.consumer_group.as_deref().unwrap_or("null");
        let topic = self.topic.as_deref().unwrap_or("null");
        let retry = self.retry.as_deref().unwrap_or("null");
        let bit_set = self.bit_set.as_ref().map_or_else(
            || "null".to_owned(),
            |bit_set| bit_set.with(BitSet::java_to_string),
        );
        write!(
            formatter,
            "BatchAck{{consumerGroup='{consumer_group}', topic='{topic}', retry='{retry}', startOffset={}, queueId={}, reviveQueueId={}, popTime={}, invisibleTime={}, bitSet={bit_set}}}",
            self.start_offset,
            self.queue_id,
            self.revive_queue_id,
            self.pop_time,
            self.invisible_time
        )
    }
}

pub type MessageRequestModeGroup =
    std::collections::BTreeMap<String, SharedValue<SetMessageRequestModeRequestBody>>;
pub type SharedMessageRequestModeGroup = SharedValue<MessageRequestModeGroup>;
pub type MessageRequestModeMap = std::collections::BTreeMap<String, SharedMessageRequestModeGroup>;
pub type SharedMessageRequestModeMap = SharedValue<MessageRequestModeMap>;

#[derive(Debug, Clone, Serialize)]
/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::MessageRequestModeSerializeWrapper;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<MessageRequestModeSerializeWrapper>(); }
/// ```
#[serde(rename_all = "camelCase")]
pub struct MessageRequestModeSerializeWrapper {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_request_mode_map: Option<SharedMessageRequestModeMap>,
}

impl Default for MessageRequestModeSerializeWrapper {
    fn default() -> Self {
        Self {
            message_request_mode_map: Some(SharedValue::new(std::collections::BTreeMap::new())),
        }
    }
}

impl MessageRequestModeSerializeWrapper {
    pub fn get_message_request_mode_map(&self) -> Option<SharedMessageRequestModeMap> {
        self.message_request_mode_map.clone()
    }

    pub fn get_message_request_mode_map_mut(&mut self) -> Option<&mut SharedMessageRequestModeMap> {
        self.message_request_mode_map.as_mut()
    }

    pub fn set_message_request_mode_map(
        &mut self,
        message_request_mode_map: Option<SharedMessageRequestModeMap>,
    ) {
        self.message_request_mode_map = message_request_mode_map;
    }
}

fn default_message_request_mode_map() -> Option<
    std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, SetMessageRequestModeRequestBody>,
    >,
> {
    Some(std::collections::BTreeMap::new())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageRequestModeSerializeWrapperFields {
    #[serde(default = "default_message_request_mode_map")]
    message_request_mode_map: Option<
        std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<String, SetMessageRequestModeRequestBody>,
        >,
    >,
}

impl<'de> Deserialize<'de> for MessageRequestModeSerializeWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: MessageRequestModeSerializeWrapperFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        let message_request_mode_map = fields.message_request_mode_map.map(|topics| {
            SharedValue::new(
                topics
                    .into_iter()
                    .map(|(topic, groups)| {
                        let groups = SharedValue::new(
                            groups
                                .into_iter()
                                .map(|(group, body)| (group, SharedValue::new(body)))
                                .collect(),
                        );
                        (topic, groups)
                    })
                    .collect(),
            )
        });
        Ok(Self {
            message_request_mode_map,
        })
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::LockBatchRequestBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<LockBatchRequestBody>(); }
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LockBatchRequestBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default)]
    pub only_this_broker: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mq_set: Option<SharedMessageQueueSet>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LockBatchRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    consumer_group: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    only_this_broker: bool,
    #[serde(
        default = "default_message_queue_set",
        deserialize_with = "deserialize_optional_message_queue_set"
    )]
    mq_set: Option<MessageQueueSet>,
}

impl<'de> Deserialize<'de> for LockBatchRequestBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: LockBatchRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            consumer_group: fields.consumer_group,
            client_id: fields.client_id,
            only_this_broker: fields.only_this_broker,
            mq_set: fields.mq_set.map(SharedValue::new),
        })
    }
}

impl Default for LockBatchRequestBody {
    fn default() -> Self {
        Self {
            consumer_group: None,
            client_id: None,
            only_this_broker: false,
            mq_set: default_message_queue_set().map(SharedValue::new),
        }
    }
}

impl LockBatchRequestBody {
    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, consumer_group: Option<String>) {
        self.consumer_group = consumer_group;
    }

    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, client_id: Option<String>) {
        self.client_id = client_id;
    }

    pub fn is_only_this_broker(&self) -> bool {
        self.only_this_broker
    }

    pub fn set_only_this_broker(&mut self, only_this_broker: bool) {
        self.only_this_broker = only_this_broker;
    }

    /// Returns a handle to the same mutable Set object assigned to the Java DTO.
    pub fn get_mq_set(&self) -> Option<SharedMessageQueueSet> {
        self.mq_set.clone()
    }

    pub fn set_mq_set(&mut self, mq_set: Option<SharedMessageQueueSet>) {
        self.mq_set = mq_set;
    }
}

impl std::fmt::Display for LockBatchRequestBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "LockBatchRequestBody{{consumerGroup={}, clientId={}, onlyThisBroker={}, mqSet={}}}",
            self.consumer_group.as_deref().unwrap_or("null"),
            self.client_id.as_deref().unwrap_or("null"),
            self.only_this_broker,
            java_message_queue_set_text(&self.mq_set)
        )
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::UnlockBatchRequestBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<UnlockBatchRequestBody>(); }
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnlockBatchRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    pub only_this_broker: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mq_set: Option<SharedMessageQueueSet>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UnlockBatchRequestBodyFields {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    consumer_group: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    client_id: Option<String>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    only_this_broker: bool,
    #[serde(
        default = "default_message_queue_set",
        deserialize_with = "deserialize_optional_message_queue_set"
    )]
    mq_set: Option<MessageQueueSet>,
}

impl<'de> Deserialize<'de> for UnlockBatchRequestBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: UnlockBatchRequestBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            consumer_group: fields.consumer_group,
            client_id: fields.client_id,
            only_this_broker: fields.only_this_broker,
            mq_set: fields.mq_set.map(SharedValue::new),
        })
    }
}

impl Default for UnlockBatchRequestBody {
    fn default() -> Self {
        Self {
            consumer_group: None,
            client_id: None,
            only_this_broker: false,
            mq_set: default_message_queue_set().map(SharedValue::new),
        }
    }
}

impl UnlockBatchRequestBody {
    pub fn get_consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn set_consumer_group(&mut self, consumer_group: Option<String>) {
        self.consumer_group = consumer_group;
    }

    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn set_client_id(&mut self, client_id: Option<String>) {
        self.client_id = client_id;
    }

    pub fn is_only_this_broker(&self) -> bool {
        self.only_this_broker
    }

    pub fn set_only_this_broker(&mut self, only_this_broker: bool) {
        self.only_this_broker = only_this_broker;
    }

    /// Returns a handle to the same mutable Set object assigned to the Java DTO.
    pub fn get_mq_set(&self) -> Option<SharedMessageQueueSet> {
        self.mq_set.clone()
    }

    pub fn set_mq_set(&mut self, mq_set: Option<SharedMessageQueueSet>) {
        self.mq_set = mq_set;
    }
}

impl std::fmt::Display for UnlockBatchRequestBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "UnlockBatchRequestBody{{consumerGroup={}, clientId={}, onlyThisBroker={}, mqSet={}}}",
            self.consumer_group.as_deref().unwrap_or("null"),
            self.client_id.as_deref().unwrap_or("null"),
            self.only_this_broker,
            java_message_queue_set_text(&self.mq_set)
        )
    }
}

fn java_message_queue_set_text(queues: &Option<SharedMessageQueueSet>) -> String {
    match queues {
        None => "null".to_owned(),
        Some(queues) => queues.with(|queues| {
            format!(
                "[{}]",
                queues
                    .iter()
                    .map(|queue| queue
                        .as_ref()
                        .map(|message_queue| message_queue.with(ToString::to_string))
                        .unwrap_or_else(|| "null".to_owned()))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }),
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::LockBatchResponseBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<LockBatchResponseBody>(); }
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LockBatchResponseBody {
    #[serde(rename = "lockOKMQSet", skip_serializing_if = "Option::is_none")]
    pub lock_okmq_set: Option<SharedMessageQueueSet>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LockBatchResponseBodyFields {
    #[serde(
        default = "default_message_queue_set",
        rename = "lockOKMQSet",
        deserialize_with = "deserialize_optional_message_queue_set"
    )]
    lock_okmq_set: Option<MessageQueueSet>,
}

impl<'de> Deserialize<'de> for LockBatchResponseBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: LockBatchResponseBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            lock_okmq_set: fields.lock_okmq_set.map(SharedValue::new),
        })
    }
}

impl Default for LockBatchResponseBody {
    fn default() -> Self {
        Self {
            lock_okmq_set: default_message_queue_set().map(SharedValue::new),
        }
    }
}

impl LockBatchResponseBody {
    /// Returns a handle to the same mutable Set object assigned to the Java DTO.
    pub fn get_lock_okmq_set(&self) -> Option<SharedMessageQueueSet> {
        self.lock_okmq_set.clone()
    }

    pub fn set_lock_okmq_set(&mut self, lock_okmq_set: Option<SharedMessageQueueSet>) {
        self.lock_okmq_set = lock_okmq_set;
    }
}

/// Java inherits identity-based equality and hashing for this DTO.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerListByGroupResponseBody;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<ConsumerListByGroupResponseBody>(); }
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerListByGroupResponseBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_id_list: Option<SharedValue<Vec<Option<String>>>>,
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

                Ok(ConsumerListByGroupResponseBody {
                    consumer_id_list: consumer_id_list.map(SharedValue::new),
                })
            }
        }

        deserializer.deserialize_map(ConsumerListVisitor)
    }
}

impl ConsumerListByGroupResponseBody {
    /// Returns a handle to the same mutable List object assigned to the Java DTO.
    pub fn get_consumer_id_list(&self) -> Option<SharedValue<Vec<Option<String>>>> {
        self.consumer_id_list.clone()
    }

    pub fn set_consumer_id_list(
        &mut self,
        consumer_id_list: Option<SharedValue<Vec<Option<String>>>>,
    ) {
        self.consumer_id_list = consumer_id_list;
    }
}

/// Java `GetConsumerStatusBody` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::GetConsumerStatusBody;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<GetConsumerStatusBody>();
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetConsumerStatusBody {
    #[serde(with = "message_queue_offset_table")]
    pub message_queue_table: Option<SharedQueueOffsetEntries>,
    #[serde(with = "nested_message_queue_offset_table")]
    pub consumer_table: Option<SharedConsumerQueueOffsets>,
}

impl GetConsumerStatusBody {
    pub fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut consumer_entries = Vec::new();
        if let Some(consumer_table) = &self.consumer_table {
            consumer_table.with(|consumer_table| {
                for (consumer, entries) in consumer_table {
                    if let Some(entries) = entries {
                        let encoded =
                            entries.with(|entries| encode_java_queue_table_text(entries))?;
                        consumer_entries.push(format!(
                            "{}:{}",
                            serde_json::to_string(consumer)?,
                            encoded
                        ));
                    }
                }
                Ok::<_, serde_json::Error>(())
            })?;
        }
        let mut fields = Vec::new();
        if self.consumer_table.is_some() {
            fields.push(format!(
                "\"consumerTable\":{{{}}}",
                consumer_entries.join(",")
            ));
        }
        if let Some(entries) = &self.message_queue_table {
            fields.push(format!(
                "\"messageQueueTable\":{}",
                entries.with(|entries| encode_java_queue_table_text(entries))?
            ));
        }
        Ok(format!("{{{}}}", fields.join(",")).into_bytes())
    }

    /// Decode the Java Fastjson body syntax, including MessageQueue objects used as map keys.
    pub fn decode_java_json(input: &str) -> Result<Self, serde_json::Error> {
        let normalized = serde_fastjson::normalize_json_input(input);
        let normalized = normalize_java_object_map_keys(&normalized)?;
        serde_json::from_str(&normalized)
    }

    pub fn get_message_queue_table(&self) -> Option<SharedQueueOffsetEntries> {
        self.message_queue_table.clone()
    }

    pub fn get_message_queue_table_mut(&mut self) -> Option<SharedQueueOffsetEntries> {
        self.get_message_queue_table()
    }

    pub fn set_message_queue_table(&mut self, value: Option<SharedQueueOffsetEntries>) {
        self.message_queue_table = value;
    }

    pub fn get_consumer_table(&self) -> Option<SharedConsumerQueueOffsets> {
        self.consumer_table.clone()
    }

    pub fn get_consumer_table_mut(&mut self) -> Option<SharedConsumerQueueOffsets> {
        self.get_consumer_table()
    }

    pub fn set_consumer_table(&mut self, value: Option<SharedConsumerQueueOffsets>) {
        self.consumer_table = value;
    }
}

impl Default for GetConsumerStatusBody {
    fn default() -> Self {
        Self {
            message_queue_table: Some(SharedValue::new(Vec::new())),
            consumer_table: Some(SharedValue::new(std::collections::BTreeMap::new())),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
struct GetConsumerStatusBodyFields {
    #[serde(with = "message_queue_offset_table")]
    message_queue_table: Option<SharedQueueOffsetEntries>,
    #[serde(with = "nested_message_queue_offset_table")]
    consumer_table: Option<SharedConsumerQueueOffsets>,
}

impl Default for GetConsumerStatusBodyFields {
    fn default() -> Self {
        Self {
            message_queue_table: Some(SharedValue::new(Vec::new())),
            consumer_table: Some(SharedValue::new(std::collections::BTreeMap::new())),
        }
    }
}

impl<'de> Deserialize<'de> for GetConsumerStatusBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let fields: GetConsumerStatusBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            message_queue_table: fields.message_queue_table,
            consumer_table: fields.consumer_table,
        })
    }
}

/// Java `ProcessQueueInfo` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerProcessQueueInfo;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<ConsumerProcessQueueInfo>();
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerProcessQueueInfo {
    pub commit_offset: i64,
    pub cached_msg_min_offset: i64,
    pub cached_msg_max_offset: i64,
    pub cached_msg_count: i32,
    #[serde(rename = "cachedMsgSizeInMiB")]
    pub cached_msg_size_in_mi_b: i32,
    pub transaction_msg_min_offset: i64,
    pub transaction_msg_max_offset: i64,
    pub transaction_msg_count: i32,
    pub locked: bool,
    pub try_unlock_times: i64,
    pub last_lock_timestamp: i64,
    pub droped: bool,
    pub last_pull_timestamp: i64,
    pub last_consume_timestamp: i64,
}

/// Java `PopProcessQueueInfo` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerPopProcessQueueInfo;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<ConsumerPopProcessQueueInfo>();
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerPopProcessQueueInfo {
    pub wait_ack_count: i32,
    pub droped: bool,
    pub last_pop_timestamp: i64,
}

/// Java `ConsumeStatus` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerConsumeStatus;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<ConsumerConsumeStatus>();
/// ```
#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConsumeStatus {
    #[serde(rename = "pullRT")]
    pub pull_rt: f64,
    #[serde(rename = "pullTPS")]
    pub pull_tps: f64,
    #[serde(rename = "consumeRT")]
    pub consume_rt: f64,
    #[serde(rename = "consumeOKTPS")]
    pub consume_ok_tps: f64,
    #[serde(rename = "consumeFailedTPS")]
    pub consume_failed_tps: f64,
    pub consume_failed_msgs: i64,
}

/// Pinned Java `ConsumerRunningInfo` JSON body. Fastjson encodes the
/// `MessageQueue` map keys as object values rather than quoted strings. The
/// corresponding Java DTO inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumerRunningInfoBody;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<ConsumerRunningInfoBody>();
/// ```
#[derive(Debug, Clone, Default)]
pub struct ConsumerRunningInfoBody {
    pub properties: BTreeMap<String, String>,
    pub subscription_set: Vec<SubscriptionData>,
    pub mq_table: Vec<(MessageQueue, ConsumerProcessQueueInfo)>,
    pub mq_pop_table: Vec<(MessageQueue, ConsumerPopProcessQueueInfo)>,
    pub status_table: BTreeMap<String, ConsumerConsumeStatus>,
    pub user_consumer_info: BTreeMap<String, String>,
    pub jstack: Option<String>,
}

impl ConsumerRunningInfoBody {
    pub fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut subscription_set = self.subscription_set.clone();
        subscription_set.sort_by(|left, right| left.java_compare_to(right).cmp(&0));
        let subscription_set = subscription_set
            .iter()
            .map(encode_java_subscription_data)
            .collect::<Result<Vec<_>, _>>()?;
        let mut queues = self.mq_table.iter().collect::<Vec<_>>();
        queues.sort_by(|left, right| left.0.cmp(&right.0));
        let mut mq_table = Vec::with_capacity(queues.len());
        for (queue, info) in queues {
            mq_table.push((queue.clone(), serde_json::to_string(info)?));
        }
        let mut pop_queues = self.mq_pop_table.iter().collect::<Vec<_>>();
        pop_queues.sort_by(|left, right| left.0.cmp(&right.0));
        let mut mq_pop_table = Vec::with_capacity(pop_queues.len());
        for (queue, info) in pop_queues {
            mq_pop_table.push((queue.clone(), serde_json::to_string(info)?));
        }

        let mut fields = Vec::with_capacity(7);
        if let Some(jstack) = &self.jstack {
            fields.push(format!("\"jstack\":{}", serde_json::to_string(jstack)?));
        }
        fields.push(format!(
            "\"mqPopTable\":{}",
            encode_java_object_key_map(&mq_pop_table)?
        ));
        fields.push(format!(
            "\"mqTable\":{}",
            encode_java_object_key_map(&mq_table)?
        ));
        fields.push(format!(
            "\"properties\":{}",
            serde_json::to_string(&self.properties)?
        ));
        fields.push(format!(
            "\"statusTable\":{}",
            serde_json::to_string(&self.status_table)?
        ));
        fields.push(format!(
            "\"subscriptionSet\":{}",
            serde_json::to_string(&subscription_set)?
        ));
        fields.push(format!(
            "\"userConsumerInfo\":{}",
            serde_json::to_string(&self.user_consumer_info)?
        ));
        Ok(format!("{{{}}}", fields.join(",")).into_bytes())
    }

    pub fn decode_java_json(input: &str) -> Result<serde_json::Value, serde_json::Error> {
        let normalized = normalize_java_object_map_keys(input)?;
        serde_json::from_str(&normalized)
    }
}

fn encode_java_subscription_data(
    subscription: &SubscriptionData,
) -> Result<serde_json::Value, serde_json::Error> {
    let mut fields = serde_json::Map::new();
    fields.insert(
        "classFilterMode".into(),
        serde_json::Value::Bool(subscription.class_filter_mode),
    );
    if let Some(topic) = &subscription.topic {
        fields.insert("topic".into(), serde_json::Value::String(topic.clone()));
    }
    if let Some(sub_string) = &subscription.sub_string {
        fields.insert(
            "subString".into(),
            serde_json::Value::String(sub_string.clone()),
        );
    }
    if let Some(tags) = &subscription.tags_set {
        let mut tags = tags.snapshot().into_iter().collect::<Vec<_>>();
        tags.sort_by(java_optional_string_cmp);
        fields.insert("tagsSet".into(), serde_json::to_value(tags)?);
    }
    if let Some(codes) = &subscription.code_set {
        let mut codes = codes.snapshot().into_iter().collect::<Vec<_>>();
        codes.sort();
        fields.insert("codeSet".into(), serde_json::to_value(codes)?);
    }
    fields.insert(
        "subVersion".into(),
        serde_json::Value::Number(subscription.sub_version.into()),
    );
    if let Some(expression_type) = &subscription.expression_type {
        fields.insert(
            "expressionType".into(),
            serde_json::Value::String(expression_type.clone()),
        );
    }
    Ok(serde_json::Value::Object(fields))
}

fn java_optional_string_cmp(left: &Option<String>, right: &Option<String>) -> std::cmp::Ordering {
    match (left, right) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(left), Some(right)) => left.encode_utf16().cmp(right.encode_utf16()),
    }
}

fn encode_java_object_key_map(
    entries: &[(MessageQueue, String)],
) -> Result<String, serde_json::Error> {
    let parts = entries
        .iter()
        .map(|(queue, value)| Ok(format!("{}:{value}", encode_java_message_queue_key(queue)?)))
        .collect::<Result<Vec<_>, serde_json::Error>>()?;
    Ok(format!("{{{}}}", parts.join(",")))
}

fn encode_java_message_queue_key(queue: &MessageQueue) -> Result<String, serde_json::Error> {
    let mut queue_fields = Vec::with_capacity(3);
    if let Some(broker_name) = queue.broker_name() {
        queue_fields.push(format!(
            "\"brokerName\":{}",
            serde_json::to_string(broker_name)?
        ));
    }
    queue_fields.push(format!(
        "\"queueId\":{}",
        serde_json::to_string(&queue.queue_id())?
    ));
    if let Some(topic) = queue.topic() {
        queue_fields.push(format!("\"topic\":{}", serde_json::to_string(topic)?));
    }
    Ok(format!("{{{}}}", queue_fields.join(",")))
}

fn normalize_java_object_map_keys(input: &str) -> Result<String, serde_json::Error> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ObjectKind {
        Root,
        ConsumerTable,
        QueueTable,
        Other,
    }

    struct ObjectFrame {
        kind: ObjectKind,
        last_key: Option<String>,
    }

    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut objects = Vec::<ObjectFrame>::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'"' {
            let end = skip_json_string(bytes, index).unwrap_or(bytes.len());
            let is_key = bytes.get(skip_json_whitespace(bytes, end)) == Some(&b':');
            if is_key {
                if objects
                    .last()
                    .is_some_and(|object| object.kind == ObjectKind::QueueTable)
                {
                    return Err(java_queue_key_error());
                }
                if let Some(object) = objects.last_mut() {
                    object.last_key = serde_json::from_str(&input[index..end]).ok();
                }
            }
            output.push_str(&input[index..end]);
            index = end;
            continue;
        }

        if bytes[index] == b'{'
            && objects
                .last()
                .is_some_and(|object| object.kind == ObjectKind::QueueTable)
            && matches!(previous_json_byte(bytes, index), Some(b'{' | b','))
        {
            if let Some(end) = find_json_object_end(bytes, index) {
                if bytes.get(skip_json_whitespace(bytes, end)) == Some(&b':') {
                    let encoded_key = serde_json::to_string(&input[index..end])?;
                    output.push_str(&encoded_key);
                    index = end;
                    continue;
                }
            }
        }

        if bytes[index] == b'{' {
            let kind = match objects.last() {
                None => ObjectKind::Root,
                Some(parent) if previous_json_byte(bytes, index) == Some(b':') => {
                    match (parent.kind, parent.last_key.as_deref()) {
                        (
                            ObjectKind::Root,
                            Some("messageQueueTable" | "offsetTable" | "mqTable" | "mqPopTable"),
                        ) => ObjectKind::QueueTable,
                        (ObjectKind::Root, Some("consumerTable")) => ObjectKind::ConsumerTable,
                        (ObjectKind::ConsumerTable, _) => ObjectKind::QueueTable,
                        _ => ObjectKind::Other,
                    }
                }
                Some(_) => ObjectKind::Other,
            };
            output.push('{');
            objects.push(ObjectFrame {
                kind,
                last_key: None,
            });
            index += 1;
            continue;
        }

        if bytes[index] == b'}' {
            output.push('}');
            objects.pop();
            index += 1;
            continue;
        }

        if bytes[index] == b',' {
            if let Some(object) = objects.last_mut() {
                object.last_key = None;
            }
            output.push(',');
            index += 1;
            continue;
        }

        let character = input[index..]
            .chars()
            .next()
            .expect("byte index is within a UTF-8 string");
        output.push(character);
        index += character.len_utf8();
    }
    Ok(output)
}

fn java_queue_key_error() -> serde_json::Error {
    serde_json::Error::io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "Fastjson MessageQueue map keys must be object values",
    ))
}

fn skip_json_string(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'"') {
        return None;
    }
    let mut index = start + 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index = index.checked_add(2)?,
            b'"' => return Some(index + 1),
            _ => index += 1,
        }
    }
    None
}

fn find_json_object_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut index = start;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => index = skip_json_string(bytes, index)?,
            b'{' => {
                depth += 1;
                index += 1;
            }
            b'}' => {
                depth = depth.checked_sub(1)?;
                index += 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => index += 1,
        }
    }
    None
}

fn skip_json_whitespace(bytes: &[u8], mut index: usize) -> usize {
    while bytes
        .get(index)
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t' | b'\r' | b'\n'))
    {
        index += 1;
    }
    index
}

fn previous_json_byte(bytes: &[u8], index: usize) -> Option<u8> {
    let mut previous = index.checked_sub(1)?;
    while bytes
        .get(previous)
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t' | b'\r' | b'\n'))
    {
        previous = previous.checked_sub(1)?;
    }
    bytes.get(previous).copied()
}

/// Java `ResetOffsetBody` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ResetOffsetBody;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<ResetOffsetBody>();
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetOffsetBody {
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "optional_message_queue_offset_table"
    )]
    pub offset_table: Option<SharedQueueOffsetEntries>,
}

impl ResetOffsetBody {
    pub fn encode_java_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let Some(entries) = &self.offset_table else {
            return Ok(b"{}".to_vec());
        };
        let table = entries.with(|entries| encode_java_queue_table_text(entries))?;
        Ok(format!("{{\"offsetTable\":{table}}}").into_bytes())
    }

    pub fn decode_java_json(input: &str) -> Result<Self, serde_json::Error> {
        let normalized = serde_fastjson::normalize_json_input(input);
        let normalized = normalize_java_object_map_keys(&normalized)?;
        serde_json::from_str(&normalized)
    }

    pub fn get_offset_table(&self) -> Option<SharedQueueOffsetEntries> {
        self.offset_table.clone()
    }

    pub fn get_offset_table_mut(&mut self) -> Option<SharedQueueOffsetEntries> {
        self.get_offset_table()
    }

    pub fn set_offset_table(&mut self, value: Option<SharedQueueOffsetEntries>) {
        self.offset_table = value;
    }
}

impl Default for ResetOffsetBody {
    fn default() -> Self {
        Self {
            offset_table: Some(SharedValue::new(Vec::new())),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct ResetOffsetBodyFields {
    #[serde(with = "optional_message_queue_offset_table")]
    offset_table: Option<SharedQueueOffsetEntries>,
}

impl Default for ResetOffsetBodyFields {
    fn default() -> Self {
        Self {
            offset_table: Some(SharedValue::new(Vec::new())),
        }
    }
}

impl<'de> Deserialize<'de> for ResetOffsetBody {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let fields: ResetOffsetBodyFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            offset_table: fields.offset_table,
        })
    }
}

fn encode_java_queue_table_text(
    entries: &[(MessageQueue, Option<i64>)],
) -> Result<String, serde_json::Error> {
    let mut table = std::collections::BTreeMap::new();
    for (queue, offset) in entries {
        table.insert(queue.clone(), *offset);
    }
    let mut parts = Vec::with_capacity(entries.len());
    for (queue, offset) in table {
        let Some(offset) = offset else {
            continue;
        };
        let mut queue_fields = Vec::with_capacity(3);
        if let Some(broker_name) = queue.broker_name() {
            queue_fields.push(format!(
                "\"brokerName\":{}",
                serde_json::to_string(broker_name)?
            ));
        }
        queue_fields.push(format!(
            "\"queueId\":{}",
            serde_json::to_string(&queue.queue_id())?
        ));
        if let Some(topic) = queue.topic() {
            queue_fields.push(format!("\"topic\":{}", serde_json::to_string(topic)?));
        }
        let queue_key = format!("{{{}}}", queue_fields.join(","));
        parts.push(format!("{queue_key}:{}", serde_json::to_string(&offset)?));
    }
    Ok(format!("{{{}}}", parts.join(",")))
}

pub type QueueOffsetEntries = Vec<(MessageQueue, Option<i64>)>;
pub type NullableQueueOffsetEntries = QueueOffsetEntries;
pub type SharedQueueOffsetEntries = SharedValue<QueueOffsetEntries>;
pub type ConsumerQueueOffsets =
    std::collections::BTreeMap<String, Option<SharedQueueOffsetEntries>>;
pub type SharedConsumerQueueOffsets = SharedValue<ConsumerQueueOffsets>;

impl PartialEq for SharedValue<QueueOffsetEntries> {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot() == other.snapshot()
    }
}

impl Eq for SharedValue<QueueOffsetEntries> {}

impl PartialEq for SharedValue<ConsumerQueueOffsets> {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot() == other.snapshot()
    }
}

impl Eq for SharedValue<ConsumerQueueOffsets> {}

#[derive(Debug, Clone, Copy)]
struct FastjsonLong(Option<i64>);

impl FastjsonLong {
    fn from_raw<E>(raw: &str) -> Result<Self, E>
    where
        E: de::Error,
    {
        if raw == "null" {
            return Ok(Self(None));
        }
        if raw == "true" {
            return Ok(Self(Some(1)));
        }
        if raw == "false" {
            return Ok(Self(Some(0)));
        }
        if raw.starts_with('"') {
            let text: String = serde_json::from_str(raw).map_err(E::custom)?;
            return Self::from_string::<E>(&text);
        }
        if raw.contains('.') && !raw.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
            return decimal_text_to_java_long::<E>(raw).map(|value| Self(Some(value)));
        }
        if raw.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
            let value = raw.parse::<f64>().map_err(E::custom)?;
            return Ok(Self(Some(value as i64)));
        }
        raw.parse::<i64>()
            .map(|value| Self(Some(value)))
            .map_err(E::custom)
    }

    fn from_string<E>(text: &str) -> Result<Self, E>
    where
        E: de::Error,
    {
        if text.is_empty() {
            return Ok(Self(None));
        }
        if let Ok(value) = text.parse::<i64>() {
            return Ok(Self(Some(value)));
        }
        if text.contains('.') && !text.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
            return decimal_text_to_java_long::<E>(text).map(|value| Self(Some(value)));
        }
        if text.bytes().any(|byte| matches!(byte, b'e' | b'E')) {
            let value = text.parse::<f64>().map_err(E::custom)?;
            return Ok(Self(Some(value as i64)));
        }
        Err(E::custom("string is not a Java Long value"))
    }
}

fn decimal_text_to_java_long<E>(text: &str) -> Result<i64, E>
where
    E: de::Error,
{
    let (negative, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let mut pieces = unsigned.split('.');
    let integer = pieces.next().unwrap_or_default();
    let fraction = pieces
        .next()
        .ok_or_else(|| E::custom("invalid decimal value"))?;
    if pieces.next().is_some()
        || (integer.is_empty() && fraction.is_empty())
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(E::custom("invalid decimal value"));
    }
    let mut magnitude = 0u64;
    for digit in integer.bytes() {
        magnitude = magnitude
            .wrapping_mul(10)
            .wrapping_add(u64::from(digit - b'0'));
    }
    let value = if negative {
        0u64.wrapping_sub(magnitude)
    } else {
        magnitude
    };
    Ok(value as i64)
}

mod message_queue_offset_table {
    use super::encode_table;
    use super::{MessageQueue, QueueOffsetEntries, SharedQueueOffsetEntries};
    use crate::SharedValue;
    use serde::de::MapAccess;
    use serde::de::Visitor;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;

    pub fn serialize<S>(
        entries: &Option<SharedQueueOffsetEntries>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match entries {
            Some(entries) => entries
                .with(|entries| encode_table(entries))
                .map_err(serde::ser::Error::custom)?
                .serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<SharedQueueOffsetEntries>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(encoded) = Option::<RawQueueOffsetEntries>::deserialize(deserializer)? else {
            return Ok(None);
        };
        decode_entries(encoded).map(SharedValue::new).map(Some)
    }

    pub(super) struct RawQueueOffsetEntries(Vec<(String, Option<i64>)>);

    pub(super) fn decode_entries<E>(encoded: RawQueueOffsetEntries) -> Result<QueueOffsetEntries, E>
    where
        E: serde::de::Error,
    {
        let mut table = std::collections::BTreeMap::new();
        for (key, offset) in encoded.0 {
            let queue: MessageQueue =
                serde_json::from_str(&key).map_err(serde::de::Error::custom)?;
            table.insert(queue, offset);
        }
        Ok(table.into_iter().collect())
    }

    impl<'de> Deserialize<'de> for RawQueueOffsetEntries {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct TableVisitor;

            impl<'de> Visitor<'de> for TableVisitor {
                type Value = RawQueueOffsetEntries;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a MessageQueue-keyed offset object")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut entries = Vec::new();
                    while let Some(key) = map.next_key::<String>()? {
                        let raw = map.next_value::<Box<serde_json::value::RawValue>>()?;
                        let offset = super::FastjsonLong::from_raw::<A::Error>(raw.get())?.0;
                        entries.push((key, offset));
                    }
                    Ok(RawQueueOffsetEntries(entries))
                }
            }

            deserializer.deserialize_map(TableVisitor)
        }
    }
}

fn encode_table(
    entries: &[(MessageQueue, Option<i64>)],
) -> Result<std::collections::BTreeMap<String, Option<i64>>, serde_json::Error> {
    entries
        .iter()
        .map(|(queue, offset)| Ok((serde_json::to_string(queue)?, *offset)))
        .collect()
}

mod nested_message_queue_offset_table {
    use super::SharedConsumerQueueOffsets;
    use super::encode_table;
    use crate::SharedValue;
    use serde::de::MapAccess;
    use serde::de::Visitor;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;

    pub fn serialize<S>(
        entries: &Option<SharedConsumerQueueOffsets>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match entries {
            Some(entries) => entries.with(|entries| {
                let mut encoded = std::collections::BTreeMap::new();
                for (key, values) in entries {
                    let values = values
                        .as_ref()
                        .map(|values| values.with(|values| encode_table(values)))
                        .transpose()
                        .map_err(serde::ser::Error::custom)?;
                    encoded.insert(key, values);
                }
                encoded.serialize(serializer)
            }),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<SharedConsumerQueueOffsets>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(raw) = Option::<RawConsumerQueueOffsets>::deserialize(deserializer)? else {
            return Ok(None);
        };
        let mut entries = std::collections::BTreeMap::new();
        for (consumer, table) in raw.0 {
            let table = table
                .map(super::message_queue_offset_table::decode_entries::<D::Error>)
                .map(|result| result.map(SharedValue::new))
                .transpose()?;
            entries.insert(consumer, table);
        }
        Ok(Some(SharedValue::new(entries)))
    }

    struct RawConsumerQueueOffsets(
        Vec<(
            String,
            Option<super::message_queue_offset_table::RawQueueOffsetEntries>,
        )>,
    );

    impl<'de> Deserialize<'de> for RawConsumerQueueOffsets {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct ConsumerTableVisitor;

            impl<'de> Visitor<'de> for ConsumerTableVisitor {
                type Value = RawConsumerQueueOffsets;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a consumer-keyed MessageQueue offset object")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut entries = Vec::new();
                    while let Some(consumer) = map.next_key::<String>()? {
                        entries.push(
                            (
                                consumer,
                                map.next_value::<Option<
                                    super::message_queue_offset_table::RawQueueOffsetEntries,
                                >>()?,
                            ),
                        );
                    }
                    Ok(RawConsumerQueueOffsets(entries))
                }
            }

            deserializer.deserialize_map(ConsumerTableVisitor)
        }
    }
}

mod optional_message_queue_offset_table {
    use super::{MessageQueue, SharedQueueOffsetEntries};
    use crate::SharedValue;
    use serde::de::{MapAccess, Visitor};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt;

    pub fn serialize<S>(
        entries: &Option<SharedQueueOffsetEntries>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match entries {
            Some(entries) => entries.with(|entries| {
                let mut table = std::collections::BTreeMap::new();
                for (queue, offset) in entries {
                    table.insert(
                        serde_json::to_string(queue).map_err(serde::ser::Error::custom)?,
                        offset,
                    );
                }
                table.serialize(serializer)
            }),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<SharedQueueOffsetEntries>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(encoded) = Option::<RawQueueOffsetTable>::deserialize(deserializer)? else {
            return Ok(None);
        };
        let mut table = std::collections::BTreeMap::new();
        for (key, offset) in encoded.0 {
            let queue: MessageQueue =
                serde_json::from_str(&key).map_err(serde::de::Error::custom)?;
            table.insert(queue, offset);
        }
        Ok(Some(SharedValue::new(table.into_iter().collect())))
    }

    struct RawQueueOffsetTable(Vec<(String, Option<i64>)>);

    impl<'de> Deserialize<'de> for RawQueueOffsetTable {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct TableVisitor;

            impl<'de> Visitor<'de> for TableVisitor {
                type Value = RawQueueOffsetTable;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a MessageQueue-keyed nullable offset object")
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut entries = Vec::new();
                    while let Some(key) = map.next_key::<String>()? {
                        let raw = map.next_value::<Box<serde_json::value::RawValue>>()?;
                        let offset = super::FastjsonLong::from_raw::<A::Error>(raw.get())?.0;
                        entries.push((key, offset));
                    }
                    Ok(RawQueueOffsetTable(entries))
                }
            }

            deserializer.deserialize_map(TableVisitor)
        }
    }
}

/// Java `ConsumeMessageDirectlyResult` inherits identity-based `Object.equals`.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::body::ConsumeMessageDirectlyResult;
/// fn requires_partial_eq<T: PartialEq>() {}
/// requires_partial_eq::<ConsumeMessageDirectlyResult>();
/// ```
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeMessageDirectlyResult {
    pub order: bool,
    pub auto_commit: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consume_result: Option<ConsumeMessageResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remark: Option<String>,
    pub spent_time_mills: i64,
}

impl Default for ConsumeMessageDirectlyResult {
    fn default() -> Self {
        Self {
            order: false,
            auto_commit: default_auto_commit(),
            consume_result: None,
            remark: None,
            spent_time_mills: 0,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConsumeMessageDirectlyResultFields {
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    order: bool,
    #[serde(
        default = "default_auto_commit",
        deserialize_with = "serde_fastjson::deserialize_bool"
    )]
    auto_commit: bool,
    #[serde(
        default,
        deserialize_with = "super::heartbeat::deserialize_fastjson_optional_enum"
    )]
    consume_result: Option<ConsumeMessageResult>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string"
    )]
    remark: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    spent_time_mills: i64,
}

impl<'de> Deserialize<'de> for ConsumeMessageDirectlyResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: ConsumeMessageDirectlyResultFields =
            serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            order: fields.order,
            auto_commit: fields.auto_commit,
            consume_result: fields.consume_result,
            remark: fields.remark,
            spent_time_mills: fields.spent_time_mills,
        })
    }
}

impl ConsumeMessageDirectlyResult {
    pub fn is_order(&self) -> bool {
        self.order
    }

    pub fn set_order(&mut self, order: bool) {
        self.order = order;
    }

    pub fn is_auto_commit(&self) -> bool {
        self.auto_commit
    }

    pub fn set_auto_commit(&mut self, auto_commit: bool) {
        self.auto_commit = auto_commit;
    }

    pub fn get_consume_result(&self) -> Option<ConsumeMessageResult> {
        self.consume_result
    }

    pub fn set_consume_result(&mut self, consume_result: Option<ConsumeMessageResult>) {
        self.consume_result = consume_result;
    }

    pub fn get_remark(&self) -> Option<&str> {
        self.remark.as_deref()
    }

    pub fn set_remark(&mut self, remark: Option<String>) {
        self.remark = remark;
    }

    pub fn get_spent_time_mills(&self) -> i64 {
        self.spent_time_mills
    }

    pub fn set_spent_time_mills(&mut self, spent_time_mills: i64) {
        self.spent_time_mills = spent_time_mills;
    }
}

impl std::fmt::Display for ConsumeMessageDirectlyResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let consume_result = self
            .consume_result
            .map(|result| result.name())
            .unwrap_or("null");
        write!(
            formatter,
            "ConsumeMessageDirectlyResult [order={}, autoCommit={}, consumeResult={}, remark={}, spentTimeMills={}]",
            self.order,
            self.auto_commit,
            consume_result,
            self.remark.as_deref().unwrap_or("null"),
            self.spent_time_mills
        )
    }
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

impl_java_remoting_serializable_adapters!(
    CheckClientRequestBody,
    LiteSubscriptionCtlRequestBody,
    QueryAssignmentRequestBody,
    QueryAssignmentResponseBody,
    QueryConsumeTimeSpanBody,
    SetMessageRequestModeRequestBody,
    MessageRequestModeSerializeWrapper,
    LockBatchRequestBody,
    UnlockBatchRequestBody,
    LockBatchResponseBody,
    ConsumerListByGroupResponseBody,
    ConsumeMessageDirectlyResult,
);

macro_rules! impl_java_remoting_serializable_body_adapters {
    ($($dto:ty),+ $(,)?) => {
        $(
            impl $dto {
                pub fn java_encode_optional(
                    value: Option<&Self>,
                ) -> Result<Option<Vec<u8>>, serde_json::Error> {
                    value.map(Self::java_encode).transpose()
                }

                pub fn java_to_json_optional(
                    value: Option<&Self>,
                    pretty_format: bool,
                ) -> Result<String, serde_json::Error> {
                    value.map_or_else(
                        || Ok("null".to_owned()),
                        |value| value.java_to_json(pretty_format),
                    )
                }

                pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
                    if pretty_format {
                        java_fastjson_remoting_body_json(self, true)
                    } else {
                        Ok(String::from_utf8_lossy(&self.encode_java_json()?).into_owned())
                    }
                }

                pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
                    self.encode_java_json()
                }

                /// Encodes with the supported inherited Fastjson2 writer features.
                pub fn java_encode_with_features(
                    &self,
                    features: &[JavaJsonWriterFeature],
                ) -> Result<Vec<u8>, serde_json::Error> {
                    Ok(java_fastjson_remoting_body_json_with_features(
                        self, false, features,
                    )?.into_bytes())
                }

                pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
                    Self::decode_java_json(&String::from_utf8_lossy(data))
                }

                pub fn java_decode_nullable(
                    data: Option<&[u8]>,
                ) -> Result<Option<Self>, serde_json::Error> {
                    let Some(data) = data else {
                        return Ok(None);
                    };
                    let input = String::from_utf8_lossy(data);
                    let normalized = normalize_java_object_map_keys(
                        &serde_fastjson::normalize_json_input(&input),
                    )?;
                    if serde_json::from_str::<serde_json::Value>(&normalized)?.is_null() {
                        return Ok(None);
                    }
                    Self::decode_java_json(&input).map(Some)
                }

                pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
                    let input = String::from_utf8_lossy(data);
                    let normalized = normalize_java_object_map_keys(
                        &serde_fastjson::normalize_json_input(&input),
                    )?;
                    serde_json::from_str(&normalized)
                }

                pub fn java_decode_list_nullable(
                    data: Option<&[u8]>,
                ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
                    let Some(data) = data else {
                        return Ok(None);
                    };
                    let input = String::from_utf8_lossy(data);
                    let normalized = normalize_java_object_map_keys(
                        &serde_fastjson::normalize_json_input(&input),
                    )?;
                    if serde_json::from_str::<serde_json::Value>(&normalized)?.is_null() {
                        return Ok(None);
                    }
                    serde_json::from_str(&normalized).map(Some)
                }

                pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
                    Self::decode_java_json(json)
                }

                pub fn java_from_json_nullable(
                    json: Option<&str>,
                ) -> Result<Option<Self>, serde_json::Error> {
                    let Some(json) = json else {
                        return Ok(None);
                    };
                    let normalized = normalize_java_object_map_keys(
                        &serde_fastjson::normalize_json_input(json),
                    )?;
                    if serde_json::from_str::<serde_json::Value>(&normalized)?.is_null() {
                        return Ok(None);
                    }
                    Self::decode_java_json(json).map(Some)
                }
            }
        )+
    };
}

impl_java_remoting_serializable_body_adapters!(GetConsumerStatusBody, ResetOffsetBody);

fn java_fastjson_remoting_body_json<T: Serialize>(
    value: &T,
    pretty_format: bool,
) -> Result<String, serde_json::Error> {
    java_fastjson_remoting_body_json_with_features(value, pretty_format, &[])
}

fn java_fastjson_remoting_body_json_with_features<T: Serialize>(
    value: &T,
    pretty_format: bool,
    features: &[JavaJsonWriterFeature],
) -> Result<String, serde_json::Error> {
    crate::route::validate_java_writer_features(features)?;
    if let Some(output) = crate::route::java_non_serializable_writer_output(features)? {
        return Ok(output);
    }
    let indent = crate::route::java_json_indent(pretty_format, features);
    let mut value = serde_json::to_value(value)?;
    let value_features = JavaJsonWriterValueFeatures {
        write_non_string_values_as_strings: features
            .contains(&JavaJsonWriterFeature::WriteNonStringValueAsString),
        write_boolean_as_number: features.contains(&JavaJsonWriterFeature::WriteBooleanAsNumber),
        write_enum_using_ordinal: features.contains(&JavaJsonWriterFeature::WriteEnumUsingOrdinal),
        browser_compatible: features.contains(&JavaJsonWriterFeature::BrowserCompatible),
        browser_secure: features.contains(&JavaJsonWriterFeature::BrowserSecure),
        bean_to_array: features.contains(&JavaJsonWriterFeature::BeanToArray),
        use_enum_names: features.contains(&JavaJsonWriterFeature::WriteEnumsUsingName)
            || features.contains(&JavaJsonWriterFeature::WriteEnumUsingToString),
        ..JavaJsonWriterValueFeatures::default()
    };
    crate::route::apply_java_json_writer_value_features(
        &mut value,
        value_features.write_non_string_values_as_strings,
        value_features.write_boolean_as_number,
        value_features.write_enum_using_ordinal,
        value_features.browser_compatible,
    );
    if features.contains(&JavaJsonWriterFeature::NotWriteDefaultValue) {
        crate::route::omit_default_writer_values(&mut value);
    }
    if features.contains(&JavaJsonWriterFeature::NotWriteEmptyArray) {
        crate::route::omit_empty_writer_values(&mut value, false);
    }
    if features.contains(&JavaJsonWriterFeature::IgnoreEmpty) {
        crate::route::omit_empty_writer_values(&mut value, true);
    }
    if features.contains(&JavaJsonWriterFeature::WriteLongAsString) {
        crate::route::apply_java_json_long_as_string(&mut value);
    }
    if features.contains(&JavaJsonWriterFeature::IgnoreNonFieldGetter) {
        crate::route::omit_java_non_field_getter_properties(&mut value);
    }
    if features.contains(&JavaJsonWriterFeature::WriteNulls)
        || features.contains(&JavaJsonWriterFeature::WriteMapNullValue)
    {
        crate::route::apply_java_write_nulls::<T>(&mut value)?;
    }
    if features.contains(&JavaJsonWriterFeature::WriteNullListAsEmpty) {
        crate::route::apply_java_null_lists::<T>(&mut value)?;
    }
    let value_features = JavaJsonWriterValueFeatures {
        use_single_quotes: features.contains(&JavaJsonWriterFeature::UseSingleQuotes),
        unquote_field_names: features.contains(&JavaJsonWriterFeature::UnquoteFieldName),
        ignore_non_field_getter: features.contains(&JavaJsonWriterFeature::IgnoreNonFieldGetter),
        ..value_features
    };
    let mut bean_to_array_maps = std::collections::HashMap::new();
    if value_features.bean_to_array {
        crate::route::apply_java_bean_to_array::<T>(
            &mut value,
            &mut bean_to_array_maps,
            value_features.use_enum_names,
        )?;
    }
    let mut output = String::new();
    let mut writer = JavaRemotingBodyWriter {
        indent,
        value_features,
        map_fields: &bean_to_array_maps,
        path: Vec::new(),
    };
    write_java_remoting_body_json(&value, None, 0, false, &mut writer, &mut output)?;
    if features.contains(&JavaJsonWriterFeature::EscapeNoneAscii) {
        Ok(crate::route::escape_none_ascii_json(&output))
    } else {
        Ok(output)
    }
}

#[derive(Clone, Copy, Default)]
struct JavaJsonWriterValueFeatures {
    write_non_string_values_as_strings: bool,
    write_boolean_as_number: bool,
    write_enum_using_ordinal: bool,
    browser_compatible: bool,
    browser_secure: bool,
    use_single_quotes: bool,
    unquote_field_names: bool,
    ignore_non_field_getter: bool,
    bean_to_array: bool,
    use_enum_names: bool,
}

struct JavaRemotingBodyWriter<'a> {
    indent: Option<&'a str>,
    value_features: JavaJsonWriterValueFeatures,
    map_fields: &'a crate::route::BeanToArrayMapFields,
    path: Vec<crate::route::JavaJsonPathSegment>,
}

fn write_java_remoting_body_json(
    value: &serde_json::Value,
    field_name: Option<&str>,
    depth: usize,
    queue_table: bool,
    writer: &mut JavaRemotingBodyWriter<'_>,
    output: &mut String,
) -> Result<(), serde_json::Error> {
    let indent = writer.indent;
    let value_features = writer.value_features;
    let field_name = writer.map_fields.get(&writer.path).copied().or(field_name);
    let queue_table = queue_table
        || matches!(
            field_name,
            Some("consumerTable" | "messageQueueTable" | "offsetTable")
        );
    match value {
        serde_json::Value::Object(fields)
            if fields.is_empty() && (indent.is_none() || field_name.is_some()) =>
        {
            output.push_str("{}")
        }
        serde_json::Value::Object(fields) if fields.is_empty() => {
            output.push_str("{\n");
            write_java_remoting_body_indent(output, depth + 1, indent);
            output.push('\n');
            write_java_remoting_body_indent(output, depth, indent);
            output.push('}');
        }
        serde_json::Value::Object(fields) => {
            output.push('{');
            for (index, (key, value)) in fields.iter().enumerate() {
                if indent.is_some() {
                    output.push('\n');
                    write_java_remoting_body_indent(output, depth + 1, indent);
                }
                let mut queue_key = if queue_table {
                    serde_json::from_str::<serde_json::Value>(key)
                        .ok()
                        .filter(serde_json::Value::is_object)
                } else {
                    None
                };
                if let Some(queue_key) = &mut queue_key {
                    crate::route::apply_java_json_writer_value_features(
                        queue_key,
                        value_features.write_non_string_values_as_strings,
                        value_features.write_boolean_as_number,
                        value_features.write_enum_using_ordinal,
                        value_features.browser_compatible,
                    );
                    if value_features.bean_to_array {
                        crate::route::apply_java_message_queue_bean_to_array(
                            queue_key,
                            value_features.use_enum_names,
                        );
                    }
                }
                if let Some(queue_key) = queue_key {
                    if value_features.ignore_non_field_getter {
                        let queue_key = serde_json::to_string(&queue_key)?;
                        crate::route::write_java_json_string_with_browser_secure(
                            &queue_key,
                            value_features.use_single_quotes,
                            value_features.browser_secure,
                            output,
                        )?;
                    } else {
                        let mut queue_key_writer = JavaRemotingBodyWriter {
                            indent: writer.indent,
                            value_features: JavaJsonWriterValueFeatures {
                                use_single_quotes: value_features.use_single_quotes,
                                unquote_field_names: value_features.unquote_field_names,
                                browser_secure: value_features.browser_secure,
                                bean_to_array: value_features.bean_to_array,
                                use_enum_names: value_features.use_enum_names,
                                ..JavaJsonWriterValueFeatures::default()
                            },
                            map_fields: writer.map_fields,
                            path: writer.path.clone(),
                        };
                        write_java_remoting_body_json(
                            &queue_key,
                            None,
                            depth + 1,
                            false,
                            &mut queue_key_writer,
                            output,
                        )?;
                    }
                } else {
                    crate::route::write_java_json_field_name(
                        key,
                        value,
                        field_name.is_some_and(crate::route::is_java_map_property),
                        value_features.use_single_quotes,
                        value_features.unquote_field_names,
                        value_features.browser_secure,
                        output,
                    )?;
                }
                output.push(':');
                let child_is_queue_table =
                    matches!(key.as_str(), "messageQueueTable" | "offsetTable")
                        || field_name == Some("consumerTable");
                writer
                    .path
                    .push(crate::route::JavaJsonPathSegment::ObjectKey(key.clone()));
                let result = write_java_remoting_body_json(
                    value,
                    Some(key),
                    depth + 1,
                    child_is_queue_table,
                    writer,
                    output,
                );
                writer.path.pop();
                result?;
                if index + 1 != fields.len() {
                    output.push(',');
                }
            }
            if indent.is_some() {
                output.push('\n');
                write_java_remoting_body_indent(output, depth, indent);
            }
            output.push('}');
        }
        serde_json::Value::Array(values) if values.is_empty() && indent.is_none() => {
            output.push_str("[]")
        }
        serde_json::Value::Array(values) if values.is_empty() => {
            output.push_str("[\n");
            write_java_remoting_body_indent(output, depth + 1, indent);
            output.push('\n');
            write_java_remoting_body_indent(output, depth, indent);
            output.push(']');
        }
        serde_json::Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if indent.is_some() {
                    output.push('\n');
                    write_java_remoting_body_indent(output, depth + 1, indent);
                }
                writer
                    .path
                    .push(crate::route::JavaJsonPathSegment::ArrayIndex(index));
                let result =
                    write_java_remoting_body_json(value, None, depth + 1, false, writer, output);
                writer.path.pop();
                result?;
                if index + 1 != values.len() {
                    output.push(',');
                }
            }
            if indent.is_some() {
                output.push('\n');
                write_java_remoting_body_indent(output, depth, indent);
            }
            output.push(']');
        }
        serde_json::Value::String(value) => {
            let use_single_quotes = value_features.use_single_quotes
                && field_name.is_none_or(|name| !crate::route::is_java_enum_field(name));
            crate::route::write_java_json_string_with_browser_secure(
                value,
                use_single_quotes,
                value_features.browser_secure,
                output,
            )?;
        }
        _ => output.push_str(&serde_json::to_string(value)?),
    }
    Ok(())
}

fn write_java_remoting_body_indent(output: &mut String, depth: usize, indent: Option<&str>) {
    let Some(indent) = indent else {
        return;
    };
    for _ in 0..depth {
        output.push_str(indent);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[allow(clippy::mutable_key_type)]
    fn message_queue_set_element_aliases_match(
        case: &serde_json::Value,
        assigned_set: &SharedMessageQueueSet,
        getter_set: &SharedMessageQueueSet,
    ) {
        let assigned_queue = assigned_set.with(|queues| {
            queues
                .iter()
                .find_map(Clone::clone)
                .expect("fixture Set contains a non-null MessageQueue")
        });
        let getter_queue = getter_set.with(|queues| {
            queues
                .iter()
                .find_map(Clone::clone)
                .expect("fixture Set contains a non-null MessageQueue")
        });
        assert_eq!(
            assigned_queue.ptr_eq(&getter_queue),
            case["getterReturnsAssignedElement"]
        );

        let original_queue_id = assigned_queue.with(MessageQueue::queue_id);
        assigned_queue.with_mut(|queue| queue.set_queue_id(original_queue_id + 5));
        assert_eq!(
            getter_set.with(|queues| {
                queues.iter().find_map(|queue| {
                    queue
                        .as_ref()
                        .map(|queue| queue.with(MessageQueue::queue_id))
                }) == Some(original_queue_id + 5)
            }),
            case["setterElementMutationVisibleToGetter"]
        );
        assigned_queue.with_mut(|queue| queue.set_queue_id(original_queue_id));

        getter_queue.with_mut(|queue| queue.set_queue_id(original_queue_id + 6));
        assert_eq!(
            assigned_queue.with(|queue| queue.queue_id() == original_queue_id + 6),
            case["getterElementMutationVisibleToSetterObject"]
        );
        getter_queue.with_mut(|queue| queue.set_queue_id(original_queue_id));
    }

    fn normalize_batch_body_value(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Array(values) => {
                for value in values.iter_mut() {
                    normalize_batch_body_value(value);
                }
                values.sort_by_key(|value| serde_json::to_string(value).unwrap());
            }
            serde_json::Value::Object(fields) => {
                for value in fields.values_mut() {
                    normalize_batch_body_value(value);
                }
            }
            _ => {}
        }
    }

    fn batch_body_cases_match_java<T>(cases: &[serde_json::Value], expected_len: usize)
    where
        T: serde::de::DeserializeOwned + Serialize + std::fmt::Debug,
    {
        assert_eq!(cases.len(), expected_len);
        let mut mismatches = Vec::new();
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let decoded = serde_json::from_str::<Option<T>>(case["rawInput"].as_str().unwrap());
            let java_accepts = case["outcome"] == "ACCEPTED";
            match (decoded, java_accepts) {
                (Ok(decoded), true) => {
                    let mut actual = serde_json::to_value(decoded).unwrap();
                    let mut expected = case["encoded"].clone();
                    normalize_batch_body_value(&mut actual);
                    normalize_batch_body_value(&mut expected);
                    if actual != expected {
                        mismatches.push(format!("{name}: encoded {actual} != Java {expected}"));
                    }
                }
                (Ok(_), false) => mismatches.push(format!("{name}: Rust accepted, Java rejected")),
                (Err(error), true) => {
                    mismatches.push(format!("{name}: Rust rejected ({error}), Java accepted"))
                }
                (Err(_), false) => {}
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[test]
    fn lock_batch_request_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        batch_body_cases_match_java::<LockBatchRequestBody>(
            fixture["lockBatchRequestBodyCases"].as_array().unwrap(),
            46,
        );
    }

    #[test]
    fn unlock_batch_request_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        batch_body_cases_match_java::<UnlockBatchRequestBody>(
            fixture["unlockBatchRequestBodyCases"].as_array().unwrap(),
            46,
        );
    }

    #[test]
    fn lock_batch_response_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        batch_body_cases_match_java::<LockBatchResponseBody>(
            fixture["lockBatchResponseBodyCases"].as_array().unwrap(),
            25,
        );
    }

    #[test]
    fn batch_ack_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["batchAckBodyInputCases"].as_array().unwrap();
        assert_eq!(cases.len(), 111);
        for case in cases {
            let decoded = BatchAckMessageRequestBody::java_decode_nullable(Some(
                case["rawInput"].as_str().unwrap().as_bytes(),
            ));
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
    fn batch_ack_public_methods_match_invoked_java() {
        use base64::{Engine as _, engine::general_purpose::STANDARD};

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let case = &fixture["batchAckApiCase"];

        let mut ack = BatchAck::default();
        assert_eq!(
            json!({
                "consumerGroup": ack.get_consumer_group(),
                "topic": ack.get_topic(),
                "retry": ack.get_retry(),
                "startOffset": ack.get_start_offset(),
                "queueId": ack.get_queue_id(),
                "reviveQueueId": ack.get_revive_queue_id(),
                "popTime": ack.get_pop_time(),
                "invisibleTime": ack.get_invisible_time(),
                "bitSet": ack
                    .get_bit_set()
                    .map(|bit_set| bit_set.with(BitSet::to_byte_array))
                    .transpose()
                    .unwrap(),
            }),
            case["defaults"]
        );

        ack.set_consumer_group(Some("workers".into()));
        ack.set_topic(Some("orders".into()));
        ack.set_retry(Some("0".into()));
        ack.set_start_offset(100);
        ack.set_queue_id(1);
        ack.set_revive_queue_id(2);
        ack.set_pop_time(10);
        ack.set_invisible_time(20);
        let assigned_bits = SharedValue::new(BitSet::from_byte_array(&[0b0000_0101, 0, 0]));
        ack.set_bit_set(Some(assigned_bits.clone()));
        let bit_set_base64 = serde_json::to_value(&ack).unwrap()["b"].clone();
        let bits = ack.get_bit_set().unwrap();
        assert!(bits.ptr_eq(&assigned_bits));
        assert_eq!(
            json!({
                "consumerGroup": ack.get_consumer_group(),
                "topic": ack.get_topic(),
                "retry": ack.get_retry(),
                "startOffset": ack.get_start_offset(),
                "queueId": ack.get_queue_id(),
                "reviveQueueId": ack.get_revive_queue_id(),
                "popTime": ack.get_pop_time(),
                "invisibleTime": ack.get_invisible_time(),
                "bitSetBase64": bit_set_base64,
                "bitSetCardinality": bits.with(BitSet::cardinality),
                "bitSetLength": bits.with(BitSet::length),
                "bitSetQueries": bits.with(|bits| [
                    bits.get(0).unwrap(), bits.get(1).unwrap(), bits.get(2).unwrap(), bits.get(5).unwrap(),
                ]),
            }),
            case["values"]
        );
        assert_eq!(ack.to_string(), case["toString"].as_str().unwrap());
        assert_eq!(case["bitSetGetterReturnsAssignedObject"], true);
        assert_eq!(case["bitSetMutationVisibleThroughGetter"], true);
        assert_eq!(case["bitSetMutationVisibleThroughSetterObject"], true);
        bits.with_mut(|bits| bits.set(5).unwrap());
        assert!(assigned_bits.with(|bits| bits.get(5).unwrap()));
        assert_eq!(
            serde_json::to_value(&ack).unwrap()["b"],
            case["mutatedBitSetBase64"]
        );
        let mut cloned_bits = ack.get_bit_set().unwrap().snapshot();
        cloned_bits.clear(0).unwrap();
        cloned_bits.set(7).unwrap();
        let mut cloned_ack = BatchAck::default();
        cloned_ack.set_bit_set(Some(SharedValue::new(cloned_bits)));
        assert_eq!(
            json!({
                "cloneIsDistinctObject": true,
                "sourceBase64": serde_json::to_value(&ack).unwrap()["b"],
                "cloneBase64": serde_json::to_value(&cloned_ack).unwrap()["b"],
                "sourceRetainsClearedBit": bits.with(|bits| bits.get(0).unwrap()),
                "sourceExcludesCloneBit": !bits.with(|bits| bits.get(7).unwrap()),
            }),
            case["bitSetClone"]
        );
        let preallocated_source = BitSet::try_with_capacity_bits(130).unwrap();
        let mut preallocated_clone = preallocated_source.clone();
        preallocated_clone.set(7).unwrap();
        assert_eq!(
            json!({
                "cloneIsDistinctObject": !std::ptr::eq(&preallocated_source, &preallocated_clone),
                "sourceSize": preallocated_source.size(),
                "cloneSize": preallocated_clone.size(),
                "sourceBase64": STANDARD.encode(preallocated_source.to_byte_array().unwrap()),
                "cloneBase64": STANDARD.encode(preallocated_clone.to_byte_array().unwrap()),
                "sourceExcludesCloneBit": !preallocated_source.get(7).unwrap(),
            }),
            case["bitSetPreallocatedClone"]
        );
        let mut operations = bits.snapshot();
        operations.clear(2).unwrap();
        operations.flip(7).unwrap();
        let mut operations_ack = BatchAck::default();
        operations_ack.set_bit_set(Some(SharedValue::new(operations.clone())));
        assert_eq!(
            json!({
                "base64": serde_json::to_value(operations_ack).unwrap()["b"],
                "cardinality": operations.cardinality(),
                "length": operations.length(),
                "isEmpty": operations.is_empty(),
                "get": [operations.get(0).unwrap(), operations.get(2).unwrap(), operations.get(5).unwrap(), operations.get(7).unwrap()],
            }),
            case["bitSetOperations"]
        );
        assert_eq!(
            json!({
                "get": "IndexOutOfBoundsException",
                "set": "IndexOutOfBoundsException",
                "clear": "IndexOutOfBoundsException",
                "flip": "IndexOutOfBoundsException",
            }),
            case["bitSetNegativeIndexErrors"]
        );
        assert_eq!(
            bits.with(|bits| bits.get(-1)),
            Err(BitSetError::NegativeIndex(-1))
        );
        assert_eq!(
            bits.with_mut(|bits| bits.set(-1)),
            Err(BitSetError::NegativeIndex(-1))
        );
        assert_eq!(
            bits.with_mut(|bits| bits.clear(-1)),
            Err(BitSetError::NegativeIndex(-1))
        );
        assert_eq!(
            bits.with_mut(|bits| bits.flip(-1)),
            Err(BitSetError::NegativeIndex(-1))
        );
        let mut word_boundary_bits = BitSet::new();
        for index in [7, 8, 63, 64, 127] {
            word_boundary_bits.set(index).unwrap();
        }
        assert_eq!(word_boundary_bits.cardinality(), 5);
        assert_eq!(word_boundary_bits.length(), 128);
        let mut word_boundary_ack = BatchAck::default();
        word_boundary_ack.set_bit_set(Some(SharedValue::new(word_boundary_bits)));
        assert_eq!(
            json!({
                "base64": serde_json::to_value(word_boundary_ack).unwrap()["b"],
                "cardinality": 5,
                "length": 128,
            }),
            case["bitSetWordBoundary"]
        );

        let bit_set_base64 = |bits: &BitSet| {
            let mut encoded_ack = BatchAck::default();
            encoded_ack.set_bit_set(Some(SharedValue::new(bits.clone())));
            serde_json::to_value(encoded_ack).unwrap()["b"]
                .as_str()
                .unwrap()
                .to_owned()
        };
        let mut ranged = BitSet::new();
        ranged.set_range(3, 12).unwrap();
        ranged.clear_range(5, 7).unwrap();
        ranged.flip_range(8, 14).unwrap();
        let slice = ranged.get_range(2, 14).unwrap();
        let mut intersects = BitSet::new();
        intersects.set(4).unwrap();
        let mut disjoint = BitSet::new();
        disjoint.set(30).unwrap();
        assert_eq!(
            json!({
                "base64": bit_set_base64(&ranged),
                "cardinality": ranged.cardinality(),
                "length": ranged.length(),
                "sliceBase64": bit_set_base64(&slice),
                "nextSetBit": [
                    ranged.next_set_bit(0).unwrap(),
                    ranged.next_set_bit(5).unwrap(),
                    ranged.next_set_bit(8).unwrap(),
                    ranged.next_set_bit(14).unwrap(),
                ],
                "nextClearBit": [
                    ranged.next_clear_bit(3).unwrap(),
                    ranged.next_clear_bit(8).unwrap(),
                    ranged.next_clear_bit(14).unwrap(),
                ],
                "previousSetBit": [
                    ranged.previous_set_bit(13).unwrap(),
                    ranged.previous_set_bit(11).unwrap(),
                    ranged.previous_set_bit(2).unwrap(),
                    ranged.previous_set_bit(-1).unwrap(),
                ],
                "previousClearBit": [
                    ranged.previous_clear_bit(13).unwrap(),
                    ranged.previous_clear_bit(3).unwrap(),
                    ranged.previous_clear_bit(-1).unwrap(),
                ],
                "intersects": ranged.intersects(&intersects),
                "disjoint": ranged.intersects(&disjoint),
            }),
            case["bitSetRanges"]
        );
        assert_eq!(ranged.next_set_bit(-1), Err(BitSetError::NegativeIndex(-1)));
        assert_eq!(
            ranged.next_clear_bit(-1),
            Err(BitSetError::NegativeIndex(-1))
        );
        assert_eq!(
            ranged.previous_set_bit(-2),
            Err(BitSetError::NegativeIndex(-2))
        );
        assert_eq!(
            ranged.previous_clear_bit(-2),
            Err(BitSetError::NegativeIndex(-2))
        );
        assert_eq!(ranged.set_range(-1, 2), Err(BitSetError::NegativeIndex(-1)));
        assert_eq!(
            ranged.set_range(3, 2),
            Err(BitSetError::InvalidRange { from: 3, to: 2 })
        );
        assert_eq!(
            ranged.clear_range(-1, 2),
            Err(BitSetError::NegativeIndex(-1))
        );
        assert_eq!(
            ranged.flip_range(3, 2),
            Err(BitSetError::InvalidRange { from: 3, to: 2 })
        );
        assert_eq!(ranged.get_range(-1, 2), Err(BitSetError::NegativeIndex(-1)));
        assert_eq!(
            ranged.get_range(3, 2),
            Err(BitSetError::InvalidRange { from: 3, to: 2 })
        );
        assert!(
            case["bitSetRangeErrors"]
                .as_object()
                .unwrap()
                .values()
                .all(|error| error == "IndexOutOfBoundsException")
        );

        let mut range_word_boundary = BitSet::new();
        range_word_boundary.set_range(63, 66).unwrap();
        range_word_boundary.clear_range(64, 65).unwrap();
        range_word_boundary.flip_range(62, 64).unwrap();
        let range_word_boundary_slice = range_word_boundary.get_range(61, 66).unwrap();
        assert_eq!(
            json!({
                "base64": bit_set_base64(&range_word_boundary),
                "sliceBase64": bit_set_base64(&range_word_boundary_slice),
                "cardinality": range_word_boundary.cardinality(),
                "length": range_word_boundary.length(),
            }),
            case["bitSetRangeWordBoundary"]
        );

        let mut boolean_operations = BitSet::new();
        boolean_operations.set_range(2, 6).unwrap();
        boolean_operations.set_with_value(4, false).unwrap();
        boolean_operations.set_with_value(8, true).unwrap();
        boolean_operations.set_range_with_value(1, 3, true).unwrap();
        boolean_operations
            .set_range_with_value(2, 5, false)
            .unwrap();
        let mut cleared = boolean_operations.clone();
        cleared.clear_all();
        assert_eq!(
            json!({
                "base64": bit_set_base64(&boolean_operations),
                "longs": boolean_operations.to_long_array().unwrap(),
                "cardinality": boolean_operations.cardinality(),
                "length": boolean_operations.length(),
                "hashCode": boolean_operations.java_hash_code(),
                "toString": boolean_operations.java_to_string(),
                "stream": boolean_operations.iter_set_bits().collect::<Vec<_>>(),
                "clear": cleared.is_empty() && cleared.cardinality() == 0 && cleared.length() == 0,
            }),
            case["bitSetBooleanOperations"]
        );

        let mut bulk_left = BitSet::new();
        for index in [0, 2, 5, 64, 127] {
            bulk_left.set(index).unwrap();
        }
        let mut bulk_right = BitSet::new();
        for index in [1, 2, 5, 64, 126] {
            bulk_right.set(index).unwrap();
        }
        let mut and_result = bulk_left.clone();
        and_result.and(&bulk_right);
        let mut or_result = bulk_left.clone();
        or_result.or(&bulk_right).unwrap();
        let mut xor_result = bulk_left.clone();
        xor_result.xor(&bulk_right).unwrap();
        let mut and_not_result = bulk_left.clone();
        and_not_result.and_not(&bulk_right);
        assert_eq!(
            json!({
                "and": bit_set_base64(&and_result),
                "or": bit_set_base64(&or_result),
                "xor": bit_set_base64(&xor_result),
                "andNot": bit_set_base64(&and_not_result),
                "intersects": bulk_left.intersects(&bulk_right),
                "equalClone": bulk_left == bulk_left.clone(),
                "leftLongs": bulk_left.to_long_array().unwrap(),
                "leftHashCode": bulk_left.java_hash_code(),
                "leftToString": bulk_left.java_to_string(),
                "leftStream": bulk_left.iter_set_bits().collect::<Vec<_>>(),
            }),
            case["bitSetBulkOperations"]
        );

        let capacity_case = &fixture["batchAckBitSetCapacityCase"];
        let mut grown = BitSet::new();
        grown.set(63).unwrap();
        let after_bit_63 = grown.size();
        grown.set(64).unwrap();
        let after_bit_64 = grown.size();
        grown.set(128).unwrap();
        let after_bit_128 = grown.size();
        grown.clear(128).unwrap();
        let mut capacity_source = BitSet::new();
        capacity_source.set(0).unwrap();
        capacity_source.set(64).unwrap();
        let mut narrow_capacity = BitSet::try_with_capacity_bits(1).unwrap();
        let mut wide_capacity = BitSet::try_with_capacity_bits(128).unwrap();
        narrow_capacity.set(4).unwrap();
        wide_capacity.set(4).unwrap();
        assert_eq!(
            json!({
                "constructorSizes": [
                    BitSet::new().size(),
                    BitSet::try_with_capacity_bits(0).unwrap().size(),
                    BitSet::try_with_capacity_bits(1).unwrap().size(),
                    BitSet::try_with_capacity_bits(64).unwrap().size(),
                    BitSet::try_with_capacity_bits(65).unwrap().size(),
                ],
                "growthAndClearSizes": [after_bit_63, after_bit_64, after_bit_128, grown.size()],
                "valueOfSizes": [
                    BitSet::from_byte_array(&[1, 0, 0]).size(),
                    BitSet::try_from_long_array(&[1, 0]).unwrap().size(),
                    BitSet::try_from_long_array(&[]).unwrap().size(),
                ],
                "negativeCapacityError": capacity_case["negativeCapacityError"],
                "sliceSize": capacity_source.get_range(0, 65).unwrap().size(),
                "emptySliceSize": BitSet::new().get_range(3, 3).unwrap().size(),
                "equalWithDifferentCapacities": narrow_capacity.size() != wide_capacity.size()
                    && narrow_capacity == wide_capacity,
            }),
            capacity_case.clone()
        );
        assert_eq!(
            BitSet::try_with_capacity_bits(-1),
            Err(BitSetError::NegativeCapacity(-1))
        );
        assert_eq!(
            capacity_case["negativeCapacityError"],
            "NegativeArraySizeException"
        );

        let mut body = BatchAckMessageRequestBody::default();
        assert_eq!(
            json!([body.get_broker_name(), body.get_acks()]),
            case["bodyDefaults"]
        );
        body.set_broker_name(Some("broker-a".into()));
        let caller_ack = SharedValue::new(ack.clone());
        let caller_acks = SharedValue::new(vec![Some(caller_ack.clone())]);
        let assigned_acks = caller_acks.clone();
        body.set_acks(Some(caller_acks.clone()));
        let getter_acks = body.get_acks().unwrap();
        assert_eq!(
            assigned_acks.ptr_eq(&getter_acks),
            case["bodyReturnsAssignedList"]
        );
        assert_eq!(body.get_broker_name(), case["bodyBrokerName"].as_str());
        assert_eq!(getter_acks.with(|acks| acks.len()), case["bodyAckCount"]);
        assert!(getter_acks.with(|acks| acks[0].as_ref().unwrap().ptr_eq(&caller_ack)));
        assert_eq!(
            json!(
                getter_acks
                    .snapshot()
                    .iter()
                    .map(|ack| ack.as_ref().map(|ack| ack.with(BatchAck::get_queue_id)))
                    .collect::<Vec<_>>()
            ),
            case["bodyAssignedQueueIds"]
        );
        caller_ack.with_mut(|ack| ack.set_queue_id(5));
        assert_eq!(
            getter_acks
                .with(|acks| { acks[0].as_ref().unwrap().with(BatchAck::get_queue_id) == 5 }),
            case["externalAckObjectMutationVisible"]
        );
        getter_acks.with(|acks| {
            acks[0]
                .as_ref()
                .unwrap()
                .with_mut(|ack| ack.set_queue_id(6))
        });
        assert_eq!(
            caller_ack.with(|ack| ack.get_queue_id() == 6),
            case["ackObjectMutationVisibleThroughBody"]
        );
        caller_acks.with_mut(|acks| acks.clear());
        let mut external_ack = BatchAck::default();
        external_ack.set_queue_id(3);
        caller_acks.with_mut(|acks| acks.push(Some(SharedValue::new(external_ack))));
        assert_eq!(
            getter_acks.with(|acks| {
                acks.iter()
                    .flatten()
                    .any(|ack| ack.with(BatchAck::get_queue_id) == 3)
            }),
            case["externalMutationVisible"]
        );
        let mut getter_ack = BatchAck::default();
        getter_ack.set_queue_id(4);
        getter_acks.with_mut(|acks| {
            acks.clear();
            acks.push(Some(SharedValue::new(getter_ack)));
        });
        assert_eq!(
            assigned_acks.with(|acks| {
                acks.iter()
                    .flatten()
                    .any(|ack| ack.with(BatchAck::get_queue_id) == 4)
            }),
            case["getterMutationVisibleToAssignedList"]
        );
        let mutated_queue_ids = getter_acks
            .snapshot()
            .iter()
            .map(|ack| ack.as_ref().map(|ack| ack.with(BatchAck::get_queue_id)))
            .collect::<Vec<_>>();
        assert_eq!(json!(mutated_queue_ids), case["bodyMutatedQueueIds"]);
        let ack_identity = &case["ackObjectIdentity"];
        assert_eq!(ack_identity["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(ack_identity["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(ack_identity["sameInstanceEquals"], true);
        assert_eq!(ack_identity["equalValuesDistinctInstanceEquals"], false);
        assert_eq!(case["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(case["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(case["sameInstanceEquals"], true);
        assert_eq!(case["equalValuesDistinctInstanceEquals"], false);
        body.set_acks(None);
        assert!(body.get_acks().is_none());
        assert_eq!(case["bodyNullAcks"], true);
    }

    #[test]
    fn batch_ack_inherited_remoting_serialization_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let api = &fixture["batchAckApiCase"];
        assert_eq!(
            api["batchAckPublicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.BatchAck()"])
        );
        assert_eq!(
            api["batchAckDeclaredPublicMethods"]
                .as_array()
                .unwrap()
                .len(),
            19
        );
        assert_eq!(
            api["batchAckDeclaredPublicMethods"],
            api["batchAckCallablePublicMethods"]
        );
        assert_eq!(
            api["bodyPublicConstructors"],
            json!(["org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody()"])
        );
        assert_eq!(
            api["bodyDeclaredPublicMethods"].as_array().unwrap().len(),
            4
        );
        assert_eq!(
            api["bodyCallablePublicMethods"].as_array().unwrap().len(),
            13
        );

        let method_names = |key: &str| {
            let mut names = api[key]
                .as_array()
                .unwrap()
                .iter()
                .map(|signature| {
                    signature
                        .as_str()
                        .unwrap()
                        .split_once('(')
                        .unwrap()
                        .0
                        .rsplit_once('.')
                        .unwrap()
                        .1
                        .to_owned()
                })
                .collect::<Vec<_>>();
            names.sort();
            names.dedup();
            names
        };
        assert_eq!(
            method_names("batchAckDeclaredPublicMethods"),
            [
                "getBitSet",
                "getConsumerGroup",
                "getInvisibleTime",
                "getPopTime",
                "getQueueId",
                "getRetry",
                "getReviveQueueId",
                "getStartOffset",
                "getTopic",
                "setBitSet",
                "setConsumerGroup",
                "setInvisibleTime",
                "setPopTime",
                "setQueueId",
                "setRetry",
                "setReviveQueueId",
                "setStartOffset",
                "setTopic",
                "toString",
            ]
        );
        assert_eq!(
            method_names("bodyDeclaredPublicMethods"),
            ["getAcks", "getBrokerName", "setAcks", "setBrokerName"]
        );
        assert_eq!(
            method_names("bodyCallablePublicMethods"),
            [
                "decode",
                "decodeList",
                "encode",
                "fromJson",
                "getAcks",
                "getBrokerName",
                "setAcks",
                "setBrokerName",
                "toJson",
            ]
        );

        for case in fixture["batchAckInheritedSerializationCases"]
            .as_array()
            .unwrap()
        {
            let raw_input = case["rawInput"].as_str().unwrap();
            let value = BatchAckMessageRequestBody::java_decode(raw_input.as_bytes()).unwrap();
            let compact = value.java_to_json(false).unwrap();
            let encoded = value.java_encode().unwrap();
            assert_eq!(
                value.java_encode_with_features(&[]).unwrap(),
                encoded,
                "{} empty writer-feature array",
                case["id"]
            );
            let browser_compatible = value
                .java_encode_with_features(&[JavaJsonWriterFeature::BrowserCompatible])
                .unwrap();
            let expected_browser_compatible = case["writerFeatureOverrides"]["BrowserCompatible"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                browser_compatible,
                expected_browser_compatible.as_bytes(),
                "{} BrowserCompatible",
                case["id"]
            );
            assert_eq!(
                value
                    .java_encode_with_features(&[JavaJsonWriterFeature::OptimizedForAscii])
                    .unwrap(),
                case["instanceEncodedJson"].as_str().unwrap().as_bytes(),
                "{} OptimizedForAscii",
                case["id"]
            );
            for (feature, feature_name) in [
                (
                    JavaJsonWriterFeature::WriteNameAsSymbol,
                    "WriteNameAsSymbol",
                ),
                (
                    JavaJsonWriterFeature::NotWriteRootClassName,
                    "NotWriteRootClassName",
                ),
                (
                    JavaJsonWriterFeature::NotWriteHashMapArrayListClassName,
                    "NotWriteHashMapArrayListClassName",
                ),
                (
                    JavaJsonWriterFeature::NotWriteNumberClassName,
                    "NotWriteNumberClassName",
                ),
                (
                    JavaJsonWriterFeature::NotWriteSetClassName,
                    "NotWriteSetClassName",
                ),
                (
                    JavaJsonWriterFeature::WriteByteArrayAsBase64,
                    "WriteByteArrayAsBase64",
                ),
                (
                    JavaJsonWriterFeature::WritePairAsJavaBean,
                    "WritePairAsJavaBean",
                ),
                (
                    JavaJsonWriterFeature::WriteThrowableClassName,
                    "WriteThrowableClassName",
                ),
                (
                    JavaJsonWriterFeature::WriterUtilDateAsMillis,
                    "WriterUtilDateAsMillis",
                ),
                (
                    JavaJsonWriterFeature::WriteBigDecimalAsPlain,
                    "WriteBigDecimalAsPlain",
                ),
                (
                    JavaJsonWriterFeature::WriteFloatSpecialAsString,
                    "WriteFloatSpecialAsString",
                ),
                (JavaJsonWriterFeature::BrowserSecure, "BrowserSecure"),
                (
                    JavaJsonWriterFeature::WriteEnumsUsingName,
                    "WriteEnumsUsingName",
                ),
                (
                    JavaJsonWriterFeature::WriteEnumUsingToString,
                    "WriteEnumUsingToString",
                ),
                (
                    JavaJsonWriterFeature::IgnoreErrorGetter,
                    "IgnoreErrorGetter",
                ),
                (
                    JavaJsonWriterFeature::IgnoreNonFieldGetter,
                    "IgnoreNonFieldGetter",
                ),
                (JavaJsonWriterFeature::WriteNulls, "WriteNulls"),
                (
                    JavaJsonWriterFeature::WriteMapNullValue,
                    "WriteMapNullValue",
                ),
                (
                    JavaJsonWriterFeature::WriteNullListAsEmpty,
                    "WriteNullListAsEmpty",
                ),
                (JavaJsonWriterFeature::BeanToArray, "BeanToArray"),
                (JavaJsonWriterFeature::FieldBased, "FieldBased"),
                (
                    JavaJsonWriterFeature::WriteNullBooleanAsFalse,
                    "WriteNullBooleanAsFalse",
                ),
                (JavaJsonWriterFeature::MapSortField, "MapSortField"),
                (
                    JavaJsonWriterFeature::SortMapEntriesByKeys,
                    "SortMapEntriesByKeys",
                ),
                (JavaJsonWriterFeature::LargeObject, "LargeObject"),
            ] {
                let featured = value.java_encode_with_features(&[feature]).unwrap();
                let expected = case["writerFeatureOverrides"][feature_name]
                    .as_str()
                    .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
                assert_eq!(
                    featured,
                    expected.as_bytes(),
                    "{} {feature_name}",
                    case["id"]
                );
            }
            assert_eq!(
                BatchAckMessageRequestBody::java_to_json_optional(Some(&value), false).unwrap(),
                compact
            );
            assert_eq!(
                BatchAckMessageRequestBody::java_encode_optional(Some(&value)).unwrap(),
                Some(encoded.clone())
            );
            assert_eq!(
                compact, case["instanceJson"],
                "{} instance JSON",
                case["id"]
            );
            assert_eq!(compact, case["staticJson"], "{} static JSON", case["id"]);
            assert_eq!(
                value.java_to_json(true).unwrap(),
                case["instancePrettyJson"],
                "{} pretty JSON",
                case["id"]
            );
            assert_eq!(
                value.java_to_json(true).unwrap(),
                case["staticPrettyJson"],
                "{} static pretty JSON",
                case["id"]
            );
            assert_eq!(
                encoded,
                case["instanceEncodedJson"].as_str().unwrap().as_bytes()
            );
            assert_eq!(
                encoded,
                case["staticEncodedJson"].as_str().unwrap().as_bytes()
            );
            assert_eq!(
                encoded,
                case["featureEncodedJson"].as_str().unwrap().as_bytes()
            );
            let featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::WriteNonStringValueAsString])
                .unwrap();
            let expected_featured = case["writerFeatureOverrides"]["WriteNonStringValueAsString"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                featured,
                expected_featured.as_bytes(),
                "{} WriteNonStringValueAsString",
                case["id"]
            );
            let boolean_featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::WriteBooleanAsNumber])
                .unwrap();
            let expected_boolean_featured = case["writerFeatureOverrides"]["WriteBooleanAsNumber"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                boolean_featured,
                expected_boolean_featured.as_bytes(),
                "{} WriteBooleanAsNumber",
                case["id"]
            );
            let combined_featured = value
                .java_encode_with_features(&[
                    JavaJsonWriterFeature::WriteBooleanAsNumber,
                    JavaJsonWriterFeature::WriteNonStringValueAsString,
                ])
                .unwrap();
            let expected_combined_featured =
                case["writerFeatureOverrides"]["WriteBooleanAsNumber+WriteNonStringValueAsString"]
                    .as_str()
                    .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                combined_featured,
                expected_combined_featured.as_bytes(),
                "{} combined boolean writer features",
                case["id"]
            );
            let escaped_featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::EscapeNoneAscii])
                .unwrap();
            let expected_escaped_featured = case["writerFeatureOverrides"]["EscapeNoneAscii"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                escaped_featured,
                expected_escaped_featured.as_bytes(),
                "{} EscapeNoneAscii",
                case["id"]
            );
            let pretty_featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::PrettyFormat])
                .unwrap();
            let expected_pretty_featured = case["writerFeatureOverrides"]["PrettyFormat"]
                .as_str()
                .unwrap_or_else(|| case["instancePrettyJson"].as_str().unwrap());
            assert_eq!(
                pretty_featured,
                expected_pretty_featured.as_bytes(),
                "{} PrettyFormat",
                case["id"]
            );
            assert_eq!(
                value
                    .java_encode_with_features(&[JavaJsonWriterFeature::WriteLongAsString])
                    .unwrap(),
                case["writerFeatureOverrides"]["WriteLongAsString"]
                    .as_str()
                    .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap())
                    .as_bytes(),
                "{} WriteLongAsString",
                case["id"]
            );
            for (feature, feature_name) in [
                (
                    JavaJsonWriterFeature::PrettyFormatWith2Space,
                    "PrettyFormatWith2Space",
                ),
                (
                    JavaJsonWriterFeature::PrettyFormatWith4Space,
                    "PrettyFormatWith4Space",
                ),
            ] {
                assert_eq!(
                    value.java_encode_with_features(&[feature]).unwrap(),
                    case["writerFeatureOverrides"][feature_name]
                        .as_str()
                        .unwrap()
                        .as_bytes(),
                    "{} {feature_name}",
                    case["id"]
                );
            }

            let decoded = BatchAckMessageRequestBody::java_decode(&encoded).unwrap();
            let from_json = BatchAckMessageRequestBody::java_from_json(&compact).unwrap();
            let list =
                BatchAckMessageRequestBody::java_decode_list(format!("[{compact}]").as_bytes())
                    .unwrap();
            assert_eq!(decoded.java_to_json(false).unwrap(), case["decodedJson"]);
            assert_eq!(from_json.java_to_json(false).unwrap(), case["fromJsonJson"]);
            assert_eq!(
                list.len(),
                case["decodedListCount"].as_u64().unwrap() as usize
            );
            assert_eq!(
                list[0].java_to_json(false).unwrap(),
                case["decodedListJson"],
                "{} list decode",
                case["id"]
            );
        }

        let nulls = &fixture["batchAckInheritedSerializationNullUtilities"];
        assert_eq!(nulls["staticEncodeNullReturnsNull"], true);
        assert_eq!(nulls["staticToJsonNull"], "null");
        assert_eq!(
            BatchAckMessageRequestBody::java_encode_optional(None).unwrap(),
            None
        );
        assert_eq!(
            BatchAckMessageRequestBody::java_to_json_optional(None, false).unwrap(),
            nulls["staticToJsonNull"]
        );
        assert!(
            BatchAckMessageRequestBody::java_decode_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            BatchAckMessageRequestBody::java_decode_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        assert!(
            BatchAckMessageRequestBody::java_from_json_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            BatchAckMessageRequestBody::java_from_json_nullable(Some("null"))
                .unwrap()
                .is_none()
        );
        assert!(
            BatchAckMessageRequestBody::java_decode_list_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            BatchAckMessageRequestBody::java_decode_list_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        let null_members =
            BatchAckMessageRequestBody::java_decode_list_nullable(Some(b"[null,{}]")).unwrap();
        assert_eq!(null_members.as_ref().map(Vec::len), Some(2));
        assert!(null_members.unwrap()[0].is_none());
        assert_eq!(nulls["decodeNullBytesReturnsNull"], true);
        assert_eq!(nulls["decodeJsonNullReturnsNull"], true);
        assert_eq!(nulls["fromJsonNullInputReturnsNull"], true);
        assert_eq!(nulls["fromJsonJsonNullReturnsNull"], true);
        assert_eq!(nulls["decodeListNullBytesReturnsNull"], true);
        assert_eq!(nulls["decodeListJsonNullReturnsNull"], true);
        assert_eq!(nulls["decodeListNullMemberCount"], 2);
        assert_eq!(nulls["decodeListPreservesNullMember"], true);
    }

    #[test]
    fn assignment_body_public_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();

        let request_case = &fixture["queryAssignmentRequestApiCase"];
        let mut request = QueryAssignmentRequestBody::default();
        assert_eq!(
            json!({
                "topic": request.get_topic(),
                "consumerGroup": request.get_consumer_group(),
                "clientId": request.get_client_id(),
                "strategyName": request.get_strategy_name(),
                "messageModel": request.get_message_model(),
            }),
            request_case["defaults"]
        );
        request.set_topic(Some("orders".into()));
        request.set_consumer_group(Some("workers".into()));
        request.set_client_id(Some("client-a".into()));
        request.set_strategy_name(Some("AllocateByCircle".into()));
        request.set_message_model(Some(super::super::heartbeat::MessageModel::Clustering));
        assert_eq!(
            json!({
                "topic": request.get_topic(),
                "consumerGroup": request.get_consumer_group(),
                "clientId": request.get_client_id(),
                "strategyName": request.get_strategy_name(),
                "messageModel": request.get_message_model(),
            }),
            request_case["values"]
        );
        request.set_topic(None);
        request.set_consumer_group(None);
        request.set_client_id(None);
        request.set_strategy_name(None);
        request.set_message_model(None);
        assert_eq!(request_case["nullSettersRemainNull"], true);
        assert_eq!(
            json!({
                "topic": request.get_topic(),
                "consumerGroup": request.get_consumer_group(),
                "clientId": request.get_client_id(),
                "strategyName": request.get_strategy_name(),
                "messageModel": request.get_message_model(),
            }),
            json!({
                "topic": null,
                "consumerGroup": null,
                "clientId": null,
                "strategyName": null,
                "messageModel": null,
            })
        );

        let response_case = &fixture["queryAssignmentResponseApiCase"];
        assert_eq!(response_case["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(response_case["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(response_case["sameInstanceEquals"], true);
        assert_eq!(response_case["equalValuesDistinctInstanceEquals"], false);
        let mut response = QueryAssignmentResponseBody::default();
        assert_eq!(
            serde_json::to_value(response.get_message_queue_assignments()).unwrap(),
            response_case["defaultAssignments"]
        );
        // This fixture intentionally exercises Java's mutable Set element objects.
        #[allow(clippy::mutable_key_type)]
        let assignments: QueryAssignmentSet =
            serde_json::from_value(response_case["assignedValues"].clone()).unwrap();
        let assigned_element = assignments
            .iter()
            .next()
            .and_then(Option::as_ref)
            .unwrap()
            .clone();
        let assigned_alias = SharedValue::new(assignments);
        let caller_alias = assigned_alias.clone();
        response.set_message_queue_assignments(Some(assigned_alias.clone()));
        let getter_alias = response.get_message_queue_assignments().unwrap();
        assert!(assigned_alias.ptr_eq(&getter_alias));
        assert!(caller_alias.ptr_eq(&getter_alias));
        let getter_element = getter_alias.with(|assignments| {
            assignments
                .iter()
                .next()
                .and_then(Option::as_ref)
                .unwrap()
                .clone()
        });
        assert!(assigned_element.ptr_eq(&getter_element));
        assert_eq!(response_case["getterReturnsAssignedElement"], true);
        assigned_element.with_mut(|assignment| assignment.set_mode(Some(MessageRequestMode::Pull)));
        assert_eq!(
            getter_element.with(MessageQueueAssignment::mode),
            Some(MessageRequestMode::Pull)
        );
        assigned_element.with_mut(|assignment| assignment.set_mode(Some(MessageRequestMode::Pop)));
        getter_element.with_mut(|assignment| assignment.set_mode(Some(MessageRequestMode::Pull)));
        assert_eq!(
            assigned_element.with(MessageQueueAssignment::mode),
            Some(MessageRequestMode::Pull)
        );
        getter_element.with_mut(|assignment| assignment.set_mode(Some(MessageRequestMode::Pop)));
        assert_eq!(response_case["setterElementMutationVisibleToGetter"], true);
        assert_eq!(
            response_case["getterElementMutationVisibleToSetterObject"],
            true
        );
        assert_eq!(
            getter_alias.with(std::collections::HashSet::len),
            response_case["valueCount"]
        );
        caller_alias.with_mut(|assignments_mut| {
            assignments_mut.clear();
            assignments_mut.insert(Some(SharedValue::new(MessageQueueAssignment {
                message_queue: Some(SharedValue::new(MessageQueue::new("orders", "broker", 3))),
                mode: Some(MessageRequestMode::Pull),
                attachments: None,
            })));
        });
        assert_eq!(
            getter_alias.with(|assignments| assignments.iter().any(|assignment| assignment
                .as_ref()
                .is_some_and(|assignment| assignment.with(|assignment| assignment
                    .message_queue()
                    .is_some_and(|queue| queue.with(|queue| queue.queue_id() == 3)))))),
            response_case["externalMutationVisible"]
        );
        getter_alias.with_mut(|assignments_mut| {
            assignments_mut.clear();
            assignments_mut.insert(Some(SharedValue::new(MessageQueueAssignment {
                message_queue: Some(SharedValue::new(MessageQueue::new("orders", "broker", 4))),
                mode: Some(MessageRequestMode::Pop),
                attachments: None,
            })));
        });
        assert_eq!(
            caller_alias.with(|assignments| assignments.iter().any(|assignment| assignment
                .as_ref()
                .is_some_and(|assignment| assignment.with(|assignment| assignment
                    .message_queue()
                    .is_some_and(|queue| queue.with(|queue| queue.queue_id() == 4)))))),
            response_case["getterMutationVisibleToAssignedSet"]
        );
        let mutated_values = getter_alias.with(|assignments| {
            assignments
                .iter()
                .map(|assignment| {
                    assignment.as_ref().map(|assignment| {
                        assignment.with(|assignment| {
                            json!({
                                "messageQueue": assignment.message_queue(),
                                "mode": assignment.mode(),
                                "attachments": assignment.attachments(),
                            })
                        })
                    })
                })
                .collect::<Vec<_>>()
        });
        assert_eq!(json!(mutated_values), response_case["mutatedValues"]);
        // Java returns the same mutable Set; the shared handle keeps its identity on both sides.
        assert_eq!(response_case["setterRetainsSetIdentity"], true);
        response.set_message_queue_assignments(None);
        assert!(response.get_message_queue_assignments().is_none());
        assert_eq!(response_case["nullSetterRemainsNull"], true);

        let mode_case = &fixture["setRequestModeRequestApiCase"];
        let mut mode_body = SetMessageRequestModeRequestBody::default();
        assert_eq!(
            json!({
                "topic": mode_body.get_topic(),
                "consumerGroup": mode_body.get_consumer_group(),
                "mode": mode_body.get_mode(),
                "popShareQueueNum": mode_body.get_pop_share_queue_num(),
            }),
            mode_case["defaults"]
        );
        mode_body.set_topic(Some("orders".into()));
        mode_body.set_consumer_group(Some("workers".into()));
        mode_body.set_mode(Some(MessageRequestMode::Pop));
        mode_body.set_pop_share_queue_num(2);
        assert_eq!(
            json!({
                "topic": mode_body.get_topic(),
                "consumerGroup": mode_body.get_consumer_group(),
                "mode": mode_body.get_mode(),
                "popShareQueueNum": mode_body.get_pop_share_queue_num(),
            }),
            mode_case["values"]
        );
        mode_body.set_topic(None);
        mode_body.set_consumer_group(None);
        mode_body.set_mode(None);
        mode_body.set_pop_share_queue_num(0);
        assert_eq!(mode_case["nullSettersRemainNull"], true);
        assert_eq!(
            mode_body.get_pop_share_queue_num(),
            mode_case["primitiveAfterReset"]
        );
    }

    #[test]
    fn protocol_dto_identity_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/protocol/dto-identity.json"))
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

        for dto in [
            "QueryAssignmentRequestBody",
            "SetMessageRequestModeRequestBody",
            "GetConsumerStatusBody",
            "ResetOffsetBody",
            "ConsumeMessageDirectlyResult",
        ] {
            let identity = &fixture["identityCases"][dto];
            assert_eq!(identity["equalsDeclaredBy"], "java.lang.Object", "{dto}");
            assert_eq!(identity["hashCodeDeclaredBy"], "java.lang.Object", "{dto}");
            assert_eq!(identity["sameObjectEqualsItself"], true, "{dto}");
            assert_eq!(identity["distinctDefaultObjectsEquals"], false, "{dto}");
        }
    }

    #[test]
    fn remaining_runtime_body_public_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let assert_java_object_identity = |case: &serde_json::Value| {
            assert_eq!(case["equalsDeclaringClass"], "java.lang.Object");
            assert_eq!(case["hashCodeDeclaringClass"], "java.lang.Object");
            assert_eq!(case["sameInstanceEquals"], true);
            assert_eq!(case["equalValuesDistinctInstanceEquals"], false);
        };

        let check_case = &fixture["checkClientBodyApiCase"];
        let mut check = CheckClientRequestBody::default();
        assert_eq!(check_case["equalsSameInstance"], true);
        assert_eq!(check_case["equalsEqualFieldsDistinctInstance"], false);
        assert_eq!(check_case["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(check_case["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(
            json!({
                "clientId": check.get_client_id(),
                "group": check.get_group(),
                "subscriptionData": check.get_subscription_data(),
                "namespace": check.get_namespace(),
            }),
            check_case["defaults"]
        );
        let subscription = SharedValue::new(
            serde_json::from_value(check_case["values"]["subscriptionData"].clone()).unwrap(),
        );
        check.set_client_id(Some("client-a".into()));
        check.set_group(Some("workers".into()));
        check.set_subscription_data(Some(subscription.clone()));
        check.set_namespace(Some("tenant".into()));
        let subscription_values = check.get_subscription_data().map(|value| {
            value.with(|value| {
                json!({
                    "topic": value.topic.as_deref(),
                    "subString": value.sub_string.as_deref(),
                    "subVersion": value.sub_version,
                    "classFilterMode": value.class_filter_mode,
                    "expressionType": value.expression_type.as_deref(),
                    "tagsSet": value.tags_set.as_ref(),
                })
            })
        });
        assert_eq!(
            json!({
                "clientId": check.get_client_id(),
                "group": check.get_group(),
                "subscriptionData": subscription_values,
                "namespace": check.get_namespace(),
            }),
            check_case["values"]
        );
        let subscription_mut = check.get_subscription_data_mut().unwrap();
        assert_eq!(
            subscription_mut.ptr_eq(&subscription),
            check_case["getterReturnsAssignedSubscription"]
        );
        // Mutations through the original setter handle are visible through the body getter.
        subscription.with_mut(|value| value.set_sub_version(43));
        assert_eq!(
            subscription_mut.with(|value| value.get_sub_version() == 43),
            check_case["externalMutationVisible"]
        );
        // Mutations through the getter handle are visible through the original setter handle.
        subscription_mut.with_mut(|value| value.set_sub_string(Some("orders*".into())));
        assert_eq!(
            subscription.with(|value| value.get_sub_string() == Some("orders*")),
            check_case["getterMutationVisibleToAssignedSubscription"]
        );
        assert_eq!(
            subscription_mut.with(|value| json!({
                "topic": value.get_topic(),
                "subString": value.get_sub_string(),
                "subVersion": value.get_sub_version(),
                "classFilterMode": value.is_class_filter_mode(),
                "expressionType": value.get_expression_type(),
                "tagsSet": value.get_tags_set(),
            })),
            check_case["mutatedSubscriptionData"]
        );
        check.set_client_id(None);
        check.set_group(None);
        check.set_subscription_data(None);
        check.set_namespace(None);
        assert!(check.get_client_id().is_none());
        assert!(check.get_group().is_none());
        assert!(check.get_subscription_data().is_none());
        assert!(check.get_namespace().is_none());
        assert_eq!(check_case["nullSettersRemainNull"], true);

        let list_case = &fixture["consumerListApiCase"];
        let mut consumer_list = ConsumerListByGroupResponseBody::default();
        assert_eq!(
            serde_json::to_value(consumer_list.get_consumer_id_list()).unwrap(),
            list_case["defaultConsumerIds"]
        );
        let consumer_ids: Vec<Option<String>> =
            serde_json::from_value(list_case["assignedValues"].clone()).unwrap();
        let caller_alias = SharedValue::new(consumer_ids);
        let assigned_alias = caller_alias.clone();
        consumer_list.set_consumer_id_list(Some(caller_alias.clone()));
        let getter_alias = consumer_list.get_consumer_id_list().unwrap();
        assert_eq!(
            assigned_alias.ptr_eq(&getter_alias),
            list_case["getterReturnsAssignedList"]
        );
        caller_alias.with_mut(|ids| ids.push(Some("client-c".into())));
        assert_eq!(
            getter_alias.with(|ids| ids.contains(&Some("client-c".into()))),
            list_case["externalMutationVisible"]
        );
        getter_alias.with_mut(|ids| ids.push(Some("client-d".into())));
        assert_eq!(
            assigned_alias.with(|ids| ids.contains(&Some("client-d".into()))),
            list_case["getterMutationVisibleToAssignedList"]
        );
        assert_eq!(
            serde_json::to_value(getter_alias.snapshot()).unwrap(),
            list_case["mutatedValues"]
        );
        consumer_list.set_consumer_id_list(None);
        assert!(consumer_list.get_consumer_id_list().is_none());
        assert_eq!(list_case["nullSetterRemainsNull"], true);
        assert_eq!(list_case["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(list_case["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(list_case["sameInstanceEquals"], true);
        assert_eq!(list_case["equalValuesDistinctInstanceEquals"], false);

        let lock_case = &fixture["lockBatchRequestApiCase"];
        let mut lock = LockBatchRequestBody::default();
        assert_eq!(
            json!({
                "consumerGroup": lock.get_consumer_group(),
                "clientId": lock.get_client_id(),
                "onlyThisBroker": lock.is_only_this_broker(),
                "mqSet": lock.get_mq_set(),
            }),
            lock_case["defaults"]
        );
        assert_eq!(lock.to_string(), lock_case["defaultText"].as_str().unwrap());
        #[allow(clippy::mutable_key_type)]
        let lock_queues: MessageQueueSet =
            serde_json::from_value(lock_case["assignedValues"].clone()).unwrap();
        let lock_caller = SharedValue::new(lock_queues);
        let lock_assigned = lock_caller.clone();
        lock.set_consumer_group(Some("workers".into()));
        lock.set_client_id(Some("client-a".into()));
        lock.set_only_this_broker(true);
        lock.set_mq_set(Some(lock_caller.clone()));
        let lock_getter = lock.get_mq_set().unwrap();
        assert_eq!(
            lock_assigned.ptr_eq(&lock_getter),
            lock_case["getterReturnsAssignedSet"]
        );
        message_queue_set_element_aliases_match(lock_case, &lock_caller, &lock_getter);
        lock_caller.with_mut(|queues| {
            queues.remove(&Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 2,
            ))));
            queues.insert(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
        });
        assert_eq!(
            lock_getter.with(
                |queues| queues.contains(&Some(SharedValue::new(MessageQueue::new(
                    "orders", "broker", 3
                ))))
            ),
            lock_case["externalMutationVisible"]
        );
        lock_getter.with_mut(|queues| {
            queues.remove(&Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
            queues.insert(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 4,
            ))));
        });
        assert_eq!(
            lock_assigned.with(|queues| queues.contains(&Some(SharedValue::new(
                MessageQueue::new("orders", "broker", 4)
            )))),
            lock_case["getterMutationVisibleToAssignedSet"]
        );
        assert_eq!(
            json!({
                "consumerGroup": lock.get_consumer_group(),
                "clientId": lock.get_client_id(),
                "onlyThisBroker": lock.is_only_this_broker(),
                "mqSet": lock_getter.snapshot(),
            }),
            lock_case["mutatedValues"]
        );
        assert_eq!(lock.to_string(), lock_case["text"].as_str().unwrap());
        assert_java_object_identity(lock_case);
        lock.set_consumer_group(None);
        lock.set_client_id(None);
        lock.set_only_this_broker(false);
        lock.set_mq_set(None);
        assert!(lock.get_consumer_group().is_none());
        assert!(lock.get_client_id().is_none());
        assert!(!lock.is_only_this_broker());
        assert!(lock.get_mq_set().is_none());
        assert_eq!(lock_case["nullSettersRemainNull"], true);

        let unlock_case = &fixture["unlockBatchRequestApiCase"];
        let mut unlock = UnlockBatchRequestBody::default();
        assert_eq!(
            json!({
                "consumerGroup": unlock.get_consumer_group(),
                "clientId": unlock.get_client_id(),
                "onlyThisBroker": unlock.is_only_this_broker(),
                "mqSet": unlock.get_mq_set(),
            }),
            unlock_case["defaults"]
        );
        assert_eq!(
            unlock.to_string(),
            unlock_case["defaultText"].as_str().unwrap()
        );
        #[allow(clippy::mutable_key_type)]
        let unlock_queues: MessageQueueSet =
            serde_json::from_value(unlock_case["assignedValues"].clone()).unwrap();
        let unlock_caller = SharedValue::new(unlock_queues);
        let unlock_assigned = unlock_caller.clone();
        unlock.set_consumer_group(Some("workers".into()));
        unlock.set_client_id(Some("client-a".into()));
        unlock.set_only_this_broker(true);
        unlock.set_mq_set(Some(unlock_caller.clone()));
        let unlock_getter = unlock.get_mq_set().unwrap();
        assert_eq!(
            unlock_assigned.ptr_eq(&unlock_getter),
            unlock_case["getterReturnsAssignedSet"]
        );
        message_queue_set_element_aliases_match(unlock_case, &unlock_caller, &unlock_getter);
        unlock_caller.with_mut(|queues| {
            queues.remove(&Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 2,
            ))));
            queues.insert(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
        });
        assert_eq!(
            unlock_getter.with(|queues| queues.contains(&Some(SharedValue::new(
                MessageQueue::new("orders", "broker", 3)
            )))),
            unlock_case["externalMutationVisible"]
        );
        unlock_getter.with_mut(|queues| {
            queues.remove(&Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
            queues.insert(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 4,
            ))));
        });
        assert_eq!(
            unlock_assigned.with(|queues| queues.contains(&Some(SharedValue::new(
                MessageQueue::new("orders", "broker", 4)
            )))),
            unlock_case["getterMutationVisibleToAssignedSet"]
        );
        assert_eq!(
            json!({
                "consumerGroup": unlock.get_consumer_group(),
                "clientId": unlock.get_client_id(),
                "onlyThisBroker": unlock.is_only_this_broker(),
                "mqSet": unlock_getter.snapshot(),
            }),
            unlock_case["mutatedValues"]
        );
        assert_eq!(unlock.to_string(), unlock_case["text"].as_str().unwrap());
        assert_java_object_identity(unlock_case);
        unlock.set_consumer_group(None);
        unlock.set_client_id(None);
        unlock.set_only_this_broker(false);
        unlock.set_mq_set(None);
        assert!(unlock.get_consumer_group().is_none());
        assert!(unlock.get_client_id().is_none());
        assert!(!unlock.is_only_this_broker());
        assert!(unlock.get_mq_set().is_none());
        assert_eq!(unlock_case["nullSettersRemainNull"], true);

        let lock_response_case = &fixture["lockBatchResponseApiCase"];
        let mut lock_response = LockBatchResponseBody::default();
        assert_eq!(
            serde_json::to_value(lock_response.get_lock_okmq_set()).unwrap(),
            lock_response_case["defaultLockOKMQSet"]
        );
        #[allow(clippy::mutable_key_type)]
        let lock_response_queues: MessageQueueSet =
            serde_json::from_value(lock_response_case["assignedValues"].clone()).unwrap();
        let lock_response_caller = SharedValue::new(lock_response_queues);
        let lock_response_assigned = lock_response_caller.clone();
        lock_response.set_lock_okmq_set(Some(lock_response_caller.clone()));
        let lock_response_getter = lock_response.get_lock_okmq_set().unwrap();
        assert_eq!(
            lock_response_assigned.ptr_eq(&lock_response_getter),
            lock_response_case["getterReturnsAssignedSet"]
        );
        message_queue_set_element_aliases_match(
            lock_response_case,
            &lock_response_caller,
            &lock_response_getter,
        );
        lock_response_caller.with_mut(|queues| {
            queues.remove(&Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 2,
            ))));
            queues.insert(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
        });
        assert_eq!(
            lock_response_getter.with(|queues| queues.contains(&Some(SharedValue::new(
                MessageQueue::new("orders", "broker", 3)
            )))),
            lock_response_case["externalMutationVisible"]
        );
        lock_response_getter.with_mut(|queues| {
            queues.remove(&Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 3,
            ))));
            queues.insert(Some(SharedValue::new(MessageQueue::new(
                "orders", "broker", 4,
            ))));
        });
        assert_eq!(
            lock_response_assigned.with(|queues| queues.contains(&Some(SharedValue::new(
                MessageQueue::new("orders", "broker", 4)
            )))),
            lock_response_case["getterMutationVisibleToAssignedSet"]
        );
        assert_eq!(
            serde_json::to_value(lock_response_getter.snapshot()).unwrap(),
            lock_response_case["mutatedValues"]
        );
        assert_java_object_identity(lock_response_case);
        lock_response.set_lock_okmq_set(None);
        assert!(lock_response.get_lock_okmq_set().is_none());
        assert_eq!(lock_response_case["nullSetterRemainsNull"], true);

        let direct_case = &fixture["consumeDirectResultApiCase"];
        let mut direct = ConsumeMessageDirectlyResult::default();
        assert_eq!(
            json!({
                "order": direct.is_order(),
                "autoCommit": direct.is_auto_commit(),
                "consumeResult": direct.get_consume_result(),
                "remark": direct.get_remark(),
                "spentTimeMills": direct.get_spent_time_mills(),
            }),
            direct_case["defaults"]
        );
        assert_eq!(
            direct.to_string(),
            direct_case["defaultText"].as_str().unwrap()
        );
        direct.set_order(true);
        direct.set_auto_commit(false);
        direct.set_consume_result(Some(ConsumeMessageResult::CrSuccess));
        direct.set_remark(Some("consumed".into()));
        direct.set_spent_time_mills(42);
        assert_eq!(
            json!({
                "order": direct.is_order(),
                "autoCommit": direct.is_auto_commit(),
                "consumeResult": direct.get_consume_result(),
                "remark": direct.get_remark(),
                "spentTimeMills": direct.get_spent_time_mills(),
            }),
            direct_case["values"]
        );
        assert_eq!(direct.to_string(), direct_case["text"].as_str().unwrap());
        direct.set_consume_result(None);
        direct.set_remark(None);
        assert!(direct.get_consume_result().is_none());
        assert!(direct.get_remark().is_none());
        assert_eq!(direct_case["nullSettersRemainNull"], true);
    }

    #[test]
    fn consumer_status_and_reset_offset_public_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();

        let status_case = &fixture["consumerStatusBodyApiCase"];
        let mut status = GetConsumerStatusBody::default();
        assert_eq!(
            serde_json::to_value(status.get_message_queue_table()).unwrap(),
            status_case["defaults"]["messageQueueTable"]
        );
        assert_eq!(
            serde_json::to_value(status.get_consumer_table()).unwrap(),
            status_case["defaults"]["consumerTable"]
        );
        let message_queue_table = SharedValue::new(
            serde_json::from_value::<QueueOffsetEntries>(
                status_case["values"]["messageQueueTable"].clone(),
            )
            .unwrap(),
        );
        let consumer_table = SharedValue::new(
            serde_json::from_value::<ConsumerQueueOffsets>(
                status_case["values"]["consumerTable"].clone(),
            )
            .unwrap(),
        );
        let workers_table = consumer_table.with(|table| table["workers"].as_ref().unwrap().clone());
        status.set_message_queue_table(Some(message_queue_table.clone()));
        status.set_consumer_table(Some(consumer_table.clone()));
        assert!(
            status
                .get_message_queue_table()
                .unwrap()
                .ptr_eq(&message_queue_table)
        );
        assert!(status.get_consumer_table().unwrap().ptr_eq(&consumer_table));
        let nested_workers_table =
            consumer_table.with(|table| table["workers"].as_ref().unwrap().clone());
        assert!(workers_table.ptr_eq(&nested_workers_table));
        assert_eq!(
            serde_json::to_value(status.get_message_queue_table()).unwrap(),
            status_case["values"]["messageQueueTable"]
        );
        assert_eq!(
            serde_json::to_value(status.get_consumer_table()).unwrap(),
            status_case["values"]["consumerTable"]
        );
        message_queue_table.with_mut(|table| {
            table.push((MessageQueue::new("orders", "broker", 4), Some(99)));
        });
        assert_eq!(
            status.get_message_queue_table().unwrap().with(|table| table
                .iter()
                .any(|(queue, offset)| queue.queue_id() == 4 && *offset == Some(99))),
            status_case["messageQueueTableExternalMutationVisible"]
        );
        status.get_consumer_table().unwrap().with_mut(|table| {
            table.insert(
                "getter-added".to_owned(),
                Some(SharedValue::new(vec![(
                    MessageQueue::new("orders", "broker", 5),
                    Some(100),
                )])),
            );
        });
        assert_eq!(
            consumer_table.with(|table| table.contains_key("getter-added")),
            status_case["consumerTableGetterMutationVisibleToAssignedMap"]
        );
        workers_table.with_mut(|table| {
            table.push((MessageQueue::new("orders", "broker", 6), Some(101)));
        });
        assert_eq!(
            consumer_table.with(
                |table| table["workers"].as_ref().unwrap().with(|entries| entries
                    .iter()
                    .any(|(queue, offset)| queue.queue_id() == 6 && *offset == Some(101)))
            ),
            status_case["nestedGetterMutationVisibleToAssignedTable"]
        );
        assert_eq!(
            json!({
                "messageQueueTable": status.get_message_queue_table(),
                "consumerTable": status.get_consumer_table(),
            }),
            status_case["mutatedValues"]
        );
        // Java returns the same mutable outer Maps and nested Map assigned by setters.
        assert_eq!(status_case["getterReturnsAssignedMessageQueueTable"], true);
        assert_eq!(status_case["getterReturnsAssignedConsumerTable"], true);
        assert_eq!(status_case["nestedGetterReturnsAssignedTable"], true);
        status.set_message_queue_table(None);
        status.set_consumer_table(None);
        assert!(status.get_message_queue_table().is_none());
        assert!(status.get_consumer_table().is_none());
        assert_eq!(status_case["nullSettersRemainNull"], true);

        let reset_case = &fixture["resetOffsetBodyApiCase"];
        let mut reset = ResetOffsetBody::default();
        assert_eq!(
            serde_json::to_value(reset.get_offset_table()).unwrap(),
            reset_case["defaults"]
        );
        let offset_table = SharedValue::new(
            serde_json::from_value::<NullableQueueOffsetEntries>(reset_case["values"].clone())
                .unwrap(),
        );
        reset.set_offset_table(Some(offset_table.clone()));
        assert!(reset.get_offset_table().unwrap().ptr_eq(&offset_table));
        assert_eq!(
            serde_json::to_value(reset.get_offset_table()).unwrap(),
            reset_case["values"]
        );
        assert_eq!(reset_case["getterReturnsAssignedMap"], true);
        assert_eq!(reset_case["externalMutationVisible"], true);
        assert_eq!(reset_case["getterMutationVisibleToAssignedMap"], true);
        offset_table.with_mut(|table| {
            table.push((MessageQueue::new("orders", "broker", 4), Some(99)));
        });
        reset.get_offset_table().unwrap().with_mut(|table| {
            table.push((MessageQueue::new("orders", "broker", 5), Some(100)));
        });
        assert_eq!(
            serde_json::to_value(reset.get_offset_table()).unwrap(),
            reset_case["mutatedValues"]
        );
        reset.set_offset_table(None);
        assert!(reset.get_offset_table().is_none());
        assert_eq!(reset_case["nullSetterRemainsNull"], true);
    }

    #[test]
    fn check_client_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/client-check.json"))
                .unwrap();
        let cases = fixture["checkClientBodyInputCases"].as_array().unwrap();
        assert_eq!(cases.len(), 43);
        for required in [
            "shape-clientId-array",
            "shape-group-fraction",
            "shape-subscriptionData-object",
            "shape-namespace-boolean",
            "root-null",
            "root-empty-object",
            "root-empty-array",
            "root-string",
            "root-integer",
            "root-fraction",
            "root-boolean",
        ] {
            assert!(
                cases.iter().any(|case| case["id"] == required),
                "missing {required}"
            );
        }
        for case in cases {
            let decoded = serde_fastjson::from_fastjson_str::<CheckClientRequestBody>(
                case["rawInput"].as_str().unwrap(),
            );
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(decoded.is_ok(), java_accepts, "{}: {decoded:?}", case["id"]);
            if let Ok(decoded) = decoded {
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    case["fields"],
                    "{}",
                    case["id"]
                );
            }
        }
    }

    #[test]
    fn check_client_inherited_remoting_serialization_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let api = &fixture["checkClientBodyApiCase"];
        let inherited = api["bodyInheritedSerializableMethods"].as_array().unwrap();
        for method in [
            "encode()",
            "encode([Lcom.alibaba.fastjson2.JSONWriter$Feature;)",
            "toJson()",
            "toJson(boolean)",
            "encode(java.lang.Object)",
            "toJson(java.lang.Object,boolean)",
            "decode([B,java.lang.Class)",
            "decodeList([B,java.lang.Class)",
            "fromJson(java.lang.String,java.lang.Class)",
        ] {
            assert!(
                inherited.iter().any(|signature| {
                    signature
                        .as_str()
                        .is_some_and(|signature| signature.contains(method))
                }),
                "Java inherited method missing from fixture: {method}"
            );
        }

        for case in fixture["checkClientInheritedSerializationCases"]
            .as_array()
            .unwrap()
        {
            let value =
                CheckClientRequestBody::java_decode(case["rawInput"].as_str().unwrap().as_bytes())
                    .unwrap();
            let encoded = value.java_encode().unwrap();
            assert_eq!(
                value.java_encode_with_features(&[]).unwrap(),
                encoded,
                "{} empty writer-feature array",
                case["id"]
            );
            assert_eq!(
                String::from_utf8(encoded.clone()).unwrap(),
                case["instanceEncodedJson"],
                "{} encode",
                case["id"]
            );
            let browser_compatible = value
                .java_encode_with_features(&[JavaJsonWriterFeature::BrowserCompatible])
                .unwrap();
            let expected_browser_compatible = case["writerFeatureOverrides"]["BrowserCompatible"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                browser_compatible,
                expected_browser_compatible.as_bytes(),
                "{} BrowserCompatible",
                case["id"]
            );
            assert_eq!(
                value
                    .java_encode_with_features(&[JavaJsonWriterFeature::OptimizedForAscii])
                    .unwrap(),
                case["instanceEncodedJson"].as_str().unwrap().as_bytes(),
                "{} OptimizedForAscii",
                case["id"]
            );
            for (feature, feature_name) in [
                (
                    JavaJsonWriterFeature::WriteNameAsSymbol,
                    "WriteNameAsSymbol",
                ),
                (
                    JavaJsonWriterFeature::NotWriteRootClassName,
                    "NotWriteRootClassName",
                ),
                (
                    JavaJsonWriterFeature::NotWriteHashMapArrayListClassName,
                    "NotWriteHashMapArrayListClassName",
                ),
                (
                    JavaJsonWriterFeature::NotWriteNumberClassName,
                    "NotWriteNumberClassName",
                ),
                (
                    JavaJsonWriterFeature::NotWriteSetClassName,
                    "NotWriteSetClassName",
                ),
                (
                    JavaJsonWriterFeature::WriteByteArrayAsBase64,
                    "WriteByteArrayAsBase64",
                ),
                (
                    JavaJsonWriterFeature::WritePairAsJavaBean,
                    "WritePairAsJavaBean",
                ),
                (
                    JavaJsonWriterFeature::WriteThrowableClassName,
                    "WriteThrowableClassName",
                ),
                (
                    JavaJsonWriterFeature::WriterUtilDateAsMillis,
                    "WriterUtilDateAsMillis",
                ),
                (
                    JavaJsonWriterFeature::WriteBigDecimalAsPlain,
                    "WriteBigDecimalAsPlain",
                ),
                (
                    JavaJsonWriterFeature::WriteFloatSpecialAsString,
                    "WriteFloatSpecialAsString",
                ),
                (JavaJsonWriterFeature::BrowserSecure, "BrowserSecure"),
                (
                    JavaJsonWriterFeature::WriteEnumsUsingName,
                    "WriteEnumsUsingName",
                ),
                (
                    JavaJsonWriterFeature::WriteEnumUsingToString,
                    "WriteEnumUsingToString",
                ),
                (
                    JavaJsonWriterFeature::IgnoreErrorGetter,
                    "IgnoreErrorGetter",
                ),
                (
                    JavaJsonWriterFeature::IgnoreNonFieldGetter,
                    "IgnoreNonFieldGetter",
                ),
                (JavaJsonWriterFeature::WriteNulls, "WriteNulls"),
                (
                    JavaJsonWriterFeature::WriteMapNullValue,
                    "WriteMapNullValue",
                ),
                (
                    JavaJsonWriterFeature::WriteNullListAsEmpty,
                    "WriteNullListAsEmpty",
                ),
                (JavaJsonWriterFeature::BeanToArray, "BeanToArray"),
                (JavaJsonWriterFeature::FieldBased, "FieldBased"),
                (
                    JavaJsonWriterFeature::WriteNullBooleanAsFalse,
                    "WriteNullBooleanAsFalse",
                ),
                (JavaJsonWriterFeature::MapSortField, "MapSortField"),
                (
                    JavaJsonWriterFeature::SortMapEntriesByKeys,
                    "SortMapEntriesByKeys",
                ),
                (JavaJsonWriterFeature::LargeObject, "LargeObject"),
            ] {
                let featured = value.java_encode_with_features(&[feature]).unwrap();
                let expected = case["writerFeatureOverrides"][feature_name]
                    .as_str()
                    .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
                assert_eq!(
                    featured,
                    expected.as_bytes(),
                    "{} {feature_name}",
                    case["id"]
                );
            }
            assert_eq!(
                value
                    .java_encode_with_features(&[
                        JavaJsonWriterFeature::BrowserSecure,
                        JavaJsonWriterFeature::UnquoteFieldName,
                    ])
                    .unwrap(),
                case["writerFeatureOverrides"]["BrowserSecure+UnquoteFieldName"]
                    .as_str()
                    .unwrap()
                    .as_bytes(),
                "{} BrowserSecure+UnquoteFieldName",
                case["id"]
            );
            let featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::WriteNonStringValueAsString])
                .unwrap();
            let expected_featured = case["writerFeatureOverrides"]["WriteNonStringValueAsString"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                featured,
                expected_featured.as_bytes(),
                "{} WriteNonStringValueAsString",
                case["id"]
            );
            let boolean_featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::WriteBooleanAsNumber])
                .unwrap();
            let expected_boolean_featured = case["writerFeatureOverrides"]["WriteBooleanAsNumber"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                boolean_featured,
                expected_boolean_featured.as_bytes(),
                "{} WriteBooleanAsNumber",
                case["id"]
            );
            let combined_featured = value
                .java_encode_with_features(&[
                    JavaJsonWriterFeature::WriteBooleanAsNumber,
                    JavaJsonWriterFeature::WriteNonStringValueAsString,
                ])
                .unwrap();
            let expected_combined_featured =
                case["writerFeatureOverrides"]["WriteBooleanAsNumber+WriteNonStringValueAsString"]
                    .as_str()
                    .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                combined_featured,
                expected_combined_featured.as_bytes(),
                "{} combined boolean writer features",
                case["id"]
            );
            let escaped_featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::EscapeNoneAscii])
                .unwrap();
            let expected_escaped_featured = case["writerFeatureOverrides"]["EscapeNoneAscii"]
                .as_str()
                .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap());
            assert_eq!(
                escaped_featured,
                expected_escaped_featured.as_bytes(),
                "{} EscapeNoneAscii",
                case["id"]
            );
            let pretty_featured = value
                .java_encode_with_features(&[JavaJsonWriterFeature::PrettyFormat])
                .unwrap();
            let expected_pretty_featured = case["writerFeatureOverrides"]["PrettyFormat"]
                .as_str()
                .unwrap_or_else(|| case["instancePrettyJson"].as_str().unwrap());
            assert_eq!(
                pretty_featured,
                expected_pretty_featured.as_bytes(),
                "{} PrettyFormat",
                case["id"]
            );
            assert_eq!(
                value
                    .java_encode_with_features(&[JavaJsonWriterFeature::WriteLongAsString])
                    .unwrap(),
                case["writerFeatureOverrides"]["WriteLongAsString"]
                    .as_str()
                    .unwrap_or_else(|| case["instanceEncodedJson"].as_str().unwrap())
                    .as_bytes(),
                "{} WriteLongAsString",
                case["id"]
            );
            for (feature, feature_name) in [
                (
                    JavaJsonWriterFeature::PrettyFormatWith2Space,
                    "PrettyFormatWith2Space",
                ),
                (
                    JavaJsonWriterFeature::PrettyFormatWith4Space,
                    "PrettyFormatWith4Space",
                ),
            ] {
                assert_eq!(
                    value.java_encode_with_features(&[feature]).unwrap(),
                    case["writerFeatureOverrides"][feature_name]
                        .as_str()
                        .unwrap()
                        .as_bytes(),
                    "{} {feature_name}",
                    case["id"]
                );
            }
            assert_eq!(
                String::from_utf8(
                    CheckClientRequestBody::java_encode_optional(Some(&value))
                        .unwrap()
                        .unwrap()
                )
                .unwrap(),
                case["staticEncodedJson"],
                "{} static encode",
                case["id"]
            );
            assert_eq!(
                value.java_to_json(false).unwrap(),
                case["instanceJson"],
                "{} compact JSON",
                case["id"]
            );
            assert_eq!(
                value.java_to_json(true).unwrap(),
                case["instancePrettyJson"],
                "{} pretty JSON",
                case["id"]
            );
            assert_eq!(
                CheckClientRequestBody::java_to_json_optional(Some(&value), false).unwrap(),
                case["staticJson"],
                "{} static JSON",
                case["id"]
            );
            assert_eq!(
                CheckClientRequestBody::java_decode(&encoded)
                    .unwrap()
                    .java_to_json(false)
                    .unwrap(),
                case["decodedJson"],
                "{} decode",
                case["id"]
            );
            assert_eq!(
                CheckClientRequestBody::java_from_json(case["instanceJson"].as_str().unwrap())
                    .unwrap()
                    .java_to_json(false)
                    .unwrap(),
                case["fromJsonJson"],
                "{} fromJson",
                case["id"]
            );
            let list = format!("[{}]", case["instanceJson"].as_str().unwrap());
            assert_eq!(
                CheckClientRequestBody::java_decode_list(list.as_bytes()).unwrap()[0]
                    .java_to_json(false)
                    .unwrap(),
                case["decodedListJson"],
                "{} decodeList",
                case["id"]
            );
        }

        let nulls = &fixture["checkClientInheritedSerializationNullUtilities"];
        assert_eq!(
            CheckClientRequestBody::java_encode_optional(None).unwrap(),
            None
        );
        assert_eq!(
            CheckClientRequestBody::java_to_json_optional(None, false).unwrap(),
            nulls["staticToJsonNull"]
        );
        assert_eq!(
            CheckClientRequestBody::java_decode_nullable(None)
                .unwrap()
                .is_none(),
            nulls["decodeNullBytesReturnsNull"]
        );
        assert_eq!(
            CheckClientRequestBody::java_decode_nullable(Some(b"null"))
                .unwrap()
                .is_none(),
            nulls["decodeJsonNullReturnsNull"]
        );
        assert!(
            CheckClientRequestBody::java_from_json_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            CheckClientRequestBody::java_from_json_nullable(Some("null"))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            CheckClientRequestBody::java_decode_list_nullable(None)
                .unwrap()
                .is_none(),
            nulls["decodeListNullBytesReturnsNull"]
        );
        assert_eq!(
            CheckClientRequestBody::java_decode_list_nullable(Some(b"null"))
                .unwrap()
                .is_none(),
            nulls["decodeListJsonNullReturnsNull"]
        );
        let list = CheckClientRequestBody::java_decode_list_nullable(Some(b"[null,{}]"))
            .unwrap()
            .unwrap();
        assert_eq!(list.len(), nulls["decodeListNullMemberCount"]);
        assert_eq!(list[0].is_none(), nulls["decodeListPreservesNullMember"]);
    }

    #[test]
    fn escape_none_ascii_matches_invoked_java_for_inherited_body_serializers() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let case = &fixture["remotingSerializableWriterEscapingCase"];
        let raw_input = case["rawInput"].as_str().unwrap();
        let value = CheckClientRequestBody::java_decode(raw_input.as_bytes()).unwrap();
        assert_eq!(
            String::from_utf8(value.java_encode().unwrap()).unwrap(),
            case["instanceEncodedJson"]
        );
        assert_eq!(
            String::from_utf8(
                value
                    .java_encode_with_features(&[JavaJsonWriterFeature::EscapeNoneAscii])
                    .unwrap()
            )
            .unwrap(),
            case["escapeNoneAsciiJson"]
        );

        let generic_body: serde_json::Value = serde_json::from_str(raw_input).unwrap();
        assert_eq!(
            java_fastjson_remoting_body_json_with_features(
                &generic_body,
                false,
                &[JavaJsonWriterFeature::EscapeNoneAscii],
            )
            .unwrap(),
            case["escapeNoneAsciiJson"]
        );
    }

    #[test]
    fn remoting_serializable_body_adapters_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let timezone = match std::env::var("TZ").as_deref() {
            Ok("Asia/Taipei") => "Asia/Taipei",
            Ok("UTC" | "Etc/UTC" | "GMT" | "Etc/GMT") | Err(_) => "UTC",
            Ok(other) => panic!(
                "runtime lookup fixture has timestamp strings for UTC and Asia/Taipei; TZ={other}"
            ),
        };

        macro_rules! assert_body_serialization {
            ($dto:ty, $name:literal) => {{
                let dto_case = &fixture["remotingSerializableBodyCases"][$name];
                let methods = dto_case["inheritedMethods"].as_array().unwrap();
                assert_eq!(methods.len(), 9, "{} inherited method inventory", $name);
                for case in dto_case["cases"].as_array().unwrap() {
                    let expected = case["byTimeZone"].get(timezone).unwrap_or(case);
                    let value =
                        <$dto>::java_decode(case["rawInput"].as_str().unwrap().as_bytes()).unwrap();
                    let encoded = value.java_encode().unwrap();
                    assert_eq!(
                        value.java_encode_with_features(&[]).unwrap(),
                        encoded,
                        "{} {} empty writer-feature array",
                        $name,
                        case["id"]
                    );
                    let browser_compatible = value
                        .java_encode_with_features(&[JavaJsonWriterFeature::BrowserCompatible])
                        .unwrap();
                    let expected_browser_compatible = expected["writerFeatureOverrides"]
                        ["BrowserCompatible"]
                        .as_str()
                        .unwrap_or_else(|| expected["instanceEncodedJson"].as_str().unwrap());
                    assert_eq!(
                        browser_compatible,
                        expected_browser_compatible.as_bytes(),
                        "{} {} BrowserCompatible",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        value
                            .java_encode_with_features(&[
                                JavaJsonWriterFeature::OptimizedForAscii,
                            ])
                            .unwrap(),
                        expected["instanceEncodedJson"].as_str().unwrap().as_bytes(),
                        "{} {} OptimizedForAscii",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        String::from_utf8(encoded.clone()).unwrap(),
                        expected["instanceEncodedJson"],
                        "{} {} instance encode",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        String::from_utf8(
                            <$dto>::java_encode_optional(Some(&value)).unwrap().unwrap()
                        )
                        .unwrap(),
                        expected["staticEncodedJson"],
                        "{} {} static encode",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        value.java_to_json(false).unwrap(),
                        expected["instanceJson"],
                        "{} {} instance JSON",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        value.java_to_json(true).unwrap(),
                        expected["instancePrettyJson"],
                        "{} {} pretty JSON",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        <$dto>::java_to_json_optional(Some(&value), false).unwrap(),
                        expected["staticJson"],
                        "{} {} static JSON",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        <$dto>::java_decode(&encoded)
                            .unwrap()
                            .java_to_json(false)
                            .unwrap(),
                        expected["decodedJson"],
                        "{} {} decode",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        <$dto>::java_from_json(case["instanceJson"].as_str().unwrap())
                            .unwrap()
                            .java_to_json(false)
                            .unwrap(),
                        expected["fromJsonJson"],
                        "{} {} fromJson",
                        $name,
                        case["id"]
                    );
                    let encoded_list = format!("[{}]", case["instanceJson"].as_str().unwrap());
                    assert_eq!(
                        <$dto>::java_decode_list(encoded_list.as_bytes()).unwrap()[0]
                            .java_to_json(false)
                            .unwrap(),
                        expected["decodedListJson"],
                        "{} {} decodeList",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        expected["featureEncodedJson"], expected["instanceEncodedJson"],
                        "{} {} empty writer features",
                        $name, case["id"]
                    );
                    let featured = value
                        .java_encode_with_features(&[
                            JavaJsonWriterFeature::WriteNonStringValueAsString,
                        ])
                        .unwrap();
                    let expected_featured =
                        expected["writerFeatureOverrides"]["WriteNonStringValueAsString"]
                            .as_str()
                            .unwrap_or_else(|| expected["instanceEncodedJson"].as_str().unwrap());
                    assert_eq!(
                        featured,
                        expected_featured.as_bytes(),
                        "{} {} WriteNonStringValueAsString",
                        $name,
                        case["id"]
                    );
                    let boolean_featured = value
                        .java_encode_with_features(&[JavaJsonWriterFeature::WriteBooleanAsNumber])
                        .unwrap();
                    let expected_boolean_featured =
                        expected["writerFeatureOverrides"]["WriteBooleanAsNumber"]
                            .as_str()
                            .unwrap_or_else(|| expected["instanceEncodedJson"].as_str().unwrap());
                    assert_eq!(
                        boolean_featured,
                        expected_boolean_featured.as_bytes(),
                        "{} {} WriteBooleanAsNumber",
                        $name,
                        case["id"]
                    );
                    let combined_featured = value
                        .java_encode_with_features(&[
                            JavaJsonWriterFeature::WriteBooleanAsNumber,
                            JavaJsonWriterFeature::WriteNonStringValueAsString,
                        ])
                        .unwrap();
                    let expected_combined_featured = expected[
                        "writerFeatureOverrides"
                    ]["WriteBooleanAsNumber+WriteNonStringValueAsString"]
                        .as_str()
                        .unwrap_or_else(|| expected["instanceEncodedJson"].as_str().unwrap());
                    assert_eq!(
                        combined_featured,
                        expected_combined_featured.as_bytes(),
                        "{} {} combined boolean writer features",
                        $name,
                        case["id"]
                    );
                    let escaped_featured = value
                        .java_encode_with_features(&[JavaJsonWriterFeature::EscapeNoneAscii])
                        .unwrap();
                    let expected_escaped_featured = expected["writerFeatureOverrides"][
                        "EscapeNoneAscii"
                    ]
                        .as_str()
                        .unwrap_or_else(|| expected["instanceEncodedJson"].as_str().unwrap());
                    assert_eq!(
                        escaped_featured,
                        expected_escaped_featured.as_bytes(),
                        "{} {} EscapeNoneAscii",
                        $name,
                        case["id"]
                    );
                    let pretty_featured = value
                        .java_encode_with_features(&[JavaJsonWriterFeature::PrettyFormat])
                        .unwrap();
                    let expected_pretty_featured =
                        expected["writerFeatureOverrides"]["PrettyFormat"]
                            .as_str()
                            .unwrap_or_else(|| expected["instancePrettyJson"].as_str().unwrap());
                    assert_eq!(
                        pretty_featured,
                        expected_pretty_featured.as_bytes(),
                        "{} {} PrettyFormat",
                        $name,
                        case["id"]
                    );
                    for (feature, feature_name) in [
                        (
                            JavaJsonWriterFeature::NotWriteEmptyArray,
                            "NotWriteEmptyArray",
                        ),
                        (JavaJsonWriterFeature::IgnoreEmpty, "IgnoreEmpty"),
                        (
                            JavaJsonWriterFeature::WriteNonStringKeyAsString,
                            "WriteNonStringKeyAsString",
                        ),
                        (JavaJsonWriterFeature::WriteLongAsString, "WriteLongAsString"),
                        (
                            JavaJsonWriterFeature::WriteEnumUsingOrdinal,
                            "WriteEnumUsingOrdinal",
                        ),
                        (
                            JavaJsonWriterFeature::WriteEnumsUsingName,
                            "WriteEnumsUsingName",
                        ),
                        (
                            JavaJsonWriterFeature::WriteEnumUsingToString,
                            "WriteEnumUsingToString",
                        ),
                        (
                            JavaJsonWriterFeature::IgnoreErrorGetter,
                            "IgnoreErrorGetter",
                        ),
                        (
                            JavaJsonWriterFeature::IgnoreNonFieldGetter,
                            "IgnoreNonFieldGetter",
                        ),
                        (JavaJsonWriterFeature::WriteNulls, "WriteNulls"),
                        (
                            JavaJsonWriterFeature::WriteMapNullValue,
                            "WriteMapNullValue",
                        ),
                        (
                            JavaJsonWriterFeature::WriteNullListAsEmpty,
                            "WriteNullListAsEmpty",
                        ),
                        (JavaJsonWriterFeature::BeanToArray, "BeanToArray"),
                        (JavaJsonWriterFeature::FieldBased, "FieldBased"),
                        (
                            JavaJsonWriterFeature::WriteNullBooleanAsFalse,
                            "WriteNullBooleanAsFalse",
                        ),
                        (JavaJsonWriterFeature::MapSortField, "MapSortField"),
                        (
                            JavaJsonWriterFeature::SortMapEntriesByKeys,
                            "SortMapEntriesByKeys",
                        ),
                        (JavaJsonWriterFeature::LargeObject, "LargeObject"),
                        (
                            JavaJsonWriterFeature::NotWriteDefaultValue,
                            "NotWriteDefaultValue",
                        ),
                        (
                            JavaJsonWriterFeature::PrettyFormatWith2Space,
                            "PrettyFormatWith2Space",
                        ),
                        (
                            JavaJsonWriterFeature::PrettyFormatWith4Space,
                            "PrettyFormatWith4Space",
                        ),
                        (JavaJsonWriterFeature::UseSingleQuotes, "UseSingleQuotes"),
                        (
                            JavaJsonWriterFeature::WriteNameAsSymbol,
                            "WriteNameAsSymbol",
                        ),
                        (
                            JavaJsonWriterFeature::NotWriteRootClassName,
                            "NotWriteRootClassName",
                        ),
                        (
                            JavaJsonWriterFeature::NotWriteHashMapArrayListClassName,
                            "NotWriteHashMapArrayListClassName",
                        ),
                        (
                            JavaJsonWriterFeature::NotWriteNumberClassName,
                            "NotWriteNumberClassName",
                        ),
                        (
                            JavaJsonWriterFeature::NotWriteSetClassName,
                            "NotWriteSetClassName",
                        ),
                        (
                            JavaJsonWriterFeature::WriteByteArrayAsBase64,
                            "WriteByteArrayAsBase64",
                        ),
                        (
                            JavaJsonWriterFeature::WritePairAsJavaBean,
                            "WritePairAsJavaBean",
                        ),
                        (
                            JavaJsonWriterFeature::WriteThrowableClassName,
                            "WriteThrowableClassName",
                        ),
                        (
                            JavaJsonWriterFeature::WriterUtilDateAsMillis,
                            "WriterUtilDateAsMillis",
                        ),
                        (
                            JavaJsonWriterFeature::WriteBigDecimalAsPlain,
                            "WriteBigDecimalAsPlain",
                        ),
                        (
                            JavaJsonWriterFeature::WriteFloatSpecialAsString,
                            "WriteFloatSpecialAsString",
                        ),
                        (JavaJsonWriterFeature::BrowserSecure, "BrowserSecure"),
                        (JavaJsonWriterFeature::UnquoteFieldName, "UnquoteFieldName"),
                        (
                            JavaJsonWriterFeature::IgnoreNoneSerializable,
                            "IgnoreNoneSerializable",
                        ),
                    ] {
                        assert_eq!(
                            value.java_encode_with_features(&[feature]).unwrap(),
                            expected["writerFeatureOverrides"][feature_name]
                                .as_str()
                                .unwrap_or_else(|| expected["instanceEncodedJson"].as_str().unwrap())
                                .as_bytes(),
                            "{} {} {feature_name}",
                            $name,
                            case["id"]
                        );
                    }
                    assert_eq!(
                        value
                            .java_encode_with_features(&[
                                JavaJsonWriterFeature::BrowserSecure,
                                JavaJsonWriterFeature::UnquoteFieldName,
                            ])
                            .unwrap(),
                        expected["writerFeatureOverrides"]["BrowserSecure+UnquoteFieldName"]
                            .as_str()
                            .unwrap()
                            .as_bytes(),
                        "{} {} BrowserSecure+UnquoteFieldName",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        expected["writerFeatureOverrides"]["ErrorOnNoneSerializable"]["error"],
                        "JSONException",
                        "{} {} Java error feature result",
                        $name,
                        case["id"]
                    );
                    assert!(
                        value
                            .java_encode_with_features(&[
                                JavaJsonWriterFeature::ErrorOnNoneSerializable,
                            ])
                            .is_err(),
                        "{} {} ErrorOnNoneSerializable",
                        $name,
                        case["id"]
                    );
                    assert_eq!(
                        value
                            .java_encode_with_features(&[
                                JavaJsonWriterFeature::ErrorOnNoneSerializable,
                                JavaJsonWriterFeature::IgnoreNoneSerializable,
                            ])
                            .unwrap(),
                        b"null",
                        "{} {} IgnoreNoneSerializable takes precedence",
                        $name,
                        case["id"]
                    );
                }

                let nulls = &fixture["remotingSerializableBodyNullUtilities"][$name];
                assert!(<$dto>::java_encode_optional(None).unwrap().is_none());
                assert_eq!(
                    <$dto>::java_to_json_optional(None, false).unwrap(),
                    nulls["staticToJsonNull"]
                );
                assert_eq!(
                    <$dto>::java_decode_nullable(None).unwrap().is_none(),
                    nulls["decodeNullBytesReturnsNull"]
                );
                assert_eq!(
                    <$dto>::java_decode_nullable(Some(b"null"))
                        .unwrap()
                        .is_none(),
                    nulls["decodeJsonNullReturnsNull"]
                );
                assert!(<$dto>::java_from_json_nullable(None).unwrap().is_none());
                assert!(
                    <$dto>::java_from_json_nullable(Some("null"))
                        .unwrap()
                        .is_none()
                );
                assert_eq!(
                    <$dto>::java_decode_list_nullable(None).unwrap().is_none(),
                    nulls["decodeListNullBytesReturnsNull"]
                );
                assert_eq!(
                    <$dto>::java_decode_list_nullable(Some(b"null"))
                        .unwrap()
                        .is_none(),
                    nulls["decodeListJsonNullReturnsNull"]
                );
                let list = <$dto>::java_decode_list_nullable(Some(b"[null,{}]"))
                    .unwrap()
                    .unwrap();
                assert_eq!(list.len(), nulls["decodeListNullMemberCount"]);
                assert_eq!(list[0].is_none(), nulls["decodeListPreservesNullMember"]);
            }};
        }

        assert_body_serialization!(
            LiteSubscriptionCtlRequestBody,
            "LiteSubscriptionCtlRequestBody"
        );
        assert_body_serialization!(QueryAssignmentRequestBody, "QueryAssignmentRequestBody");
        assert_body_serialization!(QueryAssignmentResponseBody, "QueryAssignmentResponseBody");
        assert_body_serialization!(QueryConsumeTimeSpanBody, "QueryConsumeTimeSpanBody");
        assert_body_serialization!(
            SetMessageRequestModeRequestBody,
            "SetMessageRequestModeRequestBody"
        );
        assert_body_serialization!(
            MessageRequestModeSerializeWrapper,
            "MessageRequestModeSerializeWrapper"
        );
        assert_body_serialization!(LockBatchRequestBody, "LockBatchRequestBody");
        assert_body_serialization!(UnlockBatchRequestBody, "UnlockBatchRequestBody");
        assert_body_serialization!(LockBatchResponseBody, "LockBatchResponseBody");
        assert_body_serialization!(
            ConsumerListByGroupResponseBody,
            "GetConsumerListByGroupResponseBody"
        );
        assert_body_serialization!(ConsumeMessageDirectlyResult, "ConsumeMessageDirectlyResult");
        assert_body_serialization!(GetConsumerStatusBody, "GetConsumerStatusBody");
        assert_body_serialization!(ResetOffsetBody, "ResetOffsetBody");

        let queue_api = &fixture["queueTimeSpanApiCase"];
        let default_expected = &queue_api["defaultsByTimeZone"][timezone];
        let default_span = QueueTimeSpan::default();
        assert_eq!(
            default_span.get_min_time_stamp_str(),
            default_expected["minTimeStampStr"]
        );
        assert_eq!(
            default_span.get_max_time_stamp_str(),
            default_expected["maxTimeStampStr"]
        );
        assert_eq!(
            default_span.get_consume_time_stamp_str(),
            default_expected["consumeTimeStampStr"]
        );
        let expected = &queue_api["valuesByTimeZone"][timezone];
        let mut span = QueueTimeSpan::default();
        span.set_message_queue(Some(MessageQueue::new("orders", "broker-a", 1)));
        span.set_min_time_stamp(1_000);
        span.set_max_time_stamp(9_000);
        span.set_consume_time_stamp(5_000);
        span.set_delay_time(4_000);
        assert_eq!(span.get_min_time_stamp_str(), expected["minTimeStampStr"]);
        assert_eq!(span.get_max_time_stamp_str(), expected["maxTimeStampStr"]);
        assert_eq!(
            span.get_consume_time_stamp_str(),
            expected["consumeTimeStampStr"]
        );
        let serialized = serde_json::to_value(&span).unwrap();
        assert_eq!(serialized["minTimeStampStr"], expected["minTimeStampStr"]);
        assert_eq!(serialized["maxTimeStampStr"], expected["maxTimeStampStr"]);
        assert_eq!(
            serialized["consumeTimeStampStr"],
            expected["consumeTimeStampStr"]
        );
        let extreme_expected = &queue_api["extremeValuesByTimeZone"][timezone];
        let mut extremes = QueueTimeSpan::default();
        extremes.set_min_time_stamp(i64::MIN);
        extremes.set_max_time_stamp(i64::MAX);
        extremes.set_consume_time_stamp(i64::MIN);
        assert_eq!(
            extremes.get_min_time_stamp_str(),
            extreme_expected["minTimeStampStr"]
        );
        assert_eq!(
            extremes.get_max_time_stamp_str(),
            extreme_expected["maxTimeStampStr"]
        );
        assert_eq!(
            extremes.get_consume_time_stamp_str(),
            extreme_expected["consumeTimeStampStr"]
        );
        for method in [
            "getMinTimeStampStr",
            "getMaxTimeStampStr",
            "getConsumeTimeStampStr",
        ] {
            assert!(
                queue_api["declaredPublicMethods"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|signature| signature.as_str().unwrap().contains(method))
            );
        }
    }

    #[test]
    fn lock_and_unlock_bodies_preserve_collection_and_boolean_semantics() {
        let lock = LockBatchRequestBody {
            consumer_group: Some("workers".into()),
            client_id: Some("client".into()),
            only_this_broker: false,
            mq_set: Some(SharedValue::new(std::collections::HashSet::from([Some(
                SharedValue::new(MessageQueue::new("orders", "broker-a", 0)),
            )]))),
        };
        let unlock = UnlockBatchRequestBody {
            consumer_group: lock.consumer_group.clone(),
            client_id: lock.client_id.clone(),
            mq_set: lock.mq_set.clone(),
            only_this_broker: true,
        };
        let decoded =
            serde_json::from_value::<LockBatchRequestBody>(serde_json::to_value(&lock).unwrap())
                .unwrap();
        assert_eq!(
            decoded.mq_set.as_ref().map(SharedValue::snapshot),
            lock.mq_set.as_ref().map(SharedValue::snapshot)
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
            populated.consumer_id_list.unwrap().snapshot(),
            vec![Some("client".into())]
        );
    }

    #[test]
    fn lock_request_uses_java_message_queue_field_names() {
        let body = LockBatchRequestBody {
            consumer_group: Some("workers".into()),
            client_id: Some("client".into()),
            mq_set: Some(SharedValue::new(std::collections::HashSet::from([Some(
                SharedValue::new(MessageQueue::new("orders", "broker-a", 0)),
            )]))),
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
            offset_table: Some(SharedValue::new(vec![(queue.clone(), Some(7))])),
        };
        let value = serde_json::to_value(reset).unwrap();
        let offset_table = value["offsetTable"].as_object().unwrap();
        assert_eq!(offset_table.values().next().unwrap(), 7);
        let key: serde_json::Value =
            serde_json::from_str(offset_table.keys().next().unwrap()).unwrap();
        assert_eq!(key["queueId"], 1);

        let status = GetConsumerStatusBody {
            message_queue_table: Some(SharedValue::new(vec![(queue.clone(), Some(7))])),
            consumer_table: Some(SharedValue::new(std::collections::BTreeMap::from([(
                "consumer-a".into(),
                Some(SharedValue::new(vec![(queue, Some(8))])),
            )]))),
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
    #[allow(clippy::mutable_key_type)]
    fn assignment_request_mode_lite_and_batch_ack_match_java_json_shapes() {
        let assignment = MessageQueueAssignment {
            message_queue: Some(SharedValue::new(MessageQueue::new("orders", "broker-a", 1))),
            mode: Some(MessageRequestMode::Pop),
            attachments: Some(SharedValue::new(std::collections::BTreeMap::from([(
                "zone".into(),
                Some("z1".into()),
            )]))),
        };
        let value = serde_json::to_value(assignment).unwrap();
        assert_eq!(value["mode"], "POP");
        assert_eq!(value["messageQueue"]["brokerName"], "broker-a");

        let lite = LiteSubscriptionDto {
            action: Some(LiteSubscriptionAction::CompleteAdd),
            client_id: Some("client".into()),
            group: Some("workers".into()),
            topic: Some("orders".into()),
            lite_topic_set: Some(SharedValue::new(std::collections::BTreeSet::from([Some(
                "lite-orders".into(),
            )]))),
            offset_option: Some(SharedValue::new(OffsetOption {
                option_type: Some(OffsetOptionType::Timestamp),
                value: 99,
            })),
            version: 2,
        };
        let value = serde_json::to_value(lite).unwrap();
        assert_eq!(value["action"], "COMPLETE_ADD");
        assert_eq!(value["offsetOption"]["type"], "TIMESTAMP");

        let body = BatchAckMessageRequestBody {
            broker_name: Some("broker-a".into()),
            acks: Some(SharedValue::new(vec![Some(SharedValue::new(BatchAck {
                consumer_group: Some("workers".into()),
                topic: Some("orders".into()),
                retry: Some("0".into()),
                start_offset: 100,
                queue_id: 1,
                revive_queue_id: 0,
                pop_time: 10,
                invisible_time: 20,
                bit_set: Some(SharedValue::new(BitSet::from_byte_array(&[0b0000_0101]))),
            }))])),
        };
        let value = serde_json::to_value(&body).unwrap();
        let ack = &value["acks"][0];
        assert_eq!(ack["c"], "workers");
        assert_eq!(ack["t"], "orders");
        assert_eq!(ack["b"], "BQ==");
        let round_trip: BatchAckMessageRequestBody = serde_json::from_value(value).unwrap();
        assert_eq!(
            round_trip.acks.unwrap().snapshot()[0]
                .as_ref()
                .unwrap()
                .with(|ack| ack.bit_set.as_ref().unwrap().snapshot()),
            BitSet::from_byte_array(&[0b0000_0101])
        );
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
                    .and_then(|assignments| assignments.snapshot().into_iter().next())
                    .flatten()
                    .unwrap()
                    .snapshot();
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
                    .and_then(|assignments| assignments.snapshot().into_iter().next())
                    .flatten()
                    .unwrap()
                    .snapshot();
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
    fn consumer_list_root_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["consumerListRootCases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
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
    fn consumer_status_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["consumerStatusBodyInputCases"].as_array().unwrap();
        assert_eq!(cases.len(), 143);
        let rows = |entries: Option<&[(MessageQueue, Option<i64>)]>| {
            let Some(entries) = entries else {
                return serde_json::Value::Null;
            };
            let mut rows = entries
                .iter()
                .map(|(queue, offset)| {
                    serde_json::json!({
                        "topic": queue.topic(),
                        "brokerName": queue.broker_name(),
                        "queueId": queue.queue_id(),
                        "offset": offset
                    })
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(serde_json::Value::to_string);
            serde_json::Value::Array(rows)
        };
        let mut mismatches = Vec::new();
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let decoded =
                GetConsumerStatusBody::decode_java_json(case["rawInput"].as_str().unwrap());
            let java_accepts = case["outcome"] == "ACCEPTED";
            match (decoded, java_accepts) {
                (Ok(decoded), true) => {
                    let encoded = String::from_utf8(decoded.encode_java_json().unwrap()).unwrap();
                    if encoded != case["encodedBody"] {
                        mismatches.push(format!(
                            "{name}: encoded body {encoded} != Java {}",
                            case["encodedBody"]
                        ));
                    }
                    let actual_message_queue_table = decoded
                        .message_queue_table
                        .as_ref()
                        .map(|table| table.with(|table| rows(Some(table))))
                        .unwrap_or(serde_json::Value::Null);
                    if actual_message_queue_table != case["messageQueueTable"] {
                        mismatches.push(format!(
                            "{name}: messageQueueTable {} != Java {}",
                            actual_message_queue_table, case["messageQueueTable"]
                        ));
                    }
                    let actual_consumer_table = decoded.consumer_table.as_ref().map(|table| {
                        table.with(|table| {
                            table
                                .iter()
                                .map(|(consumer, entries)| {
                                    (
                                        consumer.clone(),
                                        entries
                                            .as_ref()
                                            .map(|entries| {
                                                entries.with(|entries| rows(Some(entries)))
                                            })
                                            .unwrap_or(serde_json::Value::Null),
                                    )
                                })
                                .collect::<std::collections::BTreeMap<_, _>>()
                        })
                    });
                    if serde_json::to_value(&actual_consumer_table).unwrap()
                        != case["consumerTable"]
                    {
                        mismatches.push(format!(
                            "{name}: consumerTable {} != Java {}",
                            serde_json::to_value(actual_consumer_table).unwrap(),
                            case["consumerTable"]
                        ));
                    }
                }
                (Ok(_), false) => mismatches.push(format!("{name}: Rust accepted, Java rejected")),
                (Err(error), true) => {
                    mismatches.push(format!("{name}: Rust rejected ({error}), Java accepted"))
                }
                (Err(_), false) => {}
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[test]
    fn consumer_status_body_accessors_keep_java_null_and_empty_states() {
        let mut body = GetConsumerStatusBody::default();
        assert!(body.get_message_queue_table().unwrap().with(Vec::is_empty));
        assert!(
            body.get_consumer_table()
                .unwrap()
                .with(std::collections::BTreeMap::is_empty)
        );

        let queue = MessageQueue::new("orders", "broker-a", 1);
        let message_queue_table = SharedValue::new(vec![(queue.clone(), Some(9))]);
        let consumer_queue_table = SharedValue::new(vec![(queue.clone(), Some(8))]);
        let consumer_table = SharedValue::new(std::collections::BTreeMap::from([(
            "consumer-a".to_owned(),
            Some(consumer_queue_table.clone()),
        )]));
        body.set_message_queue_table(Some(message_queue_table.clone()));
        body.set_consumer_table(Some(consumer_table.clone()));
        assert!(
            body.get_message_queue_table()
                .unwrap()
                .ptr_eq(&message_queue_table)
        );
        assert!(body.get_consumer_table().unwrap().ptr_eq(&consumer_table));
        assert!(consumer_table.with(|table| {
            table["consumer-a"]
                .as_ref()
                .unwrap()
                .ptr_eq(&consumer_queue_table)
        }));
        assert_eq!(
            body.get_message_queue_table().unwrap().snapshot(),
            vec![(queue.clone(), Some(9))]
        );
        assert_eq!(
            body.get_consumer_table()
                .unwrap()
                .with(|table| table["consumer-a"].as_ref().unwrap().snapshot()),
            vec![(queue, Some(8))]
        );

        let queue = MessageQueue::new("orders", "broker-a", 1);
        body.set_message_queue_table(Some(SharedValue::new(vec![(queue.clone(), None)])));
        body.set_consumer_table(Some(SharedValue::new(std::collections::BTreeMap::from([
            (
                "consumer-a".to_owned(),
                Some(SharedValue::new(vec![(queue.clone(), None)])),
            ),
        ]))));
        assert_eq!(
            body.get_message_queue_table().unwrap().snapshot(),
            vec![(queue.clone(), None)]
        );
        assert_eq!(
            body.get_consumer_table()
                .unwrap()
                .with(|table| table["consumer-a"].as_ref().unwrap().snapshot()),
            vec![(queue, None)]
        );

        body.set_message_queue_table(None);
        body.set_consumer_table(None);
        assert!(body.get_message_queue_table().is_none());
        assert!(body.get_consumer_table().is_none());
    }

    #[test]
    fn reset_offset_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["resetOffsetBodyInputCases"].as_array().unwrap();
        assert_eq!(cases.len(), 77);
        let rows = |entries: Option<&[(MessageQueue, Option<i64>)]>| {
            let Some(entries) = entries else {
                return serde_json::Value::Null;
            };
            let mut rows = entries
                .iter()
                .map(|(queue, offset)| {
                    serde_json::json!({
                        "topic": queue.topic(),
                        "brokerName": queue.broker_name(),
                        "queueId": queue.queue_id(),
                        "offset": offset
                    })
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(serde_json::Value::to_string);
            serde_json::Value::Array(rows)
        };
        let mut mismatches = Vec::new();
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let decoded = ResetOffsetBody::decode_java_json(case["rawInput"].as_str().unwrap());
            let java_accepts = case["outcome"] == "ACCEPTED";
            match (decoded, java_accepts) {
                (Ok(decoded), true) => {
                    let actual_offset_table = decoded
                        .offset_table
                        .as_ref()
                        .map(|table| table.with(|table| rows(Some(table))))
                        .unwrap_or(serde_json::Value::Null);
                    if actual_offset_table != case["offsetTable"] {
                        mismatches.push(format!(
                            "{name}: offsetTable {} != Java {}",
                            actual_offset_table, case["offsetTable"]
                        ));
                    }
                    let encoded = String::from_utf8(decoded.encode_java_json().unwrap()).unwrap();
                    if encoded != case["encodedBody"] {
                        mismatches.push(format!(
                            "{name}: encoded body {encoded} != Java {}",
                            case["encodedBody"]
                        ));
                    }
                }
                (Ok(_), false) => mismatches.push(format!("{name}: Rust accepted, Java rejected")),
                (Err(error), true) => {
                    mismatches.push(format!("{name}: Rust rejected ({error}), Java accepted"))
                }
                (Err(_), false) => {}
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
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
        assert_eq!(cases.len(), 30);
        for required in [
            "fraction-field",
            "string-element",
            "number-element",
            "boolean-element",
            "array-element",
            "nested-array-object-element",
            "mixed-null-object-elements",
            "mixed-scalar-object-elements",
            "message-queue-null",
            "message-queue-string",
            "message-queue-number",
            "message-queue-boolean",
            "message-queue-array",
            "message-queue-empty-object",
            "message-queue-object",
        ] {
            assert!(
                cases.iter().any(|case| case["name"] == required),
                "missing {required}"
            );
        }
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
                        .snapshot()
                        .iter()
                        .map(|item| match item {
                            Some(item) => {
                                let item = item.snapshot();
                                serde_json::json!({
                                    "attachments": item.attachments.as_ref().map(SharedValue::snapshot),
                                    "messageQueue": item.message_queue.as_ref().map(|queue| queue.with(|queue| {
                                        serde_json::json!({
                                            "topic": queue.topic(),
                                            "brokerName": queue.broker_name(),
                                            "queueId": queue.queue_id(),
                                        })
                                    })),
                                    "mode": item.mode,
                                })
                            },
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
    fn assignment_body_root_shapes_match_invoked_java_fastjson() {
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
        let cases = fixture["assignmentBodyRootCases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
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
                        .snapshot()
                        .iter()
                        .map(|item| match item {
                            Some(item) => {
                                let item = item.snapshot();
                                serde_json::json!({
                                    "attachments": item.attachments.as_ref().map(SharedValue::snapshot),
                                    "messageQueue": item.message_queue.as_ref().map(|queue| queue.with(|queue| {
                                        serde_json::json!({
                                            "topic": queue.topic(),
                                            "brokerName": queue.broker_name(),
                                            "queueId": queue.queue_id(),
                                        })
                                    })),
                                    "mode": item.mode,
                                })
                            },
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
    fn message_request_mode_wrapper_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["messageRequestModeWrapperCases"]
            .as_array()
            .unwrap();
        batch_body_cases_match_java::<MessageRequestModeSerializeWrapper>(cases, 33);
        let default_encoded =
            serde_json::to_value(MessageRequestModeSerializeWrapper::default()).unwrap();
        assert_eq!(
            default_encoded,
            cases
                .iter()
                .find(|case| case["name"] == "default-empty-map")
                .unwrap()["encoded"],
            "Rust Default should match Java's no-argument wrapper values"
        );

        let api = &fixture["messageRequestModeWrapperApiCase"];
        let mut wrapper = MessageRequestModeSerializeWrapper::default();
        assert_eq!(api["equalsDeclaringClass"], "java.lang.Object");
        assert_eq!(api["hashCodeDeclaringClass"], "java.lang.Object");
        assert_eq!(api["sameInstanceEquals"], true);
        assert_eq!(api["equalValuesDistinctInstanceEquals"], false);
        assert_eq!(api["defaultMapNonNull"], true);
        assert_eq!(api["defaultMapSize"], 0);
        assert_eq!(
            wrapper
                .get_message_request_mode_map()
                .unwrap()
                .with(std::collections::BTreeMap::len),
            0
        );
        wrapper.set_message_request_mode_map(None);
        assert_eq!(api["nullSetterRemainsNull"], true);
        assert!(wrapper.get_message_request_mode_map().is_none());
        let assigned_body = SharedValue::new(SetMessageRequestModeRequestBody {
            topic: Some("orders".into()),
            consumer_group: Some("workers".into()),
            mode: Some(MessageRequestMode::Pop),
            pop_share_queue_num: 2,
        });
        let assigned_group = SharedValue::new(std::collections::BTreeMap::from([(
            "workers".into(),
            assigned_body.clone(),
        )]));
        let assigned_map = SharedValue::new(std::collections::BTreeMap::from([(
            "orders".into(),
            assigned_group.clone(),
        )]));
        wrapper.set_message_request_mode_map(Some(assigned_map.clone()));
        let getter_map = wrapper.get_message_request_mode_map().unwrap();
        let getter_group = getter_map.with(|map| map.get("orders").unwrap().clone());
        let getter_body = getter_group.with(|groups| groups.get("workers").unwrap().clone());
        assert!(assigned_map.ptr_eq(&getter_map));
        assert!(assigned_group.ptr_eq(&getter_group));
        assert!(assigned_body.ptr_eq(&getter_body));
        assert_eq!(api["setterRetainsMapIdentity"], true);
        assert_eq!(api["setterRetainsGroupMapIdentity"], true);
        assert_eq!(api["getterReturnsAssignedBody"], true);

        assigned_map.with_mut(|map| {
            map.insert(
                "caller-topic".into(),
                SharedValue::new(std::collections::BTreeMap::new()),
            );
        });
        assert!(getter_map.with(|map| map.contains_key("caller-topic")));
        assigned_map.with_mut(|map| {
            map.remove("caller-topic");
        });
        assigned_group.with_mut(|groups| {
            groups.insert("caller-group".into(), assigned_body.clone());
        });
        assert!(getter_group.with(|groups| groups.contains_key("caller-group")));
        assigned_group.with_mut(|groups| {
            groups.remove("caller-group");
        });
        assigned_body.with_mut(|body| body.set_mode(Some(MessageRequestMode::Pull)));
        assert_eq!(
            getter_body.with(SetMessageRequestModeRequestBody::get_mode),
            Some(MessageRequestMode::Pull)
        );
        assigned_body.with_mut(|body| body.set_mode(Some(MessageRequestMode::Pop)));
        getter_body.with_mut(|body| body.set_mode(Some(MessageRequestMode::Pull)));
        assert_eq!(
            assigned_body.with(SetMessageRequestModeRequestBody::get_mode),
            Some(MessageRequestMode::Pull)
        );
        getter_body.with_mut(|body| body.set_mode(Some(MessageRequestMode::Pop)));
        assert_eq!(api["callerOuterMutationVisible"], true);
        assert_eq!(api["callerGroupMutationVisible"], true);
        assert_eq!(api["callerBodyMutationVisible"], true);
        assert_eq!(api["getterBodyMutationVisibleToSetterObject"], true);
        assert_eq!(
            serde_json::to_value(&wrapper).unwrap()["messageRequestModeMap"],
            api["populatedMap"]
        );
        assert_eq!(api["getterMutationVisible"], true);
        let map = wrapper.get_message_request_mode_map_mut().unwrap();
        map.with_mut(|map| {
            map.entry("archive".into())
                .or_insert_with(|| SharedValue::new(std::collections::BTreeMap::new()));
            let readers = map
                .get("orders")
                .unwrap()
                .with(|groups| groups.get("workers").unwrap().clone());
            map.get("orders")
                .unwrap()
                .with_mut(|groups| groups.insert("readers".into(), readers));
        });
        assert_eq!(
            serde_json::to_value(wrapper).unwrap()["messageRequestModeMap"],
            api["getterMutatedMap"]
        );
    }

    #[test]
    fn lite_subscription_enum_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionEnumCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            33,
            "Java oracle includes lite enum values, duplicate fields, and list shapes"
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
            "subscription-set-null",
            "subscription-set-empty",
            "subscription-set-single-object",
            "subscription-set-null-element",
            "subscription-set-string-element",
            "subscription-set-scalar-number",
        ] {
            assert!(
                cases.iter().any(|case| case["name"] == required),
                "missing {required}"
            );
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
    fn lite_subscription_field_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionFieldCases"].as_array().unwrap();
        assert_eq!(cases.len(), 49);
        for case in cases {
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(
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
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn lite_subscription_identity_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionIdentityCases"].as_array().unwrap();
        assert_eq!(cases.len(), 3);
        for (case, expected_class) in cases.iter().zip([
            "LiteSubscriptionDTO",
            "LiteSubscription",
            "LiteSubscriptionCtlRequestBody",
        ]) {
            assert_eq!(case["class"], expected_class);
            assert_eq!(case["sameObjectEquals"], true, "{}", case["class"]);
            assert_eq!(
                case["equalFieldDistinctObjectsEquals"], false,
                "{}",
                case["class"]
            );
            assert_eq!(case["equalFieldHashSetSize"], 2, "{}", case["class"]);
        }
    }

    #[test]
    fn lite_subscription_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let case = &fixture["liteSubscriptionApiCases"][0];
        let mut subscription = LiteSubscription::new();
        let subscription_ptr = std::ptr::addr_of_mut!(subscription);
        assert_eq!(
            subscription.set_group(Some("workers".into())) as *mut LiteSubscription,
            subscription_ptr
        );
        assert_eq!(
            subscription.set_topic(Some("orders".into())) as *mut LiteSubscription,
            subscription_ptr
        );
        assert_eq!(subscription.group(), Some("workers"));
        assert_eq!(subscription.topic(), Some("orders"));
        subscription.set_update_time(case["initialTime"].as_i64().unwrap());

        let mut setter_input = std::collections::BTreeSet::from(["lite-a".to_owned()]);
        assert_eq!(
            subscription.set_lite_topic_set(&setter_input) as *mut LiteSubscription,
            subscription_ptr
        );
        assert_eq!(subscription.update_time(), 7);
        setter_input.insert("setter-only".into());
        assert!(
            subscription
                .lite_topic_set()
                .with(|topics| topics.contains("lite-a"))
        );
        assert!(
            !subscription
                .lite_topic_set()
                .with(|topics| topics.contains("setter-only"))
        );
        let live_topics = subscription.lite_topic_set();
        assert!(live_topics.ptr_eq(&subscription.lite_topic_set()));
        assert!(subscription.add_lite_topic("lite-b"));
        assert!(!subscription.add_lite_topic("lite-b"));
        assert!(subscription.update_time() > 7);
        subscription.add_lite_topics(["lite-c", "lite-d"]);
        assert!(subscription.update_time() > 7);
        assert!(live_topics.with(|topics| topics.contains("lite-c")));
        assert!(subscription.remove_lite_topic("lite-b"));
        subscription.remove_lite_topics(["lite-c", "lite-d"]);
        live_topics.with_mut(|topics| {
            topics.insert("getter-added".into());
        });
        assert!(
            subscription
                .lite_topic_set()
                .with(|topics| topics.contains("getter-added"))
        );
        live_topics.with_mut(|topics| {
            topics.remove("getter-added");
        });

        let before_set_lite_topic_set = subscription.update_time();
        subscription.set_lite_topic_set(["lite-e"]);
        assert!(live_topics.with(|topics| topics.contains("lite-a")));
        assert!(live_topics.with(|topics| topics.contains("lite-e")));
        assert_eq!(subscription.update_time(), before_set_lite_topic_set);
        subscription.remove_lite_topic("lite-e");
        subscription.set_update_time(7);

        assert!(case["groupSetterReturnsThis"].as_bool().unwrap());
        assert!(case["topicSetterReturnsThis"].as_bool().unwrap());
        assert!(case["setLiteTopicSetReturnsThis"].as_bool().unwrap());
        assert!(!case["setLiteTopicSetRetainsInputSet"].as_bool().unwrap());
        assert!(!case["setterInputMutationVisible"].as_bool().unwrap());
        assert!(case["setLiteTopicSetKeepsExisting"].as_bool().unwrap());
        assert!(case["setLiteTopicSetLeavesUpdateTime"].as_bool().unwrap());
        assert!(case["getterReturnsSameSetObject"].as_bool().unwrap());
        assert!(
            case["getterMutationVisibleToSubscription"]
                .as_bool()
                .unwrap()
        );
        assert!(case["singleAddResult"].as_bool().unwrap());
        assert!(!case["duplicateAddResult"].as_bool().unwrap());
        assert!(case["singleAddUpdatesTime"].as_bool().unwrap());
        assert!(case["collectionAddUpdatesTime"].as_bool().unwrap());
        assert!(case["getterReturnsLiveSet"].as_bool().unwrap());
        assert!(case["singleRemoveResult"].as_bool().unwrap());
        assert!(case["collectionRemoveClearsMembers"].as_bool().unwrap());
        assert!(
            case["setLiteTopicSetAddsWithoutClearing"]
                .as_bool()
                .unwrap()
        );
        assert!(case["setLiteTopicSetDoesNotUpdateTime"].as_bool().unwrap());
        assert_eq!(subscription.to_string(), case["toString"].as_str().unwrap());

        let encoded = serde_json::to_value(&subscription).unwrap();
        assert_eq!(encoded["group"], "workers");
        assert_eq!(encoded["topic"], "orders");
        assert_eq!(encoded["liteTopicSet"], serde_json::json!(["lite-a"]));
        assert_eq!(encoded["updateTime"], 7);
        assert!(encoded.get("clientId").is_none());
    }

    #[test]
    fn lite_subscription_fastjson_inputs_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionInputCases"].as_array().unwrap();
        assert_eq!(cases.len(), 16);
        for case in cases {
            let decoded =
                serde_json::from_str::<LiteSubscription>(case["rawInput"].as_str().unwrap());
            if case["outcome"] == "REJECTED" {
                assert!(decoded.is_err(), "{}: {decoded:?}", case["name"]);
                continue;
            }

            let decoded = decoded.unwrap_or_else(|error| {
                panic!("{}: {error}", case["name"]);
            });
            assert_eq!(
                serde_json::to_value(decoded.group).unwrap(),
                case["group"],
                "{}",
                case["name"]
            );
            assert_eq!(
                serde_json::to_value(decoded.topic).unwrap(),
                case["topic"],
                "{}",
                case["name"]
            );
            let lite_topics = decoded
                .lite_topic_set
                .snapshot()
                .into_iter()
                .collect::<Vec<_>>();
            assert_eq!(
                serde_json::to_value(lite_topics).unwrap(),
                case["liteTopicSet"],
                "{}",
                case["name"]
            );
            if let Some(update_time) = case["updateTime"].as_i64() {
                assert_eq!(decoded.update_time, update_time, "{}", case["name"]);
            }
            if case["updateTimeWasInitialized"] == true {
                assert!(decoded.update_time > 0, "{}", case["name"]);
            }
        }
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn lite_subscription_related_dto_apis_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();

        let dto_case = &fixture["liteSubscriptionDtoApiCase"];
        let mut dto = LiteSubscriptionDto::default();
        let dto_ptr = std::ptr::addr_of_mut!(dto);
        assert_eq!(
            dto.set_action(Some(LiteSubscriptionAction::CompleteAdd)) as *mut LiteSubscriptionDto,
            dto_ptr
        );
        assert_eq!(
            dto.set_client_id(Some("client".into())) as *mut LiteSubscriptionDto,
            dto_ptr
        );
        assert_eq!(
            dto.set_group(Some("workers".into())) as *mut LiteSubscriptionDto,
            dto_ptr
        );
        assert_eq!(
            dto.set_topic(Some("orders".into())) as *mut LiteSubscriptionDto,
            dto_ptr
        );
        let lite_topics =
            SharedValue::new(std::collections::BTreeSet::from([Some("lite-a".into())]));
        assert_eq!(
            dto.set_lite_topic_set(Some(lite_topics.clone())) as *mut LiteSubscriptionDto,
            dto_ptr
        );
        let assigned_option =
            SharedValue::new(OffsetOption::new(Some(OffsetOptionType::Policy), 11));
        dto.set_offset_option(Some(assigned_option.clone()));
        assert_eq!(dto.set_version(9) as *mut LiteSubscriptionDto, dto_ptr);
        assert_eq!(dto.action(), Some(LiteSubscriptionAction::CompleteAdd));
        assert_eq!(dto.client_id(), Some("client"));
        assert_eq!(dto.group(), Some("workers"));
        assert_eq!(dto.topic(), Some("orders"));
        let getter_topics = dto.lite_topic_set().unwrap();
        let getter_option = dto.offset_option().unwrap();
        assert!(lite_topics.ptr_eq(&getter_topics));
        assert!(assigned_option.ptr_eq(&getter_option));
        assert_eq!(dto.version(), 9);
        assert!(dto_case["actionSetterReturnsThis"].as_bool().unwrap());
        assert!(dto_case["clientIdSetterReturnsThis"].as_bool().unwrap());
        assert!(dto_case["groupSetterReturnsThis"].as_bool().unwrap());
        assert!(dto_case["topicSetterReturnsThis"].as_bool().unwrap());
        assert!(dto_case["liteTopicSetSetterReturnsThis"].as_bool().unwrap());
        assert!(
            dto_case["liteTopicSetGetterReturnsSameSet"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(dto_case["liteTopicSetExternalMutationVisible"], true);
        assert_eq!(
            dto_case["liteTopicSetGetterMutationVisibleToAssignedSet"],
            true
        );
        lite_topics.with_mut(|topics| {
            topics.insert(Some("setter-added".into()));
        });
        getter_topics.with_mut(|topics| {
            topics.insert(Some("getter-added".into()));
        });
        assert_eq!(
            serde_json::to_value(getter_topics.snapshot()).unwrap(),
            dto_case["mutatedLiteTopicSet"]
        );
        assert!(
            dto_case["offsetOptionGetterReturnsSameObject"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(dto_case["offsetOptionExternalMutationVisible"], true);
        assigned_option.with_mut(|option| option.set_value(12));
        assert_eq!(getter_option.with(|option| option.value()), 12);
        assert_eq!(
            dto_case["offsetOptionGetterMutationVisibleToAssignedObject"],
            true
        );
        getter_option.with_mut(|option| option.set_value(13));
        assert_eq!(
            getter_option.with(|option| option.value()),
            dto_case["mutatedOffsetOptionValue"].as_i64().unwrap()
        );
        assert_eq!(
            assigned_option.with(|option| option.value()),
            dto_case["mutatedOffsetOptionValue"].as_i64().unwrap()
        );
        assert_eq!(dto.to_string(), dto_case["toString"].as_str().unwrap());
        assert!(dto_case["versionSetterReturnsThis"].as_bool().unwrap());
        assert_eq!(dto_case["action"], "COMPLETE_ADD");
        assert_eq!(dto_case["clientId"], "client");
        assert_eq!(dto_case["group"], "workers");
        assert_eq!(dto_case["topic"], "orders");
        assert_eq!(dto_case["liteTopicSet"], serde_json::json!(["lite-a"]));
        assert_eq!(dto_case["offsetOptionType"], "POLICY");
        assert_eq!(dto_case["offsetOptionValue"], 11);
        assert_eq!(dto_case["version"], 9);

        let mut option = OffsetOption::default();
        option.set_option_type(Some(OffsetOptionType::TailN));
        option.set_value(23);
        let option_case = &fixture["offsetOptionApiCase"];
        assert_eq!(option.option_type(), Some(OffsetOptionType::TailN));
        assert_eq!(option.value(), 23);
        assert_eq!(
            option.to_string(),
            option_case["toString"].as_str().unwrap()
        );
        assert_eq!(option_case["defaultType"], serde_json::Value::Null);
        assert_eq!(option_case["defaultValue"], 0);
        assert_eq!(
            option_case["policyLastValue"],
            OffsetOption::POLICY_LAST_VALUE
        );
        assert_eq!(
            option_case["policyMinValue"],
            OffsetOption::POLICY_MIN_VALUE
        );
        assert_eq!(
            option_case["policyMaxValue"],
            OffsetOption::POLICY_MAX_VALUE
        );
        assert_eq!(option_case["type"], "TAIL_N");
        assert_eq!(option_case["value"], 23);
        assert!(option_case["equalValue"].as_bool().unwrap());
        assert!(!option_case["differentValue"].as_bool().unwrap());
        assert!(!option_case["differentType"].as_bool().unwrap());
        assert!(option_case["nullEqualsFalse"].as_bool().unwrap());
        assert!(option_case["wrongTypeEqualsFalse"].as_bool().unwrap());
        assert!(option_case["hashCodeStableOnSameObject"].as_bool().unwrap());
        assert!(
            option_case["equalInstancesHaveSameHashCode"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(option, OffsetOption::new(Some(OffsetOptionType::TailN), 23));
        assert!(option.java_equals(Some(&option as &dyn std::any::Any)));
        assert!(!option.java_equals(None));
        let wrong_type = "not an OffsetOption";
        assert!(!option.java_equals(Some(&wrong_type as &dyn std::any::Any)));
        let equal_option = OffsetOption::new(Some(OffsetOptionType::TailN), 23);
        assert!(option.java_equals(Some(&equal_option as &dyn std::any::Any)));
        let different_type = OffsetOption::new(Some(OffsetOptionType::Policy), 23);
        assert!(!option.java_equals(Some(&different_type as &dyn std::any::Any)));
        let equal_options = std::collections::HashSet::from([option.clone(), equal_option]);
        assert_eq!(
            equal_options.len(),
            option_case["equalHashSetSize"].as_u64().unwrap() as usize
        );

        let control_case = &fixture["liteSubscriptionControlApiCase"];
        let mut control = LiteSubscriptionCtlRequestBody::default();
        assert!(control.subscription_set().is_none());
        let worker = SharedValue::new(dto);
        let assigned_subscriptions =
            SharedValue::new(std::collections::HashSet::from([Some(worker.clone())]));
        let repeated_reference_set =
            std::collections::HashSet::from([Some(worker.clone()), Some(worker.clone())]);
        assert_eq!(repeated_reference_set.len(), 1);
        let equal_content_distinct = SharedValue::new(worker.snapshot());
        let identity_distinct_set =
            std::collections::HashSet::from([Some(worker.clone()), Some(equal_content_distinct)]);
        assert_eq!(identity_distinct_set.len(), 2);
        control.set_subscription_set(Some(assigned_subscriptions.clone()));
        let getter_subscriptions = control.subscription_set().unwrap();
        assert!(assigned_subscriptions.ptr_eq(&getter_subscriptions));
        assert_eq!(getter_subscriptions.with(std::collections::HashSet::len), 1);
        assert!(
            getter_subscriptions
                .with(|subscriptions| subscriptions.contains(&Some(worker.clone())))
        );
        assert!(
            control_case["defaultSubscriptionSetIsNull"]
                .as_bool()
                .unwrap()
        );
        assert!(control_case["setterStoresSameSet"].as_bool().unwrap());
        assert!(control_case["setterElementIdentity"].as_bool().unwrap());
        assert_eq!(control_case["size"], 1);
        assert_eq!(control_case["externalMutationVisible"], true);
        assert_eq!(control_case["getterMutationVisibleToAssignedSet"], true);
        assert!(control_case["externalElementIdentity"].as_bool().unwrap());
        assert!(control_case["getterElementIdentity"].as_bool().unwrap());
        let consumer = SharedValue::new(LiteSubscriptionDto::default());
        assigned_subscriptions.with_mut(|subscriptions| {
            subscriptions.insert(Some(consumer.clone()));
        });
        assert!(
            getter_subscriptions
                .with(|subscriptions| subscriptions.contains(&Some(consumer.clone())))
        );
        let admin = SharedValue::new(LiteSubscriptionDto::default());
        getter_subscriptions.with_mut(|subscriptions| {
            subscriptions.insert(Some(admin.clone()));
        });
        assert!(
            assigned_subscriptions
                .with(|subscriptions| subscriptions.contains(&Some(admin.clone())))
        );
        assert_eq!(
            getter_subscriptions.with(std::collections::HashSet::len),
            control_case["mutatedSize"].as_u64().unwrap() as usize
        );
    }

    #[test]
    fn lite_subscription_root_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionRootCases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(
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
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn lite_subscription_control_field_shapes_match_invoked_java_fastjson() {
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
        let cases = fixture["liteSubscriptionControlFieldCases"]
            .as_array()
            .unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(
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
                let mut actual = serde_json::to_value(decoded).unwrap();
                let mut expected = case["encoded"].clone();
                normalize(&mut actual);
                normalize(&mut expected);
                assert_eq!(actual, expected, "{}", case["name"]);
            }
        }
    }

    #[test]
    fn lite_subscription_set_element_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionElementCases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(
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
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn lite_subscription_offset_value_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["liteSubscriptionOffsetValueCases"]
            .as_array()
            .unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(
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
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn lite_subscription_set_duplicates_match_invoked_java_fastjson() {
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
        let cases = fixture["liteSubscriptionSetCases"].as_array().unwrap();
        assert_eq!(cases.len(), 4);
        for case in cases {
            let decoded = serde_json::from_str::<LiteSubscriptionCtlRequestBody>(
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
                let mut actual = serde_json::to_value(decoded).unwrap();
                let mut expected = case["encoded"].clone();
                normalize(&mut actual);
                normalize(&mut expected);
                assert_eq!(actual, expected, "{}", case["name"]);
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
    fn query_assignment_request_field_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["queryAssignmentFieldCases"].as_array().unwrap();
        assert_eq!(cases.len(), 35);
        for case in cases {
            let decoded = serde_json::from_str::<QueryAssignmentRequestBody>(
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
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn query_assignment_request_root_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["queryAssignmentRootCases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<Option<QueryAssignmentRequestBody>>(
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
    fn set_request_mode_field_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["setRequestModeFieldCases"].as_array().unwrap();
        assert_eq!(cases.len(), 28);
        for case in cases {
            let decoded = serde_json::from_str::<SetMessageRequestModeRequestBody>(
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
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn set_request_mode_root_shapes_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["setRequestModeRootCases"].as_array().unwrap();
        assert_eq!(cases.len(), 7);
        for case in cases {
            let decoded = serde_json::from_str::<Option<SetMessageRequestModeRequestBody>>(
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
                    serde_json::to_value(decoded).unwrap(),
                    case["encoded"],
                    "{}",
                    case["name"]
                );
            }
        }
    }

    #[test]
    fn consume_direct_result_body_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["consumeDirectResultCases"].as_array().unwrap();
        batch_body_cases_match_java::<ConsumeMessageDirectlyResult>(cases, 66);
        let default_encoded =
            serde_json::to_value(ConsumeMessageDirectlyResult::default()).unwrap();
        assert_eq!(
            default_encoded,
            cases
                .iter()
                .find(|case| case["name"] == "root-empty-object")
                .unwrap()["encoded"],
            "Rust Default should match Java's no-argument DTO values"
        );
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
            let queue_alias =
                serde_json::from_value::<Option<MessageQueue>>(case["messageQueue"].clone())
                    .unwrap()
                    .map(SharedValue::new);
            let caller_queue_alias = queue_alias.clone();
            assignment.set_message_queue(queue_alias);
            assignment.set_mode(
                serde_json::from_value::<Option<MessageRequestMode>>(case["mode"].clone()).unwrap(),
            );
            let attachment_alias = serde_json::from_value::<
                Option<std::collections::BTreeMap<String, Option<String>>>,
            >(case["attachments"].clone())
            .unwrap()
            .map(SharedValue::new);
            let caller_attachment_alias = attachment_alias.clone();
            assignment.set_attachments(attachment_alias);

            assert_eq!(
                assignment.message_queue().map(|queue| queue.snapshot()),
                serde_json::from_value::<Option<MessageQueue>>(case["messageQueue"].clone())
                    .unwrap(),
                "{}",
                case["name"]
            );
            assert_eq!(assignment.mode(), assignment.mode, "{}", case["name"]);
            assert_eq!(
                assignment.attachments().map(|attachments| attachments.snapshot()),
                serde_json::from_value::<Option<
                    std::collections::BTreeMap<String, Option<String>>,
                >>(case["attachments"].clone())
                .unwrap(),
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
                different_queue.set_message_queue(Some(SharedValue::new(MessageQueue::new(
                    "orders", "broker", 3,
                ))));
                assert_eq!(
                    assignment.java_equals(Some(&different_queue)),
                    Ok(case["equalsDifferentQueue"].as_bool().unwrap())
                );
                if case["name"] == "populated" {
                    assert_eq!(case["attachmentsGetterReturnsSetterMap"], true);
                    let caller_attachments = caller_attachment_alias.as_ref().unwrap();
                    let getter_attachments = assignment.attachments().unwrap();
                    assert!(caller_attachments.ptr_eq(&getter_attachments));
                    assert_eq!(case["attachmentsSetterMutationVisibleToGetter"], true);
                    caller_attachments.with_mut(|attachments| {
                        attachments.insert("caller".into(), Some("visible".into()));
                    });
                    assert_eq!(
                        getter_attachments.with(|attachments| attachments.get("caller").cloned()),
                        Some(Some("visible".into()))
                    );
                    getter_attachments.with_mut(|attachments| {
                        attachments.insert("getter".into(), Some("visible".into()));
                    });
                    assert_eq!(case["attachmentsGetterMutationVisibleToSetterMap"], true);
                    assert_eq!(
                        serde_json::to_value(getter_attachments.snapshot()).unwrap(),
                        case["mutatedAttachments"]
                    );
                    assert_eq!(case["messageQueueGetterReturnsSetterObject"], true);
                    let caller_queue = caller_queue_alias.as_ref().unwrap();
                    let getter_queue = assignment.message_queue().unwrap();
                    assert!(caller_queue.ptr_eq(&getter_queue));
                    caller_queue.with_mut(|queue| queue.set_queue_id(3));
                    assert_eq!(getter_queue.with(|queue| queue.queue_id()), 3);
                    assert_eq!(case["messageQueueSetterMutationVisibleToGetter"], true);
                    caller_queue.with_mut(|queue| queue.set_queue_id(2));
                    getter_queue.with_mut(|queue| queue.set_queue_id(4));
                    assert_eq!(
                        case["messageQueueGetterMutationVisibleToSetterObject"],
                        caller_queue.with(|queue| queue.queue_id() == 4)
                    );
                    assert_eq!(
                        serde_json::to_value(getter_queue.snapshot()).unwrap(),
                        case["mutatedMessageQueue"]
                    );
                }
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
    #[allow(clippy::mutable_key_type)]
    fn lite_subscription_control_and_wrapper_preserve_nested_wire_shape() {
        let lite = LiteSubscriptionDto {
            action: Some(LiteSubscriptionAction::CompleteAdd),
            client_id: Some("client".into()),
            group: Some("workers".into()),
            topic: Some("orders".into()),
            lite_topic_set: Some(SharedValue::new(std::collections::BTreeSet::from([Some(
                "shard-a".into(),
            )]))),
            offset_option: Some(SharedValue::new(OffsetOption {
                option_type: Some(OffsetOptionType::Policy),
                value: 1,
            })),
            version: 99,
        };
        let ctl = LiteSubscriptionCtlRequestBody {
            subscription_set: Some(SharedValue::new(std::collections::HashSet::from([Some(
                SharedValue::new(lite),
            )]))),
        };
        let value = serde_json::to_value(&ctl).unwrap();
        assert_eq!(value["subscriptionSet"][0]["action"], "COMPLETE_ADD");
        assert_eq!(
            value["subscriptionSet"][0]["offsetOption"]["type"],
            "POLICY"
        );
        let round_tripped =
            serde_json::from_value::<LiteSubscriptionCtlRequestBody>(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(round_tripped).unwrap(), value);

        let wrapper = MessageRequestModeSerializeWrapper {
            message_request_mode_map: Some(SharedValue::new(std::collections::BTreeMap::from([(
                "orders".into(),
                SharedValue::new(std::collections::BTreeMap::from([(
                    "workers".into(),
                    SharedValue::new(SetMessageRequestModeRequestBody {
                        topic: Some("orders".into()),
                        consumer_group: Some("workers".into()),
                        mode: Some(MessageRequestMode::Pop),
                        pop_share_queue_num: 2,
                    }),
                )])),
            )]))),
        };
        let value = serde_json::to_value(wrapper).unwrap();
        assert_eq!(
            value["messageRequestModeMap"]["orders"]["workers"]["mode"],
            "POP"
        );
    }
}
