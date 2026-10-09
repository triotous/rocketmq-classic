use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::marker::PhantomData;

use crate::codes::JavaEnumLookupError;
use rocketmq_classic_common::serde_fastjson;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, de};
use std::fmt;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionData {
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    pub class_filter_mode: bool,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub topic: Option<String>,
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub sub_string: Option<String>,
    #[serde(
        default = "empty_tags",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_string_set",
        skip_serializing_if = "Option::is_none"
    )]
    pub tags_set: Option<HashSet<Option<String>>>,
    #[serde(
        default = "empty_codes",
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_int_set",
        skip_serializing_if = "Option::is_none"
    )]
    pub code_set: Option<HashSet<Option<i32>>>,
    #[serde(
        default = "current_subscription_version",
        deserialize_with = "serde_fastjson::deserialize_i64_reject_object"
    )]
    pub sub_version: i64,
    #[serde(
        default = "default_expression_type",
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub expression_type: Option<String>,
    #[serde(
        rename = "filterClassSource",
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing
    )]
    pub filter_class_source: Option<String>,
}

impl PartialEq for SubscriptionData {
    fn eq(&self, other: &Self) -> bool {
        self.class_filter_mode == other.class_filter_mode
            && self.topic == other.topic
            && self.sub_string == other.sub_string
            && self.tags_set == other.tags_set
            && self.code_set == other.code_set
            && self.sub_version == other.sub_version
            && self.expression_type == other.expression_type
    }
}

impl Eq for SubscriptionData {}

impl Default for SubscriptionData {
    fn default() -> Self {
        Self {
            class_filter_mode: false,
            topic: None,
            sub_string: None,
            tags_set: empty_tags(),
            code_set: empty_codes(),
            sub_version: current_subscription_version(),
            expression_type: default_expression_type(),
            filter_class_source: None,
        }
    }
}

impl Hash for SubscriptionData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.class_filter_mode.hash(state);
        self.topic.hash(state);
        self.sub_string.hash(state);
        hash_optional_set(&self.tags_set, state);
        hash_optional_set(&self.code_set, state);
        self.sub_version.hash(state);
        self.expression_type.hash(state);
    }
}

impl SubscriptionData {
    pub const SUB_ALL: &str = "*";

    /// Mirrors the Java `(topic, subString)` constructor while retaining
    /// Rust's explicit representation for nullable Java strings.
    pub fn new(topic: Option<String>, sub_string: Option<String>) -> Self {
        Self {
            topic,
            sub_string,
            ..Self::default()
        }
    }

    /// Java `SubscriptionData.hashCode`; subVersion and filterClassSource do
    /// not participate even though subVersion is checked by `equals`.
    pub fn java_hash_code(&self) -> i32 {
        let mut hash = 1i32;
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(if self.class_filter_mode { 1231 } else { 1237 });
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(java_optional_set_hash(&self.code_set, |value| {
                value.unwrap_or(0)
            }));
        hash = hash.wrapping_mul(31).wrapping_add(
            self.sub_string
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
        );
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(java_optional_set_hash(&self.tags_set, |value| {
                value.as_deref().map(java_string_hash_code).unwrap_or(0)
            }));
        hash = hash.wrapping_mul(31).wrapping_add(
            self.topic
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
        );
        hash.wrapping_mul(31).wrapping_add(
            self.expression_type
                .as_deref()
                .map(java_string_hash_code)
                .unwrap_or(0),
        )
    }

    /// Java `SubscriptionData.toString` field order and labels. Set contents
    /// are sorted in Java string order because Java HashSet iteration order is
    /// unspecified and Rust HashSet iteration is randomized.
    pub fn java_to_string(&self) -> String {
        format!(
            "SubscriptionData [classFilterMode={}, topic={}, subString={}, tagsSet={}, codeSet={}, subVersion={}, expressionType={}]",
            self.class_filter_mode,
            java_nullable_string(&self.topic),
            java_nullable_string(&self.sub_string),
            java_string_set_to_string(&self.tags_set),
            java_integer_set_to_string(&self.code_set),
            self.sub_version,
            java_nullable_string(&self.expression_type),
        )
    }

    /// Java compares the concatenated `topic@subString` keys as UTF-16. A
    /// null component is rendered as the literal `null` by StringBuilder.
    pub fn java_compare_to(&self, other: &Self) -> i32 {
        let left = format!(
            "{}@{}",
            java_nullable_string(&self.topic),
            java_nullable_string(&self.sub_string)
        );
        let right = format!(
            "{}@{}",
            java_nullable_string(&other.topic),
            java_nullable_string(&other.sub_string)
        );
        java_string_compare(&left, &right)
    }
}

fn java_optional_set_hash<T>(
    values: &Option<HashSet<T>>,
    mut value_hash: impl FnMut(&T) -> i32,
) -> i32 {
    values.as_ref().map_or(0, |values| {
        values
            .iter()
            .fold(0i32, |hash, value| hash.wrapping_add(value_hash(value)))
    })
}

fn java_nullable_string(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or("null")
}

fn java_string_hash_code(value: &str) -> i32 {
    value.encode_utf16().fold(0i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn java_string_compare(left: &str, right: &str) -> i32 {
    let mut left = left.encode_utf16();
    let mut right = right.encode_utf16();
    loop {
        match (left.next(), right.next()) {
            (Some(left), Some(right)) if left != right => {
                return i32::from(left) - i32::from(right);
            }
            (Some(_), Some(_)) => {}
            (None, Some(_)) => return -(1 + right.count() as i32),
            (Some(_), None) => return 1 + left.count() as i32,
            (None, None) => return 0,
        }
    }
}

fn java_string_set_to_string(values: &Option<HashSet<Option<String>>>) -> String {
    let Some(values) = values else {
        return "null".to_owned();
    };
    let mut values = values
        .iter()
        .map(|value| value.as_deref())
        .collect::<Vec<_>>();
    values.sort_by(|left, right| match (left, right) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(left), Some(right)) => java_string_compare(left, right).cmp(&0),
    });
    format!(
        "[{}]",
        values
            .into_iter()
            .map(|value| value.unwrap_or("null"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn java_integer_set_to_string(values: &Option<HashSet<Option<i32>>>) -> String {
    let Some(values) = values else {
        return "null".to_owned();
    };
    let mut values = values.iter().copied().collect::<Vec<_>>();
    values.sort_unstable();
    format!(
        "[{}]",
        values
            .into_iter()
            .map(|value| value.map_or_else(|| "null".to_owned(), |value| value.to_string()))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn java_hash_set_to_string<T>(
    values: &Option<HashSet<Option<T>>>,
    mut item_to_string: impl FnMut(&T) -> String,
) -> String {
    let Some(values) = values else {
        return "null".to_owned();
    };
    let mut values = values
        .iter()
        .map(|value| {
            value.as_ref().map_or_else(
                || (0, "null".to_owned()),
                |value| (1, item_to_string(value)),
            )
        })
        .collect::<Vec<_>>();
    values.sort();
    format!(
        "[{}]",
        values
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn java_vec_to_string<T>(
    values: &Option<Vec<Option<T>>>,
    mut item_to_string: impl FnMut(&T) -> String,
) -> String {
    let Some(values) = values else {
        return "null".to_owned();
    };
    let mut values = values
        .iter()
        .map(|value| {
            value.as_ref().map_or_else(
                || (0, "null".to_owned()),
                |value| (1, item_to_string(value)),
            )
        })
        .collect::<Vec<_>>();
    values.sort();
    format!(
        "[{}]",
        values
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn hash_set<T: Hash, H: Hasher>(values: &HashSet<T>, state: &mut H) {
    let mut hashes = values
        .iter()
        .map(|value| {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        })
        .collect::<Vec<_>>();
    hashes.sort_unstable();
    hashes.hash(state);
}

fn hash_optional_set<T: Hash, H: Hasher>(values: &Option<HashSet<T>>, state: &mut H) {
    match values {
        Some(values) => {
            true.hash(state);
            hash_set(values, state);
        }
        None => false.hash(state),
    }
}

fn empty_tags() -> Option<HashSet<Option<String>>> {
    Some(HashSet::new())
}

fn empty_codes() -> Option<HashSet<Option<i32>>> {
    Some(HashSet::new())
}

fn current_subscription_version() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

#[derive(Deserialize)]
#[serde(untagged)]
enum FastjsonEnumInput {
    Name(String),
    Ordinal(i64),
    Decimal(f64),
}

trait JavaOrdinalEnum: Sized {
    fn from_java_ordinal(ordinal: i64) -> Option<Self>;
}

fn deserialize_fastjson_optional_enum<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + JavaOrdinalEnum,
{
    match Option::<FastjsonEnumInput>::deserialize(deserializer)? {
        None => Ok(None),
        Some(FastjsonEnumInput::Name(name)) => Ok(T::deserialize(
            serde::de::value::StringDeserializer::<D::Error>::new(name.to_ascii_uppercase()),
        )
        .ok()),
        Some(FastjsonEnumInput::Ordinal(ordinal)) => T::from_java_ordinal(ordinal)
            .map(Some)
            .ok_or_else(|| <D::Error as serde::de::Error>::custom("enum ordinal out of range")),
        Some(FastjsonEnumInput::Decimal(ordinal)) => T::from_java_ordinal(ordinal.trunc() as i64)
            .map(Some)
            .ok_or_else(|| <D::Error as serde::de::Error>::custom("enum ordinal out of range")),
    }
}

fn default_expression_type() -> Option<String> {
    Some("TAG".to_owned())
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConsumeType {
    #[default]
    ConsumeActively,
    ConsumePassively,
    ConsumePop,
}

impl_java_enum_api!(
    ConsumeType,
    "org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType",
    3,
    ConsumeActively => "CONSUME_ACTIVELY" = 0,
    ConsumePassively => "CONSUME_PASSIVELY" = 1,
    ConsumePop => "CONSUME_POP" = 2,
);

impl ConsumeType {
    pub const fn type_cn(self) -> &'static str {
        match self {
            Self::ConsumeActively => "PULL",
            Self::ConsumePassively => "PUSH",
            Self::ConsumePop => "POP",
        }
    }
}

impl JavaOrdinalEnum for ConsumeType {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::ConsumeActively),
            1 => Some(Self::ConsumePassively),
            2 => Some(Self::ConsumePop),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageModel {
    #[default]
    Clustering,
    Broadcasting,
    LiteSelective,
}

impl_java_enum_api!(
    MessageModel,
    "org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel",
    3,
    Broadcasting => "BROADCASTING" = 0,
    Clustering => "CLUSTERING" = 1,
    LiteSelective => "LITE_SELECTIVE" = 2,
);

impl PartialOrd for MessageModel {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MessageModel {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ordinal().cmp(&other.ordinal())
    }
}

impl MessageModel {
    pub const fn mode_cn(self) -> &'static str {
        match self {
            Self::Broadcasting => "BROADCASTING",
            Self::Clustering => "CLUSTERING",
            Self::LiteSelective => "LITE_SELECTIVE",
        }
    }
}

impl JavaOrdinalEnum for MessageModel {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::Broadcasting),
            1 => Some(Self::Clustering),
            2 => Some(Self::LiteSelective),
            _ => None,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConsumeFromWhere {
    #[default]
    ConsumeFromLastOffset,
    #[serde(rename = "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST")]
    ConsumeFromLastOffsetAndFromMinWhenBootFirst,
    ConsumeFromMinOffset,
    ConsumeFromMaxOffset,
    ConsumeFromFirstOffset,
    ConsumeFromTimestamp,
}

impl_java_enum_api!(
    ConsumeFromWhere,
    "org.apache.rocketmq.common.consumer.ConsumeFromWhere",
    6,
    ConsumeFromLastOffset => "CONSUME_FROM_LAST_OFFSET" = 0,
    ConsumeFromLastOffsetAndFromMinWhenBootFirst => "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST" = 1,
    ConsumeFromMinOffset => "CONSUME_FROM_MIN_OFFSET" = 2,
    ConsumeFromMaxOffset => "CONSUME_FROM_MAX_OFFSET" = 3,
    ConsumeFromFirstOffset => "CONSUME_FROM_FIRST_OFFSET" = 4,
    ConsumeFromTimestamp => "CONSUME_FROM_TIMESTAMP" = 5,
);

impl JavaOrdinalEnum for ConsumeFromWhere {
    fn from_java_ordinal(ordinal: i64) -> Option<Self> {
        match ordinal {
            0 => Some(Self::ConsumeFromLastOffset),
            1 => Some(Self::ConsumeFromLastOffsetAndFromMinWhenBootFirst),
            2 => Some(Self::ConsumeFromMinOffset),
            3 => Some(Self::ConsumeFromMaxOffset),
            4 => Some(Self::ConsumeFromFirstOffset),
            5 => Some(Self::ConsumeFromTimestamp),
            _ => None,
        }
    }
}

/// Java inherits identity based `Object.equals`/`hashCode` for this DTO, so
/// the native type intentionally has no structural equality or hash traits.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::heartbeat::ProducerData;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<ProducerData>(); }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProducerData {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub group_name: Option<String>,
}

impl ProducerData {
    pub fn java_to_string(&self) -> String {
        format!(
            "ProducerData [groupName={}]",
            java_nullable_string(&self.group_name)
        )
    }
}

/// Java inherits identity based `Object.equals`/`hashCode` for this DTO, so
/// the native type intentionally has no structural equality or hash traits.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::heartbeat::ConsumerData;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<ConsumerData>(); }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerData {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub group_name: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub consume_type: Option<ConsumeType>,
    #[serde(
        default,
        deserialize_with = "deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_model: Option<MessageModel>,
    #[serde(
        default,
        deserialize_with = "deserialize_fastjson_optional_enum",
        skip_serializing_if = "Option::is_none"
    )]
    pub consume_from_where: Option<ConsumeFromWhere>,
    #[serde(
        default = "empty_subscriptions",
        deserialize_with = "deserialize_nullable_hash_set_with_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub subscription_data_set: Option<HashSet<Option<SubscriptionData>>>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_bool")]
    pub unit_mode: bool,
}

impl Default for ConsumerData {
    fn default() -> Self {
        Self {
            group_name: None,
            consume_type: None,
            message_model: None,
            consume_from_where: None,
            subscription_data_set: empty_subscriptions(),
            unit_mode: false,
        }
    }
}

impl ConsumerData {
    pub fn java_to_string(&self) -> String {
        format!(
            "ConsumerData [groupName={}, consumeType={}, messageModel={}, consumeFromWhere={}, unitMode={}, subscriptionDataSet={}]",
            java_nullable_string(&self.group_name),
            self.consume_type
                .map(consume_type_java_name)
                .unwrap_or("null"),
            self.message_model
                .map(message_model_java_name)
                .unwrap_or("null"),
            self.consume_from_where
                .map(consume_from_where_java_name)
                .unwrap_or("null"),
            self.unit_mode,
            java_hash_set_to_string(&self.subscription_data_set, |subscription| {
                subscription.java_to_string()
            }),
        )
    }
}

fn consume_type_java_name(value: ConsumeType) -> &'static str {
    match value {
        ConsumeType::ConsumeActively => "CONSUME_ACTIVELY",
        ConsumeType::ConsumePassively => "CONSUME_PASSIVELY",
        ConsumeType::ConsumePop => "CONSUME_POP",
    }
}

fn message_model_java_name(value: MessageModel) -> &'static str {
    match value {
        MessageModel::Clustering => "CLUSTERING",
        MessageModel::Broadcasting => "BROADCASTING",
        MessageModel::LiteSelective => "LITE_SELECTIVE",
    }
}

fn consume_from_where_java_name(value: ConsumeFromWhere) -> &'static str {
    match value {
        ConsumeFromWhere::ConsumeFromLastOffset => "CONSUME_FROM_LAST_OFFSET",
        ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst => {
            "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST"
        }
        ConsumeFromWhere::ConsumeFromMinOffset => "CONSUME_FROM_MIN_OFFSET",
        ConsumeFromWhere::ConsumeFromMaxOffset => "CONSUME_FROM_MAX_OFFSET",
        ConsumeFromWhere::ConsumeFromFirstOffset => "CONSUME_FROM_FIRST_OFFSET",
        ConsumeFromWhere::ConsumeFromTimestamp => "CONSUME_FROM_TIMESTAMP",
    }
}

fn empty_subscriptions() -> Option<HashSet<Option<SubscriptionData>>> {
    Some(HashSet::new())
}

/// Java inherits identity based `Object.equals`/`hashCode` for this DTO, so
/// the native type intentionally has no structural equality or hash traits.
///
/// ```compile_fail
/// use rocketmq_classic_protocol::heartbeat::HeartbeatData;
/// fn require_java_identity<T: PartialEq + Eq + std::hash::Hash>() {}
/// fn main() { require_java_identity::<HeartbeatData>(); }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatData {
    #[serde(
        rename = "clientID",
        default,
        deserialize_with = "serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub client_id: Option<String>,
    #[serde(default = "empty_producers", skip_serializing_if = "Option::is_none")]
    /// Java ProducerData inherits identity equality, so repeated equal wire
    /// rows must remain distinct after decoding.
    #[serde(deserialize_with = "deserialize_identity_set_with_null")]
    pub producer_data_set: Option<Vec<Option<ProducerData>>>,
    #[serde(default = "empty_consumers", skip_serializing_if = "Option::is_none")]
    /// Java ConsumerData has object identity equality. Preserve its wire
    /// multiplicity rather than deduplicating structurally equal metadata.
    #[serde(deserialize_with = "deserialize_identity_set_with_null")]
    pub consumer_data_set: Option<Vec<Option<ConsumerData>>>,
    #[serde(default, deserialize_with = "serde_fastjson::deserialize_i32")]
    pub heartbeat_fingerprint: i32,
    #[serde(
        default,
        rename = "withoutSub",
        deserialize_with = "serde_fastjson::deserialize_bool"
    )]
    pub is_without_sub: bool,
}

#[cfg(test)]
mod fastjson_dto_input_tests {
    use super::{ConsumerData, HeartbeatData, ProducerData, SubscriptionData};
    use rocketmq_classic_common::serde_fastjson;

    fn normalize_json(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Array(values) => {
                for value in values.iter_mut() {
                    normalize_json(value);
                }
                values.sort_by_key(|value| serde_json::to_string(value).unwrap());
            }
            serde_json::Value::Object(fields) => {
                fields.retain(|_, value| !value.is_null());
                for value in fields.values_mut() {
                    normalize_json(value);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn heartbeat_dto_inputs_match_invoked_java_fastjson() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let cases = fixture["heartbeatDtoInputCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            52,
            "Java oracle includes DTO scalar, numeric-boundary, and collection-shape probes"
        );
        for required in [
            "subscription-high-precision-number-to-string",
            "subscription-exponent-number-to-string",
            "subscription-decimal-exponent-number-to-string",
            "subscription-precision-exponent-number-to-string",
            "subscription-tags-high-precision-number-to-string",
            "subscription-leading-plus-zero-version",
            "heartbeat-leading-plus-zero-fingerprint",
            "consumer-form-feed-and-leading-zero",
            "heartbeat-bom-prefix",
            "subscription-form-feed-before-version",
            "consumer-vertical-tab-prefix",
            "heartbeat-embedded-bom",
            "subscription-object-to-string",
            "subscription-array-to-string",
            "producer-object-to-string",
            "heartbeat-object-to-string",
            "subscription-tags-singleton-string",
            "subscription-tags-singleton-number",
            "subscription-tags-singleton-boolean",
            "subscription-tags-singleton-object",
            "subscription-tags-object-member",
            "subscription-codes-singleton-number",
            "subscription-codes-singleton-string",
            "subscription-codes-singleton-boolean",
            "subscription-codes-singleton-object",
            "subscription-codes-object-member",
            "subscription-long-object-to-zero",
            "subscription-long-empty-object-to-zero",
            "subscription-long-empty-array",
            "subscription-code-set-boundaries",
            "subscription-code-set-overflow-positive",
            "subscription-code-set-overflow-negative",
            "subscription-code-set-numeric-string-exponent",
            "subscription-code-set-wide-decimal",
            "subscription-code-set-high-precision-decimal",
            "subscription-code-set-decimal-long-maximum",
            "subscription-code-set-decimal-long-overflow-positive",
            "subscription-code-set-decimal-long-overflow-negative",
            "subscription-tags-coercion-deduplicates",
            "consumer-subscriptions-singleton-object",
            "heartbeat-producers-singleton-object",
            "heartbeat-consumers-singleton-object",
        ] {
            assert!(
                cases.iter().any(|case| case["id"] == required),
                "missing {required}"
            );
        }
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let decoded = match case["type"].as_str().unwrap() {
                "subscription" => serde_fastjson::from_fastjson_str::<SubscriptionData>(raw_input)
                    .and_then(serde_json::to_value),
                "producer" => serde_fastjson::from_fastjson_str::<ProducerData>(raw_input)
                    .and_then(serde_json::to_value),
                "consumer" => serde_fastjson::from_fastjson_str::<ConsumerData>(raw_input)
                    .and_then(serde_json::to_value),
                "heartbeat" => {
                    HeartbeatData::java_from_json(raw_input).and_then(serde_json::to_value)
                }
                other => panic!("unknown heartbeat DTO case type {other}"),
            };
            let java_accepts = case["outcome"] == "ACCEPTED";
            assert_eq!(decoded.is_ok(), java_accepts, "{}: {decoded:?}", case["id"]);
            if let Ok(mut decoded) = decoded {
                let mut expected = case["fields"].clone();
                normalize_json(&mut decoded);
                normalize_json(&mut expected);
                assert_eq!(decoded, expected, "{}", case["id"]);
            }
        }
    }
}

impl Default for HeartbeatData {
    fn default() -> Self {
        Self {
            client_id: None,
            producer_data_set: empty_producers(),
            consumer_data_set: empty_consumers(),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        }
    }
}

impl HeartbeatData {
    /// Decodes the inherited Java `RemotingSerializable.decode` form.
    pub fn java_decode(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }

    /// Decodes the nullable inherited Java utility, including null bytes and
    /// a JSON `null` root.
    pub fn java_decode_nullable(data: Option<&[u8]>) -> Result<Option<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_slice_nullable(data)
    }

    /// Decodes the inherited Java `RemotingSerializable.decodeList` form.
    pub fn java_decode_list(data: &[u8]) -> Result<Vec<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_slice(data)
    }

    /// Decodes a nullable Java list, preserving null roots and members.
    pub fn java_decode_list_nullable(
        data: Option<&[u8]>,
    ) -> Result<Option<Vec<Option<Self>>>, serde_json::Error> {
        serde_fastjson::from_fastjson_list_nullable(data)
    }

    /// Decodes the inherited Java `RemotingSerializable.fromJson` form.
    pub fn java_from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_fastjson::from_fastjson_str(json)
    }

    /// Decodes the nullable Java `fromJson` utility, including JSON `null`.
    pub fn java_from_json_nullable(json: Option<&str>) -> Result<Option<Self>, serde_json::Error> {
        serde_fastjson::from_fastjson_str_nullable(json)
    }

    /// Java `RemotingSerializable.toJson(Object, prettyFormat)` and the
    /// inherited instance `toJson(boolean)` serialize Fastjson's sorted bean
    /// fields. `serde_json::Value` supplies the same deterministic key order.
    pub fn java_to_json(&self, pretty_format: bool) -> Result<String, serde_json::Error> {
        let value = serde_json::to_value(self)?;
        if pretty_format {
            serde_json::to_string_pretty(&value)
        } else {
            serde_json::to_string(&value)
        }
    }

    /// Java `RemotingSerializable.encode(Object)` and inherited `encode()`
    /// return the compact UTF-8 representation produced by `java_to_json`.
    pub fn java_encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        Ok(self.java_to_json(false)?.into_bytes())
    }

    /// Java `HeartbeatData.toString`; set ordering is normalized because the
    /// source uses HashSet iteration with unspecified order.
    pub fn java_to_string(&self) -> String {
        format!(
            "HeartbeatData [clientID={}, producerDataSet={}, consumerDataSet={}]",
            java_nullable_string(&self.client_id),
            java_vec_to_string(&self.producer_data_set, ProducerData::java_to_string),
            java_vec_to_string(&self.consumer_data_set, ConsumerData::java_to_string),
        )
    }

    /// Computes the client-local heartbeat fingerprint using Java 5.5.1's
    /// JSON-copy defaults, normalized fields and Java UTF-16 string hash.
    pub fn compute_heartbeat_fingerprint(&self) -> Result<i32, HeartbeatFingerprintError> {
        let mut value = serde_json::to_value(self)?;
        // Java serializes and reparses a copy first. Omitted nullable
        // collections regain their constructor-empty values during parsing.
        if !value["producerDataSet"].is_array() {
            value["producerDataSet"] = serde_json::json!([]);
        }
        if !value["consumerDataSet"].is_array() {
            value["consumerDataSet"] = serde_json::json!([]);
        }
        value["clientID"] = "".into();
        value["heartbeatFingerprint"] = 0.into();
        value["withoutSub"] = false.into();
        if let Some(consumers) = value["consumerDataSet"].as_array_mut() {
            for consumer in consumers {
                if consumer.is_null() {
                    return Err(HeartbeatFingerprintError::NullConsumerData);
                }
                if !consumer["subscriptionDataSet"].is_array() {
                    consumer["subscriptionDataSet"] = serde_json::json!([]);
                }
                if let Some(subscriptions) = consumer["subscriptionDataSet"].as_array_mut() {
                    for subscription in subscriptions {
                        if subscription.is_null() {
                            return Err(HeartbeatFingerprintError::NullSubscriptionData);
                        }
                        subscription["subVersion"] = 0.into();
                    }
                }
            }
        }
        fn canonicalize(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        canonicalize(value);
                    }
                    values.sort_unstable_by_key(serde_json::Value::to_string);
                }
                serde_json::Value::Object(values) => {
                    for value in values.values_mut() {
                        canonicalize(value);
                    }
                }
                _ => {}
            }
        }
        canonicalize(&mut value);
        Ok(value.to_string().encode_utf16().fold(0i32, |hash, unit| {
            hash.wrapping_mul(31).wrapping_add(i32::from(unit))
        }))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HeartbeatFingerprintError {
    #[error("heartbeat fingerprint JSON conversion failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Java NullPointerException: null ConsumerData in consumerDataSet")]
    NullConsumerData,
    #[error("Java NullPointerException: null SubscriptionData in subscriptionDataSet")]
    NullSubscriptionData,
}

struct IdentitySetVisitor<T>(PhantomData<T>);

impl<'de, T> Visitor<'de> for IdentitySetVisitor<T>
where
    T: Deserialize<'de>,
{
    type Value = Option<Vec<Option<T>>>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON array, object, or null for a Java Set field")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<Option<T>>()? {
            values.push(value);
        }
        Ok(Some(deduplicate_null_set_member(values)))
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let value = T::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
        Ok(Some(vec![Some(value)]))
    }
}

fn deduplicate_null_set_member<T>(mut values: Vec<Option<T>>) -> Vec<Option<T>> {
    let mut retained_null = false;
    values.retain(|value| {
        if value.is_some() {
            true
        } else if retained_null {
            false
        } else {
            retained_null = true;
            true
        }
    });
    values
}

fn deserialize_identity_set_with_null<'de, T, D>(
    deserializer: D,
) -> Result<Option<Vec<Option<T>>>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(IdentitySetVisitor(PhantomData))
}

fn deserialize_nullable_hash_set_with_null<'de, T, D>(
    deserializer: D,
) -> Result<Option<HashSet<Option<T>>>, D::Error>
where
    T: Deserialize<'de> + Eq + std::hash::Hash,
    D: Deserializer<'de>,
{
    deserialize_identity_set_with_null(deserializer)
        .map(|values| values.map(|values| values.into_iter().collect()))
}

fn empty_producers() -> Option<Vec<Option<ProducerData>>> {
    Some(Vec::new())
}

fn empty_consumers() -> Option<Vec<Option<ConsumerData>>> {
    Some(Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_fields_match_invoked_java_nulls_and_defaults() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["subscriptionFieldCases"].as_array().unwrap() {
            let raw = case["rawInput"].as_str().unwrap();
            let before = current_millis();
            let value: SubscriptionData = serde_json::from_str(raw).unwrap();
            let after = current_millis();
            let encoded = serde_json::to_value(&value).unwrap();
            assert_eq!(value.topic.is_none(), case["topicNull"]);
            assert_eq!(value.sub_string.is_none(), case["subStringNull"]);
            assert_eq!(value.tags_set.is_none(), case["tagsNull"]);
            assert_eq!(value.code_set.is_none(), case["codeNull"]);
            assert_eq!(value.expression_type.is_none(), case["expressionNull"]);
            assert_eq!(
                value.filter_class_source,
                case["filterClassSource"].as_str().map(str::to_owned)
            );
            assert_eq!(
                value.filter_class_source.is_none(),
                case["filterClassSourceNull"]
            );
            assert_eq!(
                value.tags_set.as_ref().is_some_and(HashSet::is_empty),
                case["tagsEmpty"]
            );
            assert_eq!(
                value.code_set.as_ref().is_some_and(HashSet::is_empty),
                case["codeEmpty"]
            );
            assert_eq!(
                value.tags_set.as_ref().map(HashSet::len),
                case["tagsCount"].as_u64().map(|count| count as usize)
            );
            assert_eq!(
                value.code_set.as_ref().map(HashSet::len),
                case["codeCount"].as_u64().map(|count| count as usize)
            );
            assert_eq!(
                value
                    .tags_set
                    .as_ref()
                    .is_some_and(|values| values.contains(&None)),
                case["tagsHasNull"]
            );
            assert_eq!(
                value
                    .code_set
                    .as_ref()
                    .is_some_and(|values| values.contains(&None)),
                case["codeHasNull"]
            );
            if case["versionCurrent"] == true {
                assert!((before..=after).contains(&value.sub_version));
            }
            assert_eq!(value.sub_version == 0, case["versionZero"]);
            assert_eq!(value.sub_version == 42, case["versionFortyTwo"]);
            for (field, marker) in [
                ("topic", "encodedHasTopic"),
                ("subString", "encodedHasSubString"),
                ("tagsSet", "encodedHasTags"),
                ("codeSet", "encodedHasCode"),
                ("expressionType", "encodedHasExpression"),
                ("filterClassSource", "encodedHasFilterClassSource"),
            ] {
                assert_eq!(encoded.get(field).is_some(), case[marker], "{field}");
                if field == "filterClassSource" {
                    assert_eq!(
                        encoded
                            .get(field)
                            .cloned()
                            .unwrap_or(serde_json::Value::Null),
                        case["encodedFilterClassSource"]
                    );
                }
            }
            for (field, marker) in [("tagsSet", "tagsHasNull"), ("codeSet", "codeHasNull")] {
                assert_eq!(
                    encoded
                        .get(field)
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|values| values.contains(&serde_json::Value::Null)),
                    case[marker],
                    "{field} null member"
                );
            }
            for (field, expected_field) in [("tagsSet", "encodedTags"), ("codeSet", "encodedCode")]
            {
                if let Some(expected) = case[expected_field].as_array() {
                    let mut expected = expected.clone();
                    let mut actual = encoded[field].as_array().unwrap().clone();
                    expected.sort_by_key(serde_json::Value::to_string);
                    actual.sort_by_key(serde_json::Value::to_string);
                    assert_eq!(actual, expected, "{field} JSON values");
                }
            }
        }
    }

    #[test]
    fn subscription_constant_and_constructors_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        assert_eq!(
            SubscriptionData::SUB_ALL,
            fixture["subscriptionMethods"]["subAll"]
        );
        for case in fixture["subscriptionConstructorCases"].as_array().unwrap() {
            let value = match case["case"].as_str().unwrap() {
                "no-argument" => SubscriptionData::default(),
                "two-argument" => SubscriptionData::new(Some("topic".into()), Some("tag".into())),
                "two-null-arguments" => SubscriptionData::new(None, None),
                name => panic!("unknown SubscriptionData constructor case: {name}"),
            };
            assert_eq!(value.topic, case["topic"].as_str().map(str::to_owned));
            assert_eq!(
                value.sub_string,
                case["subString"].as_str().map(str::to_owned)
            );
            assert_eq!(
                value.tags_set.as_ref().is_some_and(HashSet::is_empty),
                case["tagsEmpty"]
            );
            assert_eq!(
                value.code_set.as_ref().is_some_and(HashSet::is_empty),
                case["codeEmpty"]
            );
            assert_eq!(
                value.expression_type,
                case["expressionType"].as_str().map(str::to_owned)
            );
            assert_eq!(value.sub_version > 0, case["versionCurrent"]);
        }
    }

    #[test]
    fn subscription_equality_matches_invoked_java_method() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["subscriptionMethods"]["equalCases"]
            .as_array()
            .unwrap()
        {
            let mut left: SubscriptionData =
                serde_json::from_str(case["leftInput"].as_str().unwrap()).unwrap();
            let mut right: SubscriptionData =
                serde_json::from_str(case["rightInput"].as_str().unwrap()).unwrap();
            left.filter_class_source = case["leftFilterClassSource"].as_str().map(str::to_owned);
            right.filter_class_source = case["rightFilterClassSource"].as_str().map(str::to_owned);
            assert_eq!(left == right, case["equal"], "{}", case["case"]);
        }
    }

    #[test]
    fn subscription_methods_match_invoked_java_hash_text_and_comparison() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let methods = &fixture["subscriptionMethods"];
        for case in methods["values"].as_array().unwrap() {
            let mut value: SubscriptionData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            value.filter_class_source = Some("ignored-in-text-and-equality".to_owned());
            assert_eq!(
                value.java_hash_code(),
                case["hashCode"].as_i64().unwrap() as i32,
                "{} hashCode",
                case["case"]
            );
            assert_eq!(
                value.java_to_string(),
                case["toString"],
                "{} toString",
                case["case"]
            );
            let encoded = serde_json::to_value(&value).unwrap();
            assert_eq!(
                encoded.get("filterClassSource").is_some(),
                case["encodedHasFilterClassSource"],
                "{} JSON field",
                case["case"]
            );
            assert_eq!(
                encoded
                    .get("filterClassSource")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                case["encodedFilterClassSource"],
                "{} JSON value",
                case["case"]
            );
        }
        for case in methods["equalCases"].as_array().unwrap() {
            let left: SubscriptionData =
                serde_json::from_str(case["leftInput"].as_str().unwrap()).unwrap();
            let right: SubscriptionData =
                serde_json::from_str(case["rightInput"].as_str().unwrap()).unwrap();
            assert_eq!(
                left.java_hash_code(),
                case["leftHashCode"].as_i64().unwrap() as i32,
                "{} left hashCode",
                case["case"]
            );
            assert_eq!(
                right.java_hash_code(),
                case["rightHashCode"].as_i64().unwrap() as i32,
                "{} right hashCode",
                case["case"]
            );
        }
        for case in methods["compareCases"].as_array().unwrap() {
            let left: SubscriptionData =
                serde_json::from_str(case["leftInput"].as_str().unwrap()).unwrap();
            let right: SubscriptionData =
                serde_json::from_str(case["rightInput"].as_str().unwrap()).unwrap();
            assert_eq!(
                left.java_compare_to(&right),
                case["comparison"].as_i64().unwrap() as i32,
                "{} compareTo",
                case["case"]
            );
        }
    }

    #[test]
    fn consumer_enum_inputs_match_invoked_java_names_and_rejections() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let cases = fixture["consumerEnumCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            51,
            "Java oracle includes enum name and ordinal coercions"
        );
        for case in cases {
            let input = case["rawInput"].as_str().unwrap();
            match case["outcome"].as_str().unwrap() {
                "accepted" | "null" => {
                    let value: ConsumerData = serde_json::from_str(input)
                        .unwrap_or_else(|error| panic!("{}: {error}", case["rawInput"]));
                    let encoded = serde_json::to_value(value).unwrap();
                    assert_eq!(
                        encoded
                            .get(case["field"].as_str().unwrap())
                            .cloned()
                            .unwrap_or(serde_json::Value::Null),
                        case["encodedName"],
                        "{} encode",
                        case["rawInput"]
                    );
                }
                "rejected" => assert!(
                    serde_json::from_str::<ConsumerData>(input).is_err(),
                    "{} should reject like Java ({})",
                    case["rawInput"],
                    case["error"]
                ),
                outcome => panic!("unknown Java enum outcome: {outcome}"),
            }
        }
    }

    #[test]
    fn consumer_enum_public_methods_match_invoked_java_values() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["consumerEnumMethodCases"].as_array().unwrap() {
            let actual = match case["enum"].as_str().unwrap() {
                "consumeType" => {
                    let value: ConsumeType =
                        serde_json::from_value(case["constant"].clone()).unwrap();
                    value.type_cn()
                }
                "messageModel" => {
                    let value: MessageModel =
                        serde_json::from_value(case["constant"].clone()).unwrap();
                    value.mode_cn()
                }
                kind => panic!("unknown enum kind: {kind}"),
            };
            assert_eq!(actual, case["methodValue"].as_str().unwrap(), "{case}");
        }
    }

    #[test]
    fn heartbeat_dto_identity_methods_match_invoked_java_object_behavior() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let cases = fixture["identityMethodCases"].as_array().unwrap();
        assert_eq!(cases.len(), 3);
        for case in cases {
            assert_eq!(case["equalsDeclaringClass"], "java.lang.Object", "{case}");
            assert_eq!(case["hashCodeDeclaringClass"], "java.lang.Object", "{case}");
            assert_eq!(case["sameInstanceEquals"], true, "{case}");
            assert_eq!(case["equalFieldsDistinctInstanceEquals"], false, "{case}");
            assert_eq!(case["equalFieldsSetSize"], 2, "{case}");
        }
    }

    #[test]
    fn heartbeat_inherited_serialization_methods_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let methods = &fixture["heartbeatSerializationMethodCases"];
        assert_eq!(methods["staticEncodeNullIsNull"], true);
        assert_eq!(methods["staticToJsonNull"], "null");
        assert_eq!(methods["decodeNullIsNull"], true);
        assert_eq!(methods["decodeListNullIsNull"], true);
        assert_eq!(methods["fromJsonNullIsNull"], true);
        assert_eq!(methods["decodeJsonNullIsNull"], true);
        assert_eq!(methods["decodeListJsonNullIsNull"], true);
        assert_eq!(methods["fromJsonJsonNullIsNull"], true);
        assert_eq!(methods["decodeListPreservesNullMember"], true);
        assert_eq!(
            methods["featureEncodeParameterType"],
            "com.alibaba.fastjson2.JSONWriter$Feature"
        );

        for case in methods["cases"].as_array().unwrap() {
            let raw_input = case["rawInput"].as_str().unwrap();
            let value = HeartbeatData::java_from_json(raw_input).unwrap();
            let native_json = value.java_to_json(false).unwrap();
            assert_eq!(
                native_json, case["instanceJson"],
                "{} instance encode",
                case["case"]
            );
            assert_eq!(
                native_json, case["staticJson"],
                "{} static toJson",
                case["case"]
            );
            assert_eq!(
                native_json, case["instanceEncodedJson"],
                "{} encode bytes",
                case["case"]
            );
            assert_eq!(
                native_json, case["staticEncodedJson"],
                "{} static encode",
                case["case"]
            );
            assert_eq!(
                native_json, case["featureEncodedJson"],
                "{} feature encode",
                case["case"]
            );
            assert_eq!(
                value.java_encode().unwrap(),
                native_json.as_bytes(),
                "{} UTF-8 encode",
                case["case"]
            );

            let pretty_json = value.java_to_json(true).unwrap();
            let actual_pretty: serde_json::Value = serde_json::from_str(&pretty_json).unwrap();
            for field in ["instancePrettyJson", "staticPrettyJson"] {
                let expected: serde_json::Value =
                    serde_json::from_str(case[field].as_str().unwrap()).unwrap();
                assert_eq!(actual_pretty, expected, "{} {field}", case["case"]);
            }

            let decoded_from_bytes = HeartbeatData::java_decode(native_json.as_bytes()).unwrap();
            let decoded_from_text = HeartbeatData::java_from_json(&native_json).unwrap();
            let decoded_list =
                HeartbeatData::java_decode_list(format!("[{native_json}]").as_bytes()).unwrap();
            for decoded in [
                decoded_from_bytes,
                decoded_from_text,
                decoded_list.into_iter().next().unwrap(),
            ] {
                let actual = serde_json::to_value(decoded).unwrap();
                for field in ["decodedJson", "fromJsonJson", "decodedListJson"] {
                    let expected: serde_json::Value =
                        serde_json::from_str(case[field].as_str().unwrap()).unwrap();
                    assert_eq!(actual, expected, "{} {field}", case["case"]);
                }
            }
            assert_eq!(case["decodedListCount"], 1, "{} decodeList", case["case"]);
        }

        assert_eq!(
            serde_json::to_string(&Option::<HeartbeatData>::None).unwrap(),
            methods["staticToJsonNull"]
        );
        assert!(
            serde_json::from_str::<Option<HeartbeatData>>("null")
                .unwrap()
                .is_none()
        );
        assert!(HeartbeatData::java_decode_nullable(None).unwrap().is_none());
        assert!(
            HeartbeatData::java_decode_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        assert!(
            HeartbeatData::java_decode_list_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            HeartbeatData::java_decode_list_nullable(Some(b"null"))
                .unwrap()
                .is_none()
        );
        assert!(
            HeartbeatData::java_from_json_nullable(None)
                .unwrap()
                .is_none()
        );
        assert!(
            HeartbeatData::java_from_json_nullable(Some("null"))
                .unwrap()
                .is_none()
        );
        let decoded_null_members = HeartbeatData::java_decode_list_nullable(Some(b"[null,{}]"))
            .unwrap()
            .unwrap();
        assert_eq!(decoded_null_members.len(), 2);
        assert!(decoded_null_members[0].is_none());
        assert!(decoded_null_members[1].is_some());
    }

    #[test]
    fn heartbeat_owner_fields_match_invoked_java_nulls_and_defaults() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        let cases = &fixture["ownerFieldCases"];
        for case in cases["producers"].as_array().unwrap() {
            let value: ProducerData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            let encoded = serde_json::to_value(&value).unwrap();
            assert_eq!(value.group_name.is_none(), case["groupNull"]);
            assert_eq!(value.java_to_string(), case["toString"]);
            assert_eq!(encoded.get("groupName").is_some(), case["encodedHasGroup"]);
        }
        for case in cases["consumers"].as_array().unwrap() {
            let value: ConsumerData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            let encoded = serde_json::to_value(&value).unwrap();
            assert_eq!(value.group_name.is_none(), case["groupNull"]);
            assert_eq!(value.java_to_string(), case["toString"]);
            assert_eq!(value.consume_type.is_none(), case["consumeTypeNull"]);
            assert_eq!(value.message_model.is_none(), case["messageModelNull"]);
            assert_eq!(
                value.consume_from_where.is_none(),
                case["consumeFromWhereNull"]
            );
            assert_eq!(
                value.subscription_data_set.is_none(),
                case["subscriptionsNull"]
            );
            assert_eq!(
                value
                    .subscription_data_set
                    .as_ref()
                    .is_some_and(HashSet::is_empty),
                case["subscriptionsEmpty"]
            );
            assert_eq!(value.unit_mode, case["unitMode"]);
            for (field, marker) in [
                ("groupName", "encodedHasGroup"),
                ("consumeType", "encodedHasConsumeType"),
                ("messageModel", "encodedHasMessageModel"),
                ("consumeFromWhere", "encodedHasConsumeFromWhere"),
                ("subscriptionDataSet", "encodedHasSubscriptions"),
            ] {
                assert_eq!(encoded.get(field).is_some(), case[marker], "{field}");
            }
        }
    }

    #[test]
    fn heartbeat_collections_match_invoked_java_nulls_and_defaults() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["heartbeatFieldCases"].as_array().unwrap() {
            let value: HeartbeatData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            let encoded = serde_json::to_value(&value).unwrap();
            assert_eq!(value.java_to_string(), case["toString"]);
            assert_eq!(value.client_id.is_none(), case["clientNull"]);
            assert_eq!(value.producer_data_set.is_none(), case["producersNull"]);
            assert_eq!(value.consumer_data_set.is_none(), case["consumersNull"]);
            assert_eq!(
                value
                    .producer_data_set
                    .as_ref()
                    .map_or(-1, |set| set.len() as i64),
                case["producerCount"]
            );
            assert_eq!(
                encoded["producerDataSet"]
                    .as_array()
                    .map_or(-1, |set| set.len() as i64),
                case["producerCount"]
            );
            assert_eq!(
                value
                    .consumer_data_set
                    .as_ref()
                    .map_or(-1, |set| set.len() as i64),
                case["consumerCount"]
            );
            assert_eq!(value.heartbeat_fingerprint, case["fingerprint"]);
            assert_eq!(
                i64::from(value.compute_heartbeat_fingerprint().unwrap()),
                case["computedFingerprint"]
            );
            assert_eq!(value.is_without_sub, case["withoutSub"]);
            assert_eq!(encoded.get("clientID").is_some(), case["encodedHasClient"]);
            assert_eq!(
                encoded.get("producerDataSet").is_some(),
                case["encodedHasProducers"]
            );
            assert_eq!(
                encoded.get("consumerDataSet").is_some(),
                case["encodedHasConsumers"]
            );
        }
    }

    #[test]
    fn heartbeat_null_set_members_match_invoked_java() {
        fn sort_arrays(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Array(values) => {
                    for value in values.iter_mut() {
                        sort_arrays(value);
                    }
                    values.sort_unstable_by_key(serde_json::Value::to_string);
                }
                serde_json::Value::Object(values) => {
                    for value in values.values_mut() {
                        sort_arrays(value);
                    }
                }
                _ => {}
            }
        }

        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["heartbeatNullElementCases"].as_array().unwrap() {
            let data: HeartbeatData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            assert_eq!(
                data.producer_data_set.as_ref().map(Vec::len),
                case["producerCount"].as_u64().map(|count| count as usize),
                "{} producer count",
                case["case"]
            );
            assert_eq!(
                data.producer_data_set
                    .as_ref()
                    .is_some_and(|values| values.iter().any(Option::is_none)),
                case["producerHasNull"],
                "{} producer null",
                case["case"]
            );
            assert_eq!(
                data.consumer_data_set.as_ref().map(Vec::len),
                case["consumerCount"].as_u64().map(|count| count as usize),
                "{} consumer count",
                case["case"]
            );
            assert_eq!(
                data.consumer_data_set
                    .as_ref()
                    .is_some_and(|values| values.iter().any(Option::is_none)),
                case["consumerHasNull"],
                "{} consumer null",
                case["case"]
            );

            if let Some(consumers) = data.consumer_data_set.as_ref() {
                if let Some(consumer) = consumers.iter().flatten().next() {
                    if case.get("subscriptionCount").is_some() {
                        let subscriptions = consumer.subscription_data_set.as_ref().unwrap();
                        assert_eq!(
                            subscriptions.len(),
                            case["subscriptionCount"].as_u64().unwrap() as usize
                        );
                        assert_eq!(subscriptions.contains(&None), case["subscriptionHasNull"]);
                    }
                }
            }

            let encoded = serde_json::to_value(&data).unwrap();
            for field in ["producerDataSet", "consumerDataSet"] {
                let mut actual = encoded[field].clone();
                let fixture_field = match field {
                    "producerDataSet" => "encodedProducerDataSet",
                    "consumerDataSet" => "encodedConsumerDataSet",
                    _ => unreachable!(),
                };
                let mut expected = case[fixture_field].clone();
                sort_arrays(&mut actual);
                sort_arrays(&mut expected);
                assert_eq!(actual, expected, "{} {field} JSON", case["case"]);
            }
            assert_eq!(
                data.java_to_string(),
                case["toString"],
                "{} text",
                case["case"]
            );

            let fingerprint = data.compute_heartbeat_fingerprint();
            match case["fingerprintError"].as_str() {
                None => assert_eq!(
                    fingerprint.unwrap(),
                    case["computedFingerprint"].as_i64().unwrap() as i32,
                    "{} fingerprint",
                    case["case"]
                ),
                Some("NullPointerException") if case["case"] == "consumer-null" => {
                    assert!(matches!(
                        fingerprint,
                        Err(HeartbeatFingerprintError::NullConsumerData)
                    ));
                }
                Some("NullPointerException") => {
                    assert!(matches!(
                        fingerprint,
                        Err(HeartbeatFingerprintError::NullSubscriptionData)
                    ));
                }
                Some(error) => panic!("unexpected Java fingerprint error: {error}"),
            }
        }
    }

    #[test]
    fn heartbeat_alias_inputs_match_invoked_java_property_names() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["heartbeatAliasCases"].as_array().unwrap() {
            let value: HeartbeatData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            assert_eq!(
                value.client_id,
                case["clientID"].as_str().map(str::to_owned),
                "{} clientID",
                case["rawInput"]
            );
            assert_eq!(
                value.is_without_sub, case["withoutSub"],
                "{} withoutSub",
                case["rawInput"]
            );
            let encoded = serde_json::to_value(value).unwrap();
            for (field, key) in [
                ("clientID", "encodedClientID"),
                ("withoutSub", "encodedWithoutSub"),
            ] {
                assert_eq!(
                    encoded
                        .get(field)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                    case[key],
                    "{} encode {field}",
                    case["rawInput"]
                );
            }
        }
    }

    #[test]
    fn primitive_nulls_match_invoked_java_decoder() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["primitiveNullCases"].as_array().unwrap() {
            let raw = case["rawInput"].as_str().unwrap();
            let actual = match case["case"].as_str().unwrap() {
                "subscription-version" => serde_json::from_str::<SubscriptionData>(raw)
                    .map(|value| serde_json::json!(value.sub_version)),
                "subscription-class-filter" => serde_json::from_str::<SubscriptionData>(raw)
                    .map(|value| serde_json::json!(value.class_filter_mode)),
                "heartbeat-fingerprint" => serde_json::from_str::<HeartbeatData>(raw)
                    .map(|value| serde_json::json!(value.heartbeat_fingerprint)),
                "heartbeat-without-sub" => serde_json::from_str::<HeartbeatData>(raw)
                    .map(|value| serde_json::json!(value.is_without_sub)),
                "consumer-unit-mode" => serde_json::from_str::<ConsumerData>(raw)
                    .map(|value| serde_json::json!(value.unit_mode)),
                name => panic!("unknown primitive case: {name}"),
            };
            assert_eq!(actual.unwrap(), case["value"], "{}", case["case"]);
        }
    }

    #[test]
    fn public_heartbeat_fingerprint_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let data: HeartbeatData = serde_json::from_value(case["body"].clone()).unwrap();
            assert_eq!(
                data.compute_heartbeat_fingerprint().unwrap(),
                case["fingerprint"].as_i64().unwrap() as i32,
                "{}",
                case["case"]
            );
        }
    }

    fn current_millis() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    #[test]
    fn consumer_metadata_multiplicity_matches_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/heartbeat.json"))
                .unwrap();
        for case in fixture["multiplicityCases"].as_array().unwrap() {
            let expected = case["body"]["consumerDataSet"].as_array().unwrap();
            let data: HeartbeatData = serde_json::from_value(case["body"].clone()).unwrap();
            assert_eq!(
                data.consumer_data_set.as_ref().unwrap().len(),
                expected.len(),
                "{}",
                case["case"]
            );
            let encoded = serde_json::to_value(data).unwrap();
            assert_eq!(
                encoded["consumerDataSet"].as_array().unwrap().len(),
                expected.len()
            );
        }
    }

    #[test]
    fn heartbeat_json_uses_java_field_names_and_preserves_subscription_data() {
        let mut heartbeat = HeartbeatData {
            client_id: Some("client".into()),
            ..HeartbeatData::default()
        };
        heartbeat
            .consumer_data_set
            .as_mut()
            .unwrap()
            .push(Some(ConsumerData {
                group_name: Some("group".into()),
                consume_from_where: Some(ConsumeFromWhere::ConsumeFromLastOffset),
                subscription_data_set: Some(HashSet::from([Some(SubscriptionData {
                    topic: Some("topic".into()),
                    sub_string: Some("*".into()),
                    sub_version: i64::MAX,
                    ..SubscriptionData::default()
                })])),
                ..ConsumerData::default()
            }));
        let value = serde_json::to_value(&heartbeat).unwrap();
        assert_eq!(value["clientID"], "client");
        assert_eq!(value["withoutSub"], false);
        assert!(value.get("isWithoutSub").is_none());
        assert!(
            serde_json::from_value::<HeartbeatData>(serde_json::json!({
                "withoutSub": true
            }))
            .unwrap()
            .is_without_sub
        );
        assert_eq!(
            value["consumerDataSet"][0]["subscriptionDataSet"][0]["subVersion"],
            i64::MAX
        );
        assert!(
            value["consumerDataSet"][0]["subscriptionDataSet"][0]
                .get("filterClassSource")
                .is_none()
        );
    }
}
