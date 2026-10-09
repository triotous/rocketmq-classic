use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

pub const NAME_VALUE_SEPARATOR: char = '\u{1}';
pub const PROPERTY_SEPARATOR: char = '\u{2}';

pub type MessageProperties = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MessagePropertyError {
    #[error("property key cannot be empty or blank")]
    EmptyKey,
    #[error("user property name is reserved by RocketMQ")]
    ReservedProperty,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Message {
    topic: String,
    body: Vec<u8>,
    flag: i32,
    properties: MessageProperties,
    #[serde(rename = "transactionId", skip_serializing_if = "Option::is_none")]
    transaction_id: Option<String>,
}

impl Message {
    pub fn new(topic: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: topic.into(),
            body: body.into(),
            flag: 0,
            properties: BTreeMap::from([(
                crate::message::constants::PROPERTY_WAIT_STORE_MSG_OK.to_owned(),
                "true".to_owned(),
            )]),
            transaction_id: None,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
    pub fn set_topic(&mut self, topic: impl Into<String>) {
        self.topic = topic.into();
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    pub fn set_body(&mut self, body: impl Into<Vec<u8>>) {
        self.body = body.into();
    }
    pub fn take_body(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.body)
    }
    pub fn flag(&self) -> i32 {
        self.flag
    }
    pub fn properties(&self) -> &MessageProperties {
        &self.properties
    }
    pub fn tags(&self) -> Option<&str> {
        self.properties
            .get(crate::message::constants::PROPERTY_TAGS)
            .map(String::as_str)
    }
    pub fn keys(&self) -> Option<&str> {
        self.properties
            .get(crate::message::constants::PROPERTY_KEYS)
            .map(String::as_str)
    }
    pub fn is_wait_store_msg_ok(&self) -> bool {
        self.properties
            .get(crate::message::constants::PROPERTY_WAIT_STORE_MSG_OK)
            .is_none_or(|value| value.eq_ignore_ascii_case("true"))
    }
    pub fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }
    pub fn set_flag(&mut self, flag: i32) {
        self.flag = flag;
    }
    pub fn set_transaction_id(&mut self, transaction_id: Option<String>) {
        self.transaction_id = transaction_id;
    }
    pub fn set_property(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        self.put_user_property(key, value)
    }

    pub fn put_user_property(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        let key = key.into();
        let value = value.into();
        validate_property(&key, &value)?;
        if crate::message::constants::STRING_HASH_SET.contains(&key.as_str()) {
            return Err(MessagePropertyError::ReservedProperty);
        }
        Ok(self.properties.insert(key, value))
    }

    pub fn set_system_property(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        let key = key.into();
        let value = value.into();
        Ok(self.properties.insert(key, value))
    }

    pub fn set_tags(
        &mut self,
        tags: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        self.set_system_property(crate::message::constants::PROPERTY_TAGS, tags)
    }

    pub fn set_keys(
        &mut self,
        keys: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        self.set_system_property(crate::message::constants::PROPERTY_KEYS, keys)
    }

    pub fn set_wait_store_msg_ok(
        &mut self,
        wait: bool,
    ) -> Result<Option<String>, MessagePropertyError> {
        self.set_system_property(
            crate::message::constants::PROPERTY_WAIT_STORE_MSG_OK,
            wait.to_string(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MessageExt {
    #[serde(flatten)]
    message: Message,
    #[serde(rename = "brokerName", skip_serializing_if = "Option::is_none")]
    broker_name: Option<String>,
    #[serde(rename = "queueId")]
    queue_id: i32,
    #[serde(rename = "storeSize")]
    store_size: i32,
    #[serde(rename = "queueOffset")]
    queue_offset: i64,
    #[serde(rename = "sysFlag")]
    sys_flag: i32,
    #[serde(rename = "bornTimestamp")]
    born_timestamp: i64,
    #[serde(rename = "bornHost", skip_serializing_if = "Option::is_none")]
    born_host: Option<String>,
    #[serde(rename = "storeTimestamp")]
    store_timestamp: i64,
    #[serde(rename = "storeHost", skip_serializing_if = "Option::is_none")]
    store_host: Option<String>,
    #[serde(rename = "msgId", skip_serializing_if = "Option::is_none")]
    message_id: Option<String>,
    #[serde(rename = "commitLogOffset")]
    commit_log_offset: i64,
    #[serde(rename = "bodyCRC")]
    body_crc: i32,
    #[serde(rename = "reconsumeTimes")]
    reconsume_times: i32,
    #[serde(rename = "preparedTransactionOffset")]
    prepared_transaction_offset: i64,
}

impl MessageExt {
    pub fn new(message: Message) -> Self {
        Self {
            message,
            ..Self::default()
        }
    }
    pub fn message(&self) -> &Message {
        &self.message
    }
    pub fn message_mut(&mut self) -> &mut Message {
        &mut self.message
    }
    pub fn broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }
    pub fn set_message(&mut self, message: Message) {
        self.message = message;
    }
    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }
    pub fn queue_offset(&self) -> i64 {
        self.queue_offset
    }
    pub fn commit_log_offset(&self) -> i64 {
        self.commit_log_offset
    }
    pub fn sys_flag(&self) -> i32 {
        self.sys_flag
    }
    pub fn born_timestamp(&self) -> i64 {
        self.born_timestamp
    }
    pub fn store_timestamp(&self) -> i64 {
        self.store_timestamp
    }
    pub fn reconsume_times(&self) -> i32 {
        self.reconsume_times
    }
    pub fn store_size(&self) -> i32 {
        self.store_size
    }
    pub fn body_crc(&self) -> i32 {
        self.body_crc
    }
    pub fn born_host(&self) -> Option<&str> {
        self.born_host.as_deref()
    }
    pub fn store_host(&self) -> Option<&str> {
        self.store_host.as_deref()
    }
    pub fn message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }
    pub fn prepared_transaction_offset(&self) -> i64 {
        self.prepared_transaction_offset
    }
    pub fn set_broker_name(&mut self, value: Option<String>) {
        self.broker_name = value;
    }
    pub fn set_queue_id(&mut self, value: i32) {
        self.queue_id = value;
    }
    pub fn set_queue_offset(&mut self, value: i64) {
        self.queue_offset = value;
    }
    pub fn set_commit_log_offset(&mut self, value: i64) {
        self.commit_log_offset = value;
    }
    pub fn set_sys_flag(&mut self, value: i32) {
        self.sys_flag = value;
    }
    pub fn set_born_timestamp(&mut self, value: i64) {
        self.born_timestamp = value;
    }
    pub fn set_store_timestamp(&mut self, value: i64) {
        self.store_timestamp = value;
    }
    pub fn set_reconsume_times(&mut self, value: i32) {
        self.reconsume_times = value;
    }
    pub fn set_store_size(&mut self, value: i32) {
        self.store_size = value;
    }
    pub fn set_body_crc(&mut self, value: i32) {
        self.body_crc = value;
    }
    pub fn set_born_host(&mut self, value: Option<String>) {
        self.born_host = value;
    }
    pub fn set_store_host(&mut self, value: Option<String>) {
        self.store_host = value;
    }
    pub fn set_message_id(&mut self, value: Option<String>) {
        self.message_id = value;
    }
    pub fn set_prepared_transaction_offset(&mut self, value: i64) {
        self.prepared_transaction_offset = value;
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize)]
pub struct MessageQueue {
    #[serde(
        default,
        deserialize_with = "crate::serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    topic: Option<String>,
    #[serde(
        rename = "brokerName",
        default,
        deserialize_with = "crate::serde_fastjson::deserialize_nullable_string",
        skip_serializing_if = "Option::is_none"
    )]
    broker_name: Option<String>,
    #[serde(
        rename = "queueId",
        default,
        deserialize_with = "crate::serde_fastjson::deserialize_i32"
    )]
    queue_id: i32,
}

#[derive(Deserialize)]
struct MessageQueueFields {
    #[serde(
        default,
        deserialize_with = "crate::serde_fastjson::deserialize_nullable_string"
    )]
    topic: Option<String>,
    #[serde(
        rename = "brokerName",
        default,
        deserialize_with = "crate::serde_fastjson::deserialize_nullable_string"
    )]
    broker_name: Option<String>,
    #[serde(
        rename = "queueId",
        default,
        deserialize_with = "crate::serde_fastjson::deserialize_i32"
    )]
    queue_id: i32,
}

impl<'de> Deserialize<'de> for MessageQueue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields: MessageQueueFields =
            crate::serde_fastjson::deserialize_fastjson_last_wins_object(deserializer)?;
        Ok(Self {
            topic: fields.topic,
            broker_name: fields.broker_name,
            queue_id: fields.queue_id,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MessageQueueCompareError {
    #[error("MessageQueue topic is null")]
    NullTopic,
    #[error("MessageQueue broker name is null")]
    NullBrokerName,
}

impl MessageQueue {
    pub fn new(topic: impl Into<String>, broker_name: impl Into<String>, queue_id: i32) -> Self {
        Self {
            topic: Some(topic.into()),
            broker_name: Some(broker_name.into()),
            queue_id,
        }
    }
    pub fn new_nullable_broker(
        topic: impl Into<String>,
        broker_name: Option<String>,
        queue_id: i32,
    ) -> Self {
        Self {
            topic: Some(topic.into()),
            broker_name,
            queue_id,
        }
    }
    pub fn new_nullable(topic: Option<String>, broker_name: Option<String>, queue_id: i32) -> Self {
        Self {
            topic,
            broker_name,
            queue_id,
        }
    }
    pub fn topic(&self) -> Option<&str> {
        self.topic.as_deref()
    }
    pub fn broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
    }
    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn set_topic(&mut self, topic: Option<String>) {
        self.topic = topic;
    }
    pub fn set_broker_name(&mut self, broker_name: Option<String>) {
        self.broker_name = broker_name;
    }
    pub fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }

    /// Returns the pinned Java `MessageQueue.hashCode` value.
    pub fn java_hash_code(&self) -> i32 {
        let mut result = 31i32.wrapping_add(java_optional_string_hash(&self.broker_name));
        result = 31i32.wrapping_mul(result).wrapping_add(self.queue_id);
        31i32
            .wrapping_mul(result)
            .wrapping_add(java_optional_string_hash(&self.topic))
    }

    /// Returns the exact pinned Java 5.5.1 `MessageQueue.compareTo` result.
    pub fn java_compare_to(&self, other: &Self) -> Result<i32, MessageQueueCompareError> {
        let topic = self
            .topic
            .as_deref()
            .ok_or(MessageQueueCompareError::NullTopic)?;
        let other_topic = other
            .topic
            .as_deref()
            .ok_or(MessageQueueCompareError::NullTopic)?;
        let result = java_string_compare_to(topic, other_topic);
        if result != 0 {
            return Ok(result);
        }
        let broker = self
            .broker_name
            .as_deref()
            .ok_or(MessageQueueCompareError::NullBrokerName)?;
        let other_broker = other
            .broker_name
            .as_deref()
            .ok_or(MessageQueueCompareError::NullBrokerName)?;
        let result = java_string_compare_to(broker, other_broker);
        if result != 0 {
            return Ok(result);
        }
        Ok(match self.queue_id.cmp(&other.queue_id) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        })
    }
}

impl std::fmt::Display for MessageQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MessageQueue [topic={}, brokerName={}, queueId={}]",
            self.topic.as_deref().unwrap_or("null"),
            self.broker_name.as_deref().unwrap_or("null"),
            self.queue_id
        )
    }
}

impl Ord for MessageQueue {
    fn cmp(&self, other: &Self) -> Ordering {
        java_optional_string_cmp(&self.topic, &other.topic)
            .then_with(|| match (&self.broker_name, &other.broker_name) {
                (Some(left), Some(right)) => java_string_cmp(left, right),
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
            })
            .then_with(|| self.queue_id.cmp(&other.queue_id))
    }
}

impl PartialOrd for MessageQueue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn java_string_cmp(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn java_string_compare_to(left: &str, right: &str) -> i32 {
    for (a, b) in left.encode_utf16().zip(right.encode_utf16()) {
        if a != b {
            return i32::from(a) - i32::from(b);
        }
    }
    (left.encode_utf16().count() as i32) - (right.encode_utf16().count() as i32)
}

fn java_optional_string_hash(value: &Option<String>) -> i32 {
    value.as_deref().map_or(0, java_string_hash_code)
}

fn java_string_hash_code(value: &str) -> i32 {
    value.encode_utf16().fold(0i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn java_optional_string_cmp(left: &Option<String>, right: &Option<String>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => java_string_cmp(left, right),
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
    }
}

fn validate_property(key: &str, value: &str) -> Result<(), MessagePropertyError> {
    if key.trim().is_empty() || value.trim().is_empty() {
        return Err(MessagePropertyError::EmptyKey);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_queue_fastjson_inputs_match_invoked_java() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/lookup.json")).unwrap();
        let cases = fixture["messageQueueInputCases"].as_array().unwrap();
        assert_eq!(
            cases.len(),
            19,
            "Java oracle includes scalar, numeric and duplicate-field boundaries"
        );
        for case in cases {
            let raw_input = case["rawInput"].as_str().unwrap();
            let decoded = serde_json::from_str::<MessageQueue>(raw_input);
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
    fn java_queue_order_uses_utf16_code_units() {
        let supplementary = MessageQueue::new("\u{10000}", "b", 0);
        let bmp = MessageQueue::new("\u{e000}", "b", 0);
        assert!(supplementary < bmp);
    }

    #[test]
    fn queue_equality_includes_all_java_fields_and_signed_id() {
        let a = MessageQueue::new("t", "b", i32::MIN);
        let b = MessageQueue::new("t", "b", i32::MIN);
        assert_eq!(a, b);
        assert_eq!(a.queue_id(), i32::MIN);
        assert_ne!(a, MessageQueue::new("t", "b", i32::MAX));
    }

    #[test]
    fn message_queue_setters_preserve_nullable_java_fields() {
        let mut queue = MessageQueue::new("topic", "broker", 1);
        queue.set_topic(None);
        queue.set_broker_name(None);
        queue.set_queue_id(i32::MIN);
        assert_eq!(queue.topic(), None);
        assert_eq!(queue.broker_name(), None);
        assert_eq!(queue.queue_id(), i32::MIN);
        queue.set_topic(Some("next".into()));
        queue.set_broker_name(Some("new-broker".into()));
        queue.set_queue_id(-17);
        assert_eq!(queue.topic(), Some("next"));
        assert_eq!(queue.broker_name(), Some("new-broker"));
        assert_eq!(queue.queue_id(), -17);
    }

    #[test]
    fn message_has_separate_user_and_broker_metadata() {
        let mut message = Message::new("topic", [0, 1, 255]);
        message.set_flag(-1);
        message.set_transaction_id(Some("tx".into()));
        assert!(message.is_wait_store_msg_ok());
        message.set_tags("tag").unwrap();
        assert_eq!(message.tags(), Some("tag"));
        let mut ext = MessageExt::new(message.clone());
        ext.set_queue_offset(i64::MIN);
        ext.set_commit_log_offset(i64::MAX);
        assert_eq!(ext.message(), &message);
        assert_eq!(ext.queue_offset(), i64::MIN);
        assert_eq!(ext.commit_log_offset(), i64::MAX);
    }

    #[test]
    fn java_property_setters_accept_wire_separators() {
        let mut message = Message::new("topic", vec![]);
        message.set_property("key", "bad\u{2}value").unwrap();
        assert_eq!(message.properties().get("key").unwrap(), "bad\u{2}value");
        assert_eq!(
            message.set_property("", "value"),
            Err(MessagePropertyError::EmptyKey)
        );
        assert_eq!(
            message.set_property("KEYS", "user-value"),
            Err(MessagePropertyError::ReservedProperty)
        );
        assert_eq!(
            message.set_property("custom", "  "),
            Err(MessagePropertyError::EmptyKey)
        );
    }
}
