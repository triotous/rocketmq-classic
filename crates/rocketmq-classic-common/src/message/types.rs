use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use super::host_address::{
    MessageHostAddress, MessageHostAddressBytesError, parse_java_i32, parse_java_i64,
};

pub const NAME_VALUE_SEPARATOR: char = '\u{1}';
pub const PROPERTY_SEPARATOR: char = '\u{2}';

pub type MessageProperties = BTreeMap<String, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicFilterType {
    SingleTag,
    MultiTag,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MessagePropertyError {
    #[error("property key cannot be empty or blank")]
    EmptyKey,
    #[error("property value cannot be empty or blank")]
    EmptyValue,
    #[error("user property name is reserved by RocketMQ")]
    ReservedProperty,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid Java integer in message property {property}: {value:?}")]
pub struct MessagePropertyParseError {
    property: &'static str,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid Java integer value: {value:?}")]
pub struct MessageSysFlagParseError {
    value: String,
}

#[derive(Debug)]
pub struct Message {
    topic: Option<String>,
    body: Option<Vec<u8>>,
    flag: i32,
    properties: Option<MessageProperties>,
    null_properties: BTreeSet<String>,
    properties_read_initialized: AtomicBool,
    transaction_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct MessageSerde {
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    body: Option<Vec<u8>>,
    #[serde(default)]
    flag: i32,
    #[serde(default)]
    properties: Option<BTreeMap<String, Option<String>>>,
    #[serde(
        default,
        rename = "transactionId",
        skip_serializing_if = "Option::is_none"
    )]
    transaction_id: Option<String>,
}

impl Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let properties = self.get_properties().map(|properties| {
            let mut nullable = properties
                .iter()
                .map(|(key, value)| (key.clone(), Some(value.clone())))
                .collect::<BTreeMap<_, _>>();
            for key in &self.null_properties {
                nullable.entry(key.clone()).or_insert(None);
            }
            nullable
        });
        MessageSerde {
            topic: self.topic.clone(),
            body: self.body.clone(),
            flag: self.flag,
            properties,
            transaction_id: self.transaction_id.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = MessageSerde::deserialize(deserializer)?;
        let (properties, null_properties) = match wire.properties {
            Some(values) => {
                let mut properties = BTreeMap::new();
                let mut null_properties = BTreeSet::new();
                for (key, value) in values {
                    if let Some(value) = value {
                        properties.insert(key, value);
                    } else {
                        null_properties.insert(key);
                    }
                }
                (Some(properties), null_properties)
            }
            None => (None, BTreeSet::new()),
        };
        Ok(Self {
            topic: wire.topic,
            body: wire.body,
            flag: wire.flag,
            properties,
            null_properties,
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: wire.transaction_id,
        })
    }
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            body: self.body.clone(),
            flag: self.flag,
            properties: self.properties.clone(),
            null_properties: self.null_properties.clone(),
            properties_read_initialized: AtomicBool::new(
                self.properties_read_initialized
                    .load(AtomicOrdering::Relaxed),
            ),
            transaction_id: self.transaction_id.clone(),
        }
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic
            && self.body == other.body
            && self.flag == other.flag
            && self.properties_nullable() == other.properties_nullable()
            && self.null_properties == other.null_properties
            && self.transaction_id == other.transaction_id
    }
}

impl Eq for Message {}

impl Default for Message {
    fn default() -> Self {
        Self {
            topic: None,
            body: None,
            flag: 0,
            properties: None,
            null_properties: BTreeSet::new(),
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: None,
        }
    }
}

fn empty_message_properties() -> &'static MessageProperties {
    static EMPTY: OnceLock<MessageProperties> = OnceLock::new();
    EMPTY.get_or_init(MessageProperties::new)
}

impl Message {
    pub(crate) fn from_decoded(
        topic: String,
        body: Vec<u8>,
        properties: MessageProperties,
    ) -> Self {
        Self {
            topic: Some(topic),
            body: Some(body),
            flag: 0,
            properties: Some(properties),
            null_properties: BTreeSet::new(),
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: None,
        }
    }

    pub fn new(topic: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: Some(topic.into()),
            body: Some(body.into()),
            flag: 0,
            properties: Some(BTreeMap::from([(
                crate::message::constants::PROPERTY_WAIT_STORE_MSG_OK.to_owned(),
                "true".to_owned(),
            )])),
            null_properties: BTreeSet::new(),
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: None,
        }
    }

    /// Java's two-argument constructor, preserving null topic and body values.
    pub fn new_nullable(topic: Option<String>, body: Option<Vec<u8>>) -> Self {
        Self::with_nullable_fields(topic, None, None, 0, body, true)
    }

    /// Java's six-argument `Message(topic, tags, keys, flag, body, waitStoreMsgOK)`
    /// constructor, with Rust-owned values and optional tag/key properties.
    pub fn with_fields(
        topic: impl Into<String>,
        tags: Option<&str>,
        keys: Option<&str>,
        flag: i32,
        body: impl Into<Vec<u8>>,
        wait_store_msg_ok: bool,
    ) -> Self {
        Self::with_nullable_fields(
            Some(topic.into()),
            tags.map(str::to_owned),
            keys.map(str::to_owned),
            flag,
            Some(body.into()),
            wait_store_msg_ok,
        )
    }

    /// Java's six-argument constructor, including nullable topic/body inputs.
    pub fn with_nullable_fields(
        topic: Option<String>,
        tags: Option<String>,
        keys: Option<String>,
        flag: i32,
        body: Option<Vec<u8>>,
        wait_store_msg_ok: bool,
    ) -> Self {
        let mut message = Self {
            topic,
            body,
            flag,
            ..Self::default()
        };
        if let Some(tags) = tags.filter(|tags| !tags.is_empty()) {
            message.put_property_value(crate::message::constants::PROPERTY_TAGS, Some(tags));
        }
        if let Some(keys) = keys.filter(|keys| !keys.is_empty()) {
            message.put_property_value(crate::message::constants::PROPERTY_KEYS, Some(keys));
        }
        message.put_property_value(
            crate::message::constants::PROPERTY_WAIT_STORE_MSG_OK,
            Some(wait_store_msg_ok.to_string()),
        );
        message
    }

    pub fn topic(&self) -> &str {
        self.topic.as_deref().unwrap_or_default()
    }
    pub fn topic_nullable(&self) -> Option<&str> {
        self.topic.as_deref()
    }
    pub fn set_topic(&mut self, topic: impl Into<String>) {
        self.topic = Some(topic.into());
    }
    pub fn set_topic_nullable(&mut self, topic: Option<String>) {
        self.topic = topic;
    }
    pub fn body(&self) -> &[u8] {
        self.body.as_deref().unwrap_or_default()
    }
    pub fn body_nullable(&self) -> Option<&[u8]> {
        self.body.as_deref()
    }
    pub fn set_body(&mut self, body: impl Into<Vec<u8>>) {
        self.body = Some(body.into());
    }
    pub fn set_body_nullable(&mut self, body: Option<Vec<u8>>) {
        self.body = body;
    }
    pub fn take_body(&mut self) -> Vec<u8> {
        self.body.take().unwrap_or_default()
    }
    pub fn take_body_nullable(&mut self) -> Option<Vec<u8>> {
        self.body.take()
    }
    pub fn flag(&self) -> i32 {
        self.flag
    }
    pub fn properties(&self) -> &MessageProperties {
        self.properties
            .as_ref()
            .unwrap_or_else(|| empty_message_properties())
    }
    pub fn properties_nullable(&self) -> Option<&MessageProperties> {
        self.properties.as_ref().or_else(|| {
            self.properties_read_initialized
                .load(AtomicOrdering::Relaxed)
                .then(empty_message_properties)
        })
    }
    pub fn get_properties(&self) -> Option<&MessageProperties> {
        self.properties_nullable()
    }
    pub fn properties_mut(&mut self) -> &mut MessageProperties {
        self.properties.get_or_insert_with(MessageProperties::new)
    }
    pub fn get_properties_mut(&mut self) -> Option<&mut MessageProperties> {
        if self.properties.is_none()
            && !self
                .properties_read_initialized
                .swap(false, AtomicOrdering::Relaxed)
        {
            return None;
        }
        Some(self.properties.get_or_insert_with(MessageProperties::new))
    }
    pub fn get_property(&self, key: &str) -> Option<&str> {
        if self.properties.is_none() {
            self.properties_read_initialized
                .store(true, AtomicOrdering::Relaxed);
        }
        self.properties
            .as_ref()
            .and_then(|properties| properties.get(key))
            .map(String::as_str)
    }
    pub fn get_nullable_property(&self, key: Option<&str>) -> Option<&str> {
        key.and_then(|key| self.get_property(key))
    }
    pub fn get_user_property(&self, key: &str) -> Option<&str> {
        self.get_property(key)
    }
    pub fn has_property(&self, key: &str) -> bool {
        self.properties
            .as_ref()
            .is_some_and(|properties| properties.contains_key(key))
            || self.null_properties.contains(key)
    }
    pub fn has_nullable_property(&self, key: Option<&str>) -> bool {
        key.is_some_and(|key| self.has_property(key))
    }
    pub fn tags(&self) -> Option<&str> {
        self.get_property(crate::message::constants::PROPERTY_TAGS)
    }
    pub fn keys(&self) -> Option<&str> {
        self.get_property(crate::message::constants::PROPERTY_KEYS)
    }
    pub fn is_wait_store_msg_ok(&self) -> bool {
        self.get_property(crate::message::constants::PROPERTY_WAIT_STORE_MSG_OK)
            .is_none_or(|value| value.eq_ignore_ascii_case("true"))
    }
    pub fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }
    pub fn set_flag(&mut self, flag: i32) {
        self.flag = flag;
    }
    pub fn buyer_id(&self) -> Option<&str> {
        self.get_property(crate::message::constants::PROPERTY_BUYER_ID)
    }
    pub fn set_buyer_id(&mut self, buyer_id: impl Into<String>) {
        self.put_property_value(
            crate::message::constants::PROPERTY_BUYER_ID,
            Some(buyer_id.into()),
        );
    }
    pub fn set_buyer_id_nullable(&mut self, buyer_id: Option<String>) {
        self.put_property_value(crate::message::constants::PROPERTY_BUYER_ID, buyer_id);
    }
    pub fn set_instance_id(&mut self, instance_id: impl Into<String>) {
        self.put_property_value(
            crate::message::constants::PROPERTY_INSTANCE_ID,
            Some(instance_id.into()),
        );
    }
    pub fn set_instance_id_nullable(&mut self, instance_id: Option<String>) {
        self.put_property_value(crate::message::constants::PROPERTY_INSTANCE_ID, instance_id);
    }
    pub fn delay_time_level(&self) -> Result<i32, MessagePropertyParseError> {
        self.parse_i32_property(crate::message::constants::PROPERTY_DELAY_TIME_LEVEL)
    }
    pub fn set_delay_time_level(&mut self, level: i32) {
        self.put_property_value(
            crate::message::constants::PROPERTY_DELAY_TIME_LEVEL,
            Some(level.to_string()),
        );
    }
    pub fn delay_time_sec(&self) -> Result<i64, MessagePropertyParseError> {
        self.parse_i64_property(crate::message::constants::PROPERTY_TIMER_DELAY_SEC)
    }
    pub fn set_delay_time_sec(&mut self, seconds: i64) {
        self.put_property_value(
            crate::message::constants::PROPERTY_TIMER_DELAY_SEC,
            Some(seconds.to_string()),
        );
    }
    pub fn delay_time_ms(&self) -> Result<i64, MessagePropertyParseError> {
        self.parse_i64_property(crate::message::constants::PROPERTY_TIMER_DELAY_MS)
    }
    pub fn set_delay_time_ms(&mut self, milliseconds: i64) {
        self.put_property_value(
            crate::message::constants::PROPERTY_TIMER_DELAY_MS,
            Some(milliseconds.to_string()),
        );
    }
    pub fn deliver_time_ms(&self) -> Result<i64, MessagePropertyParseError> {
        self.parse_i64_property(crate::message::constants::PROPERTY_TIMER_DELIVER_MS)
    }
    pub fn set_deliver_time_ms(&mut self, milliseconds: i64) {
        self.put_property_value(
            crate::message::constants::PROPERTY_TIMER_DELIVER_MS,
            Some(milliseconds.to_string()),
        );
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

    pub fn put_user_property_nullable(
        &mut self,
        key: Option<String>,
        value: Option<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        if key
            .as_deref()
            .is_some_and(|key| crate::message::constants::STRING_HASH_SET.contains(&key))
        {
            return Err(MessagePropertyError::ReservedProperty);
        }
        let key = key
            .filter(|key| !key.trim().is_empty())
            .ok_or(MessagePropertyError::EmptyKey)?;
        let value = value
            .filter(|value| !value.trim().is_empty())
            .ok_or(MessagePropertyError::EmptyValue)?;
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
        Ok(self.put_property_value(key, Some(value)))
    }

    pub fn set_system_property(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        let key = key.into();
        let value = value.into();
        Ok(self.put_property_value(key, Some(value)))
    }

    /// Stores a system property whose Java `HashMap` value may be null.
    pub fn set_system_property_nullable(&mut self, key: impl Into<String>, value: Option<String>) {
        self.put_property_value(key, value);
    }

    /// Removes a system property, corresponding to Java `MessageAccessor.clearProperty`.
    pub fn clear_system_property(&mut self, key: &str) -> Option<String> {
        self.null_properties.remove(key);
        self.properties
            .as_mut()
            .and_then(|properties| properties.remove(key))
    }

    pub fn set_tags(
        &mut self,
        tags: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        self.set_system_property(crate::message::constants::PROPERTY_TAGS, tags)
    }
    pub fn set_tags_nullable(&mut self, tags: Option<String>) {
        self.put_property_value(crate::message::constants::PROPERTY_TAGS, tags);
    }

    pub fn set_keys(
        &mut self,
        keys: impl Into<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        self.set_system_property(crate::message::constants::PROPERTY_KEYS, keys)
    }
    pub fn set_keys_nullable(&mut self, keys: Option<String>) {
        self.put_property_value(crate::message::constants::PROPERTY_KEYS, keys);
    }

    /// Java's `setKeys(Collection<String>)` overload; collection order is kept.
    pub fn set_keys_from<I, S>(&mut self, keys: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let joined = keys
            .into_iter()
            .map(|key| key.as_ref().to_owned())
            .collect::<Vec<_>>()
            .join(crate::message::constants::KEY_SEPARATOR);
        self.put_property_value(crate::message::constants::PROPERTY_KEYS, Some(joined));
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

    fn parse_i32_property(&self, property: &'static str) -> Result<i32, MessagePropertyParseError> {
        let Some(value) = self.get_property(property) else {
            return Ok(0);
        };
        parse_java_i32(value).ok_or_else(|| MessagePropertyParseError {
            property,
            value: value.to_owned(),
        })
    }

    fn parse_i64_property(&self, property: &'static str) -> Result<i64, MessagePropertyParseError> {
        let Some(value) = self.get_property(property) else {
            return Ok(0);
        };
        parse_java_i64(value).ok_or_else(|| MessagePropertyParseError {
            property,
            value: value.to_owned(),
        })
    }

    fn put_property_value(
        &mut self,
        key: impl Into<String>,
        value: Option<String>,
    ) -> Option<String> {
        let key = key.into();
        self.properties_read_initialized
            .store(false, AtomicOrdering::Relaxed);
        match value {
            Some(value) => {
                self.null_properties.remove(&key);
                self.properties
                    .get_or_insert_with(MessageProperties::new)
                    .insert(key, value)
            }
            None => {
                let previous = self
                    .properties
                    .get_or_insert_with(MessageProperties::new)
                    .remove(&key);
                self.null_properties.insert(key);
                previous
            }
        }
    }

    /// Java-style diagnostic representation. Java's `HashMap` property order
    /// is unspecified; Rust emits the `BTreeMap` order for reproducibility.
    pub fn java_to_string(&self) -> String {
        let properties = if self.properties_nullable().is_none() {
            "null".to_owned()
        } else {
            let mut entries = self
                .properties()
                .iter()
                .map(|(key, value)| (key.as_str(), Some(value.as_str())))
                .collect::<Vec<_>>();
            entries.extend(self.null_properties.iter().filter_map(|key| {
                (!self.properties().contains_key(key)).then_some((key.as_str(), None))
            }));
            entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
            format!(
                "{{{}}}",
                entries
                    .into_iter()
                    .map(|(key, value)| format!("{key}={}", value.unwrap_or("null")))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let body = self
            .body
            .as_ref()
            .map(|body| {
                format!(
                    "[{}]",
                    body.iter()
                        .map(|byte| (*byte as i8).to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .unwrap_or_else(|| "null".to_owned());
        let transaction_id = self.transaction_id.as_deref().unwrap_or("null");
        format!(
            "Message{{topic='{}', flag={}, properties={}, body={}, transactionId='{}'}}",
            self.topic.as_deref().unwrap_or("null"),
            self.flag,
            properties,
            body,
            transaction_id
        )
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.java_to_string())
    }
}

impl Deref for MessageExt {
    type Target = Message;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl DerefMut for MessageExt {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.message
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
    born_host: Option<MessageHostAddress>,
    #[serde(rename = "storeTimestamp")]
    store_timestamp: i64,
    #[serde(rename = "storeHost", skip_serializing_if = "Option::is_none")]
    store_host: Option<MessageHostAddress>,
    #[serde(rename = "msgId", skip_serializing_if = "Option::is_none")]
    message_id: Option<String>,
    #[serde(rename = "offsetMsgId", skip_serializing_if = "Option::is_none")]
    offset_message_id: Option<String>,
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
    /// Java `MessageExt.parseTopicFilterType(int)`.
    pub fn parse_topic_filter_type(sys_flag: i32) -> TopicFilterType {
        if sys_flag & crate::sysflag::message::MULTI_TAGS_FLAG
            == crate::sysflag::message::MULTI_TAGS_FLAG
        {
            TopicFilterType::MultiTag
        } else {
            TopicFilterType::SingleTag
        }
    }
    /// Constructor equivalent to Java's
    /// `MessageExt(int, long, SocketAddress, long, SocketAddress, String)`.
    pub fn with_socket_addresses(
        queue_id: i32,
        born_timestamp: i64,
        born_host: Option<MessageHostAddress>,
        store_timestamp: i64,
        store_host: Option<MessageHostAddress>,
        message_id: Option<String>,
    ) -> Self {
        Self {
            queue_id,
            born_timestamp,
            born_host,
            store_timestamp,
            store_host,
            message_id,
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
    /// Java `MessageExt.getBornHost()`, represented as a resolved or unresolved
    /// Rust `MessageHostAddress` value.
    pub fn born_host(&self) -> Option<&MessageHostAddress> {
        self.born_host.as_ref()
    }
    /// Java `MessageExt.getStoreHost()`, represented as a resolved or unresolved
    /// Rust `MessageHostAddress` value.
    pub fn store_host(&self) -> Option<&MessageHostAddress> {
        self.store_host.as_ref()
    }
    /// Java `MessageExt.getBornHostString()`; unresolved hosts return `None`.
    pub fn born_host_string(&self) -> Option<String> {
        self.born_host
            .as_ref()
            .and_then(MessageHostAddress::host_address_string)
    }
    /// Java `MessageExt.getBornHostNameString()` without reverse DNS lookup.
    pub fn born_host_name_string(&self) -> Option<&str> {
        self.born_host.as_ref().map(MessageHostAddress::host_name)
    }
    /// Java `MessageExt.getBornHostBytes()` in network byte order.
    pub fn born_host_bytes(&self) -> Result<Vec<u8>, MessageHostAddressBytesError> {
        self.born_host
            .as_ref()
            .ok_or(MessageHostAddressBytesError::MissingAddress)?
            .network_bytes()
            .ok_or(MessageHostAddressBytesError::UnresolvedAddress)
    }
    /// Java `MessageExt.getStoreHostBytes()` in network byte order.
    pub fn store_host_bytes(&self) -> Result<Vec<u8>, MessageHostAddressBytesError> {
        self.store_host
            .as_ref()
            .ok_or(MessageHostAddressBytesError::MissingAddress)?
            .network_bytes()
            .ok_or(MessageHostAddressBytesError::UnresolvedAddress)
    }
    /// Fill a Rust byte slice with the Java `getBornHostBytes(ByteBuffer)` layout.
    pub fn write_born_host_bytes(
        &self,
        output: &mut [u8],
    ) -> Result<usize, MessageHostAddressBytesError> {
        self.born_host
            .as_ref()
            .ok_or(MessageHostAddressBytesError::MissingAddress)?
            .write_network_bytes(output)
    }
    /// Fill a Rust byte slice with the Java `getStoreHostBytes(ByteBuffer)` layout.
    pub fn write_store_host_bytes(
        &self,
        output: &mut [u8],
    ) -> Result<usize, MessageHostAddressBytesError> {
        self.store_host
            .as_ref()
            .ok_or(MessageHostAddressBytesError::MissingAddress)?
            .write_network_bytes(output)
    }
    pub fn message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }
    pub fn offset_message_id(&self) -> Option<&str> {
        self.offset_message_id.as_deref()
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
    /// Set the Java `BORNHOST_V6_FLAG` bit without changing other system flags.
    pub fn set_born_host_v6_flag(&mut self) {
        self.sys_flag |= crate::sysflag::message::BORNHOST_V6_FLAG;
    }
    /// Set the Java `STOREHOSTADDRESS_V6_FLAG` bit without changing other flags.
    pub fn set_store_host_address_v6_flag(&mut self) {
        self.sys_flag |= crate::sysflag::message::STOREHOSTADDRESS_V6_FLAG;
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
    /// Java `MessageExt.setBornHost(SocketAddress)` using Rust value semantics.
    pub fn set_born_host(&mut self, value: Option<MessageHostAddress>) {
        self.born_host = value;
    }
    /// Java `MessageExt.setStoreHost(SocketAddress)` using Rust value semantics.
    pub fn set_store_host(&mut self, value: Option<MessageHostAddress>) {
        self.store_host = value;
    }
    pub fn set_message_id(&mut self, value: Option<String>) {
        self.message_id = value;
    }
    pub fn set_offset_message_id(&mut self, value: Option<String>) {
        self.offset_message_id = value;
    }
    pub fn set_prepared_transaction_offset(&mut self, value: i64) {
        self.prepared_transaction_offset = value;
    }
    /// Read Java's transient topic system flag property. An empty property is
    /// treated as absent; malformed decimal input is returned as a parse error.
    pub fn topic_sys_flag(&self) -> Result<Option<i32>, MessageSysFlagParseError> {
        parse_transient_sys_flag(
            self.message
                .get_property(crate::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG),
        )
    }
    /// Set or clear Java's transient topic system flag property.
    pub fn set_topic_sys_flag(&mut self, value: Option<i32>) {
        set_transient_sys_flag(
            &mut self.message,
            crate::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG,
            value,
        );
    }
    /// Read Java's transient group system flag property. An empty property is
    /// treated as absent; malformed decimal input is returned as a parse error.
    pub fn group_sys_flag(&self) -> Result<Option<i32>, MessageSysFlagParseError> {
        parse_transient_sys_flag(
            self.message
                .get_property(crate::message::constants::PROPERTY_TRANSIENT_GROUP_CONFIG),
        )
    }
    /// Set or clear Java's transient group system flag property.
    pub fn set_group_sys_flag(&mut self, value: Option<i32>) {
        set_transient_sys_flag(
            &mut self.message,
            crate::message::constants::PROPERTY_TRANSIENT_GROUP_CONFIG,
            value,
        );
    }

    /// Java-style diagnostic string. Property entries are emitted in Rust's
    /// deterministic map order; Java's backing `HashMap` does not promise an
    /// iteration order when multiple properties are present.
    pub fn java_to_string(&self) -> String {
        let born_host = self
            .born_host
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "null".to_owned());
        let store_host = self
            .store_host
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| "null".to_owned());
        let message_id = self.message_id.as_deref().unwrap_or("null");
        let broker_name = self.broker_name.as_deref().unwrap_or("null");
        format!(
            "MessageExt [brokerName={broker_name}, queueId={}, storeSize={}, queueOffset={}, sysFlag={}, bornTimestamp={}, bornHost={born_host}, storeTimestamp={}, storeHost={store_host}, msgId={message_id}, commitLogOffset={}, bodyCRC={}, reconsumeTimes={}, preparedTransactionOffset={}, toString()={}]",
            self.queue_id,
            self.store_size,
            self.queue_offset,
            self.sys_flag,
            self.born_timestamp,
            self.store_timestamp,
            self.commit_log_offset,
            self.body_crc,
            self.reconsume_times,
            self.prepared_transaction_offset,
            self.message.java_to_string(),
        )
    }
}

impl std::fmt::Display for MessageExt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.java_to_string())
    }
}

fn parse_transient_sys_flag(value: Option<&str>) -> Result<Option<i32>, MessageSysFlagParseError> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    parse_java_i32(value)
        .map(Some)
        .ok_or_else(|| MessageSysFlagParseError {
            value: value.to_owned(),
        })
}

fn set_transient_sys_flag(message: &mut Message, key: &str, value: Option<i32>) {
    match value {
        Some(value) => {
            message.put_property_value(key, Some(value.to_string()));
        }
        None => {
            message.clear_system_property(key);
        }
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
    if key.trim().is_empty() {
        return Err(MessagePropertyError::EmptyKey);
    }
    if value.trim().is_empty() {
        return Err(MessagePropertyError::EmptyValue);
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
            Err(MessagePropertyError::EmptyValue)
        );
    }
}
