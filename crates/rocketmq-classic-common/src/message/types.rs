use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use super::host_address::{
    MessageHostAddress, MessageHostAddressBytesError, parse_java_i32, parse_java_i64,
};

pub const NAME_VALUE_SEPARATOR: char = '\u{1}';
pub const PROPERTY_SEPARATOR: char = '\u{2}';

pub type MessageProperties = BTreeMap<String, String>;
/// Java-style property map capable of representing null keys and values.
pub type NullableMessageProperties = BTreeMap<Option<String>, Option<String>>;

#[derive(Debug, Clone, Default)]
struct MessagePropertyStorage {
    values: MessageProperties,
    null_values: BTreeSet<String>,
    null_key_value: Option<Option<String>>,
}

struct MessageBodyReadView<'a>(Option<RwLockReadGuard<'a, Vec<u8>>>);

impl Deref for MessageBodyReadView<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().map(|body| &body[..]).unwrap_or_default()
    }
}

impl AsRef<[u8]> for MessageBodyReadView<'_> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::fmt::Debug for MessageBodyReadView<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, formatter)
    }
}

impl PartialEq for MessageBodyReadView<'_> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for MessageBodyReadView<'_> {
    fn eq(&self, other: &&[u8; N]) -> bool {
        **self == other[..]
    }
}

impl PartialEq<Vec<u8>> for MessageBodyReadView<'_> {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_ref() == other.as_slice()
    }
}

impl PartialEq<&[u8]> for MessageBodyReadView<'_> {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_ref() == *other
    }
}

struct MessageBodyWriteView<'a>(RwLockWriteGuard<'a, Vec<u8>>);

impl Deref for MessageBodyWriteView<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MessageBodyWriteView<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

enum MessagePropertiesReadSource<'a> {
    Shared(RwLockReadGuard<'a, MessagePropertyStorage>),
    Empty,
}

struct MessagePropertiesReadView<'a>(MessagePropertiesReadSource<'a>);

impl Deref for MessagePropertiesReadView<'_> {
    type Target = MessageProperties;

    fn deref(&self) -> &Self::Target {
        match &self.0 {
            MessagePropertiesReadSource::Shared(properties) => &properties.values,
            MessagePropertiesReadSource::Empty => empty_message_properties(),
        }
    }
}

impl std::fmt::Debug for MessagePropertiesReadView<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, formatter)
    }
}

impl PartialEq for MessagePropertiesReadView<'_> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Serialize for MessagePropertiesReadView<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (**self).serialize(serializer)
    }
}

struct MessagePropertiesWriteView<'a>(RwLockWriteGuard<'a, MessagePropertyStorage>);

impl Deref for MessagePropertiesWriteView<'_> {
    type Target = MessageProperties;

    fn deref(&self) -> &Self::Target {
        &self.0.values
    }
}

impl DerefMut for MessagePropertiesWriteView<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.values
    }
}

fn read_message_state<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_message_state<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

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
    #[error("The priority must be greater than or equal to 0")]
    NegativePriority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MessageKeyCollectionError {
    #[error("Java Message.setKeys(Collection) rejects a null collection")]
    NullCollection,
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

/// RocketMQ message value corresponding to Java's `Message` API.
///
/// ```compile_fail
/// use rocketmq_classic_common::message::Message;
/// let mut message = Message::default();
/// let _body = message.take_body();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::Message;
/// let mut message = Message::default();
/// let _body = message.take_body_nullable();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::Message;
/// let mut message = Message::default();
/// message.set_property("key", "value");
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::Message;
/// let mut message = Message::default();
/// message.properties_mut();
/// ```
///
/// Java `Message.toString()` is represented by Rust's `Display`/`ToString`;
/// its formatter helper is not a separate public method:
///
/// ```compile_fail
/// use rocketmq_classic_common::message::Message;
/// let _ = Message::default().java_to_string();
/// ```
///
/// Java `Message` inherits identity-based `Object.equals`/`hashCode`; Rust
/// does not provide structural `PartialEq`/`Eq` for this type:
///
/// ```compile_fail
/// use rocketmq_classic_common::message::Message;
/// assert_eq!(Message::default(), Message::default());
/// ```
#[derive(Debug)]
pub struct Message {
    topic: Option<String>,
    body: Option<Arc<RwLock<Vec<u8>>>>,
    flag: i32,
    properties: Option<Arc<RwLock<MessagePropertyStorage>>>,
    properties_read_initialized: AtomicBool,
    transaction_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct NullablePropertyValue {
    value: Option<String>,
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
        rename = "nullKeyProperty",
        skip_serializing_if = "Option::is_none"
    )]
    null_key_property: Option<NullablePropertyValue>,
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
        let property_snapshot = match self.properties.as_ref() {
            Some(properties) => {
                let properties = read_message_state(properties);
                let mut nullable = properties
                    .values
                    .iter()
                    .map(|(key, value)| (key.clone(), Some(value.clone())))
                    .collect::<BTreeMap<_, _>>();
                for key in &properties.null_values {
                    nullable.entry(key.clone()).or_insert(None);
                }
                Some(nullable)
            }
            None if self
                .properties_read_initialized
                .load(AtomicOrdering::Relaxed) =>
            {
                Some(BTreeMap::new())
            }
            None => None,
        };
        MessageSerde {
            topic: self.topic.clone(),
            body: self
                .body
                .as_ref()
                .map(|body| read_message_state(body).clone()),
            flag: self.flag,
            properties: property_snapshot,
            null_key_property: self
                .properties
                .as_ref()
                .and_then(|properties| read_message_state(properties).null_key_value.clone())
                .clone()
                .map(|value| NullablePropertyValue { value }),
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
        let properties = match wire.properties {
            Some(values) => {
                let mut properties = BTreeMap::new();
                let mut null_values = BTreeSet::new();
                for (key, value) in values {
                    if let Some(value) = value {
                        properties.insert(key, value);
                    } else {
                        null_values.insert(key);
                    }
                }
                Some(Arc::new(RwLock::new(MessagePropertyStorage {
                    values: properties,
                    null_values,
                    null_key_value: None,
                })))
            }
            None => None,
        };
        if let (Some(properties), Some(null_key_property)) = (
            &properties,
            wire.null_key_property.map(|property| property.value),
        ) {
            write_message_state(properties).null_key_value = Some(null_key_property);
        }
        Ok(Self {
            topic: wire.topic,
            body: wire.body.map(|body| Arc::new(RwLock::new(body))),
            flag: wire.flag,
            properties,
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: wire.transaction_id,
        })
    }
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            body: self
                .body
                .as_ref()
                .map(|body| Arc::new(RwLock::new(read_message_state(body).clone()))),
            flag: self.flag,
            properties: self
                .properties
                .as_ref()
                .map(|properties| Arc::new(RwLock::new(read_message_state(properties).clone()))),
            properties_read_initialized: AtomicBool::new(
                self.properties_read_initialized
                    .load(AtomicOrdering::Relaxed),
            ),
            transaction_id: self.transaction_id.clone(),
        }
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            topic: None,
            body: None,
            flag: 0,
            properties: None,
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
            body: Some(Arc::new(RwLock::new(body))),
            flag: 0,
            properties: Some(Arc::new(RwLock::new(MessagePropertyStorage {
                values: properties,
                ..MessagePropertyStorage::default()
            }))),
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: None,
        }
    }

    pub(crate) fn from_compact(body: Vec<u8>, flag: i32, properties: MessageProperties) -> Self {
        Self {
            topic: None,
            body: Some(Arc::new(RwLock::new(body))),
            flag,
            properties: Some(Arc::new(RwLock::new(MessagePropertyStorage {
                values: properties,
                ..MessagePropertyStorage::default()
            }))),
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: None,
        }
    }

    pub fn new(topic: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: Some(topic.into()),
            body: Some(Arc::new(RwLock::new(body.into()))),
            flag: 0,
            properties: Some(Arc::new(RwLock::new(MessagePropertyStorage {
                values: BTreeMap::from([(
                    crate::message::constants::PROPERTY_WAIT_STORE_MSG_OK.to_owned(),
                    "true".to_owned(),
                )]),
                ..MessagePropertyStorage::default()
            }))),
            properties_read_initialized: AtomicBool::new(false),
            transaction_id: None,
        }
    }

    /// Creates the `MessageAccessor.cloneMessage` projection. Body bytes and
    /// the property map are shared with the source until either message
    /// replaces that field; the transaction ID is left unset.
    pub fn clone_message(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            body: self.body.clone(),
            flag: self.flag,
            properties: self.properties.clone(),
            properties_read_initialized: AtomicBool::new(
                self.properties_read_initialized
                    .load(AtomicOrdering::Relaxed),
            ),
            transaction_id: None,
        }
    }

    /// Java's two-argument constructor, preserving null topic and body values.
    pub fn new_nullable(topic: Option<String>, body: Option<Vec<u8>>) -> Self {
        Self::with_nullable_fields(topic, None, None, 0, body, true)
    }

    /// Java's `Message(topic, tags, body)` constructor, including null inputs.
    pub fn with_tags(topic: Option<String>, tags: Option<String>, body: Option<Vec<u8>>) -> Self {
        Self::with_nullable_fields(topic, tags, None, 0, body, true)
    }

    /// Java's `Message(topic, tags, keys, body)` constructor, including null inputs.
    pub fn with_tags_and_keys(
        topic: Option<String>,
        tags: Option<String>,
        keys: Option<String>,
        body: Option<Vec<u8>>,
    ) -> Self {
        Self::with_nullable_fields(topic, tags, keys, 0, body, true)
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
            body: body.map(|body| Arc::new(RwLock::new(body))),
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
    pub fn body(
        &self,
    ) -> impl Deref<Target = [u8]> + AsRef<[u8]> + std::fmt::Debug + PartialEq + '_ {
        MessageBodyReadView(self.body.as_ref().map(|body| read_message_state(body)))
    }
    pub fn body_nullable(
        &self,
    ) -> Option<impl Deref<Target = [u8]> + AsRef<[u8]> + std::fmt::Debug + PartialEq + '_> {
        self.body
            .as_ref()
            .map(|body| MessageBodyReadView(Some(read_message_state(body))))
    }
    /// Mutably borrows the Java `getBody()` byte array when one is present.
    /// Messages returned by `clone_message()` share this array until either
    /// message calls `set_body` or `set_body_nullable`.
    pub fn body_mut(&mut self) -> Option<impl DerefMut<Target = [u8]> + '_> {
        self.body
            .as_ref()
            .map(|body| MessageBodyWriteView(write_message_state(body)))
    }
    pub fn set_body(&mut self, body: impl Into<Vec<u8>>) {
        self.body = Some(Arc::new(RwLock::new(body.into())));
    }
    pub fn set_body_nullable(&mut self, body: Option<Vec<u8>>) {
        self.body = body.map(|body| Arc::new(RwLock::new(body)));
    }
    pub fn flag(&self) -> i32 {
        self.flag
    }
    pub fn properties(
        &self,
    ) -> impl Deref<Target = MessageProperties> + Serialize + std::fmt::Debug + PartialEq + '_ {
        MessagePropertiesReadView(match self.properties.as_ref() {
            Some(properties) => MessagePropertiesReadSource::Shared(read_message_state(properties)),
            None => MessagePropertiesReadSource::Empty,
        })
    }
    pub fn properties_nullable(
        &self,
    ) -> Option<impl Deref<Target = MessageProperties> + Serialize + std::fmt::Debug + PartialEq + '_>
    {
        match self.properties.as_ref() {
            Some(properties) => Some(MessagePropertiesReadView(
                MessagePropertiesReadSource::Shared(read_message_state(properties)),
            )),
            None if self
                .properties_read_initialized
                .load(AtomicOrdering::Relaxed) =>
            {
                Some(MessagePropertiesReadView(
                    MessagePropertiesReadSource::Empty,
                ))
            }
            None => None,
        }
    }
    pub fn get_properties(
        &self,
    ) -> Option<impl Deref<Target = MessageProperties> + Serialize + std::fmt::Debug + PartialEq + '_>
    {
        self.properties_nullable()
    }
    /// Replace the message property map, corresponding to Java's
    /// `MessageAccessor.setProperties`. Rust takes ownership of the supplied
    /// map; `None` preserves Java's null-map state.
    pub fn set_properties(&mut self, properties: Option<MessageProperties>) {
        self.properties = properties.map(|values| {
            Arc::new(RwLock::new(MessagePropertyStorage {
                values,
                ..MessagePropertyStorage::default()
            }))
        });
        self.properties_read_initialized
            .store(false, AtomicOrdering::Relaxed);
    }
    /// Replace the property map while preserving Java `HashMap` null keys and
    /// values. `None` represents a null map; `Some(empty)` represents an empty
    /// map.
    pub fn set_nullable_properties(&mut self, properties: Option<NullableMessageProperties>) {
        self.properties_read_initialized
            .store(false, AtomicOrdering::Relaxed);
        self.properties = properties.map(|properties| {
            let mut storage = MessagePropertyStorage::default();
            for (key, value) in properties {
                match key {
                    Some(key) => {
                        if let Some(value) = value {
                            storage.values.insert(key, value);
                        } else {
                            storage.null_values.insert(key);
                        }
                    }
                    None => {
                        storage.null_key_value = Some(value);
                    }
                }
            }
            Arc::new(RwLock::new(storage))
        });
    }
    /// Returns stored property entries, including Java-style null values, in
    /// deterministic key order. `properties()` and `get_properties()` retain
    /// the Rust string-to-string map view; this iterator exposes the entries
    /// that Java callers can observe through `Message.getProperties()`.
    pub fn property_entries(&self) -> impl Iterator<Item = (String, Option<String>)> {
        self.nullable_property_entries()
            .filter_map(|(key, value)| key.map(|key| (key, value)))
    }
    /// Returns stored Java-style entries, including a null key or null value,
    /// in deterministic key order. A null key is returned as `None`.
    pub fn nullable_property_entries(
        &self,
    ) -> impl Iterator<Item = (Option<String>, Option<String>)> {
        let mut entries = self
            .properties
            .as_ref()
            .map_or_else(Vec::new, |properties| {
                let properties = read_message_state(properties);
                properties
                    .values
                    .iter()
                    .map(|(key, value)| (Some(key.clone()), Some(value.clone())))
                    .chain(
                        properties
                            .null_values
                            .iter()
                            .filter(|key| !properties.values.contains_key(*key))
                            .map(|key| (Some(key.clone()), None)),
                    )
                    .chain(
                        properties
                            .null_key_value
                            .as_ref()
                            .map(|value| (None, value.clone())),
                    )
                    .collect::<Vec<_>>()
            });
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        entries.into_iter()
    }
    pub fn get_properties_mut(&mut self) -> Option<impl DerefMut<Target = MessageProperties> + '_> {
        if self.properties.is_none()
            && !self
                .properties_read_initialized
                .swap(false, AtomicOrdering::Relaxed)
        {
            return None;
        }
        let properties = self
            .properties
            .get_or_insert_with(|| Arc::new(RwLock::new(MessagePropertyStorage::default())));
        Some(MessagePropertiesWriteView(write_message_state(properties)))
    }
    pub fn get_property(&self, key: &str) -> Option<String> {
        if self.properties.is_none() {
            self.properties_read_initialized
                .store(true, AtomicOrdering::Relaxed);
        }
        self.properties
            .as_ref()
            .and_then(|properties| read_message_state(properties).values.get(key).cloned())
    }
    pub fn get_nullable_property(&self, key: Option<&str>) -> Option<String> {
        match key {
            Some(key) => self.get_property(key),
            None => {
                if self.properties.is_none() {
                    self.properties_read_initialized
                        .store(true, AtomicOrdering::Relaxed);
                }
                self.properties.as_ref().and_then(|properties| {
                    read_message_state(properties)
                        .null_key_value
                        .as_ref()
                        .and_then(Clone::clone)
                })
            }
        }
    }
    pub fn get_user_property(&self, key: &str) -> Option<String> {
        self.get_property(key)
    }
    pub fn get_nullable_user_property(&self, key: Option<&str>) -> Option<String> {
        self.get_nullable_property(key)
    }
    pub fn has_property(&self, key: &str) -> bool {
        self.properties.as_ref().is_some_and(|properties| {
            let properties = read_message_state(properties);
            properties.values.contains_key(key) || properties.null_values.contains(key)
        })
    }
    pub fn has_nullable_property(&self, key: Option<&str>) -> bool {
        match key {
            Some(key) => self.has_property(key),
            None => self
                .properties
                .as_ref()
                .is_some_and(|properties| read_message_state(properties).null_key_value.is_some()),
        }
    }
    pub fn tags(&self) -> Option<String> {
        self.get_property(crate::message::constants::PROPERTY_TAGS)
    }
    pub fn keys(&self) -> Option<String> {
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
    pub fn buyer_id(&self) -> Option<String> {
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
    pub fn priority(&self) -> i32 {
        self.get_property(crate::message::constants::PROPERTY_PRIORITY)
            .and_then(|value| parse_java_i32(&value))
            .unwrap_or(-1)
    }
    pub fn set_priority(&mut self, priority: i32) -> Result<(), MessagePropertyError> {
        if priority < 0 {
            return Err(MessagePropertyError::NegativePriority);
        }
        self.put_property_value(
            crate::message::constants::PROPERTY_PRIORITY,
            Some(priority.to_string()),
        );
        Ok(())
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
    pub fn put_user_property_nullable(
        &mut self,
        key: Option<String>,
        value: Option<String>,
    ) -> Result<Option<String>, MessagePropertyError> {
        if crate::message::constants::is_reserved_user_property(key.as_deref()) {
            return Err(MessagePropertyError::ReservedProperty);
        }
        let key = key
            .filter(|key| !java_trim_is_empty(key))
            .ok_or(MessagePropertyError::EmptyKey)?;
        let value = value
            .filter(|value| !java_trim_is_empty(value))
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
        if crate::message::constants::is_reserved_user_property(Some(&key)) {
            return Err(MessagePropertyError::ReservedProperty);
        }
        validate_property(&key, &value)?;
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

    /// Set a property through Java's nullable `MessageAccessor.putProperty`
    /// boundary. The nested return value distinguishes an absent key from a
    /// key whose previous value was null.
    pub fn set_property_nullable(
        &mut self,
        key: Option<String>,
        value: Option<String>,
    ) -> Option<Option<String>> {
        self.properties_read_initialized
            .store(false, AtomicOrdering::Relaxed);
        match key {
            Some(key) => {
                let previous = self.has_property(&key).then(|| self.get_property(&key));
                self.put_property_value(key, value);
                previous
            }
            None => {
                let properties = self.properties.get_or_insert_with(|| {
                    Arc::new(RwLock::new(MessagePropertyStorage::default()))
                });
                write_message_state(properties)
                    .null_key_value
                    .replace(value)
            }
        }
    }

    /// Removes a system property, corresponding to Java `MessageAccessor.clearProperty`.
    pub fn clear_system_property(&mut self, key: &str) -> Option<String> {
        self.properties.as_mut().and_then(|properties| {
            let mut properties = write_message_state(properties);
            properties.null_values.remove(key);
            properties.values.remove(key)
        })
    }

    /// Clear a nullable property key through Java's `MessageAccessor` API.
    /// The nested return value distinguishes an absent key from a key whose
    /// value was null.
    pub fn clear_property_nullable(&mut self, key: Option<&str>) -> Option<Option<String>> {
        match key {
            Some(key) => {
                let was_present = self.has_property(key);
                let value = self.clear_system_property(key);
                was_present.then_some(value)
            }
            None => self
                .properties
                .as_ref()
                .and_then(|properties| write_message_state(properties).null_key_value.take()),
        }
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

    /// Java's `String.join` collection overload: null elements become `"null"`;
    /// a null collection is reported as an error instead of panicking.
    pub fn set_keys_from_nullable<I, S>(
        &mut self,
        keys: Option<I>,
    ) -> Result<(), MessageKeyCollectionError>
    where
        I: IntoIterator<Item = Option<S>>,
        S: AsRef<str>,
    {
        let keys = keys.ok_or(MessageKeyCollectionError::NullCollection)?;
        let joined = keys
            .into_iter()
            .map(|key| {
                key.map(|key| key.as_ref().to_owned())
                    .unwrap_or_else(|| "null".to_owned())
            })
            .collect::<Vec<_>>()
            .join(crate::message::constants::KEY_SEPARATOR);
        self.put_property_value(crate::message::constants::PROPERTY_KEYS, Some(joined));
        Ok(())
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
        parse_java_i32(&value).ok_or_else(|| MessagePropertyParseError {
            property,
            value: value.to_owned(),
        })
    }

    fn parse_i64_property(&self, property: &'static str) -> Result<i64, MessagePropertyParseError> {
        let Some(value) = self.get_property(property) else {
            return Ok(0);
        };
        parse_java_i64(&value).ok_or_else(|| MessagePropertyParseError {
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
        let properties = self
            .properties
            .get_or_insert_with(|| Arc::new(RwLock::new(MessagePropertyStorage::default())));
        let mut properties = write_message_state(properties);
        match value {
            Some(value) => {
                properties.null_values.remove(&key);
                properties.values.insert(key, value)
            }
            None => {
                let previous = properties.values.remove(&key);
                properties.null_values.insert(key);
                previous
            }
        }
    }

    /// Java-style diagnostic representation. Java's `HashMap` property order
    /// is unspecified; Rust emits the `BTreeMap` order for reproducibility.
    fn java_to_string(&self) -> String {
        let properties = if self.properties_nullable().is_none() {
            "null".to_owned()
        } else {
            format!(
                "{{{}}}",
                self.nullable_property_entries()
                    .map(|(key, value)| {
                        format!(
                            "{}={}",
                            key.as_deref().unwrap_or("null"),
                            value.as_deref().unwrap_or("null")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let body = self
            .body
            .as_ref()
            .map(|body| {
                let body = read_message_state(body);
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

/// Extended RocketMQ message data corresponding to Java's `MessageExt`.
///
/// Java `MessageExt.toString()` is exposed through Rust's `Display`/`ToString`;
/// its formatter helper is not a separate public method:
///
/// Rust's `DerefMut` exposes inherited `Message` setters in place, so there is
/// no Java-equivalent operation that replaces the whole inherited message:
///
/// ```compile_fail
/// use rocketmq_classic_common::message::MessageExt;
/// let mut extended = MessageExt::default();
/// extended.set_message(Default::default());
/// ```
///
/// The Java class inherits `Message` directly; it has no composition accessors:
///
/// ```compile_fail
/// use rocketmq_classic_common::message::MessageExt;
/// let _ = MessageExt::default().message();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::MessageExt;
/// MessageExt::default().message_mut();
/// ```
///
/// ```compile_fail
/// use rocketmq_classic_common::message::MessageExt;
/// let _ = MessageExt::default().java_to_string();
/// ```
///
/// Java `MessageExt` inherits identity-based `Object.equals`/`hashCode`;
/// Rust equality also follows the message identity token instead of comparing
/// fields. Cloning a `MessageExt` preserves that token:
///
/// ```
/// use rocketmq_classic_common::message::MessageExt;
/// let message = MessageExt::default();
/// assert_eq!(message, message.clone());
/// assert_ne!(message, MessageExt::default());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MessageExt {
    #[serde(skip)]
    identity: Arc<()>,
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

impl PartialEq for MessageExt {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity)
    }
}

impl Eq for MessageExt {}

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
    pub fn broker_name(&self) -> Option<&str> {
        self.broker_name.as_deref()
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
                .get_property(crate::message::constants::PROPERTY_TRANSIENT_TOPIC_CONFIG)
                .as_deref(),
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
                .get_property(crate::message::constants::PROPERTY_TRANSIENT_GROUP_CONFIG)
                .as_deref(),
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
    fn java_to_string(&self) -> String {
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

/// Client-decoded `MessageExt` view corresponding to Java's
/// `MessageClientExt`. The wrapped base message retains its physical ID;
/// `message_id()` resolves the live `UNIQ_KEY` property first, and the Java
/// subclass's `setMsgId` override remains a no-op.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(transparent)]
pub struct MessageClientExt(MessageExt);

impl MessageClientExt {
    pub fn new(message: MessageExt) -> Self {
        Self(message)
    }

    pub fn message_id(&self) -> Option<String> {
        self.0
            .get_property(crate::message::constants::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            .or_else(|| self.0.offset_message_id().map(str::to_owned))
            .or_else(|| self.0.message_id().map(str::to_owned))
    }

    /// Java `MessageClientExt.setMsgId` intentionally does nothing.
    pub fn set_message_id(&mut self, _value: Option<String>) {}

    /// Java `MessageClientExt.getOffsetMsgId` returns the inherited `msgId`.
    pub fn offset_message_id(&self) -> Option<&str> {
        self.0.offset_message_id().or_else(|| self.0.message_id())
    }

    /// Java `MessageClientExt.setOffsetMsgId` writes the inherited `msgId`.
    pub fn set_offset_message_id(&mut self, value: Option<String>) {
        self.0.set_offset_message_id(value);
    }
}

impl From<MessageExt> for MessageClientExt {
    fn from(message: MessageExt) -> Self {
        Self::new(message)
    }
}

impl Deref for MessageClientExt {
    type Target = MessageExt;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MessageClientExt {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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

/// Queue identity corresponding to Java's `MessageQueue` constructors.
///
/// ```compile_fail
/// use rocketmq_classic_common::message::MessageQueue;
/// let _queue = MessageQueue::new_nullable_broker("topic", None, 0);
/// ```
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
    if java_trim_is_empty(key) {
        return Err(MessagePropertyError::EmptyKey);
    }
    if java_trim_is_empty(value) {
        return Err(MessagePropertyError::EmptyValue);
    }
    Ok(())
}

fn java_trim_is_empty(value: &str) -> bool {
    value
        .trim_matches(|character: char| character <= '\u{20}')
        .is_empty()
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
        assert_eq!(message.tags(), Some("tag".to_owned()));
        let mut ext = MessageExt::new(message.clone());
        ext.set_queue_offset(i64::MIN);
        ext.set_commit_log_offset(i64::MAX);
        assert_eq!(ext.topic_nullable(), message.topic_nullable());
        assert_eq!(ext.body_nullable(), message.body_nullable());
        assert_eq!(ext.properties(), message.properties());
        assert_eq!(ext.queue_offset(), i64::MIN);
        assert_eq!(ext.commit_log_offset(), i64::MAX);
    }

    #[test]
    fn java_property_setters_accept_wire_separators() {
        let mut message = Message::new("topic", vec![]);
        message.put_user_property("key", "bad\u{2}value").unwrap();
        assert_eq!(message.properties().get("key").unwrap(), "bad\u{2}value");
        assert_eq!(
            message.put_user_property("", "value"),
            Err(MessagePropertyError::EmptyKey)
        );
        assert_eq!(
            message.put_user_property("KEYS", "user-value"),
            Err(MessagePropertyError::ReservedProperty)
        );
        assert_eq!(
            message.put_user_property("custom", "  "),
            Err(MessagePropertyError::EmptyValue)
        );
    }
}
