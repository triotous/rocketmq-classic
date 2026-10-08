use rocketmq_classic_common::{message::MessageQueue, serde_fastjson};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::Value;

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

fn default_assignment_mode() -> Option<MessageRequestMode> {
    Some(MessageRequestMode::Pull)
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

    fn from_number<E: de::Error>(number: f64) -> Result<Option<MessageRequestMode>, E> {
        if !number.is_finite() {
            return Err(E::custom("Fastjson MessageRequestMode must be finite"));
        }
        let ordinal = number.trunc();
        if !(0.0..=1.0).contains(&ordinal) {
            return Err(E::custom(format!(
                "Fastjson MessageRequestMode ordinal {ordinal} is out of range"
            )));
        }
        Ok(Some(if ordinal == 0.0 {
            MessageRequestMode::Pull
        } else {
            MessageRequestMode::Pop
        }))
    }

    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Null => Ok(None),
        Value::String(value) if value.eq_ignore_ascii_case("PULL") => {
            Ok(Some(MessageRequestMode::Pull))
        }
        Value::String(value) if value.eq_ignore_ascii_case("POP") => {
            Ok(Some(MessageRequestMode::Pop))
        }
        Value::String(value) => match value.parse::<i64>() {
            Ok(ordinal) => from_ordinal::<D::Error>(ordinal),
            Err(_) => Ok(None),
        },
        Value::Number(value) => {
            if let Some(ordinal) = value.as_i64() {
                from_ordinal::<D::Error>(ordinal)
            } else if let Some(ordinal) = value.as_u64() {
                match ordinal {
                    0 => Ok(Some(MessageRequestMode::Pull)),
                    1 => Ok(Some(MessageRequestMode::Pop)),
                    _ => Err(de::Error::custom(format!(
                        "Fastjson MessageRequestMode ordinal {ordinal} is out of range"
                    ))),
                }
            } else if let Some(number) = value.as_f64() {
                from_number::<D::Error>(number)
            } else {
                Err(de::Error::custom("invalid Fastjson MessageRequestMode"))
            }
        }
        _ => Err(de::Error::custom(
            "Fastjson cannot coerce this value to MessageRequestMode",
        )),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LiteSubscriptionAction {
    PartialAdd,
    PartialRemove,
    CompleteAdd,
    CompleteRemove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OffsetOptionType {
    Policy,
    Offset,
    TailN,
    Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct OffsetOption {
    #[serde(rename = "type")]
    pub option_type: Option<OffsetOptionType>,
    pub value: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LiteSubscriptionDto {
    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_model: Option<super::heartbeat::MessageModel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

impl Default for MessageQueueAssignment {
    fn default() -> Self {
        Self {
            message_queue: None,
            mode: default_assignment_mode(),
            attachments: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryAssignmentResponseBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_queue_assignments: Option<Vec<MessageQueueAssignment>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SetMessageRequestModeRequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    #[serde(default)]
    pub mode: MessageRequestMode,
    pub pop_share_queue_num: i32,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerListByGroupResponseBody {
    #[serde(
        default,
        deserialize_with = "serde_fastjson::deserialize_optional_nullable_string_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub consumer_id_list: Option<Vec<Option<String>>>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
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
        for case in fixture["assignmentModeCases"].as_array().unwrap() {
            let decoded =
                serde_json::from_value::<QueryAssignmentResponseBody>(case["input"].clone());
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
                    .and_then(|assignments| assignments.first())
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
        for case in fixture["assignmentAttachmentCases"].as_array().unwrap() {
            let decoded =
                serde_json::from_value::<QueryAssignmentResponseBody>(case["input"].clone());
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
                    .and_then(|assignments| assignments.first())
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
        for case in fixture["consumerListCases"].as_array().unwrap() {
            let decoded =
                serde_json::from_value::<ConsumerListByGroupResponseBody>(case["input"].clone());
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
        assert_eq!(mode.mode, MessageRequestMode::Pull);
        assert_eq!(serde_json::to_value(mode).unwrap()["mode"], "PULL");

        let absent: LiteSubscriptionCtlRequestBody =
            serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(serde_json::to_value(absent).unwrap(), serde_json::json!({}));
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
                        mode: MessageRequestMode::Pop,
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
