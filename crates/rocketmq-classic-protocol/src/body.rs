use rocketmq_classic_common::message::MessageQueue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageRequestMode {
    #[default]
    Pull,
    Pop,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MessageQueueAssignment {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_queue: Option<MessageQueue>,
    pub mode: MessageRequestMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attachments: Option<std::collections::BTreeMap<String, String>>,
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
    #[serde(default)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_id_list: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResetOffsetBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset_table: Option<Vec<(MessageQueue, i64)>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetConsumerStatusBody {
    #[serde(default)]
    pub message_queue_table: Vec<(MessageQueue, i64)>,
    #[serde(default)]
    pub consumer_table: std::collections::BTreeMap<String, Vec<(MessageQueue, i64)>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeMessageDirectlyResult {
    #[serde(default)]
    pub order: bool,
    #[serde(default = "default_auto_commit")]
    pub auto_commit: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consume_result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remark: Option<String>,
    #[serde(default)]
    pub spent_time_mills: i64,
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
        assert_eq!(value["offsetTable"][0][0]["queueId"], 1);

        let status = GetConsumerStatusBody {
            message_queue_table: vec![(queue.clone(), 7)],
            consumer_table: std::collections::BTreeMap::from([(
                "consumer-a".into(),
                vec![(queue, 8)],
            )]),
        };
        let value = serde_json::to_value(status).unwrap();
        assert_eq!(value["consumerTable"]["consumer-a"][0][1], 8);

        let direct: ConsumeMessageDirectlyResult =
            serde_json::from_value(serde_json::json!({"order":false, "spentTimeMills":0})).unwrap();
        let value = serde_json::to_value(direct).unwrap();
        assert_eq!(value["autoCommit"], true);
        assert!(value.get("consumeResult").is_none());
    }

    #[test]
    fn assignment_request_mode_lite_and_batch_ack_match_java_json_shapes() {
        let assignment = MessageQueueAssignment {
            message_queue: Some(MessageQueue::new("orders", "broker-a", 1)),
            mode: MessageRequestMode::Pop,
            attachments: Some(std::collections::BTreeMap::from([(
                "zone".into(),
                "z1".into(),
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
