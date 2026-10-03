use rocketmq_classic_common::message::MessageQueue;
use serde::{Deserialize, Serialize};

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
}
