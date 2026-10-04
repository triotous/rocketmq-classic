use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionData {
    #[serde(default)]
    pub class_filter_mode: bool,
    pub topic: String,
    pub sub_string: String,
    #[serde(default)]
    pub tags_set: HashSet<String>,
    #[serde(default)]
    pub code_set: HashSet<i32>,
    pub sub_version: i64,
    #[serde(default = "default_expression_type")]
    pub expression_type: String,
    #[serde(rename = "filterClassSource", skip_serializing)]
    pub filter_class_source: Option<String>,
}

impl Default for SubscriptionData {
    fn default() -> Self {
        Self {
            class_filter_mode: false,
            topic: String::new(),
            sub_string: String::new(),
            tags_set: HashSet::new(),
            code_set: HashSet::new(),
            sub_version: 0,
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
        hash_set(&self.tags_set, state);
        hash_set(&self.code_set, state);
        self.sub_version.hash(state);
        self.expression_type.hash(state);
        self.filter_class_source.hash(state);
    }
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

fn default_expression_type() -> String {
    "TAG".to_owned()
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

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageModel {
    #[default]
    Clustering,
    Broadcasting,
    LiteSelective,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProducerData {
    pub group_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerData {
    pub group_name: String,
    pub consume_type: ConsumeType,
    pub message_model: MessageModel,
    pub consume_from_where: ConsumeFromWhere,
    #[serde(default)]
    pub subscription_data_set: HashSet<SubscriptionData>,
    pub unit_mode: bool,
}

impl Hash for ConsumerData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.group_name.hash(state);
        self.consume_type.hash(state);
        self.message_model.hash(state);
        self.consume_from_where.hash(state);
        hash_set(&self.subscription_data_set, state);
        self.unit_mode.hash(state);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatData {
    #[serde(rename = "clientID")]
    pub client_id: Option<String>,
    #[serde(default)]
    pub producer_data_set: HashSet<ProducerData>,
    #[serde(default)]
    pub consumer_data_set: HashSet<ConsumerData>,
    #[serde(default)]
    pub heartbeat_fingerprint: i32,
    #[serde(default, rename = "withoutSub", alias = "isWithoutSub")]
    pub is_without_sub: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_json_uses_java_field_names_and_preserves_subscription_data() {
        let mut heartbeat = HeartbeatData {
            client_id: Some("client".into()),
            ..HeartbeatData::default()
        };
        heartbeat.consumer_data_set.insert(ConsumerData {
            group_name: "group".into(),
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            subscription_data_set: HashSet::from([SubscriptionData {
                topic: "topic".into(),
                sub_string: "*".into(),
                sub_version: i64::MAX,
                ..SubscriptionData::default()
            }]),
            ..ConsumerData::default()
        });
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
