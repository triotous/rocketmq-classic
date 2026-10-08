use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionData {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub class_filter_mode: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_string: Option<String>,
    #[serde(default = "empty_tags", skip_serializing_if = "Option::is_none")]
    pub tags_set: Option<HashSet<String>>,
    #[serde(default = "empty_codes", skip_serializing_if = "Option::is_none")]
    pub code_set: Option<HashSet<i32>>,
    #[serde(
        default = "current_subscription_version",
        deserialize_with = "deserialize_null_default"
    )]
    pub sub_version: i64,
    #[serde(
        default = "default_expression_type",
        skip_serializing_if = "Option::is_none"
    )]
    pub expression_type: Option<String>,
    #[serde(rename = "filterClassSource", skip_serializing)]
    pub filter_class_source: Option<String>,
}

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

fn hash_optional_set<T: Hash, H: Hasher>(values: &Option<HashSet<T>>, state: &mut H) {
    match values {
        Some(values) => {
            true.hash(state);
            hash_set(values, state);
        }
        None => false.hash(state),
    }
}

fn empty_tags() -> Option<HashSet<String>> {
    Some(HashSet::new())
}

fn empty_codes() -> Option<HashSet<i32>> {
    Some(HashSet::new())
}

fn current_subscription_version() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consume_type: Option<ConsumeType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_model: Option<MessageModel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consume_from_where: Option<ConsumeFromWhere>,
    #[serde(
        default = "empty_subscriptions",
        skip_serializing_if = "Option::is_none"
    )]
    pub subscription_data_set: Option<HashSet<SubscriptionData>>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
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

impl Hash for ConsumerData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.group_name.hash(state);
        self.consume_type.hash(state);
        self.message_model.hash(state);
        self.consume_from_where.hash(state);
        hash_optional_set(&self.subscription_data_set, state);
        self.unit_mode.hash(state);
    }
}

fn empty_subscriptions() -> Option<HashSet<SubscriptionData>> {
    Some(HashSet::new())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatData {
    #[serde(rename = "clientID", default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default = "empty_producers", skip_serializing_if = "Option::is_none")]
    pub producer_data_set: Option<HashSet<ProducerData>>,
    #[serde(default = "empty_consumers", skip_serializing_if = "Option::is_none")]
    /// Java ConsumerData has object identity equality. Preserve its wire
    /// multiplicity rather than deduplicating structurally equal metadata.
    pub consumer_data_set: Option<Vec<ConsumerData>>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub heartbeat_fingerprint: i32,
    #[serde(
        default,
        rename = "withoutSub",
        alias = "isWithoutSub",
        deserialize_with = "deserialize_null_default"
    )]
    pub is_without_sub: bool,
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

fn empty_producers() -> Option<HashSet<ProducerData>> {
    Some(HashSet::new())
}

fn empty_consumers() -> Option<Vec<ConsumerData>> {
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
                value.tags_set.as_ref().is_some_and(HashSet::is_empty),
                case["tagsEmpty"]
            );
            assert_eq!(
                value.code_set.as_ref().is_some_and(HashSet::is_empty),
                case["codeEmpty"]
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
            ] {
                assert_eq!(encoded.get(field).is_some(), case[marker], "{field}");
            }
        }
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
            assert_eq!(encoded.get("groupName").is_some(), case["encodedHasGroup"]);
        }
        for case in cases["consumers"].as_array().unwrap() {
            let value: ConsumerData =
                serde_json::from_str(case["rawInput"].as_str().unwrap()).unwrap();
            let encoded = serde_json::to_value(&value).unwrap();
            assert_eq!(value.group_name.is_none(), case["groupNull"]);
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
                value
                    .consumer_data_set
                    .as_ref()
                    .map_or(-1, |set| set.len() as i64),
                case["consumerCount"]
            );
            assert_eq!(value.heartbeat_fingerprint, case["fingerprint"]);
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
            .push(ConsumerData {
                group_name: Some("group".into()),
                consume_from_where: Some(ConsumeFromWhere::ConsumeFromLastOffset),
                subscription_data_set: Some(HashSet::from([SubscriptionData {
                    topic: Some("topic".into()),
                    sub_string: Some("*".into()),
                    sub_version: i64::MAX,
                    ..SubscriptionData::default()
                }])),
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
