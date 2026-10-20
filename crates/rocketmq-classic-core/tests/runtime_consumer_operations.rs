use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use rocketmq_classic_core::{
    config::ClientConfig,
    runtime::{ClientRuntimeManager, ConsumerRuntime, RuntimeError},
};
use rocketmq_classic_protocol::{
    SharedValue,
    body::ConsumerRunningInfoBody,
    heartbeat::{ConsumeType, ConsumerData},
};
use serde_json::Value;

struct RunningInfoOwner {
    snapshot: ConsumerRunningInfoBody,
    name: String,
    consume_type: Option<ConsumeType>,
    calls: Mutex<Vec<&'static str>>,
}

impl ConsumerRuntime for RunningInfoOwner {
    fn heartbeat_data(&self) -> ConsumerData {
        panic!("Running Info must use consumeType directly")
    }

    fn consume_type(&self) -> Option<ConsumeType> {
        self.calls.lock().unwrap().push("type");
        self.consume_type
    }

    fn consumer_running_info(&self) -> Result<ConsumerRunningInfoBody, RuntimeError> {
        self.calls.lock().unwrap().push("info");
        match self.name.as_str() {
            "null-snapshot" | "snapshot-throws" => {
                Err(RuntimeError::ConsumerOperation(self.name.clone()))
            }
            _ => Ok(self.snapshot.clone()),
        }
    }
}

#[tokio::test]
async fn running_info_matches_java_enrichment_aliases_and_partial_failure_order() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/runtime/consumer-operations.json"
    ))
    .unwrap();
    let cases = fixture["runningInfo"].as_array().unwrap();
    assert_eq!(cases.len(), 9);
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let mut config = ClientConfig::new(name);
        config.set_namesrv_addr(None);
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        if let Some(addresses) = case["addresses"].as_array() {
            runtime
                .remoting()
                .update_name_server_address_list(vec!["seed:9876".into()]);
            runtime.remoting().with_name_server_address_list(|list| {
                *list.unwrap() = addresses
                    .iter()
                    .map(|address| address.as_str().unwrap().into())
                    .collect();
            });
        }
        let mut snapshot = ConsumerRunningInfoBody::default();
        if name == "null-properties" {
            snapshot.set_properties(None);
        } else {
            snapshot.set_properties(Some(SharedValue::new(BTreeMap::from([
                ("owner".into(), "retained".into()),
                (
                    ConsumerRunningInfoBody::PROP_NAMESERVER_ADDR.into(),
                    "owner-ns".into(),
                ),
                (
                    ConsumerRunningInfoBody::PROP_CONSUME_TYPE.into(),
                    "owner-type".into(),
                ),
                (
                    ConsumerRunningInfoBody::PROP_CLIENT_VERSION.into(),
                    "owner-version".into(),
                ),
            ]))));
        }
        let properties = snapshot.get_properties();
        let owner = Arc::new(RunningInfoOwner {
            snapshot,
            name: name.into(),
            consume_type: match case["consumeType"].as_str() {
                Some("CONSUME_ACTIVELY") => Some(ConsumeType::ConsumeActively),
                Some("CONSUME_PASSIVELY") => Some(ConsumeType::ConsumePassively),
                None => None,
                Some(other) => panic!("unknown type: {other}"),
            },
            calls: Mutex::new(Vec::new()),
        });
        if name != "missing-group" {
            runtime
                .consumer_table()
                .put("selected", owner.clone())
                .await
                .unwrap();
        }
        let result = runtime.consumer_running_info(case["group"].as_str());
        if case["error"].is_null() {
            let result = result.unwrap();
            assert_eq!(
                result.is_some(),
                case["resultPresent"].as_bool().unwrap(),
                "{name}"
            );
            if let Some(result) = result {
                assert!(case["sameSnapshot"].as_bool().unwrap());
                assert!(case["sameProperties"].as_bool().unwrap());
                assert!(
                    result
                        .get_properties()
                        .unwrap()
                        .ptr_eq(properties.as_ref().unwrap())
                );
            }
        } else {
            assert!(result.is_err(), "{name}");
        }
        assert_eq!(
            serde_json::to_value(&*owner.calls.lock().unwrap()).unwrap(),
            case["calls"],
            "{name}"
        );
        let actual = if name == "null-snapshot" {
            Value::Null
        } else {
            serde_json::to_value(properties.as_ref().map(SharedValue::snapshot)).unwrap()
        };
        assert_eq!(actual, case["propertiesAfter"], "{name}");
        runtime.consumer_table().remove("selected").await.unwrap();
        runtime.shutdown().await.unwrap();
    }
}
