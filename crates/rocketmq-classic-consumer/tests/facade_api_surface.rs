use serde_json::Value;

const JAVA_FACADE_API: &str = include_str!("../../../fixtures/consumer/facade-api.json");

fn method_named(api: &Value, method: &str) -> bool {
    api["declaredPublicMethods"]
        .as_array()
        .expect("declared Java methods")
        .iter()
        .filter_map(Value::as_str)
        .any(|signature| signature.contains(&format!(".{method}(")))
}

#[test]
fn pinned_java_consumer_facade_inventory_covers_declared_and_inherited_methods() {
    let fixture: Value = serde_json::from_str(JAVA_FACADE_API).expect("valid Java API fixture");
    assert_eq!(
        fixture["provenance"]["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );

    let apis = &fixture["apis"];
    for (name, constructors, declared, callable) in [
        ("DefaultMQPushConsumer", 11, 91, 165),
        ("MQPushConsumer", 0, 12, 23),
        ("DefaultMQPullConsumer", 5, 60, 135),
        ("MQPullConsumer", 0, 20, 31),
        ("DefaultLitePullConsumer", 5, 80, 155),
        ("LitePullConsumer", 0, 31, 31),
        ("MQAdmin", 0, 8, 8),
    ] {
        let api = &apis[name];
        assert_eq!(
            api["publicConstructors"].as_array().unwrap().len(),
            constructors,
            "{name}"
        );
        assert_eq!(
            api["declaredPublicMethodCount"].as_u64(),
            Some(declared),
            "{name}"
        );
        assert_eq!(api["publicMethodCount"].as_u64(), Some(callable), "{name}");
    }

    for name in ["DefaultMQPushConsumer", "DefaultMQPullConsumer"] {
        let api = &apis[name];
        assert!(
            method_named(api, "viewMessage"),
            "{name} exposes viewMessage"
        );
        assert!(
            method_named(api, "queryMessage"),
            "{name} exposes queryMessage"
        );
        assert!(
            !method_named(api, "queryMessageByUniqKey"),
            "{name} keeps unique-key lookup behind viewMessage"
        );
    }
}
