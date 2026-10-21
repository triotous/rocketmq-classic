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

#[test]
fn pinned_java_nested_rebalance_and_process_queue_api_denominator_is_recorded() {
    let fixture: Value = serde_json::from_str(JAVA_FACADE_API).expect("valid Java API fixture");
    assert_eq!(
        fixture["provenance"]["sourceCommit"],
        "efa1be59d6fc747ad220f036c86030f05f0419f9"
    );

    for (name, constructors, declared, callable) in [
        ("RebalanceImpl", 1, 32, 32),
        ("RebalancePushImpl", 2, 12, 32),
        ("RebalancePullImpl", 2, 11, 32),
        ("ProcessQueue", 1, 32, 32),
        ("PopProcessQueue", 1, 11, 11),
    ] {
        let api = &fixture["implementationApis"][name];
        assert_eq!(
            api["publicConstructors"].as_array().unwrap().len(),
            constructors,
            "{name} constructors"
        );
        assert_eq!(
            api["declaredPublicMethodCount"].as_u64(),
            Some(declared),
            "{name} declared methods"
        );
        assert_eq!(
            api["publicMethodCount"].as_u64(),
            Some(callable),
            "{name} callable methods"
        );
    }

    let implementations = &fixture["implementationApis"];
    for (owner, method) in [
        ("RebalanceImpl", "getProcessQueueTable"),
        ("RebalanceImpl", "getPopProcessQueueTable"),
        ("ProcessQueue", "cleanExpiredMsg"),
        ("ProcessQueue", "getMsgTreeMap"),
        ("PopProcessQueue", "ack"),
    ] {
        let methods = implementations[owner]["publicMethods"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        assert!(
            methods
                .iter()
                .any(|signature| signature.contains(&format!(".{method}("))),
            "pinned Java {owner}.{method} is inventoried"
        );
    }
}

#[test]
fn pinned_java_consumer_callable_method_names_have_phase_crosswalks() {
    let fixture: Value = serde_json::from_str(JAVA_FACADE_API).expect("valid Java API fixture");
    let apis = &fixture["apis"];
    let crosswalks = [
        include_str!("../../../docs/compatibility/PHASE_6_API_CONFIG_MAP.md"),
        include_str!("../../../docs/compatibility/PHASE_9_OFFLINE_METHOD_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_10_OFFLINE_METHOD_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_11_OFFLINE_METHOD_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_12_ORDERLY_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_13_LITE_POP_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_14_TRANSACTION_MATRIX.md"),
        include_str!("../../../docs/compatibility/PHASE_15_REQUEST_REPLY_MATRIX.md"),
        include_str!(
            "../../../docs/compatibility/PHASE_16_NAMESPACE_SECURITY_OPERATIONS_MATRIX.md"
        ),
        include_str!("../../../docs/compatibility/PHASE_17_ADMIN_MATRIX.md"),
    ]
    .join("\n");

    let mut missing = Vec::new();
    for (owner, expected_count) in [
        ("DefaultMQPushConsumer", 165),
        ("MQPushConsumer", 23),
        ("DefaultMQPullConsumer", 135),
        ("MQPullConsumer", 31),
        ("DefaultLitePullConsumer", 155),
        ("LitePullConsumer", 31),
        ("MQAdmin", 8),
    ] {
        let methods = apis[owner]["publicMethods"]
            .as_array()
            .expect("Java callable methods");
        assert_eq!(
            methods.len(),
            expected_count,
            "unexpected {owner} method count"
        );

        for signature in methods {
            let signature = signature.as_str().expect("Java method signature");
            let method_name = signature
                .split('(')
                .next()
                .expect("method name")
                .rsplit('.')
                .next()
                .expect("declaring type and method name");
            if !crosswalks.contains(method_name) {
                missing.push(signature);
            }
        }
    }

    assert!(
        missing.is_empty(),
        "Java consumer facade/inherited methods have no phase crosswalk:\n{}",
        missing.join("\n")
    );
}
