//! Exhaustive Java 5.5.1 fixture inventory for public basic CommandHeader models.
use std::collections::BTreeSet;

use rocketmq_classic_protocol::header::*;

mod support;
use support::{java_camel_to_snake, rust_header_method_exists};

fn java_fields(stem: &str) -> HeaderMap {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
    let path = format!("{root}/fixtures/remoting/{stem}.json");
    let metadata: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap())
            .unwrap_or_else(|error| panic!("{path}: {error}"));
    assert_eq!(metadata["java_client_version"], "5.5.1", "{stem}");
    serde_json::from_value(metadata["normalized_fields"]["ext_fields"].clone())
        .unwrap_or_else(|error| panic!("{stem}: {error}"))
}

// Each row names one public model, its Java->Rust
// oracle, and Rust->Java evidence when the Java verifier checks typed-*.bin.
macro_rules! inventory {
    ($visit:ident) => {
        $visit! {
            QueryConsumerOffsetRequestHeader => ("header-query-offset-request", "typed-query-offset.bin"),
            QueryConsumeTimeSpanRequestHeader => ("header-query-consume-time-span-request", "typed-query-consume-time-span-request.bin"),
            UpdateConsumerOffsetRequestHeader => ("header-update-offset-request", "typed-update-offset.bin"),
            QueryConsumerOffsetResponseHeader => ("header-query-offset-response", "typed-query-offset-response.bin"),
            SearchOffsetRequestHeader => ("header-search-offset-request", "typed-search-offset-request.bin"),
            GetMaxOffsetRequestHeader => ("header-max-offset-request", "typed-max-offset-request.bin"),
            GetMinOffsetRequestHeader => ("header-min-offset-request", "typed-min-offset-request.bin"),
            GetEarliestMsgStoretimeRequestHeader => ("header-earliest-msg-storetime-request", "typed-earliest-msg-storetime-request.bin"),
            SearchOffsetResponseHeader => ("header-search-offset-response", "typed-search-offset-response.bin"),
            GetMaxOffsetResponseHeader => ("header-max-offset-response", "typed-max-offset-response.bin"),
            GetMinOffsetResponseHeader => ("header-min-offset-response", "typed-min-offset-response.bin"),
            GetEarliestMsgStoretimeResponseHeader => ("header-earliest-msg-storetime-response", "typed-earliest-msg-storetime-response.bin"),
            PullMessageRequestHeader => ("header-pull-request", "typed-pull.bin"),
            PullMessageResponseHeader => ("header-pull-response", "typed-pull-response.bin"),
            UnregisterClientRequestHeader => ("header-unregister-request", "typed-unregister-request.bin"),
            GetRouteInfoRequestHeader => ("header-route-request", "typed-route.bin"),
            CreateTopicRequestHeader => ("header-create-topic-request", "typed-create-topic-request.bin"),
            QueryMessageRequestHeader => ("header-query-message-request", "typed-query-message-request.bin"),
            QueryMessageResponseHeader => ("header-query-message-response", "typed-query-message-response.bin"),
            QueryTopicsByConsumerRequestHeader => ("header-query-topics-by-consumer-request", "typed-query-topics-by-consumer-request.bin"),
            QuerySubscriptionByConsumerRequestHeader => ("header-query-subscription-by-consumer-request", "typed-query-subscription-by-consumer-request.bin"),
            QueryTopicConsumeByWhoRequestHeader => ("header-query-topic-consume-by-who-request", "typed-query-topic-consume-by-who-request.bin"),
            GetTopicStatsInfoRequestHeader => ("header-topic-stats-info-request", "typed-topic-stats-info-request.bin"),
            GetTopicsByClusterRequestHeader => ("header-topics-by-cluster-request", "typed-topics-by-cluster-request.bin"),
            GetSubscriptionGroupConfigRequestHeader => ("header-get-subscription-group-config-request", "typed-get-subscription-group-config-request.bin"),
            GetConsumeStatsRequestHeader => ("header-consume-stats-request", "typed-consume-stats-request.bin"),
            DeleteTopicRequestHeader => ("header-delete-topic-request", "typed-delete-topic-request.bin"),
            DeleteTopicFromNamesrvRequestHeader => ("header-delete-topic-from-namesrv-request", "typed-delete-topic-from-namesrv-request.bin"),
            DeleteKvConfigRequestHeader => ("header-delete-kv-config-request", "typed-delete-kv-config-request.bin"),
            GetKvConfigRequestHeader => ("header-get-kv-config-request", "typed-get-kv-config-request.bin"),
            GetKvConfigResponseHeader => ("header-get-kv-config-response", "typed-get-kv-config-response.bin"),
            WipeWritePermOfBrokerRequestHeader => ("header-wipe-write-perm-request", "typed-wipe-write-perm-request.bin"),
            AddWritePermOfBrokerRequestHeader => ("header-add-write-perm-request", "typed-add-write-perm-request.bin"),
            WipeWritePermOfBrokerResponseHeader => ("header-wipe-write-perm-response", "typed-wipe-write-perm-response.bin"),
            AddWritePermOfBrokerResponseHeader => ("header-add-write-perm-response", "typed-add-write-perm-response.bin"),
            GetKvListByNamespaceRequestHeader => ("header-get-kv-list-by-namespace-request", "typed-get-kv-list-by-namespace-request.bin"),
            PutKvConfigRequestHeader => ("header-put-kv-config-request", "typed-put-kv-config-request.bin"),
            DeleteSubscriptionGroupRequestHeader => ("header-delete-subscription-group-request", "typed-delete-subscription-group-request.bin"),
            ViewMessageRequestHeader => ("header-view-message-request", "typed-view-message-request.bin"),
            LockBatchMqRequestHeader => ("header-lock-batch-request", "typed-lock-batch-request.bin"),
            UnlockBatchMqRequestHeader => ("header-unlock-batch-request", "typed-unlock-batch-request.bin"),
            GetConsumerListByGroupRequestHeader => ("header-consumer-list-request", "typed-consumer-list-request.bin"),
            GetConsumerRunningInfoRequestHeader => ("header-consumer-running-info-request", "typed-consumer-running-info-request.bin"),
            ConsumerSendMsgBackRequestHeader => ("header-send-back-request", "typed-send-back-request.bin"),
            UnregisterClientResponseHeader => ("header-unregister-response", "typed-unregister-response.bin"),
            EndTransactionRequestHeader => ("header-end-transaction-request", "typed-end-transaction.bin"),
            EndTransactionResponseHeader => ("header-end-transaction-response", "typed-end-transaction-response.bin"),
            CheckTransactionStateRequestHeader => ("header-check-transaction-request", "typed-check-transaction.bin"),
            CheckTransactionStateResponseHeader => ("header-check-transaction-response", "typed-check-transaction-response.bin"),
            NotificationRequestHeader => ("header-notification-request", "typed-notification.bin"),
            NotificationResponseHeader => ("header-notification-response", "typed-notification-response.bin"),
            GetLiteTopicInfoRequestHeader => ("header-lite-topic-info-request", "typed-lite-topic-info-request.bin"),
            GetLiteClientInfoRequestHeader => ("header-lite-client-info-request", "typed-lite-client-info-request.bin"),
            GetLiteGroupInfoRequestHeader => ("header-lite-group-info-request", "typed-lite-group-info-request.bin"),
            GetParentTopicInfoRequestHeader => ("header-parent-topic-info-request", "typed-parent-topic-info-request.bin"),
            ResetOffsetRequestHeader => ("header-reset-offset-request", "typed-reset-offset.bin"),
            ResetMasterFlushOffsetHeader => ("header-reset-master-flush-offset", "typed-reset-master-flush-offset.bin"),
            ExportRocksDBConfigToJsonRequestHeader => ("header-export-rocksdb-config-request", "typed-export-rocksdb-config-request.bin"),
            CheckRocksdbCqWriteProgressRequestHeader => ("header-check-rocksdb-cq-write-progress-request", "typed-check-rocksdb-cq-write-progress-request.bin"),
            GetConsumerStatusRequestHeader => ("header-consumer-status-request", "typed-consumer-status-request.bin"),
            GetConsumerConnectionListRequestHeader => ("header-consumer-connection-list-request", "typed-consumer-connection-list-request.bin"),
            PopMessageRequestHeader => ("header-pop-request", "typed-pop.bin"),
            PopMessageResponseHeader => ("header-pop-response", "typed-pop-response.bin"),
            PopLiteMessageResponseHeader => ("header-pop-lite-response", "typed-pop-lite-response.bin"),
            AckMessageRequestHeader => ("header-ack-request", "typed-ack.bin"),
            PollingInfoRequestHeader => ("header-polling-request", "typed-polling-request.bin"),
            PollingInfoResponseHeader => ("header-polling-response", "typed-polling-response.bin"),
            PopLiteMessageRequestHeader => ("header-pop-lite-request", "typed-pop-lite-request.bin"),
            LiteSubscriptionCtlRequestHeader => ("header-lite-subscription-request", "typed-lite-subscription-request.bin"),
            NotifyUnsubscribeLiteRequestHeader => ("header-notify-unsubscribe-lite-request", "typed-notify-unsubscribe-lite-request.bin"),
            ChangeInvisibleTimeRequestHeader => ("header-change-invisible-request", "typed-change-invisible.bin"),
            ChangeInvisibleTimeResponseHeader => ("header-change-invisible-response", "typed-change-invisible-response.bin"),
            HeartbeatRequestHeader => ("header-heartbeat-request", "typed-heartbeat-request.bin"),
            ConsumeMessageDirectlyResultRequestHeader => ("header-consume-directly-request", "typed-consume-directly-request.bin"),
            ReplyMessageRequestHeader => ("header-reply-request", "typed-reply.bin"),
            SendMessageResponseHeader => ("header-send-response", "typed-send-response.bin"),
            RecallMessageRequestHeader => ("header-recall-message-request", "typed-recall-request.bin"),
            RecallMessageResponseHeader => ("header-recall-message-response", "typed-recall-response.bin"),
            SendMessageRequestHeaderV2 => ("header-send-v2", "typed-send-v2.bin"),
            SendMessageRequestHeaderV1 => ("header-send-v1", "typed-send-v1.bin"),
        }
    };
}

macro_rules! cases {
    ($($model:ident => ($stem:literal, $reverse:literal),)*) => {
        #[test]
        fn every_java_header_decodes_all_fields() {
            $(
                let raw = java_fields($stem);
                let decoded = $model::decode_fields(&raw)
                    .unwrap_or_else(|error| panic!("{} / {}: {error}", stringify!($model), $stem));
                let mut encoded = HeaderMap::new();
                decoded.encode_fields(&mut encoded);
                assert_eq!(encoded, raw, "{} / {}: decode must preserve every modeled field", stringify!($model), $stem);
            )*
        }

        #[test]
        fn inventory_matches_public_source_and_reverse_evidence() {
            let source = include_str!("../src/header/basic.rs");
            let exports = include_str!("../src/header/mod.rs");
            let generator = include_str!("../../rocketmq-classic-remoting/examples/generate_fixtures.rs");
            let java = include_str!("../../../compat/java-reference/src/main/java/compat/Main.java");
            let implementations: BTreeSet<_> = source.lines().filter_map(|line| {
                line.trim().strip_prefix("impl CommandHeader for ")
                    .and_then(|tail| tail.strip_suffix(" {") )
            }).collect();
            let inventory: BTreeSet<_> = [$(stringify!($model)),*].into_iter().collect();
            assert_eq!(inventory, implementations, "update fixtures when basic.rs adds a CommandHeader");
            assert!(source.contains("pub struct UnlockBatchMqRequestHeader"));
            $(
                assert!(exports.contains(stringify!($model)), "{} must be public", stringify!($model));
                let _ = java_fields($stem);
                assert!(!$reverse.is_empty(), "missing reverse inventory entry: {}", stringify!($model));
                assert!(generator.contains($reverse), "missing Rust frame: {}", $reverse);
                assert!(java.contains(&format!("\"{}\"", $reverse)), "missing Java decoder assertion: {}", $reverse);
            )*
        }
    };
}

inventory!(cases);

#[test]
#[allow(deprecated)]
fn heartbeat_request_header_inherits_java_rpc_surface() {
    let identity: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/request-header-identity.json"
    ))
    .unwrap();
    let java = &identity["identityCases"]["HeartbeatRequestHeader"];

    let mut header = HeartbeatRequestHeader::default();
    assert_eq!(
        header.java_hash_code(),
        java["defaultHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(
        header.java_to_string(),
        java["defaultToString"].as_str().unwrap()
    );

    header.set_namespace(Some("namespace".into()));
    header.set_namespaced(Some(true));
    header.set_bname(Some("broker".into()));
    header.set_oneway(Some(false));
    assert_eq!(header.get_namespace(), Some("namespace"));
    assert_eq!(header.get_namespaced(), Some(true));
    assert_eq!(header.get_broker_name(), Some("broker"));
    assert_eq!(header.get_oneway(), Some(false));
    assert_eq!(header.get_bname(), Some("broker"));
    assert_eq!(
        header.java_hash_code(),
        java["baseHashCode"].as_i64().unwrap() as i32
    );
    assert_eq!(
        header.java_to_string(),
        java["baseToString"].as_str().unwrap()
    );
}

#[test]
fn request_header_api_inventories_match_java_fields_and_accessors() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/request-header-identity.json"
    ))
    .unwrap();
    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    let inventories = fixture["apiInventories"].as_object().unwrap();
    let identities = fixture["identityCases"].as_object().unwrap();
    assert_eq!(inventories.len(), identities.len());

    for (rust_type, inventory) in inventories {
        assert_eq!(
            inventory["publicFields"],
            serde_json::json!([]),
            "Java {rust_type} must not expose public fields"
        );
        let declaration = format!("pub struct {rust_type} {{");
        let struct_body = source
            .split_once(&declaration)
            .unwrap_or_else(|| panic!("missing Rust request header {rust_type}"))
            .1
            .split_once("\n}\n")
            .unwrap()
            .0;
        assert!(
            struct_body
                .lines()
                .all(|line| !line.trim_start().starts_with("pub ")),
            "Rust {rust_type} must not expose public fields"
        );

        let methods = inventory["callablePublicMethods"].as_array().unwrap();
        for signature in methods.iter().filter_map(serde_json::Value::as_str) {
            let (prefix, parameters) = signature.split_once('(').unwrap();
            let name = prefix
                .split_whitespace()
                .last()
                .unwrap()
                .rsplit('.')
                .next()
                .unwrap();
            if matches!(name, "encode" | "decode") {
                let rust_method = if name == "encode" {
                    "encode_fields"
                } else {
                    "decode_fields"
                };
                assert!(
                    trait_source.contains(&format!("fn {rust_method}(")),
                    "Java {rust_type}::{name} has no Rust CommandHeader::{rust_method}"
                );
                continue;
            }
            if matches!(
                name,
                "getAndCheckNotNull" | "writeIfNotNull" | "writeInt" | "writeLong"
            ) {
                let rust_method = java_camel_to_snake(name);
                assert!(
                    rust_header_method_exists(source, trait_source, rust_type, &rust_method),
                    "Java {rust_type}::{name} has no Rust FastCodesHeader::{rust_method}"
                );
                continue;
            }
            if !(name.starts_with("get") || name.starts_with("set") || name.starts_with("is")) {
                continue;
            }
            let parameter_count = if parameters.starts_with(')') {
                0
            } else {
                parameters.matches(',').count() + 1
            };
            if (name.starts_with("get") || name.starts_with("is")) && parameter_count != 0 {
                continue;
            }
            if name.starts_with("set") && parameter_count != 1 {
                continue;
            }
            let rust_method = java_camel_to_snake(name);
            assert!(
                rust_header_method_exists(source, trait_source, rust_type, &rust_method),
                "Java {rust_type}::{name} has no Rust {rust_type}::{rust_method}"
            );
        }
    }
}

#[test]
fn get_topics_by_cluster_header_api_matches_java_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let inventory = &fixture["apiInventories"]["GetTopicsByClusterRequestHeader"];
    assert_eq!(inventory["publicFields"], serde_json::json!([]));
    assert_eq!(
        inventory["publicConstructors"],
        serde_json::json!([
            "org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader()"
        ])
    );
    assert_eq!(
        inventory["declaredPublicMethods"],
        serde_json::json!([
            "java.lang.String org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader.getCluster()",
            "void org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader.checkFields()",
            "void org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader.setCluster(java.lang.String)"
        ])
    );

    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    assert!(rust_header_method_exists(
        source,
        trait_source,
        "GetTopicsByClusterRequestHeader",
        "get_cluster"
    ));
    assert!(rust_header_method_exists(
        source,
        trait_source,
        "GetTopicsByClusterRequestHeader",
        "set_cluster"
    ));
    assert!(rust_header_method_exists(
        source,
        trait_source,
        "GetTopicsByClusterRequestHeader",
        "check_fields"
    ));
}

#[test]
fn get_subscription_group_config_header_api_matches_java_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let inventory = &fixture["apiInventories"]["GetSubscriptionGroupConfigRequestHeader"];
    assert_eq!(inventory["publicFields"], serde_json::json!([]));
    assert_eq!(
        inventory["publicConstructors"],
        serde_json::json!([
            "org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader()"
        ])
    );
    assert_eq!(
        inventory["declaredPublicMethods"],
        serde_json::json!([
            "java.lang.String org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader.getGroup()",
            "void org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader.checkFields()",
            "void org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader.setGroup(java.lang.String)"
        ])
    );
    let callable_methods = inventory["callablePublicMethods"].as_array().unwrap();
    for java_method in [
        "getNamespace",
        "setNamespace",
        "getNamespaced",
        "setNamespaced",
        "getBname",
        "setBname",
        "getBrokerName",
        "setBrokerName",
        "getOneway",
        "setOneway",
    ] {
        assert!(callable_methods.iter().any(|method| {
            method
                .as_str()
                .is_some_and(|signature| signature.contains(&format!(".{java_method}(")))
        }));
    }

    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    for method in [
        "get_group",
        "set_group",
        "check_fields",
        "get_namespace",
        "set_namespace",
        "get_namespaced",
        "set_namespaced",
        "get_bname",
        "set_bname",
        "get_broker_name",
        "set_broker_name",
        "get_oneway",
        "set_oneway",
    ] {
        assert!(rust_header_method_exists(
            source,
            trait_source,
            "GetSubscriptionGroupConfigRequestHeader",
            method
        ));
    }
}

#[test]
fn reset_master_flush_offset_header_api_matches_java_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let inventory = &fixture["apiInventories"]["ResetMasterFlushOffsetHeader"];
    assert_eq!(inventory["publicFields"], serde_json::json!([]));
    assert_eq!(
        inventory["publicConstructors"],
        serde_json::json!([
            "org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader()"
        ])
    );
    assert_eq!(
        inventory["declaredPublicMethods"],
        serde_json::json!([
            "java.lang.Long org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader.getMasterFlushOffset()",
            "void org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader.checkFields()",
            "void org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader.setMasterFlushOffset(java.lang.Long)"
        ])
    );

    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    for method in [
        "get_master_flush_offset",
        "set_master_flush_offset",
        "check_fields",
    ] {
        assert!(rust_header_method_exists(
            source,
            trait_source,
            "ResetMasterFlushOffsetHeader",
            method
        ));
    }
}

#[test]
fn export_rocksdb_config_header_and_enum_api_match_java_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let header = &fixture["apiInventories"]["ExportRocksDBConfigToJsonRequestHeader"];
    assert_eq!(header["publicFields"], serde_json::json!([]));
    let header_methods = header["declaredPublicMethods"].as_array().unwrap();
    for java_method in [
        "fetchConfigType",
        "checkFields",
        "getConfigType",
        "updateConfigType",
        "setConfigType",
    ] {
        assert!(header_methods.iter().any(|method| {
            method
                .as_str()
                .is_some_and(|signature| signature.contains(&format!(".{java_method}(")))
        }));
    }

    let enum_api = &fixture["apiInventories"]["ExportRocksDBConfigType"];
    let enum_methods = enum_api["declaredPublicMethods"].as_array().unwrap();
    for java_method in [
        "getConfigTypeByName",
        "fromString",
        "toString",
        "getTypeName",
    ] {
        assert!(enum_methods.iter().any(|method| {
            method
                .as_str()
                .is_some_and(|signature| signature.contains(&format!(".{java_method}(")))
        }));
    }

    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    for method in [
        "fetch_config_type",
        "update_config_type",
        "get_config_type",
        "set_config_type",
        "check_fields",
    ] {
        assert!(rust_header_method_exists(
            source,
            trait_source,
            "ExportRocksDBConfigToJsonRequestHeader",
            method
        ));
    }
    for method in [
        "get_config_type_by_name",
        "from_string",
        "to_string",
        "get_type_name",
        "value_of",
        "name",
        "java_to_string",
    ] {
        assert!(
            rust_header_method_exists(source, trait_source, "ExportRocksDBConfigType", method),
            "missing Rust enum API method {method}"
        );
    }
    assert!(source.contains("pub const fn values()"));
    assert!(source.contains("pub const fn ordinal(self)"));
}

#[test]
fn check_rocksdb_cq_write_progress_header_matches_java_public_api_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let api = &fixture["apiInventories"]["CheckRocksdbCqWriteProgressRequestHeader"];
    assert_eq!(api["publicFields"], serde_json::json!([]));
    let declared = api["declaredPublicMethods"].as_array().unwrap();
    for method in [
        "getTopic",
        "setTopic",
        "getCheckStoreTime",
        "setCheckStoreTime",
        "checkFields",
    ] {
        assert!(
            declared.iter().any(|signature| {
                signature
                    .as_str()
                    .is_some_and(|signature| signature.contains(&format!(".{method}(")))
            }),
            "missing Java method {method}"
        );
    }

    let source = include_str!("../src/header/basic.rs");
    let exports = include_str!("../src/header/mod.rs");
    for method in [
        "get_topic",
        "set_topic",
        "get_check_store_time",
        "set_check_store_time",
    ] {
        assert!(rust_header_method_exists(
            source,
            exports,
            "CheckRocksdbCqWriteProgressRequestHeader",
            method
        ));
    }
    assert!(source.contains("impl CommandHeader for CheckRocksdbCqWriteProgressRequestHeader"));
}

#[test]
fn write_permission_headers_match_java_public_api_inventory() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/remoting/query-admin-header-decode-modes.json"
    ))
    .unwrap();
    let source = include_str!("../src/header/basic.rs");
    let trait_source = include_str!("../src/header/mod.rs");
    for (java_type, rust_type, methods) in [
        (
            "WipeWritePermOfBrokerRequestHeader",
            "WipeWritePermOfBrokerRequestHeader",
            ["get_broker_name", "set_broker_name", "check_fields"],
        ),
        (
            "AddWritePermOfBrokerRequestHeader",
            "AddWritePermOfBrokerRequestHeader",
            ["get_broker_name", "set_broker_name", "check_fields"],
        ),
        (
            "WipeWritePermOfBrokerResponseHeader",
            "WipeWritePermOfBrokerResponseHeader",
            [
                "get_wipe_topic_count",
                "set_wipe_topic_count",
                "check_fields",
            ],
        ),
        (
            "AddWritePermOfBrokerResponseHeader",
            "AddWritePermOfBrokerResponseHeader",
            ["get_add_topic_count", "set_add_topic_count", "check_fields"],
        ),
    ] {
        let api = &fixture["apiInventories"][java_type];
        assert_eq!(api["publicFields"], serde_json::json!([]), "{java_type}");
        let declared = api["declaredPublicMethods"].as_array().unwrap();
        let java_method_names = if java_type.ends_with("RequestHeader") {
            ["getBrokerName", "setBrokerName", "checkFields"]
        } else if java_type.starts_with("Wipe") {
            ["getWipeTopicCount", "setWipeTopicCount", "checkFields"]
        } else {
            ["getAddTopicCount", "setAddTopicCount", "checkFields"]
        };
        for java_method in java_method_names {
            assert!(
                declared.iter().any(|method| {
                    method
                        .as_str()
                        .is_some_and(|signature| signature.contains(&format!(".{java_method}(")))
                }),
                "missing Java API {java_type}.{java_method}"
            );
        }
        for method in methods {
            assert!(rust_header_method_exists(
                source,
                trait_source,
                rust_type,
                method
            ));
        }
    }
}

#[test]
fn name_server_kv_headers_keep_java_nullable_fields_and_names() {
    let mut get = GetKvConfigRequestHeader::default();
    assert_eq!(get.get_namespace(), None);
    assert_eq!(get.get_key(), None);
    get.set_namespace(Some("tenant".into()));
    get.set_key(Some("key".into()));
    let mut get_fields = HeaderMap::new();
    get.encode_fields(&mut get_fields);
    assert_eq!(
        get_fields,
        HeaderMap::from([
            ("key".into(), "key".into()),
            ("namespace".into(), "tenant".into()),
        ])
    );
    let decoded = GetKvConfigRequestHeader::decode_fields(&get_fields).unwrap();
    assert_eq!(decoded.get_namespace(), Some("tenant"));
    assert_eq!(decoded.get_key(), Some("key"));

    let mut put = PutKvConfigRequestHeader::default();
    assert_eq!(put.get_namespace(), None);
    assert_eq!(put.get_key(), None);
    assert_eq!(put.get_value(), None);
    put.set_namespace(Some("tenant".into()));
    put.set_key(Some("key".into()));
    put.set_value(Some("value".into()));
    let mut put_fields = HeaderMap::new();
    put.encode_fields(&mut put_fields);
    assert_eq!(put_fields.get("value").map(String::as_str), Some("value"));

    let mut list = GetKvListByNamespaceRequestHeader::default();
    assert_eq!(list.get_namespace(), None);
    list.set_namespace(Some("tenant".into()));
    let mut list_fields = HeaderMap::new();
    list.encode_fields(&mut list_fields);
    assert_eq!(
        list_fields,
        HeaderMap::from([("namespace".into(), "tenant".into())])
    );

    let mut response = GetKvConfigResponseHeader::default();
    assert_eq!(response.get_value(), None);
    response.set_value(Some("value".into()));
    let mut response_fields = HeaderMap::new();
    response.encode_fields(&mut response_fields);
    assert_eq!(
        response_fields,
        HeaderMap::from([("value".into(), "value".into())])
    );
    assert_eq!(
        GetKvConfigResponseHeader::decode_fields(&response_fields)
            .unwrap()
            .get_value(),
        Some("value")
    );
}

#[test]
fn consume_stats_topic_list_helpers_match_java_header_behavior() {
    let mut header =
        GetConsumeStatsRequestHeader::decode_fields(&java_fields("header-consume-stats-request"))
            .unwrap();
    assert_eq!(header.fetch_topic_list(), ["orders", "payments"]);

    let current = header.get_topic_list().map(str::to_owned);
    header.update_topic_list(None);
    header.update_topic_list(Some(&[]));
    assert_eq!(header.get_topic_list(), current.as_deref());

    let topics = [Some("orders".to_owned()), None, Some("payments".to_owned())];
    header.update_topic_list(Some(&topics));
    assert_eq!(header.get_topic_list(), Some("orders;null;payments;"));
    assert_eq!(header.fetch_topic_list(), ["orders", "null", "payments"]);
}
