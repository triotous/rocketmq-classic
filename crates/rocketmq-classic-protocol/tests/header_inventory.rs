//! Exhaustive Java 5.5.1 fixture inventory for public basic CommandHeader models.
use std::collections::BTreeSet;

use rocketmq_classic_protocol::header::*;

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
            ResetOffsetRequestHeader => ("header-reset-offset-request", "typed-reset-offset.bin"),
            GetConsumerStatusRequestHeader => ("header-consumer-status-request", "typed-consumer-status-request.bin"),
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
