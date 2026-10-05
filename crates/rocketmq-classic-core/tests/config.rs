use rocketmq_classic_core::config::{AccessChannel, ClientConfig};
use serde_json::Value;

fn optional(value: &Value) -> Option<String> {
    value.as_str().map(str::to_owned)
}

#[test]
fn endpoint_namespace_and_resource_helpers_match_invoked_java() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    for case in fixture["endpoints"].as_array().unwrap() {
        let mut config = ClientConfig::new("ip");
        config.set_namesrv_addr(optional(&case["endpoint"]));
        assert_eq!(config.get_namesrv_addr(), optional(&case["address"]));
        match config.get_namespace() {
            Ok(namespace) => {
                assert_eq!(case["outcome"], "OK");
                assert_eq!(namespace, case["namespace"].as_str());
            }
            Err(_) => assert_eq!(case["outcome"], "StringIndexOutOfBoundsException"),
        }
    }
    for case in fixture["resources"].as_array().unwrap() {
        let mut config = ClientConfig::new("ip");
        config.set_namespace(optional(&case["namespace"]));
        assert_eq!(
            config.with_namespace(case["resource"].as_str()).unwrap(),
            optional(&case["with"])
        );
        assert_eq!(
            config.without_namespace(case["resource"].as_str()).unwrap(),
            optional(&case["without"])
        );
    }
    for outcome in fixture["pid"].as_object().unwrap().values() {
        assert_eq!(outcome, true);
    }
}

#[tokio::test]
async fn runtime_uses_endpoint_address_getter_without_rewriting_raw_configuration() {
    let mut config = ClientConfig::new("192.0.2.6");
    config.set_namesrv_addr(Some("http://MQ_INST_a_b.example:9876".into()));
    let runtime = rocketmq_classic_core::runtime::ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();
    assert_eq!(
        runtime.remoting().get_name_server_address_list().unwrap(),
        ["MQ_INST_a_b.example:9876"]
    );
    assert_eq!(runtime.config().namesrv_addr, config.namesrv_addr);
}

#[test]
fn client_identity_matches_invoked_java_including_unicode_blanks_and_nulls() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    for case in fixture["identities"].as_array().unwrap() {
        let mut config = ClientConfig::new("192.0.2.4");
        config.client_ip = optional(&case["ip"]);
        config.instance_name = optional(&case["instance"]);
        config.unit_name = optional(&case["unit"]);
        config.namespace = Some("must-not-affect-id".into());
        config.enable_stream_request_type = case["stream"].as_bool().unwrap();
        assert_eq!(config.build_client_id(), case["id"].as_str().unwrap());
    }
}

#[test]
fn centralized_defaults_and_java_clone_copy_exact_fields() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let mut config = ClientConfig::new("192.0.2.4");
    let defaults = &fixture["defaults"];
    for (key, actual) in [
        (
            "pollNameServerInterval",
            i64::from(config.poll_name_server_interval),
        ),
        (
            "heartbeatBrokerInterval",
            i64::from(config.heartbeat_broker_interval),
        ),
        (
            "persistConsumerOffsetInterval",
            i64::from(config.persist_consumer_offset_interval),
        ),
        (
            "pullTimeDelayMillsWhenException",
            config.pull_time_delay_millis_when_exception,
        ),
        (
            "mqClientApiTimeout",
            i64::from(config.mq_client_api_timeout),
        ),
        ("detectTimeout", i64::from(config.detect_timeout)),
        ("detectInterval", i64::from(config.detect_interval)),
        ("traceMsgBatchNum", i64::from(config.trace_msg_batch_num)),
        (
            "maxPageSizeInGetMetadata",
            i64::from(config.max_page_size_in_get_metadata),
        ),
    ] {
        assert_eq!(actual, defaults[key].as_i64().unwrap(), "{key}");
    }
    assert_eq!(config.access_channel, AccessChannel::Local);
    assert!(!config.enable_concurrent_heartbeat);
    assert!(config.enable_heartbeat_channel_event_listener);
    config.trace_msg_batch_num = 99;
    config.max_page_size_in_get_metadata = 77;
    config.access_channel = AccessChannel::Cloud;
    config.namespace_v2 = Some("v2".into());
    config.enable_concurrent_heartbeat = true;
    config.concurrent_heartbeat_thread_pool_size = 7;
    let copy = config.clone_client_config();
    let java = &fixture["clone"];
    assert_eq!(
        i64::from(copy.trace_msg_batch_num),
        java["traceMsgBatchNum"].as_i64().unwrap()
    );
    assert_eq!(
        i64::from(copy.max_page_size_in_get_metadata),
        java["maxPageSizeInGetMetadata"].as_i64().unwrap()
    );
    assert_eq!(copy.access_channel, AccessChannel::Local);
    assert_eq!(copy.namespace_v2.as_deref(), java["namespaceV2"].as_str());
    assert_eq!(
        copy.enable_concurrent_heartbeat,
        java["enableConcurrentHeartbeat"].as_bool().unwrap()
    );
    assert_eq!(
        i64::from(copy.concurrent_heartbeat_thread_pool_size),
        java["concurrentHeartbeatThreadPoolSize"].as_i64().unwrap()
    );
    let mut reset = ClientConfig::new("other");
    reset.trace_msg_batch_num = 42;
    reset.max_page_size_in_get_metadata = 43;
    reset.access_channel = AccessChannel::Cloud;
    reset.reset_client_config(&config);
    assert_eq!(reset.trace_msg_batch_num, 42);
    assert_eq!(reset.max_page_size_in_get_metadata, 43);
    assert_eq!(reset.access_channel, AccessChannel::Cloud);
    assert_eq!(reset.build_client_id(), config.build_client_id());
}
