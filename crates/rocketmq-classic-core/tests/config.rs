use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_core::config::{AccessChannel, AccessChannelLookupError, ClientConfig};
use rocketmq_classic_protocol::codes::LanguageCode;
use serde_json::Value;

fn optional(value: &Value) -> Option<String> {
    value.as_str().map(str::to_owned)
}

fn client_config_snapshot(config: &mut ClientConfig) -> Value {
    serde_json::json!({
        "namesrvAddr": config.get_namesrv_addr(),
        "clientIP": config.client_ip().map(str::to_owned),
        "instanceName": config.instance_name().map(str::to_owned),
        "clientCallbackExecutorThreads": config.client_callback_executor_threads(),
        "namespace": config.get_namespace().unwrap().map(str::to_owned),
        "namespaceV2": config.namespace_v2().map(str::to_owned),
        "accessChannel": config.access_channel().map(|channel| match channel {
            AccessChannel::Local => "LOCAL",
            AccessChannel::Cloud => "CLOUD",
        }),
        "pollNameServerInterval": config.poll_name_server_interval(),
        "heartbeatBrokerInterval": config.heartbeat_broker_interval(),
        "persistConsumerOffsetInterval": config.persist_consumer_offset_interval(),
        "pullTimeDelayMillsWhenException": config.pull_time_delay_millis_when_exception(),
        "traceMsgBatchNum": config.trace_msg_batch_num(),
        "unitMode": config.unit_mode(),
        "unitName": config.unit_name().map(str::to_owned),
        "decodeReadBody": config.decode_read_body(),
        "decodeDecompressBody": config.decode_decompress_body(),
        "vipChannelEnabled": config.vip_channel_enabled(),
        "useHeartbeatV2": config.use_heartbeat_v2(),
        "useTLS": config.use_tls(),
        "socksProxyConfig": config.socks_proxy_config(),
        "mqClientApiTimeout": config.mq_client_api_timeout(),
        "detectTimeout": config.detect_timeout(),
        "detectInterval": config.detect_interval(),
        "language": config.language().map(|language| match language {
            LanguageCode::Cpp => "CPP",
            _ => "OTHER",
        }),
        "enableStreamRequestType": config.enable_stream_request_type(),
        "sendLatencyEnable": config.send_latency_enable(),
        "startDetectorEnable": config.start_detector_enable(),
        "enableHeartbeatChannelEventListener": config.enable_heartbeat_channel_event_listener(),
        "enableConcurrentHeartbeat": config.enable_concurrent_heartbeat(),
        "concurrentHeartbeatThreadPoolSize": config.concurrent_heartbeat_thread_pool_size(),
        "enableTrace": config.enable_trace(),
        "traceTopic": config.trace_topic().map(str::to_owned),
        "maxPageSizeInGetMetadata": config.max_page_size_in_get_metadata(),
    })
}

fn client_config_from_java_text_fields(fields: &Value) -> ClientConfig {
    let string = |name: &str| fields[name].as_str().map(str::to_owned);
    let i32_value = |name: &str| i32::try_from(fields[name].as_i64().unwrap()).unwrap();
    let mut config = ClientConfig::new("fixture-ip");
    config.namesrv_addr = string("namesrvAddr");
    config.client_ip = string("clientIP");
    config.instance_name = string("instanceName");
    config.client_callback_executor_threads = i32_value("clientCallbackExecutorThreads");
    config.namespace = string("namespace");
    config.namespace_v2 = string("namespaceV2");
    config.access_channel = match fields["accessChannel"].as_str() {
        Some("LOCAL") => Some(AccessChannel::Local),
        Some("CLOUD") => Some(AccessChannel::Cloud),
        Some(other) => panic!("unknown Java AccessChannel {other}"),
        None => None,
    };
    config.poll_name_server_interval = i32_value("pollNameServerInterval");
    config.heartbeat_broker_interval = i32_value("heartbeatBrokerInterval");
    config.persist_consumer_offset_interval = i32_value("persistConsumerOffsetInterval");
    config.pull_time_delay_millis_when_exception =
        fields["pullTimeDelayMillsWhenException"].as_i64().unwrap();
    config.trace_msg_batch_num = i32_value("traceMsgBatchNum");
    config.unit_mode = fields["unitMode"].as_bool().unwrap();
    config.unit_name = string("unitName");
    config.decode_read_body = fields["decodeReadBody"].as_bool().unwrap();
    config.decode_decompress_body = fields["decodeDecompressBody"].as_bool().unwrap();
    config.vip_channel_enabled = fields["vipChannelEnabled"].as_bool().unwrap();
    config.use_heartbeat_v2 = fields["useHeartbeatV2"].as_bool().unwrap();
    config.use_tls = fields["useTLS"].as_bool().unwrap();
    config.socks_proxy_config = string("socksProxyConfig");
    config.mq_client_api_timeout = i32_value("mqClientApiTimeout");
    config.detect_timeout = i32_value("detectTimeout");
    config.detect_interval = i32_value("detectInterval");
    config.language = match fields["language"].as_str() {
        Some("JAVA") => Some(LanguageCode::Java),
        Some("CPP") => Some(LanguageCode::Cpp),
        Some("DOTNET") => Some(LanguageCode::Dotnet),
        Some("PYTHON") => Some(LanguageCode::Python),
        Some("DELPHI") => Some(LanguageCode::Delphi),
        Some("ERLANG") => Some(LanguageCode::Erlang),
        Some("RUBY") => Some(LanguageCode::Ruby),
        Some("OTHER") => Some(LanguageCode::Other),
        Some("HTTP") => Some(LanguageCode::Http),
        Some("GO") => Some(LanguageCode::Go),
        Some("PHP") => Some(LanguageCode::Php),
        Some("OMS") => Some(LanguageCode::Oms),
        Some("RUST") => Some(LanguageCode::Rust),
        Some("NODE_JS") => Some(LanguageCode::NodeJs),
        Some(other) => panic!("unknown Java LanguageCode {other}"),
        None => None,
    };
    config.enable_stream_request_type = fields["enableStreamRequestType"].as_bool().unwrap();
    config.send_latency_enable = fields["sendLatencyEnable"].as_bool().unwrap();
    config.start_detector_enable = fields["startDetectorEnable"].as_bool().unwrap();
    config.enable_heartbeat_channel_event_listener = fields["enableHeartbeatChannelEventListener"]
        .as_bool()
        .unwrap();
    config.enable_concurrent_heartbeat = fields["enableConcurrentHeartbeat"].as_bool().unwrap();
    config.concurrent_heartbeat_thread_pool_size = i32_value("concurrentHeartbeatThreadPoolSize");
    config.enable_trace = fields["enableTrace"].as_bool().unwrap();
    config.trace_topic = string("traceTopic");
    config.max_page_size_in_get_metadata = i32_value("maxPageSizeInGetMetadata");
    if fields["namespaceInitialized"].as_bool().unwrap() {
        config.set_namespace(string("namespace"));
    }
    config
}

#[test]
fn default_constructor_selects_a_local_ip_before_identity_construction() {
    let config = ClientConfig::default();
    let address = config.client_ip.as_deref().unwrap();
    assert!(!address.is_empty());
    if address.starts_with('[') {
        address
            .trim_start_matches('[')
            .trim_end_matches(']')
            .split('%')
            .next()
            .unwrap()
            .parse::<std::net::Ipv6Addr>()
            .unwrap();
    } else {
        address.parse::<std::net::Ipv4Addr>().unwrap();
    }
    assert!(config.build_client_id().starts_with(address));
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
async fn nullable_access_and_socks_settings_match_invoked_java_copy_and_construction() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["nullableSettings"];
    let mut source = ClientConfig::new("nullable-settings");
    source.namesrv_addr = Some(String::new());
    source.access_channel = None;
    source.socks_proxy_config = None;
    source.language = None;
    assert_eq!(source.access_channel.is_none(), java["inputAccessNull"]);
    assert_eq!(source.socks_proxy_config.is_none(), java["inputSocksNull"]);
    assert_eq!(source.language.is_none(), java["inputLanguageNull"]);
    let copy = source.clone_client_config();
    assert_eq!(copy.access_channel, Some(AccessChannel::Local));
    assert_eq!(java["cloneAccess"], "LOCAL");
    assert_eq!(copy.socks_proxy_config.is_none(), java["cloneSocksNull"]);
    assert_eq!(copy.language.is_none(), java["cloneLanguageNull"]);
    let mut receiver = ClientConfig::new("receiver");
    receiver.access_channel = Some(AccessChannel::Cloud);
    receiver.reset_client_config(&source);
    assert_eq!(receiver.access_channel, Some(AccessChannel::Cloud));
    assert_eq!(java["resetAccess"], "CLOUD");
    assert_eq!(
        receiver.socks_proxy_config.is_none(),
        java["resetSocksNull"]
    );
    assert_eq!(receiver.language.is_none(), java["resetLanguageNull"]);
    receiver.access_channel = None;
    receiver.reset_client_config(&source);
    assert!(receiver.access_channel.is_none());
    let runtime = rocketmq_classic_core::runtime::ClientRuntimeManager::new()
        .get_or_create(&source)
        .await;
    assert_eq!(runtime.is_ok(), java["runtimeConstructed"]);
    let runtime = runtime.unwrap();
    assert_eq!(
        runtime
            .config()
            .read()
            .unwrap()
            .socks_proxy_config
            .is_none(),
        java["runtimeSocksNull"]
    );
    assert_eq!(
        runtime.config().read().unwrap().language.is_none(),
        java["runtimeLanguageNull"]
    );
}

#[tokio::test]
async fn callback_thread_constructor_acceptance_and_raw_values_match_invoked_java() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    for java in fixture["callbackThreadsConstruction"].as_array().unwrap() {
        let threads = i32::try_from(java["input"].as_i64().unwrap()).unwrap();
        let mut config = ClientConfig::new(format!("callback-threads-{threads}"));
        config.namesrv_addr = Some(String::new());
        config.client_callback_executor_threads = threads;
        let result = rocketmq_classic_core::runtime::ClientRuntimeManager::new()
            .get_or_create(&config)
            .await;
        assert_eq!(result.is_ok(), java["outcome"] == "OK", "{java}");
        assert_eq!(
            i64::from(
                result
                    .unwrap()
                    .config()
                    .read()
                    .unwrap()
                    .client_callback_executor_threads
            ),
            java["raw"].as_i64().unwrap()
        );
    }
}

#[tokio::test]
async fn socks_construction_matches_invoked_java_empty_and_whitespace_boundaries() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    for (index, java) in fixture["socksConstruction"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let mut config = ClientConfig::new(format!("socks-construction-{index}"));
        config.namesrv_addr = Some(String::new());
        config.socks_proxy_config = optional(&java["input"]);
        let result = rocketmq_classic_core::runtime::ClientRuntimeManager::new()
            .get_or_create(&config)
            .await;
        assert_eq!(result.is_ok(), java["outcome"] == "OK", "{java}");
        if let Ok(runtime) = result {
            assert_eq!(java["rawPreserved"], true);
            assert_eq!(
                runtime.config().read().unwrap().socks_proxy_config.clone(),
                config.socks_proxy_config
            );
        }
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
    assert_eq!(
        runtime.config().read().unwrap().namesrv_addr,
        config.namesrv_addr
    );
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

#[tokio::test]
async fn manager_configuration_copy_matches_invoked_java_without_mutating_caller() {
    use rocketmq_classic_core::runtime::ClientRuntimeManager;
    use std::sync::Arc;

    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managed"];
    let mut config = ClientConfig::new("manager-copy");
    config.trace_msg_batch_num = 99;
    config.max_page_size_in_get_metadata = 77;
    config.access_channel = Some(AccessChannel::Cloud);
    config.namespace_v2 = Some("v2".into());
    config.enable_concurrent_heartbeat = true;
    config.concurrent_heartbeat_thread_pool_size = 7;
    config.set_namesrv_addr(Some("MQ_INST_a_b.example:9876".into()));
    config.set_namespace(None);
    let manager = ClientRuntimeManager::new();
    let runtime = manager.get_or_create(&config).await.unwrap();
    // Full Rust Clone preserves the observed lazy state; another Java-style
    // clone here would reset it again and hide a construction-path mismatch.
    let mut actual = runtime.config().read().unwrap().clone();
    assert_eq!(
        i64::from(actual.trace_msg_batch_num),
        java["traceMsgBatchNum"]
    );
    assert_eq!(
        i64::from(actual.max_page_size_in_get_metadata),
        java["maxPageSizeInGetMetadata"]
    );
    assert_eq!(actual.access_channel, Some(AccessChannel::Local));
    assert_eq!(java["accessChannel"], "LOCAL");
    assert_eq!(actual.namespace_v2.as_deref(), java["namespaceV2"].as_str());
    assert_eq!(
        actual.enable_concurrent_heartbeat,
        java["enableConcurrentHeartbeat"]
    );
    assert_eq!(
        i64::from(actual.concurrent_heartbeat_thread_pool_size),
        java["concurrentHeartbeatThreadPoolSize"]
    );
    assert_eq!(java["configGetterSameObject"], true);
    assert_eq!(java["configMutationVisible"], true);
    runtime.config().write().unwrap().mq_client_api_timeout = 4321;
    assert_eq!(runtime.config().read().unwrap().mq_client_api_timeout, 4321);
    assert_eq!(config.mq_client_api_timeout, 3_000);
    assert_eq!(actual.get_namespace().unwrap(), java["namespace"].as_str());
    assert_eq!(
        config.get_namespace().unwrap().is_none(),
        java["inputNamespaceStillNull"]
    );
    assert_eq!(config.trace_msg_batch_num, 99);
    assert_eq!(config.max_page_size_in_get_metadata, 77);
    assert_eq!(config.access_channel, Some(AccessChannel::Cloud));
    assert_eq!(
        Arc::ptr_eq(&runtime, &manager.get_or_create(&config).await.unwrap()),
        java["sharedIdentity"]
    );
    config.trace_msg_batch_num = 123;
    config.max_page_size_in_get_metadata = 456;
    config.access_channel = Some(AccessChannel::Local);
    assert!(Arc::ptr_eq(
        &runtime,
        &manager.get_or_create(&config).await.unwrap()
    ));
}

#[tokio::test]
async fn runtime_client_id_remains_constructor_snapshot_after_config_mutation() {
    use rocketmq_classic_core::runtime::ClientRuntimeManager;

    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["managerSharing"];
    let mut config = ClientConfig::new("198.51.100.223");
    config.instance_name = Some("phase6-manager-sharing".into());
    config.namesrv_addr = Some(String::new());
    let runtime = ClientRuntimeManager::new()
        .get_or_create(&config)
        .await
        .unwrap();

    assert_eq!(
        runtime.client_id(),
        java["clientIdBeforeConfigMutation"].as_str().unwrap()
    );
    {
        let retained_config = runtime.config();
        let mut retained = retained_config.write().unwrap();
        retained.client_ip = Some("203.0.113.44".into());
        retained.instance_name = Some("mutated-runtime-instance".into());
        retained.unit_name = Some("mutated-runtime-unit".into());
        assert_eq!(
            retained.build_client_id(),
            java["configDerivedClientIdAfterMutation"].as_str().unwrap()
        );
    }
    assert_eq!(
        runtime.client_id(),
        java["clientIdAfterConfigMutation"].as_str().unwrap()
    );
}

#[tokio::test]
async fn active_nameserver_addresses_remain_constructor_snapshot_after_config_mutation() {
    use rocketmq_classic_core::runtime::ClientRuntimeManager;

    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    for (index, case) in fixture["nameServerAddressSnapshotCases"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let mut config = ClientConfig::new("192.0.2.80");
        config.instance_name = Some(format!("phase6-nameserver-snapshot-{index}"));
        config.namesrv_addr = case["configuredAtConstruction"].as_str().map(str::to_owned);
        let runtime = ClientRuntimeManager::new()
            .get_or_create(&config)
            .await
            .unwrap();
        let java_addresses = |value: &Value| {
            value.as_array().map(|addresses| {
                addresses
                    .iter()
                    .map(|address| address.as_str().unwrap().to_owned())
                    .collect::<Vec<_>>()
            })
        };
        assert_eq!(
            runtime.remoting().get_name_server_address_list(),
            java_addresses(&case["activeAddressesBeforeMutation"]),
            "before mutation, case {index}"
        );

        let changed = case["configAfterMutation"].as_str().unwrap().to_owned();
        runtime
            .config()
            .write()
            .unwrap()
            .set_namesrv_addr(Some(changed));
        assert_eq!(
            runtime.remoting().get_name_server_address_list(),
            java_addresses(&case["activeAddressesAfterMutation"]),
            "after mutation, case {index}"
        );
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
    assert_eq!(config.access_channel, Some(AccessChannel::Local));
    assert!(!config.enable_concurrent_heartbeat);
    assert!(config.enable_heartbeat_channel_event_listener);
    config.trace_msg_batch_num = 99;
    config.max_page_size_in_get_metadata = 77;
    config.access_channel = Some(AccessChannel::Cloud);
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
    assert_eq!(copy.access_channel, Some(AccessChannel::Local));
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
    reset.access_channel = Some(AccessChannel::Cloud);
    reset.reset_client_config(&config);
    assert_eq!(reset.trace_msg_batch_num, 42);
    assert_eq!(reset.max_page_size_in_get_metadata, 43);
    assert_eq!(reset.access_channel, Some(AccessChannel::Cloud));
    assert_eq!(reset.build_client_id(), config.build_client_id());
}

#[test]
fn public_property_names_match_pinned_client_config_constants() {
    assert_eq!(
        ClientConfig::SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY,
        "com.rocketmq.sendMessageWithVIPChannel"
    );
    assert_eq!(
        ClientConfig::SOCKS_PROXY_CONFIG,
        "com.rocketmq.socks.proxy.config"
    );
    assert_eq!(ClientConfig::DECODE_READ_BODY, "com.rocketmq.read.body");
    assert_eq!(
        ClientConfig::DECODE_DECOMPRESS_BODY,
        "com.rocketmq.decompress.body"
    );
    assert_eq!(
        ClientConfig::SEND_LATENCY_ENABLE,
        "com.rocketmq.sendLatencyEnable"
    );
    assert_eq!(
        ClientConfig::START_DETECTOR_ENABLE,
        "com.rocketmq.startDetectorEnable"
    );
    assert_eq!(ClientConfig::HEART_BEAT_V2, "com.rocketmq.heartbeat.v2");
}

#[test]
fn client_config_to_string_matches_invoked_java() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    for case in fixture["clientConfigToString"].as_array().unwrap() {
        let config = client_config_from_java_text_fields(&case["input"]);
        assert_eq!(
            config.to_string(),
            case["toString"].as_str().unwrap(),
            "{}",
            case["case"]
        );
    }
}

#[test]
fn access_channel_enum_methods_match_invoked_java() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let cases = fixture["accessChannelMethodCases"].as_array().unwrap();
    let values = AccessChannel::values();
    for case in cases {
        match case["case"].as_str().unwrap() {
            "constant" => {
                let name = case["name"].as_str().unwrap();
                let channel = AccessChannel::from_java_name(Some(name)).unwrap();
                assert_eq!(channel.name(), name, "{case}");
                assert_eq!(
                    channel.ordinal(),
                    case["ordinal"].as_i64().unwrap() as i32,
                    "{case}"
                );
                assert_eq!(
                    channel.to_string(),
                    case["text"].as_str().unwrap(),
                    "{case}"
                );
                assert_eq!(channel.name(), case["valueOf"].as_str().unwrap(), "{case}");
                assert_eq!(channel, values[channel.ordinal() as usize], "{case}");
                let next = values[(channel.ordinal() as usize + 1) % values.len()];
                assert_eq!(
                    channel.java_compare_to(next),
                    case["compareNext"].as_i64().unwrap() as i32,
                    "{case}"
                );
                assert_eq!(case["values"], serde_json::json!(["LOCAL", "CLOUD"]));
                assert_eq!(case["valuesDefensiveCopy"], true);
            }
            "lookup" => {
                let name = case["input"].as_str();
                match AccessChannel::from_java_name(name) {
                    Ok(channel) => {
                        assert_eq!(case["outcome"], "OK", "{case}");
                        assert_eq!(case["decoded"], channel.name(), "{case}");
                    }
                    Err(AccessChannelLookupError::NullName) => {
                        assert_eq!(case["outcome"], "NullPointerException", "{case}");
                        assert_eq!(case["message"], "Name is null", "{case}");
                    }
                    Err(AccessChannelLookupError::UnknownName(name)) => {
                        assert_eq!(case["outcome"], "IllegalArgumentException", "{case}");
                        assert_eq!(
                            case["message"],
                            format!(
                                "No enum constant org.apache.rocketmq.client.AccessChannel.{name}"
                            )
                        );
                    }
                }
            }
            kind => panic!("unknown Java AccessChannel case: {kind}"),
        }
    }
}

fn message_queue_value(queue: &MessageQueue) -> Value {
    serde_json::json!({
        "topic": queue.topic(),
        "brokerName": queue.broker_name(),
        "queueId": queue.queue_id(),
    })
}

fn message_queue_rows(queues: Option<&[Option<MessageQueue>]>) -> Value {
    match queues {
        None => Value::Null,
        Some(queues) => Value::Array(
            queues
                .iter()
                .map(|queue| {
                    queue
                        .as_ref()
                        .map(message_queue_value)
                        .unwrap_or(Value::Null)
                })
                .collect(),
        ),
    }
}

#[test]
fn message_queue_setters_match_invoked_java() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["messageQueueSetters"];
    let mut queue = MessageQueue::new_nullable(None, None, 0);
    assert_eq!(message_queue_value(&queue), java["defaults"]);
    queue.set_topic(Some("topic".into()));
    queue.set_broker_name(Some("broker".into()));
    queue.set_queue_id(-17);
    assert_eq!(message_queue_value(&queue), java["populated"]);
    queue.set_topic(None);
    queue.set_broker_name(None);
    queue.set_queue_id(i32::MIN);
    assert_eq!(message_queue_value(&queue), java["nullable"]);
}

#[test]
fn queues_with_namespace_matches_invoked_java_collection_and_null_behavior() {
    use rocketmq_classic_core::config::ConfigError;

    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    for case in fixture["queuesWithNamespace"].as_array().unwrap() {
        let mut config = ClientConfig::new("fixture-ip");
        config.set_namespace(optional(&case["namespace"]));
        let mut queues: Option<Vec<Option<MessageQueue>>> =
            serde_json::from_value(case["inputQueues"].clone()).unwrap();
        let input = message_queue_rows(queues.as_deref());
        assert_eq!(input, case["inputQueues"], "{} input", case["case"]);
        let input_address = queues.as_mut().map(|rows| rows.as_mut_ptr() as usize);
        let (outcome, same_collection, output) =
            match config.queues_with_namespace(queues.as_deref_mut()) {
                Ok(returned) => {
                    let returned_address = returned.as_ref().map(|rows| rows.as_ptr() as usize);
                    (
                        "OK",
                        returned_address == input_address,
                        message_queue_rows(returned.as_deref()),
                    )
                }
                Err(ConfigError::NullQueueCollection | ConfigError::NullMessageQueue) => (
                    "NullPointerException",
                    false,
                    message_queue_rows(queues.as_deref()),
                ),
                Err(error) => panic!("{} returned unexpected error: {error}", case["case"]),
            };
        assert_eq!(
            outcome,
            case["outcome"].as_str().unwrap(),
            "{}",
            case["case"]
        );
        assert_eq!(
            same_collection, case["returnedSameCollection"],
            "{}",
            case["case"]
        );
        assert_eq!(output, case["outputQueues"], "{}", case["case"]);
    }
}

#[test]
fn clone_and_reset_copy_every_public_setting_except_java_omissions() {
    use rocketmq_classic_protocol::codes::LanguageCode;

    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/runtime/client-config.json")).unwrap();
    let java = &fixture["copyFields"];
    // Distinct from every constructor default, including signed-width limits.
    let mut source = ClientConfig::new("changed-ip");
    source.set_namesrv_addr(Some("changed-nameserver".into()));
    source.set_client_ip(None);
    source.set_instance_name(None);
    source.set_client_callback_executor_threads(i32::MIN);
    source.set_namespace(Some("changed-namespace".into()));
    source.set_namespace_v2(Some("changed-v2".into()));
    source.set_access_channel(Some(AccessChannel::Cloud));
    source.set_poll_name_server_interval(i32::MIN);
    source.set_heartbeat_broker_interval(i32::MAX);
    source.set_persist_consumer_offset_interval(-1);
    source.set_pull_time_delay_millis_when_exception(i64::MIN);
    source.set_trace_msg_batch_num(-10);
    source.set_unit_mode(true);
    source.set_unit_name(Some("changed-unit".into()));
    source.set_decode_read_body(false);
    source.set_decode_decompress_body(false);
    source.set_vip_channel_enabled(true);
    source.set_use_heartbeat_v2(true);
    source.set_use_tls(true);
    source.set_socks_proxy_config(Some("changed-proxy".into()));
    source.set_mq_client_api_timeout(i32::MIN);
    source.set_detect_timeout(-200);
    source.set_detect_interval(i32::MAX);
    source.set_language(Some(LanguageCode::Cpp));
    source.set_enable_stream_request_type(true);
    source.set_send_latency_enable(true);
    source.set_start_detector_enable(true);
    source.set_enable_heartbeat_channel_event_listener(false);
    source.set_enable_concurrent_heartbeat(true);
    source.set_concurrent_heartbeat_thread_pool_size(-7);
    source.set_enable_trace(true);
    source.set_trace_topic(Some("changed-trace".into()));
    source.set_max_page_size_in_get_metadata(-2000);
    let source_snapshot = client_config_snapshot(&mut source);
    assert_eq!(source_snapshot, java["source"]);
    assert_eq!(source_snapshot, java["sourceGetters"]);

    let mut expected_clone = source.clone();
    // reset of the source namespace state is part of clone construction, not
    // a plain Rust Clone operation (covered independently by the private test).
    expected_clone.set_namesrv_addr(source.namesrv_addr.clone());
    expected_clone.access_channel = Some(AccessChannel::Local);
    expected_clone.trace_msg_batch_num = 10;
    expected_clone.max_page_size_in_get_metadata = 2000;
    let mut copy = source.clone_client_config();
    assert_eq!(copy, expected_clone);
    let clone_snapshot = client_config_snapshot(&mut copy);
    assert_eq!(clone_snapshot, java["clone"]);
    assert_eq!(clone_snapshot, java["cloneGetters"]);

    let mut receiver = ClientConfig::new("receiver");
    receiver.set_namespace(None);
    receiver.set_access_channel(Some(AccessChannel::Cloud));
    receiver.set_trace_msg_batch_num(42);
    receiver.set_max_page_size_in_get_metadata(43);
    let mut expected_reset = source.clone();
    expected_reset.access_channel = receiver.access_channel;
    expected_reset.trace_msg_batch_num = receiver.trace_msg_batch_num;
    expected_reset.max_page_size_in_get_metadata = receiver.max_page_size_in_get_metadata;
    receiver.reset_client_config(&source);
    assert_eq!(receiver, expected_reset);
    let reset_snapshot = client_config_snapshot(&mut receiver);
    assert_eq!(reset_snapshot, java["reset"]);
    assert_eq!(reset_snapshot, java["resetGetters"]);
    assert_eq!(source.pull_time_delay_millis_when_exception(), i64::MIN);
}
