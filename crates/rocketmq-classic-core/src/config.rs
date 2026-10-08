//! Settings of pinned Java 5.5.1 `org.apache.rocketmq.client.ClientConfig`.
//! Durations retain Java signed widths and milliseconds. Validation happens
//! when the owning runtime/service uses a setting, not in Java's plain setters.
//! Use namespace/NameServer setters to preserve Java's lazy-cache transitions.

use rocketmq_classic_common::message::MessageQueue;
use rocketmq_classic_protocol::codes::LanguageCode;
use std::collections::BTreeSet;

mod local_address;
pub(crate) mod namespace;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("local client address selection failed: {0}")]
    LocalAddress(String),
    #[error("instance endpoint path precedes its instance boundary")]
    InvalidInstanceEndpoint,
    #[error("instance name is null")]
    NullInstanceName,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AccessChannel {
    #[default]
    Local,
    Cloud,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub namesrv_addr: Option<String>,
    pub client_ip: Option<String>,
    pub instance_name: Option<String>,
    pub client_callback_executor_threads: i32,
    pub namespace: Option<String>,
    namespace_initialized: bool,
    pub namespace_v2: Option<String>,
    pub access_channel: Option<AccessChannel>,
    pub poll_name_server_interval: i32,
    pub heartbeat_broker_interval: i32,
    pub persist_consumer_offset_interval: i32,
    pub pull_time_delay_millis_when_exception: i64,
    pub trace_msg_batch_num: i32,
    pub unit_mode: bool,
    pub unit_name: Option<String>,
    pub decode_read_body: bool,
    pub decode_decompress_body: bool,
    pub vip_channel_enabled: bool,
    pub use_heartbeat_v2: bool,
    pub use_tls: bool,
    pub socks_proxy_config: Option<String>,
    pub mq_client_api_timeout: i32,
    pub detect_timeout: i32,
    pub detect_interval: i32,
    pub language: Option<LanguageCode>,
    pub enable_stream_request_type: bool,
    pub send_latency_enable: bool,
    pub start_detector_enable: bool,
    pub enable_heartbeat_channel_event_listener: bool,
    pub enable_concurrent_heartbeat: bool,
    pub concurrent_heartbeat_thread_pool_size: i32,
    pub enable_trace: bool,
    pub trace_topic: Option<String>,
    pub max_page_size_in_get_metadata: i32,
}

impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("client_id", &self.build_client_id())
            .field("use_tls", &self.use_tls)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let access_channel = match self.access_channel {
            Some(AccessChannel::Local) => "LOCAL",
            Some(AccessChannel::Cloud) => "CLOUD",
            None => "null",
        };
        let language = self
            .language
            .map(|language| match language {
                LanguageCode::Java => "JAVA".to_owned(),
                LanguageCode::Cpp => "CPP".to_owned(),
                LanguageCode::Dotnet => "DOTNET".to_owned(),
                LanguageCode::Python => "PYTHON".to_owned(),
                LanguageCode::Delphi => "DELPHI".to_owned(),
                LanguageCode::Erlang => "ERLANG".to_owned(),
                LanguageCode::Ruby => "RUBY".to_owned(),
                LanguageCode::Other => "OTHER".to_owned(),
                LanguageCode::Http => "HTTP".to_owned(),
                LanguageCode::Go => "GO".to_owned(),
                LanguageCode::Php => "PHP".to_owned(),
                LanguageCode::Oms => "OMS".to_owned(),
                LanguageCode::Rust => "RUST".to_owned(),
                LanguageCode::NodeJs => "NODE_JS".to_owned(),
                LanguageCode::Unknown(code) => format!("Unknown({code})"),
            })
            .unwrap_or_else(|| "null".to_owned());
        write!(
            f,
            "ClientConfig{{namesrvAddr='{}', clientIP='{}', instanceName='{}', clientCallbackExecutorThreads={}, namespace='{}', namespaceInitialized={}, namespaceV2='{}', accessChannel={}, pollNameServerInterval={}, heartbeatBrokerInterval={}, persistConsumerOffsetInterval={}, pullTimeDelayMillsWhenException={}, unitMode={}, unitName='{}', decodeReadBody={}, decodeDecompressBody={}, vipChannelEnabled={}, useHeartbeatV2={}, useTLS={}, socksProxyConfig='{}', mqClientApiTimeout={}, detectTimeout={}, detectInterval={}, language={}, enableStreamRequestType={}, sendLatencyEnable={}, startDetectorEnable={}, enableHeartbeatChannelEventListener={}, enableTrace={}, traceTopic='{}', enableConcurrentHeartbeat={}, concurrentHeartbeatThreadPoolSize={}}}",
            self.namesrv_addr.as_deref().unwrap_or("null"),
            self.client_ip.as_deref().unwrap_or("null"),
            self.instance_name.as_deref().unwrap_or("null"),
            self.client_callback_executor_threads,
            self.namespace.as_deref().unwrap_or("null"),
            self.namespace_initialized,
            self.namespace_v2.as_deref().unwrap_or("null"),
            access_channel,
            self.poll_name_server_interval,
            self.heartbeat_broker_interval,
            self.persist_consumer_offset_interval,
            self.pull_time_delay_millis_when_exception,
            self.unit_mode,
            self.unit_name.as_deref().unwrap_or("null"),
            self.decode_read_body,
            self.decode_decompress_body,
            self.vip_channel_enabled,
            self.use_heartbeat_v2,
            self.use_tls,
            self.socks_proxy_config.as_deref().unwrap_or("null"),
            self.mq_client_api_timeout,
            self.detect_timeout,
            self.detect_interval,
            language,
            self.enable_stream_request_type,
            self.send_latency_enable,
            self.start_detector_enable,
            self.enable_heartbeat_channel_event_listener,
            self.enable_trace,
            self.trace_topic.as_deref().unwrap_or("null"),
            self.enable_concurrent_heartbeat,
            self.concurrent_heartbeat_thread_pool_size,
        )
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self::new_auto().expect("automatic client-IP initialization failed")
    }
}

impl ClientConfig {
    pub const SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY: &'static str =
        "com.rocketmq.sendMessageWithVIPChannel";
    pub const SOCKS_PROXY_CONFIG: &'static str = "com.rocketmq.socks.proxy.config";
    pub const DECODE_READ_BODY: &'static str = "com.rocketmq.read.body";
    pub const DECODE_DECOMPRESS_BODY: &'static str = "com.rocketmq.decompress.body";
    pub const SEND_LATENCY_ENABLE: &'static str = "com.rocketmq.sendLatencyEnable";
    pub const START_DETECTOR_ENABLE: &'static str = "com.rocketmq.startDetectorEnable";
    pub const HEART_BEAT_V2: &'static str = "com.rocketmq.heartbeat.v2";

    /// Automatic IP initialization corresponding to Java's no-argument constructor.
    pub fn new_auto() -> Result<Self, ConfigError> {
        Ok(Self::new(local_address::get().map_err(|error| {
            ConfigError::LocalAddress(error.to_string())
        })?))
    }
    /// Explicit client-IP selection, corresponding to Java `setClientIP`.
    /// Known Java system-property keys are read from the process environment;
    /// ROCKETMQ_NAMESRV_ADDR remains the Java environment fallback.
    pub fn new(client_ip: impl Into<String>) -> Self {
        Self::with_properties(client_ip.into(), |key| std::env::var(key).ok())
    }

    fn with_properties(client_ip: String, property: impl Fn(&str) -> Option<String>) -> Self {
        let boolean = |key: &str, default: bool| {
            property(key).map_or(default, |value| value.eq_ignore_ascii_case("true"))
        };
        let processors = std::thread::available_parallelism()
            .ok()
            .and_then(|count| i32::try_from(count.get()).ok())
            .unwrap_or(1);
        Self {
            namesrv_addr: property("rocketmq.namesrv.addr")
                .or_else(|| property("ROCKETMQ_NAMESRV_ADDR")),
            client_ip: Some(client_ip),
            instance_name: Some(
                property("rocketmq.client.name").unwrap_or_else(|| "DEFAULT".into()),
            ),
            client_callback_executor_threads: processors,
            namespace: None,
            namespace_initialized: false,
            namespace_v2: None,
            access_channel: Some(AccessChannel::Local),
            poll_name_server_interval: 30_000,
            heartbeat_broker_interval: 30_000,
            persist_consumer_offset_interval: 5_000,
            pull_time_delay_millis_when_exception: 1_000,
            trace_msg_batch_num: 10,
            unit_mode: false,
            unit_name: None,
            decode_read_body: boolean(Self::DECODE_READ_BODY, true),
            decode_decompress_body: boolean(Self::DECODE_DECOMPRESS_BODY, true),
            vip_channel_enabled: boolean(Self::SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, false),
            use_heartbeat_v2: boolean(Self::HEART_BEAT_V2, false),
            use_tls: boolean(
                "tls.enable",
                property("tls.server.mode").as_deref() == Some("enforcing"),
            ),
            socks_proxy_config: Some(
                property(Self::SOCKS_PROXY_CONFIG).unwrap_or_else(|| "{}".into()),
            ),
            mq_client_api_timeout: 3_000,
            detect_timeout: 200,
            detect_interval: 2_000,
            language: Some(LanguageCode::Java),
            enable_stream_request_type: false,
            send_latency_enable: boolean(Self::SEND_LATENCY_ENABLE, false),
            start_detector_enable: boolean(Self::START_DETECTOR_ENABLE, false),
            enable_heartbeat_channel_event_listener: true,
            enable_concurrent_heartbeat: false,
            concurrent_heartbeat_thread_pool_size: processors,
            enable_trace: false,
            trace_topic: None,
            max_page_size_in_get_metadata: 2_000,
        }
    }

    pub fn build_client_id(&self) -> String {
        let mut id = format!(
            "{}@{}",
            self.client_ip.as_deref().unwrap_or("null"),
            self.instance_name.as_deref().unwrap_or("null")
        );
        if let Some(unit) = &self.unit_name {
            if !unit.chars().all(java_whitespace) {
                id.push('@');
                id.push_str(unit);
            }
        }
        if self.enable_stream_request_type {
            id.push_str("@STREAM");
        }
        id
    }

    pub fn set_namesrv_addr(&mut self, address: Option<String>) {
        self.namesrv_addr = address;
        self.namespace_initialized = false;
    }

    pub fn get_namesrv_addr(&self) -> Option<String> {
        self.namesrv_addr.as_deref().map(namespace::address)
    }

    pub fn set_namespace(&mut self, namespace: Option<String>) {
        self.namespace = namespace;
        self.namespace_initialized = true;
    }

    /// Mutable getter represents Java's lazy cached endpoint namespace.
    pub fn get_namespace(&mut self) -> Result<Option<&str>, ConfigError> {
        if !self.namespace_initialized && self.namespace.as_deref().is_none_or(str::is_empty) {
            if let Some(raw) = self
                .namesrv_addr
                .as_deref()
                .filter(|raw| namespace::instance_endpoint(raw))
            {
                self.namespace = Some(namespace::instance(raw)?.into());
            }
            self.namespace_initialized = true;
        }
        Ok(self.namespace.as_deref())
    }

    pub fn with_namespace(
        &mut self,
        resource: Option<&str>,
    ) -> Result<Option<String>, ConfigError> {
        Ok(namespace::wrap(self.get_namespace()?, resource))
    }

    pub fn without_namespace(
        &mut self,
        resource: Option<&str>,
    ) -> Result<Option<String>, ConfigError> {
        Ok(namespace::unwrap(self.get_namespace()?, resource))
    }

    pub fn with_namespace_set(
        &mut self,
        resources: &BTreeSet<Option<String>>,
    ) -> Result<BTreeSet<Option<String>>, ConfigError> {
        resources
            .iter()
            .map(|resource| self.with_namespace(resource.as_deref()))
            .collect()
    }

    pub fn without_namespace_set(
        &mut self,
        resources: &BTreeSet<Option<String>>,
    ) -> Result<BTreeSet<Option<String>>, ConfigError> {
        resources
            .iter()
            .map(|resource| self.without_namespace(resource.as_deref()))
            .collect()
    }

    pub fn queue_with_namespace(
        &mut self,
        queue: &MessageQueue,
    ) -> Result<MessageQueue, ConfigError> {
        let topic = self.with_namespace(queue.topic())?;
        Ok(MessageQueue::new_nullable(
            topic,
            queue.broker_name().map(str::to_owned),
            queue.queue_id(),
        ))
    }

    /// Java mutates the supplied collection's queues; Rust mutates values in place.
    pub fn queues_with_namespace(
        &mut self,
        queues: &mut [MessageQueue],
    ) -> Result<(), ConfigError> {
        let namespace = self.get_namespace()?;
        for queue in queues {
            let topic = namespace::wrap(namespace, queue.topic());
            *queue = MessageQueue::new_nullable(
                topic,
                queue.broker_name().map(str::to_owned),
                queue.queue_id(),
            );
        }
        Ok(())
    }

    pub fn change_instance_name_to_pid(&mut self) -> Result<(), ConfigError> {
        let name = self
            .instance_name
            .as_deref()
            .ok_or(ConfigError::NullInstanceName)?;
        if name == "DEFAULT" {
            static ORIGIN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
            let nanos = ORIGIN
                .get_or_init(std::time::Instant::now)
                .elapsed()
                .as_nanos() as i64;
            self.instance_name = Some(format!("{}#{nanos}", std::process::id() as i32));
        }
        Ok(())
    }

    /// Unlike Rust `Clone`, Java's cloneClientConfig omits three settings.
    pub fn clone_client_config(&self) -> Self {
        let mut copy = Self::new(self.client_ip.clone().unwrap_or_default());
        copy.reset_client_config(self);
        copy
    }

    /// Preserve the receiver's fields omitted by Java resetClientConfig.
    pub fn reset_client_config(&mut self, source: &Self) {
        let (trace_batch, page_size, channel, namespace_initialized) = (
            self.trace_msg_batch_num,
            self.max_page_size_in_get_metadata,
            self.access_channel,
            self.namespace_initialized,
        );
        *self = source.clone();
        self.trace_msg_batch_num = trace_batch;
        self.max_page_size_in_get_metadata = page_size;
        self.access_channel = channel;
        self.namespace_initialized = namespace_initialized;
    }
}

// Character.isWhitespace, used by Apache StringUtils.isBlank. Rust Unicode
// White_Space also includes NBSP/NEL; Java deliberately excludes those.
pub(crate) fn java_whitespace(value: char) -> bool {
    matches!(value, '\u{0009}'..='\u{000d}' | '\u{001c}'..='\u{0020}'
        | '\u{1680}' | '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}'
        | '\u{2028}' | '\u{2029}' | '\u{205f}' | '\u{3000}')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn every_constructor_field_matches_pinned_client_config_defaults() {
        let processors = std::thread::available_parallelism().unwrap().get() as i32;
        assert_eq!(
            ClientConfig::with_properties("explicit-ip".into(), |_| None),
            ClientConfig {
                namesrv_addr: None,
                client_ip: Some("explicit-ip".into()),
                instance_name: Some("DEFAULT".into()),
                client_callback_executor_threads: processors,
                namespace: None,
                namespace_initialized: false,
                namespace_v2: None,
                access_channel: Some(AccessChannel::Local),
                poll_name_server_interval: 30_000,
                heartbeat_broker_interval: 30_000,
                persist_consumer_offset_interval: 5_000,
                pull_time_delay_millis_when_exception: 1_000,
                trace_msg_batch_num: 10,
                unit_mode: false,
                unit_name: None,
                decode_read_body: true,
                decode_decompress_body: true,
                vip_channel_enabled: false,
                use_heartbeat_v2: false,
                use_tls: false,
                socks_proxy_config: Some("{}".into()),
                mq_client_api_timeout: 3_000,
                detect_timeout: 200,
                detect_interval: 2_000,
                language: Some(LanguageCode::Java),
                enable_stream_request_type: false,
                send_latency_enable: false,
                start_detector_enable: false,
                enable_heartbeat_channel_event_listener: true,
                enable_concurrent_heartbeat: false,
                concurrent_heartbeat_thread_pool_size: processors,
                enable_trace: false,
                trace_topic: None,
                max_page_size_in_get_metadata: 2_000,
            }
        );
    }

    #[test]
    fn namespace_cache_setter_clone_reset_and_empty_collection_state_matches_java() {
        let mut config = ClientConfig::new("ip");
        config.set_namesrv_addr(Some("MQ_INST_a_b.example".into()));
        assert_eq!(config.get_namespace().unwrap(), Some("MQ_INST_a_b"));
        config.set_namesrv_addr(Some("MQ_INST_c_d.example".into()));
        assert_eq!(config.get_namespace().unwrap(), Some("MQ_INST_a_b"));
        config.set_namespace(None);
        assert_eq!(config.get_namespace().unwrap(), None);
        let mut copy = config.clone_client_config();
        assert_eq!(copy.get_namespace().unwrap(), Some("MQ_INST_c_d"));
        let mut initialized = ClientConfig::new("ip");
        initialized.set_namespace(None);
        initialized.reset_client_config(&config);
        assert_eq!(initialized.get_namespace().unwrap(), None);
        config.set_namesrv_addr(Some("MQ_INST_c_d.example".into()));
        assert_eq!(config.get_namespace().unwrap(), Some("MQ_INST_c_d"));
        let mut invalid = ClientConfig::new("ip");
        invalid.set_namesrv_addr(Some("http://MQ_INST_a_b.example/path".into()));
        assert!(
            invalid
                .with_namespace_set(&BTreeSet::new())
                .unwrap()
                .is_empty()
        );
        assert!(!invalid.namespace_initialized);
        assert!(invalid.queues_with_namespace(&mut []).is_err());
    }

    #[test]
    fn resource_sets_deduplicate_and_queue_collection_is_updated_in_place() {
        let mut config = ClientConfig::new("ip");
        config.set_namespace(Some("ns".into()));
        let resources = BTreeSet::from([
            None,
            Some("topic".into()),
            Some("ns%topic".into()),
            Some("%RETRY%group".into()),
        ]);
        let wrapped = config.with_namespace_set(&resources).unwrap();
        assert_eq!(
            wrapped,
            BTreeSet::from([
                None,
                Some("ns%topic".into()),
                Some("%RETRY%ns%group".into())
            ])
        );
        assert_eq!(
            config.without_namespace_set(&wrapped).unwrap(),
            BTreeSet::from([None, Some("topic".into()), Some("%RETRY%group".into())])
        );
        let queue = MessageQueue::new("topic", "broker", -1);
        assert_eq!(
            config.queue_with_namespace(&queue).unwrap(),
            MessageQueue::new("ns%topic", "broker", -1)
        );
        assert_eq!(queue.topic(), Some("topic"));
        let mut queues = [queue, MessageQueue::new("%DLQ%group", "broker", 2)];
        config.queues_with_namespace(&mut queues).unwrap();
        assert_eq!(queues[1], MessageQueue::new("%DLQ%ns%group", "broker", 2));
        config.queues_with_namespace(&mut queues).unwrap();
        assert_eq!(queues[0].topic(), Some("ns%topic"));
    }

    #[test]
    fn pid_rename_only_changes_default_once_and_null_is_a_typed_error() {
        let mut config = ClientConfig::new("ip");
        config.instance_name = Some("DEFAULT".into());
        config.change_instance_name_to_pid().unwrap();
        let name = config.instance_name.clone().unwrap();
        let (pid, nanos) = name.split_once('#').unwrap();
        assert_eq!(pid, (std::process::id() as i32).to_string());
        nanos.parse::<i64>().unwrap();
        config.change_instance_name_to_pid().unwrap();
        assert_eq!(config.instance_name.as_deref(), Some(name.as_str()));
        for custom in ["custom", "default", ""] {
            config.instance_name = Some(custom.into());
            config.change_instance_name_to_pid().unwrap();
            assert_eq!(config.instance_name.as_deref(), Some(custom));
        }
        config.instance_name = None;
        assert_eq!(
            config.change_instance_name_to_pid(),
            Err(ConfigError::NullInstanceName)
        );
    }

    #[test]
    fn java_properties_have_exact_precedence_and_parse_boolean_rules() {
        let values = HashMap::from([
            ("rocketmq.namesrv.addr", ""),
            ("ROCKETMQ_NAMESRV_ADDR", "fallback:9876"),
            ("rocketmq.client.name", "custom"),
            ("com.rocketmq.heartbeat.v2", "TrUe"),
            ("com.rocketmq.read.body", " true "),
            ("com.rocketmq.decompress.body", "invalid"),
            (
                "com.rocketmq.socks.proxy.config",
                "sensitive-proxy-credentials",
            ),
        ]);
        let config = ClientConfig::with_properties("ip".into(), |key| {
            values.get(key).map(|value| (*value).to_owned())
        });
        assert_eq!(config.namesrv_addr.as_deref(), Some(""));
        assert_eq!(config.build_client_id(), "ip@custom");
        assert!(config.use_heartbeat_v2);
        assert!(!config.decode_read_body);
        assert!(!config.decode_decompress_body);
        assert!(!format!("{config:?}").contains("credentials"));
    }

    #[test]
    fn tls_enable_inherits_enforcing_mode_unless_explicitly_overridden() {
        let config = ClientConfig::with_properties("ip".into(), |key| {
            (key == "tls.server.mode").then(|| "enforcing".into())
        });
        assert!(config.use_tls);
        let config = ClientConfig::with_properties("ip".into(), |key| match key {
            "tls.server.mode" => Some("enforcing".into()),
            "tls.enable" => Some("false".into()),
            _ => None,
        });
        assert!(!config.use_tls);
    }
}
