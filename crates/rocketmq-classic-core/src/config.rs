//! Settings of pinned Java 5.5.1 `org.apache.rocketmq.client.ClientConfig`.
//! Durations retain Java signed widths and milliseconds. Validation happens
//! when the owning runtime/service uses a setting, not in Java's plain setters.

use rocketmq_classic_protocol::codes::LanguageCode;

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
    pub namespace_v2: Option<String>,
    pub access_channel: AccessChannel,
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
    pub socks_proxy_config: String,
    pub mq_client_api_timeout: i32,
    pub detect_timeout: i32,
    pub detect_interval: i32,
    pub language: LanguageCode,
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

impl ClientConfig {
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
            namespace_v2: None,
            access_channel: AccessChannel::Local,
            poll_name_server_interval: 30_000,
            heartbeat_broker_interval: 30_000,
            persist_consumer_offset_interval: 5_000,
            pull_time_delay_millis_when_exception: 1_000,
            trace_msg_batch_num: 10,
            unit_mode: false,
            unit_name: None,
            decode_read_body: boolean("com.rocketmq.read.body", true),
            decode_decompress_body: boolean("com.rocketmq.decompress.body", true),
            vip_channel_enabled: boolean("com.rocketmq.sendMessageWithVIPChannel", false),
            use_heartbeat_v2: boolean("com.rocketmq.heartbeat.v2", false),
            use_tls: boolean(
                "tls.enable",
                property("tls.server.mode").as_deref() == Some("enforcing"),
            ),
            socks_proxy_config: property("com.rocketmq.socks.proxy.config")
                .unwrap_or_else(|| "{}".into()),
            mq_client_api_timeout: 3_000,
            detect_timeout: 200,
            detect_interval: 2_000,
            language: LanguageCode::Java,
            enable_stream_request_type: false,
            send_latency_enable: boolean("com.rocketmq.sendLatencyEnable", false),
            start_detector_enable: boolean("com.rocketmq.startDetectorEnable", false),
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

    /// Unlike Rust `Clone`, Java's cloneClientConfig omits three settings.
    pub fn clone_client_config(&self) -> Self {
        let mut copy = Self::new(self.client_ip.clone().unwrap_or_default());
        copy.reset_client_config(self);
        copy
    }

    /// Preserve the receiver's fields omitted by Java resetClientConfig.
    pub fn reset_client_config(&mut self, source: &Self) {
        let (trace_batch, page_size, channel) = (
            self.trace_msg_batch_num,
            self.max_page_size_in_get_metadata,
            self.access_channel,
        );
        *self = source.clone();
        self.trace_msg_batch_num = trace_batch;
        self.max_page_size_in_get_metadata = page_size;
        self.access_channel = channel;
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
