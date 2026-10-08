use std::collections::HashMap;

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::TransportMode;
use crate::error::TransportError;

/// SOCKS5 proxy settings corresponding to Java's `SocksProxyConfig`.
#[derive(Clone, Debug, Default, Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct SocksProxyConfig {
    #[serde(rename = "addr")]
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

// Java keeps malformed proxy entries until a matching route is connected.
// Keep those cases private so the public, address-bearing config stays honest.
#[derive(Clone, Debug)]
pub(super) enum ParsedProxyRoute {
    Direct,
    Proxy(SocksProxyConfig),
    MissingAddress,
}

impl From<SocksProxyConfig> for ParsedProxyRoute {
    fn from(value: SocksProxyConfig) -> Self {
        Self::Proxy(value)
    }
}

#[derive(Clone, Debug)]
enum FastjsonValue {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
}

impl FastjsonValue {
    fn java_string(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Null => "null".into(),
            Self::Bool(value) => value.to_string(),
            Self::Number(value) => value.to_string(),
            Self::Array(_) | Self::Object(_) => {
                let mut output = String::new();
                self.write_compact_json(&mut output);
                output
            }
        }
    }

    fn write_compact_json(&self, output: &mut String) {
        match self {
            Self::Null => output.push_str("null"),
            Self::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            Self::Number(value) => output.push_str(&value.to_string()),
            Self::String(value) => write_json_string(value, output),
            Self::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    value.write_compact_json(output);
                }
                output.push(']');
            }
            Self::Object(fields) => {
                output.push('{');
                for (index, (key, value)) in fields.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_json_string(key, output);
                    output.push(':');
                    value.write_compact_json(output);
                }
                output.push('}');
            }
        }
    }
}

fn write_json_string(value: &str, output: &mut String) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{0008}' => output.push_str("\\b"),
            '\u{000c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{0000}'..='\u{001f}' => {
                let byte = character as u8;
                output.push_str("\\u00");
                output.push(HEX[(byte >> 4) as usize] as char);
                output.push(HEX[(byte & 0x0f) as usize] as char);
            }
            other => output.push(other),
        }
    }
    output.push('"');
}

impl<'de> Deserialize<'de> for FastjsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FastjsonValueVisitor;

        impl<'de> Visitor<'de> for FastjsonValueVisitor {
            type Value = FastjsonValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON value")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(FastjsonValue::Null)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(FastjsonValue::Null)
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(FastjsonValue::Bool(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(FastjsonValue::Number(value.into()))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(FastjsonValue::Number(value.into()))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                serde_json::Number::from_f64(value)
                    .map(FastjsonValue::Number)
                    .ok_or_else(|| E::custom("non-finite JSON number"))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(FastjsonValue::String(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(FastjsonValue::String(value))
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(FastjsonValue::Array(values))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields: Vec<(String, FastjsonValue)> = Vec::new();
                while let Some((key, value)) = map.next_entry::<String, FastjsonValue>()? {
                    if let Some((_, existing)) = fields.iter_mut().find(|(name, _)| *name == key) {
                        *existing = value;
                    } else {
                        fields.push((key, value));
                    }
                }
                Ok(FastjsonValue::Object(fields))
            }
        }

        deserializer.deserialize_any(FastjsonValueVisitor)
    }
}

#[derive(Deserialize)]
#[serde(transparent)]
struct RawProxyRoute(HashMap<String, Option<FastjsonValue>>);

impl RawProxyRoute {
    fn java_string_field(&self, name: &str) -> Option<String> {
        self.0
            .get(name)
            .and_then(Option::as_ref)
            .map(FastjsonValue::java_string)
    }
}

fn contains_fastjson_rejected_literal(input: &str) -> bool {
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' | b'\'' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        index = (index + 2).min(bytes.len());
                    } else if bytes[index] == quote {
                        index += 1;
                        break;
                    } else {
                        index += 1;
                    }
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            b'0' if (index == 0 || !is_number_token_byte(bytes[index - 1]))
                && matches!(bytes.get(index + 1), Some(b'x' | b'X'))
                && bytes.get(index + 2).is_some_and(u8::is_ascii_hexdigit) =>
            {
                let mut end = index + 2;
                while bytes.get(end).is_some_and(u8::is_ascii_hexdigit) {
                    end += 1;
                }
                if bytes
                    .get(end)
                    .is_none_or(|byte| !is_number_token_byte(*byte))
                {
                    let next = skip_json5_trivia(bytes, end);
                    if bytes.get(next) != Some(&b':') {
                        return true;
                    }
                }
                index = end;
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' | b'$'
                if index == 0 || !is_identifier_token_byte(bytes[index - 1]) =>
            {
                let start = index;
                index += 1;
                while bytes
                    .get(index)
                    .is_some_and(|byte| is_identifier_token_byte(*byte))
                {
                    index += 1;
                }
                let token = &bytes[start..index];
                let next = skip_json5_trivia(bytes, index);
                if bytes.get(next) == Some(&b':')
                    || matches!(token, b"NaN" | b"Infinity" | b"undefined")
                {
                    return true;
                }
            }
            _ => index += 1,
        }
    }
    false
}

fn is_number_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$' | b'.')
}

fn is_identifier_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$')
}

fn skip_json5_trivia(bytes: &[u8], mut index: usize) -> usize {
    loop {
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index += 1;
        }
        if bytes.get(index..index + 2) == Some(b"//") {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
        } else if bytes.get(index..index + 2) == Some(b"/*") {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
        } else {
            return index;
        }
    }
}

fn normalize_fastjson_leading_zero_numbers(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut copied_through = 0;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' | b'\'' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\\' {
                        index = (index + 2).min(bytes.len());
                    } else if bytes[index] == quote {
                        index += 1;
                        break;
                    } else {
                        index += 1;
                    }
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            b'0'..=b'9' if index == 0 || !is_number_token_byte(bytes[index - 1]) => {
                let mut end = index;
                while bytes.get(end).is_some_and(u8::is_ascii_digit) {
                    end += 1;
                }
                if end - index > 1 && bytes[index] == b'0' {
                    let next = skip_json5_trivia(bytes, end);
                    let is_field_name = bytes.get(next) == Some(&b':');
                    let valid_number_end = bytes.get(end).is_none_or(|byte| {
                        byte.is_ascii_whitespace()
                            || matches!(byte, b',' | b'}' | b']' | b'.' | b'e' | b'E')
                    });
                    if !is_field_name && valid_number_end {
                        output.extend_from_slice(&bytes[copied_through..index]);
                        let first_significant = (index..end)
                            .find(|position| bytes[*position] != b'0')
                            .unwrap_or(end - 1);
                        output.extend_from_slice(&bytes[first_significant..end]);
                        copied_through = end;
                    }
                }
                index = end;
            }
            _ => index += 1,
        }
    }
    output.extend_from_slice(&bytes[copied_through..]);
    String::from_utf8(output).expect("normalization preserves valid UTF-8")
}

impl SocksProxyConfig {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            username: None,
            password: None,
        }
    }

    pub fn with_credentials(
        address: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            address: address.into(),
            username: Some(username.into()),
            password: Some(password.into()),
        }
    }
}

/// Client controls mapped from Java `NettyClientConfig` settings used by this transport.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub transport: TransportMode,
    /// Java `connectTimeoutMillis`; negative values leave Netty's 30-second default in effect.
    pub connect_timeout: i32,
    /// Java `channelNotActiveInterval` in milliseconds; inert in the pinned client's normal flow.
    pub channel_not_active_interval: i64,
    pub async_semaphore_value: usize,
    pub oneway_semaphore_value: usize,
    pub worker_threads: usize,
    pub callback_executor_threads: usize,
    pub disable_callback_executor: bool,
    pub client_socket_send_buffer_size: usize,
    pub client_socket_receive_buffer_size: usize,
    pub write_buffer_high_water_mark: usize,
    pub write_buffer_low_water_mark: usize,
    pub pooled_byte_buffer_allocator_enabled: bool,
    pub disable_netty_worker_group: bool,
    /// Java `clientChannelMaxIdleTimeSeconds`; non-positive values disable idle detection.
    pub max_idle_time: i32,
    pub close_socket_if_timeout: bool,
    pub scan_available_nameserver: bool,
    pub enable_reconnect_for_go_away: bool,
    /// Java `long maxReconnectIntervalTimeSeconds`; retained but unused by the pinned client.
    pub max_reconnect_interval_time_seconds: i64,
    /// Java `NettyClientConfig.socksProxyConfig` JSON text, keyed by IPv4 CIDR.
    pub socks_proxy_config: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            transport: TransportMode::Plain,
            connect_timeout: 3000,
            channel_not_active_interval: 60_000,
            async_semaphore_value: 65535,
            oneway_semaphore_value: 65535,
            worker_threads: 4,
            callback_executor_threads: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
            disable_callback_executor: false,
            client_socket_send_buffer_size: 0,
            client_socket_receive_buffer_size: 0,
            write_buffer_high_water_mark: 0,
            write_buffer_low_water_mark: 0,
            pooled_byte_buffer_allocator_enabled: false,
            disable_netty_worker_group: false,
            max_idle_time: 120,
            close_socket_if_timeout: true,
            scan_available_nameserver: true,
            enable_reconnect_for_go_away: true,
            max_reconnect_interval_time_seconds: 60,
            socks_proxy_config: "{}".into(),
        }
    }
}

impl ClientConfig {
    pub(super) fn connect_timeout_duration(&self) -> std::time::Duration {
        if self.connect_timeout < 0 {
            std::time::Duration::from_secs(30)
        } else {
            std::time::Duration::from_millis(self.connect_timeout as u64)
        }
    }

    pub(super) fn scan_interval(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.connect_timeout.max(100) as u64)
    }

    pub(super) fn max_idle_time_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.max_idle_time.max(0) as u64)
    }

    pub fn validate(&self) -> Result<(), TransportError> {
        if self.worker_threads == 0
            || i32::try_from(self.worker_threads).is_err()
            || i32::try_from(self.callback_executor_threads).is_err()
            || i32::try_from(self.async_semaphore_value).is_err()
            || i32::try_from(self.oneway_semaphore_value).is_err()
            || self.async_semaphore_value > tokio::sync::Semaphore::MAX_PERMITS
            || self.oneway_semaphore_value > tokio::sync::Semaphore::MAX_PERMITS
            || i32::try_from(self.client_socket_send_buffer_size).is_err()
            || i32::try_from(self.client_socket_receive_buffer_size).is_err()
            || i32::try_from(self.write_buffer_high_water_mark).is_err()
            || i32::try_from(self.write_buffer_low_water_mark).is_err()
        {
            return Err(TransportError::InvalidConfiguration(
                "integer client settings must fit their Java signed integer ranges; worker count must be nonzero"
                    .into(),
            ));
        }
        if self.write_buffer_low_water_mark > 0
            && self.write_buffer_high_water_mark > 0
            && self.write_buffer_low_water_mark > self.write_buffer_high_water_mark
        {
            return Err(TransportError::InvalidConfiguration(
                "write buffer low water mark must not exceed high water mark when both are enabled"
                    .into(),
            ));
        }
        Ok(())
    }

    pub(super) fn write_buffer_watermarks(&self) -> (usize, usize) {
        if self.write_buffer_low_water_mark > 0 && self.write_buffer_high_water_mark > 0 {
            (
                self.write_buffer_high_water_mark,
                self.write_buffer_low_water_mark,
            )
        } else {
            (64 * 1024, 32 * 1024)
        }
    }

    /// Java-compatible getter/setter behavior: retain JSON text until client construction.
    pub fn get_socks_proxy_config(&self) -> &str {
        &self.socks_proxy_config
    }

    pub fn set_socks_proxy_config(&mut self, json: impl Into<String>) {
        self.socks_proxy_config = json.into();
    }

    pub(super) fn parse_socks_proxy_config(
        &self,
    ) -> Result<HashMap<String, ParsedProxyRoute>, TransportError> {
        if self.socks_proxy_config.is_empty() || self.socks_proxy_config.trim() == "null" {
            return Ok(HashMap::new());
        }
        if contains_fastjson_rejected_literal(&self.socks_proxy_config) {
            return Err(TransportError::InvalidConfiguration(
                "Fastjson SOCKS parser rejects this JSON5 literal".into(),
            ));
        }
        let normalized = normalize_fastjson_leading_zero_numbers(&self.socks_proxy_config);
        let parsed: Option<HashMap<String, Option<RawProxyRoute>>> =
            json5::from_str(&normalized)
                .map_err(|error| TransportError::InvalidConfiguration(error.to_string()))?;
        Ok(parsed
            .unwrap_or_default()
            .into_iter()
            .map(|(cidr, entry)| {
                let route = match entry {
                    None => ParsedProxyRoute::Direct,
                    Some(raw) => match raw.java_string_field("addr") {
                        Some(address) => ParsedProxyRoute::Proxy(SocksProxyConfig {
                            address,
                            username: raw.java_string_field("username"),
                            password: raw.java_string_field("password"),
                        }),
                        None => ParsedProxyRoute::MissingAddress,
                    },
                };
                (cidr, route)
            })
            .collect())
    }

    pub(super) fn proxy_for<T: Clone + Into<ParsedProxyRoute>>(
        routes: &HashMap<String, T>,
        host: &str,
    ) -> Result<Option<SocksProxyConfig>, TransportError> {
        for (cidr, route) in routes {
            if cidr == "0.0.0.0/0" || ipv4_in_cidr(host, cidr)? {
                return match route.clone().into() {
                    ParsedProxyRoute::Direct => Ok(None),
                    ParsedProxyRoute::Proxy(proxy) => Ok(Some(proxy)),
                    ParsedProxyRoute::MissingAddress => Err(TransportError::InvalidConfiguration(
                        "matched SOCKS proxy has no address".into(),
                    )),
                };
            }
        }
        Ok(None)
    }
}

fn ipv4_in_cidr(address: &str, cidr: &str) -> Result<bool, TransportError> {
    let address = java_ip_to_int(address)?;
    let mut parts = cidr.split('/');
    let network = parts.next().unwrap_or_default();
    let prefix = parts.next().ok_or_else(|| {
        TransportError::InvalidConfiguration(format!("invalid SOCKS proxy CIDR: {cidr}"))
    })?;
    let network = java_ip_to_int(network)?;
    let prefix = prefix.parse::<i32>().map_err(|error| {
        TransportError::InvalidConfiguration(format!("invalid SOCKS proxy CIDR {cidr}: {error}"))
    })?;
    // Java int shifts mask the shift distance to five bits, including /0 and out-of-range prefixes.
    let mask = (-1_i32).wrapping_shl(32_i32.wrapping_sub(prefix) as u32);
    Ok((address & mask) == (network & mask))
}

fn java_ip_to_int(ip: &str) -> Result<i32, TransportError> {
    // Java's ipToInt reads the first four split fields and ignores any extras.
    let mut address = 0_i32;
    let mut octets = ip.split('.');
    for index in 0..4 {
        let octet = octets.next().ok_or_else(|| {
            TransportError::InvalidConfiguration(format!(
                "invalid IPv4 address in SOCKS proxy CIDR: {ip}"
            ))
        })?;
        let value = octet.parse::<i32>().map_err(|error| {
            TransportError::InvalidConfiguration(format!(
                "invalid IPv4 address in SOCKS proxy CIDR {ip}: {error}"
            ))
        })?;
        address |= value.wrapping_shl((24 - index * 8) as u32);
    }
    Ok(address)
}

#[cfg(test)]
mod tests {
    use super::{ClientConfig, SocksProxyConfig};

    #[test]
    fn signed_time_settings_match_java_normalization() {
        let defaults = ClientConfig::default();
        assert_eq!(
            defaults.connect_timeout_duration(),
            std::time::Duration::from_secs(3)
        );
        assert_eq!(defaults.scan_interval(), std::time::Duration::from_secs(3));

        let negative = ClientConfig {
            connect_timeout: -1,
            max_idle_time: -1,
            channel_not_active_interval: -1,
            ..defaults
        };
        assert!(negative.validate().is_ok());
        assert_eq!(
            negative.connect_timeout_duration(),
            std::time::Duration::from_secs(30)
        );
        assert_eq!(
            negative.scan_interval(),
            std::time::Duration::from_millis(100)
        );
        assert_eq!(negative.max_idle_time_duration(), std::time::Duration::ZERO);
    }

    #[test]
    fn write_buffer_watermarks_match_java_enablement() {
        let defaults = ClientConfig::default();
        assert_eq!(defaults.write_buffer_watermarks(), (64 * 1024, 32 * 1024));

        let low_only = ClientConfig {
            write_buffer_low_water_mark: 10,
            ..defaults.clone()
        };
        assert!(low_only.validate().is_ok());
        assert_eq!(low_only.write_buffer_watermarks(), (64 * 1024, 32 * 1024));

        let high_only = ClientConfig {
            write_buffer_high_water_mark: 128,
            ..defaults.clone()
        };
        assert!(high_only.validate().is_ok());
        assert_eq!(high_only.write_buffer_watermarks(), (64 * 1024, 32 * 1024));

        let enabled = ClientConfig {
            write_buffer_high_water_mark: 128,
            write_buffer_low_water_mark: 32,
            ..defaults.clone()
        };
        assert_eq!(enabled.write_buffer_watermarks(), (128, 32));

        let disabled = ClientConfig {
            write_buffer_high_water_mark: 128,
            write_buffer_low_water_mark: 0,
            ..defaults.clone()
        };
        assert_eq!(disabled.write_buffer_watermarks(), (64 * 1024, 32 * 1024));

        let invalid = ClientConfig {
            write_buffer_high_water_mark: 32,
            write_buffer_low_water_mark: 128,
            ..defaults
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn java_socks_proxy_json_round_trips_java_field_names() {
        let mut config = ClientConfig::default();
        let json = r#"{"10.1.0.0/16":{"addr":"proxy.internal:1080","username":"user","password":"secret"}}"#;
        config.set_socks_proxy_config(json);
        assert_eq!(config.get_socks_proxy_config(), json);
        assert!(matches!(
            &config.parse_socks_proxy_config().unwrap()["10.1.0.0/16"],
            super::ParsedProxyRoute::Proxy(proxy)
                if proxy == &SocksProxyConfig::with_credentials("proxy.internal:1080", "user", "secret")
        ));
        let malformed = ClientConfig {
            socks_proxy_config: r#"{"broken":{]"#.into(),
            ..ClientConfig::default()
        };
        assert!(malformed.validate().is_ok());
        assert!(malformed.parse_socks_proxy_config().is_err());
        let null_map = ClientConfig {
            socks_proxy_config: "null".into(),
            ..ClientConfig::default()
        };
        assert!(null_map.parse_socks_proxy_config().unwrap().is_empty());
    }

    #[test]
    fn fastjson_literal_guard_ignores_quoted_and_comment_text() {
        assert!(super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{"addr":0x2a}}"#
        ));
        assert!(super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{"addr":NaN}}"#
        ));
        assert!(super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{"addr":Infinity}}"#
        ));
        assert!(super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{addr: "proxy:1080"}}"#
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{"addr":"0x2a"}}"#
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            r#"{"10.0.0.0/8":{'addr':"proxy:1080"}}"#
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            r#"{/* NaN and 0x2a are comment text */"10.0.0.0/8":{"addr":"proxy"}}"#
        ));
        assert!(!super::contains_fastjson_rejected_literal(
            r#"{0x2a:{"addr":"proxy"}}"#
        ));
    }

    #[test]
    fn fastjson_constructor_inputs_preserve_matched_route_behavior() {
        let mut config = ClientConfig::default();
        config.set_socks_proxy_config(
            "{'10.1.0.0/16':{'addr':'proxy:1080'},'10.2.0.0/16':null,'10.3.0.0/16':{}}",
        );
        let routes = config.parse_socks_proxy_config().unwrap();
        assert_eq!(
            ClientConfig::proxy_for(&routes, "10.1.2.3")
                .unwrap()
                .unwrap()
                .address,
            "proxy:1080"
        );
        assert!(
            ClientConfig::proxy_for(&routes, "10.2.2.3")
                .unwrap()
                .is_none()
        );
        assert!(ClientConfig::proxy_for(&routes, "10.3.2.3").is_err());
    }

    #[test]
    fn scalar_socks_fields_match_invoked_java_parsed_proxy_values() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        for case in fixture["socksConstruction"]
            .as_array()
            .unwrap()
            .iter()
            .skip(19)
        {
            let mut config = ClientConfig::default();
            config.set_socks_proxy_config(case["input"].as_str().unwrap());
            let parsed = config.parse_socks_proxy_config();
            assert_eq!(parsed.is_ok(), case["outcome"] == "OK", "{}", case["input"]);
            if case["outcome"] != "OK" {
                continue;
            }
            let routes = parsed.unwrap();
            match &routes["10.0.0.0/8"] {
                super::ParsedProxyRoute::Proxy(proxy) => {
                    assert_eq!(Some(proxy.address.as_str()), case["parsedAddr"].as_str());
                    assert_eq!(proxy.username.as_deref(), case["parsedUsername"].as_str());
                    assert_eq!(proxy.password.as_deref(), case["parsedPassword"].as_str());
                }
                super::ParsedProxyRoute::MissingAddress => {
                    assert!(case["parsedAddr"].is_null());
                }
                super::ParsedProxyRoute::Direct => panic!("Java decoded a nonnull route object"),
            }
        }
    }

    #[test]
    fn socks_cidr_matching_matches_invoked_java_helper() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        for case in fixture["socksCidr"].as_array().unwrap() {
            let result = super::ipv4_in_cidr(
                case["address"].as_str().unwrap(),
                case["cidr"].as_str().unwrap(),
            );
            assert_eq!(result.is_ok(), case["outcome"] == "OK", "{case}");
            if let Ok(matches) = result {
                assert_eq!(matches, case["match"]);
            }
        }
    }

    #[test]
    fn socks_proxy_selection_matches_invoked_java_remoting_client() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/runtime/client-config.json"
        ))
        .unwrap();
        for case in fixture["socksProxySelection"].as_array().unwrap() {
            let mut config = ClientConfig::default();
            config.set_socks_proxy_config(case["config"].as_str().unwrap());
            let routes = config.parse_socks_proxy_config().unwrap();
            let selected = ClientConfig::proxy_for(&routes, case["host"].as_str().unwrap());
            assert_eq!(selected.is_ok(), case["outcome"] == "OK", "{case}");
            if case["outcome"] != "OK" {
                continue;
            }
            assert_eq!(
                selected.unwrap().map(|proxy| proxy.address).as_deref(),
                case["proxyAddress"].as_str(),
                "{case}"
            );
        }
    }

    #[test]
    fn socks_proxy_routes_match_java_ipv4_cidr_and_error_behavior() {
        let private_route = std::collections::HashMap::from([(
            "10.20.0.0/16".into(),
            SocksProxyConfig::new("private-proxy:1080"),
        )]);
        assert_eq!(
            ClientConfig::proxy_for(&private_route, "10.20.4.5")
                .unwrap()
                .unwrap()
                .address,
            "private-proxy:1080"
        );
        let default_route = std::collections::HashMap::from([(
            "0.0.0.0/0".into(),
            SocksProxyConfig::new("default-proxy:1080"),
        )]);
        assert_eq!(
            ClientConfig::proxy_for(&default_route, "192.0.2.5")
                .unwrap()
                .unwrap()
                .address,
            "default-proxy:1080"
        );
        assert!(
            ClientConfig::proxy_for(&default_route, "broker.example")
                .unwrap()
                .is_some()
        );
        assert!(
            ClientConfig::proxy_for(&default_route, "2001:db8::1")
                .unwrap()
                .is_some()
        );

        let zero_prefix = std::collections::HashMap::from([(
            "10.20.0.0/0".into(),
            SocksProxyConfig::new("zero-prefix-proxy:1080"),
        )]);
        assert!(
            ClientConfig::proxy_for(&zero_prefix, "10.20.4.5")
                .unwrap()
                .is_none()
        );
        assert!(ClientConfig::proxy_for(&private_route, "broker.example").is_err());
        let invalid_cidr = std::collections::HashMap::from([(
            "10.20.0.0/not-a-prefix".into(),
            SocksProxyConfig::new("proxy:1080"),
        )]);
        assert!(ClientConfig::proxy_for(&invalid_cidr, "10.20.4.5").is_err());

        // Java ipToInt parses signed integers and Java int shifts mask shift counts to 0..31.
        let java_shifted_prefix = std::collections::HashMap::from([(
            "10.20.0.0/33".into(),
            SocksProxyConfig::new("shifted-prefix-proxy:1080"),
        )]);
        assert!(
            ClientConfig::proxy_for(&java_shifted_prefix, "10.20.4.5")
                .unwrap()
                .is_some()
        );
        let java_integer_octets = std::collections::HashMap::from([(
            "266.20.0.0/16".into(),
            SocksProxyConfig::new("integer-octet-proxy:1080"),
        )]);
        assert!(
            ClientConfig::proxy_for(&java_integer_octets, "10.20.4.5")
                .unwrap()
                .is_some()
        );
    }
}
