use std::collections::HashMap;

use serde::Deserialize;

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
    pub connect_timeout: std::time::Duration,
    pub channel_not_active_interval: std::time::Duration,
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
    pub max_idle_time: std::time::Duration,
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
            connect_timeout: std::time::Duration::from_millis(3000),
            channel_not_active_interval: std::time::Duration::from_secs(60),
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
            max_idle_time: std::time::Duration::from_secs(120),
            close_socket_if_timeout: true,
            scan_available_nameserver: true,
            enable_reconnect_for_go_away: true,
            max_reconnect_interval_time_seconds: 60,
            socks_proxy_config: "{}".into(),
        }
    }
}

impl ClientConfig {
    pub fn validate(&self) -> Result<(), TransportError> {
        if self.connect_timeout.is_zero()
            || self.async_semaphore_value == 0
            || self.oneway_semaphore_value == 0
            || self.worker_threads == 0
            || i32::try_from(self.client_socket_send_buffer_size).is_err()
            || i32::try_from(self.client_socket_receive_buffer_size).is_err()
        {
            return Err(TransportError::InvalidConfiguration(
                "timeouts, semaphore limits, and worker counts must be nonzero".into(),
            ));
        }
        if self.write_buffer_low_water_mark > self.write_buffer_high_water_mark {
            return Err(TransportError::InvalidConfiguration(
                "write buffer low/high water marks must both be zero or satisfy low <= high".into(),
            ));
        }
        Ok(())
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
    ) -> Result<HashMap<String, SocksProxyConfig>, TransportError> {
        if self.socks_proxy_config.trim() == "null" {
            return Ok(HashMap::new());
        }
        serde_json::from_str(&self.socks_proxy_config)
            .map_err(|error| TransportError::InvalidConfiguration(error.to_string()))
    }

    pub(super) fn proxy_for(
        routes: &HashMap<String, SocksProxyConfig>,
        host: &str,
    ) -> Option<SocksProxyConfig> {
        routes.iter().find_map(|(cidr, proxy)| {
            if cidr == "0.0.0.0/0" || ipv4_in_cidr(host, cidr) {
                Some(proxy.clone())
            } else {
                None
            }
        })
    }
}

fn ipv4_in_cidr(address: &str, cidr: &str) -> bool {
    let Some(address) = address.parse::<std::net::Ipv4Addr>().ok() else {
        return false;
    };
    let Some((network, prefix)) = cidr.split_once('/') else {
        return false;
    };
    let (Ok(network), Ok(prefix)) = (network.parse::<std::net::Ipv4Addr>(), prefix.parse::<u32>())
    else {
        return false;
    };
    if prefix > 32 {
        return false;
    }
    let mask = u32::MAX.wrapping_shl((32 - prefix) % 32);
    u32::from(address) & mask == u32::from(network) & mask
}

#[cfg(test)]
mod tests {
    use super::{ClientConfig, SocksProxyConfig};

    #[test]
    fn java_socks_proxy_json_round_trips_java_field_names() {
        let mut config = ClientConfig::default();
        let json = r#"{"10.1.0.0/16":{"addr":"proxy.internal:1080","username":"user","password":"secret"}}"#;
        config.set_socks_proxy_config(json);
        assert_eq!(config.get_socks_proxy_config(), json);
        assert_eq!(
            config.parse_socks_proxy_config().unwrap()["10.1.0.0/16"],
            SocksProxyConfig::with_credentials("proxy.internal:1080", "user", "secret")
        );
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
    fn socks_proxy_routes_match_ipv4_cidr_and_java_default_route() {
        let private_route = std::collections::HashMap::from([(
            "10.20.0.0/16".into(),
            SocksProxyConfig::new("private-proxy:1080"),
        )]);
        assert_eq!(
            ClientConfig::proxy_for(&private_route, "10.20.4.5")
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
                .address,
            "default-proxy:1080"
        );
        assert!(ClientConfig::proxy_for(&default_route, "broker.example").is_some());
        assert!(ClientConfig::proxy_for(&default_route, "2001:db8::1").is_some());
    }
}
