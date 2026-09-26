use super::TransportMode;
use crate::error::TransportError;

/// SOCKS5 proxy settings corresponding to Java's `SocksProxyConfig`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SocksProxyConfig {
    pub address: String,
    pub username: Option<String>,
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
    pub socks_proxy: Option<SocksProxyConfig>,
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
            socks_proxy: None,
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
        if let Some(proxy) = &self.socks_proxy {
            if !matches!(
                super::connection::split_address(&proxy.address),
                Ok((ref host, port)) if !host.is_empty() && port != 0
            ) {
                return Err(TransportError::InvalidConfiguration(
                    "SOCKS5 proxy address must be a socket address".into(),
                ));
            }
            if proxy.username.is_some() != proxy.password.is_some() {
                return Err(TransportError::InvalidConfiguration(
                    "SOCKS5 username and password must be configured together".into(),
                ));
            }
        }
        Ok(())
    }
}
