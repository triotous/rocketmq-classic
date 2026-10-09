use std::sync::{Arc, Weak};

use tokio::time::Instant;

use crate::error::TransportError;

use super::{ClientInner, connection};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(super) struct Address(String);

impl Address {
    pub(super) fn parse(value: &str) -> Result<Self, TransportError> {
        let normalized = if value.starts_with('[') {
            let (host, port) = value
                .strip_prefix('[')
                .and_then(|value| value.split_once("]:"))
                .ok_or_else(|| TransportError::Connect("invalid bracketed IPv6 address".into()))?;
            let parsed_host = normalize_ipv6_host(host)?;
            let parsed_port = parse_socket_port(port)?;
            format!("[{}]:{}", parsed_host, parsed_port)
        } else {
            let (host, port) = value
                .rsplit_once(':')
                .ok_or_else(|| TransportError::Connect("missing address port".into()))?;
            let parsed_port = parse_socket_port(port)?;
            if host.is_empty() {
                return Err(TransportError::Connect("host must not be empty".into()));
            }
            if host.contains(':') {
                let parsed_host = normalize_ipv6_host(host)?;
                format!("[{}]:{}", parsed_host, parsed_port)
            } else {
                format!("{}:{}", host.to_ascii_lowercase(), parsed_port)
            }
        };
        Ok(Self(normalized))
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

pub(super) fn parse_socket_port(value: &str) -> Result<u16, TransportError> {
    let port = parse_java_i32(value)
        .ok_or_else(|| TransportError::Connect("invalid address port".into()))?;
    u16::try_from(port).map_err(|_| TransportError::Connect("invalid address port".into()))
}

// Java Integer.parseInt applies Character.digit to UTF-16 units, so BMP
// decimal scripts are valid while supplementary mathematical digits are not.
fn parse_java_i32(value: &str) -> Option<i32> {
    let mut units = value.encode_utf16().peekable();
    let negative = match units.peek().copied()? {
        0x2d => {
            units.next();
            true
        }
        0x2b => {
            units.next();
            false
        }
        _ => false,
    };
    units.peek()?;
    let limit = if negative { i32::MIN } else { -i32::MAX };
    let multiply_limit = limit / 10;
    let mut result = 0_i32;
    for unit in units {
        let digit = i32::from(java_decimal_digit(unit)?);
        if result < multiply_limit {
            return None;
        }
        result *= 10;
        if result < limit + digit {
            return None;
        }
        result -= digit;
    }
    Some(if negative { result } else { -result })
}

fn java_decimal_digit(unit: u16) -> Option<u16> {
    const ZEROS: &[u16] = &[
        0x0030, 0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66, 0x0be6, 0x0c66,
        0x0ce6, 0x0d66, 0x0de6, 0x0e50, 0x0ed0, 0x0f20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946,
        0x19d0, 0x1a80, 0x1a90, 0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0,
        0xa9f0, 0xaa50, 0xabf0, 0xff10,
    ];
    ZEROS
        .iter()
        .find_map(|zero| unit.checked_sub(*zero).filter(|digit| *digit < 10))
}

fn normalize_ipv6_host(host: &str) -> Result<String, TransportError> {
    let (address, scope) = host
        .split_once('%')
        .map_or((host, None), |(address, scope)| (address, Some(scope)));
    let parsed_host = address
        .parse::<std::net::Ipv6Addr>()
        .map_err(|error| TransportError::Connect(error.to_string()))?;
    let Some(scope) = scope else {
        return Ok(parsed_host.to_string());
    };
    if scope.is_empty() || scope.contains('%') {
        return Err(TransportError::Connect("invalid IPv6 scope id".into()));
    }
    if scope.bytes().all(|byte| byte.is_ascii_digit()) {
        let scope = scope
            .parse::<u32>()
            .map_err(|_| TransportError::Connect("invalid IPv6 scope id".into()))?;
        return if scope == 0 {
            Ok(parsed_host.to_string())
        } else {
            Ok(format!("{parsed_host}%{scope}"))
        };
    }
    if !scope
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"_.-:".contains(&byte))
    {
        return Err(TransportError::Connect("invalid IPv6 scope name".into()));
    }
    Ok(format!("{parsed_host}%{scope}"))
}

impl ClientInner {
    pub(super) fn retire_for_go_away(
        &self,
        address: &str,
        connection: Arc<connection::Connection>,
    ) -> bool {
        let generation = connection.generation;
        let address = Address::parse(address)
            .map(|address| address.0)
            .unwrap_or_else(|_| address.to_owned());
        let mut retired = self.retired_connections.lock().unwrap();
        if retired.contains_key(&generation) || connection.is_retired() {
            return false;
        }
        let mut connections = self.connections.lock().unwrap();
        let current = connections
            .iter()
            .find(|(_, current)| current.generation == generation)
            .map(|(address, current)| (address.clone(), current.clone()));
        let (address, current) = match current {
            Some((address, current)) => {
                connections.remove(&address);
                (address, current)
            }
            None => {
                if connections.contains_key(&address) {
                    return false;
                }
                connection.mark_retired();
                if !connection.is_closed() {
                    retired.insert(generation, (address, connection));
                }
                return true;
            }
        };
        current.mark_retired();
        retired.insert(generation, (address, current));
        true
    }

    pub(super) fn close_retired_connections(&self, addresses: &[String]) {
        let normalized = addresses
            .iter()
            .filter_map(|address| Address::parse(address).ok())
            .map(|address| address.0)
            .collect::<std::collections::HashSet<_>>();
        let retired = {
            let mut table = self.retired_connections.lock().unwrap();
            let generations = table
                .iter()
                .filter_map(|(generation, (address, _))| {
                    normalized.contains(address).then_some(*generation)
                })
                .collect::<Vec<_>>();
            generations
                .into_iter()
                .filter_map(|generation| table.remove(&generation))
                .collect::<Vec<_>>()
        };
        for (address, connection) in retired {
            connection.close();
            self.fail_generation(
                connection.generation,
                TransportError::SendRequest {
                    remote_addr: address,
                    cause: None,
                },
            );
        }
    }

    pub(super) fn connection_closed_for_address(&self, address: &str) {
        let generation = self
            .connections
            .lock()
            .unwrap()
            .get(address)
            .map(|connection| connection.generation);
        if let Some(generation) = generation {
            self.connection_closed(generation, Some(TransportError::Closed));
        }
    }

    pub(super) async fn connection(
        self: &Arc<Self>,
        address: &str,
        deadline: Instant,
    ) -> Result<Arc<connection::Connection>, TransportError> {
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        if Instant::now() >= deadline {
            return Err(TransportError::Timeout);
        }
        let key = Address::parse(address)?.0;
        if let Some(connection) = self.connections.lock().unwrap().get(&key).cloned() {
            if !connection.is_closed() && !connection.is_retired() {
                return Ok(connection);
            }
        }
        let gate = {
            let mut connecting = self.connecting.lock().unwrap();
            connecting.retain(|_, gate| gate.strong_count() > 0);
            match connecting.get(&key).and_then(Weak::upgrade) {
                Some(gate) => gate,
                None => {
                    let gate = Arc::new(tokio::sync::Mutex::new(()));
                    connecting.insert(key.clone(), Arc::downgrade(&gate));
                    gate
                }
            }
        };
        let _guard = tokio::time::timeout_at(deadline, gate.lock())
            .await
            .map_err(|_| TransportError::Timeout)?;
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        if let Some(connection) = self.connections.lock().unwrap().get(&key).cloned() {
            if !connection.is_closed() && !connection.is_retired() {
                return Ok(connection);
            }
        }
        let mut shutdown = self.shutdown.subscribe();
        let connect_timeout = self.config.connect_timeout_duration();
        let connect_deadline = if self.config.connect_timeout == 0 {
            deadline
        } else {
            deadline.min(Instant::now() + connect_timeout)
        };
        let (host, _) = connection::split_address(&key)?;
        let proxy = super::config::ClientConfig::proxy_for(&self.socks_proxy_routes, &host)?;
        self.notify_channel(&key, super::NettyEventType::Connect);
        let mut dial = Box::pin(connection::connect(
            &key,
            &self.security,
            proxy.as_ref(),
            self.config.client_socket_send_buffer_size,
            self.config.client_socket_receive_buffer_size,
            connect_deadline,
        ));
        let stream = tokio::select! {
            result = &mut dial => result?,
            result = shutdown.changed() => {
                let _ = result;
                drop(dial);
                return Err(TransportError::Shutdown);
            }
        };
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            drop(stream);
            return Err(TransportError::Shutdown);
        }
        let generation = self
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let connection = connection::Connection::spawn(
            stream,
            generation,
            Arc::downgrade(self),
            self.max_frame_size,
            self.config.write_buffer_watermarks(),
            key.clone(),
            self.config.pooled_byte_buffer_allocator_enabled,
        );
        let mut connections = self.connections.lock().unwrap();
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            connection.close();
            drop(connections);
            connection.join().await;
            return Err(TransportError::Shutdown);
        }
        connections.insert(key.clone(), connection.clone());
        drop(connections);
        self.notify_channel(&key, super::NettyEventType::Active);
        Ok(connection)
    }

    pub(super) fn connection_closed(&self, generation: u64, failure: Option<TransportError>) {
        let (was_retired, address, removed) = {
            let mut retired = self.retired_connections.lock().unwrap();
            let mut connections = self.connections.lock().unwrap();
            if let Some((address, _)) = retired.remove(&generation) {
                (true, Some(address), None)
            } else {
                let address = connections.iter().find_map(|(address, connection)| {
                    (connection.generation == generation).then(|| address.clone())
                });
                let removed = address
                    .as_ref()
                    .and_then(|address| connections.remove(address));
                (false, address, removed)
            }
        };
        if let Some(address) = &address {
            if failure.is_some() && failure != Some(TransportError::Closed) {
                self.notify_channel(address, super::NettyEventType::Exception);
            }
            self.notify_channel(address, super::NettyEventType::Close);
            if !was_retired {
                let mut chosen = self.chosen_nameserver.lock().unwrap();
                if chosen.as_deref() == Some(address.as_str()) {
                    *chosen = None;
                }
                drop(chosen);
                self.available_nameservers.lock().unwrap().remove(address);
            }
        }
        if let Some(connection) = removed {
            connection.close();
        }
        self.fail_generation(generation, failure.unwrap_or(TransportError::Closed));
    }
}

#[cfg(test)]
mod tests {
    use super::Address;
    use crate::client::connection;
    use std::net::ToSocketAddrs;

    #[test]
    fn address_splitting_matches_pinned_java_last_colon_oracle() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/address.json"))
                .unwrap();
        for case in fixture["clientAddressCases"].as_array().unwrap() {
            let input = case["address"].as_str().unwrap();
            let java_host = case["host"].as_str().unwrap();
            let java_accepts = case["socketAddressAccepted"].as_bool().unwrap();
            let parsed = Address::parse(input);
            assert_eq!(
                parsed.is_ok(),
                java_accepts,
                "port/address acceptance for {case}"
            );
            if !java_accepts {
                continue;
            }
            let java_port = case["portNumber"].as_i64().unwrap() as u16;
            let expected_host = java_host
                .strip_prefix('[')
                .and_then(|host| host.strip_suffix(']'))
                .unwrap_or(java_host);

            let parsed = parsed.unwrap_or_else(|error| {
                panic!("native parser rejected Java address {input:?}: {error}")
            });
            assert_eq!(
                connection::split_address(input).unwrap(),
                (expected_host.to_owned(), java_port),
                "{case}"
            );
            let normalized = connection::split_address(parsed.as_str()).unwrap();
            if java_host.is_empty() {
                assert_eq!(normalized.1, java_port, "normalized port {case}");
                let resolves_to_loopback = (normalized.0.as_str(), normalized.1)
                    .to_socket_addrs()
                    .unwrap()
                    .any(|address| address.ip().is_loopback());
                assert_eq!(
                    resolves_to_loopback,
                    case["resolvedHostIsLoopback"].as_bool().unwrap(),
                    "normalized empty-host resolution {case}"
                );
            } else {
                assert_eq!(normalized, (expected_host.to_owned(), java_port), "{case}");
            }
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn named_ipv6_scope_resolves_to_the_interface_index() {
        use std::net::ToSocketAddrs;

        let interface = if cfg!(target_os = "macos") {
            "lo0"
        } else {
            "lo"
        };
        let host = format!("fe80::1%{interface}");
        let resolved = (host.as_str(), 10911)
            .to_socket_addrs()
            .expect("the loopback interface name resolves")
            .find_map(|address| match address {
                std::net::SocketAddr::V6(address) => Some(address),
                std::net::SocketAddr::V4(_) => None,
            })
            .expect("the IPv6 literal resolves to IPv6");
        assert_eq!(
            resolved.ip(),
            &"fe80::1".parse::<std::net::Ipv6Addr>().unwrap()
        );
        assert_ne!(resolved.scope_id(), 0);

        let parsed = Address::parse(&format!("{host}:10911")).unwrap();
        assert_eq!(
            connection::split_address(parsed.as_str()).unwrap(),
            (host, 10911)
        );
    }
}
