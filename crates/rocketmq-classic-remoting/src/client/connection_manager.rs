use std::sync::{Arc, Weak};

use tokio::time::Instant;

use crate::error::TransportError;

use super::{ChannelHandle, ClientInner, connection};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(super) struct Address(String);

impl Address {
    pub(super) fn parse(value: &str) -> Result<Self, TransportError> {
        let (_, port) = value
            .rsplit_once(':')
            .ok_or_else(|| TransportError::Connect("missing address port".into()))?;
        parse_socket_port(port)?;
        // Java's channelTables is keyed by the unmodified address string.
        // Parsing validates the endpoint but must not collapse alternate spellings.
        Ok(Self(value.to_owned()))
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

pub(super) fn parse_socket_port(value: &str) -> Result<u16, TransportError> {
    let port = super::java_number::parse_i32(value)
        .ok_or_else(|| TransportError::Connect("invalid address port".into()))?;
    u16::try_from(port).map_err(|_| TransportError::Connect("invalid address port".into()))
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
        self.connection_with_deadline(address, Some(deadline)).await
    }

    pub(super) async fn connection_for_scan(
        self: &Arc<Self>,
        address: &str,
    ) -> Result<Arc<connection::Connection>, TransportError> {
        self.connection_with_deadline(address, None).await
    }

    async fn connection_with_deadline(
        self: &Arc<Self>,
        address: &str,
        deadline: Option<Instant>,
    ) -> Result<Arc<connection::Connection>, TransportError> {
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        let key = Address::parse(address)?.0;
        if let Some(connection) = self.connections.lock().unwrap().get(&key).cloned() {
            if !connection.is_closed() && !connection.is_retired() {
                return Ok(connection);
            }
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(TransportError::Timeout);
        }
        let max_frame_size = self.frame_max_length()?;
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
        let gate_lock = gate.lock();
        let _guard = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, gate_lock)
                .await
                .map_err(|_| TransportError::Timeout)?,
            None => gate_lock.await,
        };
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        if let Some(connection) = self.connections.lock().unwrap().get(&key).cloned() {
            if !connection.is_closed() && !connection.is_retired() {
                return Ok(connection);
            }
        }
        let mut shutdown = self.shutdown.subscribe();
        let (host, _) = connection::split_address(&key)?;
        let selected = super::config::ClientConfig::proxy_for(&self.socks_proxy_routes, &host)?;
        let proxy_cidr = selected.as_ref().map(|selection| selection.cidr.as_str());
        let connect_deadline = self.connection_deadline_for_route(proxy_cidr, deadline);
        let connection_settings = self.connection_settings_for_route(proxy_cidr)?;
        let generation = self
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let proxy = match selected.as_ref() {
            Some(selection) => selection.connect_proxy()?,
            None => None,
        };
        let channel = ChannelHandle::pending(&key, generation);
        self.notify_channel(super::NettyEventType::Connect, channel.clone());
        let (max_idle_time, disable_worker_group) = self.channel_pipeline_settings();
        let mut dial = Box::pin(connection::connect(
            &key,
            &self.security,
            &self.use_tls,
            proxy,
            connection_settings.send_buffer_size,
            connection_settings.receive_buffer_size,
            connect_deadline,
        ));
        let mut channel_closed = channel.subscribe_closed();
        if *channel_closed.borrow() {
            drop(dial);
            return Err(TransportError::Closed);
        }
        let stream = tokio::select! {
            result = &mut dial => match result {
                Ok(stream) => stream,
                Err(error) => {
                    channel.mark_closed();
                    return Err(error);
                }
            },
            result = shutdown.changed() => {
                let _ = result;
                drop(dial);
                channel.mark_closed();
                return Err(TransportError::Shutdown);
            },
            result = channel_closed.changed() => {
                let _ = result;
                drop(dial);
                return Err(TransportError::Closed);
            }
        };
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            drop(stream);
            channel.mark_closed();
            return Err(TransportError::Shutdown);
        }
        let connection = connection::Connection::spawn_with_channel_settings(
            stream,
            generation,
            Arc::downgrade(self),
            channel.clone(),
            max_frame_size,
            connection_settings.write_buffer_watermarks,
            key.clone(),
            connection_settings.pooled_byte_buffer_allocator_enabled,
            max_idle_time,
            disable_worker_group,
        );
        let mut connections = self.connections.lock().unwrap();
        if self.closed.load(std::sync::atomic::Ordering::Acquire) || !channel.is_open() {
            connection.close();
            drop(connections);
            connection.join().await;
            return Err(TransportError::Shutdown);
        }
        connections.insert(key.clone(), connection.clone());
        drop(connections);
        self.notify_channel(super::NettyEventType::Active, channel);
        Ok(connection)
    }

    pub(super) fn connection_closed(&self, generation: u64, failure: Option<TransportError>) {
        let (was_retired, address, removed) = {
            let mut retired = self.retired_connections.lock().unwrap();
            let mut connections = self.connections.lock().unwrap();
            if let Some((address, connection)) = retired.remove(&generation) {
                (true, Some(address), Some(connection))
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
            let channel = removed
                .as_ref()
                .map(|connection| connection.channel_handle());
            if failure.is_some() && failure != Some(TransportError::Closed) {
                if let Some(channel) = &channel {
                    self.notify_channel(super::NettyEventType::Exception, channel.clone());
                }
            }
            if let Some(channel) = &channel {
                self.notify_channel(super::NettyEventType::Close, channel.clone());
            }
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
    use super::{Address, parse_socket_port};
    use crate::client::connection;
    use std::collections::BTreeMap;
    use std::net::ToSocketAddrs;

    #[test]
    fn address_splitting_matches_pinned_java_last_colon_oracle() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/address.json"))
                .unwrap();
        let cases = fixture["clientAddressCases"].as_array().unwrap();
        assert_eq!(cases.len(), 49);
        for (case, expected) in cases.iter().skip(12).take(14).zip([
            "127.0.0.1:+1",
            "127.0.0.1:-0",
            "127.0.0.1:+0001",
            "127.0.0.1:00000",
            "127.0.0.1:+١",
            "127.0.0.1:1 ",
            "127.0.0.1: 1",
            "127.0.0.1:\t1",
            "127.0.0.1:＋1",
            "127.0.0.1:−1",
            "127.0.0.1:00000000000000001",
            "127.0.0.1:2147483647",
            "127.0.0.1:-2147483648",
            "127.0.0.1:-2147483649",
        ]) {
            assert_eq!(case["address"], expected);
        }
        for (case, expected) in cases.iter().skip(38).zip([
            "",
            "broker.example",
            "broker.example:",
            "broker.example: ",
            "host:1:2",
            "host::2",
            "[2001:db8::1:10911",
            "[2001:db8::1]10911",
            "[2001:db8::1]junk:10911",
            " host:10911",
            "host :10911",
        ]) {
            assert_eq!(case["address"], expected);
        }
        for case in cases {
            let input = case["address"].as_str().unwrap();
            if case.get("splitError").is_some() {
                assert!(Address::parse(input).is_err(), "Java split rejected {case}");
                continue;
            }
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
            assert_eq!(parsed.as_str(), input, "Java channel-table key for {case}");
            assert_eq!(
                connection::split_address(input).unwrap(),
                (expected_host.to_owned(), java_port),
                "{case}"
            );
            let normalized = connection::split_address(parsed.as_str()).unwrap();
            if java_host.is_empty() {
                assert_eq!(normalized.1, java_port, "normalized port {case}");
                let host = connection::connect_host(&normalized.0);
                let resolved = (host.as_str(), normalized.1)
                    .to_socket_addrs()
                    .unwrap()
                    .next()
                    .expect("normalized host resolves");
                let java_resolved_ip: std::net::IpAddr = case["resolvedHostAddress"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                assert_eq!(resolved.ip(), java_resolved_ip, "normalized target {case}");
                assert_eq!(
                    resolved.ip().is_loopback(),
                    case["resolvedHostIsLoopback"].as_bool().unwrap(),
                    "normalized empty-host resolution {case}"
                );
            } else {
                assert_eq!(normalized, (expected_host.to_owned(), java_port), "{case}");
            }
        }
    }

    #[test]
    fn address_cache_keys_preserve_java_raw_endpoint_identity() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/address.json"))
                .unwrap();
        for case in fixture["channelTableKeyCases"].as_array().unwrap() {
            assert_eq!(
                case["javaChannelTableKeysDistinct"].as_bool(),
                Some(true),
                "pinned Java channel-table result for {case}"
            );
            let first = case["firstAddress"].as_str().unwrap();
            let second = case["secondAddress"].as_str().unwrap();
            let first_key = Address::parse(first).unwrap();
            let second_key = Address::parse(second).unwrap();
            assert_ne!(
                first_key.as_str(),
                second_key.as_str(),
                "Java retains these distinct channel-table keys: {case}"
            );
        }
    }

    #[test]
    fn every_bmp_port_digit_matches_pinned_java_integer_parse_int() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/address.json"))
                .unwrap();
        let cases = fixture["javaPortDigitCases"].as_array().unwrap();
        let mut java_digits = BTreeMap::new();
        for case in cases {
            let code_unit = case["codeUnit"].as_u64().unwrap() as u16;
            let port = case["port"].as_u64().unwrap() as u16;
            assert!(java_digits.insert(code_unit, port).is_none(), "{case}");
        }
        for code_unit in 0..=u16::MAX {
            let character = char::from_u32(u32::from(code_unit)).unwrap_or('\u{fffd}');
            let input = character.to_string();
            let actual = parse_socket_port(&input).ok();
            let expected = java_digits.get(&code_unit).copied();
            assert_eq!(actual, expected, "U+{code_unit:04X} ({input:?})");
        }
    }

    #[test]
    fn every_representable_bmp_host_unit_matches_pinned_java_endpoint_split() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/address.json"))
                .unwrap();
        let accepted_count = fixture["javaHostCodeUnitAcceptedCount"].as_u64().unwrap();
        assert_eq!(accepted_count, u64::from(u16::MAX) + 1);
        let ranges = fixture["javaHostCodeUnitRanges"].as_array().unwrap();
        let is_java_accepted = |code_unit: u16| {
            ranges.iter().any(|range| {
                let start = range[0].as_u64().unwrap() as u16;
                let end = range[1].as_u64().unwrap() as u16;
                (start..=end).contains(&code_unit)
            })
        };

        for code_unit in 0..=u16::MAX {
            let Some(character) = char::from_u32(u32::from(code_unit)) else {
                // Rust str cannot represent Java's isolated UTF-16 surrogates.
                continue;
            };
            let host = format!("broker{character}.example");
            let address = format!("{host}:10911");
            let expected = is_java_accepted(code_unit);
            let parsed = Address::parse(&address);
            assert_eq!(
                parsed.is_ok(),
                expected,
                "U+{code_unit:04X} ({character:?}) Java unresolved-host acceptance"
            );
            if expected {
                assert_eq!(parsed.unwrap().as_str(), address);
                assert_eq!(
                    connection::split_address(&address).unwrap(),
                    (host, 10911),
                    "U+{code_unit:04X} ({character:?}) Java host/port split"
                );
            }
        }
    }

    #[test]
    fn every_unicode_scalar_host_matches_pinned_java_endpoint_split() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/address.json"))
                .unwrap();
        let accepted_count = fixture["javaHostCodePointAcceptedCount"].as_u64().unwrap();
        assert_eq!(accepted_count, 0x110000 - 0x800);
        let ranges = fixture["javaHostCodePointRanges"].as_array().unwrap();
        let is_java_accepted = |code_point: u32| {
            ranges.iter().any(|range| {
                let start = range[0].as_u64().unwrap() as u32;
                let end = range[1].as_u64().unwrap() as u32;
                (start..=end).contains(&code_point)
            })
        };

        for code_point in 0..=0x10_FFFF {
            let Some(character) = char::from_u32(code_point) else {
                continue;
            };
            let host = format!("broker{character}.example");
            let address = format!("{host}:10911");
            let expected = is_java_accepted(code_point);
            let parsed = Address::parse(&address);
            assert_eq!(
                parsed.is_ok(),
                expected,
                "U+{code_point:04X} ({character:?}) Java unresolved-host acceptance"
            );
            if expected {
                assert_eq!(parsed.unwrap().as_str(), address);
                assert_eq!(
                    connection::split_address(&address).unwrap(),
                    (host, 10911),
                    "U+{code_point:04X} ({character:?}) Java host/port split"
                );
            }
        }
    }

    #[test]
    fn every_java_valid_socket_port_is_accepted_and_preserved() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../../fixtures/runtime/address.json"))
                .unwrap();
        let oracle = &fixture["javaPortRangeCase"];
        let minimum = oracle["minimum"].as_u64().unwrap() as u16;
        let maximum = oracle["maximum"].as_u64().unwrap() as u16;
        assert_eq!(minimum, 0);
        assert_eq!(maximum, u16::MAX);
        assert_eq!(oracle["acceptedCount"].as_u64(), Some(65_536));
        assert_eq!(oracle["acceptedPortSum"].as_u64(), Some(2_147_450_880));
        assert_eq!(oracle["allAcceptedAndPreserved"].as_bool(), Some(true));

        for port in minimum..=maximum {
            let input = format!("127.0.0.1:{port}");
            let key = Address::parse(&input)
                .unwrap_or_else(|error| panic!("pinned Java accepted socket port {port}: {error}"));
            assert_eq!(key.as_str(), input);
            assert_eq!(
                connection::split_address(key.as_str()).unwrap(),
                ("127.0.0.1".to_owned(), port),
                "Java accepted and preserved port {port}"
            );
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
