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
            let parsed_host = host
                .parse::<std::net::Ipv6Addr>()
                .map_err(|error| TransportError::Connect(error.to_string()))?;
            if port.parse::<u16>().is_err() {
                return Err(TransportError::Connect("invalid address port".into()));
            }
            format!("[{}]:{}", parsed_host, port)
        } else if value.matches(':').count() > 1 {
            return Err(TransportError::Connect(
                "IPv6 addresses must be enclosed in brackets".into(),
            ));
        } else {
            let (host, port) = value
                .rsplit_once(':')
                .ok_or_else(|| TransportError::Connect("missing address port".into()))?;
            let parsed_port = port
                .parse::<u16>()
                .map_err(|error| TransportError::Connect(error.to_string()))?;
            if host.is_empty() {
                return Err(TransportError::Connect("host must not be empty".into()));
            }
            format!("{}:{}", host.to_ascii_lowercase(), parsed_port)
        };
        let (_, port) = connection::split_address(&normalized)?;
        if port == 0 {
            return Err(TransportError::Connect("port must be nonzero".into()));
        }
        Ok(Self(normalized))
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
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
        self.notify_channel(&key, super::ChannelEvent::Connect);
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
        self.notify_channel(&key, super::ChannelEvent::Active);
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
                self.notify_channel(address, super::ChannelEvent::Exception);
            }
            self.notify_channel(address, super::ChannelEvent::Close);
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
