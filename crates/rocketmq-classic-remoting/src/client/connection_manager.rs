use std::sync::{Arc, Weak};

use tokio::time::Instant;

use crate::error::TransportError;

use super::{ClientInner, connection};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Address(String);

impl Address {
    pub fn parse(value: &str) -> Result<Self, TransportError> {
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

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl ClientInner {
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
            if !connection.is_closed() {
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
            if !connection.is_closed() {
                return Ok(connection);
            }
        }
        let mut shutdown = self.shutdown.subscribe();
        let connect_deadline = if self.config.connect_timeout.is_zero() {
            deadline
        } else {
            deadline.min(Instant::now() + self.config.connect_timeout)
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
        let address = self
            .connections
            .lock()
            .unwrap()
            .iter()
            .find_map(|(address, connection)| {
                (connection.generation == generation).then(|| address.clone())
            });
        if let Some(address) = &address {
            if failure.is_some() && failure != Some(TransportError::Closed) {
                self.notify_channel(address, super::ChannelEvent::Exception);
                self.notify_channel(address, super::ChannelEvent::Close);
            } else {
                self.notify_channel(address, super::ChannelEvent::Close);
            }
            let mut chosen = self.chosen_nameserver.lock().unwrap();
            if chosen.as_ref() == Some(address) {
                *chosen = None;
            }
            drop(chosen);
            self.available_nameservers.lock().unwrap().remove(address);
        }
        let removed = {
            let mut connections = self.connections.lock().unwrap();
            let addresses = connections
                .iter()
                .filter_map(|(address, connection)| {
                    (connection.generation == generation).then_some(address.clone())
                })
                .collect::<Vec<_>>();
            addresses
                .into_iter()
                .filter_map(|address| connections.remove(&address))
                .collect::<Vec<_>>()
        };
        for connection in removed {
            connection.close();
        }
        self.fail_generation(generation, failure.unwrap_or(TransportError::Closed));
    }
}
