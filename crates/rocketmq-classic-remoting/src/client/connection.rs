use std::sync::{Arc, Weak, atomic::AtomicBool};

use bytes::BytesMut;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpSocket,
    sync::mpsc,
    task::JoinHandle,
    time::{self, Instant},
};
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_socks::tcp::Socks5Stream;

use crate::{codec::frame, error::TransportError};

use super::{ClientInner, config::SocksProxyConfig};

pub use super::tls::TransportMode;

pub(super) type BoxedStream = Box<dyn AsyncIo>;

pub(super) trait AsyncIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncIo for T {}

struct OutboundFrame {
    bytes: Vec<u8>,
    deadline: Instant,
    cancelled: Option<tokio::sync::oneshot::Receiver<()>>,
    written: tokio::sync::oneshot::Sender<Result<(), TransportError>>,
}

pub(crate) struct Connection {
    pub(crate) generation: u64,
    sender: mpsc::Sender<OutboundFrame>,
    address: String,
    last_activity: std::sync::Mutex<std::time::Instant>,
    closed: Arc<AtomicBool>,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    queued_write_bytes: Arc<std::sync::atomic::AtomicUsize>,
    write_watermarks: Option<(usize, usize)>,
    writable: Arc<AtomicBool>,
}

impl Connection {
    pub(crate) fn spawn(
        stream: BoxedStream,
        generation: u64,
        inner: Weak<ClientInner>,
        max_frame_size: usize,
        outbound_queue_capacity: usize,
        write_watermarks: Option<(usize, usize)>,
        address: String,
    ) -> Arc<Self> {
        let (sender, receiver) = mpsc::channel(outbound_queue_capacity);
        let closed = Arc::new(AtomicBool::new(false));
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let connection = Arc::new(Self {
            generation,
            sender,
            address: address.clone(),
            last_activity: std::sync::Mutex::new(std::time::Instant::now()),
            closed: closed.clone(),
            task: tokio::sync::Mutex::new(None),
            shutdown,
            queued_write_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            write_watermarks,
            writable: Arc::new(AtomicBool::new(true)),
        });
        let task = tokio::spawn(run_connection(
            stream,
            receiver,
            inner,
            ConnectionTask {
                generation,
                closed,
                shutdown: shutdown_rx,
                max_frame_size,
                address,
                queued_write_bytes: connection.queued_write_bytes.clone(),
                write_watermarks,
                writable: connection.writable.clone(),
            },
        ));
        connection
            .task
            .try_lock()
            .expect("new connection task lock")
            .replace(task);
        connection
    }

    pub(crate) async fn send(
        &self,
        bytes: Vec<u8>,
        deadline: Instant,
    ) -> Result<(), TransportError> {
        let (written, response) = tokio::sync::oneshot::channel();
        let length = bytes.len();
        self.reserve_write_bytes(length);
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        let mut cancel = Some(cancel);
        let queued = time::timeout_at(
            deadline,
            self.sender.send(OutboundFrame {
                bytes,
                deadline,
                cancelled: Some(cancelled),
                written,
            }),
        )
        .await;
        match queued {
            Err(_) => {
                self.release_write_bytes(length);
                return Err(TransportError::Timeout);
            }
            Ok(Err(_)) => {
                self.release_write_bytes(length);
                return Err(TransportError::Closed);
            }
            Ok(Ok(())) => {}
        }
        let result = time::timeout_at(deadline, response)
            .await
            .map_err(|_| {
                drop(cancel.take());
                TransportError::Timeout
            })?
            .map_err(|_| TransportError::Closed)?;
        drop(cancel.take());
        result
    }

    pub(crate) fn try_send(&self, bytes: Vec<u8>, deadline: Instant) -> Result<(), TransportError> {
        let (written, _response) = tokio::sync::oneshot::channel();
        self.reserve_write_bytes(bytes.len());
        let length = bytes.len();
        self.sender
            .try_send(OutboundFrame {
                bytes,
                deadline,
                cancelled: None,
                written,
            })
            .map_err(|error| {
                self.release_write_bytes(length);
                match error {
                    mpsc::error::TrySendError::Full(_) => TransportError::InboundOverloaded,
                    mpsc::error::TrySendError::Closed(_) => TransportError::Closed,
                }
            })
    }

    pub(crate) fn close(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.shutdown.send(true);
    }

    pub(crate) fn address(&self) -> &str {
        &self.address
    }

    pub(crate) fn idle_for(&self) -> std::time::Duration {
        self.last_activity.lock().unwrap().elapsed()
    }

    pub(crate) fn mark_active(&self) {
        *self.last_activity.lock().unwrap() = std::time::Instant::now();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn is_writable(&self) -> bool {
        !self.is_closed()
            && self
                .write_watermarks
                .map_or(self.sender.capacity() > 0, |_| {
                    self.writable.load(std::sync::atomic::Ordering::Acquire)
                })
    }

    #[cfg(test)]
    pub(super) fn watermark_is_writable(&self) -> bool {
        self.writable.load(std::sync::atomic::Ordering::Acquire)
    }

    fn reserve_write_bytes(&self, length: usize) {
        let queued = self
            .queued_write_bytes
            .fetch_add(length, std::sync::atomic::Ordering::AcqRel)
            .saturating_add(length);
        if let Some((high, _)) = self.write_watermarks {
            if queued >= high {
                self.writable
                    .store(false, std::sync::atomic::Ordering::Release);
            }
        }
    }

    pub(super) fn release_write_bytes(&self, length: usize) {
        let queued = self
            .queued_write_bytes
            .fetch_sub(length, std::sync::atomic::Ordering::AcqRel)
            .saturating_sub(length);
        if let Some((_, low)) = self.write_watermarks {
            if queued <= low {
                self.writable
                    .store(true, std::sync::atomic::Ordering::Release);
            }
        }
    }

    pub(crate) async fn join(&self) {
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
    }
}

struct ConnectionTask {
    generation: u64,
    closed: Arc<AtomicBool>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    max_frame_size: usize,
    address: String,
    queued_write_bytes: Arc<std::sync::atomic::AtomicUsize>,
    write_watermarks: Option<(usize, usize)>,
    writable: Arc<AtomicBool>,
}

async fn run_connection(
    stream: BoxedStream,
    mut outbound: mpsc::Receiver<OutboundFrame>,
    inner: Weak<ClientInner>,
    task: ConnectionTask,
) {
    let ConnectionTask {
        generation,
        closed,
        mut shutdown,
        max_frame_size,
        address,
        queued_write_bytes,
        write_watermarks,
        writable,
    } = task;
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut input = BytesMut::with_capacity(8192);
    let mut read_buffer = vec![0; 8192];
    let failure = 'connection: loop {
        tokio::select! {
            biased;
            result = shutdown.changed() => if result.is_err() || *shutdown.borrow() { break None },
            frame = outbound.recv() => match frame {
                Some(frame) => {
                    let OutboundFrame { bytes, deadline, mut cancelled, written } = frame;
                    let write_length = bytes.len();
                    let result = tokio::select! {
                        result = time::timeout_at(deadline, writer.write_all(&bytes)) => result.map_err(|_| TransportError::Timeout).and_then(|result| result.map_err(|error| TransportError::Send(error.to_string()))),
                        _ = async { match &mut cancelled { Some(cancelled) => { let _ = cancelled.await; }, None => std::future::pending().await } } => Err(TransportError::Closed),
                        result = shutdown.changed() => {
                            let _ = result;
                            Err(TransportError::Shutdown)
                        }
                    };
                    if result.is_ok() {
                        if let Some(inner) = inner.upgrade() {
                            let connection = inner
                                .connections
                                .lock()
                                .unwrap()
                                .values()
                                .find(|connection| connection.generation == generation)
                                .cloned();
                            if let Some(connection) = connection {
                                connection.mark_active();
                            }
                        }
                    }
                    let queued = queued_write_bytes
                        .fetch_sub(write_length, std::sync::atomic::Ordering::AcqRel)
                        .saturating_sub(write_length);
                    if let Some((_, low)) = write_watermarks {
                        if queued <= low {
                            writable.store(true, std::sync::atomic::Ordering::Release);
                        }
                    }
                    let _ = written.send(result.clone());
                    if result.is_err() {
                        break if result == Err(TransportError::Shutdown) { None } else { result.err() }
                    }
                },
                None if closed.load(std::sync::atomic::Ordering::Acquire) => break None,
                None => break Some(TransportError::Closed),
            },
            result = reader.read(&mut read_buffer) => match result {
                Ok(0) => break Some(TransportError::Closed),
                Ok(length) => {
                    if let Some(inner) = inner.upgrade() {
                        if let Some(connection) = inner
                            .connections
                            .lock()
                            .unwrap()
                            .values()
                            .find(|connection| connection.generation == generation)
                            .cloned()
                        {
                            connection.mark_active();
                        }
                    }
                    input.extend_from_slice(&read_buffer[..length]);
                    loop {
                        match frame::decode_next(&mut input, max_frame_size) {
                            Ok(Some(command)) => {
                                if command.is_response() {
                                    if let Some(inner) = inner.upgrade() {
                                        inner.complete_response(generation, command);
                                    }
                                } else if let Some(inner) = inner.upgrade() {
                                    let oneway = command.is_oneway();
                                    let opaque = command.opaque;
                                    if inner.inbound.try_send(super::processor::InboundRequest {
                                        generation,
                                        remote_addr: address.clone(),
                                        command,
                                    }).is_err() && !oneway {
                                        inner.send_overload_response(generation, opaque);
                                    }
                                } else {
                                    break 'connection None;
                                }
                            }
                            Ok(None) => break,
                            Err(error) => break 'connection Some(TransportError::MalformedFrame(error)),
                        }
                    }
                }
                Err(_) => break Some(TransportError::Closed),
            },
        }
    };
    closed.store(true, std::sync::atomic::Ordering::Release);
    if let Some(inner) = inner.upgrade() {
        inner.connection_closed(generation, failure);
    }
}

pub(super) async fn connect(
    address: &str,
    mode: &TransportMode,
    proxy: Option<&SocksProxyConfig>,
    send_buffer_size: usize,
    receive_buffer_size: usize,
    deadline: Instant,
) -> Result<BoxedStream, TransportError> {
    let (host, port) = split_address(address)?;
    let stream = time::timeout(deadline.saturating_duration_since(Instant::now()), async {
        if let Some(proxy) = proxy {
            let stream = match (&proxy.username, &proxy.password) {
                (Some(username), Some(password)) => {
                    Socks5Stream::connect_with_password(
                        proxy.address.as_str(),
                        (host.as_str(), port),
                        username,
                        password,
                    )
                    .await
                }
                _ => Socks5Stream::connect(proxy.address.as_str(), (host.as_str(), port)).await,
            }
            .map_err(|error| TransportError::Connect(error.to_string()))?
            .into_inner();
            let socket = socket2::SockRef::from(&stream);
            if send_buffer_size > 0 {
                socket
                    .set_send_buffer_size(send_buffer_size)
                    .map_err(|error| TransportError::Connect(error.to_string()))?;
            }
            if receive_buffer_size > 0 {
                socket
                    .set_recv_buffer_size(receive_buffer_size)
                    .map_err(|error| TransportError::Connect(error.to_string()))?;
            }
            Ok(Box::new(stream) as BoxedStream)
        } else {
            let addresses = tokio::net::lookup_host((host.as_str(), port))
                .await
                .map_err(|error| TransportError::Connect(error.to_string()))?
                .collect::<Vec<_>>();
            if addresses.is_empty() {
                return Err(TransportError::Connect(
                    "address resolved to no socket addresses".into(),
                ));
            }
            let mut connected = None;
            let mut last_error = None;
            for address in addresses {
                let socket = if address.is_ipv6() {
                    TcpSocket::new_v6()
                } else {
                    TcpSocket::new_v4()
                }
                .map_err(|error| TransportError::Connect(error.to_string()))?;
                let socket_ref = socket2::SockRef::from(&socket);
                if send_buffer_size > 0 {
                    socket_ref
                        .set_send_buffer_size(send_buffer_size)
                        .map_err(|error| TransportError::Connect(error.to_string()))?;
                }
                if receive_buffer_size > 0 {
                    socket_ref
                        .set_recv_buffer_size(receive_buffer_size)
                        .map_err(|error| TransportError::Connect(error.to_string()))?;
                }
                match socket.connect(address).await {
                    Ok(stream) => {
                        connected = Some(stream);
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            let stream = connected.ok_or_else(|| {
                TransportError::Connect(
                    last_error
                        .map_or_else(|| "connection failed".into(), |error| error.to_string()),
                )
            })?;
            stream
                .set_nodelay(true)
                .map_err(|error| TransportError::Connect(error.to_string()))?;
            Ok(Box::new(stream) as BoxedStream)
        }
    })
    .await
    .map_err(|_| TransportError::Timeout)??;
    match mode {
        TransportMode::Plain => Ok(stream),
        TransportMode::Tls(config) => {
            let server_name = ServerName::try_from(host)
                .map_err(|error| TransportError::Tls(error.to_string()))?;
            let connector = config.connector()?;
            time::timeout(
                deadline.saturating_duration_since(Instant::now()),
                connector.connect(server_name, stream),
            )
            .await
            .map_err(|_| TransportError::Timeout)?
            .map(|tls| Box::new(tls) as BoxedStream)
            .map_err(|error| TransportError::Tls(error.to_string()))
        }
    }
}

pub(super) fn split_address(address: &str) -> Result<(String, u16), TransportError> {
    let (host, port) = if let Some(rest) = address.strip_prefix('[') {
        let (host, suffix) = rest
            .split_once(']')
            .ok_or_else(|| TransportError::Connect("invalid bracketed IPv6 address".into()))?;
        let port = suffix
            .strip_prefix(':')
            .ok_or_else(|| TransportError::Connect("missing address port".into()))?;
        (host, port)
    } else {
        address
            .rsplit_once(':')
            .ok_or_else(|| TransportError::Connect("missing address port".into()))?
    };
    let port = port
        .parse()
        .map_err(|error: std::num::ParseIntError| TransportError::Connect(error.to_string()))?;
    Ok((host.to_owned(), port))
}
