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

use crate::{
    codec::frame,
    error::{ErrorSource, TransportError},
};

use super::{ClientInner, config::SocksProxyConfig};

pub use super::tls::TransportMode;

pub(super) type BoxedStream = Box<dyn AsyncIo>;

pub(super) trait AsyncIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncIo for T {}

struct OutboundFrame {
    bytes: Option<Vec<u8>>,
    frame_pool: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    pooled: bool,
    deadline: Option<Instant>,
    cancelled: Option<tokio::sync::oneshot::Receiver<()>>,
    written: Option<tokio::sync::oneshot::Sender<Result<(), TransportError>>>,
}

impl Drop for OutboundFrame {
    fn drop(&mut self) {
        if self.pooled {
            if let Some(mut bytes) = self.bytes.take() {
                bytes.clear();
                let mut pool = self.frame_pool.lock().unwrap();
                if pool.len() < 4 {
                    pool.push(bytes);
                }
            }
        }
    }
}

pub(crate) struct Connection {
    pub(crate) generation: u64,
    sender: mpsc::UnboundedSender<OutboundFrame>,
    address: String,
    pooled_byte_buffer_allocator_enabled: bool,
    closed: Arc<AtomicBool>,
    retired: AtomicBool,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    queued_write_bytes: Arc<std::sync::atomic::AtomicUsize>,
    write_watermarks: (usize, usize),
    writable: Arc<AtomicBool>,
    frame_pool: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

struct FrameGuard<'a> {
    connection: &'a Connection,
    bytes: Option<Vec<u8>>,
}

impl FrameGuard<'_> {
    fn take(&mut self) -> Vec<u8> {
        self.bytes.take().expect("frame is present until queued")
    }
}

impl Drop for FrameGuard<'_> {
    fn drop(&mut self) {
        if let Some(bytes) = self.bytes.take() {
            self.connection.recycle_frame(bytes);
        }
    }
}

impl Connection {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        stream: BoxedStream,
        generation: u64,
        inner: Weak<ClientInner>,
        max_frame_size: usize,
        write_watermarks: (usize, usize),
        address: String,
        pooled_byte_buffer_allocator_enabled: bool,
    ) -> Arc<Self> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let closed = Arc::new(AtomicBool::new(false));
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let disable_worker_group = inner
            .upgrade()
            .is_some_and(|client| client.config.disable_netty_worker_group);
        let connection = Arc::new(Self {
            generation,
            sender,
            address: address.clone(),
            pooled_byte_buffer_allocator_enabled,
            closed: closed.clone(),
            retired: AtomicBool::new(false),
            task: tokio::sync::Mutex::new(None),
            shutdown,
            queued_write_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            write_watermarks,
            writable: Arc::new(AtomicBool::new(true)),
            frame_pool: Arc::new(std::sync::Mutex::new(Vec::new())),
        });
        let max_idle_time = inner.upgrade().map_or(std::time::Duration::ZERO, |client| {
            client.config.max_idle_time_duration()
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
                pooled_byte_buffer_allocator_enabled,
                max_idle_time,
                frame_pool: connection.frame_pool.clone(),
                disable_worker_group,
            },
        ));
        connection
            .task
            .try_lock()
            .expect("new connection task lock")
            .replace(task);
        connection
    }

    pub(crate) fn encode_command(
        &self,
        command: &crate::command::RemotingCommand,
        max_frame_size: usize,
    ) -> Result<Vec<u8>, TransportError> {
        let mut bytes = self.take_frame_buffer();
        if let Err(error) = frame::encode_into(command, max_frame_size, &mut bytes) {
            self.recycle_frame(bytes);
            return Err(TransportError::Codec(error));
        }
        Ok(bytes)
    }

    fn take_frame_buffer(&self) -> Vec<u8> {
        if self.pooled_byte_buffer_allocator_enabled {
            self.frame_pool.lock().unwrap().pop().unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    pub(crate) fn recycle_frame(&self, mut bytes: Vec<u8>) {
        if self.pooled_byte_buffer_allocator_enabled {
            bytes.clear();
            let mut pool = self.frame_pool.lock().unwrap();
            if pool.len() < 4 {
                pool.push(bytes);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn queued_write_bytes(&self) -> usize {
        self.queued_write_bytes
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn pooled_frame_count(&self) -> usize {
        self.frame_pool.lock().unwrap().len()
    }

    #[cfg(test)]
    pub(super) fn pooled_frame_capacity(&self) -> usize {
        self.frame_pool
            .lock()
            .unwrap()
            .iter()
            .map(Vec::capacity)
            .max()
            .unwrap_or_default()
    }

    pub(crate) async fn send(
        &self,
        bytes: Vec<u8>,
        deadline: Instant,
    ) -> Result<(), TransportError> {
        self.send_with_deadline(bytes, Some(deadline)).await
    }

    pub(crate) async fn send_without_deadline(&self, bytes: Vec<u8>) -> Result<(), TransportError> {
        self.send_with_deadline(bytes, None).await
    }

    async fn send_with_deadline(
        &self,
        bytes: Vec<u8>,
        deadline: Option<Instant>,
    ) -> Result<(), TransportError> {
        let mut frame = FrameGuard {
            connection: self,
            bytes: Some(bytes),
        };
        let (written, response) = tokio::sync::oneshot::channel();
        let length = frame.bytes.as_ref().unwrap().len();
        self.reserve_write_bytes(length);
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        let mut cancel = Some(cancel);
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            self.release_write_bytes(length);
            return Err(TransportError::Timeout);
        }
        if let Err(error) = self.sender.send(OutboundFrame {
            bytes: Some(frame.take()),
            frame_pool: self.frame_pool.clone(),
            pooled: self.pooled_byte_buffer_allocator_enabled,
            deadline,
            cancelled: Some(cancelled),
            written: Some(written),
        }) {
            self.release_write_bytes(length);
            drop(error.0);
            return Err(TransportError::Closed);
        }
        let write_result = match deadline {
            Some(deadline) => time::timeout_at(deadline, response).await.map_err(|_| {
                drop(cancel.take());
                TransportError::Timeout
            })?,
            None => response.await,
        };
        let result = write_result.map_err(|_| TransportError::Closed)?;
        drop(cancel.take());
        result
    }

    #[cfg(test)]
    pub(crate) fn try_send(&self, bytes: Vec<u8>, deadline: Instant) -> Result<(), TransportError> {
        let (written, _response) = tokio::sync::oneshot::channel();
        self.reserve_write_bytes(bytes.len());
        let length = bytes.len();
        match self.sender.send(OutboundFrame {
            bytes: Some(bytes),
            frame_pool: self.frame_pool.clone(),
            pooled: self.pooled_byte_buffer_allocator_enabled,
            deadline: Some(deadline),
            cancelled: None,
            written: Some(written),
        }) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.release_write_bytes(length);
                match error {
                    mpsc::error::SendError(_) => Err(TransportError::Closed),
                }
            }
        }
    }

    pub(crate) fn mark_retired(&self) {
        self.retired
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn close(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.shutdown.send(true);
    }

    pub(crate) fn address(&self) -> &str {
        &self.address
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn is_writable(&self) -> bool {
        !self.is_closed() && self.writable.load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn watermark_is_writable(&self) -> bool {
        self.writable.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(super) fn reserve_write_bytes(&self, length: usize) {
        let queued = self
            .queued_write_bytes
            .fetch_add(length, std::sync::atomic::Ordering::AcqRel)
            .saturating_add(length);
        let (high, _) = self.write_watermarks;
        if queued > high {
            self.writable
                .store(false, std::sync::atomic::Ordering::Release);
        }
    }

    pub(super) fn release_write_bytes(&self, length: usize) {
        let queued = self
            .queued_write_bytes
            .fetch_sub(length, std::sync::atomic::Ordering::AcqRel)
            .saturating_sub(length);
        let (_, low) = self.write_watermarks;
        if queued < low {
            self.writable
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    pub(crate) async fn join(&self) {
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
    }

    pub(crate) async fn join_until(&self, deadline: Instant) -> bool {
        let mut task = self.task.lock().await;
        let Some(join) = task.as_mut() else {
            return true;
        };
        if tokio::time::timeout_at(deadline, join).await.is_err() {
            return false;
        }
        task.take();
        true
    }

    pub(crate) async fn abort_task(&self) {
        if let Some(task) = self.task.lock().await.take() {
            task.abort();
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
    write_watermarks: (usize, usize),
    writable: Arc<AtomicBool>,
    pooled_byte_buffer_allocator_enabled: bool,
    max_idle_time: std::time::Duration,
    frame_pool: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    disable_worker_group: bool,
}

async fn run_connection(
    stream: BoxedStream,
    mut outbound: mpsc::UnboundedReceiver<OutboundFrame>,
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
        pooled_byte_buffer_allocator_enabled,
        max_idle_time,
        frame_pool,
        disable_worker_group,
    } = task;
    let (mut reader, mut writer) = tokio::io::split(stream);
    let idle_timer = time::sleep(max_idle_time);
    tokio::pin!(idle_timer);
    let mut input = BytesMut::with_capacity(8192);
    let mut read_buffer = vec![0; 8192];
    let failure = 'connection: loop {
        tokio::select! {
            biased;
            result = shutdown.changed() => if result.is_err() || *shutdown.borrow() { break None },
            _ = &mut idle_timer, if !max_idle_time.is_zero() => {
                if let Some(inner) = inner.upgrade() {
                    inner.notify_channel(&address, super::ChannelEvent::Idle);
                }
                break None;
            },
            frame = outbound.recv() => match frame {
                Some(mut frame) => {
                    let bytes = frame.bytes.as_ref().expect("queued frame bytes");
                    let write_length = bytes.len();
                    let result = tokio::select! {
                        result = async {
                            match frame.deadline {
                                Some(deadline) => time::timeout_at(deadline, writer.write_all(bytes))
                                    .await
                                    .map_err(|_| TransportError::Timeout)?
                                    .map_err(|error| TransportError::SendSource(ErrorSource::new(error))),
                                None => writer.write_all(bytes)
                                    .await
                                    .map_err(|error| TransportError::SendSource(ErrorSource::new(error))),
                            }
                        } => result,
                        _ = async { match &mut frame.cancelled { Some(cancelled) => { let _ = cancelled.await; }, None => std::future::pending().await } } => Err(TransportError::Closed),
                        result = shutdown.changed() => {
                            let _ = result;
                            Err(TransportError::Shutdown)
                        }
                    };
                    if result.is_ok() && !max_idle_time.is_zero() {
                        idle_timer
                            .as_mut()
                            .reset(Instant::now() + max_idle_time);
                    }
                    let queued = queued_write_bytes
                        .fetch_sub(write_length, std::sync::atomic::Ordering::AcqRel)
                        .saturating_sub(write_length);
                    let (_, low) = write_watermarks;
                    if queued < low {
                        writable.store(true, std::sync::atomic::Ordering::Release);
                    }
                    if let Some(bytes) = frame.bytes.take() {
                        if pooled_byte_buffer_allocator_enabled {
                            let mut bytes = bytes;
                            bytes.clear();
                            let mut pool = frame_pool.lock().unwrap();
                            if pool.len() < 4 {
                                pool.push(bytes);
                            }
                        }
                    }
                    if let Some(written) = frame.written.take() {
                        let _ = written.send(result.clone());
                    }
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
                    if !max_idle_time.is_zero() {
                        idle_timer
                            .as_mut()
                            .reset(Instant::now() + max_idle_time);
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
                                    let request = super::processor::InboundRequest {
                                        generation,
                                        remote_addr: address.clone(),
                                        command,
                                    };
                                    if disable_worker_group {
                                        super::processor::process_one(&Arc::downgrade(&inner), request);
                                    } else {
                                        let _ = inner.inbound.send(request);
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
            .map_err(TransportError::connect_source)?
            .into_inner();
            let socket = socket2::SockRef::from(&stream);
            if send_buffer_size > 0 {
                socket
                    .set_send_buffer_size(send_buffer_size)
                    .map_err(TransportError::connect_source)?;
            }
            if receive_buffer_size > 0 {
                socket
                    .set_recv_buffer_size(receive_buffer_size)
                    .map_err(TransportError::connect_source)?;
            }
            Ok(Box::new(stream) as BoxedStream)
        } else {
            let addresses = tokio::net::lookup_host((host.as_str(), port))
                .await
                .map_err(TransportError::connect_source)?
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
                .map_err(TransportError::connect_source)?;
                let socket_ref = socket2::SockRef::from(&socket);
                if send_buffer_size > 0 {
                    socket_ref
                        .set_send_buffer_size(send_buffer_size)
                        .map_err(TransportError::connect_source)?;
                }
                if receive_buffer_size > 0 {
                    socket_ref
                        .set_recv_buffer_size(receive_buffer_size)
                        .map_err(TransportError::connect_source)?;
                }
                match socket.connect(address).await {
                    Ok(stream) => {
                        connected = Some(stream);
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            let stream = connected.ok_or_else(|| match last_error {
                Some(error) => TransportError::connect_source(error),
                None => TransportError::Connect("connection failed".into()),
            })?;
            stream
                .set_nodelay(true)
                .map_err(TransportError::connect_source)?;
            Ok(Box::new(stream) as BoxedStream)
        }
    })
    .await
    .map_err(|_| TransportError::Timeout)??;
    match mode {
        TransportMode::Plain => Ok(stream),
        TransportMode::Tls(config) => {
            let server_name = ServerName::try_from(host).map_err(TransportError::tls_source)?;
            let connector = config.connector()?;
            time::timeout(
                deadline.saturating_duration_since(Instant::now()),
                connector.connect(server_name, stream),
            )
            .await
            .map_err(|_| TransportError::Timeout)?
            .map(|tls| Box::new(tls) as BoxedStream)
            .map_err(TransportError::tls_source)
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
