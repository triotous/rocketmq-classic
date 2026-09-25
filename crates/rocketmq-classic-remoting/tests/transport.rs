use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::BytesMut;
use rocketmq_classic_protocol::codes::{SerializeType, request, response};
use rocketmq_classic_remoting::{
    RemotingCommand,
    client::{
        Address, CallbackExecutor, ChannelEventListener, ClientConfig, ContextRequestPipeline,
        InvokeCallback, InvokeCallbackResult, RemotingClient, RequestContext, RequestExecutor,
        RequestPipeline, RequestPipelineError, RequestProcessor, RpcHook, RpcHookError,
        SocksProxyConfig, TransportMode, tls::TlsConfig,
    },
    codec::frame::{self, DEFAULT_MAX_FRAME_SIZE},
    error::TransportError,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::Instant,
};

async fn read_command<S: AsyncRead + Unpin>(stream: &mut S) -> RemotingCommand {
    let mut prefix = [0; 4];
    stream.read_exact(&mut prefix).await.unwrap();
    let frame_length = u32::from_be_bytes(prefix) as usize;
    let mut bytes = BytesMut::with_capacity(frame_length + 4);
    bytes.extend_from_slice(&prefix);
    bytes.resize(frame_length + 4, 0);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    frame::decode_next(&mut bytes, DEFAULT_MAX_FRAME_SIZE)
        .unwrap()
        .unwrap()
}

async fn write_command<S: AsyncWriteExt + Unpin>(stream: &mut S, command: &RemotingCommand) {
    stream
        .write_all(&frame::encode(command, DEFAULT_MAX_FRAME_SIZE).unwrap())
        .await
        .unwrap();
}

fn command(code: i32) -> RemotingCommand {
    RemotingCommand {
        code,
        serialization: SerializeType::Json,
        ..RemotingCommand::default()
    }
}

struct ContextCallback {
    sender: tokio::sync::mpsc::UnboundedSender<InvokeCallbackResult>,
    events: Arc<std::sync::Mutex<Vec<&'static str>>>,
}

struct EventCallback(Arc<std::sync::Mutex<Vec<&'static str>>>);

impl InvokeCallback for EventCallback {
    fn operation_complete_with_context(&self, _: &InvokeCallbackResult) {
        self.0.lock().unwrap().push("complete");
    }

    fn operation_fail(&self, _: &TransportError) {
        self.0.lock().unwrap().push("fail");
    }
}

impl InvokeCallback for ContextCallback {
    fn operation_complete_with_context(&self, context: &InvokeCallbackResult) {
        self.events.lock().unwrap().push("complete");
        let _ = self.sender.send(context.clone());
    }

    fn operation_succeed(&self, _: &RemotingCommand) {
        self.events.lock().unwrap().push("succeed");
    }

    fn operation_fail(&self, _: &TransportError) {
        self.events.lock().unwrap().push("fail");
    }
}

#[tokio::test]
async fn p3_async_connect_failure_calls_fail_without_complete() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let client = RemotingClient::new(TransportMode::Plain);
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    client
        .invoke_async(
            address,
            command(1),
            Instant::now() + Duration::from_secs(1),
            Arc::new(EventCallback(events.clone())),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while events.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(*events.lock().unwrap(), vec!["fail"]);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_async_callback_contains_response_future_metadata_and_timeout_grace() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut response = command(19);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let (callback_tx, mut callback_rx) = tokio::sync::mpsc::unbounded_channel();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut request = command(18);
    request.opaque = 73;
    client
        .invoke_async(
            address.to_string(),
            request,
            Instant::now() + Duration::from_millis(50),
            Arc::new(ContextCallback {
                sender: callback_tx,
                events: events.clone(),
            }),
        )
        .unwrap();
    let context = tokio::time::timeout(Duration::from_secs(2), callback_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.response.as_ref().unwrap().code, 19);
    assert_eq!(
        context.request.opaque,
        context.response.as_ref().unwrap().opaque
    );
    assert!(context.send_request_ok);
    assert!(!context.timed_out);
    assert!(context.error.is_none());
    assert!(context.elapsed >= Duration::from_millis(100));
    assert_eq!(*events.lock().unwrap(), vec!["succeed", "complete"]);
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_expired_async_callback_retains_request_and_timeout_metadata() {
    let client = RemotingClient::new(TransportMode::Plain);
    let (callback_tx, mut callback_rx) = tokio::sync::mpsc::unbounded_channel();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut request = command(20);
    request.opaque = 74;
    client
        .invoke_async(
            "127.0.0.1:1",
            request,
            Instant::now(),
            Arc::new(ContextCallback {
                sender: callback_tx,
                events: events.clone(),
            }),
        )
        .unwrap();
    let context = callback_rx.recv().await.unwrap();
    assert_eq!(context.request.code, 20);
    assert_eq!(context.request.opaque, 74);
    assert!(context.response.is_none());
    assert_eq!(context.error, Some(TransportError::Timeout));
    assert!(context.timed_out);
    assert!(!context.send_request_ok);
    assert_eq!(*events.lock().unwrap(), vec!["fail", "complete"]);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_nameserver_update_shuffles_changed_lists_and_preserves_duplicates() {
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    let addresses = vec![
        "127.0.0.1:1001".into(),
        "127.0.0.1:1001".into(),
        "127.0.0.1:1002".into(),
    ];
    client.update_name_server_address_list(addresses);
    let reordered_same_list = vec![
        "127.0.0.1:1002".into(),
        "127.0.0.1:1001".into(),
        "127.0.0.1:1001".into(),
    ];
    client.update_name_server_address_list(reordered_same_list);
    let initial = client.get_name_server_address_list();
    assert_eq!(initial.len(), 3);
    assert_eq!(
        initial
            .iter()
            .filter(|addr| *addr == "127.0.0.1:1001")
            .count(),
        2
    );
    assert_eq!(
        initial
            .iter()
            .filter(|addr| *addr == "127.0.0.1:1002")
            .count(),
        1
    );
    client.update_name_server_address_list(vec![
        "127.0.0.1:1002".into(),
        "127.0.0.1:1001".into(),
        "127.0.0.1:1001".into(),
    ]);
    assert_eq!(client.get_name_server_address_list(), initial);

    client.update_name_server_address_list(vec![
        "127.0.0.1:1002".into(),
        "127.0.0.1:1002".into(),
        "127.0.0.1:1003".into(),
    ]);
    let mut updated = client.get_name_server_address_list();
    updated.sort();
    assert_eq!(
        updated,
        vec![
            "127.0.0.1:1002".to_owned(),
            "127.0.0.1:1002".to_owned(),
            "127.0.0.1:1003".to_owned(),
        ]
    );
    client.shutdown().await;
}

#[tokio::test]
async fn p3_nameserver_list_selection_and_availability_tracking() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (close_peer_tx, close_peer_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(17);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let _ = close_peer_rx.await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        connect_timeout: Duration::from_secs(3600),
        ..ClientConfig::default()
    })
    .unwrap();
    client.update_name_server_address_list(vec![address.to_string(), "127.0.0.1:1".into()]);
    assert_eq!(client.get_name_server_address_list().len(), 2);
    client.update_name_server_address_list(Vec::new());
    assert_eq!(client.get_name_server_address_list().len(), 2);
    assert!(client.get_available_name_srv_list().is_empty());
    let response = client
        .invoke_name_server(command(1), Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(response.code, 17);
    assert_eq!(
        client.get_available_name_srv_list(),
        vec![address.to_string()]
    );
    let _ = close_peer_tx.send(());
    peer.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !client.get_available_name_srv_list().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.update_name_server_address_list(vec!["127.0.0.1:1".into()]);
    assert!(client.get_available_name_srv_list().is_empty());
    client.shutdown().await;
}

#[tokio::test]
async fn p3_nameserver_selection_fails_over_after_connect_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let good = listener.local_addr().unwrap();
    let bad_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bad = bad_listener.local_addr().unwrap();
    drop(bad_listener);
    let (keep_peer_open_tx, keep_peer_open_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(18);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let _ = keep_peer_open_rx.await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.update_name_server_address_list(vec![bad.to_string(), good.to_string()]);
    let response = client
        .invoke_name_server(command(2), Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(response.code, 18);
    assert_eq!(client.get_available_name_srv_list(), vec![good.to_string()]);
    let _ = keep_peer_open_tx.send(());
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_nameserver_recovers_selection_after_chosen_channel_closes() {
    let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_address = first_listener.local_addr().unwrap();
    let (close_first_tx, close_first_rx) = tokio::sync::oneshot::channel();
    let first_peer = tokio::spawn(async move {
        let (mut stream, _) = first_listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(21);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let _ = close_first_rx.await;
    });
    let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_address = second_listener.local_addr().unwrap();
    let (close_second_tx, close_second_rx) = tokio::sync::oneshot::channel();
    let second_peer = tokio::spawn(async move {
        let (mut stream, _) = second_listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(22);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let _ = close_second_rx.await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        scan_available_nameserver: false,
        ..ClientConfig::default()
    })
    .unwrap();
    client.update_name_server_address_list(vec![
        first_address.to_string(),
        second_address.to_string(),
    ]);
    let first_response = client
        .invoke_name_server(command(1), Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let (closed_address, expected_recovery, close_selected, close_backup) =
        match first_response.code {
            21 => (
                first_address.to_string(),
                22,
                Box::new(move || {
                    let _ = close_first_tx.send(());
                }) as Box<dyn FnOnce()>,
                Box::new(move || {
                    let _ = close_second_tx.send(());
                }) as Box<dyn FnOnce()>,
            ),
            22 => (
                second_address.to_string(),
                21,
                Box::new(move || {
                    let _ = close_second_tx.send(());
                }) as Box<dyn FnOnce()>,
                Box::new(move || {
                    let _ = close_first_tx.send(());
                }) as Box<dyn FnOnce()>,
            ),
            code => panic!("unexpected NameServer response {code}"),
        };
    assert_eq!(client.get_available_name_srv_list(), vec![closed_address]);
    close_selected();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !client.get_available_name_srv_list().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        client
            .invoke_name_server(command(2), Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
            .code,
        expected_recovery
    );
    close_backup();
    first_peer.await.unwrap();
    second_peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_channel_management_matches_java_unknown_writable_and_explicit_close_contract() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut byte = [0; 1];
        stream.read(&mut byte).await.unwrap()
    });
    let client = RemotingClient::with_config(ClientConfig {
        client_socket_send_buffer_size: 32 * 1024,
        client_socket_receive_buffer_size: 32 * 1024,
        ..ClientConfig::default()
    })
    .unwrap();
    let address_string = address.to_string();
    assert!(client.is_channel_writable(&address_string));
    assert!(
        client
            .is_address_reachable(&address_string, Instant::now() + Duration::from_secs(2),)
            .await
    );
    assert!(client.is_channel_writable(&address_string));
    client.close_channels(std::slice::from_ref(&address_string));
    assert_eq!(peer.await.unwrap(), 0);
    assert!(client.is_channel_writable(&address_string));
    assert!(
        !client
            .is_address_reachable(&address_string, Instant::now() + Duration::from_millis(20),)
            .await
    );
    client.shutdown().await;
}

#[tokio::test]
async fn p3_close_channels_interrupts_pending_pull_requests_only() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (received_tx, received_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let pull = read_command(&mut stream).await;
        received_tx.send(pull.code).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let pull_client = client.clone();
    let pull_address = address.clone();
    let pull = tokio::spawn(async move {
        pull_client
            .invoke(
                &pull_address,
                command(request::PULL_MESSAGE),
                Instant::now() + Duration::from_secs(2),
            )
            .await
    });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), received_rx)
            .await
            .unwrap()
            .unwrap(),
        request::PULL_MESSAGE
    );
    client.close_channels(std::slice::from_ref(&address));
    assert_eq!(pull.await.unwrap(), Err(TransportError::Interrupted));
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_v01_parallel_responses_and_opaque_lifecycle() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        for _ in 0..2 {
            received.push(read_command(&mut stream).await);
        }
        for request in received.into_iter().rev() {
            let mut response = command(request::GET_ROUTEINFO_BY_TOPIC);
            response.opaque = request.opaque;
            response.mark_response();
            write_command(&mut stream, &response).await;
        }
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let deadline = Instant::now() + Duration::from_secs(2);
    let address = addr.to_string();
    let (first, second) = tokio::join!(
        client.invoke(&address, command(request::GET_ROUTEINFO_BY_TOPIC), deadline),
        client.invoke(&address, command(request::GET_ROUTEINFO_BY_TOPIC), deadline),
    );
    assert!(first.unwrap().is_response());
    assert!(second.unwrap().is_response());
    peer.await.unwrap();
    client.shutdown().await;
    client.shutdown().await;
}

#[tokio::test]
async fn p3_v02_single_flight_shares_connection_and_stale_close_keeps_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_peer = accepted.clone();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        accepted_peer.fetch_add(1, Ordering::SeqCst);
        for _ in 0..3 {
            let request = read_command(&mut stream).await;
            assert!(request.is_oneway());
        }
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let deadline = Instant::now() + Duration::from_secs(2);
    let address = addr.to_string();
    let (a, b, c) = tokio::join!(
        client.invoke_oneway(&address, command(1), deadline),
        client.invoke_oneway(&address, command(2), deadline),
        client.invoke_oneway(&address, command(3), deadline),
    );
    a.unwrap();
    b.unwrap();
    c.unwrap();
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_v03_inbound_dispatch_and_reordered_parallel_responses() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let outbound = read_command(&mut stream).await;
        let mut inbound = command(777_777);
        inbound.opaque = 41;
        write_command(&mut stream, &inbound).await;
        let processor_response = read_command(&mut stream).await;
        let mut response = command(55);
        response.opaque = outbound.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let mut unsupported = command(888_888);
        unsupported.opaque = 42;
        write_command(&mut stream, &unsupported).await;
        let unsupported_response = read_command(&mut stream).await;
        let mut oneway = command(999_999);
        oneway.opaque = 43;
        oneway.mark_oneway();
        write_command(&mut stream, &oneway).await;
        let mut unexpected = [0; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), stream.read(&mut unexpected))
                .await
                .is_err()
        );
        (processor_response, unsupported_response)
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.register_processor(
        777_777,
        Arc::new(|_request: RemotingCommand| Ok(Some(command(99)))),
    );
    let response = client
        .invoke(
            &addr.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(response.is_response());
    let (processor_response, unsupported_response) = peer.await.unwrap();
    assert_eq!(processor_response.code, 99);
    assert_eq!(processor_response.opaque, 41);
    assert!(processor_response.is_response());
    assert_eq!(
        unsupported_response.code,
        response::REQUEST_CODE_NOT_SUPPORTED
    );
    assert_eq!(unsupported_response.opaque, 42);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_inbound_oneway_runs_processor_but_never_writes_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(900_003);
        request.mark_oneway();
        write_command(&mut stream, &request).await;
        let mut unsupported_oneway = command(900_004);
        unsupported_oneway.mark_oneway();
        write_command(&mut stream, &unsupported_oneway).await;
        let mut unexpected = [0; 1];
        let result =
            tokio::time::timeout(Duration::from_millis(100), stream.read(&mut unexpected)).await;
        result.is_err()
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.register_processor(
        900_003,
        Arc::new(move |request: RemotingCommand| {
            let _ = processed_tx.send(request.is_oneway());
            Ok(Some(command(903)))
        }),
    );
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    assert!(processed_rx.recv().await.unwrap());
    assert!(peer.await.unwrap());
    client.shutdown().await;
}

#[tokio::test]
async fn p3_default_processor_handles_unknown_codes_but_exact_registration_wins() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut unknown = command(900_001);
        unknown.opaque = 41;
        write_command(&mut stream, &unknown).await;
        let mut exact = command(900_002);
        exact.opaque = 42;
        write_command(&mut stream, &exact).await;
        let first = read_command(&mut stream).await;
        let second = read_command(&mut stream).await;
        (first, second)
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.register_default_processor(Some(Arc::new(|_: RemotingCommand| Ok(Some(command(901))))));
    client.register_processor(
        900_002,
        Arc::new(|_: RemotingCommand| Ok(Some(command(902)))),
    );
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let (first, second) = peer.await.unwrap();
    let responses = [first, second]
        .into_iter()
        .map(|response| (response.opaque, response.code))
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(responses.get(&41), Some(&901));
    assert_eq!(responses.get(&42), Some(&902));
    client.register_default_processor(None);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_u01_raw_route_deadline_shutdown_and_address_parsing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut reply = command(request::GET_ROUTEINFO_BY_TOPIC);
        reply.opaque = request.opaque;
        reply.mark_response();
        write_command(&mut stream, &reply).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let response = client
        .invoke(
            &addr.to_string(),
            command(request::GET_ROUTEINFO_BY_TOPIC),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert!(response.is_response());
    peer.await.unwrap();
    let expired = client
        .invoke(&addr.to_string(), command(1), Instant::now())
        .await;
    assert!(matches!(expired, Err(TransportError::Timeout)));
    assert!(Address::parse("[::1]:10911").is_ok());
    assert!(Address::parse("localhost:10911").is_ok());
    assert!(Address::parse("::1:10911").is_err());
    assert!(Address::parse("localhost:70000").is_err());
    client.shutdown().await;
    assert!(matches!(
        client
            .invoke(
                &addr.to_string(),
                command(1),
                Instant::now() + Duration::from_secs(1)
            )
            .await,
        Err(TransportError::Shutdown)
    ));
}

#[tokio::test]
async fn p3_u01_inflight_invoke_times_out_when_peer_does_not_reply() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _request = read_command(&mut stream).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let result = client
        .invoke(
            &address.to_string(),
            command(request::GET_ROUTEINFO_BY_TOPIC),
            Instant::now() + Duration::from_millis(20),
        )
        .await;
    assert!(matches!(result, Err(TransportError::Timeout)));
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_u01_tls_verifies_ca_and_hostname_without_insecure_fallback() {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_pem = certificate.cert.pem();
    let ca = TlsConfig {
        ca_pem: Some(cert_pem.as_bytes().to_vec()),
        client_cert_pem: None,
        client_key_pem: None,
    };
    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            tokio_rustls::rustls::pki_types::PrivateKeyDer::Pkcs8(
                tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer::from(
                    certificate.key_pair.serialize_der(),
                ),
            ),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    let peer = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let Ok(mut stream) = acceptor.accept(socket).await else {
                continue;
            };
            if let Ok(request) =
                tokio::time::timeout(Duration::from_secs(1), read_command(&mut stream)).await
            {
                let mut reply = command(request::GET_ROUTEINFO_BY_TOPIC);
                reply.opaque = request.opaque;
                reply.mark_response();
                let _ = write_command(&mut stream, &reply).await;
            }
        }
    });
    let client = RemotingClient::new(TransportMode::Tls(ca.clone()));
    let result = client
        .invoke(
            &format!("localhost:{port}"),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await;
    assert!(result.unwrap().is_response());
    client.shutdown().await;

    let bad_name = RemotingClient::new(TransportMode::Tls(ca));
    let result = bad_name
        .invoke(
            &format!("127.0.0.1:{port}"),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await;
    assert!(matches!(result, Err(TransportError::Tls(_))));
    bad_name.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_controls_validate_defaults_hooks_callback_and_channel_events() {
    let defaults = ClientConfig::default();
    assert_eq!(defaults.connect_timeout, Duration::from_millis(3000));
    assert_eq!(defaults.async_semaphore_value, 65535);
    assert_eq!(defaults.oneway_semaphore_value, 65535);
    assert_eq!(defaults.worker_threads, 4);
    assert_eq!(
        defaults.callback_executor_threads,
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
    );
    assert_eq!(
        defaults.channel_not_active_interval,
        Duration::from_secs(60)
    );
    assert!(
        ClientConfig {
            channel_not_active_interval: Duration::ZERO,
            ..defaults.clone()
        }
        .validate()
        .is_ok()
    );
    assert!(
        ClientConfig {
            max_idle_time: Duration::ZERO,
            ..defaults.clone()
        }
        .validate()
        .is_ok()
    );
    assert_eq!(defaults.client_socket_send_buffer_size, 0);
    assert_eq!(defaults.client_socket_receive_buffer_size, 0);
    assert_eq!(defaults.write_buffer_high_water_mark, 0);
    assert_eq!(defaults.write_buffer_low_water_mark, 0);
    assert!(!defaults.pooled_byte_buffer_allocator_enabled);
    assert_eq!(defaults.max_idle_time, Duration::from_secs(120));
    assert!(defaults.close_socket_if_timeout);
    assert!(defaults.scan_available_nameserver);
    assert!(defaults.socks_proxy.is_none());
    assert!(
        ClientConfig {
            socks_proxy: Some(SocksProxyConfig::new("localhost:1080")),
            ..defaults.clone()
        }
        .validate()
        .is_ok()
    );
    assert!(
        ClientConfig {
            async_semaphore_value: 0,
            ..defaults.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        ClientConfig {
            callback_executor_threads: 0,
            ..defaults.clone()
        }
        .validate()
        .is_ok()
    );
    assert!(
        ClientConfig {
            write_buffer_low_water_mark: 10,
            write_buffer_high_water_mark: 5,
            ..defaults.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        ClientConfig {
            pooled_byte_buffer_allocator_enabled: true,
            ..defaults.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        ClientConfig {
            write_buffer_low_water_mark: 0,
            write_buffer_high_water_mark: 128,
            ..defaults.clone()
        }
        .validate()
        .is_ok()
    );
    assert!(
        ClientConfig {
            write_buffer_high_water_mark: 128,
            ..defaults.clone()
        }
        .validate()
        .is_ok()
    );
    assert!(
        ClientConfig {
            client_socket_send_buffer_size: usize::MAX,
            ..defaults.clone()
        }
        .validate()
        .is_err()
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(12);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let observed = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let hook_observed = observed.clone();
    client.add_rpc_hook(Arc::new(move |_: &str, _: &RemotingCommand| {
        hook_observed.lock().unwrap().push("before".into());
    }));
    let callback_observed = observed.clone();
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
    let callback_tx = Arc::new(std::sync::Mutex::new(Some(callback_tx)));
    let callback = Arc::new(move |result: Result<RemotingCommand, TransportError>| {
        callback_observed.lock().unwrap().push("callback".into());
        if let Some(sender) = callback_tx.lock().unwrap().take() {
            let _ = sender.send(result);
        }
    });
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
            callback,
        )
        .unwrap();
    assert!(callback_rx.await.unwrap().unwrap().is_response());
    peer.await.unwrap();
    client.shutdown().await;
    assert!(
        observed
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "before")
    );
    assert!(
        observed
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "callback")
    );
}

struct RejectingCallbackExecutor;

impl CallbackExecutor for RejectingCallbackExecutor {
    fn execute(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), Box<dyn FnOnce() + Send + 'static>> {
        Err(task)
    }
}

struct ThreadCallbackExecutor;

impl CallbackExecutor for ThreadCallbackExecutor {
    fn execute(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), Box<dyn FnOnce() + Send + 'static>> {
        std::thread::spawn(task);
        Ok(())
    }
}

struct TestHook(Arc<std::sync::Mutex<Vec<&'static str>>>);
impl RpcHook for TestHook {
    fn before_request(&self, _: &str, _: &RemotingCommand) {
        self.0.lock().unwrap().push("before");
    }
    fn after_response(&self, _: &str, _: &RemotingCommand, _: &RemotingCommand) {
        self.0.lock().unwrap().push("after");
    }
}

#[tokio::test]
async fn p3_rpc_hooks_deduplicate_and_clear() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for _ in 0..2 {
            let request = read_command(&mut stream).await;
            let mut response = command(12);
            response.opaque = request.opaque;
            response.mark_response();
            write_command(&mut stream, &response).await;
        }
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let hook: Arc<dyn RpcHook> = Arc::new(TestHook(events.clone()));
    client.add_rpc_hook(hook.clone());
    client.add_rpc_hook(hook);
    client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(*events.lock().unwrap(), vec!["before", "after"]);
    client.clear_rpc_hooks();
    client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(*events.lock().unwrap(), vec!["before", "after"]);
    peer.await.unwrap();
    client.shutdown().await;
}

struct BeforeCounter(Arc<AtomicUsize>);
impl RpcHook for BeforeCounter {
    fn before_request(&self, _: &str, _: &RemotingCommand) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct TestEvents(Arc<std::sync::Mutex<Vec<&'static str>>>);
impl ChannelEventListener for TestEvents {
    fn on_connect(&self, _: &str) {
        self.0.lock().unwrap().push("connect");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p3_callback_executor_runs_callbacks_on_bounded_workers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for _ in 0..2 {
            let request = read_command(&mut stream).await;
            let mut response = command(12);
            response.opaque = request.opaque;
            response.mark_response();
            write_command(&mut stream, &response).await;
        }
    });
    let client = RemotingClient::with_config(ClientConfig {
        callback_executor_threads: 2,
        ..ClientConfig::default()
    })
    .unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let active = active.clone();
        let max_active = max_active.clone();
        let completed = completed.clone();
        client
            .invoke_async(
                address.to_string(),
                command(1),
                Instant::now() + Duration::from_secs(2),
                Arc::new(move |_: Result<RemotingCommand, TransportError>| {
                    let running = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(running, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(30));
                    active.fetch_sub(1, Ordering::SeqCst);
                    completed.fetch_add(1, Ordering::SeqCst);
                }),
            )
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while completed.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(max_active.load(Ordering::SeqCst), 2);
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_caller_callback_executor_routes_callbacks_to_owned_executor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(12);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let callback_executor: Arc<dyn CallbackExecutor> = Arc::new(ThreadCallbackExecutor);
    client.set_callback_executor(Some(callback_executor.clone()));
    assert!(Arc::ptr_eq(
        &client.get_callback_executor().unwrap(),
        &callback_executor
    ));
    client.set_callback_executor(None);
    assert!(client.get_callback_executor().is_none());
    client.set_callback_executor(Some(callback_executor));
    let caller_thread = std::thread::current().id();
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
    let callback_tx = Arc::new(std::sync::Mutex::new(Some(callback_tx)));
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                let sender = callback_tx.lock().unwrap().take().unwrap();
                let _ = sender.send((result, std::thread::current().id()));
            }),
        )
        .unwrap();
    let (result, callback_thread) = callback_rx.await.unwrap();
    assert!(result.unwrap().is_response());
    assert_ne!(callback_thread, caller_thread);
    peer.await.unwrap();
    client.shutdown().await;
}

#[tokio::test]
async fn p3_callback_executor_rejection_runs_callback_inline() {
    let client = RemotingClient::new(TransportMode::Plain);
    client.set_callback_executor(Some(Arc::new(RejectingCallbackExecutor)));
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
    let callback_tx = Arc::new(std::sync::Mutex::new(Some(callback_tx)));
    client
        .invoke_async(
            "127.0.0.1:1",
            command(1),
            Instant::now() - Duration::from_millis(1),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                let _ = callback_tx.lock().unwrap().take().unwrap().send(result);
            }),
        )
        .unwrap();
    assert!(matches!(
        callback_rx.await.unwrap(),
        Err(TransportError::Timeout)
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn p3_async_admission_timeout_completes_through_callback() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = accepted_tx.send(());
        let _request = read_command(&mut stream).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        async_semaphore_value: 1,
        ..ClientConfig::default()
    })
    .unwrap();
    let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
    let callback_tx = Arc::new(std::sync::Mutex::new(Some(callback_tx)));
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                if let Some(sender) = callback_tx.lock().unwrap().take() {
                    let _ = sender.send(result);
                }
            }),
        )
        .unwrap();
    accepted_rx.await.unwrap();
    let (second_tx, second_rx) = tokio::sync::oneshot::channel();
    let second_tx = Arc::new(std::sync::Mutex::new(Some(second_tx)));
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_millis(20),
            Arc::new(move |result: Result<RemotingCommand, TransportError>| {
                if let Some(sender) = second_tx.lock().unwrap().take() {
                    let _ = sender.send(result);
                }
            }),
        )
        .unwrap();
    assert_eq!(
        second_rx.await.unwrap().unwrap_err(),
        TransportError::Timeout
    );
    assert!(callback_rx.await.unwrap().is_err());
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_async_callback_completes_on_deadline_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1200)).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    client
        .invoke_async(
            address.to_string(),
            command(1),
            Instant::now() + Duration::from_millis(20),
            Arc::new(ContextCallback {
                sender: tx,
                events: events.clone(),
            }),
        )
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .error
            .unwrap(),
        TransportError::Timeout
    );
    assert_eq!(*events.lock().unwrap(), vec!["fail", "complete"]);
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_go_away_retries_once_on_a_new_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let request = read_command(&mut first).await;
        let mut go_away = command(response::GO_AWAY);
        go_away.opaque = request.opaque;
        go_away.mark_response();
        write_command(&mut first, &go_away).await;
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        let retry = read_command(&mut second).await;
        let mut response = command(42);
        response.opaque = retry.opaque;
        response.mark_response();
        write_command(&mut second, &response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let hook_calls = Arc::new(AtomicUsize::new(0));
    client.add_rpc_hook(Arc::new(BeforeCounter(hook_calls.clone())));
    let result = client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(result.code, 42);
    assert_eq!(hook_calls.load(Ordering::SeqCst), 1);
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_connect_failure_does_not_run_before_rpc_hooks() {
    let client = RemotingClient::new(TransportMode::Plain);
    let hook_calls = Arc::new(AtomicUsize::new(0));
    client.add_rpc_hook(Arc::new(BeforeCounter(hook_calls.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let result = client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(1),
        )
        .await;
    assert!(result.is_err());
    assert_eq!(hook_calls.load(Ordering::SeqCst), 0);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_go_away_response_uses_explicit_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(response::GO_AWAY);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::with_config(ClientConfig {
        enable_reconnect_for_go_away: false,
        ..ClientConfig::default()
    })
    .unwrap();
    let error = client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert_eq!(error, TransportError::GoAway);
    client.shutdown().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn p3_hooks_and_channel_connect_event_follow_successful_exchange() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(12);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    client.add_rpc_hook(Arc::new(TestHook(events.clone())));
    let channel_listener: Arc<dyn ChannelEventListener> = Arc::new(TestEvents(events.clone()));
    client.set_channel_event_listener(Some(channel_listener.clone()));
    assert!(Arc::ptr_eq(
        &client.get_channel_event_listener().unwrap(),
        &channel_listener
    ));
    client.set_channel_event_listener(None);
    assert!(client.get_channel_event_listener().is_none());
    client.set_channel_event_listener(Some(channel_listener));
    client
        .invoke(
            &address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    peer.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if events.lock().unwrap().contains(&"connect") {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.shutdown().await;
    let events = events.lock().unwrap();
    assert_eq!(
        events.iter().filter(|event| **event == "connect").count(),
        1
    );
    assert!(
        events.iter().position(|event| *event == "before")
            < events.iter().position(|event| *event == "after")
    );
}

struct AbortPipeline;

impl RequestPipeline for AbortPipeline {
    fn execute(&self, _: &RemotingCommand) -> Result<(), i32> {
        Ok(())
    }

    fn execute_detailed(
        &self,
        _: &RequestContext,
        _: &RemotingCommand,
    ) -> Result<(), RequestPipelineError> {
        Err(RequestPipelineError::Abort {
            response_code: 812,
            remark: Some("pipeline stopped request".into()),
        })
    }
}

struct FallibleHook;

impl RpcHook for FallibleHook {
    fn before_request_result(
        &self,
        _: &str,
        request: &RemotingCommand,
    ) -> Result<(), RpcHookError> {
        if request.code == 830 {
            Err(RpcHookError::Abort {
                response_code: 831,
                remark: Some("hook stopped request".into()),
            })
        } else {
            Ok(())
        }
    }

    fn after_response_result(
        &self,
        _: &str,
        request: &RemotingCommand,
        _: &RemotingCommand,
    ) -> Result<(), RpcHookError> {
        if request.code == 832 {
            Err(RpcHookError::Failed("after hook failed".into()))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn p3_rpc_hook_abort_and_failures_map_to_java_response_paths() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for (code, opaque) in [(830, 71), (832, 72)] {
            let mut request = command(code);
            request.opaque = opaque;
            write_command(&mut stream, &request).await;
        }
        let first = read_command(&mut stream).await;
        let second = read_command(&mut stream).await;
        [first, second]
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let processor_calls = Arc::new(AtomicUsize::new(0));
    for code in [830, 832] {
        let processor_calls = processor_calls.clone();
        client.register_processor(
            code,
            Arc::new(move |_: RemotingCommand| {
                processor_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Some(command(833)))
            }),
        );
    }
    client.add_rpc_hook(Arc::new(FallibleHook));
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let responses = peer.await.unwrap();
    let responses = responses
        .into_iter()
        .map(|response| (response.opaque, response))
        .collect::<std::collections::HashMap<_, _>>();
    let aborted = responses.get(&71).unwrap();
    assert_eq!(aborted.code, 831);
    assert_eq!(aborted.remark.as_deref(), Some("hook stopped request"));
    assert_eq!(responses.get(&72).unwrap().code, response::SYSTEM_ERROR);
    assert_eq!(processor_calls.load(Ordering::SeqCst), 1);
    client.shutdown().await;
}

struct BeforeHookFailure {
    after_hook_code: Arc<AtomicUsize>,
}

impl RpcHook for BeforeHookFailure {
    fn before_request_result(&self, _: &str, _: &RemotingCommand) -> Result<(), RpcHookError> {
        Err(RpcHookError::Failed("before hook failed".into()))
    }

    fn after_response_result(
        &self,
        _: &str,
        _: &RemotingCommand,
        response: &RemotingCommand,
    ) -> Result<(), RpcHookError> {
        self.after_hook_code
            .store(response.code as usize, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn p3_before_hook_failure_runs_pipeline_and_after_hook_but_skips_processor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(836);
        request.opaque = 73;
        write_command(&mut stream, &request).await;
        read_command(&mut stream).await
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let pipeline_calls = Arc::new(AtomicUsize::new(0));
    let after_hook_code = Arc::new(AtomicUsize::new(0));
    let processor_calls = Arc::new(AtomicUsize::new(0));
    let pipeline_counter = pipeline_calls.clone();
    client.set_request_pipeline(Some(Arc::new(move |_: &RemotingCommand| {
        pipeline_counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })));
    client.register_processor(836, {
        let processor_calls = processor_calls.clone();
        Arc::new(move |_: RemotingCommand| {
            processor_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(command(837)))
        })
    });
    client.add_rpc_hook(Arc::new(BeforeHookFailure {
        after_hook_code: after_hook_code.clone(),
    }));
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let response = peer.await.unwrap();
    assert_eq!(response.code, response::SYSTEM_ERROR);
    assert_eq!(pipeline_calls.load(Ordering::SeqCst), 1);
    assert_eq!(processor_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        after_hook_code.load(Ordering::SeqCst),
        response::SYSTEM_ERROR as usize
    );
    client.shutdown().await;
}

struct OptionalResponseHook(Arc<AtomicUsize>);

impl RpcHook for OptionalResponseHook {
    fn after_response_optional_result(
        &self,
        _: &str,
        _: &RemotingCommand,
        response: Option<&RemotingCommand>,
    ) -> Result<(), RpcHookError> {
        if response.is_none() {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[tokio::test]
async fn p3_after_hook_runs_when_processor_returns_no_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let outgoing = read_command(&mut stream).await;
        let mut response = command(835);
        response.opaque = outgoing.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        let mut request = command(834);
        request.opaque = 73;
        write_command(&mut stream, &request).await;
        let mut byte = [0; 1];
        tokio::time::timeout(Duration::from_millis(100), stream.read(&mut byte))
            .await
            .is_err()
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let after_hook_calls = Arc::new(AtomicUsize::new(0));
    client.register_processor(834, Arc::new(|_: RemotingCommand| Ok(None)));
    client.add_rpc_hook(Arc::new(OptionalResponseHook(after_hook_calls.clone())));
    client
        .invoke(
            &address.to_string(),
            command(836),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(peer.await.unwrap());
    assert_eq!(after_hook_calls.load(Ordering::SeqCst), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_request_pipeline_pipe_runs_new_stage_before_existing_stage() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(810);
        request.opaque = 52;
        write_command(&mut stream, &request).await;
        read_command(&mut stream).await
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let stages = Arc::new(std::sync::Mutex::new(Vec::new()));
    let old_stage = stages.clone();
    client.set_request_pipeline(Some(Arc::new(move |_: &RemotingCommand| {
        old_stage.lock().unwrap().push("existing");
        Ok(())
    })));
    let new_stage = stages.clone();
    client.pipe_request_pipeline(Arc::new(move |_: &RemotingCommand| {
        new_stage.lock().unwrap().push("new");
        Ok(())
    }));
    client.register_processor(810, Arc::new(|_: RemotingCommand| Ok(Some(command(812)))));
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let response = peer.await.unwrap();
    assert_eq!(response.code, 812);
    assert_eq!(*stages.lock().unwrap(), vec!["new", "existing"]);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_pipeline_abort_preserves_java_style_code_and_message() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(811);
        request.opaque = 53;
        write_command(&mut stream, &request).await;
        read_command(&mut stream).await
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.register_processor(811, Arc::new(|_: RemotingCommand| Ok(Some(command(813)))));
    client.set_request_pipeline(Some(Arc::new(AbortPipeline)));
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let response = peer.await.unwrap();
    assert_eq!(response.code, 812);
    assert_eq!(response.remark.as_deref(), Some("pipeline stopped request"));
    assert_eq!(response.opaque, 53);
    client.shutdown().await;
}

struct ThreadRequestExecutor {
    executions: Arc<AtomicUsize>,
}

impl RequestExecutor for ThreadRequestExecutor {
    fn execute(&self, task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), String> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        std::thread::spawn(task);
        Ok(())
    }
}

struct RejectRequestExecutor;

impl RequestExecutor for RejectRequestExecutor {
    fn execute(&self, _task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), String> {
        Err("executor saturated".into())
    }
}

#[tokio::test]
async fn p3_processor_specific_and_default_executors_route_or_reject_tasks() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut first = command(820);
        first.opaque = 61;
        write_command(&mut stream, &first).await;
        let mut second = command(821);
        second.opaque = 62;
        write_command(&mut stream, &second).await;
        let one = read_command(&mut stream).await;
        let two = read_command(&mut stream).await;
        [one, two]
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let (processor_tx, processor_rx) = tokio::sync::oneshot::channel();
    let processor_tx = Arc::new(std::sync::Mutex::new(Some(processor_tx)));
    let executions = Arc::new(AtomicUsize::new(0));
    client.register_processor_with_executor(
        820,
        Arc::new(move |_: RemotingCommand| {
            if let Some(sender) = processor_tx.lock().unwrap().take() {
                let _ = sender.send(std::thread::current().id());
            }
            Ok(Some(command(822)))
        }),
        Some(Arc::new(ThreadRequestExecutor {
            executions: executions.clone(),
        })),
    );
    client.register_default_processor_with_executor(
        Some(Arc::new(|_: RemotingCommand| Ok(Some(command(823))))),
        Some(Arc::new(RejectRequestExecutor)),
    );
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let processor_thread = tokio::time::timeout(Duration::from_secs(2), processor_rx)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(processor_thread, std::thread::current().id());
    let responses = peer.await.unwrap();
    let responses = responses
        .into_iter()
        .map(|response| (response.opaque, response.code))
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(responses.get(&61), Some(&822));
    assert_eq!(responses.get(&62), Some(&response::SYSTEM_BUSY));
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_context_aware_pipeline_receives_connection_metadata() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = command(810);
        request.opaque = 51;
        write_command(&mut stream, &request).await;
        let response = read_command(&mut stream).await;
        (request.opaque, response)
    });
    let client = RemotingClient::new(TransportMode::Plain);
    let (context_tx, context_rx) = tokio::sync::oneshot::channel();
    let context_tx = Arc::new(std::sync::Mutex::new(Some(context_tx)));
    client.register_processor(810, Arc::new(|_: RemotingCommand| Ok(Some(command(811)))));
    client.set_request_pipeline(Some(Arc::new(ContextRequestPipeline(
        move |context: &RequestContext, _request: &RemotingCommand| {
            if let Some(sender) = context_tx.lock().unwrap().take() {
                let _ = sender.send(context.clone());
            }
            Ok(())
        },
    ))));
    assert!(
        client
            .is_address_reachable(
                &address.to_string(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
    );
    let (request_opaque, response) = peer.await.unwrap();
    let context = context_rx.await.unwrap();
    assert_eq!(context.remote_addr, address.to_string());
    assert_ne!(context.connection_generation, 0);
    assert!(response.is_response());
    assert_eq!(response.opaque, request_opaque);
    client.shutdown().await;
}

#[tokio::test]
async fn p3_request_pipeline_and_processor_rejection_return_configured_codes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let outbound = read_command(&mut stream).await;
        let mut pipeline_request = command(78);
        pipeline_request.opaque = 41;
        write_command(&mut stream, &pipeline_request).await;
        let pipeline_reply = read_command(&mut stream).await;
        let mut rejected_request = command(77);
        rejected_request.opaque = 42;
        write_command(&mut stream, &rejected_request).await;
        let rejected_reply = read_command(&mut stream).await;
        let mut response = command(9);
        response.opaque = outbound.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
        (pipeline_reply, rejected_reply)
    });
    let client = RemotingClient::new(TransportMode::Plain);
    client.register_processor(77, Arc::new(RejectingProcessor));
    client.register_processor(78, Arc::new(|_: RemotingCommand| Ok(None)));
    client.set_request_pipeline(Some(Arc::new(|_: &RemotingCommand| Err(733))));
    let deadline = Instant::now() + Duration::from_secs(2);
    assert_eq!(
        client
            .invoke(&address.to_string(), command(1), deadline)
            .await
            .unwrap()
            .code,
        9
    );
    client.set_request_pipeline(None);
    client.shutdown().await;
    let (pipeline_reply, rejected_reply) = peer.await.unwrap();
    assert_eq!(pipeline_reply.code, 733);
    assert!(pipeline_reply.is_response());
    assert_eq!(rejected_reply.code, 2);
    assert!(rejected_reply.is_response());
}

struct RejectingProcessor;
impl RequestProcessor for RejectingProcessor {
    fn process(&self, _: RemotingCommand) -> Result<Option<RemotingCommand>, String> {
        Ok(None)
    }

    fn reject_request(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn p3_u01_socks5_proxy_tunnels_a_remoting_request() {
    let broker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_address = broker.local_addr().unwrap();
    let broker_peer = tokio::spawn(async move {
        let (mut stream, _) = broker.accept().await.unwrap();
        let request = read_command(&mut stream).await;
        let mut response = command(42);
        response.opaque = request.opaque;
        response.mark_response();
        write_command(&mut stream, &response).await;
    });
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy.local_addr().unwrap();
    let proxy_peer = tokio::spawn(async move {
        let (mut client, _) = proxy.accept().await.unwrap();
        let mut greeting = [0; 4];
        client.read_exact(&mut greeting).await.unwrap();
        assert_eq!(&greeting, &[5, 2, 0, 2]);
        client.write_all(&[5, 2]).await.unwrap();
        let mut auth_header = [0; 2];
        client.read_exact(&mut auth_header).await.unwrap();
        assert_eq!(&auth_header, &[1, 6]);
        let mut username = [0; 6];
        client.read_exact(&mut username).await.unwrap();
        assert_eq!(&username, b"rocket");
        let mut password_length = [0; 1];
        client.read_exact(&mut password_length).await.unwrap();
        assert_eq!(password_length[0], 6);
        let mut password = [0; 6];
        client.read_exact(&mut password).await.unwrap();
        assert_eq!(&password, b"secret");
        client.write_all(&[1, 0]).await.unwrap();
        let mut request = [0; 10];
        client.read_exact(&mut request[..4]).await.unwrap();
        assert_eq!(&request[..4], &[5, 1, 0, 1]);
        client.read_exact(&mut request[4..]).await.unwrap();
        let target = std::net::SocketAddr::new(
            std::net::Ipv4Addr::new(request[4], request[5], request[6], request[7]).into(),
            u16::from_be_bytes([request[8], request[9]]),
        );
        let upstream = tokio::net::TcpStream::connect(target).await.unwrap();
        client
            .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 1])
            .await
            .unwrap();
        let (mut client_read, mut client_write) = client.into_split();
        let (mut upstream_read, mut upstream_write) = upstream.into_split();
        let a = tokio::io::copy(&mut client_read, &mut upstream_write);
        let b = tokio::io::copy(&mut upstream_read, &mut client_write);
        let _ = tokio::join!(a, b);
    });
    let config = ClientConfig {
        socks_proxy: Some(SocksProxyConfig::with_credentials(
            proxy_address.to_string(),
            "rocket",
            "secret",
        )),
        ..ClientConfig::default()
    };
    let client = RemotingClient::with_config(config).unwrap();
    let response = client
        .invoke(
            &broker_address.to_string(),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(response.code, 42);
    client.shutdown().await;
    broker_peer.await.unwrap();
    proxy_peer.await.unwrap();
}

#[tokio::test]
async fn p3_u01_tls_rejects_untrusted_ca() {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            tokio_rustls::rustls::pki_types::PrivateKeyDer::Pkcs8(
                tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer::from(
                    certificate.key_pair.serialize_der(),
                ),
            ),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), acceptor.accept(socket)).await;
    });
    let client = RemotingClient::new(TransportMode::Tls(TlsConfig {
        ca_pem: Some(Vec::new()),
        client_cert_pem: None,
        client_key_pem: None,
    }));
    let result = client
        .invoke(
            &format!("localhost:{port}"),
            command(1),
            Instant::now() + Duration::from_secs(2),
        )
        .await;
    assert!(matches!(result, Err(TransportError::Tls(_))));
    client.shutdown().await;
    peer.await.unwrap();
}
