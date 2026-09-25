# Phase 3 Remoting API and Configuration Inventory

Java source baseline: `rocketmq-all-5.5.1`, commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. Rust paths are relative to the repository root. Tests refer to `crates/rocketmq-classic-remoting/tests/transport.rs` unless noted otherwise. This ledger is for the client-side remoting scope only; Netty implementation details that have no Tokio semantic equivalent are recorded as differences, not parity.

Status meanings: `MAPPED` means a Rust API or internal behavior and named local test exists; `PARTIAL` means behavior differs or not all Java branches are covered; `NOT MAPPED` means no Rust equivalent. No row claims live Broker validation.

## Public client/service methods

| Java declaration/source | Rust mapping | Coverage / status |
|---|---|---|
| `RemotingService.start()` | Construction starts Tokio-owned tasks in `RemotingClient::with_config`; there is no separate public start call | `PARTIAL`: ownership is different; constructor requires runtime. |
| `RemotingService.shutdown()` | `RemotingClient::shutdown()` | `MAPPED`: client unit `shutdown_is_idempotent_and_new_calls_are_rejected` and shutdown portions of `p3_u01_raw_route_deadline_shutdown_and_address_parsing`, TLS test. |
| `RemotingService.registerRPCHook(RPCHook)` / `clearRPCHook()` | `add_rpc_hook`; no clear-all API | `PARTIAL`: insertion only; no clear operation. Hook tests: `p3_rpc_hook_abort_and_failures_map_to_java_response_paths`, `p3_before_hook_failure_runs_pipeline_and_after_hook_but_skips_processor`, `p3_after_hook_runs_when_processor_returns_no_response`, `p3_connect_failure_does_not_run_before_rpc_hooks`. |
| `RemotingService.setRequestPipeline(RequestPipeline)` | `set_request_pipeline(Option<Arc<dyn RequestPipeline>>)` | `PARTIAL`: replacement API versus Java's `RequestPipeline.pipe` composition; context-aware path and typed errors tested by `p3_context_aware_pipeline_receives_connection_metadata`, `p3_pipeline_abort_preserves_java_style_code_and_message`; Java `ChannelHandlerContext` is not available. |
| `RemotingClient.updateNameServerAddressList(List<String>)` | `update_name_server_address_list(Vec<String>)` | `PARTIAL`: non-empty replacement and chosen/connection cleanup exist; Java mutates/shuffles changed input and preserves duplicate entries; Rust currently deduplicates and preserves order. Tests: `p3_nameserver_list_selection_and_availability_tracking`, `p3_nameserver_recovers_selection_after_chosen_channel_closes`. |
| `getNameServerAddressList()` | `get_name_server_address_list()` | `MAPPED`; same NameServer tests. |
| `getAvailableNameSrvList()` | `get_available_name_srv_list()` | `PARTIAL`: Rust returns configured-order snapshot; Java returns keys from a concurrent map with unspecified order. Same NameServer tests. |
| `invokeSync(String, RemotingCommand, long)` | `invoke(addr, command, deadline)` and `invoke_sync(addr, command, Duration)` | `PARTIAL`: async Rust future rather than Java blocking API/checked exception types. Covered by `p3_v01_parallel_responses_and_opaque_lifecycle`, `p3_u01_raw_route_deadline_shutdown_and_address_parsing`, `p3_go_away_response_uses_explicit_transport_error`. |
| `invokeAsync(String, RemotingCommand, long, InvokeCallback)` | `invoke_async(addr, command, deadline, callback)` | `PARTIAL`: completion metadata/order and admission timeout exist; Java callback throwable taxonomy, `ResponseFuture` contract and races are not identical. Covered by callback metadata/grace, expiry, admission and custom-executor tests listed below. |
| `invokeOneway(String, RemotingCommand, long)` | `invoke_oneway(addr, command, deadline)` | `PARTIAL`: bounded local queue/socket-write completion only; Java executor/semaphore and exception behavior differ. Covered by `p3_u01_raw_route_deadline_shutdown_and_address_parsing`. |
| default `invoke(String, RemotingCommand, long)` returning `CompletableFuture<RemotingCommand>` | No direct future-returning async method; `invoke_async` accepts callback | `PARTIAL`: a caller can adapt callback to a future; not a direct API overload. |
| `registerProcessor(int, NettyRequestProcessor, ExecutorService)` | `register_processor_with_executor(code, processor, Option<Arc<dyn RequestExecutor>>)` | `PARTIAL`: per-code executor and rejection covered by `p3_processor_specific_and_default_executors_route_or_reject_tasks`; Java executor lifecycle and `ChannelHandlerContext` unavailable. |
| `setCallbackExecutor(ExecutorService)` / `getCallbackExecutor()` | `set_callback_executor(Option<Arc<dyn CallbackExecutor>>)`; no getter | `PARTIAL`: `p3_caller_callback_executor_routes_callbacks_to_owned_executor`, `p3_callback_executor_rejection_runs_callback_inline`; no getter or Java `ExecutorService` lifecycle. |
| `isChannelWritable(String)` | `is_channel_writable(&str)` | `PARTIAL`: queue capacity or Rust queued-byte hysteresis, not Netty channel writability. Covered by `p3_channel_management_matches_java_unknown_writable_and_explicit_close_contract`. |
| `isAddressReachable(String)` | `is_address_reachable(&str, deadline)` | `PARTIAL`: Rust operation is async and requires explicit deadline. Same channel management test. |
| `closeChannels(List<String>)` | `close_channels(&[String])` | `PARTIAL`: matching pull requests are interrupted and cached channels closed; error representation differs. Covered by `p3_close_channels_interrupts_pending_pull_requests_only`. |
| `getChannelEventListener()` | `set_channel_event_listener(Option<Arc<dyn ChannelEventListener>>)`; no getter | `PARTIAL`: callbacks omit the Java `Channel` argument and worker lifecycle/order differ. Covered by `p3_controls_validate_defaults_hooks_callback_and_channel_events`, `p3_hooks_and_channel_connect_event_follow_successful_exchange`. |
| protected `scanChannelTablesOfNameServer()` / private `scanAvailableNameSrv()` | Internal `timeout_scheduler` / `scan_available_name_servers` | `PARTIAL`: timing, executor, channel-table selection and cleanup differ; not public Rust API. |
| protected `invokeImpl(Channel, ...)`, `invokeSyncImpl(Channel, ...)`, `invokeAsyncImpl(...)`, `invokeOnewayImpl(...)` | Private connection/invoke internals | `PARTIAL`: Tokio connection and owned-future model; Netty `Channel` is not surfaced. |

## Callback and response-future surface

| Java source API | Rust mapping | Coverage / status |
|---|---|---|
| `InvokeCallback.operationComplete(ResponseFuture)` | `InvokeCallback::operation_complete(&Result<...>)`; overridable `operation_complete_with_context(&InvokeCallbackResult)` | `PARTIAL`: result/context abstraction differs from mutable Java `ResponseFuture`. |
| `InvokeCallback.operationSucceed(RemotingCommand)` / `operationFail(Throwable)` | `operation_succeed(&RemotingCommand)` / `operation_fail(&TransportError)` | `PARTIAL`: Rust error enum does not preserve every Java exception class/cause. |
| `ResponseFuture.executeInvokeCallback`, `interrupt`, `release`, `isTimeout`, `waitResponse`, getters/setters for begin time/send status/timeout/callback/cause/response/opaque/request/channel/interrupted | No public `ResponseFuture`; private `PendingEntry`, `PendingGuard`, `InvokeCallbackResult` | `PARTIAL`: ownership and immutability differ; Java channel/cause/release contract is not exposed. Tests: `p3_async_callback_contains_response_future_metadata_and_timeout_grace`, `p3_expired_async_callback_retains_request_and_timeout_metadata`, `p3_async_callback_completes_on_deadline_failure`, `p3_async_admission_timeout_completes_through_callback`, `p3_callback_executor_rejection_runs_callback_inline`. |
| `NettyRemotingAbstract.scanResponseTable()` timeout completion | Owned Tokio `timeout_scheduler`, pending deadline checks and async callback scan grace | `PARTIAL`: no Java callback-executor scan semantics or identical timeout race. Covered by callback metadata/grace and core pending-race tests. |

## Client configuration fields

Java declarations/defaults below are from `NettyClientConfig` and `NettySystemConfig` at the pinned source. `NettySystemConfig` defaults may be overridden by JVM system properties named by the matching `COM_ROCKETMQ_*` constant. Rust uses an explicit `ClientConfig`; no JVM-style global system-property loading is intended.

| Java property/getter-setter | Java type/default | Rust mapping/default | Coverage / status |
|---|---|---|---|
| `clientWorkerThreads` / `getClientWorkerThreads`, `setClientWorkerThreads`; system property `com.rocketmq.remoting.client.worker.size` | `int`, `4` | `worker_threads: usize`, `4`; validation rejects zero | `PARTIAL`: Tokio worker/task topology differs. |
| `clientCallbackExecutorThreads` / getter-setter | `int`, `Runtime.availableProcessors()` | `callback_executor_threads: usize`, available parallelism; zero maps to 4 at runtime | `PARTIAL`: Java executor ownership/topology differs; controls tests. |
| `clientOnewaySemaphoreValue` / getter-setter; property `com.rocketmq.remoting.clientOnewaySemaphoreValue` | `int`, `65535` | `oneway_semaphore_value: usize`, `65535`; rejects zero | `PARTIAL`: admission is deadline-bound; Java semaphore exception semantics differ. |
| `clientAsyncSemaphoreValue` / getter-setter; property `com.rocketmq.remoting.clientAsyncSemaphoreValue` | `int`, `65535` | `async_semaphore_value: usize`, `65535`; rejects zero | `PARTIAL`: same admission difference. |
| `connectTimeoutMillis` / getter-setter; property `com.rocketmq.remoting.client.connect.timeout` | `int`, `3000` | `connect_timeout: Duration`, 3000 ms; rejects zero | `PARTIAL`: type/unit converted. |
| `channelNotActiveInterval` / getter-setter | `long`, 60000 ms | `channel_not_active_interval: Duration`, 60 s | `PARTIAL`: scan algorithm/lifecycle differs. |
| `isScanAvailableNameSrv` / getter-setter | `boolean`, `true` | `scan_available_nameserver: bool`, `true` | `PARTIAL`: Rust scan cadence is connect timeout and execution topology differs. |
| `clientChannelMaxIdleTimeSeconds` / getter-setter; property `com.rocketmq.remoting.client.channel.maxIdleTimeSeconds` | `int`, 120 s | `max_idle_time: Duration`, 120 s; zero disables | `PARTIAL`: Java Netty idle event versus Tokio timer. |
| `clientSocketSndBufSize` / getter-setter; property `com.rocketmq.remoting.socket.sndbuf.size` | `int`, 0 | `client_socket_send_buffer_size: usize`, 0; validates Java signed-int range | `MAPPED` to socket option; OS effective buffer sizing may vary. |
| `clientSocketRcvBufSize` / getter-setter; property `com.rocketmq.remoting.socket.rcvbuf.size` | `int`, 0 | `client_socket_receive_buffer_size: usize`, 0; validates Java signed-int range | `MAPPED` to socket option; OS effective buffer sizing may vary. |
| `clientPooledByteBufAllocatorEnable` / getter-setter; property `com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable` | `boolean`, `false` | `pooled_byte_buffer_allocator_enabled: bool`, `false` | `PARTIAL`: `true` rejected because Tokio has no Netty pooled allocator control. |
| `clientCloseSocketIfTimeout` / getter-setter; property `com.rocketmq.remoting.client.closeSocketIfTimeout` | `boolean`, `true` | `close_socket_if_timeout: bool`, `true` | `PARTIAL`: socket-close heuristic differs. |
| `useTLS` / getter-setter; property `tls.enable` defaulting from TLS mode | `boolean`, client default false unless TLS mode is enforcing | `transport: TransportMode`, default plain | `PARTIAL`: explicit `TlsConfig` and secure verification, Java TLS system/config files not mapped. |
| `writeBufferHighWaterMark` / getter-setter; property `com.rocketmq.remoting.write.buffer.high.water.mark` | `int`, `0` | `write_buffer_high_water_mark: usize`, `0` | `PARTIAL`: Rust counts queued outbound bytes, not Netty channel pending bytes. |
| `writeBufferLowWaterMark` / getter-setter; property `com.rocketmq.remoting.write.buffer.low.water.mark` | `int`, `0` | `write_buffer_low_water_mark: usize`, `0` | `PARTIAL`: same accounting difference. |
| `disableCallbackExecutor` / getter-setter | `boolean`, `false` | `disable_callback_executor: bool`, `false` | `PARTIAL`: Rust direct callback execution replaces Java event-loop/caller semantics. |
| `disableNettyWorkerGroup` / getter-setter | `boolean`, `false` | None | `NOT MAPPED`: Netty event-loop worker-group switch has no Tokio equivalent. |
| `maxReconnectIntervalTimeSeconds` / getter-setter | `long`, `60` | None | `NOT MAPPED`: Java ChannelWrapper reconnect backoff; Rust GO_AWAY single retry has no equivalent delay setting. |
| `enableReconnectForGoAway` / getter-setter | `boolean`, `true` | `enable_reconnect_for_go_away: bool`, `true` | `PARTIAL`: one retry exists; ChannelWrapper reconnect semantics differ. |
| `socksProxyConfig` / getter-setter | JSON `String`, `"{}"` | `Option<SocksProxyConfig>`, default `None` | `PARTIAL`: typed config rather than Java JSON parser/config semantics. |

## TLS system/configuration inventory

| Java source setting or control | Rust mapping | Status / reason |
|---|---|---|
| `tls.enable`, `tls.server.mode` | Explicit `TransportMode::{Plain,Tls}` | `PARTIAL`: no JVM properties or permissive/server mode. |
| `tls.config.file` and `tls.properties` load/reload | None | `NOT MAPPED`: Rust TLS values are constructed explicitly; dynamic Java config reload is absent. |
| `tls.test.mode.enable` (Java default `true`) | None | `NOT MAPPED`: Rust keeps certificate-chain and hostname validation; does not reproduce insecure test-mode behavior. |
| `tls.client.keyPath`, `tls.client.keyPassword`, `tls.client.certPath`, `tls.client.authServer`, `tls.client.trustCertPath` | `TlsConfig::{ca_pem,client_cert_pem,client_key_pem}`; CA file helper | `PARTIAL`: PEM bytes/file and mutual TLS supported; password-protected key, toggleable insecure auth, and Java property loading absent. |
| Server TLS key/cert/client-auth/trust settings (`tls.server.*`) | Not applicable to client-only crate | `OUT OF SCOPE` for Phase 3 client. |
| `TlsHelper.DecryptionStrategy` registration | None | `NOT MAPPED`: no implicit private-key decryption plugin. |

## Netty-specific behavior inventory

| Java behavior | Rust status |
|---|---|
| `ChannelHandlerContext` passed to `NettyRequestProcessor.processRequest` and pipeline callbacks | `NOT EQUIVALENT`: Rust passes owned command and `RequestContext` (remote address/generation); no mutable Netty channel pipeline/context abstraction. |
| `Channel` passed to channel-event callbacks and exposed by `ResponseFuture` | `NOT EQUIVALENT`: Rust event carries remote address only; adding a fake channel handle would not reproduce Netty channel operations. |
| `ExecutorService` ownership/shutdown, rejection policy and Java thread identities | `PARTIAL`: caller-provided Rust executor traits exist for callbacks/processors; Rust does not own/shutdown Java-style executors. Tokio task and blocking worker lifecycle is different. |
| `NettyEventExecutor` event queue lifecycle, ordering, overload | `PARTIAL`: Rust bounded single-worker queue (capacity 10001) drops when full; Java event executor lifecycle is not identical. |
| `Channel.isWritable()` and Netty high/low accounting | `NOT EQUIVALENT`: Rust measures its own bounded writer queue bytes/capacity; not the Netty event loop's pending outbound bytes. |
| Pooled ByteBuf allocator and disabling Netty worker group | `NOT EQUIVALENT`: explicitly reject pooled allocator enablement; no worker-group toggle. |
| `ChannelWrapper` reconnect/replacement/backoff and channel-table scans | `PARTIAL`: per-address Tokio connection generations and single retry are implemented, but wrapper identity and Java scan/reconnect executor semantics do not exist. |

## Current test-ID index

| Test ID/name | Java behavior under test |
|---|---|
| `p3_v01_parallel_responses_and_opaque_lifecycle`; client unit `pending_terminal_race_and_opaque_wrap_never_replace_live_entries`, `cancelled_invoke_guard_removes_only_its_pending_generation`, `response_timeout_and_shutdown_race_has_one_terminal_completion` | Pending correlation, cancellation, timeout/shutdown terminal ownership and opaque wrap. |
| `p3_v02_single_flight_shares_connection_and_stale_close_keeps_generation` | Per-address connection reuse and stale generation close. |
| `p3_v03_inbound_dispatch_and_reordered_parallel_responses`; `p3_inbound_oneway_runs_processor_but_never_writes_response`; `p3_default_processor_handles_unknown_codes_but_exact_registration_wins` | Inbound dispatch, oneway, fallback processor and response matching. |
| `p3_async_callback_contains_response_future_metadata_and_timeout_grace`; `p3_expired_async_callback_retains_request_and_timeout_metadata`; `p3_async_callback_completes_on_deadline_failure`; `p3_async_admission_timeout_completes_through_callback`; callback executor tests | Callback metadata, timeout, executor admission/rejection and ordering. |
| `p3_nameserver_list_selection_and_availability_tracking`; `p3_nameserver_selection_fails_over_after_connect_failure`; `p3_nameserver_recovers_selection_after_chosen_channel_closes` | NameServer list, availability, chosen server and connection failover. |
| `p3_channel_management_matches_java_unknown_writable_and_explicit_close_contract`; `p3_close_channels_interrupts_pending_pull_requests_only` | Writability, reachability, close and pull interruption. |
| `p3_context_aware_pipeline_receives_connection_metadata`; hook/pipeline tests | Pipeline context, abort/failure mapping, hook execution order. |
| `p3_processor_specific_and_default_executors_route_or_reject_tasks` | Processor executor routing and rejection. |
| `p3_controls_validate_defaults_hooks_callback_and_channel_events`; `p3_hooks_and_channel_connect_event_follow_successful_exchange` | Config defaults/validation and basic event delivery. |
| `p3_u01_raw_route_deadline_shutdown_and_address_parsing`; `p3_u01_inflight_invoke_times_out_when_peer_does_not_reply`; TLS and SOCKS5 tests | Local transport/deadline/security paths only; not live Broker validation. |

## Inventory conclusion

The named Phase 3 Java public interface, client implementation, callback/response-future, processor, event, configuration and TLS source areas have been enumerated against current Rust APIs and named scripted/unit tests. Some inherited `NettyRemotingAbstract` control paths (notably response-table hooks and reconnect internals) still require a line-by-line branch review; this ledger is not an exhaustive branch proof. This is a source-scope inventory, not a parity pass: rows explicitly marked `PARTIAL`, `NOT MAPPED` or `NOT EQUIVALENT` are still acceptance blockers unless a source-based review proves a behavior is genuinely Netty-specific and the phase owner revises the acceptance contract. `P3-V04` remains `PENDING_PHASE_19`; offline tests and this ledger are not Broker evidence.
