# RocketMQ Classic Rust Client

A Rust reimplementation of the Apache RocketMQ 5.5.1 Java client, with RocketMQ server 5.3.4 as the planned local/live validation baseline. Compatibility work is tracked phase by phase in [`plan/2026/09/README.md`](plan/2026/09/README.md); model and protocol status is documented in [`docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md`](docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md).

## Current workspace

Implementation work now covers phases 0–18, including typed protocol/remoting layers, the core RPC/runtime, Producer, Pull/Push/Lite consumers, orderly processing, transactions, request/reply, namespace/security/operations, admin/query APIs, and server-free parity gates. The P18-V02 server-free gates were refreshed on 2026-10-24, and the P18-V03 documentation/package review is recorded. P18-V01 remains open: the broad Phase 6 source/API/input/reverse-export reconciliation is still partial. Native Windows runtime and current-revision Ubuntu CI remain unverified, so P18-V04 remains open. The Phase 9 Pull raw implementation-accessor review is resolved; the Push raw implementation surface has method-level mappings or reviewed dispositions. The project does not claim full compatibility or release readiness. Phase 19 owns live Broker/Docker validation and final acceptance. See the [Phase 6 audit](docs/compatibility/PHASE_6_REQUIREMENT_AUDIT.md), [Phase 9 matrix](docs/compatibility/PHASE_9_OFFLINE_METHOD_MATRIX.md), and [release checklist](docs/release-checklist.md) for current evidence and remaining work.

Phase 19 local live coverage is 36/37 PASS. `P17-V02` passes
duplicate-key, bounded-time, partial-Broker-failure, and terminal-error checks
against the pinned 5.3.4 image. `P12-V03` also passes mixed-client orderly
handoff and post-shutdown offset/lock checks; `P16-V02` remains pending while
the local ARM64 image's missing Linux ARM64 RocksDB JNI library is evaluated.
See the
[server results](docs/compatibility/SERVER_5_3_4_RESULTS.md) for evidence and
limits.

Phase 5's `rocketmq-classic-core` provides the typed one-attempt RPC façade over the common/protocol/remoting crates. Ordinary/Lite pull, producer sync/future/oneway, lifecycle and POP/ACK extension calls have 148 pinned Java invocation comparisons plus scripted status/error/deadline/pending tests. P5-V01/V03/U01 are locally verified; P5-V02 remains Phase 19. The extension live attempt reached heartbeat, heartbeat V2, assignment, and request-mode success, then Broker 5.3.4 rejected Lite subscription request 200071 as unsupported; successful Lite/POP behavior remains unverified. Pull/POP RPCs return raw message bytes; Phase 8 provides the bounded decoder and the consumer crates own higher-level processing. See the [Phase 5 method inventory](docs/compatibility/PHASE_5_OFFLINE_METHOD_MATRIX.md) and [handoff](docs/compatibility/PHASE_5_HANDOFF.md).

The lower-level workspace crates are `rocketmq-classic-common`, `rocketmq-classic-protocol`, and `rocketmq-classic-remoting` (bounded codecs and the locally verified TCP/TLS/SOCKS5 remoting transport; Broker/live validation is deferred to Phase 19). Construct a `RemotingClient` and call `start()` from an active Tokio runtime before invoking transport operations; `start()` is idempotent and constructors do not start background workers. The transport exposes async callbacks with configurable callback workers or a caller-supplied callback executor (`set_callback_executor`/`get_callback_executor`; disabled mode ignores custom executors and runs inline), sync futures, deadline-bound admission controls, request pipeline (legacy and context-aware; supports typed abort/error outcomes, Java-order stage composition via `pipe_request_pipeline`), backward-compatible fallible RPC hooks with typed abort/system-error mapping, optional Java-style equality keys for deduplication, and explicit `add_rpc_hook`/`get_rpc_hooks`/`remove_rpc_hook`/`clear_rpc_hooks` (snapshot inspection/removal without exposing a live mutable hook list), outbound hooks after successful connection establishment, Java-style pipeline/after-hook handling after a before-hook failure, and after-hook notification when a processor returns no response, exact-code/default request processors with optional per-registration executors (inbound one-way requests are processed without replies), bounded asynchronous channel/idle events, NameServer list/selection/availability scanning, reachability/writability/explicit-close helpers, configured TCP socket buffers, Java-shaped `socks_proxy_config` JSON CIDR routing, and one GO_AWAY retry that reconstructs a fresh request while preserving its code/ext fields/body. It does not claim Java 5.5.1 parity: callback context now carries the request, optional response/error, send status (Java default-true on timeout after channel establishment and false on established-channel close/shutdown/write failure), timeout flag and elapsed duration, and async response waiting includes Java's +1-second scan grace; the remaining Java/Netty differences are explicitly scoped: Java send-failure/error text taxonomy and executor/thread lifecycle are not reproduced exactly; processor rejection, channel-event ordering, and NameServer wrapper/scan scheduling use documented Tokio behavior; pooled allocator and worker-group controls have Rust-native implementations (bounded per-connection frame reuse and direct inbound dispatch); TLS Java-compatible property/config-file loading, reload at TLS context construction, and process-wide key-decryption strategy registration via `register_decryption_strategy` are implemented, with provider-supported encrypted-key limits. Existing clients retain their TLS context settings. These differences do not imply missing Phase 3 local features and do not claim full Java parity. Rust clears chosen/available NameServer state on socket close. TLS explicitly models Java's client `tls.test.mode.enable` (default `true`) and `tls.client.authServer` (default `false`) trust behavior, including trust-all when either Java condition disables verification; set `test_mode: false, auth_server: true` for verified CA and hostname checks. The scoped Java public remoting API/configuration inventory now covers the listed public/protected client declarations, settings, TLS controls, and Rust public surface in both directions; inventory closure does not mean semantic parity, and the remaining differences are enumerated in the Phase 3 inventory. Configured write watermarks track queued outbound bytes with high/low hysteresis; they do not represent Netty's event-loop write-buffer accounting. The Rust pooled mode reuses up to four frame buffers per connection, and `disable_netty_worker_group` switches inbound dispatch to the connection task; neither reproduces Netty's allocator caches or event-loop thread lifecycle. It is pinned to Rust 1.85.0 (MSRV 1.85) and uses edition 2024. Producer and consumer processing runtimes, namespace/security/trace/operational features, and admin/query surfaces have implementation work through Phase 17. Phase 18 server-free parity and release preparation is in progress; its open audit items and the Phase 19 live validation handoff are tracked in the compatibility matrices. See the [compatibility matrix](docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md), [Phase 3 remoting inventory](docs/compatibility/PHASE_3_REMOTING_INVENTORY.md), and ADR-0001.

## Producer usage and delivery lifecycle

The facade follows the Java producer lifecycle: create a `ProducerConfig` and
`Producer`, call `start()`, send a mutable `Message`, then await
`shutdown()`. The [compiled producer example](examples/producer.rs) shows this
flow; set `ROCKETMQ_NAMESRV_ADDR` and `ROCKETMQ_EXAMPLE_TOPIC` to run it against
a configured cluster.

Send timeouts and retries do not guarantee that a timed-out message was not
stored: a Broker can accept the message before its response is lost, and a
retry can store it again. Applications that retry after an ambiguous result
should use stable business identifiers and idempotent handling. Consumer
redelivery has the same practical requirement: message handlers should tolerate
receiving a message again before its offset is durably advanced.

Await producer and consumer `shutdown()` before exiting so owned tasks stop,
clients unregister, and offsets can be persisted. Push consumer shutdown uses
`awaitTerminationMillisWhenShutdown` to bound how long it waits for in-flight
consume work; work still running when that wait expires can be cancelled and
delivered again later. Check shutdown results because offset persistence,
orderly unlock, or runtime unregistration can fail.

The Java API reference remains RocketMQ client 5.5.1. RocketMQ Broker 5.3.4 is
the Phase 19 interoperability baseline. Phase 18 source/API audit and live
validation are still in progress; these usage notes do not claim full
compatibility.

Run workspace checks with:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
scripts/check-workspace-metadata.py
```
