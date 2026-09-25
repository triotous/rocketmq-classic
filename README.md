# RocketMQ Classic Rust Client

A Rust reimplementation of the Apache RocketMQ 5.5.1 Java client, with RocketMQ server 5.3.4 as the planned local/live validation baseline. Compatibility work is tracked phase by phase in [`plan/2026/09/README.md`](plan/2026/09/README.md); model and protocol status is documented in [`docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md`](docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md).

## Current workspace

The current workspace contains `rocketmq-classic-common`, `rocketmq-classic-protocol`, and `rocketmq-classic-remoting` (bounded codecs and a partial TCP/TLS/SOCKS5 transport). The transport exposes async callbacks with configurable callback workers or a caller-supplied callback executor (`set_callback_executor`/`get_callback_executor`), sync futures, deadline-bound admission controls, request pipeline (legacy and context-aware; supports typed abort/error outcomes, Java-order stage composition via `pipe_request_pipeline`), backward-compatible fallible RPC hooks with typed abort/system-error mapping and explicit deduplicating `add_rpc_hook`/`clear_rpc_hooks`, outbound hooks after successful connection establishment, Java-style pipeline/after-hook handling after a before-hook failure, and after-hook notification when a processor returns no response, exact-code/default request processors with optional per-registration executors (inbound one-way requests are processed without replies), bounded asynchronous channel/idle events, NameServer list/selection/availability scanning, reachability/writability/explicit-close helpers, configured TCP socket buffers and one GO_AWAY retry. It does not claim Java 5.5.1 parity: callback context now carries the request, optional response/error, send status, timeout flag and elapsed duration, and async response waiting includes Java's +1-second scan grace; precise send-failure/error taxonomy, Java-owned per-processor/default executor lifecycle/thread semantics, processor executor rejection details, channel lifecycle, full NameServer channel-wrapper/scan lifecycle semantics, pooled allocator and Netty worker-group controls, and TLS system-property/config-file/reload/test-mode controls remain. Rust clears chosen/available NameServer state on socket close; secure rustls verification intentionally does not adopt Java's insecure TLS test-mode defaults. The Java public API/configuration inventory is still not exhaustively closed in both directions. Configured write watermarks track queued outbound bytes with high/low hysteresis; they do not represent Netty's event-loop write-buffer accounting and are not a Netty allocator or worker-group equivalent. It is pinned to Rust 1.85.0 (MSRV 1.85) and uses edition 2024. See the [compatibility matrix](docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md), [Phase 3 remoting inventory](docs/compatibility/PHASE_3_REMOTING_INVENTORY.md), and ADR-0001. Producer/consumer and higher-level lifecycle features remain planned in later phases.

Run workspace checks with:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
scripts/check-workspace-metadata.py
```
