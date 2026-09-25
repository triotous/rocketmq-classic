# RocketMQ Classic Rust Client

A Rust reimplementation of the Apache RocketMQ 5.5.1 Java client, with RocketMQ server 5.3.4 as the planned local/live validation baseline. Compatibility work is tracked phase by phase in [`plan/2026/09/README.md`](plan/2026/09/README.md); model and protocol status is documented in [`docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md`](docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md).

## Current workspace

The current workspace contains `rocketmq-classic-common` and `rocketmq-classic-protocol`. It is pinned to Rust 1.85.0 (MSRV 1.85) and uses edition 2024. The packages are not yet a complete client; network, producer/consumer and lifecycle features are planned in later phases.

Run workspace checks with:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
scripts/check-workspace-metadata.py
```
