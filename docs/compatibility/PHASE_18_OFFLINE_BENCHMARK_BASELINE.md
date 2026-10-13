# Phase 18 Offline Benchmark Baseline

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-13. These are local baseline observations, not performance thresholds.
They describe this machine and toolchain only.

## Environment

- OS and architecture: macOS, AArch64
- CPU: Apple M5 Max, 18 logical CPUs
- Rust: `rustc 1.85.0 (4d91de4e4 2025-02-17)`
- Profile: optimized release benchmark profile
- Fixed inputs: checked-in compressed-message fixture, fixed Remoting command,
  fixed 512-byte message body, 128-entry route table, and local plain-TCP
  request/response peer

## Results

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,703.1 ns | 39.00 (1,000 samples) | 4,612.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,151.8 ns | 20.00 (1,000 samples) | 197,407.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 983.0 ns | 43.00 (1,000 samples) | 3,247.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.4 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 62,265.5 ns | 77.00 (100 samples) | 9,234.0 (100 samples) |

Allocation figures are aggregate allocator calls and requested bytes divided by
the number of sampled operations; they are not retained-heap measurements.
The loopback result includes local TCP and Tokio scheduling. Repeat measurements
on the same host before comparing revisions. This baseline sets no acceptance
threshold.

## Reproduction

```bash
cargo bench -p rocketmq-classic --bench offline --locked
cargo clippy -p rocketmq-classic --bench offline --all-features --locked -- -D warnings
cargo fmt --all -- --check
```
