# Phase 18 Offline Benchmark Baseline

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-13 at revision `a8c510d1cbc7`. These are local baseline observations,
not performance thresholds. They describe this machine and toolchain only.

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
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,943.6 ns | 39.00 (1,000 samples) | 4,612.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,250.8 ns | 20.00 (1,000 samples) | 197,407.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 990.7 ns | 43.00 (1,000 samples) | 3,247.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.2 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 62,282.0 ns | 77.00 (100 samples) | 9,234.0 (100 samples) |

Allocation figures are aggregate allocator calls and requested bytes divided by
the number of sampled operations; they are not retained-heap measurements.
The loopback result includes local TCP and Tokio scheduling. Repeat measurements
on the same host before comparing revisions. This baseline sets no acceptance
threshold.

## Final current-revision rerun

Captured with the same command and environment on 2026-10-13 at revision
`97e27ff9`. This rerun includes the TopicValidator nullable-input change; its
benchmarks do not target TopicValidator. The results remain machine-specific
observations.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,999.9 ns | 39.00 (1,000 samples) | 4,612.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,282.7 ns | 20.00 (1,000 samples) | 197,407.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,077.6 ns | 43.00 (1,000 samples) | 3,247.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 63,866.0 ns | 77.00 (100 samples) | 9,234.0 (100 samples) |

The sample timing varies from the earlier run while allocation counts and byte
totals match. This is not evidence of a performance regression or improvement.

## Post Producer async executor API revision

Captured with the same command and environment on 2026-10-13 at revision
`0a3d949c`. These measurements include the Producer callback and async sender
executor API change. They remain machine-specific observations, not thresholds.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,468.0 ns | 39.00 (1,000 samples) | 4,612.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,098.5 ns | 20.00 (1,000 samples) | 197,407.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 929.2 ns | 43.00 (1,000 samples) | 3,247.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.3 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 65,799.4 ns | 77.00 (100 samples) | 9,234.0 (100 samples) |

Allocation counts and byte totals match the earlier captures. Timing variation
is not evidence of a performance regression or improvement.

## CheckForbidden error-parity revision

Captured with the same command and environment on 2026-10-14 at revision
`39afef81`. This includes the CheckForbidden retry/error mapping changes. The
measurements remain machine-specific observations, not acceptance thresholds.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,895.0 ns | 39.00 (1,000 samples) | 4,612.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,250.7 ns | 20.00 (1,000 samples) | 197,407.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 999.4 ns | 43.00 (1,000 samples) | 3,247.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 16.0 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 63,480.1 ns | 77.00 (100 samples) | 9,234.0 (100 samples) |

Allocation counts and byte totals match the earlier captures. Timing variation
is not evidence of a performance regression or improvement.

## Reproduction

```bash
cargo bench -p rocketmq-classic --bench offline --locked
cargo clippy -p rocketmq-classic --bench offline --all-features --locked -- -D warnings
cargo fmt --all -- --check
```
