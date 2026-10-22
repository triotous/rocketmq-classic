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

## Current Phase 18 code revision

Captured with the reproduction command on 2026-10-16 at code revision
`15c6ca4f` (documentation-only commits follow it). Environment and fixed
inputs match the earlier captures above. This is one same-host sample; timing
is not an acceptance threshold or a cross-machine comparison.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,932.5 ns | 39.00 (1,000 samples) | 4,612.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,162.8 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,176.4 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 59,048.3 ns | 77.00 (100 samples) | 9,234.0 (100 samples) |

The sample meets the Phase 18 offline measurement evidence requirement. Changes
in timing/allocation observations from earlier revisions are recorded without
claiming regression or improvement; this benchmark defines no performance
threshold.

## Remoting custom-header revision — Linux container sample

Captured with the reproduction command on 2026-10-17 at code revision
`b6da737e`. The benchmark ran in the Rust 1.85.0 Bookworm container on Linux
AArch64 with 18 logical CPUs; the container reported only `aarch64` as its
hardware label. This sample is kept separate from the native macOS results
above and must not be compared with them as a performance change.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 1,197.1 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 712.7 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 806.0 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 12.2 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 5,409.3 ns | 79.00 (100 samples) | 9,442.0 (100 samples) |

These measurements satisfy the current-revision offline benchmark run; they
are observations, not acceptance thresholds. Allocation totals are aggregate
allocator calls and requested bytes divided by sampled operations.

## Phase 18 compatibility-closure revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-19 at code revision `aaff724edb116c87e73c2040b73c56841d18ab31`.
Environment and fixed inputs match the native macOS samples above. This is a
machine-specific observation, not a performance threshold or cross-machine
comparison.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,437.1 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,183.8 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,222.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 16.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 67,389.6 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing and allocation observations are recorded without an acceptance
threshold. Allocation totals are aggregate allocator calls and requested bytes
divided by sampled operations; loopback timing includes local TCP and Tokio
scheduling.

## Current Phase 18 server-free gate revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at code revision `da6345bb`. The benchmark ran on macOS AArch64,
Apple M5 Max, 18 logical CPUs, with Rust 1.85.0. It follows the Remoting
header-decode inventory test update; this machine-specific run sets no
acceptance threshold and is not a cross-machine comparison.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,668.9 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,832.1 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,274.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 26.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 44,968.6 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are allocator calls and requested bytes divided by sampled
operations. Timing and allocation observations are not acceptance thresholds;
loopback time includes local TCP and Tokio scheduling.

## Concurrent expiry cleanup and nested API inventory revision

Captured with the same command on 2026-10-21 at code revision
`bbcc88ae89c7fb4cf73f59f6abf7290458e217ad`. Environment and fixed inputs match
the native macOS samples above. This is a machine-specific observation, not a
performance threshold or cross-machine comparison.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,545.8 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,408.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,837.6 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 34.7 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 88,039.0 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

The latest timing sample varies from earlier runs; allocation observations are
stable. The benchmark does not exercise concurrent consume expiry and sets no
acceptance threshold.

## Cluster-topic header inventory revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at code revision `6441ca56`. The benchmark ran on macOS AArch64,
Apple M5 Max, 18 logical CPUs, with Rust 1.85.0. This machine-specific sample
does not set an acceptance threshold or support cross-machine comparisons.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,746.2 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,994.4 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,407.3 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 29.6 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 77,380.9 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Timing and allocation observations are recorded without an acceptance
threshold. Allocation totals are aggregate allocator calls and requested bytes
divided by sampled operations; loopback timing includes local TCP and Tokio
scheduling.

## Lite group-info and latest compatibility-closure revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at code revision `06ce7eb4`. The benchmark ran on macOS AArch64,
Apple M5 Max, 18 logical CPUs, with Rust 1.85.0. Fixed inputs match the native
macOS captures above. This sample is machine-specific and sets no acceptance
threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,201.2 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,186.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,646.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 29.5 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 58,945.6 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are aggregate allocator calls and requested bytes divided by
sampled operations. Timing variation across runs is not evidence of a
regression or improvement; loopback timing includes local TCP and Tokio
scheduling.

## Client message subtype closure revision

Captured with the same command and environment on 2026-10-21 at code revision
`6e2e3d09`. These observations include the `MessageClientExt` runtime-subtype
adapter and are machine-specific; they set no acceptance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,469.9 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,387.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,622.5 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 28.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 80,951.7 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation figures are aggregate allocator calls and requested bytes divided by
sampled operations. Timing varies between runs; loopback timing includes local
TCP and Tokio scheduling.

## Null/empty membership rebalance revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at code revision `9008c10e`. The benchmark ran on macOS AArch64,
Apple M5 Max, 18 logical CPUs, with Rust 1.85.0. Fixed inputs match the native
macOS captures above. This is a machine-specific sample and sets no acceptance
threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,311.3 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,116.3 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,187.6 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 15.2 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 69,798.0 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are aggregate allocator calls and requested bytes divided by
sampled operations. Timing variation across runs is not evidence of a
regression or improvement; loopback timing includes local TCP and Tokio
scheduling.

## ProcessQueue expiry error and retry revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at code revision `9147a902`. The benchmark ran on macOS AArch64,
Apple M5 Max, 18 logical CPUs, with Rust 1.85.0. Fixed inputs match the native
macOS captures above. This sample is machine-specific and sets no acceptance
threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,347.7 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,136.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,187.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 14.4 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 71,564.5 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are aggregate allocator calls and requested bytes divided by
sampled operations. Timing variation across runs is not evidence of a
regression or improvement; loopback timing includes local TCP and Tokio
scheduling. The offline benchmark does not measure ProcessQueue expiry.

## Caller-defined MessageExt subtype revision

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at code revision `7fd89286`. The benchmark ran on macOS AArch64,
Apple M5 Max, 18 logical CPUs, with Rust 1.85.0. Fixed inputs match the native
macOS captures above. This machine-specific sample sets no acceptance
threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,251.8 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,154.5 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,178.6 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 13.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 62,831.9 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are aggregate allocator calls and requested bytes divided by
sampled operations. Timing varies between runs; loopback timing includes local
TCP and Tokio scheduling. The offline benchmark does not measure subtype
dispatch directly.

## Current full server-free refresh

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
2026-10-21 at revision `0f6a0244`. The environment is macOS AArch64, Apple M5
Max, 18 logical CPUs, Rust 1.85.0. Fixed inputs match the native macOS
captures above. This is one machine-specific observation, not an acceptance
threshold or a cross-revision performance claim.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,021.8 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,115.0 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,174.2 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 17.1 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 66,419.3 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are aggregate allocator calls and requested bytes divided by
sampled operations. Timing varies between runs; loopback timing includes local
TCP and Tokio scheduling.

## Current revision refresh — 2026-10-22

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
revision `d030c74d2be4404db8ee2ac5323ab6bc44f5d8ff`. Environment: macOS
AArch64, Apple M5 Max, 18 logical CPUs, Rust 1.85.0. This is a same-host
observation with the fixed inputs above; it sets no performance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,692.2 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,993.0 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,182.5 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 27.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 81,564.5 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation counts and byte totals match the preceding current-revision sample.
Timing is higher in this run and is recorded without interpreting the single
sample as a regression or improvement.

## Current revision after Query Admin header parity fix — 2026-10-22

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
revision `80faab5a`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. Inputs match the fixed fixtures above. This is a single
host observation and sets no acceptance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 3,349.2 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 2,341.8 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 2,533.5 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 34.0 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 85,556.8 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation totals are per-operation observations. Timing varies between runs;
the sample is not interpreted as a cross-revision regression or improvement.

## Current revision after Producer startup-route parity — 2026-10-22

Captured with `cargo bench -p rocketmq-classic --bench offline --locked` on
revision `5400211e`. Environment: macOS AArch64, Apple M5 Max, 18 logical
CPUs, Rust 1.85.0. Inputs and iteration counts match the fixed fixtures above.
This is a single-host observation and sets no acceptance threshold.

| Operation | Input | Iterations | Time per iteration | Allocation calls per iteration | Allocated bytes per iteration |
| --- | --- | ---: | ---: | ---: | ---: |
| Remoting frame encode/decode | 427 encoded frame bytes | 20,000 | 2,299.3 ns | 40.00 (1,000 samples) | 4,676.0 (1,000 samples) |
| Zstd message-record decode | 146 record bytes | 10,000 | 1,165.8 ns | 17.00 (1,000 samples) | 197,493.0 (1,000 samples) |
| Producer send-request construction | 512-byte body | 20,000 | 1,203.1 ns | 54.00 (1,000 samples) | 3,665.0 (1,000 samples) |
| Publish-address lookup | 10-byte key, 128 route entries | 100,000 | 13.9 ns | 0.00 (1,000 samples) | 0.0 (1,000 samples) |
| Loopback pending-request completion | 128-byte body over local TCP | 500 | 73,612.1 ns | 81.00 (100 samples) | 9,546.0 (100 samples) |

Allocation observations match earlier captures. Timing varies across runs and
does not imply a cross-revision regression or improvement; loopback time
includes local TCP and Tokio scheduling.
