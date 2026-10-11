# Phase 9 offline method matrix

Java source baseline: Apache RocketMQ client 5.5.1, commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. The 5.3.4 Broker is not contacted by these checks. Boundary RPC fixture hashes and source provenance are recorded in [`fixtures/rpc-invoked-offset-boundaries/provenance.json`](../../fixtures/rpc-invoked-offset-boundaries/provenance.json).

## Source-to-Rust mapping

| Java method or behavior | Rust owner | Evidence |
| --- | --- | --- |
| `OffsetStore` read/update/freeze/remove/snapshot transitions | `rocketmq_classic_consumer::offset::{OffsetTable, OffsetStore}` | `offset/mod.rs` tests cover missing versus found, memory/store reads, increase-only stale updates, unconditional reset, atomic freeze/reset, removal and loading replacement. |
| `RemoteBrokerOffsetStore` route selection, QUERY/UPDATE offset RPC, oneway persistence and failure retention | `offset/remote.rs`; `core::api::consumer::{query_consumer_offset, update_consumer_offset}` | `tests/offsets.rs` verifies master/fallback route selection, cache population, QUERY_NOT_FOUND separately from transport/RPC failure, oneway fields and failed-persist cache retention. |
| `LocalFileOffsetStore` path layout, Fastjson object-key JSON, load/read/persist and backup recovery | `offset/local.rs` | Java layout fixture plus unit and restart tests verify exact i64 offsets, safe path components, single-writer ownership, backup recovery and error when both snapshots are corrupt. Fault injection at each temp/sync/backup/rename/directory-sync step always recovers an old or new snapshot. |
| `DefaultMQPullConsumerImpl` explicit pull, selector/blocking overloads and explicit offset persistence | `pull::PullConsumer` | `tests/pull_consumer.rs` checks request validation, namespaced queue/group, per-queue Broker suggestion, slave commit flags, all four pull statuses, decode/filter hooks, no implicit offset update/persist, explicit update/read/persist, registration and shutdown. |
| `MQClientAPIImpl.searchOffset` / `getMaxOffset` / `getMinOffset` / `getEarliestMsgStoretime` | `core::api::consumer::{search_offset, get_max_offset, get_min_offset, get_earliest_msg_storetime}` | Request codes 29–32, Java header names/defaults, signed timestamp, signed response fields, successful `-1`, response decoding and Broker error handling are checked by `rpc_invoked_offset_boundary_fixtures.rs` and `offset_boundary_headers.rs`. |
| `MQAdminImpl` queue-to-master lookup for offset-boundary calls | `pull::consumer::find_publish_broker` and the four public `PullConsumer` boundary methods | The pull facade loopback test asserts route refresh, master address selection, `bname`, signed values and all four request codes. |
| `BoundaryType` enum methods | protocol `header::BoundaryType` | Constant order/names, enum `valueOf` failures, `getName`, `getType`, ordinal and comparison semantics are checked in `offset_boundary_headers.rs`. The wire uses Java enum names `LOWER`/`UPPER`; Java `getName()` returns lowercase text. |

## Rust public surface mapping

| Rust surface | Java counterpart | Evidence |
| --- | --- | --- |
| `PullConsumer::{pull, pull_with_timeout, pull_with_selector, pull_block_if_not_found, pull_block_with_selector}` | `DefaultMQPullConsumerImpl.pull/pullBlockIfNotFound` overloads | Pull facade scripted peer and pull-result fixtures. |
| `PullConsumer::{update_consume_offset, fetch_consume_offset, persist_offset, persist_consumer_offset, update_consume_offset_to_broker}` | `DefaultMQPullConsumerImpl` explicit offset controls and Java `OffsetStore` | `offsets.rs` and `pull_consumer.rs`; pulling alone leaves offsets unchanged. |
| `PullConsumer::{search_offset, search_offset_with_boundary, get_max_offset, get_min_offset, get_earliest_msg_store_time}` | Java `MQAdminImpl` offset helpers; default search boundary is LOWER, max offset requests committed data | Boundary RPC fixtures and routed facade test. Return values remain signed `i64`; `-1` is not treated as an RPC failure. |
| `OffsetStore`, `OffsetRead`, `ReadOffsetType`, `LocalFileOffsetStore`, `RemoteBrokerOffsetStore` | Java `OffsetStore`, remote and local implementations | Unit tests, Java local-store fixture and scripted RPC peer. |
| `PullConsumerConfig` | Java `DefaultMQPullConsumer` settings implemented by this phase | Phase 9 consumer configuration tests and Phase 9 API map in code. |

## Verification status

| ID | Status | Coverage |
| --- | --- | --- |
| P9-V01 | `UNIT_VERIFIED` | Offset read/update/freeze modes, missing-value distinction, stale updates, reset ordering, remote persistence failure and Java store fixture. |
| P9-V02 | `UNIT_VERIFIED` | Local-file write-step fault injection, recoverable old/new generation, backup recovery, corrupt snapshots, path traversal rejection and writer ownership. |
| P9-U01 | `UNIT_VERIFIED` | Scripted pull statuses, next offset handling and persistence, Broker routing, boundary RPCs, local-store restart and offset readback. |
| P9-V03 | `PENDING_PHASE_19` | Live 5.3.4 Broker pull/restart, all statuses, suggested Broker and boundary calls have not run. |

Offline test fixtures demonstrate client request/response behavior and do not claim live Broker interoperability. Phase 9 implementation evidence is not final compatibility or release acceptance; Phase 19 owns live Broker validation.
