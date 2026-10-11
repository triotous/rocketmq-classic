# Phase 9 offline method matrix

Java source baseline: Apache RocketMQ client 5.5.1, commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. The 5.3.4 Broker is not contacted by these checks. Boundary RPC fixture hashes and source provenance are recorded in [`fixtures/rpc-invoked-offset-boundaries/provenance.json`](../../fixtures/rpc-invoked-offset-boundaries/provenance.json).

## Source-to-Rust mapping

| Java method or behavior | Rust owner | Evidence |
| --- | --- | --- |
| `OffsetStore` read/update/freeze/remove/snapshot transitions | `rocketmq_classic_consumer::offset::{OffsetTable, OffsetStore}` | `offset/mod.rs` tests cover missing versus found, memory/store reads, increase-only stale updates, unconditional reset, atomic freeze/reset, removal and loading replacement. |
| `RemoteBrokerOffsetStore` route selection, QUERY/UPDATE offset RPC, oneway persistence and failure retention | `offset/remote.rs`; `core::api::consumer::{query_consumer_offset, update_consumer_offset}` | `tests/offsets.rs` verifies master/fallback route selection, cache population, QUERY_NOT_FOUND separately from transport/RPC failure, oneway fields and failed-persist cache retention. |
| `LocalFileOffsetStore` path layout, Fastjson object-key JSON, load/read/persist and backup recovery | `offset/local.rs` | Java layout fixture plus unit and restart tests verify exact i64 offsets, safe path components, single-writer ownership, backup recovery and error when both snapshots are corrupt. Fault injection at each temp/sync/backup/rename/directory-sync step always recovers an old or new snapshot. |
| `DefaultMQPullConsumerImpl` explicit pull, selector/blocking overloads and explicit offset persistence | `pull::PullConsumer` | `tests/pull_consumer.rs` checks request validation, default/custom `maxMsgBytes`, namespaced queue/group, per-queue Broker suggestion, slave commit flags, all four pull statuses, decode/filter hooks, no implicit offset update/persist, explicit update/read/persist, registration and shutdown. |
| `MQClientAPIImpl.searchOffset` / `getMaxOffset` / `getMinOffset` / `getEarliestMsgStoretime` | `core::api::consumer::{search_offset, get_max_offset, get_min_offset, get_earliest_msg_storetime}` | Request codes 29–32, Java header names/defaults, signed timestamp, signed response fields, successful `-1`, response decoding and Broker error handling are checked by `rpc_invoked_offset_boundary_fixtures.rs` and `offset_boundary_headers.rs`. |
| `MQAdminImpl` queue-to-master lookup for offset-boundary calls | `pull::consumer::find_publish_broker` and the four public `PullConsumer` boundary methods | The pull facade loopback test asserts route refresh, master address selection, `bname`, signed values and all four request codes. |
| `BoundaryType` enum methods | protocol `header::BoundaryType` | Constant order/names, enum `valueOf` failures, `getName`, `getType`, ordinal and comparison semantics are checked in `offset_boundary_headers.rs`. The wire uses Java enum names `LOWER`/`UPPER`; Java `getName()` returns lowercase text. |

## Rust public surface mapping

| Rust surface | Java counterpart | Evidence |
| --- | --- | --- |
| `PullConsumer::{pull, pull_with_timeout, pull_with_max_size, pull_with_max_size_and_timeout, pull_with_selector, pull_block_if_not_found, pull_block_with_selector}` | `DefaultMQPullConsumerImpl.pull/pullBlockIfNotFound` overloads; callback-based `maxSizeInBytes` overload is exposed as a Future | Pull facade scripted peer checks a positive custom byte cap in `maxMsgBytes`, default `i32::MAX` on other overloads, and rejects zero/negative values before sending a request. |
| `PullConsumer::{update_consume_offset, fetch_consume_offset, persist_offset, persist_consumer_offset, update_consume_offset_to_broker}` | `DefaultMQPullConsumerImpl` explicit offset controls and Java `OffsetStore` | `offsets.rs` and `pull_consumer.rs`; pulling alone leaves offsets unchanged. |
| `PullConsumer::{get_offset_store, set_offset_store}` | Deprecated Java `DefaultMQPullConsumer.getOffsetStore/setOffsetStore` | Lifecycle unit test checks `None` before selection, model-selected store after startup, and retained access after shutdown. |
| `PullConsumer::register_filter_message_hook` | Java `DefaultMQPullConsumerImpl.registerFilterMessageHook` | Pull facade test verifies the hook receives Java unit-mode context. |
| `PullConsumer::{search_offset, search_offset_with_boundary, get_max_offset, get_min_offset, get_earliest_msg_store_time}` | Java `MQAdminImpl` offset helpers; default search boundary is LOWER, max offset requests committed data | Boundary RPC fixtures and routed facade test. Return values remain signed `i64`; `-1` is not treated as an RPC failure. |
| `OffsetStore`, `OffsetRead`, `ReadOffsetType`, `LocalFileOffsetStore`, `RemoteBrokerOffsetStore` | Java `OffsetStore`, remote and local implementations | Unit tests, Java local-store fixture and scripted RPC peer. |
| `PullConsumerConfig` | Java `DefaultMQPullConsumer` settings implemented by this phase | Exact setting/default/validation map below. |

## Phase 9 scope and settings

Phase 9 covers the explicit pull/store/boundary surface described in the P9 plan; this matrix does not claim that every public method on Java's legacy `DefaultMQPullConsumer` is complete. Methods outside this phase stay assigned to their planned owner:

| Java surface outside Phase 9 | Planned owner |
| --- | --- |
| `sendMessageBack`, `maxReconsumeTimes`, retry fallback | Phase 10 push/retry pipeline. |
| `MessageQueueListener`, queue allocation strategy, rebalance enablement, registration/subscription lifecycle, queues-in-balance | Phase 11 rebalance/assignment lifecycle. |
| `createTopic`, `queryMessage`, `viewMessage`, and `fetchSubscribeMessageQueues` admin/query APIs | Phase 17 admin/query surface. |
| Constructor RPC hooks | Phase 16 hook/security operations. |
| `getDefaultMQPullConsumerImpl()` raw implementation accessor | Outstanding full-surface compatibility audit; resolve in Phase 18. |

The consumer-specific settings in this phase map as follows. The inherited Java `ClientConfig` settings remain owned by the shared client configuration map and are accessed through `client_config()` / `client_config_mut()`.

| Java setting | Java 5.5.1 default/behavior | Rust setting and evidence |
| --- | --- | --- |
| `consumerGroup` | Defaults to `DEFAULT_CONSUMER`; startup rejects it until configured. | Required by `PullConsumerConfig::new`, mutable with `set_consumer_group`; validates blank/illegal values and rejects `DEFAULT_CONSUMER`. |
| `brokerSuspendMaxTimeMillis` | `20_000` ms. | `broker_suspend_max_time_millis`; default and suspend relationship covered by config tests. |
| `consumerTimeoutMillisWhenSuspend` | `30_000` ms; must be at least the Broker suspend timeout. | `consumer_timeout_millis_when_suspend`; default and relationship validation covered by config tests. |
| `consumerPullTimeoutMillis` | `10_000` ms. | `consumer_pull_timeout_millis`; supplies default pull timeout. |
| `messageModel` | `CLUSTERING`. | `message_model`; selects the default remote/local offset store. |
| `unitMode` | `false`. | `unit_mode`; controls pull flags and filter-hook context. |
| `offsetStore` | Optional override; otherwise the implementation chooses remote for clustering and local for broadcasting. `getOffsetStore()` exposes the configured/selected instance. | `set_offset_store` is valid only before startup; `get_offset_store` returns the configured store or the model-selected store after startup, retaining it after shutdown. |
| `LocalFileOffsetStore.LOCAL_OFFSET_STORE_DIR` | Java system property `rocketmq.client.localOffsetStoreDir`; default is `${user.home}/.rocketmq_offsets`. | Same-named process environment variable, then `$HOME` or `%USERPROFILE%` plus `.rocketmq_offsets`; explicit root construction is private to tests. |

The Java `maxSizeInBytes` overload is callback-based in the pinned source. Rust maps it to the Future-returning `pull_with_max_size` and `pull_with_max_size_and_timeout` methods; all existing pull overloads preserve Java's `Integer.MAX_VALUE` default. `maxMsgBytes <= 0` is rejected before route or Broker I/O.

## Verification status

| ID | Status | Coverage |
| --- | --- | --- |
| P9-V01 | `UNIT_VERIFIED` | Offset read/update/freeze modes, missing-value distinction, stale updates, reset ordering, remote persistence failure and Java store fixture. |
| P9-V02 | `UNIT_VERIFIED` | Local-file write-step fault injection, recoverable old/new generation, backup recovery, corrupt snapshots, path traversal rejection and writer ownership. |
| P9-U01 | `UNIT_VERIFIED` | Scripted pull statuses, next offset handling and persistence, Broker routing, boundary RPCs, local-store restart and offset readback. |
| P9-V03 | `PENDING_PHASE_19` | Live 5.3.4 Broker pull/restart, all statuses, suggested Broker and boundary calls have not run. |

Offline test fixtures demonstrate client request/response behavior and do not claim live Broker interoperability. Phase 9 implementation evidence is not final compatibility or release acceptance; Phase 19 owns live Broker validation.
