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
| `PullConsumer::{register_message_queue_listener, set_message_queue_listener, register_topics, set_register_topics, add_register_subscriptions, clear_register_subscriptions, fetch_message_queues_in_balance}` | Java `DefaultMQPullConsumer` registration/config APIs and `MQPullConsumer.fetchMessageQueuesInBalance` | Phase 11 matrix P11-U02 verifies defaults, namespace handling, queue-listener assignment updates, offset-safe removal and lifecycle-checked queue queries. Returned queue topics have the configured namespace removed, matching Java `parseSubscribeMessageQueues`. |
| `OffsetStore`, `OffsetRead`, `ReadOffsetType`, `LocalFileOffsetStore`, `RemoteBrokerOffsetStore` | Java `OffsetStore`, remote and local implementations | Unit tests, Java local-store fixture and scripted RPC peer. |
| `PullConsumerConfig` | Java `DefaultMQPullConsumer` settings implemented by this phase | Exact setting/default/validation map below. |

## Phase 9 scope and settings

Phase 9 covers the explicit pull/store/boundary surface described in the P9 plan; this matrix does not claim that every public method on Java's legacy `DefaultMQPullConsumer` is complete. Methods outside this phase stay assigned to their planned owner:

| Java surface outside Phase 9 | Planned owner |
| --- | --- |
| `sendMessageBack`, `maxReconsumeTimes`, retry fallback | Push processing remains in Phase 10; Phase 18 parity audit added the deprecated Pull send-back overloads, Java default setting, Broker path, and retry-topic fallback to the Pull facade. |
| `MessageQueueListener`, queue allocation strategy, rebalance enablement, registration/subscription lifecycle, queues-in-balance | Phase 11 rebalance/assignment lifecycle. |
| `createTopic`, `queryMessage`, `viewMessage`, and `fetchSubscribeMessageQueues` admin/query APIs | Phase 17 admin/query surface. |
| Constructor RPC hooks | Phase 16 hook/security operations. |
| `getDefaultMQPullConsumerImpl()` raw implementation accessor | Phase 18 accessor fixture and Rust alias getter now cover non-null/stable identity; the full public surface remains under audit below. |

### Deprecated raw implementation accessor audit (Phase 18)

The pinned Java fixture invokes `DefaultMQPullConsumer.getDefaultMQPullConsumerImpl()` twice and records stable identity, configured group, `CREATE_JUST`, null offset store, null `PullAPIWrapper`, and a constructed `RebalanceImpl`. Rust maps the separate Java impl object to the owning `PullConsumer` and tests pointer identity, configured group, and the pre-start absent offset store. Rust intentionally constructs its internal `PullAPIWrapper` with the facade, so the Java null-wrapper state has no direct structural equivalent.

The accessor exposes more public methods than the facade delegates. The following surfaces still require a method-by-method behavior decision and test mapping before this audit can close:

| Java implementation surface | Current Rust mapping/evidence | Audit status |
| --- | --- | --- |
| `sendMessageBack` overloads and `maxReconsumeTimes` | `PullConsumer::{send_message_back, send_message_back_to_broker, send_message_back_with_group}` delegates to the tested retry path with Pull's exact setting. The Java call uses a 3000 ms RPC timeout; logical queue names resolve through the namespaced queue endpoint while the request header retains the passed broker name. Tests cover timeout, endpoint resolution, default/custom retry limit, group override, namespace restore and retry metadata. | Implemented and unit-verified; live Broker behavior remains P9-V03/Phase 19. |
| `parseSubscribeMessageQueues` | `PullConsumer::parse_subscribe_message_queues` removes only the configured namespace, preserves nullable queue fields, and collects results as a set. The invoked Java accessor fixture checks ordinary, already-logical, retry-topic, foreign-namespace, null-topic and null-broker inputs, including result de-duplication. | Implemented and fixture-verified. |
| `fetchPublishMessageQueues` | `PullConsumer::fetch_publish_message_queues` queries the supplied topic unchanged through `ClientRuntime::get_topic_publish_queues`, returns writable queues in route order without publishing them to the shared runtime cache, and removes the configured namespace from returned queues. Scripted NameServer cases cover success, repeated lookup, route removal after a successful query, missing/empty routes, and malformed route data with Java's distinct error path. | Implemented and scripted-peer verified; live Broker behavior remains P9-V03/Phase 19. |
| `resetTopic`, `subscriptionAutomatically`, and `unsubscribe` | Namespace normalization and Pull topic registration APIs cover adjacent flows, but there is no per-method oracle for the raw impl entrypoints. | Open behavior audit. |
| `groupName`, `messageModel`, `isUnitMode`, `consumeType`, `consumeFromWhere`, and `subscriptions` | Configuration and registration state expose most values; active-consumer and subscription snapshots are not reconciled method by method. | Open behavior audit. |
| `executeHookBefore/After` and `registerConsumeMessageHook` | Rust registers consume hooks and invokes them around pull-result handling; direct Java method calls are not separately mapped. | Open invocation audit. |
| `doRebalance`, `tryRebalance`, `updateTopicSubscribeInfo`, and `isSubscribeTopicNeedUpdate` | The runtime owns the corresponding registration callbacks and rebalance cycle. The raw Java methods are publicly reachable through the accessor. | Open invocation audit. |
| `persistConsumerOffset`, `fetchConsumeOffset`, and `updateConsumeOffsetToBroker` | Rust has explicit offset APIs, including `persist_consumer_offset`, `fetch_consume_offset`, and `update_consume_offset_to_broker`. | Direct facade mappings exist; raw-entrypoint equivalence still needs evidence. |
| `consumerRunningInfo` | Rust builds Pull running-info bodies for inbound administrative requests; no public direct getter exists. | Open API mapping. |
| `get/setPullAPIWrapper`, `get/setServiceState`, `getConsumerStartTimestamp`, and `getRebalanceImpl` | Rust owns wrapper, lifecycle, timestamp, and rebalance state inside the facade/runtime; it does not expose Java's mutable implementation objects. | Open compatibility/design decision; do not count the accessor as fully mapped. |

The Java raw implementation surface was inspected with `javap -public` against the pinned 5.5.1 artifact and the corresponding source. This table is not a waiver: any supported observable flow must be implemented with Rust-appropriate ownership and unit evidence, or the API map must explain why an internal integration entrypoint is not an application capability. P18-V01 remains open until that review is resolved.

The consumer-specific settings in this phase map as follows. The inherited Java `ClientConfig` settings remain owned by the shared client configuration map and are accessed through `client_config()` / `client_config_mut()`.

| Java setting | Java 5.5.1 default/behavior | Rust setting and evidence |
| --- | --- | --- |
| `consumerGroup` | Defaults to `DEFAULT_CONSUMER`; startup rejects it until configured. | Required by `PullConsumerConfig::new`, mutable with `set_consumer_group`; validates blank/illegal values and rejects `DEFAULT_CONSUMER`. |
| `brokerSuspendMaxTimeMillis` | `20_000` ms. | `broker_suspend_max_time_millis`; default and suspend relationship covered by config tests. |
| `consumerTimeoutMillisWhenSuspend` | `30_000` ms; must be at least the Broker suspend timeout. | `consumer_timeout_millis_when_suspend`; default and relationship validation covered by config tests. |
| `consumerPullTimeoutMillis` | `10_000` ms. | `consumer_pull_timeout_millis`; supplies default pull timeout. |
| `maxReconsumeTimes` | `16`; setter retains any supplied integer. | `PullConsumerConfig::{max_reconsume_times, set_max_reconsume_times}`; Java invoked fixture checks the default and a negative mutation, and the Pull send-back test checks the configured value in the send-back request and retry headers. |
| `messageModel` | `CLUSTERING`. | `message_model`; selects the default remote/local offset store. |
| `unitMode` | `false`. | `unit_mode`; controls pull flags and filter-hook context. |
| `offsetStore` | Optional override; otherwise the implementation chooses remote for clustering and local for broadcasting. `getOffsetStore()` exposes the configured/selected instance. | `set_offset_store` is valid only before startup; `get_offset_store` returns the configured store or the model-selected store after startup, retaining it after shutdown. |
| `LocalFileOffsetStore.LOCAL_OFFSET_STORE_DIR` | Java system property `rocketmq.client.localOffsetStoreDir`; default is `${user.home}/.rocketmq_offsets`. | Same-named process environment variable, then `$HOME` or `%USERPROFILE%` plus `.rocketmq_offsets`; explicit root construction is private to tests. |

The Java `maxSizeInBytes` overload is callback-based in the pinned source. Rust maps it to the Future-returning `pull_with_max_size` and `pull_with_max_size_and_timeout` methods; all existing pull overloads preserve Java's `Integer.MAX_VALUE` default. `maxMsgBytes <= 0` is rejected before route or Broker I/O.

### Phase 11 Pull rebalance settings

| Java setting/API | Java 5.5.1 default/behavior | Rust setting and evidence |
| --- | --- | --- |
| `allocateMessageQueueStrategy` | `AllocateMessageQueueAveragely`; validated non-null at startup. | `PullConsumerConfig::{allocate_message_queue_strategy, set_allocate_message_queue_strategy}`; defaults to `AllocateMessageQueueAveragely`; strategy differential is P11-V01. |
| `messageQueueListener` | Unset by default; listener receives topic, all queues and the local assignment; callback exceptions are isolated. | `PullConsumerConfig::{message_queue_listener, set_message_queue_listener}` and `PullConsumer::{message_queue_listener, set_message_queue_listener, register_message_queue_listener}`; P11-U02. |
| `enableRebalance` | `true`; a disabled cycle leaves the current local assignment in place. | `PullConsumerConfig::{enable_rebalance, set_enable_rebalance}` and live `PullConsumer::{enable_rebalance, set_enable_rebalance}`; P11-U02. |
| `registerTopics` / `registerSubscriptions` | Empty at construction. Registered topics become wildcard subscriptions; a non-empty explicit subscription set is used for heartbeat data. | `PullConsumer::{register_topics, set_register_topics, register_topic, add_register_subscriptions, register_subscriptions, clear_register_subscriptions}`; P11-U02 namespace and registration checks. |
| `fetchMessageQueuesInBalance(topic)` | Returns queues currently assigned to this consumer and strips the configured namespace. | `PullConsumer::fetch_message_queues_in_balance`; P11-U02 checks running-state errors, assignment results and namespace removal. |

## Verification status

| ID | Status | Coverage |
| --- | --- | --- |
| P9-V01 | `UNIT_VERIFIED` | Offset read/update/freeze modes, missing-value distinction, stale updates, reset ordering, remote persistence failure and Java store fixture. |
| P9-V02 | `UNIT_VERIFIED` | Local-file write-step fault injection, recoverable old/new generation, backup recovery, corrupt snapshots, path traversal rejection and writer ownership. |
| P9-U01 | `UNIT_VERIFIED` | Scripted pull statuses, next offset handling and persistence, Broker routing, boundary RPCs, local-store restart and offset readback. |
| P9-V03 | `PENDING_PHASE_19` | Live 5.3.4 Broker pull/restart, all statuses, suggested Broker and boundary calls have not run. |

Offline test fixtures demonstrate client request/response behavior and do not claim live Broker interoperability. Phase 9 implementation evidence is not final compatibility or release acceptance; Phase 19 owns live Broker validation.
