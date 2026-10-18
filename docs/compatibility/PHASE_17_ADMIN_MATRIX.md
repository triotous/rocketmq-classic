# Phase 17 — Java admin API coverage audit

This audit is against Java client source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`. It separates facade coverage,
single-Broker RPCs, and local verification from Phase 19 Broker evidence.

## Public MQAdmin methods

The Java producer, push consumer, and pull consumer expose the shared
`MQAdmin` methods. A method on one Rust facade does not complete the other two.

| Java capability | Rust implementation | Status and remaining work |
| --- | --- | --- |
| `createTopic(key, topic, queues, attributes)` and `createTopic(..., topicSysFlag, attributes)` | `Producer::{create_topic,create_topic_with_sys_flag}`, `PushConsumer::{create_topic,create_topic_with_sys_flag}`, `PullConsumer::{create_topic,create_topic_with_sys_flag}`; shared helper in consumer crate | Implemented on all three facades. Java wrappers apply the namespace to the new topic and discard the attributes argument. Tests cover topic headers, system flag, successful routes and Broker permission errors on all three facades. |
| `searchOffset`, `maxOffset`, `minOffset`, `earliestMsgStoreTime` | `search_offset`, `get_max_offset`, `get_min_offset`, `get_earliest_msg_store_time` on Producer, Push, and Pull; Core single-Broker RPCs | Implemented on all three facades. Core tests cover successful values, Broker errors, and absent response fields. Public facade success and permission-error forwarding are covered on all three. |
| `queryMessage(topic, key, max, begin, end)` | `query_message` on Producer, Push, and Pull; `QueryResult::new`, `QueryResult::{get_index_last_update_timestamp,get_message_list}` and `Display` for Java `toString`; Core single-Broker RPC and bounded fan-out | Implemented. `QueryResult` keeps its result fields private and preserves Java's nullable list/elements and mutable list alias through `QueryMessageList`. It selects Brokers by Java cluster rules, passes `lastKey`, filters returned keys/topics, merges messages and the maximum index watermark, tolerates per-Broker failures, and returns Java's `NO_MESSAGE` result when no messages survive. P17-U01's scripted tests cover ordering, time bounds, partial and terminal failure against pinned Java callback aggregation; all three facades test terminal `NO_MESSAGE` forwarding. The constructor/getter/`toString` surface and alias behavior are checked against the invoked Java fixture. |
| `MQAdminImpl.queryMessage(clusterName, topic, key, max, begin, end, isUniqKey, indexType, lastKey)` | Core `AdminQueryRequest::{by_key,by_unique_key,in_cluster,with_cluster_name,with_index_type,with_last_key}` | The request keeps `route_topic` and `logical_message_queue` internal; builders preserve nullable Java `clusterName`, `indexType`, and `lastKey` inputs. A null or empty `indexType` yields no key matches, matching the pinned Java result filter. Unit and scripted RPC regressions cover ordinary, logical-topic, and wheel-timer route selection, LMQ cluster filtering/request topic, nullable index/pagination inputs, and cluster `lastKey` forwarding. This closes the request-builder surface only; broad P18-V01 reconciliation remains open. |
| `viewMessage(topic, messageId)` | `view_message` on Producer, Push, and Pull; Core physical-offset RPC and the internal `MQAdminImpl.queryMessageByUniqKey` fallback | Java first attempts physical lookup and falls back to a unique-key query if message-ID parsing or the physical RPC throws. The fallback uses max 32, the last three days through `Long.MAX_VALUE`, and returns the first result. Producer and Pull pass the raw topic to physical lookup; Push applies its namespace. All three namespace the fallback query. Rust follows the same branches and returns `Option<MessageExt>` for Java's nullable first result. Scripted tests cover malformed IDs, physical lookup failure, successful direct lookup, query fields/default window, terminal `NO_MESSAGE`, and each facade's topic handling. |
| Public query facade surface | `query_message` on Producer, Push, and Pull; `query_message_by_unique_key` is not exposed | Pinned Java reflection inventories the complete declared and inherited Push/Pull facade methods and confirms there is no public `queryMessageByUniqKey` method. Unique-key lookup remains the behavior of `viewMessage` and the internal implementation method. Rust compile-fail doctests guard against re-exposing a separate facade method. |

Query header decode follow-up (2026-10-18, code commit `66334aa9`): the pinned Java `QueryMessageRequestHeader`, `QueryMessageResponseHeader`, and `ViewMessageRequestHeader` keep reference/wrapper fields nullable and use reflection under both `useFastEncode` hints. Rust now preserves absent or malformed boxed numeric fields through decode, retains Java identity-based equality for `ViewMessageRequestHeader`, and reports a missing/invalid query timestamp at the RPC result boundary where Java unboxes it. The invoked fixture and Rust differential cover both hints; this closes only this header-decoding slice.

## Tools-only admin extension scope audit

The pinned Java client implementation also contains public
`MQClientAPIImpl.checkRocksdbCqWriteProgress`,
`MQClientAPIImpl.exportRocksDBConfigToJson`, and
`MQClientAPIImpl.getInSyncStateData` methods. At source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`, these are called by
`tools/.../DefaultMQAdminExtImpl` and exposed by the `tools/.../MQAdminExt`
interface and `DefaultMQAdminExt` facade. They are not `client` `MQAdmin`
operations or Producer/Push/Pull client-facade methods. The result DTOs
`CheckRocksdbCqWriteResult` and `BrokerReplicasInfo` likewise surface through
the tools extension. The Phase 17 plan explicitly excludes the separate
`rocketmq-tools` artifact, so these helpers are `TOOLS_OUT_OF_SCOPE`, not
missing Rust client capabilities. Rust still carries request code 354 as a
protocol constant; this does not expose a RocksDB administration API.

Pinned-source check:

```bash
git -C /Users/bruce.tsai/Projects/3rd/rocketmq grep -n -E 'checkRocksdbCqWriteProgress|exportRocksDBConfigToJson|CheckRocksdbCqWriteResult|getInSyncStateData|BrokerReplicasInfo' efa1be59d6fc747ad220f036c86030f05f0419f9 -- client/src/main/java tools/src/main/java common/src/main/java remoting/src/main/java
```

This closes the scope disposition for these three tools-backed helpers only; it
does not close P18-V01's broader source/API/input/reverse-export audit.

## Concrete facade methods outside `MQAdmin`

| Java method | Rust counterpart | Status and remaining work |
| --- | --- | --- |
| Producer `fetchPublishMessageQueues(topic)` | `Producer::fetch_publish_message_queues` | Implemented. Scripted tests cover route-not-found, empty-route, malformed-route, and successful queue conversion. |
| Push/Pull `fetchSubscribeMessageQueues(topic)` | `PushConsumer::fetch_subscribe_message_queues`, `PullConsumer::fetch_subscribe_message_queues` | Implemented. Both remove the namespace from returned queues. Tests cover missing and malformed routes. Push returns an empty set for an empty route as Java does; Pull returns the Java admin error when NameServer returns an empty queue set. |

## Push consumer offset reset

The pinned Java `MQClientInstance.resetOffset` walks the active Push process
queues, drops and clears matching queues whose keys occur in the supplied
offset table, then processes only non-null offsets. Non-orderly reset waits
10 seconds before updating offsets; orderly reset waits up to 10 seconds for
the consume write lock. Orderly clustered removal then waits up to 500 ms for
that lock, unless the dropped queue's unlock delay has elapsed. Java removes
the local queue even when that removal attempt times out; it removes the stored
offset only on the successful orderly unlock path. A null offset therefore
drops and clears a matching queue but does not update its offset or remove it.

Rust matches these transitions: reset cancels the old queue generation so late
pull replies and consume callbacks cannot update the reset offset, does not
freeze the reset value, and removes only queues with a non-null mapped offset.
The orderly removal path persists before and during removal and preserves the
Java distinction between local queue removal and stored-offset cleanup. Paused
time regressions cover the 10-second and 500-ms windows, callback fencing,
available-lock unlock, null offsets, and queue rows without a live process
queue. The pinned source is `MQClientInstance.resetOffset` and
`waitResetOffsetReady`, plus `RebalancePushImpl.removeUnnecessaryMessageQueue`
and `tryRemoveOrderMessageQueue`.

## Client inbound administrative requests

Java `ClientRemotingProcessor` implements `RESET_CONSUMER_CLIENT_OFFSET`,
`GET_CONSUMER_STATUS_FROM_CLIENT`, `GET_CONSUMER_RUNNING_INFO`, and
`CONSUME_MESSAGE_DIRECTLY`. Rust now registers Push handlers for all four and
Pull handlers for status/running-info; Pull reset stays one-way and Pull direct
consume returns Java's missing-push-consumer error. Dispatch uses a bounded
executor whose accepted work is joined during runtime shutdown. Push queue
snapshots, Pull properties, JSON layout and credential omission have local
coverage. Push/Pull consume and POP paths now populate Java-style per-topic
success/failure counts, 10-second and 10-minute TPS/RT snapshots, and the
one-hour failure count; Running Info has non-zero statistics coverage. The
direct Java fixture confirms each `MQClientInstance` has an instance-scoped
`ConsumerStatsManager`, including separate instances with the same client ID;
Rust's PushConsumer-owned statistics pass the same isolation regression.
`jstackEnable` dump enumerates process threads on macOS ARM64 and x86_64,
Linux AArch64 and x86_64, and Windows x86_64 and ARM64. Linux sends a queued
signal to each target thread, reads its `ucontext`, and walks readable frame
records; it does not use ptrace or change `PR_SET_PTRACER`. It restores the
previous signal handler after capture. Windows enumerates with Toolhelp,
suspends one thread at a time, reads its architecture-specific context, and
resumes it through an RAII guard. If the shared deadline expires, all remaining
enumerated thread IDs are still included with an unavailable-stack marker.
Thread captures are serialized on each supported platform, so simultaneous
running-info requests do not receive a degraded current-thread-only dump or
interleave process-wide signal-handler changes. Linux stack tests cover the two
concurrent readers and signal-handler restoration on ARM64; Linux stack tests
pass on both architectures. The complete Windows consumer
crate checks and strict Clippy pass for x86_64; the isolated stack module and
tests type-check with warnings denied for x86_64 and ARM64. Native Windows
runtime behavior has not been executed on this macOS host and stays an
environment check for final validation.

Linux signal-availability follow-up (`4c142334`, 2026-10-15): the latest
inspectable GitHub Ubuntu run for older revision `cc79eb6e` failed
`jstack_includes_current_and_other_named_threads` because neither existing
default-ignore candidate (`SIGWINCH`, `SIGURG`) was usable. Added `SIGCHLD` as
another default-ignore candidate and regressions for fallback selection and
the all-candidates-blocked case. The pinned Java 5.5.1 path uses
`Thread.getAllStackTraces()` and does not have this signal-selection boundary.
The updated Linux module and tests pass an isolated x86_64 Linux target
type-check; a complete workspace cross-check cannot run on this Mac because
the Linux C sysroot/compiler needed by AWS-LC and zstd is unavailable. The
current revision subsequently passed default/all-feature workspace tests and
strict Clippy in a Linux ARM64 container after the transient-task follow-up
below. Current-revision GitHub Ubuntu and native Windows job results remain
pending.

Linux transient-task exit follow-up (`ac0a16f3`, 2026-10-17): the default
workspace run exposed a race where `/proc/self/task` iteration can report
`ESRCH` while a short-lived thread exits. Thread enumeration now skips only
the disappeared task (`ENOENT`/`ESRCH`) and still reports unrelated I/O
errors. A unit test pins the error classification. The focused regression,
default/all-feature workspace suites, and strict all-target/all-feature
workspace Clippy pass on Linux ARM64. GitHub Ubuntu and Windows execution
remain separate P17/P18 platform evidence.

## Verification status

- **P17-V01:** Local API coverage passes. Route-not-found, empty-route and
  malformed-route behavior is covered for publish/subscribe queue queries.
  Offset and create-topic permission errors and unique-key terminal
  `NO_MESSAGE` are forwarded by all three facades. Core tests cover Broker
  errors and malformed responses for offset RPCs, create-topic permission
  errors, query failures/malformed bodies, and view-message not-found/malformed
  responses. Native Windows stack runtime execution remains a final platform
  check. The GitHub workflow now includes a fail-closed Windows job that runs
  the native stack capture regression; current-revision runtime evidence is
  still pending that job. Cross-target compile and strict lint evidence is
  recorded above.
- **P17-V03:** Reset fencing has late-pull and slow-callback regressions;
  direct-consume offset isolation, slow-callback status responsiveness, and
  executor shutdown joining also have targeted tests. The full consumer suite
  passes on macOS ARM64, Linux AArch64, and Linux x86_64.
- **P17-U01:** Complete. Scripted multi-Broker tests cover duplicate results,
  completion-order merge, bounded time fields, partial failure, maximum
  watermark, cluster/key filtering, `lastKey`, empty results and terminal
  Broker failures. Ordering was checked against the pinned Java callback
  aggregation in `MQAdminImpl.queryMessage`.

**P17-V02** is the Broker-backed query check assigned to Phase 19; it does not
replace the local evidence above. Phase 18 server-free gates have since been
implemented and run locally. The remaining parity and release-preparation work
is tracked in the [Phase 18 release checklist](../release-checklist.md); live
Broker evidence remains assigned to Phase 19.

Latest local gates passed: locked consumer tests and strict all-target/all-
feature consumer Clippy on macOS ARM64, Linux AArch64, and Linux x86_64 (164
unit tests plus all consumer integration/doctests per platform); core
`admin_rpc` tests (16 cases); producer tests (92 unit, 10 admin, 5 facade,
3 request/reply, 10 scripted-send, and 6 transaction cases) and strict Clippy;
pinned Maven `verify` (32 scenarios and
Java/Rust fixture cross-decodes); differential comparator self-test; workspace
metadata; `cargo fmt --all -- --check`; and `git diff --check`. The complete
consumer crate also passes locked Windows x86_64 GNU `cargo check` and strict
all-target/all-feature Clippy in a Rust 1.85.0 MinGW container. An isolated
stack harness with warnings denied passes for Windows x86_64 GNU and ARM64
MSVC. No native Windows runtime or Broker-backed Phase 19 validation was run.
