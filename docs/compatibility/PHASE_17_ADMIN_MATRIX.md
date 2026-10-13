# Phase 17 — Java admin API coverage audit

This audit is against Java client source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`. It separates facade coverage,
single-Broker RPCs, and local verification from Phase 19 Broker evidence.

## Public MQAdmin methods

The Java producer, push consumer, and pull consumer expose the shared
`MQAdmin` methods. A method on one Rust facade does not complete the other two.

| Java capability | Rust implementation | Status and remaining work |
| --- | --- | --- |
| `createTopic(key, topic, queues, attributes)` and `createTopic(..., topicSysFlag, attributes)` | `Producer`, `PushConsumer`, `PullConsumer`; shared helper in consumer crate | Implemented on all three facades. Java wrappers apply the namespace to the new topic and discard the attributes argument. Tests cover topic headers, system flag, successful routes and Broker permission errors on all three facades. |
| `searchOffset`, `maxOffset`, `minOffset`, `earliestMsgStoreTime` | Core single-Broker RPCs; Producer, Push, and Pull facade methods | Implemented on all three facades. Core tests cover successful values, Broker errors, and absent response fields. Public facade success and permission-error forwarding are covered on all three. |
| `queryMessage(topic, key, max, begin, end)` | Core single-Broker RPC and bounded fan-out; Producer, Push, Pull facades | Implemented. It selects Brokers by Java cluster rules, passes `lastKey`, filters returned keys/topics, merges messages and the maximum index watermark, tolerates per-Broker failures, and returns Java's `NO_MESSAGE` result when no messages survive. P17-U01's scripted tests cover ordering, time bounds, partial and terminal failure against pinned Java callback aggregation; all three facades test terminal `NO_MESSAGE` forwarding. |
| `viewMessage(topic, messageId)` | Core physical-offset RPC; Producer, Push, Pull facades | Implemented as Java 5.5.1 does: parse the physical message ID, send `VIEW_MESSAGE_BY_ID` with the caller's raw topic, return Broker errors directly, and return `NO_MESSAGE` for an invalid ID. There is no unique-key fallback. Direct success, not-found, malformed response, invalid ID and no-fallback cases are covered across core and facade tests. |
| Concrete facade `queryMessageByUniqKey` methods | Query-result methods on Producer, Push, and Pull | Implemented for all three facades. Java defaults, unique-key request fields, and terminal `NO_MESSAGE` forwarding are covered by facade tests. |

## Concrete facade methods outside `MQAdmin`

| Java method | Rust counterpart | Status and remaining work |
| --- | --- | --- |
| Producer `fetchPublishMessageQueues(topic)` | `Producer::fetch_publish_message_queues` | Implemented. Scripted tests cover route-not-found, empty-route, malformed-route, and successful queue conversion. |
| Push/Pull `fetchSubscribeMessageQueues(topic)` | `PushConsumer::fetch_subscribe_message_queues`, `PullConsumer::fetch_subscribe_message_queues` | Implemented. Both remove the namespace from returned queues. Tests cover missing and malformed routes. Push returns an empty set for an empty route as Java does; Pull returns the Java admin error when NameServer returns an empty queue set. |

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

## Verification status

- **P17-V01:** Local API coverage passes. Route-not-found, empty-route and
  malformed-route behavior is covered for publish/subscribe queue queries.
  Offset and create-topic permission errors and unique-key terminal
  `NO_MESSAGE` are forwarded by all three facades. Core tests cover Broker
  errors and malformed responses for offset RPCs, create-topic permission
  errors, query failures/malformed bodies, and view-message not-found/malformed
  responses. Native Windows stack runtime execution remains a final platform
  check; cross-target compile and strict lint evidence is recorded above.
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
replace the local evidence above. Phase 18 has not started.

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
