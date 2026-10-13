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
`jstackEnable` dump enumerates process threads on macOS ARM64 and x86_64, but
other targets still fall back to the request thread only. Cross-platform
all-thread stack capture is the remaining behavior gap.

## Required local evidence still open

- **P17-V01:** Route-not-found, empty-route and malformed-route behavior is now
  covered for publish/subscribe queue queries. Offset and create-topic
  permission errors and unique-key terminal `NO_MESSAGE` are forwarded by all
  three facades. Java-style Running Info statistics are implemented and covered
  locally. Remaining work is all-thread `jstackEnable` parity on non-macOS
  targets. Core tests cover Broker errors/malformed responses for offset RPCs,
  create-topic permission errors, query failures/malformed bodies, and
  view-message not-found/malformed responses.
- **P17-V03:** Reset fencing now has separate late-pull and slow-callback
  regressions; direct-consume offset isolation, slow-callback status
  responsiveness, and executor shutdown joining also have targeted tests.
  Protocol, core, remoting, consumer and producer locked suites, strict Clippy,
  formatting, Maven fixtures, differential self-test, workspace metadata and
  diff checks pass on this working tree.
- **P17-U01:** Complete. Scripted multi-Broker tests cover duplicate results,
  completion-order merge, bounded time fields, partial failure, maximum
  watermark, cluster/key filtering, `lastKey`, empty results and terminal
  Broker failures. Ordering was checked against the pinned Java callback
  aggregation in `MQAdminImpl.queryMessage`.

**P17-V02** is the Broker-backed query check assigned to Phase 19; it does not
replace the local evidence above. Phase 18 has not started.

Latest local gates passed: locked tests and strict all-target/all-feature
Clippy for protocol, remoting, core, consumer and producer; pinned Maven
`verify` (32 scenarios, including the Running Info fixture); differential
self-test; workspace metadata; `cargo fmt --all -- --check`; and
`git diff --check`. macOS x86_64 stack code also compiles from the ARM64 host
with `cargo check --target x86_64-apple-darwin -p rocketmq-classic-consumer
--locked`. These checks do not close the non-macOS all-thread stack gap.
