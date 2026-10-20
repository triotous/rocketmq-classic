# Phase 18 direct runtime consumer operations

Reference: Java 5.5.1 tag `efa1be59d6fc747ad220f036c86030f05f0419f9`,
`client/src/main/java/org/apache/rocketmq/client/impl/factory/MQClientInstance.java`.
The external checkout's HEAD is a different revision; this audit uses the tag
source and the pinned 5.5.1 Maven artifact.

## Running Info

Java `consumerRunningInfo(String)` maps to
`ClientRuntime::consumer_running_info(Option<&str>)`. The selected
`ConsumerRuntime::consumer_running_info` supplies the raw owner snapshot;
`consume_type` maps the separate Java `MQConsumerInner.consumeType()` call.
Pull and Push registrations supply their actual owner snapshots.

| Java behavior | Rust behavior / evidence |
| --- | --- |
| Missing group returns null; null group fails lookup | `Ok(None)` / typed `InvalidRoute`; direct Java fixture and Core test |
| Snapshot obtained before consume type | Core fixture comparison checks `info` then `type`; a snapshot error prevents the type call |
| NameServer address list order and duplicates retained, each with trailing semicolon | Current remoting list is read at the call boundary; null/empty and ordered duplicate-list cases |
| Shared properties overwritten in place | `SharedValue` properties retain caller alias; Java identity and Rust `ptr_eq` checks |
| Null properties fail before type lookup | Typed `ConsumerOperation`; callback ordering comparison |
| Null consume type fails after NameServer mutation, before type/version mutation | Core test compares the retained properties after failure, including old type/version values |
| Client version is `V5_5_1` and consume type comes from selected owner | Direct active/passive Java cases and actual Pull/Push facade integration |
| Runtime does not inject a thread dump | Builtin integration checks absent jstack; the inbound jstack option is separate |

`RuntimeConsumerOperationsFixtures` invokes all nine cases directly on the Java
runtime without starting it. Its fixture is
`fixtures/runtime/consumer-operations.json`, checkpoint SHA-256
`8ddd9f2aefb202c545466c37b4e8162b71e618b8e0bb545f3e1d504c31aed9bd`.
`runtime_consumer_operations::running_info_matches_java_enrichment_aliases_and_partial_failure_order`
checks the same inputs, results, call ordering and partial mutations.
`runtime_consumer_status` exercises both registered Rust consumer types.
Java exceptions map to Rust typed errors; no extra client setting is added.

## Direct consumption

Java `consumeMessageDirectly(MessageExt, String, String)` maps to async
`ClientRuntime::consume_message_directly(Option<MessageExt>, Option<&str>,
Option<&str>)`. `ConsumerRuntime::consume_message_directly` is the Push service
adapter; its optional future preserves the non-Push null-result branch without
inspecting the message. `ConsumerOperationFuture` expresses the asynchronous
Rust owner result, not a new client feature or setting.

The Java oracle calls the actual runtime and concurrent/orderly consume
services for 16 cases. `runtime_direct_consume` starts real Rust facades and
checks the same result categories, order/auto-commit flags, callback counts,
callback message/queue topics and broker names. It covers success, retry,
commit, rollback, suspend, null listener result and listener failure; nullable
broker/message, missing/Pull/custom/null group, and unavailable Push service.
Namespace conversion affects callback messages while queue context retains the
original topic. Concurrent consumption writes the message broker; orderly
consumption retains its original broker. Every case checks offset isolation.

The extended fixture checkpoint SHA-256 is
`ac828e506ea02848e9cacbe0e3ae02608dd1d189500a45d51b41e8d3a4208b22`.
Only elapsed duration and runtime-specific exception rendering are excluded
from equality: Java/Rust both assert nonnegative duration, identical failure
result category and presence of a diagnostic remark. Broker/offset/callback
relationships are compared directly.

## Remaining rows

Direct runtime `resetOffset` still requires its public entrypoint and
source/fixture reconciliation. Its inbound/owner handler is supporting
implementation, not proof of that public API.
Broad P18-V01 remains open and live evidence remains `PENDING_PHASE_19`.
