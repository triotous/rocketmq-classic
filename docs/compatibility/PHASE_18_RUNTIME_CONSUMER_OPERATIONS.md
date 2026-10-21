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
services for 26 cases. `runtime_direct_consume` starts real Rust facades and
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

Raw-input continuation (2026-10-20): the owner no longer converts every
`MessageExt` to client-message IDs. Ordinary messages retain their supplied
physical `msgId`, even with an `UNIQ_KEY` property; client-decoded Rust snapshots
retain their effective and offset IDs. The inbound processor now invokes the
existing `client_decode` adapter at Java's `MessageDecoder.clientDecode`
boundary. A separate inbound regression checks both IDs after a full encoded
record crosses the processor/owned task/listener path.

Callback message/queue topics retain null versus empty input. The retry-topic
property is read for every message, preserving Java `getProperty`'s observable
initialization of a null properties map. The ten added raw/client ID and topic
cases compare IDs, property-map presence, callback fields and offset isolation.
Fixture checkpoint SHA-256:
`c7cda96e0bf61f51c9e848c7290859382964c01cd2c4db52b34e8fed2eb00c76`.

## Offset reset

Java `resetOffset(String, String, Map<MessageQueue, Long>)` maps to async
`ClientRuntime::reset_offset(Option<&str>, Option<&str>,
Option<ConsumerOffsetTable>)`. `ConsumerOffsetTable` is a shared nullable-key,
nullable-offset map; it retains the caller alias during the drain wait. The
Push owner supplies the operation; other/missing consumer types ignore both
nullable inputs. A null group still fails lookup.

| Java branch / boundary | Rust mapping and evidence |
| --- | --- |
| Runtime monitor serializes reset, producer/consumer registration/removal and start | Private async monitor shared by those methods; Core test blocks a second reset, null registration and start while permitting raw map mutation and unsynchronized admin registration |
| Shutdown checks user tables before its synchronized block | An active-user shutdown returns false during the blocked reset; terminal shutdown uses the monitor |
| Suspend before traversal; resume in finally | Normal/error returns await the target owner's rebalance after restoring pause/scheduler state; a cancellation-only guard restores scheduling if the Rust future is dropped early |
| First traversal uses topic equality then `containsKey` | Null-topic/table branches and null-valued matching keys match Java; matching queues are dropped/cleared even when the offset value is null |
| Concurrent drain waits ten seconds; orderly reset waits on each queue's consume lock | Paused-time tests exercise the full drain and existing orderly lock windows; Java oracle uses its explicitly ignored interrupt path to bound wall-clock time |
| Second traversal reads the current map and current queue table | Live map value changes and queue insertion are checked against direct Java execution, without reusing first-pass values |
| Per-queue persistence failure is logged and does not abort sibling queues | Java/Rust compare queue/value state, persisted values and update→persist→remove order within each queue; the final rebalance can truncate a failed queue whose topic is no longer subscribed |
| Missing/null offsets skip reset's update/removal stage | Null-key, null-value, absent-assignment and empty-input cases; finally still runs rebalance |
| Store selected at each virtual-call boundary | Reset rereads the live field for update, persist and remove; a Java/Rust store-handoff case changes the field in both update and persist callbacks |

The same public runtime entrypoint now owns inbound reset and owner timestamp
reset calls. The fifteen Java cases invoke the real runtime and rebalance
removal path, including the real consumer resume and its target rebalance.
Events from distinct
ConcurrentHashMap queues are grouped by queue ID; ordering within each queue,
all values, dropped/presence states and errors remain compared.

The initial reset fixture checkpoint SHA-256 is
`594e1e51bfd5f0d3598d309ae8932b9c596a83969ab6c6f8b43f0ac347daaf06`.
`runtime_reset_matches_java_live_maps_nulls_and_per_queue_failures` compares
all fifteen cases. The reset filter also runs existing callback/pull fencing,
unassigned/null-value, cache-clear and orderly-removal regressions.

The additional `resetStoreHandoff` case records update in the first store,
persist in the second and remove in the third, with final values `[7, 60, null]`
and a persisted value of 60. It prevents a single captured store from silently
changing the Java setter's activation timing. The Rust regression also verifies
that the custom removal callback can inspect the process-queue map before its
entry is removed. No user store callback runs under that map's write lock.
Orderly persistence exceptions leave the queue registered; unsuccessful lock
or unlock attempts retain Java's separate local-removal behavior.

The final extended fixture SHA-256 is
`ed123fb25e329d0166329dea123add868ac9846ffc251e2469970493691b9665`.
Live Java map/queue changes wait for the observed drain-sleep stack frame after
the clear latch, then mutate and interrupt the worker; first-pass iterator
prefetch timing is not used as synchronization.

The private monitor guard is shared by the caller and its owned control
command. Cancelling a caller waiting for an unregister response cannot let a
reset overtake the still-running RPC. An undelivered registration reply carries
that guard through identity cleanup, including time waiting for its raw-map
key gate. Normal caller post-processing also retains admission. These ownership
rules are checked by the scripted Broker cancellation case in `lifecycle` and
`undelivered_registration_retains_monitor_until_identity_cleanup_finishes`.
The monitor/envelope/guard are private transport-runtime machinery.

## Audit boundary

The additional `unsubscribedTruncation` Java probe invokes the real
`doRebalance` after a nullable reset leaves an unsubscribed queue dropped.
Java removes that queue from the table without persisting or removing its
offset. Rust now performs the same separate truncation, cancelling the native
pull assignment while retaining the offset. The comparison failed with a lost
offset before the fix and checks table presence, dropped state, retained value,
persisted value and store callback events. Fixture checkpoint SHA-256:
`2935595b5c94dc9496d342d004f3869cf3d66d5e5ca042c6ad445bae1ec712b4`.

The `resume` fixture also checks a suspended invocation followed by an already
unpaused invocation, with a new unsubscribed queue inserted before each call.
Both calls complete the selected owner's round before returning, while leaving
a sibling owner's queue alone. Rust compares both rounds using two consumers
sharing one runtime. A private weak owner reference preserves the existing
`resume(&self)` API without looking the owner up through replaceable raw group
registrations or introducing a strong reference cycle. No state lock is held
across the rebalance callbacks. Reset's cancellation guard is disarmed before
that round, so a callback's new suspend remains effective.
An additional native callback regression checks that state inspection is
available during the round and that a new pause survives finally's return.
The real-resume fixture checkpoint SHA-256 is
`ee07499b6b8eb58ea9cefc013592013a2c378e97709f4fd0096cd2ba81ec2eea`.

The first two `rebalanceRetention` cases keep the topic subscribed after a
null-value reset. A missing route or an unchanged broadcast assignment retains
the dropped queue and its in-memory offset, with no persist/remove events.
The Java probe explicitly installs its rebalance model and runtime to exercise
those branches. The Rust comparison initially lost the retained offset on the
missing-route case. Cleanup now requires an authoritative assignment excluding
the queue; the dropped flag alone does not authorize removal. Fixture SHA-256:
`0ed55128b1905a5d0c451ca93c0fbb33b6c5089f2a0a8c5e188770354293a04b`.

The third retention case exercises Java's `ProcessQueue.isPullExpired` path
with the topic subscribed and queue still assigned. Java removes the expired
queue, clears its dirty offset before trying to read a replacement start
offset, and leaves the table entry absent when the fixture offset-store read
fails. Rust now applies the same strict `elapsed > pullMaxIdleTime` condition
to push and POP process queues, using Java-compatible signed-millisecond
subtraction. The shared Java property is
`rocketmq.client.pull.pullMaxIdleTime`, default `120000`; Rust reads the same
dotted property name from the process environment when a consumer starts and
reports invalid Java-long values as a startup error. The Java fixture records
the default and expired-assignment lifecycle; the runtime comparison checks
queue presence, dropped state, offsets and callback order. Rust unit coverage
also checks the strict boundary, signed wrapping and Java-long parsing.
Fixture SHA-256:
`3d7af953b38b28a742a0d3eeb2671e8484066203f06ade2c829e2485ead1de4f`.

The `popRebalance` fixture invokes the Java assignment update for an expired
assigned POP queue, a dropped queue that remains assigned, and an authoritative
empty assignment. Rust compares table presence, old-generation dropped state
and identity replacement, then also scripts a failed assignment lookup to
verify the current queue and worker are retained without authority. This covers
the recorded POP expiry/retention branches; other POP assignment branches and
the public Rebalance/ProcessQueue API audit remain open. Fixture SHA-256 is the
same `3d7af953b38b28a742a0d3eeb2671e8484066203f06ade2c829e2485ead1de4f`.

The nested ProcessQueue review found a missing concurrent-consume expiry flow.
Java `ConsumeMessageConcurrentlyService` records `CONSUME_START_TIME`, then
runs `cleanExpiredMsg` after one `consumeTimeout` period and at that interval.
Rust now records the same property for listener messages, tracks it on the
queue-owned entry, and runs an owned concurrent-service timer. The sweep checks
only the oldest cached message, uses Java's strict signed-millisecond timeout,
sends back at delay level 3, removes the head only after successful send-back,
caps attempts at 16 per queue, and skips orderly consume. Java invokes the
actual `ProcessQueue.cleanExpiredMsg` with scripted outcomes; Rust compares
success, repeated send-back failure and orderly skip, plus the 16-attempt cap,
strict boundary, late listener completion and initial timer delay. Fixture
SHA-256:
`76b9bdfdd20d3f6cbc1b73f4b12055c484955f39cfde5884e0d6688e38fd6f79`.

The pinned nested public API denominator and its current owner/queue mappings
are recorded in the [Rebalance and ProcessQueue API audit](PHASE_18_REBALANCE_PROCESS_QUEUE_API.md).
Rust now exposes both queue types, direct Java-shaped queue behavior, mutable
message-tree/counter handles and live Push/Pull process-queue tables. Pull
assignment reconciliation updates its process table. Pull POP behavior now
matches Java's no-queue/no-dispatch implementation and is fixture checked.
Remaining gaps are the inherited rebalance call/settings correspondence;
broad P18-V01 remains open.

The direct Running Info, direct-consume and reset runtime rows above are now
implemented with scoped offline evidence. Their existence does not prove
completion of the complete Java/Rust public API/settings/control-flow audit.
Broad P18-V01 remains open and live evidence remains `PENDING_PHASE_19`.
