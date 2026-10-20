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
| Suspend before traversal; resume in finally | Private resume guard restores pause/scheduler state on success, error and Rust future cancellation; cancellation regression also verifies monitor release |
| First traversal uses topic equality then `containsKey` | Null-topic/table branches and null-valued matching keys match Java; matching queues are dropped/cleared even when the offset value is null |
| Concurrent drain waits ten seconds; orderly reset waits on each queue's consume lock | Paused-time tests exercise the full drain and existing orderly lock windows; Java oracle uses its explicitly ignored interrupt path to bound wall-clock time |
| Second traversal reads the current map and current queue table | Live map value changes and queue insertion are checked against direct Java execution, without reusing first-pass values |
| Per-queue persistence failure is logged and does not abort sibling queues | Java/Rust compare retained queue/value state, persisted values and update→persist→remove order within each queue; a failed queue stays dropped and registered |
| Missing/null offsets do not update or remove a queue | Null-key, null-value, absent-assignment and empty-input cases |
| Store selected at each virtual-call boundary | Reset rereads the live field for update, persist and remove; a Java/Rust store-handoff case changes the field in both update and persist callbacks |

The same public runtime entrypoint now owns inbound reset and owner timestamp
reset calls. The fifteen Java cases invoke the real runtime and rebalance
removal path. The probe overrides consumer resume only to record the finally
action without launching an unrelated assignment round. Events from distinct
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

The direct Running Info, direct-consume and reset runtime rows above are now
implemented with scoped offline evidence. Their existence does not prove
completion of the complete Java/Rust public API/settings/control-flow audit.
Broad P18-V01 remains open and live evidence remains `PENDING_PHASE_19`.
