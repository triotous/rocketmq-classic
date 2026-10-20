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

## Remaining rows

Direct runtime `resetOffset` and `consumeMessageDirectly` still require their
public entrypoints and source/fixture reconciliation. Existing inbound/owner
handlers are supporting implementation, not proof of those public APIs.
Broad P18-V01 remains open and live evidence remains `PENDING_PHASE_19`.
