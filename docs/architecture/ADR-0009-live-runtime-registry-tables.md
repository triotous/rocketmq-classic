# ADR-0009: Live producer and consumer registry tables

Status: implemented for the tested Phase 6 map primitives; whole-phase acceptance remains open.

## Source and native ownership

Pinned Java client 5.5.1 `MQClientInstance.getProducerTable/getConsumerTable` return actual ConcurrentMaps. Retained views observe replacements and clears; primitive mutations affect selectors without unregister RPCs. `bf4a859` supplies `ClientRuntime::producer_table/consumer_table` and cloneable `ProducerTable/ConsumerTable`, not detached snapshots presented as live maps.

Handles share the registry but hold a weak runtime reference. Entries follow existing plan-required weak ownership: callers retain owner Arcs; owner drop leaves a binding, while get returns None and snapshot omits expired owners. len/contains_key count bindings. snapshot is explicitly a detached owned copy. Neither handle retains transport/control resources or an owner/runtime cycle. This is a native ownership boundary, not Java strong-reference/GC equivalence.

## Mutations and lifecycle

Provide put, put_if_absent, replace, identity-conditional replace/remove, Java-equality-conditional replace/remove and clear. The Rust owner traits expose an optional stable `map_value_equality_key`; `replace_if_equal/remove_if_equal` compare the expected key with the key captured when the current value entered the table. Owners without a key use Object identity, matching RocketMQ's concrete implementations. Keys are captured before registry locks, so no owner code runs under the registry mutex. The Java 5.5.1 custom-equality oracle covers equal producer and consumer proxies. Mutable equality policies and additional ConcurrentMap default operations still require reconciliation.

Every successful put/replacement creates a fresh private lease identity, even for the same owner. Old unregister/rollback leases cannot remove that binding. Map writes neither issue Broker unregister RPCs nor stop owners.

Existing control actors queue writes behind pending unregister transactions; no new actor is created. Closed cancelled replies are skipped before mutation. Before actor creation and after terminal closure, writes operate directly without resource revival. During Starting, addressing callbacks can await a table write on the startup actor itself; direct atomic mutation avoids self-deadlock, and no unregister transaction is active in that state. Reads do not wait for Broker RPCs. Synchronous guards never cross await; owned mutation arguments/returned owners are released outside the registry guard.

Source unregisterProducer/Consumer(group) unconditionally remove the current mapping and invoke best-effort Broker unregister even for absent keys. `3ccaf54` supplies separate public unregister_producer_by_group/unregister_consumer_by_group methods, serialized by the existing control actor. Their bool reports binding removal, not Broker success. Cancelled queued calls do not apply; in-flight cleanup finishes. Terminal retained bindings can be removed without resource revival. Lifecycle/PendingLease cleanup still uses identity leases exclusively; raw map remove still sends no RPCs.

## Executed evidence

`tests/registry_tables.rs` matches nineteen actual Java map outcomes per table (38 total), retained-view/selector freshness, duplicate registration after direct replacement, stale lease safety, weak owner/runtime release and access after runtime drop. Scripted NameServer/Broker responses hold unregister, observe pending writes/nonblocking reads, cancel a queued write, then verify replacement, map-only clear and shutdown EOF. Private tests exercise64 competing put-if-absent calls, detached snapshots, no-worker writes, owner Drop reentry and paused startup addressing reentry.

Observed exit0 gates: full workspace tests (core71 passed/1 lower-review ignored, registry3, runtime7), all-target/all-feature Clippy with warnings denied, offline Maven verification. Logs: `/tmp/p6-live-registry-{workspace,clippy,java-verify}-final.log`. Comparator self-test/manifest22, format/diff checks pass. Config fixture unchanged: SHA-256 `64d523c1bfeac29e549de9b09c9204914a7099a2ac8a4a4f652747a4145fff0f`.

No live Broker/Docker, lower nullable migration, new setting/dependency/worker or plan change. ADR-0007/0008 and the ignored failing nullable-Broker regression remain open; bounded map tests do not close them.

Custom equality update `4616808`/`b9f5bb6`: actual Java 5.5.1 proxy owners with equal keys allow conditional replace/remove for both producer and consumer tables. Rust exposes the optional stable key and matching `_if_equal` operations; direct `_if_same` and lifecycle lease cleanup remain pointer-identity operations. Client-config fixture SHA-256 `daeb2fde29ccf911064c779a40a5907645c61764f2d0de6960f13e9333a676fe`; full workspace, exact all-feature Clippy, Maven, comparator and manifest checks pass. Arbitrary mutable equality and other ConcurrentMap default operations remain open.

Subsequent group-unregister gates: `/tmp/p6-group-unregister-workspace-final.log`, `/tmp/p6-group-unregister-clippy-final.log` and `/tmp/p6-group-unregister-java-verify.log` all exit0; core71pass/1lower-review-ignored, registry4/runtime7. New integration matches all four invoked Java consumer method outcomes in addition to38 map outcomes. Scripted peers verify both group fields, absent-key RPC, best-effort rejection, replacement ordering, cancelled queued unregister, expired owner binding removal and stopped-map removal/EOF. Producer unregister is verified against pinned source plus wire frames, not a claim of Java proxy producer registration. Full source/input/default-map reconciliation remains open.
