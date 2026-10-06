# ADR-0009: Live producer and consumer registry tables

Status: implemented for the tested Phase 6 map primitives; whole-phase acceptance remains open.

## Source and native ownership

Pinned Java client 5.5.1 `MQClientInstance.getProducerTable/getConsumerTable` return actual ConcurrentMaps. Retained views observe replacements and clears; primitive mutations affect selectors without unregister RPCs. `bf4a859` supplies `ClientRuntime::producer_table/consumer_table` and cloneable `ProducerTable/ConsumerTable`, not detached snapshots presented as live maps.

Handles share the registry but hold a weak runtime reference. Entries follow existing plan-required weak ownership: callers retain owner Arcs; owner drop leaves a binding, while get returns None and snapshot omits expired owners. len/contains_key count bindings. snapshot is explicitly a detached owned copy. Neither handle retains transport/control resources or an owner/runtime cycle. This is a native ownership boundary, not Java strong-reference/GC equivalence.

## Mutations and lifecycle

Provide put, put_if_absent, replace, replace_if_same, remove, remove_if_same and clear. Atomic mutations use the registry lock without await or owner callbacks under it. Conditional comparisons use Arc identity, matching the invoked identity-equality Java values; arbitrary custom Java equals and additional ConcurrentMap default operations are not proven by these primitives.

Every successful put/replacement creates a fresh private lease identity, even for the same owner. Old unregister/rollback leases cannot remove that binding. Map writes neither issue Broker unregister RPCs nor stop owners.

Existing control actors queue writes behind pending unregister transactions; no new actor is created. Closed cancelled replies are skipped before mutation. Before actor creation and after terminal closure, writes operate directly without resource revival. During Starting, addressing callbacks can await a table write on the startup actor itself; direct atomic mutation avoids self-deadlock, and no unregister transaction is active in that state. Reads do not wait for Broker RPCs. Synchronous guards never cross await; owned mutation arguments/returned owners are released outside the registry guard.

Source unregisterProducer/Consumer(group) unconditionally remove the current mapping and invoke best-effort Broker unregister even for absent keys. Native identity-lease cleanup intentionally does not supply that separate public capability. Reconcile this remaining API gap independently; do not weaken lease cleanup or claim raw map remove sends RPCs.

## Executed evidence

`tests/registry_tables.rs` matches nineteen actual Java map outcomes per table (38 total), retained-view/selector freshness, duplicate registration after direct replacement, stale lease safety, weak owner/runtime release and access after runtime drop. Scripted NameServer/Broker responses hold unregister, observe pending writes/nonblocking reads, cancel a queued write, then verify replacement, map-only clear and shutdown EOF. Private tests exercise64 competing put-if-absent calls, detached snapshots, no-worker writes, owner Drop reentry and paused startup addressing reentry.

Observed exit0 gates: full workspace tests (core71 passed/1 lower-review ignored, registry3, runtime7), all-target/all-feature Clippy with warnings denied, offline Maven verification. Logs: `/tmp/p6-live-registry-{workspace,clippy,java-verify}-final.log`. Comparator self-test/manifest22, format/diff checks pass. Config fixture unchanged: SHA-256 `64d523c1bfeac29e549de9b09c9204914a7099a2ac8a4a4f652747a4145fff0f`.

No live Broker/Docker, lower nullable migration, new setting/dependency/worker or plan change. ADR-0007/0008 and the ignored failing nullable-Broker regression remain open; bounded map tests do not close them.
