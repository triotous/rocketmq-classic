# ADR-0081: Serialize registry table remapping per key

- Status: Accepted
- Date: 2026-10-10

## Context

`MQClientInstance.getProducerTable()` and `getConsumerTable()` expose Java `ConcurrentMap` instances backed by `ConcurrentHashMap`. For an eligible `computeIfAbsent`, `computeIfPresent`, `compute`, `merge`, or `replaceAll` operation, a conflicting same-key writer waits for the remapping callback. The callback runs once, and a read of the key inside the callback can still observe the mapping that existed before the remap.

Rust registry remapping originally read a value, ran the callback, then sent a conditional mutation. A same-key write could land between those steps and make the callback run again. The native public table also shares its map with registration and unregister lifecycle operations, which are ordered by the runtime control actor.

## Decision

Use weakly retained async gates indexed by `(table kind, exact key)`. Producer and consumer maps have separate key spaces. All public table writes, remapping callbacks, producer/consumer registration and unregister operations, and pending registration cleanup use the same key gate.

Remapping keeps the gate while it reads the mapping and runs the callback. Table mutations keep the gate until the control actor confirms the backing-map update. Registration and unregister commands carry their owned gate into the actor; the actor releases it immediately after changing the registry map, before any Broker unregister RPC. If an undelivered registration must be rolled back while its key is busy, cleanup waits outside the actor before it enqueues the unregister command. This keeps the actor free to apply the remapping mutation and avoids a same-key wait cycle.

Reads do not acquire the mutation gate or the registry entries mutex for the duration of a callback, so the callback can read the current mapping. Different table keys can proceed independently. Weak gate entries are pruned as the registry grows. `replaceAll` snapshots keys and remaps each still-present key under its own gate; it is not an all-map transaction. `clear` remains a direct map operation rather than an all-key transaction.

## Consequences

Each eligible registry remapping callback runs once. Same-key table writes and lifecycle changes wait until the callback's map update is applied. The actor continues to define mutation order against unregister work, and Broker network waits do not hold the table key gate.

The producer and consumer Java oracle runs a blocked `compute` callback with a concurrent same-key writer. It records that the writer waits, the callback runs once, the callback reads the old mapping, and the remapping result is observed by the writer before its value becomes current. Native integration tests compare those four outcomes for both table types. Other registry map behaviors remain covered by the existing invoked fixtures. Raw route table callback concurrency is a separate scope.

## Evidence

- Pinned Java 5.5.1 oracle: `RuntimeConfigFixtures.tableDefaultOperations`, invoked on both producer and consumer tables.
- Fixture: `fixtures/runtime/client-config.json`, SHA-256 `807ad17c66ba5e775e141630744209ac129be897977879d1af616d652979fe6b`.
- Native regression: `registry_default_map_operations_match_invoked_java` in `crates/rocketmq-classic-core/tests/registry_tables.rs`.
- Validation: full `cargo test --workspace --all-targets --locked`, strict workspace/all-target/all-feature Clippy, focused registry table tests, pinned offline Maven verify (22 scenarios), comparator self-test and manifest validation (22 scenarios), formatting, and diff checks pass. Whole Phase 6 API/source/input/platform reconciliation remains open; P6-V04 remains `PENDING_PHASE_19`.
