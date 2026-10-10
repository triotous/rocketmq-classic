# ADR-0080: Serialize FactoryTable remapping per key

- Status: Accepted
- Date: 2026-10-10

## Context

`MQClientManager.getFactoryTable()` exposes a Java `ConcurrentHashMap`. Its `compute`, `computeIfPresent`, `computeIfAbsent`, `merge`, and `replaceAll` remapping callbacks run as part of the mapping operation. A same-key writer waits for the active callback, and a callback does not run again because of that writer.

Rust `FactoryTable` originally read a value, called the closure without a per-key guard, and then conditionally wrote it. A same-key write could win between those steps, causing `computeIfPresent`, `compute`, `merge`, or `replaceAll` to call user code a second time. The direct Java race oracle records callback count, writer blocking, return value, and final mapping. The native regression failed before the correction because its writer completed during the callback.

## Decision

Use weakly retained mutation gates keyed by the exact Java-compatible table key. Each gate has async and synchronous, reentrant exclusion. `FactoryTable` mutations acquire both gates; remapping holds them across the read, callback, and write. Same-ID runtime creation and asynchronous ID removal use the async gate. Synchronous identity-safe manager removal looks up the same key gate and uses its synchronous half. Different keys do not share a gate; expired weak entries are pruned when the registry grows, so arbitrary aliases are not retained indefinitely.

Callbacks run without holding the manager's entries mutex, so ordinary reads can inspect the current table while a remapping callback runs. The mutation gates stay held through the callback. Their synchronous state lock is held only while updating the gate state, not while user code runs, so callback panic does not poison future operations. `replaceAll` still visits a traversal-start key snapshot and serializes each key separately; it is not an all-map transaction.

## Consequences

FactoryTable remapping callbacks are invoked once per eligible operation; same-key table writers and synchronous identity-safe manager removal wait until the callback's mapping result is published. Ordinary reads continue during the callback. The callback tests use the actual manager factory table and pinned Java fixture, including identity-safe removal contention, read visibility and panic recovery. The manager rendezvous test also proves two distinct keys that collided under the prior shard mask initialize concurrently. This decision covers `FactoryTable`; equivalent callback concurrency in producer/consumer registry tables and the raw route table remains a separate audit item. Phase 6 source/API/input reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Pinned Java 5.5.1 oracle: `RuntimeConfigFixtures.managerTable`; `ConcurrentHashMap.compute` holds its same-key writer until the remapping callback returns, invokes the callback once, and allows a read inside that callback to observe the old mapping.
- Fixture: `fixtures/runtime/client-config.json`, SHA-256 `3d75b063598a9fb1d0fc5300af80fdfff265c05ebf813e675bce9276fa4a0e4b`.
- Native regressions: `concurrent_factory_table_remapping_serializes_same_key_callback_like_java`, `identity_safe_manager_removal_waits_for_factory_table_remapping`, and callback read coverage in the first test. The two contention tests failed before their corresponding gate behavior was added. `remapping_callback_panic_does_not_poison_factory_table_mutations` verifies later same-key updates remain available after unwinding. `unrelated_keys_construct_concurrently_and_same_key_constructs_once` chooses two IDs that collide under the former 256-shard mask and proves they still initialize concurrently.
- Validation: the removal-fix checkpoint passed the full `cargo test --workspace --all-targets --locked` suite and strict all-target/all-feature Clippy. The read-oracle extension passes the full `manager_table` integration target, strict workspace Clippy, `cargo fmt --all -- --check`, `git diff --check`, pinned offline Maven verify (22 scenarios), differential comparator self-test, and manifest validation (22 scenarios).
