# ADR-0080: Serialize FactoryTable remapping per key

- Status: Accepted
- Date: 2026-10-10

## Context

`MQClientManager.getFactoryTable()` exposes a Java `ConcurrentHashMap`. Its `compute`, `computeIfPresent`, `computeIfAbsent`, `merge`, and `replaceAll` remapping callbacks run as part of the mapping operation. A same-key writer waits for the active callback, and a callback does not run again because of that writer.

Rust `FactoryTable` originally read a value, called the closure without a per-key guard, and then conditionally wrote it. A same-key write could win between those steps, causing `computeIfPresent`, `compute`, `merge`, or `replaceAll` to call user code a second time. The direct Java race oracle records callback count, writer blocking, return value, and final mapping. The native regression failed before the correction because its writer completed during the callback.

## Decision

Use 256 fixed mutation shards selected from the Java-compatible table key. Each shard has an async gate and a synchronous, reentrant exclusion gate. `FactoryTable` mutations acquire both gates; remapping holds them across the read, callback, and write. Same-ID runtime creation and asynchronous ID removal use the async gate. Synchronous identity-safe manager removal uses the synchronous gate, so it waits for a remapping callback on the same shard. Hash-colliding keys share a shard and serialize, while keys mapped to different shards proceed independently. Fixed shards avoid retaining arbitrary alias keys.

Callbacks run without holding the manager's entries mutex, so ordinary reads can inspect the current table while a remapping callback runs. The mutation gates stay held through the callback. Their synchronous state lock is held only while updating the gate state, not while user code runs, so callback panic does not poison future operations. `replaceAll` still visits a traversal-start key snapshot and serializes each key separately; it is not an all-map transaction.

## Consequences

FactoryTable remapping callbacks are invoked once per eligible operation; same-shard table writers and synchronous identity-safe manager removal wait until the callback's mapping result is published. Ordinary reads continue during the callback. The callback tests use the actual manager factory table and pinned Java fixture, including identity-safe removal contention and panic recovery. This decision covers `FactoryTable`; equivalent callback concurrency in producer/consumer registry tables and the raw route table remains a separate audit item. Phase 6 source/API/input reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Pinned Java 5.5.1 oracle: `RuntimeConfigFixtures.managerTable`; `ConcurrentHashMap.compute` holds its same-key writer until the remapping callback returns and invokes the callback once.
- Fixture: `fixtures/runtime/client-config.json`, SHA-256 `7c84370c42228a4f3676fde47af11656a3f20ab545d036b79febf13338b9909a`.
- Native regressions: `concurrent_factory_table_remapping_serializes_same_key_callback_like_java` and `identity_safe_manager_removal_waits_for_factory_table_remapping`; both failed before the corresponding gate behavior was added. `remapping_callback_panic_does_not_poison_factory_table_mutations` verifies later same-key updates remain available after unwinding.
- Validation: focused `cargo test -p rocketmq-classic-core --test manager_table --locked`; `cargo test --workspace --all-targets --locked`; strict `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo fmt --all -- --check`; `git diff --check`; pinned offline Maven verify (22 scenarios); differential comparator self-test; and manifest validation (22 scenarios) all pass.
