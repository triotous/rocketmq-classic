# ADR-0080: Serialize FactoryTable remapping per key

- Status: Accepted
- Date: 2026-10-10

## Context

`MQClientManager.getFactoryTable()` exposes a Java `ConcurrentHashMap`. Its `compute`, `computeIfPresent`, `computeIfAbsent`, `merge`, and `replaceAll` remapping callbacks run as part of the mapping operation. A same-key writer waits for the active callback, and a callback does not run again because of that writer.

Rust `FactoryTable` originally read a value, called the closure without a per-key guard, and then conditionally wrote it. A same-key write could win between those steps, causing `computeIfPresent`, `compute`, `merge`, or `replaceAll` to call user code a second time. The direct Java race oracle records callback count, writer blocking, return value, and final mapping. The native regression failed before the correction because its writer completed during the callback.

## Decision

Use weakly retained asynchronous mutation gates keyed by the Java-compatible table key. `FactoryTable` remapping holds one gate across the read, callback, and write. Ordinary asynchronous table mutations, same-ID runtime creation, and asynchronous ID removal use that gate as well. Gates for unrelated keys do not block one another; stale weak entries are pruned periodically so arbitrary alias keys are not retained indefinitely.

Callbacks run without holding the manager's entries mutex, so ordinary reads can inspect the current table while a remapping callback runs. `replaceAll` still visits a traversal-start key snapshot and serializes each key separately; it is not an all-map transaction.

## Consequences

FactoryTable remapping callbacks are invoked once per eligible operation and same-key table writers wait until the callback's mapping result is published. Unrelated keys remain independently mutable. The callback test uses the actual manager factory table and the pinned Java fixture. This decision covers `FactoryTable`; equivalent callback concurrency in producer/consumer registry tables and the raw route table remains a separate audit item. Synchronous identity-safe manager removal remains a separate lifecycle API and is not described as a FactoryTable map operation here. Phase 6 source/API/input reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Pinned Java 5.5.1 oracle: `RuntimeConfigFixtures.managerTable`; `ConcurrentHashMap.compute` holds its same-key writer until the remapping callback returns and invokes the callback once.
- Fixture: `fixtures/runtime/client-config.json`, SHA-256 `87e5b80ab12add8c386a13a4bc6fbbc738a780140724db3fca15c145f038dba6`.
- Native regression: `concurrent_factory_table_remapping_serializes_same_key_callback_like_java`; it failed before the implementation because the writer finished during the callback, then passed with per-key serialization.
- Validation: `cargo test --workspace --all-targets --locked`; `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo fmt --all -- --check`; `git diff --check`; pinned offline `mvn -o -q -f compat/java-reference/pom.xml -Dmaven.repo.local=/private/tmp/rocketmq-classic-m2-repository verify`; differential comparator self-test; manifest validation (22 scenarios).
