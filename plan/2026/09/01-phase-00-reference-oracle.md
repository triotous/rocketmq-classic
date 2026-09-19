# Plan: Phase 0 — Freeze the reference and test oracle

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 0; execution index: [`README.md`](README.md).
> Translation: [`01-phase-00-reference-oracle.zh-tw.md`](01-phase-00-reference-oracle.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## 1. Goal and scope

Build the executable Java oracle, reproducible local Broker environment and evidence inventory before any Rust protocol implementation. This phase creates infrastructure, not a Rust client.

## 2. Prerequisites and current-state analysis

The repository currently contains only the master plan and SVG diagrams: no Cargo workspace, Java harness, fixtures or tests exist. Treat the release/tag/commit in the master plan as the requested baseline, not as independently verified evidence.

- Read/complete first: [Execution index](README.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `RemotingCommand`
- `RocketMQSerializable`
- `MQClientAPIImpl`
- `MQClientInstance`
- `DefaultMQProducerImpl`
- `PullAPIWrapper`
- `ProcessQueue`
- `RebalanceImpl`
- `RemoteBrokerOffsetStore`
- `LocalFileOffsetStore`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
compat/java-reference/pom.xml
compat/java-reference/README.md
compat/java-reference/src/main/java/compat/Main.java
fixtures/README.md
fixtures/remoting/
fixtures/messages/
integration-tests/compose.yaml
integration-tests/setup.sh
integration-tests/README.md
tests/differential/scenarios.yaml
tests/differential/compare.py
docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md
docs/compatibility/REQUEST_CODES.md
docs/compatibility/KNOWN_DIFFERENCES.md
```

The oracle CLI has `fixtures --output <dir>`, `decode --input <file>`, `scenario --id <id> --output <file>` and `self-test` commands. Invoke it with Maven exec and `compat.Main`; one result JSON per scenario. Store `scenarios.yaml` as a JSON-compatible YAML document so the Python standard library can read it without a YAML dependency. Each entry records ID, required profile, owning phase, controlled inputs and the exact normalization rules. The Python comparator compares objects without key-order sensitivity and preserves array order unless that specific field is explicitly a set. Never normalize away error codes, queue assignment, retry counts, ordering or offset relationships.

## 4. Ordered implementation tasks

### 4.1 P0-00 — Verify the baseline before generating evidence

Resolve `rocketmq-all-5.5.1` to its peeled commit and compare with `efa1be59d6fc747ad220f036c86030f05f0419f9`. Resolve Maven `org.apache.rocketmq:rocketmq-client:5.5.1` and matching source artifacts. Record artifact SHA-256, tag/commit, JDK/Maven versions and source paths in the harness README. Do not substitute `develop`, another artifact version or an unverified Docker image if retrieval fails; block protocol work for owner review.

### 4.2 P0-01 — Create evidence and scope inventories

Create the master-plan matrix columns verbatim, including source/Rust mappings, request code, golden/unit/integration/differential identifiers and status values. Inventory public client APIs and every numeric RequestCode/ResponseCode from the pinned source; distinguish a registered constant from an implemented operation. Add source class/method links and planned phase ownership. Start all implementation rows at TODO; include transaction, reply, Lite/POP, admin, namespace, hooks and security rows. Record optional-scope decisions as pending, not COMPATIBLE.

### 4.3 P0-02 — Implement protocol fixture I/O

Use Java public APIs or a narrowly documented same-package adapter where the release requires it. Generate JSON and ROCKETMQ commands with explicit opaque, timestamps and properties where possible. Write raw full frames plus decoded semantic JSON, body length and body SHA-256. The decoder accepts both serializations and fails on malformed input. Add self-tests to Maven verify; do not rely on disabled Java assertions.

### 4.4 P0-03 — Create deterministic scenarios and comparator

Register all Phase 0 scenario IDs from the master plan, plus heartbeat, offset update and negative header cases. Implement route/send/pull/offset/lock scenarios first; use isolated topics/groups and explicit initial offsets. Pure allocation and header cases run without a Broker. Add comparator self-tests proving that nondeterministic IDs can be normalized but changed status or queue order still fails. Future scenario IDs may be registered but must remain unimplemented rather than reporting success.

### 4.5 P0-04 — Provide the local cluster and deterministic setup

Create a basic NameServer + Broker Compose profile from a verified 5.5.1 distribution/image; record checksums or image digest and mapped/advertised addresses. Add readiness checks using a real route/admin operation, not a fixed sleep. setup.sh creates fixed queues/topics/groups idempotently in the isolated test cluster and documents reset/cleanup without touching external data. Add a two-Broker profile and local TLS CA profile; add an auth profile only after the pinned auth path is identified. Never disable certificate validation for the TLS profile.

### 4.6 P0-05 — Check in reproducible evidence

Use Appendix B fixture names. Every .bin has a .json sibling with source version, scenario, serialization, request code, normalized fields, payload SHA-256 and creation command; preserve raw captures separately from normalized results. Require route, heartbeat, send, pull, offset query/update and lock/unlock fixture families now. Later phases extend this inventory before coding their feature. Wire a dependency/version check into Maven verify and document fixture regeneration and Java licensing/NOTICE provenance.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Shared commands, evidence rules and integration prerequisites are in the execution index.

- [ ] **P0-V01** — Run fixture generation twice; metadata hashes match raw files and normalized semantic results match.
- [ ] **P0-V02** — Java decodes both codec families it produced; malformed fixtures fail; comparator self-test detects meaningful differences.
- [ ] **P0-V03** — Basic local cluster: Java producer/consumer, route, heartbeat, pull, offset and lock operations produce nonempty machine-readable evidence.
- [ ] **P0-V04** — Changing the Maven client version causes verification to fail; no scenario with missing prerequisites reports PASS.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
mvn -f compat/java-reference/pom.xml verify
mvn -f compat/java-reference/pom.xml exec:java -Dexec.mainClass=compat.Main -Dexec.args="fixtures --output target/java-fixtures"
python3 tests/differential/compare.py --self-test
docker compose -f integration-tests/compose.yaml up -d
bash integration-tests/setup.sh
```

## 6. Acceptance and handoff

Commit the verified baseline record, fixture manifest, working local cluster recipe and TODO matrix together. Phase 1 may begin only after the required oracle evidence is reproducible; deployment-only tests are collected for Phase 18, not used as a substitute for this local gate.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 1](02-phase-01-common-protocol-models.md).
