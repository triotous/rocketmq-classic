# Plan: Phase 18 — Compatibility, performance and release gate

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 18; execution index: [`README.md`](README.md).
> Translation: [`19-phase-18-compatibility-release-gate.zh-tw.md`](19-phase-18-compatibility-release-gate.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## 1. Goal and scope

Produce auditable evidence for every advertised compatibility claim, exercise performance/leak boundaries and prepare a real release without automatically publishing or tagging it.

## 2. Prerequisites and current-state analysis

Phases 0–17 are accepted, including explicit owner decisions for optional APIs/security/trace/admin. The master baseline is rechecked against Phase 0 artifacts; all prior tests remain mandatory, not replaced by one end-to-end smoke test.

- Read/complete first: [Phase 17](18-phase-17-admin-query.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `Master-plan sections 18–30 and final acceptance checklist`
- `All pinned Java source and fixture provenance recorded by Phases 0–17`
- `5.5.1 request/reply, runtime-construction, blank-address, optimized-header encoding and batch-validation regressions`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md
docs/compatibility/REQUEST_CODES.md
docs/compatibility/KNOWN_DIFFERENCES.md
tests/differential/run.sh
tests/stress/
fuzz/
integration-tests/README.md
docs/release-checklist.md
docs/owner-validation.md
README.md
Cargo.toml
crates/*/Cargo.toml
```

A supported matrix row must link actual source, fixture/unit/integration/differential evidence as applicable; N/A requires a reason. TODO/PARTIAL/unapproved gaps cannot be relabeled COMPATIBLE to pass release. Intentional differences require owner approval and user-visible impact. Store machine-readable results with client/reference versions, source commit, profile, seed and normalization rules. Release gates fail on missing/skipped required suites. Performance results are measured baselines, not invented throughput promises.

## 4. Ordered implementation tasks

### 4.1 P18-01 (18.1 / 18.2) — Close the evidence matrix and execute every layer

Audit API/class/request-code coverage and source provenance, including aliases and intentionally unsupported operations. Run pure unit, golden cross-codec, local Broker integration and Java/Rust differential suites. For scenarios with generated IDs/offsets, normalize only documented nondeterminism while preserving equality relationships, duplicate counts, queue ordering and commit behavior. Add negative comparator self-tests so broad normalization cannot hide regressions.

### 4.2 P18-02 (18.1 Layer E) — Run the full cross-language matrix

Run Java producer→Rust consumer, Rust producer→Java consumer, mixed clustering, mixed orderly, Rust transaction commit/rollback/check and both request/reply directions. Use one-Broker and two-Broker profiles with controlled topic/group reset between scenarios; verify delivery/offset/ownership invariants, not just process exit. Include approved Lite/POP/admin/security/trace profiles and explicitly reject unavailable prerequisites.

### 4.3 P18-03 (18.3) — Fuzz protocol trust boundaries

Seed fuzz targets with existing Remoting/private-header/ext-field/message/property fixtures and malformed variants. Use cargo-fuzz only as the isolated fuzz harness dependency if needed; no production dependency. Add deterministic bounded mutation tests to ordinary cargo test and document a reproducible fuzz-smoke command with fixed budget/corpus. Assert no panic, checked lengths, bounded decompression and preservation of raw unknown codes. Save every discovered crash as a small regression fixture before fixing it.

### 4.4 P18-04 (18.4 / 18.6) — Run deterministic race tests and stress/leak loops

Retain barrier/paused-time tests, then stress thousands of sends, near-deadline responses, reconnects, opaque wrap, concurrent runtime creation, start/stop races, slow listeners, rebalance churn, offset persistence and duplicate replies. Count owned tasks/connections/pending entries/queue work explicitly; memory usage alone is not proof. Compare baseline after repeated shutdown: every owner count returns to zero, or the documented shared-runtime baseline when another user remains. Add an integration stress target with deterministic seed and bounded duration.

### 4.5 P18-05 (18.5) — Measure before optimizing

Benchmark JSON/private headers, frame encode/decode, message decode, send construction, pending completion, route lookup and allocation separately in release mode. Reuse fixture inputs and record CPU/toolchain/configuration, input size, iterations and latency/allocation observations. A small harness=false bench with std::time and black_box is sufficient initially; no benchmark framework is required. Preserve outputs against the oracle. Only optimize a measured bottleneck and rerun differential tests; no arbitrary pass/fail throughput threshold until the owner provides one.

### 4.6 P18-06 — Make release CI enforce the actual scope

Wire fmt, clippy all targets/features, locked unit tests, Java version pin, golden cross-decode, one/two-Broker integration, local TLS/auth, differential, fuzz smoke, leak stress and facade examples into release-branch CI. Test default features and documented supported feature combinations, not just all-features. Missing Docker/auth profile or skipped ignored tests is a failing required job, not a green build. Archive evidence with revision metadata and redact secrets.

### 4.7 P18-07 — Prepare packages, documentation and explicit release approval

Update README examples, compatibility limits, timeout/duplicate-delivery semantics, supported Broker/security versions and ownership/shutdown guidance. Review Apache LICENSE/NOTICE provenance. Recheck every crates.io name immediately before a real release; if occupied, stop for coordinated prefix rename. Set publish/version/license/repository metadata only for real packages and provide registry versions alongside workspace paths. Determine leaf-to-facade dependency order; cargo package/dry-run must resolve dependencies that already exist in the registry. Do not claim a facade dry-run can validate unpublished path-only dependencies. Actual publishing/tagging is a separate owner-approved action, never part of an automatic test.

### 4.8 P18-08 — Collect owner-executed deployment acceptance last

Create docs/owner-validation.md with exact approved target topology/profile, disposable test topic/group, credentials supplied out-of-band, commands, expected results and cleanup. Owner runs real CA/SNI/mTLS/auth/namespace connectivity, allowed/denied operations, representative failover/load and graceful shutdown where these require external infrastructure. Never modify production topics/groups or inject failures without explicit authorization. Record date, revision, environment and evidence; unexecuted required deployment checks keep release approval blocked. All locally runnable TLS/auth and Broker tests stay in P18-01/02/06 rather than being deferred here.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Shared commands, evidence rules and integration prerequisites are in the execution index.

- [ ] **P18-V01** — Every advertised row is COMPATIBLE with evidence or an approved INTENTIONAL_DIFFERENCE with impact; no required suite is missing or skipped.
- [ ] **P18-V02** — Golden/differential/cross-language/fuzz/stress suites pass, all five 5.5.1 regressions are present and shutdown counters return to baseline.
- [ ] **P18-V03** — Published API examples compile using only rocketmq-classic; package graph, licensing, docs and name availability are reviewed before authorized publication.
- [ ] **P18-V04** — Required owner deployment evidence is recorded separately; unavailable environments are reported as BLOCKED rather than PASS.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
mvn -f compat/java-reference/pom.xml verify
python3 tests/differential/compare.py --self-test
RMQ_NAMESRV_ADDR=127.0.0.1:9876 cargo test -p rocketmq-classic-integration --test broker --locked -- --ignored
RMQ_NAMESRV_ADDR=127.0.0.1:9876 cargo test -p rocketmq-classic-integration --test stress --locked -- --ignored
bash tests/differential/run.sh --all
cargo build -p rocketmq-classic --examples --release --locked
```

## 6. Acceptance and handoff

Produce an evidence-backed release candidate checklist, not a compatibility claim based on connectivity. Owner approves external acceptance and publishing separately; any unresolved required scope or failed gate keeps the project experimental.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
