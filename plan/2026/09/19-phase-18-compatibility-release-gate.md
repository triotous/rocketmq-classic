# Plan: Phase 18 — Complete unit parity and prepare final validation

> Parent: [Master plan](00-rocketmq-classic-plan.md), Phase 18; execution index: [README.md](README.md).
> Translation: [繁體中文](19-phase-18-compatibility-release-gate.zh-tw.md). This is a plan, not completed test evidence. The existing filename is retained to preserve links; final release acceptance moves to Phase 19.

## 1. Goal and scope

Close Java client **5.5.1** source/configuration/feature/control-flow coverage using **unit tests as the primary gate**. Supplement with offline golden/differential, deterministic race, fuzz-smoke and microbenchmark checks. No live Broker or Docker is required. Implement only the client; server **V5_3_4 (5.3.4)** is the production interoperability baseline for Phase 19.

Every Java client setting and feature is mandatory, with identical defaults, validation, errors, callbacks, retry, routing, offsets and lifecycle semantics. No Rust-only public setting/feature, missing Java capability or intentional behavioral difference may pass. Idiomatic Rust internals do not authorize changing observable flows.

## 2. Prerequisites and current-state analysis

Complete [Phase 17](18-phase-17-admin-query.md) and all Phase 0–17 unit gates. The repository currently contains plans only; all paths and commands below are proposed implementation work. Verify the Phase 0 Java artifact/source provenance again, not a server version substitution.

Inspect every mapped Java class, method, overload, inherited/deprecated API, client configuration path and call site. Cover the 5.5.1 request/reply, runtime construction, blank-address, header encoding and batch-validation regressions. Inventory gaps discovered here must be implemented and tested before Phase 19, even if not named in an earlier plan.

## 3. Target files and implementation contract

- `docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md`: bidirectional feature/settings/flow coverage and evidence.
- `docs/compatibility/REQUEST_CODES.md`: all client operations and inbound processors, not merely numeric constants.
- `docs/compatibility/KNOWN_DIFFERENCES.md`: unresolved blockers, server limitations and internal representation notes, never a feature-exclusion waiver.
- `tests/differential/scenarios.yaml`, `tests/differential/compare.py`: complete scenario inventory and offline comparison.
- `fuzz/`, `tests/stress/`, crate-local tests/benches: bounded offline harnesses.
- `docs/release-checklist.md`, `README.md`, workspace/crate manifests: preparation only, no publication.

Separate implementation status from live-test status. `UNIT_VERIFIED` does not mean final `COMPATIBLE`; missing live evidence remains `PENDING_PHASE_19`. Java 5.5.1 and server 5.3.4 have separate metadata fields. A server limitation never removes a Java feature from Rust.

## 4. Ordered implementation tasks

### 4.1 P18-01 — Audit complete bidirectional parity

For each Java feature/method/configuration, record source path, method/call site, type/default/unit/range, validation, control-flow branches, Rust implementation and unit IDs. Reverse-audit every Rust public API/configuration, including environment variables, feature flags, limits and test injection points: no unexplained public setting or feature is allowed. Port missing Java capabilities rather than changing the denominator. Private safety/ownership mechanisms cannot change valid-input outcomes, Java configuration semantics or public flow behavior.

### 4.2 P18-02 — Close offline oracle and scenario coverage

Run all unit, Java golden cross-decode and offline differential cases. Compare defaults, valid/invalid inputs, call ordering, callback counts, retry decisions and offset/ownership transitions. Normalize only proven nondeterminism; comparator negative tests must detect changed status, ordering, retry counts and duplicate/offset relationships. Collect every earlier phase's live handoff IDs into `scenarios.yaml`, with source phase, execution_phase=19, profile, input, assertion, cleanup and Java/Rust directions. Missing cases fail the inventory check.

### 4.3 P18-03 — Fuzz protocol trust boundaries

Seed Remoting/private-header/ext-field/message/property fuzz targets from existing fixtures. Ordinary `cargo test` includes deterministic bounded mutation regressions; an isolated fuzz harness adds a reproducible fixed-budget smoke run. Assert checked lengths, no panic or unbounded decompression and retention of unknown raw codes. Save each discovered failure as a fixture before fixing it. Do not introduce Rust-only public limit knobs; derive exposed limits from Java configuration.

### 4.4 P18-04 — Prove deterministic race and leak behavior

Use paused time, barriers, scripted peers and temporary offset files for near-deadline response, reconnect, opaque wrap, concurrent runtime construction, start/stop, slow callbacks, assignment churn and duplicate replies. Count tasks, sockets, pending entries and queue work, not just memory usage. Repeated shutdown returns to zero or the documented shared-runtime baseline. Server-backed sustained load belongs to Phase 19.

### 4.5 P18-05 — Measure offline performance

Benchmark codec/message decode, send construction, pending completion, route lookup and allocation in release mode with fixed fixture inputs. Record hardware/toolchain, input size, iterations, latency and allocation observations. Use a small `harness=false` bench with `black_box` before adding a framework. Optimize only measured bottlenecks and rerun oracle checks. Do not invent throughput acceptance thresholds or extra public tuning controls.

### 4.6 P18-06 — Enforce server-free CI

Require fmt, clippy, locked unit tests, Java version pin, golden cross-decode, comparator self-tests, deterministic race/leak checks, fuzz smoke and example compilation. Run default/all-features and supported configuration combinations. CI for Phase 0–18 must pass without a Docker daemon or server address. Do not mistake skipped live tests for passing evidence; Phase 19 adds mandatory live CI jobs.

### 4.7 P18-07 — Prepare packages and documentation

Document Java-equivalent public usage, timeout/duplicate-delivery and shutdown semantics, client 5.5.1/server 5.3.4 baseline and pending live evidence. Review Apache LICENSE/NOTICE and package dependency direction. Prepare package metadata and leaf-to-facade publishing order; packaging checks cannot claim that unpublished registry dependencies resolve. Actual registry availability checks, publication and tagging require separate authorization after Phase 19.

### 4.8 P18-08 — Hand off final validation requirements

Validate the complete Phase 19 scenario manifest and list deployment-only candidate checks with explicit reasons they cannot run locally. Do not execute production checks here. TLS/auth, restart, failover, cross-language and stress cases that can run with local containers remain automatic Phase 19 obligations, not manual exceptions.

## 5. Verification

- [ ] **P18-V01** — Every Java client setting/feature/flow maps to implemented Rust and unit evidence; reverse audit finds no extra public capability or setting.
- [ ] **P18-V02** — Unit/golden/offline differential/race/fuzz suites pass, including all five 5.5.1 regressions; shutdown counters return to baseline.
- [ ] **P18-V03** — Facade examples compile, package graph/provenance/docs are reviewed, and no final release claim is made.
- [ ] **P18-V04** — All live handoff IDs are registered for Phase 19; unit-only CI succeeds without Docker or a running Broker.

### 5.1 Commands after implementation

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo test --workspace --all-features --locked
mvn -f compat/java-reference/pom.xml verify
python3 tests/differential/compare.py --self-test
cargo build -p rocketmq-classic --examples --release --locked
```

Record exact bounded fuzz/microbenchmark commands when their targets exist. These commands are not executable in the current documentation-only tree.

## 6. Acceptance and handoff

All Java client implementation and unit coverage is complete, with no approved-away gaps or extra public configuration/features. Live evidence remains pending; this phase cannot authorize a compatibility claim or release. Handoff includes revision, source mappings, test IDs/results, fixture hashes and the complete live/manual manifest.

Next: [Phase 19 — Docker local validation and final acceptance](20-phase-19-docker-local-validation.md).
