# Plan: Phase 19 — Docker local validation and final acceptance

> Parent: [Master plan](00-rocketmq-classic-plan.md); index: [README.md](README.md).
> Translation: [繁體中文](20-phase-19-docker-local-validation.zh-tw.md). Filename `20` is the document number; this is Phase 19. All implementation paths/commands below are proposed, not existing infrastructure or completed results.

## 1. Goal and scope

Build the required Docker test environments and execute **every locally runnable test** accumulated through Phase 18. Use **Java client 5.5.1** as the behavior/source oracle and **RocketMQ server V5_3_4 (5.3.4)** as the production baseline. Implement/fix client code only; use unmodified Apache server distributions, never a Rust server or server patches to hide incompatibility.

Complete parity includes all Java client settings, defaults, features, control flows and errors; no extra Rust public setting/feature and no Java capability omitted. Docker/test-runner options are test infrastructure, not client configuration. Non-local validation is a separately tracked owner-executed manual checklist, not a reason to skip locally reproducible tests.

## 2. Prerequisites and current-state analysis

Complete [Phase 18](19-phase-18-compatibility-release-gate.md). Reuse its source matrix, offline Java harness, fixtures and `tests/differential/scenarios.yaml`; do not recreate a parallel oracle. The repository has no actual workspace or Docker environment yet. The 5.3.4 image name/digest, runtime architecture and server-supported profiles must be verified during P19-01, not guessed here.

Review Java 5.5.1 `MQClientAPIImpl`, producer/consumer/transaction/reply flows and all source mappings. Inspect server 5.3.4 source/configuration only to configure environments and explain server responses; it does not replace the client oracle or authorize server implementation work.

## 3. Target files and required environment

- `integration-tests/compose.yaml`, `integration-tests/config/`, `integration-tests/setup.sh`, `integration-tests/cleanup.sh`, `integration-tests/README.md`: isolated Docker profiles, readiness, seed data and teardown.
- `integration-tests/Cargo.toml`, `integration-tests/tests/broker.rs`, `integration-tests/tests/stress.rs`: non-published `rocketmq-classic-integration` workspace package.
- `tests/differential/run.sh`, `tests/differential/scenarios.yaml`, `tests/differential/compare.py`: profile-aware complete Java/Rust execution and comparison.
- `integration-tests/run-local.sh`: one entry point for all local suites and evidence inventory.
- `docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md`, `docs/compatibility/SERVER_5_3_4_RESULTS.md`, `docs/release-checklist.md`, `docs/owner-validation.md`: separate client parity, server results, manual evidence and release decision.

Required profiles: basic NameServer + Broker, two Brokers for route/retry/query/rebalance, TLS with an ephemeral CA and negative certificates, source-confirmed auth/ACL, and feature-specific trace/POP/transaction/filter-server or failover topologies wherever the actual Java paths require them. Add only profiles needed by inventoried scenarios. Two independent Brokers do not prove replicated HA failover; configure the actual source-supported HA topology if that scenario requires it. No profile may silently switch the baseline away from 5.3.4.

## 4. Ordered implementation tasks

### 4.1 P19-01 — Pin and verify Docker artifacts

Resolve an official 5.3.4 server image or build a local image from a verified Apache 5.3.4 binary distribution. Record immutable digest or distribution SHA-256, source version, image build inputs, JDK, OS/architecture and observed startup version. Keep Java client artifact/source pinned to 5.5.1 with independent checksums. Pin Java/Rust runner toolchains and dependencies separately; an image's bundled client must not silently replace the oracle. Fail clearly if an image/platform or artifact is unavailable.

### 4.2 P19-02 — Create isolated environments and deterministic setup

Use a dedicated Compose project/network/volumes and bind exposed ports to loopback. Configure Broker advertised addresses reachable from both test runners; container-local addresses and host-mapped addresses are not interchangeable. Prefer both language runners on the same Compose network; document host execution addresses when supported. Verify route lookup and actual send/pull readiness with bounded polling rather than fixed sleeps.

Create idempotent scenario-owned topics, groups, queue counts, retry/DLQ resources, ACL identities and controlled starting offsets. Run Java and Rust against equivalent reset states, or jointly only when mixed-client interaction is the assertion. Require a test-environment identity marker and explicit profile before any mutation. Cleanup only this Compose project's resources; archive logs/results before teardown and never delete external/production data. Generate ephemeral TLS keys and synthetic credentials; do not commit production secrets or disable verification to make tests pass.

### 4.3 P19-03 — Build fail-closed runners and migrate live obligations

Collect every earlier `P*-V*` live handoff plus P18-02/08 requirements; preserve source IDs and add new IDs for uncovered Java features/settings. Each manifest row records source phase, execution_phase=19, profile/server version, Java/Rust commands, isolated inputs, bounded timeout/seed, invariants, normalization and cleanup. Unit-test manifest coverage, version checks, missing scenarios and comparator failures.

Implement `run.sh --phase N` and `--all` against the manifest. Broker tests may be ignored in ordinary server-free cargo runs, but `run-local.sh --all` must explicitly execute every required ignored target and reconcile executed IDs against expected IDs. Missing Docker/profile/auth prerequisite, nonzero exit, skipped scenario or empty evidence fails the local gate; never return success after an early skip. Run all offline suites too, including fuzz smoke, deterministic races, benchmarks and example compilation. Archive revision, both client versions, server digest/configuration, toolchains, seed, commands, logs, raw/normalized results and durations with secrets redacted.

### 4.4 P19-04 — Execute the full local scenario matrix

| Family | Required behavior and evidence |
| --- | --- |
| Protocol/transport/runtime | Both codecs, headers, malformed/unknown responses, TCP/TLS, timeout/cancellation/oneway, route refresh/failures, heartbeat/unregister, shared lifecycle and reconnect |
| Producer | Sync/async/oneway/batch, explicit queue/selector, all Java configuration boundaries, compression, retry/fault selection, two-Broker failures, original deadline and duplicate-delivery relationships |
| Pull/Push/offsets | Every pull status, manual persistence/restart, local/remote offset stores, listener partial/failure, send-back/fallback/retry/DLQ, broadcasting/clustering, flow control and orderly lock renewal/loss |
| Rebalance/Lite/POP | Every Java allocation strategy, mixed Java/Rust join/leave, initial-offset policies, Lite assignment/subscription/seek/commit/pause, POP receipts/ACK/batch ACK/change-invisibility/expiry and complete Java POP client flows |
| Transaction/request-reply | Commit/rollback/unknown and actual Broker checks/visibility, duplicate checks, both requester/responder languages, no responder, late/duplicate reply and exactly-one completion |
| Cross-cutting/admin | Namespace, class-filter client paths, hooks/order/signatures, trace delivery/flush, auth allowed/denied, TLS failures, all MQAdmin/query operations, reset/status/running-info/direct-consume and partial Broker failure |
| Interoperability | Java producer to Rust consumer and reverse, mixed clustering/orderly, transaction consumption and bidirectional request/reply on identical server profiles |
| Stress/recovery | Fixed-seed bounded sustained load, disconnect/restart, actual configured failover, slow listeners, rebalance churn, offset recovery and repeated shutdown with task/socket/pending counts returning to baseline |

Test every public setting through its actual facade/configuration path, not internal test setters alone. Use Java 5.5.1 outcomes as assertions, including unsupported-server responses. Record performance baselines with hardware/resources and load shape; production capacity/SLO evidence is separate. Add unit regression fixtures for every client defect found, fix the owning layer and rerun its unit checks plus affected and full local suites.

### 4.5 P19-05 — Distinguish server limits from missing client parity

Run Java 5.5.1 and Rust against the same 5.3.4 profile before attributing failure to the server. A matched rejection proves only the rejection path, not successful feature operation. Record server limitation, Java/Rust result, source evidence and missing success-path evidence separately; keep the Rust feature fully implemented with unit/golden coverage.

For a success path unavailable on 5.3.4, first determine whether a verified supplementary unmodified server version/topology can run locally. If so, add a separately named/pinned Docker profile and execute it here; retain mandatory 5.3.4 results and do not claim that the supplementary profile proves production support. Only genuinely non-local requirements go to manual validation with reasons. Inability to provision an otherwise local profile is BLOCKED, not a manual waiver or PASS. Unproven required behavior prevents full compatibility/release approval.

### 4.6 P19-06 — List and hand off non-local manual validation

Create `docs/owner-validation.md`. For each item record ID, non-local reason, owner, required topology/version/access, authorized disposable resources, exact commands, expected Java/Rust outcomes, cleanup, revision/date and evidence location. No production writes or fault injection without explicit authorization.

| ID | Owner-executed item when it cannot be reproduced locally |
| --- | --- |
| MAN-01 | Actual production 5.3.4 NameServer/Broker discovery, DNS/VIP/proxy/firewall and cross-network connectivity |
| MAN-02 | Organization-issued CA/SNI/mTLS, real identity/ACL provisioning and rotation, actual allowed/denied namespace/topic/group permissions |
| MAN-03 | Production-specific HA/failover, network partitions, multi-zone routing and recovery under approved maintenance/fault-injection procedures |
| MAN-04 | Representative production workload, capacity/SLO, long soak, resource quotas and operational restart/shutdown behavior |
| MAN-05 | Any source-confirmed feature requiring infrastructure unavailable locally, with evidence that local/supplementary Docker alternatives were evaluated |

Local synthetic TLS/auth, ordinary restart/failure, mixed-client and bounded stress tests remain automatic and cannot be reassigned to these items merely for convenience. Unrun manual checks remain BLOCKED; N/A requires evidence of actual non-applicability, never missing implementation. Owner approval cannot waive Java feature/settings parity.

### 4.7 P19-07 — Enforce final acceptance and release authorization

CI runs server-free gates plus the complete Docker local matrix. Required profiles/tests are not optional on a release branch. Verify bidirectional source/settings coverage again, complete evidence IDs, no extra public controls and no missing Java flows. Keep separate statuses for client implementation, 5.3.4 interoperability, supplementary-server success and manual deployment checks. No `INTENTIONAL_DIFFERENCE` may excuse public API/configuration/behavior gaps.

A final candidate requires all local gates passed and required manual evidence complete. Block on unresolved differences, unavailable required environments or missing success-path evidence. Publishing/tagging remains a separately authorized action: recheck crate-name availability, package graph/registry dependencies and LICENSE/NOTICE before publishing leaf-to-facade. Do not publish automatically as part of tests.

## 5. Verification

- [ ] **P19-V01** — Clean Docker setup is reproducible; actual 5.3.4 version/digest and Java 5.5.1 pin are verified; readiness and safe cleanup work.
- [ ] **P19-V02** — Executed local IDs equal the complete required inventory; no missing/ignored/skipped case counts as PASS, and no available local test is deferred manually.
- [ ] **P19-V03** — Java/Rust cross-language and differential results match for every applicable family; server limitations and supplementary success evidence are distinct.
- [ ] **P19-V04** — Configuration/feature/flow reverse audit finds neither extra public Rust capability nor missing Java capability; newly found defects have unit regressions.
- [ ] **P19-V05** — Non-local items have executable owner instructions and recorded PASS/BLOCKED/N/A evidence; required BLOCKED items block final approval.

### 5.1 Proposed local entry point

```bash
docker version
docker compose version
bash integration-tests/run-local.sh --all
```

Implement `run-local.sh` to validate configuration, provision profiles, run setup and all offline/live/differential/stress/fuzz/example suites, reconcile evidence IDs, then tear down via a trap while preserving failure status and logs. Document profile-specific replay commands and addresses in `integration-tests/README.md`. This plan does not claim these commands already exist or have passed.

## 6. Acceptance and handoff

Produce an auditable report separating complete Java 5.5.1 client parity, production-baseline 5.3.4 results, any supplemental local evidence and owner manual results. All locally executable tests have actually run; no missing feature/configuration/flow or extra public behavior is accepted. Final compatibility/release approval remains blocked until required local and non-local evidence passes. No server implementation, automatic publication or production mutation is part of this task.

## 7. Owner inputs required for manual execution

Obtain actual production topology, auth/CA provisioning, allowed test resources, maintenance authorization and workload/SLO parameters before affected manual cases. These are environment inputs, not decisions about whether Java features may be omitted; they do not block building the isolated local profiles.
