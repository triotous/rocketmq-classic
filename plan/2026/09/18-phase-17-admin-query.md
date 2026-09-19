# Plan: Phase 17 — Admin/query public surface

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 17; execution index: [`README.md`](README.md).
> Translation: [`18-phase-17-admin-query.zh-tw.md`](18-phase-17-admin-query.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## 1. Goal and scope

Expose the approved MQAdmin/query operations and finish client-side administrative processors. Do not expand into the rocketmq-tools command suite, Broker management server or Dashboard.

## 2. Prerequisites and current-state analysis

Phases 0–16 are accepted. Reuse Phase 9 offset-boundary RPCs, Phase 8 message decoding, Phase 6 processor dispatch and Phase 16 namespace/hooks. Optional admin crate creation requires actual approved operations; otherwise place the small facade in core/public crate.

- Read/complete first: [Phase 16](17-phase-16-namespace-security-operations.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `MQAdmin`
- `MQAdminImpl`
- `MQClientAPIImpl query/admin methods`
- `ClientRemotingProcessor reset/status/running-info/direct-consume methods`
- `QueryResult`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-core/src/api/admin.rs
crates/rocketmq-classic-protocol/src/header/
crates/rocketmq-classic-protocol/src/body/
crates/rocketmq-classic-consumer/src/admin.rs
crates/rocketmq-classic/src/lib.rs
crates/rocketmq-classic-core/tests/admin.rs
docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md
```

Each operation follows typed models→single-attempt MQClientApi→public facade→Java differential evidence. Query results preserve Java ordering, pagination/time range, not-found semantics and per-Broker failure behavior. Administrative reset is not ordinary increase-only offset update: it must fence pull/listener generations and persist the reset safely. Direct-consume requests never silently advance normal consumer offsets.

## 4. Ordered implementation tasks

### 4.1 P17-01 — Approve the exact client admin surface

Inventory methods from pinned rocketmq-client MQAdmin, not the separate tools artifact. For create topic, fetch publish/subscribe queues, search/min/max offsets, earliest store time, view message and query-by-key/topic, record supported/deferred and owner approval. Include inbound reset-offset, status, running-info and direct-consume processor obligations from the client inventory. Keep already-used offset primitives supported regardless of optional admin exposure.

### 4.2 P17-02 — Reuse models and add missing RPC evidence

For approved missing operations, add request/response models and fixtures before network methods. Reuse route conversion and offset-boundary APIs without duplicate constants. Validate topic/key/time range/max count and distinguish unique client ID lookup from offset message ID lookup using Java source. Decode view/query messages with existing bounded decoder and apply namespace stripping consistently.

### 4.3 P17-03 — Implement query fan-out and public results

Follow Java broker selection, merge/dedup/order and partial-error semantics for multi-Broker queries. Bound concurrency with the existing runtime and share one deadline across all queries. Topic creation validates permissions/queue counts and targets the correct Broker; never create topics as an incidental read fallback. Expose typed results through the facade, preserving raw Broker errors and stable message IDs.

### 4.4 P17-04 — Activate administrative consumer processors safely

Reset-offset pauses affected queues, fences generations, coordinates in-flight work, replaces/freeze-updates the store as Java requires, persists and resumes only a coherent new generation. Status/running-info returns bounded snapshots without blocking readers or leaking credentials. Direct-consume uses the correct group/listener mode in bounded execution and maps result/timing without normal ProcessQueue commits. Unknown groups, malformed bodies and shutdown return source-compatible errors, not successful empty results.

### 4.5 P17-05 — Audit the remaining unsupported processors

Revisit every Phase 6 stub. Each is now an implemented tested handler or a documented owner-approved intentional difference; no generic success stub survives. Add minimal public examples for approved query/create operations and keep permission requirements explicit. Update REQUEST_CODES.md and the method-level matrix with direct evidence IDs.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Shared commands, evidence rules and integration prerequisites are in the execution index.

- [ ] **P17-V01** — Each approved operation has success, not-found/permission/error and malformed-response coverage; existing offset/route behavior is unchanged.
- [ ] **P17-V02** — Local multi-Broker query results match Java for duplicate keys, bounded time ranges and partial failure.
- [ ] **P17-V03** — Reset versus slow callback/late pull cannot restore old offsets; direct-consume leaves regular offsets unchanged and status requests remain responsive.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-core --test admin --locked
cargo test -p rocketmq-classic-consumer --locked
bash tests/differential/run.sh --phase 17
```

## 6. Acceptance and handoff

Phase 18 receives a fully classified public-method and inbound-processor inventory. Do not call the entire Java public API compatible if owner-approved exclusions remain undocumented.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 18](19-phase-18-compatibility-release-gate.md).

## 7. Open questions / decisions required

1. Which MQAdmin operations must be public in the first stable release? Approve the method list at P17-01; existing offset primitives and safe handling of inbound administrative requests cannot simply be removed.
