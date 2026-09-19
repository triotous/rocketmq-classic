# Plan: Phase 6 — Client runtime, routing and heartbeat

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 6; execution index: [`README.md`](README.md).
> Translation: [`07-phase-06-runtime-routing-heartbeat.zh-tw.md`](07-phase-06-runtime-routing-heartbeat.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## 1. Goal and scope

Own and share runtime resources by client identity, provide coherent routing and heartbeat snapshots, and prove complete startup rollback/shutdown.

## 2. Prerequisites and current-state analysis

Phase 5 RPCs are accepted. Consumer pull/rebalance implementations do not exist yet; runtime exposes narrow registration and wakeup contracts without inventing a Consumer scheduler here.

- Read/complete first: [Phase 5](06-phase-05-typed-rpc-api.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `MQClientManager`
- `MQClientInstance`
- `ClientConfig`
- `TopicPublishInfo`
- `ClientRemotingProcessor`
- `RebalanceService`
- `PullMessageService`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-core/src/config.rs
crates/rocketmq-classic-core/src/runtime/manager.rs
crates/rocketmq-classic-core/src/runtime/runtime.rs
crates/rocketmq-classic-core/src/runtime/tasks.rs
crates/rocketmq-classic-core/src/route/cache.rs
crates/rocketmq-classic-core/src/heartbeat.rs
crates/rocketmq-classic-core/tests/runtime.rs
docs/architecture/ADR-0002-client-runtime-sharing.md
```

ClientRuntimeManager is an explicit shareable handle, not an unresettable global singleton. The facade later shares a default manager while allowing an explicit manager for tests. A client-ID entry owns single initialization; compare runtime-affecting TLS/auth/NameServer configuration and reject conflicts rather than silently sharing incompatible transports. Model Created→Starting→Running→ShuttingDown→Stopped and startup failure. Group registration is identity-based and separate from Arc strong counts.

## 4. Ordered implementation tasks

### 4.1 P6-01 / P6-02 — Build identity and single-flight creation

Fixture-test ClientConfig ID construction, including instance/unit components and blank local serve address treated as unset. Use per-key OnceCell or entry ownership before constructing sockets/tasks. Allow unrelated keys to initialize concurrently. Failed initialization removes only its own entry and permits a documented retry. Keep runtime ownership explicit in ADR-0002.

### 4.2 P6-03 / P6-04 — Implement lifecycle and group leases

Make start/shutdown transitions serialized and idempotent where documented. Producer/consumer registrations return an identity token for rollback/unregister; reject duplicate groups according to Java. One producer shutting down must not stop a runtime still used by another registration. Avoid strong-reference cycles between registries and runtime handles.

### 4.3 P6-05 / P6-08 — Publish coherent route snapshots

Fetch without holding cache locks, convert route fully, then replace a coherent snapshot of topic/broker/publish/subscribe state. Use Phase 1 conversions, including static/ordered routes. Implement master/slave publish/subscribe resolution and VIP port transformation as pure functions with checked ports and IPv6 support. Do not discard a valid cached route on a transient refresh error unless Java requires it.

### 4.4 P6-06 / P6-07 — Own periodic tasks and heartbeat snapshots

Use centralized defaults: route poll 30000 ms, heartbeat 30000 ms, offset persistence 5000 ms; verify initial delays and missed-tick behavior from Java. Each task has cancellation and join ownership. Offset ticks call registered consumer persistence capabilities only when present. Construct heartbeat from a snapshot outside locks; test subscription versions, V1/V2 configuration, unregister and producer-only/consumer-only combinations.

### 4.5 P6-09 / P6-10 — Register inbound slots and roll back failures

Route inbound check-transaction, consumer-change, reset-offset, consumer-status, direct-consume and reply-push by group/client identity. Until an owning phase activates a handler, return an explicit supported-protocol error, never fake success. Inject failures after registration, transport start and each task start; reverse owned effects and join all work. Last-registration shutdown unregisters from Brokers before closing transport within a bounded deadline.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Shared commands, evidence rules and integration prerequisites are in the execution index.

- [ ] **P6-V01** — Many concurrent get_or_create calls for one ID yield one identity, one task set and one socket pool; different IDs are not globally serialized.
- [ ] **P6-V02** — Stale failed creation and unregister tokens cannot remove replacements; incompatible shared configuration is rejected.
- [ ] **P6-V03** — Paused-time timers, route snapshot readers and all startup-failure points leave no partial state or leaked tasks.
- [ ] **P6-V04** — Local Broker sees correct heartbeat/subscription changes and unregister; shutting one shared user leaves the other functional.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-core --test runtime --locked
bash tests/differential/run.sh --phase 6
```

## 6. Acceptance and handoff

Phase 7 receives lifecycle registration, route resolution and shared transport ownership. No stub handler may be marked COMPATIBLE, and the matrix names its activation phase.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 7](08-phase-07-producer-core.md).
