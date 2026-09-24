# Plan: Phase 12 — Orderly consumption and queue locks

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 12; execution index: [`README.md`](README.md).
> Translation: [`13-phase-12-orderly-consumption.zh-tw.md`](13-phase-12-orderly-consumption.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Add per-queue ordered listener execution, Broker lock renewal/expiry and rebalance-safe offset commits. Do not claim global ordering across queues or exactly-once side effects.

## 2. Prerequisites and current-state analysis

Phases 10–11 concurrent pipeline and reconciliation are accepted. Reuse Phase 5 lock/unlock RPCs and ProcessQueue lock fields rather than adding a second assignment registry.

- Read/complete first: [Phase 11](12-phase-11-rebalance.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `ConsumeMessageOrderlyService`
- `ProcessQueue orderly methods`
- `RebalanceImpl lock/unlock branches`
- `ConsumeOrderlyStatus`
- `ConsumeOrderlyContext`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-consumer/src/consume/orderly.rs
crates/rocketmq-classic-consumer/src/process_queue.rs
crates/rocketmq-classic-consumer/src/rebalance/mod.rs
crates/rocketmq-classic-consumer/tests/orderly.rs
examples/orderly_consumer.rs
```

Keep available, in-flight and committed orderly state distinct. A queue-specific async consume gate serializes callbacks; short state locks protect transitions and are released before user code. Independent queues may run concurrently. Broker lock ownership and local serialization are separate requirements. Callback success after lock loss/drop cannot commit under a newer assignment generation; cancellation cannot undo user side effects, so document possible redelivery.

## 4. Ordered implementation tasks

### 4.1 P12-01 — Apply batch lock results and own renewal

Group queues by Broker for LOCK_BATCH_MQ/UNLOCK_BATCH_MQ. Only returned locked queues become eligible; partial lock success is not all-or-nothing. Record monotonic freshness per generation and renew at the Java-configured interval. One owned renewal task uses current assignment snapshots; stale renewal responses cannot relock dropped/replaced queues. Broadcasting follows the Java no-distributed-lock branch where applicable.

### 4.2 P12-02 / P12-03 — Serialize consumption and enforce freshness

Acquire the per-queue consume gate, validate dropped/generation/lock freshness, move the next ordered batch to in-flight, then invoke the listener without the global state lock. If stale, suspend and reacquire rather than continue. Recheck ownership after callback and before committing. Test exact expiry boundary with paused time; copy durations from source/configuration, not an assumed constant.

### 4.3 P12-04 — Map every listener result and context mode

Fixture autoCommit true/false with SUCCESS, SUSPEND_CURRENT_QUEUE_A_MOMENT and source-supported COMMIT/ROLLBACK behavior, including deprecated modes present in Java. Success removes only the acknowledged ordered prefix; suspend/rollback restores the same order. Apply source-defined retry limits, send-back/fallback and suspend delay bounds. Never skip a failed earlier message to process a later one in the same queue.

### 4.4 P12-05 — Complete lock-aware rebalance and shutdown

On gain, lock before activating normal pull/consume. On loss, mark dropped, quiesce queue work, persist safe offset and unlock; retry incomplete cleanup without resurrecting the queue. Slow listener work must not hold the entire rebalance service hostage; use the Java-compatible bounded retry/cleanup path. Shutdown cancels renewal and joins tasks only after safe unlock/persistence attempts. Unlock failure is visible and cannot be reported as confirmed release.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [ ] **P12-V01** — Table tests cover partial lock success, renewal response races, exact expiry and every supported listener/context result.
- [ ] **P12-V02** — Concurrent queues progress independently while each queue preserves order through suspend, rollback, retry and lock loss.

- [ ] **P12-U01** — Scripted lock results, paused time and slow-callback barriers verify competition, renewal/expiry, rebalance and shutdown without stale commits.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-consumer --test orderly --locked
cargo build -p rocketmq-classic --examples --locked
```

### 5.2 Phase 19 live-test handoff

Retain these case IDs and behavior requirements for Phase 19 execution against Docker server 5.3.4; they are not this phase's pass conditions and must not be marked PASS early.

- [ ] **P12-V03** — Local mixed Java/Rust orderly consumers compete, rebalance with slow callbacks and shut down without stale commits; Broker lock state converges after cleanup/expiry.

## 6. Acceptance and handoff

Orderly implementation passes unit/fixture gates for every Java mode. Keep live lock/mixed-group evidence pending Phase 19 before advertising interoperability.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 13](14-phase-13-lite-assignment-pop.md).
