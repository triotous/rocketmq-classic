# Plan: Phase 14 — Transaction producer

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 14; execution index: [`README.md`](README.md).
> Translation: [`15-phase-14-transaction-producer.zh-tw.md`](15-phase-14-transaction-producer.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Implement half-message, local decision and Broker transaction-check flows using the existing Producer and inbound transport. A successful half send does not itself imply committed visibility.

## 2. Prerequisites and current-state analysis

Phases 7–8 producer/decoder and Phase 6 processor registry are accepted; Phase 13 complete source-mapping audit is closed. Reuse Phase 4/5 end/check transaction headers and RPCs.

- Read/complete first: [Phase 13](14-phase-13-lite-assignment-pop.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `TransactionMQProducer`
- `DefaultMQProducerImpl.sendMessageInTransaction`
- `TransactionListener`
- `LocalTransactionState`
- `EndTransactionRequestHeader`
- `ClientRemotingProcessor.checkTransactionState`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-producer/src/transaction.rs
crates/rocketmq-classic-producer/src/producer.rs
crates/rocketmq-classic-core/src/runtime/runtime.rs
crates/rocketmq-classic-producer/tests/transaction.rs
examples/transaction_producer.rs
```

Expose distinct Commit/Rollback/Unknown decisions and separate local-execute/check callbacks. Callbacks run in bounded Producer-owned work, never the socket reader. Retain a transaction context containing the exact half-send result, original message and producer identity. Do not manufacture queue/commit-log offsets from the wrong message ID. Error/panic/cancellation must not imply Commit; record the source-compatible Unknown/rollback policy explicitly.

## 4. Ordered implementation tasks

### 4.1 P14-01 / P14-02 — Add validated API and half-send construction

Require local/check callbacks at start, register by producer group and reject duplicate listener ownership. Validate transactional-message restrictions using Java fixtures. Copy the message for prepared properties/sysFlags and use normal route/compression/retry logic; do not mutate the caller message. Preserve transaction/unique IDs in SendResult and the returned transaction result.

### 4.2 P14-03 — Run the local transaction at the right point

Derive behavior for each half-send status from Java: the user callback runs only in the allowed path, never after a definite half-send failure. Map callback results/errors/panics to explicit decisions and retain the result needed to notify Broker. Caller cancellation after half-send cannot erase Broker recovery: document Unknown/check recovery and retain any accepted owned task until its safe terminal state.

### 4.3 P14-04 — Build end-transaction commands from exact evidence

Map commit/rollback/unknown flags and producerGroup/tranStateTableOffset/commitLogOffset/fromTransactionCheck/msgId/transactionId fields from source fixtures. Decode offsetMsgId versus unique ID using the correct helper. Use Java oneway semantics where required; a write success is not a Broker commit acknowledgement. Preserve local decision when end command transport fails so later check handling remains coherent.

### 4.4 P14-05 / P14-06 — Activate Broker checks and own shutdown

Decode inbound header/message with existing bounded codecs, locate the correct producer listener and enqueue a bounded check callback. Validate missing/unknown group behavior from Java. Send END_TRANSACTION with fromTransactionCheck and offsets from the inbound check. Repeated checks may legitimately invoke the user callback again; require an idempotent business check, not an unbounded permanent dedup map. On shutdown stop admission, handle queued/in-flight checks under a documented bounded policy and unregister only the matching generation.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [ ] **P14-V01** — Fixtures verify half properties/flags and all end/check fields for commit, rollback, unknown and non-OK send statuses.
- [ ] **P14-V02** — Error/panic/cancellation, repeated checks, slow callbacks and shutdown races never produce accidental commit or reader blockage.

- [ ] **P14-U01** — Scripted half-send/check inputs verify commit/rollback/unknown decisions and exact end-transaction fields; no fixture claims actual Broker visibility.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-producer --test transaction --locked
```

### 5.2 Phase 19 live-test handoff

Retain these case IDs and behavior requirements for Phase 19 execution against Docker server 5.3.4; they are not this phase's pass conditions and must not be marked PASS early.

- [ ] **P14-V03** — Local Broker commit becomes consumable, rollback remains invisible and Unknown resolves through an actual Broker check; compare Java behavior with a bounded configured check interval.

## 6. Acceptance and handoff

Unit shutdown/visibility decisions and pending Phase 19 cross-language scenarios are linked in the matrix. Do not equate successful END_TRANSACTION transport with proven transaction compatibility.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 15](16-phase-15-request-reply.md).
