# Plan: Phase 13 — Lite Pull, assignment and POP APIs

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 13; execution index: [`README.md`](README.md).
> Translation: [`14-phase-13-lite-assignment-pop.zh-tw.md`](14-phase-13-lite-assignment-pop.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## 1. Goal and scope

Finish the explicitly selected additional consumer surface and preserve Broker-issued POP receipt metadata. Do not invent a new high-level POP framework absent from the approved scope.

## 2. Prerequisites and current-state analysis

Phases 4–5 extension models/RPCs, Phase 8 decoding and Phase 11 assignment are ready. P13-01 is a scope gate because the master plan allows supported/deferred/lower-level-only APIs; code-independent inventory work may proceed before owner approval.

- Read/complete first: [Phase 12](13-phase-12-orderly-consumption.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `DefaultLitePullConsumer`
- `DefaultLitePullConsumerImpl`
- `AssignedMessageQueue`
- `MQClientAPIImpl POP/ACK methods`
- `PopResult`
- `ExtraInfoUtil`
- `assignment/request-mode bodies`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-consumer/src/lite/
crates/rocketmq-classic-consumer/src/pop.rs
crates/rocketmq-classic-core/src/api/consumer.rs
crates/rocketmq-classic-protocol/src/body/
crates/rocketmq-classic-consumer/tests/lite_pop.rs
examples/lite_pull.rs
```

Keep classic pull offsets and POP receipts separate. A POP receipt retains original extra-info bytes plus validated parsed fields; ACK/change-invisibility uses the exact Broker identity, queue, offsets and receipt semantics. A changed invisibility response may replace the handle according to Java; do not reconstruct pop time or silently reuse a stale handle. DefaultLitePullConsumer behavior is defined by its call graph, not merely by the similarly named LITE_PULL_MESSAGE constant.

## 4. Ordered implementation tasks

### 4.1 P13-01 — Approve a complete method-level scope table

Inventory subscribe/unsubscribe, assign, poll, seek, pause/resume, committed offsets, auto/manual commit and timestamp/queue helpers for Lite Pull; inventory assignment/request-mode and all actual POP calls separately. For each row choose v1 supported, later or lower-level-only, with owner approval and user-visible gap wording. Do not downgrade baseline lower-level RPCs already required by Phase 5. Unapproved rows stay TODO, not intentional differences.

### 4.2 P13-02 — Implement Lite modes and bounded polling

Prevent unsupported simultaneous subscription/explicit-assignment modes according to Java. Reuse pull scheduling/decoding without listener dispatch. Maintain fetched, delivered and committed positions separately; seek invalidates queued/in-flight results by generation and clears obsolete cache. Poll timeout, max batch, empty result, pause/resume and shutdown must match fixtures. Auto-commit runs only under its Java rules, never merely because bytes were prefetched.

### 4.3 P13-03 — Convert Broker assignments and request modes

Use QUERY_ASSIGNMENT/SET_MESSAGE_REQUEST_MODE typed RPCs and preserve static-topic mapping and mode metadata. Validate group/topic/mode at the public boundary. Separate unsupported-Broker outcomes from empty valid assignments; integrate updates through existing assignment generations rather than mutating active queue state in place.

### 4.4 P13-04 / P13-05 — Complete POP receipt operations

Decode pop results and extra-info using source-derived delimiters/field rules with bounded numeric parsing. Preserve pop time, invisibility, revive/retry and queue offset mapping exactly, including batched/mixed-queue results. Implement ACK, source-used batch ACK and change-invisibility; define expired/malformed/duplicate receipt outcomes from Java/Broker fixtures. Treat receipt data as opaque to callers except safe accessors and avoid logging it by default. Do not claim lower-level POP calls provide a complete Push POP consumer.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Shared commands, evidence rules and integration prerequisites are in the execution index.

- [ ] **P13-V01** — Every supported Lite method has success/error differential evidence; seek versus late pull, pause/resume and commit/restart do not skip prefetched messages.
- [ ] **P13-V02** — POP/ACK/change-invisibility local tests cover success, expiry/redelivery, changed receipt, malformed extra-info and mixed-queue mapping.
- [ ] **P13-V03** — Every Java consumer-facing API is classified; supported entries have integration evidence and deferred rows are excluded from advertised parity.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-consumer --test lite_pop --locked
bash tests/differential/run.sh --phase 13
```

## 6. Acceptance and handoff

Do not start Phase 14 until this phase has either implemented the approved surface or recorded owner-approved gaps and passing lower-level baselines. No silent omission is allowed.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 14](15-phase-14-transaction-producer.md).

## 7. Open questions / decisions required

1. Which Lite Pull/public POP APIs are required for v1.0, and may any remain lower-level-only? This controls P13-01/02 and public facade examples; lower-level Phase 5 calls remain required.
