# Plan: Phase 15 — Request/reply and exactly-once completion

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 15; execution index: [`README.md`](README.md).
> Translation: [`16-phase-15-request-reply.zh-tw.md`](16-phase-15-request-reply.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## Execution contract

- Port **all Java client 5.5.1 settings, features and control flows**, including defaults, validation, callbacks, errors, retries and lifecycle. Map Java source/method/configuration to Rust and unit-test IDs in both directions; no extra public Rust setting/feature and no missing Java client capability. Rust naming/ownership may differ, not observable semantics. Internal test seams stay private.
- Implement the **client only**. Java client 5.5.1 remains the oracle; **RocketMQ server V5_3_4 (5.3.4)** is the production/local Docker baseline, not the client source baseline. A server limitation does not authorize deleting a client feature; compare both clients against the same server and retain source/fixture coverage for unavailable success paths.
- **Phase 0–18 primarily use unit tests**, supplemented by Java golden/offline differential tests and small scripted loopback tests where transport requires them. No live Broker or Docker is a phase prerequisite. Broker, TLS/auth cluster, cross-language, end-to-end and server stress execution belongs to [Phase 19](20-phase-19-docker-local-validation.md); non-local checks become owner-executed manual items there. Phase acceptance is not final compatibility/release acceptance.

## 1. Goal and scope

Implement message-level correlation, reply construction and Broker-pushed reply delivery, independently of Remoting opaque matching.

## 2. Prerequisites and current-state analysis

Phases 7–8 producer/decoder and Phase 6 reply processor slot are ready; transaction work is accepted. Reuse Phase 4 reply headers/properties and add any missing Phase 5 reply-send methods without duplicating transport.

- Read/complete first: [Phase 14](15-phase-14-transaction-producer.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `DefaultMQProducerImpl request methods`
- `RequestFutureHolder`
- `RequestResponseFuture`
- `RequestCallback`
- `MessageUtil.createReplyMessage`
- `ClientRemotingProcessor.receiveReplyMessage`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-producer/src/request_reply.rs
crates/rocketmq-classic-core/src/api/producer.rs
crates/rocketmq-classic-producer/tests/request_reply.rs
examples/request_reply.rs
```

The pending correlation map is owned at the shared-client dispatch boundary with a producer-registration owner token; producers sharing a runtime cannot replace each other’s reply handler or clear each other’s requests. Each entry has deadline and oneshot sender; atomic removal is completion. Remoting opaque and RocketMQ correlation ID remain separate. TTL is the Java wire duration property while internal waiting uses one monotonic deadline.

## 4. Ordered implementation tasks

### 4.1 P15-01 / P15-02 — Prepare correlation and lifecycle ownership

Use exact Java correlation/reply-to/TTL property constants and ID format semantics. Ensure runtime client identity and Broker heartbeat registration are ready before request delivery, following Java preparation behavior. Bound pending requests using existing admission controls. Register one shared reply dispatcher and an owned timeout scheduler, not one callback executor per request.

### 4.2 P15-03 — Register before send and preserve one deadline

Insert correlation before sending. Route acquisition, heartbeat preparation, producer retries and reply wait consume the original request deadline. On send failure remove-and-fail only if this path still owns the entry; a fast reply may already have completed it. Caller cancellation removes its entry; producer shutdown drains only its own tokens. Late send completion must not recreate timed-out requests.

### 4.3 P15-04 — Implement responder construction and inbound reply

Add a public reply-message helper matching MessageUtil behavior, preserving correlation, reply-to, TTL/topic/cluster metadata and validating missing request properties. Send replies through SEND_REPLY_MESSAGE/V2 as selected by Java, not an ordinary request send. Decode PUSH_REPLY_MESSAGE_TO_CLIENT with bounded body handling, claim correlation and return the Java-compatible Remoting acknowledgement even for duplicate/late replies where Java does. Unknown correlation must not allocate a new pending entry.

### 4.4 P15-05 / P15-06 — Make the 5.5.1 regression deterministic

Use one remove-and-complete path for reply, timeout, cancellation, send failure and shutdown. Add a barrier-controlled test where reply and timeout are simultaneously eligible; instrument terminal ownership rather than merely counting Future polls. Test duplicate reply frames and response-before-send-completion. Run stress repetitions after deterministic tests. Exactly-once completion does not mean exactly-once request processing or reply delivery.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Run these checks without Docker or a live Broker; use Java fixtures, controlled clocks and scripted peers. Shared commands and evidence rules are in the execution index.

- [ ] **P15-V01** — Property/reply-header golden fixtures match Java; invalid request metadata fails before send.
- [ ] **P15-V02** — All terminal races yield one owner and zero pending entries, including two producers sharing one runtime and shutting down independently.

- [ ] **P15-U01** — Java request/reply fixtures and injected reply/no-reply/late-reply events verify both roles, correlation and timeout semantics.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-producer --test request_reply --locked
```

### 5.2 Phase 19 live-test handoff

Retain these case IDs and behavior requirements for Phase 19 execution against Docker server 5.3.4; they are not this phase's pass conditions and must not be marked PASS early.

- [ ] **P15-V03** — Java requester↔Rust responder and Rust requester↔Java responder work against the local Broker; no responder and late reply produce compatible timeouts.

## 6. Acceptance and handoff

The 5.5.1 multiple-callback regression has a deterministic runnable test and stress evidence. Correlation tasks and registrations are included in the release leak inventory.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 16](17-phase-16-namespace-security-operations.md).
