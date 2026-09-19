# Plan: Phase 5 — MQClientApi typed RPC facade

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 5; execution index: [`README.md`](README.md).
> Translation: [`06-phase-05-typed-rpc-api.zh-tw.md`](06-phase-05-typed-rpc-api.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## 1. Goal and scope

Implement one-attempt domain RPCs over Remoting, and make their local integration/differential tests runnable independently of a Producer or Consumer runtime.

## 2. Prerequisites and current-state analysis

Typed headers and transport from Phases 3–4 are accepted. Message binary decoding does not exist until Phase 8: return raw pull bytes in PullResultExt rather than duplicating that decoder here.

- Read/complete first: [Phase 4](05-phase-04-typed-headers-bodies.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `MQClientAPIImpl`
- `MQBrokerException`
- `MQClientException`
- `PullResultExt`
- `SendResult`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-core/Cargo.toml
crates/rocketmq-classic-core/src/api/mod.rs
crates/rocketmq-classic-core/src/api/route.rs
crates/rocketmq-classic-core/src/api/producer.rs
crates/rocketmq-classic-core/src/api/consumer.rs
crates/rocketmq-classic-core/src/api/transaction.rs
crates/rocketmq-classic-core/src/error.rs
crates/rocketmq-classic-core/tests/rpc.rs
integration-tests/Cargo.toml
integration-tests/tests/broker.rs
tests/differential/run.sh
```

MQClientApi owns or borrows a RemotingClient, not a process singleton. Broker methods take an explicit address, typed input and absolute deadline; no producer retry loop. NameServer address selection/failover follows the Java route path within one original deadline. Classify response code per method before decoding success-only fields. Keep raw response code, remark, request code and peer in structured errors; do not log credentials or message bodies.

## 4. Ordered implementation tasks

### 4.1 P5-01 — Create core and explicit error conversions

Add the core crate depending downward on remoting/protocol/common. Distinguish malformed protocol, connect, closed, timeout, Broker/NameServer response, invalid config and unsupported feature. Preserve error sources and raw unknown codes. Test each mapping using a scripted loopback peer instead of adding a one-implementation transport interface purely for mocking.

### 4.2 P5-02 — Implement route lookup

Build GET_ROUTEINFO_BY_TOPIC with the typed header and decode TopicRouteData. Cover existing/missing topic, stale/unavailable first NameServer, empty server list and malformed response. Do not silently manufacture a writable route. Route creation fallback for Producer is a Phase 7 policy, not a route-RPC success response.

### 4.3 P5-03 — Implement producer attempts

Add send V1/V2, batch and end-transaction methods. Map SEND_OK and storage/slave status outcomes exactly as Java; retain all SendResult identifiers and region/trace fields. Implement sync/future/oneway transport selection without choosing queues or retrying messages. For end transaction derive invocation mode from the Java call site.

### 4.4 P5-04 / P5-07 — Implement consumer methods and per-RPC status maps

Add pull, query/update offset, consumer list, send-back and lock/unlock. Pull returns status, next/min/max, suggested broker and raw binary body; non-found statuses are not generic errors. Offset-not-found remains distinct from transport failure. Respect Java oneway offset/unlock modes and per-operation successful response shapes.

### 4.5 P5-05 / P5-06 — Add lifecycle and extension RPCs

Implement heartbeat/unregister and source-confirmed heartbeat V2 negotiation when used. Add Lite/assignment/request-mode/POP/ACK/change-invisibility lower-level calls using Phase 4 models, with raw message bytes where decoding is deferred. Every family is inventoried even if public exposure is pending; never claim that an unsupported Broker response proves implementation compatibility.

### 4.6 P5-08 — Make RPC evidence executable

Create a non-published workspace test package named rocketmq-classic-integration. Its Broker tests are explicitly ignored in ordinary cargo test and require RMQ_NAMESRV_ADDR; running --ignored without configuration fails clearly rather than silently skipping. Build tests/differential/run.sh with --phase N: isolated setup, Java result, Rust result, comparator, nonzero on missing scenarios. Document required profiles and seed data. Pure tests still run without Docker.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Shared commands, evidence rules and integration prerequisites are in the execution index.

- [ ] **P5-V01** — Every method has scripted success, expected domain outcome, unknown error code and malformed-success-response tests.
- [ ] **P5-V02** — Each supported method runs against the pinned local Broker/NameServer, with Java success/error differential evidence.
- [ ] **P5-V03** — One Broker method call emits one attempt; NameServer failover and oneway paths obey their documented deadline/pending rules.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-core --test rpc --locked
RMQ_NAMESRV_ADDR=127.0.0.1:9876 cargo test -p rocketmq-classic-integration --test broker --locked -- --ignored
bash tests/differential/run.sh --phase 5
```

## 6. Acceptance and handoff

The core API can be exercised without public Producer/Consumer classes. Phase 6 receives route/heartbeat/unregister primitives; Phase 8 will attach message decoding to the existing raw pull result.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 6](07-phase-06-runtime-routing-heartbeat.md).
