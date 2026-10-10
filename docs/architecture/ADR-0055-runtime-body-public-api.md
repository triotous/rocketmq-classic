# ADR-0055: Map remaining runtime body public methods

- Status: Accepted
- Date: 2026-10-09

## Context

The pinned RocketMQ 5.5.1 body DTOs expose bean methods and diagnostic text that were not covered by the existing input and runtime-flow checks. The Rust DTOs had the corresponding data but some had no named getter/setter methods or Java-shaped `Display` implementation.

## Decision

Add Rust snake_case accessors for `CheckClientRequestBody`, `GetConsumerListByGroupResponseBody`, `LockBatchRequestBody`, `UnlockBatchRequestBody`, `LockBatchResponseBody`, and `ConsumeMessageDirectlyResult`. Add `Display` implementations for lock/unlock request bodies and direct-consume results.

Keep Java constructor defaults: nullable check-client fields; nullable consumer IDs; empty lock/unlock queue Sets with false `onlyThisBroker`; an empty lock response Set; and direct-consume defaults of `order=false`, `autoCommit=true`, null result/remark, and zero elapsed milliseconds. Preserve the invoked Java strings for the three `Display` implementations, including nested `MessageQueue` text.

This ADR's initial accessors used owned values plus mutable borrows. The alias status has since been superseded for `CheckClientRequestBody.subscriptionData` by [ADR-0060](ADR-0060-shared-java-dto-reference-aliases.md) and for `GetConsumerListByGroupResponseBody.consumerIdList` by [ADR-0061](ADR-0061-shared-consumer-list-alias.md). The two DTOs now use shared handles to preserve caller/getter identity. Lock request/response Sets and other alias-bearing DTO fields still need direct Java evidence and separate parity work.

## Consequences

The public fields, defaults, nullable setters, and tested diagnostic strings are mapped for these six DTOs. The wider Phase 6 public API and source/input audit remains incomplete; this ADR does not claim phase acceptance. P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct pinned Java calls: `checkClientBodyApiCase`, `consumerListApiCase`, `lockBatchRequestApiCase`, `unlockBatchRequestApiCase`, `lockBatchResponseApiCase`, and `consumeDirectResultApiCase` in `fixtures/runtime/lookup.json`.
- Rust differential: `remaining_runtime_body_public_methods_match_invoked_java`.
- Original method mapping: Java oracle `7e1155f8`, Rust implementation `bca4329c`.
- Later alias implementations: CheckClient `a732974a`; consumer list `456849fb`; lock Set `c8aeb821`; assignment Set `b280111c`; BatchAck List `f4d85af0`.
- Current aggregate `fixtures/runtime/lookup.json` SHA-256: `372a5250f6af3ced4c17ef23eb9e05dbe78523ceb6b75a4cd03fe1589fabf80e`.
- Validation: workspace/all-target tests; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes); comparator self-test and manifest22; format and diff checks.
- Oracle uses pinned RocketMQ 5.5.1 artifacts. The requested source checkout at `/Users/nanashi07/Projects/3rd/rocketmq` was unavailable.
