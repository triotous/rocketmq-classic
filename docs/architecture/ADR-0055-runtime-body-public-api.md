# ADR-0055: Map remaining runtime body public methods

- Status: Accepted
- Date: 2026-10-09

## Context

The pinned RocketMQ 5.5.1 body DTOs expose bean methods and diagnostic text that were not covered by the existing input and runtime-flow checks. The Rust DTOs had the corresponding data but some had no named getter/setter methods or Java-shaped `Display` implementation.

## Decision

Add Rust snake_case accessors for `CheckClientRequestBody`, `GetConsumerListByGroupResponseBody`, `LockBatchRequestBody`, `UnlockBatchRequestBody`, `LockBatchResponseBody`, and `ConsumeMessageDirectlyResult`. Add `Display` implementations for lock/unlock request bodies and direct-consume results.

Keep Java constructor defaults: nullable check-client fields; nullable consumer IDs; empty lock/unlock queue Sets with false `onlyThisBroker`; an empty lock response Set; and direct-consume defaults of `order=false`, `autoCommit=true`, null result/remark, and zero elapsed milliseconds. Preserve the invoked Java strings for the three `Display` implementations, including nested `MessageQueue` text.

This ADR's initial accessors used owned values plus mutable borrows. The alias status has since been superseded for `CheckClientRequestBody.subscriptionData` by [ADR-0060](ADR-0060-shared-java-dto-reference-aliases.md) and for `GetConsumerListByGroupResponseBody.consumerIdList` by [ADR-0061](ADR-0061-shared-consumer-list-alias.md). The two DTOs now use shared handles to preserve caller/getter identity. The three lock Set containers now use shared handles under [ADR-0062](ADR-0062-shared-lock-set-aliases.md), and `BatchAckMessageRequestBody.acks` now uses a shared List handle under [ADR-0063](ADR-0063-shared-batch-ack-list-alias.md). Aliases to mutable `MessageQueue` elements within lock Sets, individual BatchAck elements, BatchAck BitSet objects, and other DTO fields remain under audit.

## Consequences

The public fields, defaults, nullable setters, and tested diagnostic strings are mapped for these six DTOs. The wider Phase 6 public API and source/input audit remains incomplete; this ADR does not claim phase acceptance. P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct pinned Java calls: `checkClientBodyApiCase`, `consumerListApiCase`, `lockBatchRequestApiCase`, `unlockBatchRequestApiCase`, `lockBatchResponseApiCase`, and `consumeDirectResultApiCase` in `fixtures/runtime/lookup.json`.
- Rust differential: `remaining_runtime_body_public_methods_match_invoked_java`.
- Original method mapping: Java oracle `7e1155f8`, Rust implementation `bca4329c`.
- Later shared-alias implementations: CheckClient `a732974a`; consumer list `456849fb`; lock Set `045b46e7`; BatchAck List `a32d00f7`. Assignment Set getter mutation remains owned-value behavior.
- Current aggregate `fixtures/runtime/lookup.json` SHA-256: `0f0c32b433f81c717aab9fe6bcb4767466eacb4e08343d8c76b3c965dc9e1654`.
- Validation: workspace/all-target tests; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes); comparator self-test and manifest22; format and diff checks.
- Oracle uses pinned RocketMQ 5.5.1 artifacts. The requested source checkout at `/Users/nanashi07/Projects/3rd/rocketmq` was unavailable.
