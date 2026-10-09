# ADR-0055: Map remaining runtime body public methods

- Status: Accepted
- Date: 2026-10-09

## Context

The pinned RocketMQ 5.5.1 body DTOs expose bean methods and diagnostic text that were not covered by the existing input and runtime-flow checks. The Rust DTOs had the corresponding data but some had no named getter/setter methods or Java-shaped `Display` implementation.

## Decision

Add Rust snake_case accessors for `CheckClientRequestBody`, `GetConsumerListByGroupResponseBody`, `LockBatchRequestBody`, `UnlockBatchRequestBody`, `LockBatchResponseBody`, and `ConsumeMessageDirectlyResult`. Add `Display` implementations for lock/unlock request bodies and direct-consume results.

Keep Java constructor defaults: nullable check-client fields; nullable consumer IDs; empty lock/unlock queue Sets with false `onlyThisBroker`; an empty lock response Set; and direct-consume defaults of `order=false`, `autoCommit=true`, null result/remark, and zero elapsed milliseconds. The invoked Java strings for the three `Display` implementations are preserved, including nested `MessageQueue` text.

Rust setters take owned values and getters borrow them. Java getters return the identical mutable `SubscriptionData`, List, or Set object assigned by the setter. `ConsumerListByGroupResponseBody` now also exposes `get_consumer_id_list_mut`, so Rust callers can mutate the owned Vec through a mutable borrow and the DTO observes the updated contents. Java's cross-owner List identity is still not represented by Rust's move-based setter; the mutable collection behavior is mapped through Rust ownership. Other DTO aliases remain under audit.

## Consequences

The public field surfaces, constructor defaults, nullable setters, and tested diagnostic strings are mapped for these six DTOs. The wider Phase 6 public API and source/input audit remains incomplete; this ADR does not claim phase acceptance. P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct pinned Java calls: `checkClientBodyApiCase`, `consumerListApiCase`, `lockBatchRequestApiCase`, `unlockBatchRequestApiCase`, `lockBatchResponseApiCase`, and `consumeDirectResultApiCase` in `fixtures/runtime/lookup.json`.
- Rust differential: `remaining_runtime_body_public_methods_match_invoked_java`.
- Mutable consumer-list oracle commit: `0877435c`; mutable-borrow implementation commit: `dd4156c5`. Earlier method mapping commits: oracle `7e1155f8`, implementation `bca4329c`.
- Current aggregate `fixtures/runtime/lookup.json` SHA-256: `3d031033fb4f04d0ab78df0bf7818827dec7a460e8264812f303667ee42c020b`.
- Validation: full workspace/all-target tests; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes); `cargo fmt --all --check`; `git diff --check`.
- Oracle uses pinned RocketMQ 5.5.1 artifacts. The requested source checkout at `/Users/nanashi07/Projects/3rd/rocketmq` was unavailable.
