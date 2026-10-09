# ADR-0055: Map remaining runtime body public methods

- Status: Accepted
- Date: 2026-10-09

## Context

The pinned RocketMQ 5.5.1 body DTOs expose bean methods and diagnostic text that were not covered by the existing input and runtime-flow checks. The Rust DTOs had the corresponding data but some had no named getter/setter methods or Java-shaped `Display` implementation.

## Decision

Add Rust snake_case accessors for `CheckClientRequestBody`, `GetConsumerListByGroupResponseBody`, `LockBatchRequestBody`, `UnlockBatchRequestBody`, `LockBatchResponseBody`, and `ConsumeMessageDirectlyResult`. Add `Display` implementations for lock/unlock request bodies and direct-consume results.

Keep Java constructor defaults: nullable check-client fields; nullable consumer IDs; empty lock/unlock queue Sets with false `onlyThisBroker`; an empty lock response Set; and direct-consume defaults of `order=false`, `autoCommit=true`, null result/remark, and zero elapsed milliseconds. The invoked Java strings for the three `Display` implementations are preserved, including nested `MessageQueue` text.

Rust setters take owned values and getters borrow them. Java getters return the identical mutable `SubscriptionData`, List, or Set object assigned by the setter. `CheckClientRequestBody` exposes `get_subscription_data_mut`; `ConsumerListByGroupResponseBody` exposes `get_consumer_id_list_mut`; `LockBatchRequestBody` and `UnlockBatchRequestBody` expose `get_mq_set_mut`; and `LockBatchResponseBody` exposes `get_lock_okmq_set_mut`. These mutable borrows reproduce mutations through each DTO getter. Rust's move-based setters still do not preserve Java's cross-owner SubscriptionData/List/Set identity with the caller's original value. Other DTO aliases remain under audit.

## Consequences

The public field surfaces, constructor defaults, nullable setters, and tested diagnostic strings are mapped for these six DTOs. The wider Phase 6 public API and source/input audit remains incomplete; this ADR does not claim phase acceptance. P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct pinned Java calls: `checkClientBodyApiCase`, `consumerListApiCase`, `lockBatchRequestApiCase`, `unlockBatchRequestApiCase`, `lockBatchResponseApiCase`, and `consumeDirectResultApiCase` in `fixtures/runtime/lookup.json`.
- Rust differential: `remaining_runtime_body_public_methods_match_invoked_java`.
- Mutable consumer-list oracle/implementation commits: `0877435c` / `dd4156c5`; mutable lock/unlock Set commits: `f80a844d` / `c8aeb821`; mutable CheckClient SubscriptionData commits: `cd8e761e` / `ecf19a87`. Earlier method mapping commits: oracle `7e1155f8`, implementation `bca4329c`.
- Current aggregate `fixtures/runtime/lookup.json` SHA-256: `f3d9e92e8965faa082e06f075755bed7b9fc7d0435698b35a58d19ac809d8c29`.
- Validation: full workspace/all-target tests; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes); `cargo fmt --all --check`; `git diff --check`.
- Oracle uses pinned RocketMQ 5.5.1 artifacts. The requested source checkout at `/Users/nanashi07/Projects/3rd/rocketmq` was unavailable.
