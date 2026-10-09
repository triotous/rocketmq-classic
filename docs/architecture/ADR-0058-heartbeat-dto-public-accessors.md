# ADR-0058: Map heartbeat DTO public accessors

- Status: Accepted
- Date: 2026-10-09

## Context

Pinned RocketMQ 5.5.1 declares Java bean getters and setters for all fields on `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData`. Rust represented most of these DTO values as public fields, so the declared Java method surface had not been directly checked.

## Decision

Expose Rust-idiomatic accessors for every declared getter/setter on the four DTOs. Keep nullable strings, enums, and collections explicit with `Option`; preserve Java Set contents and null-member behavior in the existing native collection representations.

Direct Java calls and Rust comparisons cover constructor defaults, every property after assignment, and null setter results. `SubscriptionData.subVersion` uses a time-window assertion for its constructor-time value. Java getters return the same mutable Set assigned by a setter. Rust setters take ownership and getters lend immutable references, so collection alias identity remains a documented semantic difference.

## Consequences

Callers can use named methods that correspond to Java's public bean API while Rust retains owned collection semantics. `HeartbeatData` producer and consumer vectors continue to preserve identity-distinct rows and multiplicity; the new accessors do not change serialization or runtime preparation.

This closes the four heartbeat DTO getter/setter method surfaces. Collection aliases and the wider Phase 6 source/API/input audit remain open; Phase 6 stays PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

## Evidence

- Pinned `javap -public` signatures for `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData` in RocketMQ 5.5.1.
- Java direct cases: `subscriptionDataApiCase`, `producerDataApiCase`, `consumerDataApiCase`, and `heartbeatDataApiCase` in `fixtures/runtime/heartbeat.json`.
- Rust differential: `heartbeat_dto_public_accessors_match_invoked_java`.
- Implementation/oracle commit: `21591fc7`.
- Heartbeat fixture SHA-256: `7da03948a79dd50a476ba900198731af6186efd735c473e54688853f3209cd87`.
- Validation: full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), formatting, and diff checks.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate Java source checkout was unavailable.
