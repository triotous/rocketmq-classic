# ADR-0058: Heartbeat DTO public collection accessors

- Status: Accepted
- Date: 2026-10-09

## Context

Pinned RocketMQ 5.5.1 declares Java bean getters and setters for all fields on `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData`. Rust represented most of these DTO values as public fields, so the declared Java method surface had not been directly checked.

## Decision

Expose Rust-idiomatic accessors for every declared getter/setter on the four DTOs, with mutable borrows for each collection field. Keep nullable strings, enums, and collections explicit with `Option`; preserve Java Set contents and null-member behavior in the existing native collection representations.

Direct Java calls and Rust comparisons cover constructor defaults, every property after assignment, null setter results, and mutation through the collection getters on `SubscriptionData`, `ConsumerData`, and `HeartbeatData`. `SubscriptionData.subVersion` uses a time-window assertion for its constructor-time value. Java getters return the same mutable Set assigned by a setter. Rust exposes mutable borrows of its owned HashSets/vectors and matches getter-side mutation, but does not preserve identity with the caller-owned Java Set.

## Consequences

Callers can use named methods that correspond to Java's public bean API while Rust retains owned collection semantics. `HeartbeatData` producer and consumer vectors continue to preserve identity-distinct rows and multiplicity; the new accessors do not change serialization or runtime preparation.

This closes the four heartbeat DTO getter/setter method surfaces and matches mutation through the collection getters. Caller-owned Set alias identity and the wider Phase 6 source/API/input audit remain open; Phase 6 stays PARTIAL and P6-V04 stays `PENDING_PHASE_19`.

## Evidence

- Pinned `javap -public` signatures for `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData` in RocketMQ 5.5.1.
- Java direct cases: `subscriptionDataApiCase`, `producerDataApiCase`, `consumerDataApiCase`, and `heartbeatDataApiCase` in `fixtures/runtime/heartbeat.json`.
- Rust differential: `heartbeat_dto_public_accessors_match_invoked_java`.
- Original API oracle/implementation: `21591fc7`; mutable Set mutation oracle/implementation: `33829680` / `5151a308`.
- Current heartbeat fixture SHA-256: `a03cd054471d1782251f8635adcc35d6da29f67f9900c215eadf67f76f5d9c30`.
- Validation: full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes), comparator self-test/manifest22, formatting, and diff checks.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate Java source checkout was unavailable.
