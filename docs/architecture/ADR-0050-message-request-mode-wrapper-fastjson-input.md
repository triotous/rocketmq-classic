# ADR-0050: Match MessageRequestModeSerializeWrapper Java defaults and Fastjson input

- Status: Accepted
- Date: 2026-10-09

## Context

MessageRequestModeSerializeWrapper held an optional Rust map with a derived Default and Deserialize implementation. Pinned RocketMQ 5.5.1 initializes its map in the public no-argument constructor, exposes getter/setter methods, and uses Fastjson's last-property-wins object behavior. The derived Rust behavior therefore produced null instead of the Java default empty map and rejected repeated wrapper fields.

## Decision

Retain the nullable public Rust map type. Implement Default with an empty map, expose Java-shaped getter and setter methods, omit an explicit null map during serialization, and decode through the shared raw-object last-wins helper. Missing fields use the Java constructor default; explicit null remains null.

The 33-case Java Fastjson corpus covers seven roots, token shapes at the wrapper/outer-map/inner-map levels, duplicate wrapper fields and map keys, and malformed null/scalar/array entries. A separate direct Java API case invokes constructor, getter, and setter behavior. Null entries below the outer and inner map levels reject, matching Java's ConcurrentHashMap null-value boundary. Rust BTreeMap output is deterministic; this decision does not claim Java ConcurrentHashMap traversal order.

## Consequences

The tested constructor, public accessor, wrapper input, and normalized output behavior now match pinned Java 5.5.1. The Rust field remains optional so callers can represent the Java setter's explicit-null state. This DTO closure does not change the Phase 6 PARTIAL status or P6-V04 ownership.

## Evidence

- Java input fixture: messageRequestModeWrapperCases in fixtures/runtime/lookup.json (33 cases); direct API observation: messageRequestModeWrapperApiCase. Current fixture SHA-256: 9d4472bee58e739dbb0efa4c18a3e4405f7b58fd1e1975f862bbc431b61a016f.
- Oracle/input commit: 4456695d; direct API oracle commit: 910e6838; implementation and Rust differential assertion commit: 33b993b8.
- Validation: cargo test --workspace --all-targets --locked; cargo clippy --workspace --all-targets --all-features --locked -- -D warnings; pinned offline Maven verify with fixture drift and cross-decodes (22 scenarios); cargo fmt --all --check; git diff --check.
- The unavailable Java source checkout was not used; the oracle executes pinned RocketMQ 5.5.1 artifacts. Phase 6 remains PARTIAL and P6-V04 stays PENDING_PHASE_19.
