# ADR-0014: Match pinned Fastjson scalar coercions in route DTO inputs

Status: Accepted

## Context

The Java 5.5.1 client decodes route responses into `QueueData`, `BrokerData`, `TopicRouteData`, `TopicQueueMappingInfo`, and `MessageQueue` with Fastjson 2.0.63. The Rust DTOs previously used direct Serde field decoding. That rejected several inputs Java accepts and decoded Java's primitive null defaults differently.

## Decision

Add narrow JSON deserializers in `rocketmq-classic-common::serde_fastjson` and attach them to the affected DTO fields. The tested behavior includes scalar-to-string conversion, Java primitive defaults for null, integer strings and fractions, boolean inputs, normalized numeric map keys, nullable Broker addresses, and scalar members of route filter lists. Invalid integer ranges, invalid boolean strings, and null static-mapping values remain rejected.

The helpers keep Rust DTO field types and serialization unchanged. Java exception classes are not reproduced; Rust exposes Serde errors at the decode boundary. HashMap/ConcurrentHashMap iteration order and untested Fastjson grammar are not asserted equivalent.

## Evidence

`RuntimeRouteFixtures.routeDtoInputCases` invokes 25 actual inputs for `QueueData`, `MessageQueue`, `BrokerData`, `TopicQueueMappingInfo`, and nested `TopicRouteData`. The five added `MessageQueue.queueId` cases cover explicit null to zero, signed integer boundaries, fractional truncation, and overflow rejection. `route_dto_scalar_inputs_match_invoked_java_fastjson` compares accepted fields and rejected outcomes against the same Java inputs.

Route fixture SHA-256: `5cc8ace8e2a4622f7aa849270b77ff6b3adf2f04cd99dd6d6563566744c8a104`.

Focused differential, full workspace tests, strict all-feature Clippy, pinned offline Maven verification, comparator/manifest, formatting, and diff checks pass. This closes the recorded scalar cases only; broader DTO input, Fastjson grammar, and whole-Phase 6 reconciliation remain open.
