# ADR-0015: Match pinned Fastjson scalar coercions in heartbeat DTO inputs

Status: Accepted

## Context

The Java 5.5.1 client decodes `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData` fields with Fastjson 2.0.63. The native DTOs already represented nullable fields and Java constructor defaults, but direct Serde decoding rejected scalar values that Fastjson converts and did not apply Java's primitive coercions consistently.

## Decision

Reuse the narrow `rocketmq-classic-common::serde_fastjson` field decoders for the recorded heartbeat inputs. The covered conversions include Java boolean inputs for `classFilterMode`, `unitMode`, and `withoutSub`; numeric and string inputs for `subVersion` and `heartbeatFingerprint`; scalar-to-string inputs for string fields; and nullable `tagsSet`/`codeSet` elements. The set decoders retain one null member and use Java scalar string/integer conversions for non-null values.

Existing DTO field types, defaults for absent fields, and serialization remain unchanged. Rust returns Serde errors for rejected values; it does not reproduce Fastjson's exception classes or claim full Fastjson grammar equivalence.

## Evidence

`RuntimeHeartbeatFixtures.heartbeatDtoInputCases` invokes 10 Java inputs covering the listed fields, including numeric strings/fractions/exponents, long boundaries and overflow, nulls, invalid values, and nested `ConsumerData`. `heartbeat_dto_inputs_match_invoked_java_fastjson` checks acceptance and normalized decoded/re-encoded values against the pinned oracle.

Heartbeat fixture SHA-256: `7c63961b4199288ce0b71d47e88e0e3e2157855e016dd44420845287cfee7609`.

Focused heartbeat tests, full workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verification, comparator/manifest, formatting, and diff checks pass. This closes these recorded input cases only; other DTO combinations, full Fastjson grammar, and whole-Phase 6 reconciliation remain open.
