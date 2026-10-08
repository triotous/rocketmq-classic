# ADR-0015: Match pinned Fastjson scalar coercions in heartbeat DTO inputs

Status: Accepted

## Context

The Java 5.5.1 client decodes `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData` fields with Fastjson 2.0.63. The native DTOs already represented nullable fields and Java constructor defaults, but direct Serde decoding rejected scalar values that Fastjson converts and did not apply Java's primitive coercions consistently.

## Decision

Reuse the narrow `rocketmq-classic-common::serde_fastjson` field decoders for the recorded heartbeat inputs. The covered conversions include Java boolean inputs for `classFilterMode`, `unitMode`, and `withoutSub`; numeric and string inputs for `subVersion` and `heartbeatFingerprint`; scalar-to-string inputs for string fields; and nullable `tagsSet`/`codeSet` elements. The set decoders retain one null member and use Java scalar string/integer conversions for non-null values. The shared parser also normalizes the directly observed Fastjson forms: leading-plus/zero-prefixed numeric values, form feed outside strings, and a BOM at document start. It leaves the tested vertical tab and embedded BOM rejected.

Existing DTO field types, defaults for absent fields, and serialization remain unchanged. Rust returns Serde errors for rejected values; it does not reproduce Fastjson's exception classes or claim full Fastjson grammar equivalence.

## Evidence

`RuntimeHeartbeatFixtures.heartbeatDtoInputCases` invokes 17 Java inputs covering the listed fields, including numeric strings/fractions/exponents, long boundaries and overflow, nulls, invalid values, and nested `ConsumerData`. Seven direct grammar probes (`0694089`) show Java accepts `+02`, form feed around a token and document prefix, and a leading BOM; it rejects vertical tab and an embedded BOM. The original Rust differential failed on `subscription-leading-plus-zero-version` before `b7f19c7`; required case IDs are locked by `1036b3c`. `heartbeat_dto_inputs_match_invoked_java_fastjson` checks acceptance and normalized decoded/re-encoded values against the pinned oracle.

`HeartbeatData` now exposes `java_decode`, `java_decode_nullable`, `java_decode_list`, `java_decode_list_nullable`, `java_from_json`, and `java_from_json_nullable` alongside its existing JSON encoder. Direct utility probes (`45404c6`) establish null bytes/strings, JSON `null`, null list input, and a retained null member in `[null,{}]`; tests compare these with the nullable adapters. The heartbeat decoder uses the shared Fastjson input parser from `serde_fastjson`.

Heartbeat fixture SHA-256: `c38bc0a92efe98fdf891fd289e6f1837e53dc979887d57a8d8d7ddeea8dbc994`.

Focused heartbeat tests, full workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verification, comparator/manifest, formatting, and diff checks pass. This closes these recorded input cases only; other DTO combinations, full Fastjson grammar, and whole-Phase 6 reconciliation remain open.
