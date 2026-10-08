# ADR-0015: Match pinned Fastjson scalar coercions in heartbeat DTO inputs

Status: Accepted

## Context

The Java 5.5.1 client decodes `SubscriptionData`, `ProducerData`, `ConsumerData`, and `HeartbeatData` fields with Fastjson 2.0.63. The native DTOs already represented nullable fields and Java constructor defaults, but direct Serde decoding rejected scalar values that Fastjson converts and did not apply Java's primitive coercions consistently.

## Decision

Reuse the narrow `rocketmq-classic-common::serde_fastjson` field decoders for the recorded heartbeat inputs. The covered conversions include Java boolean inputs for `classFilterMode`, `unitMode`, and `withoutSub`; numeric and string inputs for `subVersion` and `heartbeatFingerprint`; scalar-to-string inputs for string fields; and nullable `tagsSet`/`codeSet` elements. Set shape follows the invoked Java coercions: `tagsSet` accepts a singleton string or object (object values become compact JSON strings), while scalar numbers and Booleans reject; `codeSet` accepts singleton numbers, strings, Booleans, and objects (objects become zero). Array elements use the corresponding Java conversions and Set deduplication. `subVersion` rejects object and array inputs, including an empty object, while the route DTO's separate i32 coercion retains its observed empty-object behavior. The shared parser also normalizes the directly observed Fastjson forms: leading-plus/zero-prefixed numeric values, form feed outside strings, and a BOM at document start. It leaves the tested vertical tab and embedded BOM rejected.

Existing DTO field types, defaults for absent fields, and serialization remain unchanged. Rust returns Serde errors for rejected values; it does not reproduce Fastjson's exception classes or claim full Fastjson grammar equivalence.

## Evidence

`RuntimeHeartbeatFixtures.heartbeatDtoInputCases` now invokes 43 Java inputs covering scalar conversions, numeric boundaries, set shapes, nulls, invalid values, nested DTOs, and object/array-to-String coercion. The 21-case string-coercion baseline showed Java preserves object-key order; `serde_fastjson::deserialize_nullable_string` uses a private ordered value (`87dca73`). Set-shape probes show singleton object coercion, scalar rejection for `tagsSet`, Boolean/object conversion for `codeSet`, and HashSet deduplication. Numeric boundary probes cover i32 minimum/maximum, overflow rejection, exponent strings, wide decimal narrowing, and rejection of empty objects/arrays for `subVersion`. The differential first failed on `tagsSet` supplied as a singleton string (`53add8a`), then later on the empty-object `subVersion` case (`f55e95b`); required-case test commits are `ec1d98d` and `f55e95b`, and implementation commits `478d6e0` and `f78269f`. The shared grammar probes show Java accepts `+02`, form feed around a token and document prefix, and a leading BOM; it rejects vertical tab and an embedded BOM. `heartbeat_dto_inputs_match_invoked_java_fastjson` checks acceptance and normalized decoded/re-encoded values against the pinned oracle.

`HeartbeatData` now exposes `java_decode`, `java_decode_nullable`, `java_decode_list`, `java_decode_list_nullable`, `java_from_json`, and `java_from_json_nullable` alongside its existing JSON encoder. Direct utility probes (`45404c6`) establish null bytes/strings, JSON `null`, null list input, and a retained null member in `[null,{}]`; tests compare these with the nullable adapters. The heartbeat decoder uses the shared Fastjson input parser from `serde_fastjson`.

The latest heartbeat input oracle/test commits are `f55e95b` and `ec1d98d`; the implementation follow-ups are `478d6e0` and `f78269f`. Current heartbeat fixture SHA-256: `bc147764feb4ddfca190c098eb014f585148e7b8989c804922acae5611384612`.

Focused heartbeat and route DTO differentials, full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verification (22 scenarios), comparator self-test and manifest validation, formatting, and diff checks pass. This closes these recorded input cases only; other DTO combinations, full Fastjson grammar, and whole-Phase 6 reconciliation remain open.
