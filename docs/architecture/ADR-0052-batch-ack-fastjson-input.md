# ADR-0052: Match BatchAck Fastjson input and defaults

- Status: Accepted
- Date: 2026-10-09

## Context

BatchAckMessageRequestBody and BatchAck used derived Serde decoding, which rejects inputs accepted by pinned RocketMQ 5.5.1 Fastjson and does not preserve Java constructor defaults. The remoting artifact defines acks as a Java List, primitive numeric fields default to zero, string fields use Fastjson coercion, and the BitSet codec uses Base64 with empty values omitted on encoding.

## Decision

Use Fastjson-compatible last-property-wins decoders for both DTO levels. Preserve nullable rows in acks, accept the observed singleton-object and Boolean-to-null-list conversions, apply Java string and primitive numeric coercions/defaults, and decode the BitSet through its Base64 adapter. Omit null and empty BitSets when encoding.

The 95-case Java corpus invokes BatchAckMessageRequestBody.decode and encode. It covers seven root shapes, each JSON token kind for brokerName, acks, and every compact BatchAck field, list null/singleton/duplicate states, duplicate properties, unknown fields, and Base64 inputs.

## Consequences

The tested Java 5.5.1 input and encode behavior now matches the native body DTO. BatchAckMessageRequestBody.acks uses Option<Vec<Option<BatchAck>>> to preserve Java null list members. This DTO boundary does not change the Phase 6 PARTIAL status or P6-V04 ownership.

## Evidence

- Direct Java corpus: batchAckBodyInputCases in fixtures/runtime/lookup.json (95 cases); SHA-256 39374bf2374e2ae745ea82335762d17ad05cde2b987c05bc0759b808e984267d.
- Oracle and fixture commit: 58ad8480; implementation and Rust differential: 381b6edf.
- Validation: focused BatchAck differential; cargo test --workspace --all-targets --locked; strict all-target/all-feature Clippy; pinned offline Maven verify (fixture drift and Java cross-decodes, 22 RPC scenarios); cargo fmt --all --check; git diff --check.
- Java behavior was invoked from pinned RocketMQ 5.5.1 artifacts; no alternate Java source checkout was used.
