# ADR-0053: Map BatchAck public methods

- Status: Accepted
- Date: 2026-10-09

## Context

The pinned RocketMQ 5.5.1 `BatchAck` API exposes a no-argument constructor, getters and setters for nine properties, and `toString()`. `BatchAckMessageRequestBody` exposes getters and setters for `brokerName` and `acks`. The Rust DTOs previously matched wire input but did not expose the corresponding method surface or Java diagnostic text.

## Decision

Expose Rust snake_case getters and setters for all nine `BatchAck` properties and both body properties. Use `Default` for Java's no-argument constructor values, and implement `Display` so `BatchAck::to_string()` matches the invoked Java output, including BitSet index formatting.

Rust represents the Java `BitSet` as an owned `BitSet` value and serializes its canonical bytes as Base64. The Java `List<BatchAck>` is represented as `Option<SharedValue<Vec<Option<BatchAck>>>>`; cloned getter handles and setter arguments retain the same outer List identity, while serialization keeps the existing JSON shape. The DTO deliberately has no structural `PartialEq`, `Eq`, or `Hash` implementation, matching Java's inherited identity-based `Object.equals` and `Object.hashCode`. Caller-held mutable `BatchAck` element and `BitSet` object identity remain separate open alias surfaces.

## Consequences

The invoked constructor, getter, setter, and `toString()` values match the pinned Java oracle. The `BatchAckMessageRequestBody.acks` setter and getter preserve one shared List, and mutations through either handle are visible through the other. The body uses Java identity equality semantics rather than structural Rust equality. Mutable aliases to individual `BatchAck` objects and their caller-owned `BitSet` values remain open in the wider Phase 6 reconciliation; the overall phase remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct pinned Java invocation: `batchAckApiCase` in `fixtures/runtime/lookup.json`, covering defaults, all property readbacks, `toString()`, body defaults and values, assigned-list identity/mutations, Object equality/hash declaring classes and outcomes, and null setter behavior.
- Original API oracle/implementation: `7f3a4f0e` / `c709e743`; mutable-list oracle/implementation: `51b92e77` / `f4d85af0`.
- Current aggregate `fixtures/runtime/lookup.json` SHA-256: `0f0c32b433f81c717aab9fe6bcb4767466eacb4e08343d8c76b3c965dc9e1654`.

- Aggregate fixture SHA-256 at this ADR's BatchAck API checkpoint: `d16675236d7b63ddc1ffd69c80df896370479f952f0af9f961214ed7d0237632`. Later DTO method oracles update the aggregate; see [ADR-0055](ADR-0055-runtime-body-public-api.md) for the current digest.
- Validation: `cargo test --workspace --all-targets --locked`; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios, fixture drift, Java cross-decodes); `cargo fmt --all --check`; `git diff --check`.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate Java source checkout was unavailable.

## Mutable BitSet getter continuation

The pinned Java `BatchAck.getBitSet()` returns the same mutable `BitSet` passed to `setBitSet`; this nested object identity is still not represented by the outer List correction. Mutating bit 5 through the getter is visible through both the getter and setter-held Java object. Rust adds `get_bit_set_mut()` for its owned `BitSet.toByteArray()` byte representation, matching the updated serialized bytes without introducing a Java-style mutable `BitSet` object.

- Direct Java evidence: `batchAckApiCase` in `fixtures/runtime/lookup.json`; Rust differential: `batch_ack_public_methods_match_invoked_java`.
- Oracle/test commit: `bd3d2e71`; implementation commit: `d1dc7dfd`.
- Checkpoint aggregate `fixtures/runtime/lookup.json` SHA-256: `abfe31546b68799aced43ff4f631f5dea37a4c14efac1dfc60a1ed637a22b970`.
- Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Rust does not preserve identity with the caller-owned Java `BitSet`; Phase 6 remains PARTIAL and P6-V04 stays PENDING_PHASE_19.

## Java-compatible BitSet value continuation

`BatchAck.bit_set` now uses an owned Rust `BitSet` value rather than exposing raw Base64 bytes as its mutation API. It maps the functional Java surface: index and range set/clear/flip/get, boolean setter overloads, next/previous set/clear searches, `intersects`, `and/or/xor/andNot`, cardinality, length, emptiness, size/capacity, byte and long-array values, set-bit iteration, equality, Java hash/text, and canonical `toByteArray()` encoding. BatchAck JSON keeps the existing Base64 field and omits an empty bit set as before. Negative indexes and invalid ranges return typed `BitSetError` values; the direct Java oracle confirms those operations throw `IndexOutOfBoundsException`.

- Direct Java evidence covers bits 0, 2, 5, 7, 8, 63, 64, and 127; clear/flip, cardinality, length, canonical bytes, and negative-index failures.
- Oracle/test commits: `ce97d07c` and `98d26175`; implementation/test commit: `7d2f2b98`.
- `fixtures/runtime/lookup.json` SHA-256: `4ebfec17860c569431bfb2b7b952f59c83b3eb08d683b6198e4f24d6143cb400`.
- Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios), comparator self-test/manifest22, formatting, and diff checks pass.

The direct oracle covers word-boundary crossing at bits 63–65, capacity growth and clear behavior, slices, signed long-word output, Java hash/text/stream results, bulk operations, and equality independent of capacity. Rust maps byte/long array values with slices; it does not preserve a Java NIO buffer's cursor or identity, nor identity with a separately held Java `BitSet`. Oracle commits `4196f933`, `2953f259`, `9dd083ed`, `79328f27`, and `a5385223`; implementation/test commit `0f600297`; current lookup fixture SHA-256 `4a8f9a0b49c6e5acdcef48ee9da7bc9d81c5d28361d79f8a8400dd6636ae0719`. Whole Phase 6 API/source reconciliation remains open and P6-V04 stays PENDING_PHASE_19.
