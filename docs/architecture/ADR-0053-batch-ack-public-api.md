# ADR-0053: Map BatchAck public methods

- Status: Accepted
- Date: 2026-10-09

## Context

The pinned RocketMQ 5.5.1 `BatchAck` API exposes a no-argument constructor, getters and setters for nine properties, and `toString()`. `BatchAckMessageRequestBody` exposes getters and setters for `brokerName` and `acks`. The Rust DTOs previously matched wire input but did not expose the corresponding method surface or Java diagnostic text.

## Decision

Expose Rust snake_case getters and setters for all nine `BatchAck` properties and both body properties. Use `Default` for Java's no-argument constructor values, and implement `Display` so `BatchAck::to_string()` matches the invoked Java output, including BitSet index formatting.

Rust represents the Java `BitSet` as optional bytes and the Java `List<BatchAck>` as `Option<Vec<Option<BatchAck>>>`. `get_acks_mut` now lends the owned Vec mutably, matching mutations performed through Java's `getAcks()` result. The Rust body setter still takes ownership of its vector, so identity with the caller-owned List passed to `setAcks` remains different.

## Consequences

The invoked constructor, getter, setter, and `toString()` values now match the pinned Java oracle. Direct Java cases and the Rust differential also cover mutable List contents through getter access. Cross-owner List identity remains a documented limitation in the wider Phase 6 API reconciliation, so the overall phase remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct pinned Java invocation: `batchAckApiCase` in `fixtures/runtime/lookup.json`, covering defaults, all property readbacks, `toString()`, body defaults and values, assigned-list identity/mutations, and null setter behavior.
- Original API oracle/implementation: `7f3a4f0e` / `c709e743`; mutable-list oracle/implementation: `51b92e77` / `f4d85af0`.
- Current aggregate `fixtures/runtime/lookup.json` SHA-256: `9a9c052d3d57d0c4e54e6f8adc1147c71bbfa1fbc07745d63e0920ab670f4442`.

- Aggregate fixture SHA-256 at this ADR's BatchAck API checkpoint: `d16675236d7b63ddc1ffd69c80df896370479f952f0af9f961214ed7d0237632`. Later DTO method oracles update the aggregate; see [ADR-0055](ADR-0055-runtime-body-public-api.md) for the current digest.
- Validation: `cargo test --workspace --all-targets --locked`; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios, fixture drift, Java cross-decodes); `cargo fmt --all --check`; `git diff --check`.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate Java source checkout was unavailable.

## Mutable BitSet getter continuation

The pinned Java `BatchAck.getBitSet()` returns the same mutable `BitSet` passed to `setBitSet`. Mutating bit 5 through the getter is visible through both the getter and setter-held Java object. Rust adds `get_bit_set_mut()` for its owned `BitSet.toByteArray()` byte representation, matching the updated serialized bytes without introducing a Java-style mutable `BitSet` object.

- Direct Java evidence: `batchAckApiCase` in `fixtures/runtime/lookup.json`; Rust differential: `batch_ack_public_methods_match_invoked_java`.
- Oracle/test commit: `bd3d2e71`; implementation commit: `d1dc7dfd`.
- Current aggregate `fixtures/runtime/lookup.json` SHA-256: `abfe31546b68799aced43ff4f631f5dea37a4c14efac1dfc60a1ed637a22b970`.
- Full workspace/all-target tests, strict Clippy, pinned offline Maven verify, comparator/manifest22, formatting, and diff checks pass. Rust does not preserve identity with the caller-owned Java `BitSet`; Phase 6 remains PARTIAL and P6-V04 stays PENDING_PHASE_19.
