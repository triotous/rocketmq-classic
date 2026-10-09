# ADR-0053: Map BatchAck public methods

- Status: Accepted
- Date: 2026-10-09

## Context

The pinned RocketMQ 5.5.1 `BatchAck` API exposes a no-argument constructor, getters and setters for nine properties, and `toString()`. `BatchAckMessageRequestBody` exposes getters and setters for `brokerName` and `acks`. The Rust DTOs previously matched wire input but did not expose the corresponding method surface or Java diagnostic text.

## Decision

Expose Rust snake_case getters and setters for all nine `BatchAck` properties and both body properties. Use `Default` for Java's no-argument constructor values, and implement `Display` so `BatchAck::to_string()` matches the invoked Java output, including BitSet index formatting.

Rust represents the Java `BitSet` as optional bytes and the Java `List<BatchAck>` as `Option<Vec<Option<BatchAck>>>`. The Rust body setter takes ownership of its vector and the getter lends an immutable slice. This preserves values and null states, but Java's `setAcks(list)` / `getAcks()` retain and return the same mutable List object. Rust does not preserve that mutable alias identity; this is an explicit API semantic difference.

## Consequences

The invoked constructor, getter, setter, and `toString()` values now match the pinned Java oracle. The collection alias behavior remains a documented limitation in the wider Phase 6 API reconciliation, so the overall phase remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct pinned Java invocation: `batchAckApiCase` in `fixtures/runtime/lookup.json`, covering defaults, all property readbacks, `toString()`, body defaults and values, assigned-list identity, and null setter behavior.
- Oracle commit: `7f3a4f0e`; Rust implementation: `c709e743`.
- Current aggregate `fixtures/runtime/lookup.json` SHA-256: `d16675236d7b63ddc1ffd69c80df896370479f952f0af9f961214ed7d0237632`.
- Validation: `cargo test --workspace --all-targets --locked`; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios, fixture drift, Java cross-decodes); `cargo fmt --all --check`; `git diff --check`.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate Java source checkout was unavailable.
