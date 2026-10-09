# ADR-0054: Map assignment body public methods

- Status: Accepted
- Date: 2026-10-09

## Context

The pinned RocketMQ 5.5.1 remoting DTOs `QueryAssignmentRequestBody`, `QueryAssignmentResponseBody`, and `SetMessageRequestModeRequestBody` expose JavaBean getters and setters. Existing direct Fastjson input cases verified decoding and defaults, but did not invoke those public methods on newly constructed objects.

## Decision

Expose Rust snake_case getters and setters for every public field on the three DTOs. Preserve the observed constructor defaults: query request fields are null; query response assignments are null; request mode defaults to `PULL`, and its primitive queue count defaults to zero.

Java's response DTO returns the same mutable `Set<MessageQueueAssignment>` passed to its setter. Rust consumes an owned `HashSet` and lends a reference to it. The methods preserve values and null state, but do not preserve mutable Set alias identity.

## Consequences

Direct pinned Java invocation and Rust regressions now cover each DTO's defaults, getter/setter values, and nullable setter behavior. The collection alias difference remains part of the open Phase 6 API reconciliation; the phase remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct Java calls: `queryAssignmentRequestApiCase`, `queryAssignmentResponseApiCase`, and `setRequestModeRequestApiCase` in `fixtures/runtime/lookup.json`.
- Oracle commit: `7c7d8b5b`; Rust implementation: `fc5f2336`.
- Aggregate fixture SHA-256 at this assignment API checkpoint: `e716392045bd2dc0eae95f7815c6cdc5a9485fed707515ea852ada8c56be8b59`. Later runtime DTO method oracles update the digest; see [ADR-0055](ADR-0055-runtime-body-public-api.md) for the current value.
- Validation: `cargo test --workspace --all-targets --locked`; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios, fixture drift, Java cross-decodes); `cargo fmt --all --check`; `git diff --check`.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate Java source checkout was unavailable.
