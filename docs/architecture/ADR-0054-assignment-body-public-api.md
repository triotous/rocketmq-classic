# ADR-0054: Map assignment body public methods

- Status: Accepted
- Date: 2026-10-09

## Context

The pinned RocketMQ 5.5.1 remoting DTOs `QueryAssignmentRequestBody`, `QueryAssignmentResponseBody`, and `SetMessageRequestModeRequestBody` expose JavaBean getters and setters. Existing direct Fastjson input cases verified decoding and defaults, but did not invoke those public methods on newly constructed objects.

## Decision

Expose Rust snake_case getters and setters for every public field on the three DTOs. Preserve the observed constructor defaults: query request fields are null; query response assignments are null; request mode defaults to `PULL`, and its primitive queue count defaults to zero.

Java's response DTO returns the same mutable `Set<MessageQueueAssignment>` passed to its setter. Rust consumes an owned `HashSet` and lends references to it; `get_message_queue_assignments_mut` now provides mutable access so mutation through the DTO getter is supported. This maps getter mutation behavior, while identity with the caller-owned Set before the Rust move remains different.

## Consequences

Direct pinned Java invocation and Rust regressions cover each DTO's defaults, getter/setter values, and nullable setter behavior. The response Set oracle also mutates through the setter argument and getter, and the Rust differential matches the final Set contents through its mutable borrow. External Set identity remains part of the open Phase 6 API reconciliation; the phase remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct Java calls: `queryAssignmentRequestApiCase`, `queryAssignmentResponseApiCase`, and `setRequestModeRequestApiCase` in `fixtures/runtime/lookup.json`.
- Original API oracle/implementation commits: `7c7d8b5b` / `fc5f2336`; mutable Set oracle/implementation commits: `1bfe20bf` / `b280111c`.
- Current aggregate `fixtures/runtime/lookup.json` SHA-256: `ce344c0aa35c634a730f97cf706fba3b41313fd12a2fac294d3e01250f7e9b2c`.
- Validation: `cargo test --workspace --all-targets --locked`; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios, fixture drift, Java cross-decodes); `cargo fmt --all --check`; `git diff --check`.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate Java source checkout was unavailable.
