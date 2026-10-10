# ADR-0054: Map assignment body public methods

- Status: Accepted
- Date: 2026-10-09

## Context

The pinned RocketMQ 5.5.1 remoting DTOs `QueryAssignmentRequestBody`, `QueryAssignmentResponseBody`, and `SetMessageRequestModeRequestBody` expose JavaBean getters and setters. Existing direct Fastjson input cases verified decoding and defaults, but did not invoke those public methods on newly constructed objects.

## Decision

Expose Rust snake_case getters and setters for every public field on the three DTOs. Preserve the observed constructor defaults: query request fields are null; query response assignments are null; request mode defaults to `PULL`, and its primitive queue count defaults to zero.

Java's response DTO returns the same mutable `Set<MessageQueueAssignment>` passed to its setter. The outer Set alias is now represented by `SharedValue<HashSet<...>>`; getter handles retain pointer identity and both sides can mutate the shared Set. [ADR-0065](ADR-0065-shared-query-assignment-set-alias.md) records the correction and its direct Java evidence. Identity of the mutable assignment objects stored inside that Set remains a separate nested-reference surface.

## Consequences

Direct pinned Java invocation and Rust regressions cover each DTO's defaults, getter/setter values, and nullable setter behavior. The response Set oracle also mutates through the setter argument and getter; Rust now verifies `ptr_eq` plus both mutation directions through cloned handles. The body inherits `Object.equals` and `Object.hashCode`, so the Rust DTO no longer derives structural equality. Nested assignment-element aliases and the wider Phase 6 API reconciliation remain open; the phase remains PARTIAL and P6-V04 remains `PENDING_PHASE_19`.

## Evidence

- Direct Java calls: `queryAssignmentRequestApiCase`, `queryAssignmentResponseApiCase`, and `setRequestModeRequestApiCase` in `fixtures/runtime/lookup.json`.
- Original API oracle/implementation commits: `7c7d8b5b` / `fc5f2336`; mutable Set oracle/implementation commits: `1bfe20bf` / `b280111c`; shared Set alias: this correction, `lookup.json` oracle `fb436699a2c7d128fd96c73e3f106add77904444fa581485a5ddc1d929bc251e`.
- Current aggregate `fixtures/runtime/lookup.json` SHA-256: `fb436699a2c7d128fd96c73e3f106add77904444fa581485a5ddc1d929bc251e`.
- Validation: `cargo test --workspace --all-targets --locked`; strict all-target/all-feature Clippy; pinned offline Maven verify (22 scenarios, fixture drift, Java cross-decodes); `cargo fmt --all --check`; `git diff --check`.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate Java source checkout was unavailable.
