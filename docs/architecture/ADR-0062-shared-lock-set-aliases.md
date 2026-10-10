# ADR-0062: Preserve Java lock-batch Set aliases

- Status: Accepted
- Date: 2026-10-10

## Context

Pinned RocketMQ 5.5.1 `LockBatchRequestBody`, `UnlockBatchRequestBody`, and `LockBatchResponseBody` retain the exact `Set<MessageQueue>` passed to their setters and return that same Set instance from their getters. Mutations through the caller and getter references are mutually visible. These DTO classes inherit identity-based `Object.equals` and `Object.hashCode`.

## Decision

Represent each nullable Set field as `Option<SharedValue<HashSet<Option<MessageQueue>>>>`. Cloning the shared handle retains the same outer Set identity; `with` and `with_mut` provide synchronized access. JSON serialization remains transparent to the wrapped HashSet, preserving array form, null members, empty defaults, and nullable Set behavior.

Remove derived structural equality and hashing from the three DTO body types. Keep `MQClientApi::lock_batch_mq`'s existing owned `Option<HashSet<Option<MessageQueue>>>` result by snapshotting the response field at that API boundary.

The mutable `MessageQueue` instances inside a Java Set are separate object aliases. Rust still stores those set members as values; this ADR establishes the outer Set-container alias and does not claim parity for callers that retain and mutate individual queue objects.

## Consequences

Protocol DTO callers retain or receive a cloned `SharedValue` handle for the same Set object. They can mutate from either the original handle or the getter and observe the same members. Runtime API callers continue to receive an owned snapshot.

## Evidence

- Direct pinned Java 5.5.1 API fixtures assert `getterReturnsAssignedSet`, caller/getter mutation visibility, and inherited `Object` declaring classes for equality/hash on all three DTOs.
- Native differential: `remaining_runtime_body_public_methods_match_invoked_java`.
- Implementation/oracle commit: `045b46e7`.
- Fixture SHA-256: `07fb5f45492598ce7ca844903bd0440bbbaabd7baddb9bdfbbb5c96a5b39a7ad`.
- Validation: protocol tests/doc tests, full workspace/all-target tests, strict all-feature Clippy, pinned offline Maven verify, comparator self-test, 22-scenario manifest validation, format, and diff checks.
