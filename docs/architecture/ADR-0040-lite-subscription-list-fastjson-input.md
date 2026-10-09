# ADR-0040: Match Java Lite subscription list inputs

Status: Accepted

## Context

`LiteSubscriptionCtlRequestBody.subscriptionSet` is a Java `List<LiteSubscriptionDto>` decoded by Fastjson 2.0.63. Java accepts a single object for this list field by wrapping it in a singleton list, and it retains null elements in arrays. Missing and explicit-null fields decode as null and are omitted by the DTO's default JSON encoding. Rust originally modeled a non-nullable element vector and accepted only array input.

## Decision

Represent the public Rust field as `Option<Vec<Option<LiteSubscriptionDto>>>`. Decode a JSON array as a list while preserving null elements, a JSON object as a one-element list, and null as no list. Reject scalar list values and non-object, non-null array elements. Keep `skip_serializing_if = "Option::is_none"` for the root field so missing and explicit-null Java values re-encode without `subscriptionSet`.

## Evidence

Ten new direct Java 5.5.1 inputs cover null/empty list values, singleton object conversion, null and invalid array elements, and rejected scalar field values. The combined fixture has 33 Lite subscription inputs, including enum/default conversions and duplicate-last fields, at SHA-256 `1074c30d2d829dc4dd46b68f1fbdd1f66f7b4a87a65c40f9174aff806658f619`. Oracle/test commit `27a55e3`; implementation commit `5462e2f`. The focused native differential failed on singleton-object input before the change, then matched acceptance and encoded DTO values for all cases.

Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify with deterministic fixture checks (22 scenarios), differential comparator self-test/manifest validation (22 scenarios), formatting and diff checks pass. This records input and DTO behavior only; whole Phase 6 source/API/input reconciliation remains open, and P6-V04 stays `PENDING_PHASE_19`.
