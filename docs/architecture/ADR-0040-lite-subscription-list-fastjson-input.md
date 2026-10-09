# ADR-0040: Match Java Lite subscription collection inputs

Status: Accepted

## Context

Pinned Java 5.5.1 declarations show `LiteSubscriptionCtlRequestBody.subscriptionSet` is a `Set<LiteSubscriptionDTO>`, and both `LiteSubscriptionDTO.liteTopicSet` and `LiteSubscription.liteTopicSet` are `Set<String>`. The DTO has no `equals`/`hashCode` override, so the outer Java set retains separate DTO objects with equal field values but deduplicates repeated null members. Hash-set iteration order is unspecified.

Fastjson accepts an object for either set field as a singleton set. A string becomes a singleton for `liteTopicSet`; the empty string becomes an empty set. Array members use Java String coercion, including null preservation, and `Set<String>` removes duplicate coerced values. Numeric and Boolean scalars are rejected as whole `liteTopicSet` values. `LiteSubscriptionDTO.version` and `OffsetOption.value` are primitive longs: missing/null values become zero; numeric strings, fractions and Booleans follow Java long conversion; object and array values reject.

## Decision

Represent `liteTopicSet` as `Option<BTreeSet<Option<String>>>` so Rust callers see the set behavior, deduplication and nullable members directly. Decode string and object values using the shared Fastjson String coercion, arrays member by member, an empty string as an empty set, and null as no set.

Keep `subscription_set` as `Option<Vec<Option<LiteSubscriptionDto>>>` as the Rust wire representation. Fastjson's object-to-singleton conversion is retained, duplicate null rows collapse to one, and separately decoded DTO objects remain separate even when their field values match. The vector preserves a deterministic input order; the Java HashSet's iteration order has no stable cross-process promise.

Decode primitive long fields through the Java-compatible integer adapters. Null values default to zero; `OffsetOption.value` and `LiteSubscriptionDTO.version` reject object and array input.

## Evidence

The direct pinned Java corpus combines 114 cases: the original 33 enum/default/duplicate/collection inputs, 49 per-field token shapes, seven DTO wrapper roots, seven `subscriptionSet` field shapes, seven `liteTopicSet` member shapes, seven `OffsetOption.value` shapes, and four set/empty-string cases. Native differentials match acceptance and encoded values. Test/fix commits: `bdbe660`/`8f1ddd0` and `d3a298e`/`ab3d0e2`; empty-string oracle: `df69ec0`. Current `lookup.json` SHA-256: `ecb83ddd48d7d19fd3e8e015bc6d281b67f96d1c99e59091ace43d4d86c41756`.

This records these Lite DTO input boundaries only. Broader Phase 6 source/API/input reconciliation remains open, and P6-V04 stays `PENDING_PHASE_19`.
