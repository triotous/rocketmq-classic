# ADR-0046: Match Java lock batch Set inputs

Status: Accepted

## Context

Pinned Java 5.5.1 declares `LockBatchRequestBody.mqSet`, `UnlockBatchRequestBody.mqSet`, and `LockBatchResponseBody.lockOKMQSet` as `Set<MessageQueue>`. `LockBatchRequestBody` and `UnlockBatchRequestBody` initialize `mqSet` to an empty set; `LockBatchResponseBody` initializes `lockOKMQSet` likewise. Explicit null Set values encode as omitted fields. `onlyThisBroker` is a primitive boolean, and request group/client identifiers are strings.

Fastjson accepts an object as a singleton Set, retains one null member, and collapses duplicate equal queues and repeated nulls. Empty-string Set input converts to null; ordinary nonempty string, number, and Boolean Set values reject. Non-object, non-null queue members reject. Request string fields use Fastjson String coercion, primitive boolean null/numeric/fractional inputs follow Java conversion, and duplicate properties keep the last value.

## Decision

Represent the three Java Set fields as `Option<HashSet<Option<MessageQueue>>>`. This preserves a null Set and nullable member while enforcing Java queue equality and duplicate collapse. Request DTO defaults contain an empty HashSet; response DTO defaults do too. Keep serde's HashSet array encoding as the wire representation; Java HashSet iteration order is unspecified.

Decode request string and boolean fields with the shared Fastjson adapters and process objects with last-wins decoding. `MQClientApi::lock_batch_mq` returns `Option<HashSet<Option<MessageQueue>>>` to preserve Java's Set, nullable field, and nullable member outcomes.

The pinned 5.5.1 client/common/remoting artifacts contain `LockBatchResponseBody` but no `UnlockBatchResponseBody`. At this ADR checkpoint the unused Rust-only `UnlockBatchResponseBody` was still exported; it was later removed after confirming both response paths use the shared Java DTO. See [ADR-0056](ADR-0056-remove-non-java-unlock-response-body.md).

## Evidence

Direct pinned Java input arrays contain 46 `LockBatchRequestBody`, 46 `UnlockBatchRequestBody`, and 25 `LockBatchResponseBody` cases. They cover all seven JSON token kinds for every field, all seven Set member kinds, null/empty/object Set forms, duplicate queues/nulls, duplicate properties, and root shapes. The red differential exposed String coercion, boolean primitive conversion, singleton Set conversion, nullable members, duplicate collapse, default Set values, and root array handling. Oracle/test commit `bf4a420`; implementation commit `a9c9627`. Current `lookup.json` SHA-256: `29211416d6c3f8c9af305fbe18e66b8ad1ada7c251333f69b8cd2f7970938d49`.

Full Phase 6 source/API/settings/input/branch/reverse-export reconciliation remains open, and P6-V04 stays `PENDING_PHASE_19`.
