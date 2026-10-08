# ADR-0017: Match Java assignment attachment map inputs

Status: Accepted

## Context

Phase 6 `queryAssignment` returns `MessageQueueAssignment.attachments`, a Java `Map<String, String>`. Native decoding previously required every value to be a JSON string and could not retain a null map value.

## Decision

Represent attachments as `Option<BTreeMap<String, Option<String>>>` and decode non-null values with the shared Fastjson scalar-to-string conversion. Missing and explicit-null maps remain `None`. Null entries remain present as `None` after decode; the assignment JSON serializer omits null-valued map entries, matching the Java default encoder. Non-object top-level values remain rejected.

## Evidence

`RuntimeLookupFixtures.assignmentAttachmentCases` invokes six actual Java 5.5.1 `QueryAssignmentResponseBody.decode` inputs. It covers missing/null maps, numeric/Boolean/null values, nested arrays/objects, and invalid top-level shapes. `assignment_attachment_inputs_match_invoked_java_fastjson` compares decoded values and response re-encoding.

Current combined lookup fixture SHA-256: `878cb07da0256c4aa54ae17a0c2d142f3072d4812d589b6a9fb14bf65d3900e3`.

Full workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verification, comparator/manifest, formatting, and diff checks pass. Rust parse errors do not reproduce Fastjson exception classes, and map iteration order is not asserted equivalent. This closes the recorded attachment inputs only; other assignment DTO combinations and whole-Phase 6 reconciliation remain open.
