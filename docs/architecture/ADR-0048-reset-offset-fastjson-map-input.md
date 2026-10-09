# ADR-0048: Match Java ResetOffsetBody Fastjson Map decoding

Status: Accepted

## Context

Pinned RocketMQ Java 5.5.1 declares `ResetOffsetBody.offsetTable` as `Map<MessageQueue, Long>`. The constructor initializes it to an empty map, so an omitted field decodes as empty and encodes as `{"offsetTable":{}}`; explicit null survives decode and is omitted by Fastjson encoding (`{}`). Fastjson writes `MessageQueue` instances directly as object map keys, producing the non-standard form `{"offsetTable":{{"brokerName":"b","queueId":1,"topic":"t"}:9}}`. Decode accepts this form, rejects a JSON string containing serialized queue JSON as the key, and applies last-value wins to duplicate queue keys and duplicate body fields. A null `Long` map value is accepted and retained after decode, then omitted during encoding.

## Decision

Represent the nullable table as `Option<Vec<(MessageQueue, Option<i64>)>>`. `Default` uses `Some(empty)` to match Java constructor state; the outer `Option` preserves explicit null, and the inner `Option` preserves nullable Java `Long` values. Add Java-shaped table accessors, `decode_java_json` for Fastjson object-valued keys and raw last-wins fields, and `encode_java_json` that omits null table entries. Queue rows are normalized through a `BTreeMap`, so Rust ordering is deterministic and duplicate queue keys collapse last-wins; Java `HashMap` iteration order is unspecified.

Standard Serde serialization remains the JSON representation of Rust-owned fields. `encode_java_json` is the Java body-wire encoder.

## Evidence

Nineteen direct pinned Java fixture cases invoke `ResetOffsetBody.decode` and `encode`, covering every JSON token type for `offsetTable`, non-object roots, omitted/null/empty tables, Fastjson and quoted queue keys, duplicate keys and fields, unknown fields, invalid table shapes and keys, and null offsets. Native differential checks acceptance, normalized rows, and exact encoded body strings. Fixture `fixtures/runtime/lookup.json` SHA-256: `4ac75c6dc196cbc010ebf80bb53c9074cbd844ce057bc80c298d03f31e7326f8`. Test commits: `3d517eb`, `d8a027b`; implementation: `3b8ebe5`.

Phase 6's broader source/API/settings/input reconciliation remains open; the matrix remains PARTIAL and P6-V04 stays `PENDING_PHASE_19`.
