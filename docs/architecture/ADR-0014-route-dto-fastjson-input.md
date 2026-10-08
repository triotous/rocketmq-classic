# ADR-0014: Match pinned Fastjson scalar coercions in route DTO inputs

Status: Accepted

## Context

The Java 5.5.1 client decodes route responses into `QueueData`, `BrokerData`, `TopicRouteData`, `TopicQueueMappingInfo`, and `MessageQueue` with Fastjson 2.0.63. The Rust DTOs previously used direct Serde field decoding. That rejected several inputs Java accepts and decoded Java's primitive null defaults differently.

## Decision

Add narrow JSON deserializers in `rocketmq-classic-common::serde_fastjson` and attach them to the affected DTO fields. The tested behavior includes scalar-to-string conversion, Java primitive defaults for null, integer strings and fractions, boolean inputs, normalized numeric map keys, nullable Broker addresses, and scalar members of route filter lists. Invalid integer ranges, invalid boolean strings, and null static-mapping values remain rejected.

The input helpers keep Rust DTO field types and ordinary Serde encoding unchanged. Java exception classes are not reproduced; Rust exposes Serde errors at the decode boundary. HashMap/ConcurrentHashMap iteration order and untested Fastjson grammar are not asserted equivalent.

For inherited `RemotingSerializable` methods, `TopicRouteData` and `TopicQueueMappingInfo` expose Java-compatible `java_to_json`, `java_encode`, `java_decode`, `java_decode_list`, and `java_from_json` adapters, plus nullable decode/list/fromJson forms. These adapters sort object properties, reproduce Fastjson2's unquoted numeric keys for `brokerAddrs` and `currIdMap`, and match the tested pretty-print form. The route API uses `TopicRouteData::java_decode_nullable` for response bodies. Numeric object keys are normalized before Serde parsing; other unquoted keys remain invalid. Native map output is deterministic, while Java HashMap/ConcurrentHashMap iteration order is unspecified.

## Evidence

`RuntimeRouteFixtures.routeDtoInputCases` invokes 39 actual inputs for `QueueData`, `MessageQueue`, `BrokerData`, `TopicQueueMappingInfo`, and nested `TopicRouteData`. The five `MessageQueue.queueId` cases cover explicit null to zero, signed integer boundaries, fractional truncation, and overflow rejection. Twelve added cases establish that route DTO `JSON.parseObject` rejects JSON5-style single quotes, comments, unquoted keys, trailing commas, numeric suffixes, plus malformed collection shapes; native route DTO parsing rejects the same inputs. Two further cases confirm Java rejects fullwidth and Arabic-Indic decimal strings in integer fields, which the native decoder also rejects. This route parser boundary is separate from the permissive SOCKS constructor parser.

`route_dto_inputs_match_invoked_java_fastjson` compares accepted fields and rejected outcomes against the same Java inputs.

Route fixture SHA-256 at the 37-case input update: `ce2afec05859a761ce2fd82a9034ef147b333f077a999b03d17209a305254fb0`.

The Unicode numeric-string follow-up adds direct Java `QueueData` and `TopicQueueMappingInfo` cases (`b7ea5a8`). Both reject those strings with `JSONException`; the native differential rejects them too, and the test requires both fixture IDs (`b1f36d5`). Current route fixture SHA-256: `0f4dbd7dece83869b6c93ce47dae26e297db93f421d122468c721c1365aef59c`.

The inherited serialization oracle adds six direct cases: default, explicit-null, and populated `TopicRouteData` and `TopicQueueMappingInfo` values. Each invokes compact/pretty instance and static JSON, instance/static encoding including the empty `JSONWriter.Feature[]` overload, decoding, list decoding, and fromJson; compact output bytes match the pinned Java fixture. The initial serialization update had fixture hash `935426217262daeb92860ef9b90cc61b5cde8aa71d3042ca6f4713582568e2b3`; null utility cases were then invoked on both concrete DTO classes, extending the fixture to current SHA-256 `0f4dbd7dece83869b6c93ce47dae26e297db93f421d122468c721c1365aef59c`.

The added nullable utility cases directly invoke Java `decode(null, ...)`, JSON `null`, `decodeList(null, ...)`, and `fromJson(null, ...)` on both `TopicRouteData` and `TopicQueueMappingInfo`; each returns null, while `decodeList("[null]", ...)` returns a one-element list containing null. The route API returns `Ok(None)` for a successful JSON-null body, while a missing body remains a distinct malformed-response error. `ClientRuntime` treats a null route refresh as a no-op and preserves the existing cached snapshot. The follow-up oracle is `c00e1a8`, with adapter coverage in `8420373`. See [ADR-0039](ADR-0039-nullable-route-decode.md).

Focused differential, full workspace tests, strict all-feature Clippy, pinned offline Maven verification, comparator/manifest, formatting, and diff checks pass. These records close only the invoked input and inherited-serialization cases; broader DTO input, Fastjson grammar, and whole-Phase 6 reconciliation remain open.
