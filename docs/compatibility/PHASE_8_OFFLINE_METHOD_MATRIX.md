# Phase 8 offline method matrix

Java source baseline: Apache RocketMQ client 5.5.1, commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. Fixture generation and unit checks do not contact a Broker or NameServer.

## Source-to-Rust mapping

| Java source method or behavior | Rust owner | Evidence |
| --- | --- | --- |
| `common/.../message/MessageDecoder.decode(ByteBuffer, readBody, decompressBody, isClient, isSetPropertiesString, checkCRC)` | `rocketmq_classic_common::message::decode_one` plus consumer client-ID enrichment | P8-V01 compares ten pinned records with CRC enabled and checks metadata, body, properties and generic IDs. Pull processing adds `MessageClientExt` ID semantics before filtering/hooks; `isSetPropertiesString` remains false there, as in `PullAPIWrapper`. Rust returns typed errors for malformed records instead of Java's `null` plus cursor-to-limit behavior, as required by the phase contract. |
| `MessageDecoder.decodesBatch` | `decode_many` | P8-V01/P8-V02 cover concatenated golden records, checked record progress, aggregate limits and every truncated prefix. |
| `MessageVersion.valueOfMagicCode/getTopicLength` | `decode_one` | V1 magic `-626843481` uses signed 8-bit topic length; V2 `-626843477` uses signed 16-bit length. |
| `MessageDecoder.createMessageId` | `decode_one` | Store-host IP bytes + big-endian port + commit-log offset, upper-case hex; producer `UNIQ_KEY` is kept separate. IPv4 and IPv6 lengths are checked against Java. |
| `MessageClientExt.getMsgId` / `getOffsetMsgId` | consumer client-ID enrichment / `MessageExt::{message_id, offset_message_id}` | Java `getMsgId` returns `UNIQ_KEY` when present, otherwise `offsetMsgId`; `getOffsetMsgId` retains the store-host/offset ID. Fixtures carry both generic and client-decoded expectations. |
| `MessageSysFlag` host/compression bits and `CompressorFactory` | `sysflag::message` and `compression::decompress_limited` | P8-V01 covers independent born/store IPv6 flags and ZLIB, LZ4 and ZSTD; P8-V02 covers output caps and unsupported compressor values. |
| `PullAPIWrapper.processPullResult` | `rocketmq_classic_consumer::pull::PullAPIWrapper::process_pull_result` | P8-V03 covers status/suggested broker handling, FOUND decode, optional inner-batch unwrap, tag filtering, hooks, transaction/min/max/broker/queue enrichment, offset delta and raw-buffer release. |
| `DefaultMQPushConsumerImpl.subscribe(topic, fullClassName, filterClassSource)` | `build_class_filter_subscription_data` | Builds the same local `SubscriptionData` fields: class name, class-filter mode, `*` base subscription, and source. Consumer lifecycle publishes subscription heartbeats, which is outside Phase 8. `filterClassSource` is excluded from JSON in Java (`@JSONField(serialize=false)`) and Rust. The pinned 5.5.1 client source has no class-filter upload API or call; Filter Server routing is covered below. |
| `PullAPIWrapper.computePullFromWhichFilterServer` and `PullSysFlag` | crate-internal `resolve_pull_broker_address` / `select_filter_server`, existing `sysflag::pull` | P8-V03 covers normal pulls keeping the Broker address, class-filter pulls choosing from the route's broker-address entry, and missing/empty filter-server lists. These are internal helpers, not additional Rust public API. |

## Rust-to-Java public API/configuration map

| Rust public surface | Java source/API or required phase behavior | Unit evidence |
| --- | --- | --- |
| `DecodeOptions::{read_body, decompress_body, set_properties_string}` | `ClientConfig.decodeReadBody/decodeDecompressBody`; `MessageDecoder.decode` parameters | P8-V01 `body_decode_options_match_java_and_limits_reject_oversized_fields` |
| `DecodeOptions::verify_crc` | `MessageDecoder.decode(..., checkCRC)`; pull wrapper uses Java's `false` default | P8-V01 fixture decoding; P8-V02 opt-in mismatch behavior |
| `DecodeLimits::{max_record_bytes, max_decompressed_body_bytes, max_total_decoded_bytes}` | Bounded allocation required by Phase 8; no fields are added to `ClientConfig` | P8-V02 record, decompressed-output and aggregate caps |
| `decode_one` / `decode_many` | `MessageDecoder.decode` / `MessageDecoder.decodesBatch` | P8-V01/P8-V02 |
| `MessageExt::{message_id, offset_message_id}` | Generic `MessageExt.getMsgId`; client `MessageClientExt.getMsgId/getOffsetMsgId` with Java's `UNIQ_KEY` fallback | P8-V01 generic ID fixture; P8-V03 client and inner-batch ID assertions |
| `PullAPIWrapper::{process_pull_result, register_filter_message_hook}` and `FilterMessageHook` | `PullAPIWrapper.processPullResult`, `registerFilterMessageHook`, and `FilterMessageHook` | P8-V03 pull-result and hook checks |
| `ProcessedPullResult` | Rust wrapper for Java `PullResultExt` plus `msgFoundList` | P8-V03 status and message-list checks |
| `build_class_filter_subscription_data` | Public class-filter overload's local `SubscriptionData` construction | P8-V03 class-filter setup and tag-filter suppression |

The three `DecodeLimits` parameters are per-decode safety bounds required by this phase; they are not client-wide settings. Record subfields are bounded by the declared record size, with no independent public body/topic/property limits.

## Record layout captured by the fixture generator

All integers use Java `ByteBuffer` big-endian order. After the 4-byte total-size and 4-byte magic fields, the record fields are body CRC (i32), queue ID (i32), flag (i32), queue offset (i64), physical offset (i64), system flag (i32), born timestamp (i64), born host (4/16 IP bytes + i32 port), store timestamp (i64), store host (4/16 IP bytes + i32 port), reconsume count (i32), prepared transaction offset (i64), body length/body, version-specific topic length/topic, and properties length/properties. Store size bounds every field read.

## Local verification status

| ID | Status | Coverage |
| --- | --- | --- |
| P8-V01 | `UNIT_VERIFIED` | Ten Java records: both magic versions; all four born/store IPv4/IPv6 combinations; all three compressors; valid CRC; all body-read/decompression combinations; `propertiesString`; generic/client IDs; concatenated records. |
| P8-V02 | `UNIT_VERIFIED` | Every truncated prefix; invalid magic/store/body/topic/property lengths; unknown compressor; record/decompressed/aggregate caps; empty compressed body; no panic or out-of-bounds read. |
| P8-V03 | `UNIT_VERIFIED` | FOUND and all three non-FOUND statuses; tag match/miss/missing tag/empty filter; suggested broker and offsets; inner batch success/fallback; hooks; enrichment and raw-byte release; class-filter setup/routing. |

The class-filter source is not executed in Rust. Pinned Java 5.5.1 retains its source string only on the local `SubscriptionData`; the Filter Server performs server-side class filtering. Live Broker, Filter Server and cross-language validation remain Phase 19 work.
