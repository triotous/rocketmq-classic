# Phase 8 offline method matrix

Java source baseline: Apache RocketMQ client 5.5.1, commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. Fixture generation and unit checks do not contact a Broker or NameServer.

## Source-to-Rust mapping

| Java source method or behavior | Rust owner | Evidence |
| --- | --- | --- |
| `common/.../message/MessageDecoder.decode(ByteBuffer, readBody, decompressBody, isClient, isSetPropertiesString, checkCRC)` | `rocketmq_classic_common::message::decode_one` | `message_decode` decodes ten pinned records with CRC enabled and checks metadata, body, properties and IDs. Rust returns typed errors for malformed records instead of Java's `null` plus cursor-to-limit behavior, as required by the phase contract. |
| `MessageDecoder.decodesBatch` | `decode_many` | Concatenated golden records, checked record progress, aggregate limits and every truncated prefix. |
| `MessageVersion.valueOfMagicCode/getTopicLength` | `decode_one` | V1 magic `-626843481` uses signed 8-bit topic length; V2 `-626843477` uses signed 16-bit length. |
| `MessageDecoder.createMessageId` | `decode_one` | Store-host IP bytes + big-endian port + commit-log offset, upper-case hex; producer `UNIQ_KEY` is kept separate. IPv4 and IPv6 lengths are checked against Java. |
| `MessageClientExt.getOffsetMsgId` | `MessageExt::message_id` | Pinned client decoding sets `offsetMsgId` through `setMsgId`, so both Java accessors expose the same store-host/offset ID. |
| `MessageSysFlag` host/compression bits and `CompressorFactory` | `sysflag::message` and `compression::decompress_limited` | Independent born/store IPv6 flags; ZLIB, LZ4 and ZSTD fixture records; decompressed-byte caps. |
| `PullAPIWrapper.processPullResult` | `rocketmq_classic_consumer::pull::PullAPIWrapper::process_pull_result` | Status/suggested broker handling, FOUND decode, optional inner-batch unwrap, tag filtering, hooks, transaction/min/max/broker/queue enrichment, offset delta and raw-buffer release. |
| `DefaultMQPushConsumerImpl.subscribe(topic, fullClassName, filterClassSource)` | `build_class_filter_subscription_data` | Keeps class name and source locally, sets class-filter mode and `*` base subscription, then sends the subscription heartbeat. `filterClassSource` is excluded from JSON in Java (`@JSONField(serialize=false)`) and Rust. The pinned 5.5.1 client source has no class-filter upload API or call; Filter Server routing is covered below. |
| `PullAPIWrapper.computePullFromWhichFilterServer` and `PullSysFlag` | `resolve_pull_broker_address` / `select_filter_server`, existing `sysflag::pull` | Normal pulls keep the Broker address; class-filter pulls choose from the route's broker-address entry and report missing/empty filter-server lists. |

## Record layout captured by the fixture generator

All integers use Java `ByteBuffer` big-endian order. After the 4-byte total-size and 4-byte magic fields, the record fields are body CRC (i32), queue ID (i32), flag (i32), queue offset (i64), physical offset (i64), system flag (i32), born timestamp (i64), born host (4/16 IP bytes + i32 port), store timestamp (i64), store host (4/16 IP bytes + i32 port), reconsume count (i32), prepared transaction offset (i64), body length/body, version-specific topic length/topic, and properties length/properties. Store size bounds every field read.

## Local verification status

| ID | Status | Coverage |
| --- | --- | --- |
| P8-V01 | `UNIT_VERIFIED` | Ten Java records: both magic versions; all four born/store IPv4/IPv6 combinations; all three compressors; valid CRC; all body-read/decompression settings; message IDs and properties; concatenated records. |
| P8-V02 | `UNIT_VERIFIED` | Every truncated prefix; invalid magic/store/body/property lengths; unknown compressor; record/topic/body/property/decompressed/aggregate caps; no panic or out-of-bounds read. |
| P8-V03 | `UNIT_VERIFIED` | FOUND and all three non-FOUND statuses; tag match/miss/missing tag/empty filter; suggested broker and offsets; inner batch success/fallback; hooks; enrichment and raw-byte release. |

The class-filter source is not executed in Rust. Pinned Java 5.5.1 retains its source string only on the local `SubscriptionData`; the Filter Server performs server-side class filtering. Live Broker, Filter Server and cross-language validation remain Phase 19 work.
