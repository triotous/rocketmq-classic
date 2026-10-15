# Phase 8 offline method matrix

Java source baseline: Apache RocketMQ client 5.5.1, commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. Fixture generation and unit checks do not contact a Broker or NameServer.

## Source-to-Rust mapping

| Java source method or behavior | Rust owner | Evidence |
| --- | --- | --- |
| `common/.../message/Message` constructors, property accessors, Buyer ID / instance ID, delay-level and timer-delay accessors, and `toString()`; `MessageExt` inherits this surface | `Message::{default, new, new_nullable, with_tags, with_tags_and_keys, with_fields, with_nullable_fields, topic, topic_nullable, set_topic, set_topic_nullable, body, body_nullable, set_body, set_body_nullable, flag, set_flag, properties, properties_nullable, get_properties, property_entries, properties_mut, get_property, get_nullable_property, get_user_property, get_nullable_user_property, has_property, set_keys_from, set_keys_from_nullable, buyer_id, set_buyer_id, set_buyer_id_nullable, set_instance_id, set_instance_id_nullable, delay_time_level, set_delay_time_level, delay_time_sec, set_delay_time_sec, delay_time_ms, set_delay_time_ms, deliver_time_ms, set_deliver_time_ms}`; `MessageExt` dereferences to its owned `Message`, and both types use `Display` for Java `toString()` behavior | Invoked Java `messagePublicApi` fixture compares the default constructor and all four parameterized constructor forms, non-null and null topic/body/setter inputs, default null topic/body/properties, lazy property-map initialization including null-name lookups, null-valued system properties and entry enumeration, user-property null rejection and Java `String.trim()` whitespace boundaries (including accepted non-ASCII whitespace), nullable JSON round-trips, null collection and null-element key inputs, signed integer boundaries, Unicode decimal digits, malformed/overflow errors, timer properties, diagnostic text, and identity-based `equals` outcomes for same and distinct objects, and live `MessageConst.STRING_HASH_SET` insert/remove behavior. `property_entries()` exposes null-valued Java property-map rows in deterministic key order; `get_properties()` remains the non-null Rust string-map view. `message_public_api_matches_java_551_fixture` also checks inherited access on `MessageExt`. Fixture `fixtures/messages/records.json` SHA-256: `2ec8cd462b925af5f31fe73a9d4c9103c50bc7d39169cc119c8305342b73d56c`. Broader inherited API and reverse-export branches stay under P18-V01. |
| `common/.../message/MessageDecoder.decode(ByteBuffer, readBody, decompressBody, isClient, isSetPropertiesString, checkCRC)` | `rocketmq_classic_common::message::decode_one` plus consumer client-ID enrichment | P8-V01 compares ten pinned records with CRC enabled and checks metadata, body, properties and generic IDs. Pull processing adds `MessageClientExt` ID semantics before filtering/hooks; `isSetPropertiesString` remains false there, as in `PullAPIWrapper`. Rust returns typed errors for malformed records instead of Java's `null` plus cursor-to-limit behavior, as required by the phase contract. |
| `MessageDecoder.decodesBatch` | `decode_many` | P8-V01/P8-V02 cover concatenated golden records, checked record progress, aggregate limits and every truncated prefix. |
| `MessageVersion.valueOfMagicCode/getTopicLength` | `decode_one` | V1 magic `-626843481` uses signed 8-bit topic length; V2 `-626843477` uses signed 16-bit length. |
| `MessageDecoder.createMessageId` | `decode_one` | Store-host IP bytes + big-endian port + commit-log offset, upper-case hex; producer `UNIQ_KEY` is kept separate. IPv4 and IPv6 lengths are checked against Java. |
| `MessageClientExt.getMsgId` / `getOffsetMsgId` | consumer client-ID enrichment / `MessageExt::{message_id, offset_message_id}` | Java `getMsgId` returns `UNIQ_KEY` when present, otherwise `offsetMsgId`; `getOffsetMsgId` retains the store-host/offset ID. Fixtures carry both generic and client-decoded expectations. |
| `MessageSysFlag` host/compression bits and `CompressorFactory` | `sysflag::message` and bounded internal decompression | P8-V01 covers independent born/store IPv6 flags and ZLIB, LZ4 and ZSTD; P8-V02 covers output caps and unsupported compressor values. |
| `PullAPIWrapper.processPullResult` | `rocketmq_classic_consumer::pull::PullAPIWrapper::process_pull_result` | P8-V03 covers status/suggested broker handling, FOUND decode, optional inner-batch unwrap, tag filtering, hooks, transaction/min/max/broker/queue enrichment, offset delta and raw-buffer release. |
| `DefaultMQPushConsumerImpl.subscribe(topic, fullClassName, filterClassSource)` | Public `PushConsumer::subscribe_with_class_filter`; crate-private `build_class_filter_subscription_data` constructs the local value | Builds the same local `SubscriptionData` fields and retains the source while excluding it from heartbeat JSON. Phase 10 fixture coverage invokes the Java facade overload, checks namespace application and pre-start storage, then verifies heartbeat publication once a client factory is available. The pinned 5.5.1 client source has no class-filter upload API or call; Filter Server routing is covered below. |
| `PullAPIWrapper.computePullFromWhichFilterServer` and `PullSysFlag` | crate-internal `resolve_pull_broker_address` / `select_filter_server`, existing `sysflag::pull` | P8-V03 covers normal pulls keeping the Broker address, class-filter pulls choosing from the route's broker-address entry, and missing/empty filter-server lists. These are internal helpers, not additional Rust public API. |

## Rust-to-Java public API/configuration map

| Rust public surface | Java source/API or required phase behavior | Unit evidence |
| --- | --- | --- |
| `DecodeOptions::{read_body, decompress_body, set_properties_string}` | `ClientConfig.decodeReadBody/decodeDecompressBody`; `MessageDecoder.decode` parameters | P8-V01 `body_decode_options_match_java_and_reject_truncated_topics` |
| `DecodeOptions::verify_crc` | `MessageDecoder.decode(..., checkCRC)`; pull wrapper uses Java's `false` default | P8-V01 fixture decoding; P8-V02 opt-in mismatch behavior |
| `decode_one` / `decode_many` | `MessageDecoder.decode` / `MessageDecoder.decodesBatch` | P8-V01/P8-V02 |
| `MessageExt::{message_id, offset_message_id}` | Generic `MessageExt.getMsgId`; client `MessageClientExt.getMsgId/getOffsetMsgId` with Java's `UNIQ_KEY` fallback | P8-V01 generic ID fixture; P8-V03 client and inner-batch ID assertions |
| `MessageExt::{born_host, store_host, set_born_host, set_store_host, with_socket_addresses}` | `MessageExt.get/setBornHost`, `get/setStoreHost`, and `MessageExt(int, long, SocketAddress, long, SocketAddress, String)` | Invoked Java 5.5.1 `messageExtHostApi` fixture checks resolved/unresolved `InetSocketAddress`, constructor fields, host strings and value behavior; `message_ext_socket_address_api_matches_java_551` |
| `MessageExt::{born_host_string, born_host_name_string}` / `MessageHostAddress::{host_name, host_address_string, socket_address_string}` | `getBornHostString`, `getBornHostNameString`, `NetworkUtil.socketAddress2String` | Invoked Java 5.5.1 host API fixture compares IPv4/IPv6 format and unresolved handling; the Rust differential checks the typed accessors and socket-address string |
| `MessageExt::{born_host_bytes, store_host_bytes, write_born_host_bytes, write_store_host_bytes}` / `MessageHostAddress::{network_bytes, write_network_bytes}` | `getBornHostBytes`/`getStoreHostBytes`, their `ByteBuffer` overloads, and `socketAddress2ByteBuffer` | Invoked Java fixture compares IPv4/IPv6 bytes and caller-buffer output; Rust checks unresolved and undersized-buffer errors |
| `MessageExt::{topic_sys_flag, set_topic_sys_flag, group_sys_flag, set_group_sys_flag}` | `get/setTopicSysFlag`, `get/setGroupSysFlag` over transient properties | Invoked Java `messageExtSystemFlagsApi` fixture and `message_ext_system_flag_helpers_match_java_551` cover null/empty/set/clear/malformed values and Java Unicode decimal digits |
| `MessageExt::{set_born_host_v6_flag, set_store_host_address_v6_flag, parse_topic_filter_type}` / `TopicFilterType` | `setBornHostV6Flag`, `setStoreHostAddressV6Flag`, `parseTopicFilterType` | Invoked Java fixture compares preserved flag bits and single/multi-tag selection |
| `MessageExt::to_string` / `java_to_string` | Java `MessageExt.toString()` plus `Message.toString()` | Invoked fixture and `message_ext_system_flag_helpers_match_java_551` compare all fields, signed body bytes and a single property; multiple properties use deterministic Rust map order because Java `HashMap` iteration order is unspecified |
| `MessageHostAddress::parse_java_socket_address` | `NetworkUtil.string2SocketAddress` used by `ClientRemotingProcessor.receiveReplyMessage` | Pinned reply processor fixture and `reply_host_addresses_match_invoked_java_validation` cover IPv4, unbracketed IPv6, Unicode decimal ports, port bounds and invalid inputs |
| `PullAPIWrapper::{process_pull_result, register_filter_message_hook}` and `FilterMessageHook` | `PullAPIWrapper.processPullResult`, `registerFilterMessageHook`, and `FilterMessageHook` | P8-V03 pull-result and hook checks |
| `ProcessedPullResult` | Rust wrapper for Java `PullResultExt` plus `msgFoundList` | P8-V03 status and message-list checks |
| `PushConsumer::subscribe_with_class_filter` | `DefaultMQPushConsumerImpl.subscribe(topic, fullClassName, filterClassSource)` | P8-V03 class-filter setup and tag-filter suppression exercise the subscription created through the public facade. The builder is crate-private and does not expand Rust's public API. |

Java `Message` null topic/body/default states and nullable system-property
values are covered by the invoked fixture. Java map enumeration includes null
property values; Rust exposes those rows through `Message::property_entries()`
while the existing `MessageProperties` API remains a string-to-string map.
Broader inherited API, input/control-flow branches, and Rust reverse-export
reconciliation remain open under P18-V01.

The public decoder signatures are `decode_one(record, options)` and
`decode_many(records, options)`. Fixed internal bounds cap each record at 16 MiB,
each decompressed body at 64 MiB, and aggregate decoded data at 64 MiB. No public
`DecodeLimits` type or per-call limit argument is exposed. The limited
decompression helper is crate-internal; the public decompressor uses the same
fixed output cap. Record subfields are bounded by the declared record size,
with no independent public body/topic/property limits.

## Record layout captured by the fixture generator

All integers use Java `ByteBuffer` big-endian order. After the 4-byte total-size and 4-byte magic fields, the record fields are body CRC (i32), queue ID (i32), flag (i32), queue offset (i64), physical offset (i64), system flag (i32), born timestamp (i64), born host (4/16 IP bytes + i32 port), store timestamp (i64), store host (4/16 IP bytes + i32 port), reconsume count (i32), prepared transaction offset (i64), body length/body, version-specific topic length/topic, and properties length/properties. Store size bounds every field read.

## Local verification status

| ID | Status | Coverage |
| --- | --- | --- |
| P8-V01 | `UNIT_VERIFIED` | Ten Java records: both magic versions; all four born/store IPv4/IPv6 combinations; all three compressors; valid CRC; all body-read/decompression combinations; `propertiesString`; generic/client IDs; concatenated records. |
| P8-V02 | `UNIT_VERIFIED` | Every truncated prefix; invalid magic/store/body/topic/property lengths; unknown compressor; record/decompressed/aggregate caps; empty compressed body; no panic or out-of-bounds read. |
| P8-V03 | `UNIT_VERIFIED` | FOUND and all three non-FOUND statuses; tag match/miss/missing tag/empty filter; suggested broker and offsets; inner batch success/fallback; hooks; enrichment and raw-byte release; class-filter setup/routing. |

The class-filter source is not executed in Rust. Pinned Java 5.5.1 retains its source string only on the local `SubscriptionData`; the Filter Server performs server-side class filtering. Live Broker, Filter Server and cross-language validation remain Phase 19 work.
