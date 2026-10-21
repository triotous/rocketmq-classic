# Phase 18 Java message-family API crosswalk

## Source boundary

This inventory uses the pinned RocketMQ Java 5.5.1 source at commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`, not the separate Java working tree.
The source-declared public, non-static instance-method counts are 34 for
`Message`, 41 for `MessageExt`, and 4 for `MessageClientExt`. Constructors are
listed separately: `Message` declares five, `MessageExt` declares two, and
`MessageClientExt` inherits its public no-argument constructor from Java's
implicit default-constructor rule. `MessageExt` also declares three public
static methods; the other two classes declare none. Together the classes
declare 82 public methods (79 instance and 3 static), excluding constructors.

## `Message`

| Pinned Java methods | Rust API and behavior |
| --- | --- |
| Constructors `Message()`, `(String, byte[])`, `(String, String, byte[])`, `(String, String, String, byte[])`, and `(String, String, String, int, byte[], boolean)` | `Message::default`, `Message::new`, `with_tags`, `with_tags_and_keys`, and `with_fields`, respectively; nullable/shared variants retain Java null and byte-array aliasing where Rust callers need them. |
| `getTopic`, `setTopic`, `getBody`, `setBody`, `getFlag`, `setFlag` | `topic[_nullable]`, `set_topic[_nullable]`, `body[_nullable]`, `set_body[_nullable]`, `flag`, and `set_flag`. `MessageExtSubtype` supplies virtual getter/setter hooks when the value crosses a base-typed consumer API. |
| `putUserProperty`, `getUserProperty`, `getProperty`, `hasProperty`, `getProperties` | `put_user_property[_nullable]`, `get_user_property`/`get_nullable_user_property`, `get_property`/`get_nullable_property`, `has_property`/`has_nullable_property`, and the nullable properties views. Subtypes can override `property`, `user_property`, `has_property`, and `properties` or provide a live map with `live_properties`. |
| `getTags`, `setTags`, `getKeys`, `setKeys(String)`, `setKeys(Collection<String>)` | `tags`, `set_tags[_nullable]`, `keys`, `set_keys[_nullable]`, and `set_keys_from[_nullable]`. The collection setter funnels through the String setter, preserving Java virtual dispatch. |
| `getDelayTimeLevel`, `setDelayTimeLevel`, `setPriority`, `getPriority`, `isWaitStoreMsgOK`, `setWaitStoreMsgOK`, `setInstanceId`, `setDelayTimeSec`, `getDelayTimeSec`, `setDelayTimeMs`, `getDelayTimeMs`, `setDeliverTimeMs`, `getDeliverTimeMs` | Matching `Message`/`MessageExt` methods preserve property names, defaults and Java parse/validation behavior; malformed numeric properties return typed Rust parse errors at the Rust API boundary. `MessageExtSubtype` hooks cover the virtual methods and its inherited property-based getters. |
| `getBuyerId`, `setBuyerId`, `getTransactionId`, `setTransactionId`, `toString` | `buyer_id`, `set_buyer_id[_nullable]`, `transaction_id`, `set_transaction_id`, and `Display`/`.to_string()`. The subtype adapter dispatches the transaction accessors and display override. |

## `MessageExt`

| Pinned Java methods | Rust API and behavior |
| --- | --- |
| Constructors `MessageExt()` and `(int, long, SocketAddress, long, SocketAddress, String)` | `MessageExt::new`/`Default` and `with_socket_addresses`. `MessageClientExt::new` preserves the client-extension runtime type. |
| Broker and queue accessors: `getBrokerName`, `setBrokerName`, `getQueueId`, `setQueueId`, `getQueueOffset`, `setQueueOffset` | `broker_name`, `set_broker_name`, `queue_id`, `set_queue_id`, `queue_offset`, and `set_queue_offset`; matching subtype hooks dispatch Java virtual overrides. |
| Message metadata: born/store timestamp, `get/setSysFlag`, IPv6 host-flag setters, body CRC, commit-log offset, store size, reconsume times, prepared transaction offset | Rust accessors and setters with the corresponding names in `MessageExt`; `MessageExtSubtype` contains paired override hooks for every getter/setter and both IPv6 flag setters. |
| `getBornHost`, `setBornHost`, `getStoreHost`, `setStoreHost`, `getBornHostString`, `getBornHostNameString` | `born_host`, `set_born_host`, `store_host`, `set_store_host`, `born_host_string`, and `born_host_name_string`, including nullable host values and subtype dispatch. |
| `getBornHostBytes()`, `getBornHostBytes(ByteBuffer)`, `getStoreHostBytes()`, `getStoreHostBytes(ByteBuffer)` | `born_host_bytes`, `write_born_host_bytes`, `store_host_bytes`, and `write_store_host_bytes`. The byte-array results and bytes written are compared; Java `ByteBuffer` identity and position are represented by a Rust mutable slice and returned byte count. |
| Static `parseTopicFilterType(int)`, `socketAddress2ByteBuffer(SocketAddress)`, `socketAddress2ByteBuffer(SocketAddress, ByteBuffer)` | `parse_topic_filter_type`, `MessageHostAddress::network_bytes`, and `MessageHostAddress::write_network_bytes`. |
| `getMsgId`, `setMsgId`, `getTopicSysFlag`, `setTopicSysFlag`, `getGroupSysFlag`, `setGroupSysFlag`, `toString` | `message_id_for_runtime_type`, `set_message_id`, `topic_sys_flag`, `set_topic_sys_flag`, `group_sys_flag`, `set_group_sys_flag`, and `Display`/`.to_string()`. The runtime subtype adapter dispatches the virtual methods. |

`MessageExt` carries its `Message` fields and exposes the same property, body,
topic, keys/tags, priority and delay behavior. Property-derived Java getters
therefore use the subtype's virtual `getProperty` hook rather than reading a
detached property snapshot.

## `MessageClientExt`

| Pinned Java methods | Rust API and behavior |
| --- | --- |
| `getOffsetMsgId`, `setOffsetMsgId` | `offset_message_id`, `set_offset_message_id`, with `offset_message_id` and `set_offset_message_id` subtype hooks. |
| `getMsgId`, `setMsgId` | `message_id`, `set_message_id`, with matching subtype hooks. The base getter first checks `UNIQ_KEY`, then calls the virtual offset-ID getter; the base setter is a no-op, matching Java. |

`MessageClientExt` dereferences to `MessageExt` for inherited methods. Conversion
to `MessageExt` retains the runtime client/subtype state, as a Java subclass
passed through a base-typed parameter does.

## Override and flow evidence

The complete subtype adapter lives in
`crates/rocketmq-classic-common/src/message/types.rs` as
`MessageExtSubtype`. Its nullable hook results distinguish “inherit the base
value” from an explicit Java `null`. The direct-consume Java oracle invokes
caller-defined `MessageExt` and `MessageClientExt` subclasses through
`MQClientInstance.consumeMessageDirectly`; the Rust owner path is exercised by
`crates/rocketmq-classic-consumer/tests/runtime_direct_consume.rs`.

Evidence checked on 2026-10-21:

- `fixtures/runtime/consumer-operations.json` SHA-256:
  `a8d82baca384b9b7932b4961b051902336155543428047d891eb9f84052d166d`.
- `fixtures/messages/records.json` SHA-256:
  `d0cdfbf2599adb0242a1df0dfa8523818a018c35cb34dac2b8418f8ce4bc972d`.
- `cargo test -p rocketmq-classic-common --test message_decode --locked`:
  26 passed.
- `cargo test -p rocketmq-classic-consumer --test runtime_direct_consume --locked`:
  4 passed.
- `mvn -f compat/java-reference/pom.xml verify`: passed; regenerated and
  checked 29 direct-consume cases, message fixtures, typed-header/body
  cross-decodes and the rest of the pinned offline corpus.

This closes the declared public method and selected virtual-override inventory
for these three message classes. It supersedes earlier notes that left other
`Message`/`MessageExt` overrides or the live subtype properties-map identity
open. It does not close the broader Phase 18 audit of client settings, other
Java classes, call flows, inputs/errors, or reverse Rust exports.
