# Phase 17 — Java admin API coverage audit

This is an implementation audit against Java client source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`. It separates public facade coverage
from the lower-level RPCs already present in Rust. It does not count Phase 19
Broker-backed evidence as Phase 17 unit completion. The audit began at Rust
revision `68c9631d`; later Phase 17 changes are reflected in the rows below.

## Public MQAdmin methods

The three Java facades `DefaultMQProducer`, `DefaultMQPushConsumer`, and
`DefaultMQPullConsumer` inherit the methods in `MQAdmin`. An implementation on
only one Rust facade therefore does not complete this surface.

| Java capability | Rust implementation | Status and remaining work |
| --- | --- | --- |
| `createTopic(key, topic, queues, attributes)` and `createTopic(..., topicSysFlag, attributes)` | `Producer::create_topic`, `Producer::create_topic_with_sys_flag`; core single-Broker `MQClientApi::create_topic` | **Partial.** Producer route lookup, namespace, sorting, master targeting, retries, and partial-success handling are implemented. Push/Pull facade methods are absent. |
| `searchOffset`, `maxOffset`, `minOffset`, `earliestMsgStoreTime` | Core single-Broker methods; Producer, Push, and Pull facade methods | **Implemented for all three facades.** Success-path route and header tests exist; P17-V01 still needs the Java error/not-found and malformed-response cases. |
| `queryMessage(topic, key, max, begin, end)` | Single-Broker `MQClientApi::query_message`; `QueryResult` in core | **Partial.** No facade route lookup/fan-out, bounded multi-Broker aggregation, Java key/topic filtering, watermark merge, no-message error, or cluster selection is implemented. Producer, Push, and Pull wrappers are absent. |
| `viewMessage(topic, messageId)` | Single-Broker `MQClientApi::view_message` takes a physical-offset request header | **Partial.** Java message-ID parsing, facade routing, invalid-ID mapping, and the Java producer/push/pull unique-key fallback behavior are missing. |
| `queryMessageByUniqKey` overloads exposed by the concrete Java facades | Query RPC can set `_UNIQUE_KEY_QUERY` when called directly | **Missing at facade level.** Add the query-result and first-message variants, including Java defaults and each facade's namespace behavior. |

## Concrete facade methods outside `MQAdmin`

| Java method | Rust counterpart | Status and remaining work |
| --- | --- | --- |
| Producer `fetchPublishMessageQueues(topic)` | `Producer::fetch_publish_message_queues` | **Implemented** for the producer facade. |
| Push/Pull `fetchSubscribeMessageQueues(topic)` | Pull has `fetch_message_queues`; Push has no corresponding public method | **Partial.** Check Java route-not-found/empty-route error behavior and expose the source-equivalent method on both consumer facades. |

## Client inbound administrative requests

Java `ClientRemotingProcessor` handles `RESET_CONSUMER_CLIENT_OFFSET`,
`GET_CONSUMER_STATUS_FROM_CLIENT`, `GET_CONSUMER_RUNNING_INFO`, and
`CONSUME_MESSAGE_DIRECTLY`. Rust dispatch recognizes these codes, but
`ConsumerRuntime::inbound_processor` has no Push/Pull implementation; an
unhandled request returns `REQUEST_CODE_NOT_SUPPORTED`. Implement the four
operations with the reset-generation fencing, bounded snapshots, direct-consume
offset isolation, malformed-input handling, and shutdown behavior required by
the Phase 17 plan.

## Required local evidence still open

- **P17-V01:** Every Java public admin operation needs success, error/not-found,
  and malformed-response evidence, including the existing offset and route
  methods.
- **P17-V03:** Reset fencing versus late callbacks/pulls, direct-consume offset
  isolation, and responsive status requests.
- **P17-U01:** Multi-Broker query filtering/merge, watermark, time range, partial
  failure, and Java-compatible terminal error.

**P17-V02** is a Broker-backed Phase 19 check; it is not a substitute for the
local query tests above. The complete Phase 18 compatibility/release gate has
not started.
