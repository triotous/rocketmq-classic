# Phase 17 — Java admin API coverage audit

This audit is against Java client source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`. It separates facade coverage,
single-Broker RPCs, and local verification from Phase 19 Broker evidence.

## Public MQAdmin methods

The Java producer, push consumer, and pull consumer expose the shared
`MQAdmin` methods. A method on one Rust facade does not complete the other two.

| Java capability | Rust implementation | Status and remaining work |
| --- | --- | --- |
| `createTopic(key, topic, queues, attributes)` and `createTopic(..., topicSysFlag, attributes)` | `Producer`, `PushConsumer`, `PullConsumer`; shared helper in consumer crate | Implemented on all three facades. Java wrappers apply the namespace to the new topic and discard the attributes argument. Tests cover consumer namespace, topic header, system flag, and a successful route. Error, not-found, malformed/error-path coverage remains in P17-V01. |
| `searchOffset`, `maxOffset`, `minOffset`, `earliestMsgStoreTime` | Core single-Broker RPCs; Producer, Push, and Pull facade methods | Implemented on all three facades. Success-path route/header/value tests exist. P17-V01 still needs Broker errors, not-found and malformed-response cases for each operation. |
| `queryMessage(topic, key, max, begin, end)` | Core single-Broker RPC and bounded fan-out; Producer, Push, Pull facades | Implemented. It selects Brokers by Java cluster rules, passes `lastKey`, filters returned keys/topics, merges messages and the maximum index watermark, tolerates per-Broker failures, and returns Java's `NO_MESSAGE` result when no messages survive. P17-U01 still needs explicit ordering/time-range and terminal-failure cases against the pinned Java implementation. |
| `viewMessage(topic, messageId)` | Core physical-offset RPC; Producer, Push, Pull facades | Implemented. Each facade parses Java IPv4/IPv6 message IDs and uses Java's unique-key fallback when direct lookup fails or the ID is invalid. P17-V01 still needs direct-success, Broker-error, not-found and malformed-body cases across the facade behavior. |
| Concrete facade `queryMessageByUniqKey` methods | Query-result methods on Producer, Push, and Pull | Implemented for the facades in scope. Java defaults and unique-key request fields are covered; P17-V01 still needs not-found/error behavior. |

## Concrete facade methods outside `MQAdmin`

| Java method | Rust counterpart | Status and remaining work |
| --- | --- | --- |
| Producer `fetchPublishMessageQueues(topic)` | `Producer::fetch_publish_message_queues` | Implemented. Route-not-found and empty-route error parity remains under P17-V01. |
| Push/Pull `fetchSubscribeMessageQueues(topic)` | `PushConsumer::fetch_subscribe_message_queues`, `PullConsumer::fetch_subscribe_message_queues` | Implemented. The shared helper refreshes a missing route; both methods remove the namespace from returned queues. Route-not-found, empty-route and malformed-route coverage remains under P17-V01. |

## Client inbound administrative requests

Java `ClientRemotingProcessor` implements `RESET_CONSUMER_CLIENT_OFFSET`,
`GET_CONSUMER_STATUS_FROM_CLIENT`, `GET_CONSUMER_RUNNING_INFO`, and
`CONSUME_MESSAGE_DIRECTLY`. Rust dispatch recognizes these codes, but Push/Pull
runtime registrations still provide no handler, so requests return
`REQUEST_CODE_NOT_SUPPORTED`. All four handlers remain to be implemented and
covered, including reset generation fencing, bounded status/running snapshots,
direct-consume offset isolation, malformed input, missing groups, and shutdown.

## Required local evidence still open

- **P17-V01:** Every Java public admin operation needs success,
  not-found/permission/error, and malformed-response evidence.
- **P17-V03:** Reset versus late pulls/callbacks must not restore old offsets;
  direct-consume must leave normal offsets unchanged; status requests must stay
  responsive.
- **P17-U01:** Complete the multi-Broker Java comparison for duplicate results,
  ordering, bounded time range, partial failure, watermark, and exact terminal
  errors. Existing scripted tests already cover merge, watermark, cluster and
  key filtering, `lastKey`, partial success, empty route, and the no-message
  error.

**P17-V02** is the Broker-backed query check assigned to Phase 19; it does not
replace the local evidence above. Phase 18 has not started.
