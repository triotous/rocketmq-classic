# Phase 7 Producer compatibility matrix

Oracle: pinned client `org.apache.rocketmq:rocketmq-client:5.5.1`, source commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`; the production server baseline remains
5.3.4. Java source and offline fixtures use the pinned tag. No live Broker or
NameServer was contacted.

## Pinned source paths and call sites

All Java references below are read from the pinned commit above, not the newer
HEAD in the Java checkout.

| Java source | Methods and call sites checked | Rust mapping |
| --- | --- | --- |
| `client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java` | `start`, `shutdown`, `fetchPublishMessageQueues`; default, explicit-queue, selector, oneway, and collection `send` overloads; wrapper dispatch to `sendDirect` / `sendByAccumulator`; `batch(Collection)`; producer setting getters/setters | `Producer::{start,shutdown,fetch_publish_message_queues}`; `send*` and `send_batch*`; `ProducerConfig` |
| `client/src/main/java/org/apache/rocketmq/client/impl/producer/DefaultMQProducerImpl.java` | `start`, `shutdown`, `sendDefaultImpl`, `sendSelectImpl`, `sendKernelImpl`, `tryToFindTopicPublishInfo`, `selectOneMessageQueue`, `updateFaultItem`, `pickTopic`; sync/future/oneway mode branches and the `ServiceDetector.detect` callback | `Producer` lifecycle, route cache, `send`, `selector`, `fault`, and `detect_brokers`; the detector calls `MQClientApi::probe_broker` |
| `client/src/main/java/org/apache/rocketmq/client/Validators.java` | `checkGroup`, `checkMessage`, `checkTopic`; producer call sites before route/send and during batch construction | `validation::{validate_group,validate_message,validate_topic}` and `batch::create_batch` |
| `common/src/main/java/org/apache/rocketmq/common/message/MessageClientIDSetter.java` | `setUniqID` | `UniqueIdGenerator` and immutable send preparation |
| `common/src/main/java/org/apache/rocketmq/common/message/MessageBatch.java` | `generateFromList`, `encode` | `batch::create_batch` and Java-compatible record encoding |
| `client/src/main/java/org/apache/rocketmq/client/impl/producer/TopicPublishInfo.java` | `ok`, `selectOneMessageQueue`, previous-broker avoidance | cached writable routes and `RoundRobinSelector` |
| `client/src/main/java/org/apache/rocketmq/client/latency/MQFaultStrategy.java`; `LatencyFaultToleranceImpl.java` | `selectOneMessageQueue`, `updateFaultItem`, `isAvailable`, `detectByOneRound`; `DefaultMQProducerImpl` supplies `ServiceDetector.detect` using `MQClientAPIImpl.getMaxOffset(endpoint, queue, timeoutMillis)` | `FaultStrategy` and producer-owned detector task; `MQClientApi::probe_broker` sends `GET_MAX_OFFSET` for queue 0 with `committed=true` and treats only a successful, valid offset response as reachability |

Java callback overloads are represented by Rust future-mode methods as allowed
by the Phase 7 plan; future scheduling is selected by the caller's Tokio
runtime. Rust `Producer` is itself the implementation handle, so the deprecated
Java `getDefaultMQProducerImpl()` wrapper is not duplicated.

## Pinned producer API and settings inventory

This inventory was checked against the pinned `DefaultMQProducer`,
`DefaultMQProducerImpl`, `Validators`, `MessageBatch`, `TopicPublishInfo`, and
`MQFaultStrategy` source. The high-level facade re-exports the public types
required to construct/configure a producer, including `CompressionType`.

| Java 5.5.1 source surface | Rust mapping | Evidence / scope |
| --- | --- | --- |
| `DefaultMQProducer` constructors, `start`, `shutdown`, `fetchPublishMessageQueues` | `ProducerConfig::new`, `Producer::{start,shutdown,fetch_publish_message_queues}` | `invalid_group_fails_before_creating_or_registering_runtime`, `invalid_accumulator_settings_fail_before_runtime_creation`, `repeated_start_is_rejected_and_shutdown_is_idempotent`, `shutdown_before_start_keeps_producer_startable`; scripted route cases below. `ProducerConfig` carries constructor settings; start validates and registers. |
| `send` sync overloads, `sendOneway`, `sendDirect`, `sendByAccumulator`; explicit `MessageQueue` and `MessageQueueSelector` overloads | `Producer::{send,send_with_timeout,send_future,send_future_with_timeout,send_oneway,send_oneway_with_timeout,send_to_queue,send_to_queue_with_timeout,send_to_queue_future,send_to_queue_future_with_timeout,send_to_queue_oneway,send_with_selector,send_with_selector_with_timeout,send_future_with_selector,send_future_with_selector_with_timeout,send_oneway_with_selector}` | Scripted cases cover retry, one-way, explicit queue, selector, and mode policy; `mode_specific_attempts_match_java_retry_budgets`, `sync_retries_configured_broker_codes_while_future_retries_transport_only`, `per_request_timeout_only_limits_sync_attempts_that_have_a_retry_after_them`. Rust futures are polled on the caller-selected Tokio runtime, replacing callback methods while preserving the mode-specific send/result flow. |
| `send(Collection<Message>, ...)`, `MessageBatch`, `batch(Collection)` | `Producer::{send_batch,send_batch_with_timeout,send_batch_future,send_batch_future_with_timeout,send_batch_to_queue,send_batch_to_queue_with_timeout,send_batch_to_queue_future,send_batch_to_queue_future_with_timeout}`; `batch::create_batch` | `batch_body_matches_message_decoder_record_layout_and_does_not_mutate_inputs`, `batch_checks_source_supported_rejections_and_aggregate_size`, `encoded_batch_size_limit_includes_inserted_unique_ids`, and pinned Java batch fixture comparisons. |
| `get/setProducerGroup`, `CreateTopicKey`, `Topics`, `DefaultTopicQueueNums`, `SendMsgTimeout`, `SendMsgMaxTimeoutPerRequest`, `CompressType/Level/MsgBodyOverHowmuch`, `RetryTimesWhenSendFailed/AsyncFailed`, `RetryResponseCodes`, `RetryAnotherBrokerWhenNotStoreOK`, `MaxMessageSize`, `SendMessageWithVIPChannel`, `LatencyMax`, `NotAvailableDuration`, `SendLatencyFaultEnable`, `StartDetectorEnable`, `AutoBatch`, batch delay/byte limits, async backpressure enable/limits | `ProducerConfig` getter/setter pairs: `producer_group`, `create_topic_key`, `topics`, `default_topic_queue_nums`, `send_timeout_millis`, `send_msg_max_timeout_per_request`, `compress_type`, `compress_level`, `compress_threshold`, `retry_times_when_send_failed`, `retry_times_when_send_async_failed`, `retry_response_codes`/`add_retry_response_code`/`remove_retry_response_code`, `retry_another_broker_when_not_store_ok`, `max_message_size`, `send_message_with_vip_channel`, `latency_max_millis`, `not_available_duration_millis`, `send_latency_fault_enable`, `start_detector_enable`, `auto_batch`, batch limit accessors, and async backpressure accessors; inherited settings use `client_config`/`client_config_mut` | `producer_defaults_match_pinned_java_551`, `remaining_producer_settings_preserve_java_values_and_clamps`, `accumulator_setting_validation_matches_java_ranges_and_sentinels`, `compression_system_properties_match_java_defaults_and_parsing`, fixture tests, and retry/fault/backpressure tests. Backpressure limit setters clamp to Java minima. |
| `setCallbackExecutor`, `setAsyncSenderExecutor` | No per-producer executor setting; a Rust caller chooses where to poll/spawn its returned future | Rust sends expose completion as futures. Executor placement is caller-owned rather than stored on `Producer`; there is no callback scheduling API. |
| `getCompressor`, `initProduceAccumulator` | `compress_type` selects the common compression backend; `Producer` owns and shuts down its accumulator | The backend object and explicit Java accumulator initialization hook are implementation components; observable compression and auto-batch send behavior are covered above. |
| `request*`, `sendMessageInTransaction`, admin/query/recall, trace/namespace/security | Later phase APIs | Request/reply Phase 15; transaction Phase 14; admin/query Phase 17; namespace/security/trace Phase 16. These are intentionally not claimed as Phase 7 deliverables. |

The reverse inventory found the facade's compression setting type was not
reachable from `rocketmq-classic`; `CompressionType` is now re-exported by both
producer and facade crates and is used directly by the facade example. The
Rust-only `remove_retry_response_code` operation corresponds to removal from
Java's live `getRetryResponseCodes()` set; public validation functions map the
Java `Validators` entry points. Private scripted transport seams remain
test-only.

The re-exported `CompressionType::{find_by_value,get_compression_flag}` map to
Java `CompressionType.findByValue` and `getCompressionFlag`; the enum's values,
names, ordinals, and formatting are covered by the Phase 6 compression API
entry.

The complete public `ProducerConfig` accessor surface is grouped above by its
Java property, including `set_tls_config`/`tls_config` for the shared TLS
configuration adapter. Its API and fixture values are covered by the named
configuration tests; inherited `ClientConfig` accessors are audited in the
Phase 6 API/config map.

Exact producer configuration methods in that mapping are `new`,
`client_config`/`client_config_mut`, `tls_config`/`set_tls_config`,
`producer_group`/`set_producer_group`, `create_topic_key`/`set_create_topic_key`,
`topics`/`set_topics`,
`default_topic_queue_nums`/`set_default_topic_queue_nums`,
`send_timeout_millis`/`set_send_timeout_millis`,
`send_msg_max_timeout_per_request`/`set_send_msg_max_timeout_per_request`,
`compress_type`/`set_compress_type`, `compress_level`/`set_compress_level`,
`compress_threshold`/`set_compress_threshold`,
`retry_times_when_send_failed`/`set_retry_times_when_send_failed`,
`retry_times_when_send_async_failed`/`set_retry_times_when_send_async_failed`,
`retry_response_codes`/`add_retry_response_code`/`remove_retry_response_code`,
`retry_another_broker_when_not_store_ok`/`set_retry_another_broker_when_not_store_ok`,
`max_message_size`/`set_max_message_size`,
`send_message_with_vip_channel`/`set_send_message_with_vip_channel`,
`latency_max_millis`/`set_latency_max_millis`,
`not_available_duration_millis`/`set_not_available_duration_millis`,
`send_latency_fault_enable`/`set_send_latency_fault_enable`,
`start_detector_enable`/`set_start_detector_enable`,
`detect_timeout_millis`/`set_detect_timeout_millis`,
`detect_interval_millis`/`set_detect_interval_millis`,
`auto_batch`/`set_auto_batch`,
`batch_max_delay_millis`/`set_batch_max_delay_millis`,
`batch_max_bytes`/`set_batch_max_bytes`,
`total_batch_max_bytes`/`set_total_batch_max_bytes`,
`enable_backpressure_for_async_mode`/`set_enable_backpressure_for_async_mode`,
`backpressure_async_send_num`/`set_backpressure_async_send_num`, and
`backpressure_async_send_size`/`set_backpressure_async_send_size`.

`ProducerConfig` is a pre-start builder, so its accumulator accessors expose
the configured values as `Option`s. The pinned Java API has a getter/setter
name mismatch for `totalBatchMaxBytes`: `totalBatchMaxBytes(value)` sets the
accumulator-wide hold cap, but `getTotalBatchMaxBytes()` returns the per-batch
hold size, matching `getBatchMaxBytes()`. The fixture records both that getter
result and the actual aggregate hold cap; Rust's `accumulator_settings()` maps
the setter's effect to the aggregate hold limit. The public Rust
`ProducerConfig::total_batch_max_bytes()` accessor mirrors that getter by
returning the configured per-batch value; `set_total_batch_max_bytes()` remains
the input for the aggregate cap consumed by the private accumulator settings.

## Implemented and locally verified

| Java source and behavior | Rust surface | Evidence | State |
| --- | --- | --- | --- |
| Producer defaults, retry codes, latency tables/detector defaults, compression system properties | `ProducerConfig` | `fixtures/producer/core.json`; producer unit and fixture tests; Maven fixture verification | UNIT_VERIFIED |
| Group/topic/message validation and reserved/system topics | `validation` | Pinned Java fixture and Rust differential tests, including UTF-16 length and size boundaries | UNIT_VERIFIED for representable Rust inputs; a null Java body maps to empty-body rejection because Rust `Message` owns a `Vec<u8>` |
| Unique-ID layout and preserving a caller-supplied ID | `UniqueIdGenerator` and send preparation | Java layout fixture and Rust unit tests | UNIT_VERIFIED for layout and supplied-ID preservation |
| Compression threshold and selected compression backend | send preparation and common compression module | Java threshold−1/threshold/threshold+1 fixture; config parsing fixture; Rust unit tests | UNIT_VERIFIED for covered cases |
| Batch validation and Java record encoding | `batch::create_batch` | Pinned invalid/valid cases and Java `MessageBatch.encode` golden bytes; boundary unit tests | UNIT_VERIFIED for covered cases |
| Retry attempt counts, retryable status codes, request deadlines, broker avoidance and fault classification | `send` and `FaultStrategy` | Private scripted transport test exercises two-broker retry with identical body/ID; deterministic tests cover status decisions, deadlines and fault tables | UNIT_VERIFIED for covered flows and decisions |
| Auto-batch defaults, delay/byte limits, eligible-message fallbacks, grouping, result fan-out and shutdown | `ProducerConfig` and `AutoBatchAccumulator` | Java fixture covers initialized defaults and eligibility; Rust unit tests cover limits, grouping, timers, result fan-out and close behavior; private scripted transport covers flush/send behavior | UNIT_VERIFIED for local accumulation and scripted send decisions; Docker Broker validation remains in Phase 19 |
| Async-mode backpressure defaults, clamps, permit acquisition and release | `ProducerConfig` and `Producer` | Java defaults/clamp fixture and Rust semaphore unit test | UNIT_VERIFIED for the static settings and permit lifecycle |
| Producer registration cleanup and optional detector task cancellation | `Producer::start` / `shutdown` | `runtime_start_failure_unregisters_the_producer_lease`, `repeated_start_is_rejected_and_shutdown_is_idempotent`, `shutdown_before_start_keeps_producer_startable`, and the scripted loopback send cases | UNIT_VERIFIED for registration, runtime-start rollback, detector cancellation, and repeated local shutdown; sustained-send leak checks remain P7-V04 in Phase 19 |
| Default-route selection, sync/future/oneway, explicit queue and selector send APIs | `Producer` send methods | Implemented; five scripted TCP loopback tests cover route fallback, sync/future/oneway, queue/selector/batch sends, retries, auto-batch unit behavior and lifecycle | UNIT_VERIFIED for control flow and scripted transport |
| Timeout after a Broker stores a message but before its acknowledgement reaches the client | `send` retry loop | Scripted two-Broker regression records the received request and drops its first acknowledgement; compatibility consequence documented below | UNIT_VERIFIED; both Broker peers received the unchanged body and unique ID |

## Compatibility consequence

A timeout does not establish whether the Broker stored the message. The sync
retry loop can submit the same prepared body and unique ID to another Broker;
applications can therefore observe duplicate delivery. The Producer does not
provide exactly-once delivery.

## Remaining inventory and case ownership

The Phase 7 send-settings inventory is implemented in `ProducerConfig`, with
the Java callback paths represented by Rust futures on the caller's Tokio
runtime. Java's configurable callback executors have no per-producer Rust
setting; scheduling follows the caller's runtime. Transaction and request/reply
operations belong to later phases. Namespace, security and tracing integration
remain assigned to Phase 16.

- **P7-V01:** UNIT_VERIFIED for captured producer and initialized accumulator
  defaults, validation boundaries, compression thresholds, message size and
  batch size cases.
- **P7-U01:** UNIT_VERIFIED using both private scripted transport and the five
  TCP loopback cases in `scripts/phase7-producer-scripted-tests.sh`: two-broker
  retry, response policy, oneway, explicit queue/selector/batch modes, body/ID
  preservation, and lost-ack duplicate delivery. All five loopback cases passed.
- **P7-V02–V04:** PENDING_PHASE_19, as specified by the plan. Do not mark them
  passed before the Docker server 5.3.4 handoff.

The producer detector's local probe adapter has direct scripted RPC coverage
for the queue-0 request fields, Broker error response, and missing/invalid
`offset` response branches. This is covered by
`producer_broker_probe_matches_get_max_offset_request_and_response_validation`
in `crates/rocketmq-classic-core/tests/rpc.rs`: the peer verifies GET_MAX_OFFSET,
topic, queue ID 0, and `committed=true`, and the test checks valid, Broker-error,
missing-offset, and invalid-offset outcomes. The focused test passes. This local
check does not change P7-V02–V04's Phase 19 live Broker status.

The dedicated offline Java producer fixture generator and its drift verification
pass. `mvn -f compat/java-reference/pom.xml verify` also passed, including the
runtime-config loopback fixture and Java decoding of Rust-generated fixtures.
