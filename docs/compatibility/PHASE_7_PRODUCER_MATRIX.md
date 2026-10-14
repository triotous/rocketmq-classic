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
| `client/src/main/java/org/apache/rocketmq/client/impl/producer/TopicPublishInfo.java` | `ok`, queue-list/order/router-info/route/cursor getters and setters, filtered selection, previous-broker avoidance, reset, write-queue count, diagnostic text | public `TopicPublishInfo`; live queue-list, route-data, and cursor handles; `FaultStrategy` reads the retained per-info cursor |
| `client/src/main/java/org/apache/rocketmq/client/latency/MQFaultStrategy.java`; `LatencyFaultToleranceImpl.java` | `selectOneMessageQueue`, `updateFaultItem`, `isAvailable`, `detectByOneRound`; `DefaultMQProducerImpl` supplies `ServiceDetector.detect` using `MQClientAPIImpl.getMaxOffset(endpoint, queue, timeoutMillis)` | `FaultStrategy` and producer-owned detector task; `MQClientApi::probe_broker` sends `GET_MAX_OFFSET` for queue 0 with `committed=true` and treats only a successful, valid offset response as reachability |

Java callback overloads are represented by Rust future-mode methods as allowed
by the Phase 7 plan; future scheduling is selected by the caller's Tokio
runtime. Rust `Producer` is itself the implementation handle, so the deprecated
Java `getDefaultMQProducerImpl()` wrapper is not duplicated.

## `DefaultMQProducerImpl` declared-public surface review

The invoked Java fixture now records both public constructors and all declared
public method signatures on the pinned implementation class. The pinned artifact
has 2 public constructors and 82 declared public methods; the Rust inventory
regression pins the count and the presence of direct hook and state/table
accessors. This is a declaration inventory, not proof that every entrypoint's
inputs and branches are equivalent.

| Java implementation entrypoints | Rust owner mapping | P18-V01 disposition |
| --- | --- | --- |
| `start`, `shutdown`, `send*`, queue selection, route refresh, fault updates, topic/admin queries | `Producer` lifecycle/send owner; route and RPC operations are mapped in this matrix and Phase 17 | Main owner flows have fixture/unit evidence. Boolean lifecycle overloads and each raw implementation helper still need an exact call-site and input-branch crosswalk. |
| `registerCheckForbiddenHook`, `hasCheckForbiddenHook`, `executeCheckForbiddenHook`; `registerSendMessageHook`, `hasSendMessageHook`, `executeSendMessageHookBefore/After` | `Producer::{register_check_forbidden_hook,has_check_forbidden_hook,execute_check_forbidden_hook,register_send_message_hook,has_send_message_hook,execute_send_message_hook_before,execute_send_message_hook_after}`; send flows share the same snapshot runners | Direct helper behavior now has invoked Java fixture and Rust integration evidence for registration order, check rejection short-circuit, context mutation, and send-hook panic isolation. `send.rs` uses the same snapshot runners; broad overload/input reconciliation remains open. |
| `registerEndTransactionHook`, `hasEndTransactionHook`, `executeEndTransactionHook`, `doExecuteEndTransactionHook` | `Producer` end-transaction hook APIs and public context | Fixture-backed local, Broker-check, direct execution and trace cases are recorded above and in Phase 14. |
| `getTopicPublishInfoTable` | `Producer::get_topic_publish_info_table()` returns the shared `TopicPublishInfoTable`; entries are shared `TopicPublishInfo` values whose live queue-list, route-data, order/router-info flags, and cursor feed later route snapshots and send selection | Invoked Java fixture and Rust differential cover getter/map identity, put/get/replace/remove, retained value/list/route/cursor aliases, direct queue selection, write-queue count, and a scripted default send after queue-list mutation. The table methods now include `getOrDefault`, `putIfAbsent`, `containsValue`, conditional removal/replacement, all compute/merge variants, `forEach`, `replaceAll`, and ordered `putAll`; fixtures cover nullable key/value/callback/result behavior, including Java's null-conditional-remove and values-view-remove false results, partial `putAll`, and null `replaceAll` result. Retained key/value/entry views cover single/bulk mutation, `removeIf`, nullable inputs, and `Map.Entry.setValue`. Filtered queue selection covers null/empty varargs, a null filter (NPE only when a nonempty queue list evaluates it), null/empty queue lists, and non-short-circuit filter order. Current `fixtures/producer/core.json` SHA-256 `d8e0fb04555fa9e730277d2230f37b48eb0ae159f9f49610d96eff3c369de051`. These close bounded mapping slices only; broad P18-V01 remains open. |
| `get/setServiceState` | `Producer::{service_state,set_service_state}`; public `ProducerServiceState` | The invoked Java fixture and Rust tests cover initial state, all four Java names/ordinals, null assignment, start failure, duplicate-group reset, normal start and shutdown transitions. |
| `getDefaultMQProducer`, `isUnitMode` | `Producer` itself owns the Java default-producer behavior; `Producer::config` and `Producer::is_unit_mode` expose its configuration view | Java identity/configuration and unit-mode values are captured in `implementationStateAccessors`; Rust verifies the same group and unit-mode value without adding a wrapper object. |
| `getMqFaultStrategy` | Producer fault settings are exposed through the direct live `Producer` getters/setters; `FaultStrategy` remains the internal Rust owner | Fault-array aliasing, toggles and later Broker selection are fixture/unit verified above. Direct Java nested filter/thread-local helper behavior remains part of the raw helper audit. |
| `getMqClientFactory`, deprecated `getmQClientFactory` | `ClientRuntime` owns shared runtime behavior; Producer APIs call the runtime for lifecycle, route, RPC and heartbeat work | Java source call sites are mapped to runtime owner flows. The direct returned-factory method surface still needs a reviewed disposition; no raw runtime handle equivalence is claimed. |
| `executeAsyncMessageSend`, `set/getAsyncSenderExecutor`, semaphore helpers | `AsyncSenderExecutor`, Producer runtime backpressure APIs and owned send lifecycle | Submission, rejection, backpressure and deadline behavior are fixture/unit covered. The raw helper callback overload still needs an explicit entrypoint disposition. |
| `init/destroyTransactionEnv`, `checkTransactionState`, `endTransaction`, transaction sends | Phase 14 `TransactionProducer` and producer-owned transaction runtime | Transaction worker/lifecycle and request flows map in the Phase 14 matrix; broad overload/input reconciliation remains part of P18-V01. |
| `request*`, `recallMessage`, trace hooks | Phase 15 request/reply, Producer recall, and Phase 16 trace owners | Owner-level behavior is mapped in their phase matrices; inherited/helper overload and reverse-export reconciliation remains open. |

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
| `get/setProducerGroup`, `CreateTopicKey`, `Topics`, `DefaultTopicQueueNums`, `SendMsgTimeout`, `SendMsgMaxTimeoutPerRequest`, `CompressType/Level/MsgBodyOverHowmuch`, `RetryTimesWhenSendFailed/AsyncFailed`, `RetryResponseCodes`, `RetryAnotherBrokerWhenNotStoreOK`, `MaxMessageSize`, `SendMessageWithVIPChannel`, `LatencyMax`, `NotAvailableDuration`, `SendLatencyFaultEnable`, `StartDetectorEnable`, `AutoBatch`, batch delay/byte limits, async backpressure enable/limits | `ProducerConfig` getter/setter pairs: `producer_group`, `create_topic_key`, `topics`, `default_topic_queue_nums`, `send_timeout_millis`, `send_msg_max_timeout_per_request`, `compress_type`, `compress_level`, `compress_threshold`, `retry_times_when_send_failed`, `retry_times_when_send_async_failed`, `retry_response_codes`/`add_retry_response_code`/`remove_retry_response_code`, `retry_another_broker_when_not_store_ok`, `max_message_size`, `send_message_with_vip_channel`, `latency_max_millis`, `not_available_duration_millis`, `send_latency_fault_enable`, `start_detector_enable`, `auto_batch`, batch limit accessors, and async backpressure accessors; inherited settings use `client_config`/`client_config_mut` | `producer_defaults_match_pinned_java_551`, `remaining_producer_settings_preserve_java_values_and_clamps`, `accumulator_setting_validation_matches_java_ranges_and_sentinels`, `compression_system_properties_match_java_defaults_and_parsing`, fixture tests, and retry/fault/backpressure tests. Construction-time backpressure limit setters clamp to Java minima; Producer runtime setters are mapped below. |
| `setBackPressureForAsyncSendNum/Size` at runtime | `Producer::{backpressure_async_send_num,set_backpressure_async_send_num,backpressure_async_send_size,set_backpressure_async_send_size}` plus construction-time `ProducerConfig` setters | Java adjusts live semaphore permits using the in-flight count. Rust's fair `AdjustableSemaphore` preserves in-flight count across resizing and clamps to Java minima. Invoked Java fixture records 7 held permits becoming 5 available after resizing 1024→12, then 12 after completion; `runtime_resize_preserves_in_flight_count_like_the_java_fixture` compares these outcomes. |
| `is/setEnableBackpressureForAsyncMode` at runtime | `Producer::{enable_backpressure_for_async_mode,set_enable_backpressure_for_async_mode}` plus construction-time config | The Java `runtimeSettings` fixture directly checks the mutable getter/setter. `producer_runtime_backpressure_settings_update_live_limits_and_mode` checks that later sends observe toggles and that already admitted sends retain their permits. |
| `getRetryResponseCodes()` live set / `addRetryResponseCode(int)` | `Producer::retry_response_codes` returns `RetryResponseCodes`, a shared live handle; `Producer::add_retry_response_code`; config getter returns the same handle | Java fixture mutates the returned set, mutates it through `addRetryResponseCode`, then removes through the retained set. Rust `producer_retry_response_codes_are_live_for_sync_retry_decisions` checks those mutations affect subsequent retry decisions; config clones remain independent snapshots. |
| `setBackPressureForAsyncSendNum/SizeInsideAdjust` and count/size read-lock helpers | Producer `*_inside_adjust` setters update the reported values directly; `acquire_backpressure_for_async_send_{num,size}_lock` returns `BackpressureReadGuard`, released by drop | The Rust APIs preserve the Java helper semantics while the fair limiter owns its internal state. `runtime_resize_waits_for_a_public_read_lock_guard` checks setters wait for a held public guard; `producer_runtime_backpressure_settings_update_live_limits_and_mode` covers both raw adjustment setters. |
| `setCallbackExecutor` | `Producer::set_callback_executor(Option<Arc<dyn CallbackExecutor>>)` forwarded to the started shared `RemotingClient` | The producer fixture confirms Java's pre-start null dereference; Rust reports `ProducerError::NotStarted` before start. A configured executor is retained by the shared client, and `None` restores its default callback executor. The facade re-exports `CallbackExecutor`. |
| `setAsyncSenderExecutor`, `DefaultMQProducerImpl.getAsyncSenderExecutor` / `executeAsyncMessageSend` | `Producer::{set_async_sender_executor,async_sender_executor}` and `AsyncSenderExecutor` / `AsyncSenderTask` | `fixtures/producer/core.json` directly checks Java's stable default, custom executor retention and null reset. Rust future-mode send attempts use the effective executor; rejection returns `AsyncSenderExecutorRejected`, while enabled async backpressure runs a rejected task inline, matching Java. Queued work observes the send deadline and retains its permits until it completes. Once submitted, a send continues if its result future is dropped, matching Java's callback submission lifetime. With no custom executor, Rust dispatches on the caller's Tokio runtime; it does not recreate Java's private fixed-size thread pool. The facade re-exports the executor trait and task alias. |
| `get/setCompressType`, `getCompressor` | `Producer::{compressor,set_compress_type}` returns a `ProducerCompressor` snapshot with `compress`/`decompress`; `ProducerConfig` keeps its construction-time setting | The Java fixture invokes all three compressor types, records their concrete class and round-trip result, and checks that an earlier compressor object keeps its type after `setCompressType`. Rust `producer_compressor_snapshot_and_operations_match_java_fixture` checks all types and runtime replacement. |
| `get/setCompressMsgBodyOverHowmuch`, `get/setCompressLevel` at runtime | `Producer::{compress_threshold,set_compress_threshold,compress_level,set_compress_level}`; config accessors remain available before construction | `runtimeCompressionSettings` directly updates Java, checks the threshold boundary and distinct ZLIB outputs at levels 1/9. Rust `runtime_compression_settings_match_java_and_config_clones_are_snapshots` verifies subsequent compression behavior, getters, and detached config-clone values. Java applies both settings to later sends without range validation. |
| `is/setSendLatencyFaultEnable`, `get/setLatencyMax`, `get/setNotAvailableDuration`, and `is/setStartDetectorEnable` at runtime | `Producer::{send_latency_fault_enable,set_send_latency_fault_enable,latency_max_millis,set_latency_max_millis,not_available_duration_millis,set_not_available_duration_millis,start_detector_enable,set_start_detector_enable}`; public nullable `ProducerLongArray` carries the mutable Java `long[]` settings | `runtimeFaultStrategy` and `runtimeFaultStrategyNullArrays` in `fixtures/producer/core.json` cover input-array/getter aliases, live mutation, replacement, enable toggles, subsequent Broker selection, null setter/getter behavior, deferred null-table NPE, short-duration AIOOBE and ignored extra entries. `runtime_fault_strategy_null_and_mismatched_arrays_match_java` and `runtime_detector_toggle_skips_disabled_due_work_and_joins_on_shutdown` cover the Rust behavior. Maven fixture verification passes; current fixture SHA-256 `2f8c6d40c4669a6a662085e8cddef749338c91fcbb1bf64829ef1f59f77a284e`. |
| `initProduceAccumulator`, `get/setAutoBatch` | `Producer::{init_produce_accumulator,auto_batch,set_auto_batch}`; `Producer::start` initializes the accumulator at the Java startup point | The Java lifecycle fixture checks the pre-initialization false getter, initialization, and runtime toggles. Rust `producer_auto_batch_getter_and_setter_follow_accumulator_lifecycle` checks accessor behavior; `auto_batch_flushes_one_message_batch_and_fans_out_results` toggles batching after start and verifies the later direct-send path. |
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
| `get/setCompressType`, `getCompressor`, `Compressor.compress/decompress` | `Producer::{compressor,set_compress_type}`; public `ProducerCompressor` | `compressorSettings` in `fixtures/producer/core.json`; `producer_compressor_snapshot_and_operations_match_java_fixture`; Maven fixture verification | UNIT_VERIFIED for all three Java compression backends, round trips and snapshot behavior after a runtime type update |
| `get/setCompressMsgBodyOverHowmuch`, `get/setCompressLevel` at runtime | `Producer::{compress_threshold,set_compress_threshold,compress_level,set_compress_level}` | `runtimeCompressionSettings` in `fixtures/producer/core.json`; `runtime_compression_settings_match_java_and_config_clones_are_snapshots`; Maven fixture verification | UNIT_VERIFIED for threshold boundary updates, live level updates and independent `ProducerConfig` clones |
| Runtime latency fault settings and Broker detector toggle | `Producer` live getters/setters; public nullable `ProducerLongArray` shared handle | `runtimeFaultStrategy` and `runtimeFaultStrategyNullArrays` in `fixtures/producer/core.json`; `runtime_fault_strategy_settings_match_java_and_change_later_selection`; `runtime_detector_toggle_skips_disabled_due_work_and_joins_on_shutdown`; `runtime_fault_strategy_null_and_mismatched_arrays_match_java`; Maven fixture verification | UNIT_VERIFIED for alias mutation/replacement, live enable flags, subsequent Broker selection and detector task cancellation; null setter/getter behavior; deferred NPE when null tables are read; AIOOBE only when a matching latency threshold indexes a short duration array; extra duration entries are ignored |
| `initProduceAccumulator`, runtime `get/setAutoBatch` | `Producer::{init_produce_accumulator,auto_batch,set_auto_batch}`; `Producer::start` performs the automatic initialization | `autoBatchLifecycle` in `fixtures/producer/core.json`; `producer_auto_batch_getter_and_setter_follow_accumulator_lifecycle`; `auto_batch_flushes_one_message_batch_and_fans_out_results` | UNIT_VERIFIED for pre-init getter, initialization and later runtime batching/direct-send transitions |
| Producer runtime async backpressure limits, enable toggle, live retry-code set and public adjustment/lock helpers | `Producer` runtime accessors; `RetryResponseCodes`; `BackpressureReadGuard` | `runtimeSettings` in `fixtures/producer/core.json`; `producer_runtime_backpressure_settings_update_live_limits_and_mode`; `runtime_resize_preserves_in_flight_count_like_the_java_fixture`; `runtime_resize_waits_for_a_public_read_lock_guard`; `producer_retry_response_codes_are_live_for_sync_retry_decisions`; Maven fixture verification | UNIT_VERIFIED for the recorded runtime toggles, clamping, in-flight permit adjustment, lock interaction and live-set retry behavior |
| Producer callback/sender executor API and submission outcomes | `Producer::{set_callback_executor,set_async_sender_executor,async_sender_executor}`; `AsyncSenderExecutor` | Java `executorSettings` fixture; `pinned_java_producer_core_fixture_matches_defaults_and_validation`; `producer_callback_executor_requires_an_initialized_client`; `producer_callback_executor_forwards_to_shared_remoting_client`; `configured_async_sender_executor_runs_future_send_attempts`; `dropping_future_after_async_submission_does_not_cancel_the_send`; `rejected_async_sender_executor_follows_java_backpressure_fallback`; `queued_async_sender_work_obeys_the_send_deadline`; Maven fixture verification | UNIT_VERIFIED for the recorded setter, forwarding, submission lifetime, rejection, backpressure fallback, permit retention and deadline cases; Tokio task placement is the Rust default |
| Producer implementation CheckForbidden and SendMessage hook helpers | `Producer::{has_check_forbidden_hook,execute_check_forbidden_hook,has_send_message_hook,execute_send_message_hook_before,execute_send_message_hook_after}` | `implementationHookExecution` in `fixtures/producer/core.json` (SHA-256 `ca42f599afeb81e9a977c7ae88547fe7ce2b3b7fa9dec3940055962219a9cb9e`); `implementation_hook_helpers_match_pinned_java_fixture`; existing ordered send-path, rejection and panic-isolation tests; Maven fixture verification | UNIT_VERIFIED for direct hook presence, registration order, mutable context, check-error short-circuit and per-hook send panic isolation; Java implementation method inventory remains 82 methods |
| Producer implementation lifecycle state and unit-mode accessors | `Producer::{service_state,set_service_state,is_unit_mode}`; public `ProducerServiceState` | `implementationStateAccessors` in `fixtures/producer/core.json` (SHA-256 `04bd2ccab1dbf48ba1101f3a1df84e6e16ae9ac339f2ca69a4724dbc5e16c14f`); `implementation_state_accessors_match_pinned_java_fixture`; Producer lifecycle tests | UNIT_VERIFIED for the Java enum values/order, nullable setter, unit-mode/config values, failed-start guard, duplicate-group reset, and normal lifecycle transitions |
| Transaction completion hook API on the Producer | `Producer::{register_end_transaction_hook,has_end_transaction_hook,execute_end_transaction_hook,do_execute_end_transaction_hook}`; public `EndTransactionHook` and `EndTransactionContext` | `endTransactionHooks` in `fixtures/producer/transaction.json`; `p18_end_transaction_hooks_match_java_context_order_and_exception_isolation`; `end_transaction_hook_context_and_methods_are_available_from_the_public_crate_api`; `end_transaction_trace_client_host_matches_java_host_producer_client_id`; Maven fixture verification | UNIT_VERIFIED for registration order, local/Broker-check context, transaction ID from the message, mutable message visibility, per-hook panic isolation, direct execution and trace-hook integration; transaction request details are in Phase 14 |
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
