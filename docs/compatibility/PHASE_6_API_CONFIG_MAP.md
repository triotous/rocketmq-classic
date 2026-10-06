# Phase 6 API and configuration map

Pinned client source: `rocketmq-all-5.5.1`, commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. Java paths below are relative to `client/src/main/java/org/apache/rocketmq/client/`. This supplements the [runtime evidence inventory](PHASE_6_RUNTIME_INVENTORY.md); it is not complete phase acceptance or a live-server compatibility claim.

## ClientConfig: forward and reverse field inventory

Every row maps the Java getter/setter pair to a Rust field. Java plain setters do not validate; Rust retains signed widths and validates at the owning operation. Strings are nullable `Option<String>` except the currently non-null typed SOCKS configuration. Enum null values and null SOCKS input are not represented by this typed boundary; malformed Java null behavior is not claimed equivalent. Namespace and NameServer transitions require the named Rust methods, not direct field assignment. `namespaceInitialized` remains private.

Copy column: **C** means both `cloneClientConfig` and `resetClientConfig` copy the field; **O** means Java omits it (clone constructor default, reset receiver retained). Manager column: **K** means shared-runtime conflict comparison; **ID** means client-ID component; **owner** means retained but not promoted to a runtime transport key. Callback/hook identity and rebalance properties are additional Java-derived sharing inputs, not new ClientConfig fields.

| Java field | Rust field | Type / default / input | Copy | Manager | Actual use / remaining activation owner |
| --- | --- | --- | --- | --- | --- |
| namesrvAddr | namesrv_addr | nullable String; `rocketmq.namesrv.addr` then `ROCKETMQ_NAMESRV_ADDR`, including explicit blank | C | K | normalized getter → transport; absent enables startup/periodic discovery |
| clientIP | client_ip | nullable String; automatic native local-address selection | C | ID | identity; explicit `new(ip)` represents setter |
| instanceName | instance_name | nullable String; `rocketmq.client.name` or DEFAULT | C | ID | identity/PID rename; facade startup invocation Phase 7/consumer owners |
| clientCallbackExecutorThreads | client_callback_executor_threads | i32; available processors | C | K | remoting callback capacity; zero lower fallback, negative rejected |
| namespace | namespace | nullable String; null | C | owner | lazy endpoint/resource/queue helpers; facade and decoded-message use owning phases/16 |
| namespaceInitialized | private namespace_initialized | bool; false | O | private | setter/getter cache; clone resets, reset preserves receiver |
| namespaceV2 | namespace_v2 | nullable String; null | C | K | NamespaceRpcHook `nsd`/`ns` |
| accessChannel | access_channel | LOCAL / Local | O | owner | trace/hook context Phase 16 |
| pollNameServerInterval | poll_name_server_interval | i32 milliseconds; 30000 | C | K | route periodic task; positive period required |
| heartbeatBrokerInterval | heartbeat_broker_interval | i32 milliseconds; 30000 | C | K | heartbeat periodic task; positive period required (Java comment says microseconds, actual scheduler uses milliseconds) |
| persistConsumerOffsetInterval | persist_consumer_offset_interval | i32 milliseconds; 5000 | C | K | optional consumer persistence; actual offset stores Phase 9 |
| pullTimeDelayMillsWhenException | pull_time_delay_millis_when_exception | i64 milliseconds; 1000 | C | owner | push exception retry Phase 10; not a new runtime timer |
| traceMsgBatchNum | trace_msg_batch_num | i32; 10 | O | owner | AsyncTraceDispatcher batching Phase 16 |
| unitMode | unit_mode | bool; false | C | owner | producer/consumer request owners; heartbeat uses each registered consumer's data, not runtime config |
| unitName | unit_name | nullable String; null | C | ID | Java whitespace-sensitive client-ID suffix |
| decodeReadBody | decode_read_body | bool; `com.rocketmq.read.body`, true | C | owner | single message decoder Phase 8 and POP/admin decode owners |
| decodeDecompressBody | decode_decompress_body | bool; `com.rocketmq.decompress.body`, true | C | owner | single message decoder Phase 8 and POP/admin decode owners |
| vipChannelEnabled | vip_channel_enabled | bool; `com.rocketmq.sendMessageWithVIPChannel`, false | C | owner | source-selected Broker RPC VIP conversion; not heartbeat/global transport rewriting |
| useHeartbeatV2 | use_heartbeat_v2 | bool; `com.rocketmq.heartbeat.v2`, false | C | K | V2 negotiation/full/simple/fingerprint behavior |
| useTLS | use_tls | bool; `tls.enable`, otherwise `tls.server.mode == enforcing` | C | K | remoting TLS; provisioning/live acceptance Phase 19 |
| socksProxyConfig | socks_proxy_config | String; `com.rocketmq.socks.proxy.config`, `{}` | C | K | existing remoting SOCKS transport parsing/connect |
| mqClientApiTimeout | mq_client_api_timeout | i32 milliseconds; 3000 | C | K | fresh per-runtime RPC budget; nonpositive expires |
| detectTimeout | detect_timeout | i32 milliseconds; 200 | C | owner | producer fault detector Phase 7 |
| detectInterval | detect_interval | i32 milliseconds; 2000 | C | owner | producer fault detector Phase 7 |
| language | language | JAVA / Java | C | K | only heartbeat V1/V2 configured language; typed RPC default Java; CPP admin reset override owning admin phase |
| enableStreamRequestType | enable_stream_request_type | bool; false | C | ID + K | STREAM suffix and pre-user `ReqT=0` hook |
| sendLatencyEnable | send_latency_enable | bool; `com.rocketmq.sendLatencyEnable`, false | C | owner | producer fault-tolerance selection Phase 7 |
| startDetectorEnable | start_detector_enable | bool; `com.rocketmq.startDetectorEnable`, false | C | owner | producer fault detector Phase 7 |
| enableHeartbeatChannelEventListener | enable_heartbeat_channel_event_listener | bool; true | C | K | weak active-channel listener; successful heartbeat wakes all consumers |
| enableConcurrentHeartbeat | enable_concurrent_heartbeat | bool; false | C | K | concurrent V1 round, shared heartbeat lock |
| concurrentHeartbeatThreadPoolSize | concurrent_heartbeat_thread_pool_size | i32; available processors | C | K | concurrent heartbeat capacity; positive when enabled |
| enableTrace | enable_trace | bool; false | C | owner | trace lifecycle/hooks Phase 16 |
| traceTopic | trace_topic | nullable String; null | C | owner | trace topic Phase 16 |
| maxPageSizeInGetMetadata | max_page_size_in_get_metadata | i32; 2000 | O | owner | metadata pagination in MQClientAPIImpl Phase 17 |

Property booleans follow `Boolean.parseBoolean`: case-insensitive exact `true`, no trimming; malformed values are false. Exact Java property keys are read from the Rust process environment (no JVM System property store). These are constructor inputs, except dynamic zone hooks intentionally reread each request. TLS class-initialization timing is not asserted identical to JVM static initialization.

Evidence: `config::tests::every_constructor_field_matches_pinned_client_config_defaults` exhaustively specifies every struct field with a private property reader; `java_properties_have_exact_precedence_and_parse_boolean_rules` and `tls_enable_inherits_enforcing_mode_unless_explicitly_overridden` verify inputs. `tests/config.rs::clone_and_reset_copy_every_public_setting_except_java_omissions` changes every public field and compares the complete value, including signed-width extremes. `namespace_cache_setter_clone_reset_and_empty_collection_state_matches_java` covers private cache-state omissions. `centralized_defaults_and_java_clone_copy_exact_fields` and the existing invoked `client-config.json` supply separate actual Java default/clone/identity/resource evidence; the exhaustive new assertions are source-based, not new Java fixture cases.

Manager construction-path evidence: `cbc25b5` invokes real, unstarted Java `MQClientManager.getOrCreateMQClientInstance` and observes its runtime config. `tests/config.rs::manager_configuration_copy_matches_invoked_java_without_mutating_caller` confirms defaulted omitted settings, copied V2/concurrency settings, reset lazy namespace flag, unchanged caller and shared identity. The native entry uses full Rust Clone internally, but the production constructor subsequently applies `clone_client_config`; no production bug was demonstrated. Its historical fixture SHA-256 was `e586dbe60da7abf295fbe43f4c9cf97bfcb3482a62479f0c1401215ad94f7db6`. Config integration eight, workspace, Clippy and Maven verify passed; no transport/server starts in the Java oracle.

The rebalance parser fixture at `8822d4a` had SHA-256 `b6b429a61adfb8ef944ba79495e32c84dc45438c03b2e8f14b740a00a7ad3414`. It adds twenty actual Long.parseLong cases used by the two source RebalanceService interval properties. Arabic/fullwidth/mixed/Devanagari decimal input and ASCII/fullwidth signed i64 bounds are accepted; sign-only/space/superscript/supplementary/overflow inputs are rejected. `rebalance_property_signed_longs_match_invoked_java` checks each case for both properties, including existing nonpositive immediate semantics. Shared private `java_number` also supplies the existing checked-i32 route parser without widening its public boundary. Full workspace, Clippy, Maven drift check, formatter/diff and comparator self-test passed (`/tmp/p6-rebalance-long-*.log`); core library64. This is parser/source-call-site evidence, not twenty isolated executions of Java's static property initializer.

The removal fixture at `cbd643d` had SHA-256 `099ee99aec511e165bf45cc3b1cb512e029223ae02b07abac7e0a09de0a9d299`. Actual `managerRemoval` retains the Java live factory map and verifies first/removal/replacement/stale-removal/absent behavior. The native unconditional-removal test matches these identity outcomes; controlled initializer tests prove per-key waiting, unrelated-key progress and cancellation preserving publication. Both source overloads remove mappings without shutting down the removed runtime.

Latest config fixture SHA-256: `9e1f92f7b622d7509e93b5c143641a2b0a8b13cd60316bf468ea293872ab273e`. `b31a7d9` adds actual `managerTable` map mutation outcomes and the live native `FactoryTable` counterpart. Retained handles observe later changes, primitive writes are atomic per key, conditional comparisons use runtime identity, and snapshot is explicitly a detached iteration copy. [ADR-0006](../architecture/ADR-0006-live-client-factory-table.md) records initialization, sharing metadata and cross-manager cleanup ownership. Three integration checks plus private reservation/property tests verify the golden,64-way put-if-absent, owning-value release, config/hook conflicts, logical-ID versus alias cleanup and mutation wait/cancel behavior. Core library68, full workspace, reviewed Clippy, Maven drift check, exact runtime/table gate, comparator/fmt/diff passed (`/tmp/p6-manager-table-*.log`).

## ClientConfig methods and constants

| Java public API | Rust counterpart / evidence |
| --- | --- |
| no-argument construction | `Default`, fallible `new_auto`; native-IP tests and classification fixture |
| buildMQClientId | `build_client_id`; 73 invoked identity cases |
| changeInstanceNameToPID | `change_instance_name_to_pid`; normalized Java PID outcomes and private once/null test |
| getNamesrvAddr/setNamesrvAddr | `get_namesrv_addr/set_namesrv_addr`; endpoint golden and runtime getter wire-boundary check |
| getNamespace/setNamespace (deprecated) | `get_namespace/set_namespace`; lazy-state unit test; raw field is not setter replacement |
| withNamespace(String/Set), withoutNamespace(String/Set) (deprecated) | `with_namespace/with_namespace_set`, `without_namespace/without_namespace_set`; resource golden and private set-dedup test |
| queueWithNamespace/queuesWithNamespace (deprecated) | `queue_with_namespace/queues_with_namespace`; private value/in-place collection checks; no JVM reference identity claim |
| cloneClientConfig/resetClientConfig | `clone_client_config/reset_client_config`; complete-setting test plus private namespace flag test; Rust `Clone` separately copies all fields |
| remaining field getters/setters | public fields in the exhaustive table; naming differs, values/defaults preserved; nullable typed boundary noted above |
| toString | redacted Rust `Debug` diagnostic; intentionally does not log raw SOCKS credentials or claim exact Java text formatting |
| SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, SOCKS_PROXY_CONFIG, DECODE_READ_BODY, DECODE_DECOMPRESS_BODY, SEND_LATENCY_ENABLE, START_DETECTOR_ENABLE, HEART_BEAT_V2 | same-named `ClientConfig` associated constants; `public_property_names_match_pinned_client_config_constants` checks exact strings |

## MQClientInstance public methods

Methods implemented on `ClientRuntime` unless another owner is specified. Evidence abbreviations refer to current files, not unimplemented test IDs: **R** = `tests/runtime.rs`; **C** = private `runtime/control.rs` tests; **T** = private `runtime/tasks.rs` tests; **H** = `tests/heartbeat.rs`; **Q** = private `route/cache.rs` tests plus route golden; **L** = `tests/runtime_lookup.rs`; **M** = `tests/runtime_metadata.rs`. Future-owner rows remain inactive/PARTIAL, never COMPATIBLE.

| Java public method / overload | Rust counterpart | Evidence / owner |
| --- | --- | --- |
| MQClientInstance(config,index,id[,hook]) | private owned construction through manager `get_or_create[/with_hook]`; native index is not new user tuning | R/C, hook wire tests, boot M |
| topicRouteData2TopicPublishInfo / topicRouteData2TopicSubscribeInfo | common conversion + coherent `TopicSnapshot` conversion | Q,25 actual Java converter cases plus two runtime cases; producer TopicPublishInfo selection Phase7 |
| start / shutdown | `start/shutdown`, serialized owned control task | C: 17 explicit / 18 discovery rollback points, cancellation, running identity/shared-user wire gates |
| getClientId / getBootTimestamp / getClientConfig | `client_id/boot_timestamp/config` | R/M/config tests |
| updateTopicRouteInfoFromNameServer() | `update_all_topic_routes`; periodic route tick reuses public entry | R empty-round/no-socket, union/dedup, rejection continuation and callback registry reentry; T existing schedule checks |
| updateTopicRouteInfoFromNameServer(topic) | `update_topic_route(topic, deadline)` | R/Q |
| updateTopicRouteInfoFromNameServer(topic,isDefault,producer) | `update_topic_route_from_default(topic, queue_count, deadline)` or ordinary update | R read-based clamp; producer-owned fallback Phase 7 |
| parseOffsetTableFromBroker | `parse_offset_table_from_broker` | M, 10 actual Java cases; explicit namespace/owned-key conversion |
| checkClientInBroker | `check_client_in_broker` | `tests/client_check.rs`, 15 actual Java cases; consumer-start activation Phase 10/13 |
| sendHeartbeatToAllBrokerWithLock / WithLockV2(isRebalance) | `send_heartbeat_to_all_brokers[/with_rebalance]` | H, private heartbeat gate; V2 supersedes concurrent V1 |
| sendHeartbeatToBroker(id,name,addr[,strictLockMode]) | `send_heartbeat_to_broker[/with_lock_mode]` | H single-peer/active-listener checks |
| adjustThreadPool | `adjust_thread_pool`; periodic tick reuses public entry | T per-owner construction/poll failure and missing capability; actual push adjustment Phase 10 |
| registerConsumer / unregisterConsumer | `register_consumer/unregister_consumer` with identity lease | C/R/registry tests; concrete consumer Phase 9/10/13 |
| registerProducer / unregisterProducer | `register_producer/unregister_producer` with identity lease | C/R/registry tests; concrete producer Phase 7 |
| registerAdminExt / unregisterAdminExt | `register_admin/unregister_admin` lease/last-user guard | registry/control tests; concrete admin object Phase 17 |
| rebalanceLater / rebalanceImmediately / doRebalance | `rebalance_later/rebalance_immediately/do_rebalance` | private rebalance/T; real assignment lifecycle Phase 11 |
| selectProducer / selectConsumer | `select_producer/select_consumer`, upgraded weak owners | registry tests; no strong ownership cycle |
| getBrokerNameFromMessageQueue | `RouteSnapshot::broker_name` | Q static endpoint mapping |
| findBrokerAddressInAdmin / InPublish / InSubscribe | `RouteSnapshot::admin_address/publish_address/subscribe_address` | Q exact/adjacent/fallback/version/slave cases |
| findConsumerIdList / queryAssignment | `find_consumer_id_list/query_assignment` | L exact requests, fresh budgets, VIP/error/Set checks |
| findBrokerAddrByTopic | `find_broker_address_by_topic`, raw topic BrokerData selection | Q/L, 8 invoked candidate/error cases |
| resetOffset / getConsumerStatus | owned inactive inbound capabilities, no direct public owner implementation yet | reset/status matrix; process queues/offset stores and admin flow Phase 9/10/17 |
| getAnExistTopicRouteData / queryTopicRouteData | `get_existing_topic_route/query_topic_route` | L cached-empty/absent refresh |
| getMQClientAPIImpl | `api`; `remoting` exposes its native lower transport | typed API tests; no parallel transport |
| getMQAdminImpl | no concrete admin implementation yet | Phase 17; not fabricated in Phase 6 |
| getScheduledExecutorService | owned `execute_task/execute_task_later` scheduling capability | private pull tests; no public JDK executor imitation |
| getPullMessageService | pull/pop enqueue/scheduled-task methods on runtime | private pull tests; concrete requests/process queues consumer owners |
| getDefaultMQProducer | owned internal producer registration now; concrete public producer not implemented | Phase 7; no fake sending producer |
| getTopicRouteTable | immutable `route_snapshot().topics` | Q/R reader coherence; mutation through owned refresh only |
| consumeMessageDirectly / consumerRunningInfo | inactive inbound capabilities; concrete consumer operation/statistics not implemented | Phase 10/17 activation; no success stub |
| getConsumerStatsManager | concrete statistics owner not implemented | consumer/operational phases; not a new metrics service in Phase 6 |
| getNettyClientConfig | native transport configuration initialized from ClientConfig; no Netty object | callback/TLS/SOCKS transport tests; later lower-client settings audit |
| getProducerTable / getConsumerTable | weak registration/selection/snapshot contracts, not mutable unguarded maps | registry/C; Java map mutation identity is not claimed |

## Other runtime exports, reverse inventory

`ClientRuntimeManager::{new,get_or_create,get_or_create_with_hook,remove_client_factory,remove_client_factory_by_id,factory_table}` represent MQClientManager creation/both removal overloads/live table with the plan-required explicit resettable owner. `FactoryTable` represents source ConcurrentMap access through native owned reads and per-key mutation primitives, not an additional client setting. The former reverse listing of `ClientRuntimeManager::remove` was incorrect: `remove` belongs to private ManagerLink, not the exported manager. The source's public surface is explicitly reconciled below. `with_top_addressing`, `TopAddressing`, and its update callback map the Java addressing SPI. `state`, `RuntimeState`, `RuntimeError`, and `GroupLease` expose native lifecycle/identity/error results, not additional client settings. `MessageRequest` maps pull/pop dispatch inputs; producer/consumer traits expose only source registration, subscription, route, persistence, adjustment, rebalance and owned inactive inbound capabilities. `ConsumerOperation` names the inactive request-code slots in the [inbound matrix](PHASE_6_INBOUND_MATRIX.md).

| MQClientManager public API | Native counterpart / current status |
| --- | --- |
| getInstance | explicit shareable `ClientRuntimeManager::new/Clone`, required by phase plan; facade default sharing Phase7 |
| getOrCreateMQClientInstance(config[,hook]) | `get_or_create/get_or_create_with_hook`; identity/config/oracle/concurrency/cancellation evidence |
| removeClientFactory(id,instance) | `remove_client_factory`; exact identity-safe removal, stale-replacement checks |
| removeClientFactory(id) | async `remove_client_factory_by_id`; source removal/replacement oracle, per-key initializer wait/cancel tests; does not replace identity-safe lifecycle cleanup |
| getFactoryTable | `factory_table` returns live clonable FactoryTable; actual retained-view/mutation oracle and concurrent/ownership/property tests; published reads exclude private initializer reservations |
| getOrCreateProduceAccumulator | producer batching owner Phase7; no fake accumulator in Phase6 |

| Source returned ConcurrentMap capability | Native FactoryTable API |
| --- | --- |
| get/containsKey/size/isEmpty | `get/contains_key/len/is_empty`, owned published values |
| put/putIfAbsent | async `put/put_if_absent`, return preceding identity or None |
| replace(key,value)/replace(key,expected,value) | async `replace/replace_if_same`, atomic previous-value or identity-match result |
| remove(key)/remove(key,value) | async `remove/remove_if_same`; map changes only, no runtime shutdown |
| clear | async `clear`, not a transaction against concurrent insertion |
| copy/traverse map entries | explicit `snapshot` owned copy; retained FactoryTable itself stays live, no JVM iterator/order identity claim |

`fetch_name_server_addr/name_server_addr` map MQClientAPIImpl discovery state, not a new resolver feature. `route_snapshot`, `TopicSnapshot`, `RouteSnapshot`, `BrokerAddress`, and `broker_vip_channel` map Java route/broker/publish/subscribe/static/version tables and MixAll VIP logic with immutable native ownership. `prepare_heartbeat_data` exposes the Java heartbeat preparation capability for owners/tests; it does not add another periodic task. Failure-injection seams, queue probes, property readers and deterministic selection indices are private/test-only.

This inventory identifies source/public boundaries still requiring work; it does not close complete source-branch, malformed-input, rollback or final whole-plan verification. P6-V04 remains PENDING_PHASE_19 against server 5.3.4.
