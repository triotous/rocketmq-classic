# Phase 10 offline method and configuration matrix

Java behavior source: RocketMQ client 5.5.1, commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`. The source files below were read
from that commit with `git show`; the separate Java checkout was not changed.

This is the Phase 10 slice of the bidirectional compatibility inventory. Each
row records its current evidence; `PARTIAL` means required behavior remains
without implementation or executable coverage. Phase 11 owns real queue
allocation/rebalance; Phase 12 owns orderly callback execution; Phase 13 owns
POP; Phase 16 owns tracing and public consume hooks.

## Java method to Rust implementation and check

| Java 5.5.1 source and method/call site | Rust owner and check | Status |
| --- | --- | --- |
| `DefaultMQPushConsumer` fields/getters/setters: group, model, consume-from policy/timestamp, thread bounds, concurrent max span, pull thresholds/interval/batches, unit mode, retry limit, consume timeout and shutdown wait | `push::PushConsumerConfig`; P10-01 configuration/default/range tests | UNIT_VERIFIED — Java defaults, typed accessors and checked ranges are implemented; runtime effects remain in later P10 rows |
| `DefaultMQPushConsumer.subscribe/unsubscribe`; `DefaultMQPushConsumerImpl.copySubscription` | `PushConsumer::{subscribe,unsubscribe}` and heartbeat registration; P10-01 namespace, subscription and retry-topic tests | UNIT_VERIFIED — subscribe applies the namespace and post-start subscribe sends a heartbeat; unsubscribe removes the exact topic argument without namespace wrapping or an immediate heartbeat, matching the pinned Java facade |
| `DefaultMQPushConsumer.get/setMessageListener`, `get/setSubscription`, `isPause`, `isConsumeOrderly` | `PushConsumer::{get_message_listener,set_message_listener,get_subscription,set_subscription,is_pause,is_consume_orderly}`; `tests/push_pipeline.rs` | UNIT_VERIFIED — listener reference, pause state and active orderly-mode flag are exposed; the legacy topic/expression map applies namespaces and is copied into heartbeat subscriptions |
| `DefaultMQPushConsumer.updateCorePoolSize`; `ConsumeMessageConcurrentlyService.updateCorePoolSize` | `PushConsumer::update_core_pool_size` and owned `ConcurrentConsumeService`; P10 pool-bound tests and `tests/push_pipeline.rs` | UNIT_VERIFIED — accepts only `corePoolSize > 0`, `<= Short.MAX_VALUE`, and strictly `< consumeThreadMax`; accepted values resize owned workers |
| `DefaultMQPushConsumerImpl.start`: `checkConfig`, `copySubscription`, runtime selection, rebalance wiring, offset-store selection/load, consume-service start, consumer registration, runtime start, route update, Broker check, heartbeat and immediate-rebalance request | `push::PushConsumer::{start,shutdown}` with rollback; P10-V04 startup fault-injection and P11-U01 assignment tests | UNIT_VERIFIED — startup acquisition/rollback, runtime wiring and real ProcessQueue assignment are covered locally |
| `DefaultMQPushConsumerImpl.checkConfig`: group validation; non-null model/from-policy/strategy/subscriptions/listener; timestamp format; thread range `[1,1000]` and min<=max; max span `[1,65535]`; queue count `[1,65535]`; topic count `-1` or `[1,6553500]`; queue MiB `[1,1024]`; topic MiB `-1` or `[1,102400]`; interval `[0,65535]`; consume/pull batch `[1,1024]` | `PushConsumerConfig::validate` and `PushConsumer::start`; P10-01 boundary tests | UNIT_VERIFIED — Java-owned group, listener and setting checks are covered; enum types make null model/from-policy states unrepresentable |
| `PullRequest` and `PullMessageService.executePullRequestImmediately/Later`; `DefaultMQPushConsumerImpl.executePullRequestImmediately/Later` | `push::pull_service::PullRequest` and bounded owner scheduler; P10-V02 paused-time task-bound/delay/pause/resume/cancel and P11-U01 assignment tests | UNIT_VERIFIED — due-time scheduler, bounded RPC workers, generation-safe assignment activation, real assignment reconciliation and retry dispatch are implemented |
| `DefaultMQPushConsumerImpl.pullMessage`: dropped/pause/state checks; queue count/size/span flow control; locked-queue gate; subscription and pull header construction; clustering commit offset; async response callback | `push::PushConsumer::run_pull_worker_request/fetch_push_result`; P10-V02 scripted pull/flow-control tests | PARTIAL — pause/drop/flow-control checks, routing, namespace-aware header, clustering commit offset, async pull RPC, decode, class-filter route and real assignment are covered; orderly locked-queue gating remains Phase 12 |
| `DefaultMQPushConsumerImpl` pull callback: FOUND insert/consume/reschedule; NO_NEW/NO_MATCHED next-offset and safe tag-offset correction; OFFSET_ILLEGAL drop/freeze/persist/remove/rebalance; Broker flow-control vs ordinary failure delays; `correctTagsOffset` | `push::PushConsumer::apply_pull_result/schedule_offset_recovery` and `ProcessQueue`; P10-V01/V02 scripted outcome and P11-V02/U01 tests | UNIT_VERIFIED — bounded consume work, safe offset correction, OFFSET_ILLEGAL persistence/removal/rebalance, stale-response rejection and Java retry delays are covered locally |
| `ProcessQueue.putMessage/getMaxSpan/takeMessages/removeMessage/makeMessageToConsumeAgain/setDropped`; `msgCount`, `msgSize`, `queueOffsetMax`, `lastPullTimestamp`, `lastConsumeTimestamp` | `process_queue::ProcessQueue`; P10-V01 duplicate/gap/out-of-order/drop-generation checks; [ADR-0083](../architecture/ADR-0083-process-queue-offset-ownership.md) | UNIT_VERIFIED — local ordered ownership, exact accounting and generation-safe completion pass |
| `ConsumeMessageConcurrentlyService.submitConsumeRequest/processConsumeResult/ConsumeRequest.run` | `consume::concurrent`; P10-U01 listener success/partial/failure, null/panic, retry and DLQ-decision checks | UNIT_VERIFIED — private fixed assignment with a Java-produced message fixture covers success, partial acknowledgement, missing status, panic isolation, retry acceptance and retention when both retry paths fail |
| `ConsumeConcurrentlyContext.ackIndex/delayLevelWhenNextConsume`; `CONSUME_SUCCESS` clamps ack index; `RECONSUME_LATER` resets it to `-1`; BROADCASTING drops failures | `consume::concurrent`; P10-U01 per-model acknowledgement tests | UNIT_VERIFIED — success/partial/reconsume and broadcasting behavior pass local scripted tests |
| `ConsumeMessageConcurrentlyService.sendMessageBack`; `DefaultMQPushConsumer.sendMessageBack` overloads; `DefaultMQPushConsumerImpl.sendMessageBack/sendMessageBackAsNormalMessage` | `PushConsumer::{send_message_back,send_message_back_to_broker}` shares the concurrent retry path; P10-U01 scripted Broker acceptance/failure and fallback-property tests; `tests/push_pipeline.rs` | UNIT_VERIFIED — direct API overloads route to the pinned VIP-channel/5000 ms Broker request and Java retry-topic fallback; Broker DLQ transition remains P10-V03 |
| `DefaultMQPushConsumerImpl.shutdown`; `ConsumeMessageConcurrentlyService.shutdown`; `persistConsumerOffset`; unregister and runtime shutdown | `push::PushConsumer::shutdown` with owned task cancellation/join; P10-V04 slow-listener and no-lost-work checks | UNIT_VERIFIED — shutdown drops assignments, aborts a slow callback after the configured wait, retains unresolved queue contents and joins owned workers |
| `PullAPIWrapper.processPullResult` message decoding/filtering and pull result interpretation | Existing Phase 8 `pull::PullAPIWrapper`; Phase 10 consumes its decoded result without a second decoder | UNIT_VERIFIED Phase 8 boundary |

## Java defaults and phase ownership

Values below are from `DefaultMQPushConsumer.java` at the pinned commit. Queue
and topic size thresholds are MiB; pull batch bytes are bytes; consume timeout
is minutes; pull interval and shutdown wait are milliseconds.

| Java setting | Default | Phase 10 handling |
| --- | ---: | --- |
| `messageModel` | `CLUSTERING` | Implement and validate |
| `consumeFromWhere` | `CONSUME_FROM_LAST_OFFSET` | Implement and validate |
| `consumeTimestamp` | current time minus 30 minutes, `yyyyMMddHHmmss` | Implement format validation and default |
| `consumeThreadMin`, `consumeThreadMax` | `20`, `20` | Implement and validate `[1,1000]`, min<=max |
| `consumeConcurrentlyMaxSpan` | `2000` | Implement and validate `[1,65535]` |
| `pullThresholdForQueue` | `1000` | Implement and validate `[1,65535]` |
| `pullThresholdForTopic` | `-1` | Implement and validate `-1` or `[1,6553500]` |
| `pullThresholdSizeForQueue` | `100` MiB | Implement and validate `[1,1024]` |
| `pullThresholdSizeForTopic` | `-1` MiB | Implement and validate `-1` or `[1,102400]` |
| `pullInterval` | `0` ms | Implement and validate `[0,65535]` |
| `consumeMessageBatchMaxSize` | `1` | Implement and validate `[1,1024]` |
| `pullBatchSize` | `32` | Implement and validate `[1,1024]` |
| `pullBatchSizeInBytes` | `262144` bytes | Implement as pull request size; Java `checkConfig` does not range-check it |
| `unitMode` | `false` | Implement in heartbeat and pull headers |
| `maxReconsumeTimes` | `-1` (concurrent effective default `16`) | Implement for clustering retry/send-back |
| `consumeTimeout` | `15` minutes | Implement for callback timing/expiry policy |
| `awaitTerminationMillisWhenShutdown` | `0` ms | Implement bounded consume-service wait |
| `subscription` | empty map | Implement Java TAG subscription entry points; empty remains valid |
| `postSubscriptionWhenPull` | `false` | Implement pull-header behavior |
| `adjustThreadPoolNumsThreshold` | `100000` | Retain configuration. Java schedules `adjustThreadPool` once per minute, but `incCorePoolSize` and `decCorePoolSize` are empty methods; preserve that no-op automatic behavior. Direct `updateCorePoolSize` remains effective under the Java range checks above. |
| `popThresholdForQueue`, `popInvisibleTime`, `popBatchNums` | `96`, `60000` ms, `32` | Phase 13; do not activate POP in Phase 10 |
| `allocateMessageQueueStrategy`, `clientRebalance`, `messageQueueListener` | average strategy, `true`, unset | Phase 11 owns real allocation and assignment callbacks |
| orderly listener and `suspendCurrentQueueTimeMillis` | no listener, `1000` ms | Reject orderly startup until Phase 12 |
| trace dispatcher / consume hooks | unset/empty | Phase 16 |

## Case ownership and status

| Case | Check required here | Execution boundary | Status |
| --- | --- | --- | --- |
| P10-V01 | Ordered queue ownership, duplicate accounting, gaps, out-of-order completion, stale generation | Local unit tests | UNIT_VERIFIED |
| P10-V02 | Bounded scheduler, pause/flow-control thresholds, delayed retry, dropped generation, send-back failure retention | Local paused-time/scripted tests | UNIT_VERIFIED — scheduler, pressure thresholds, generation fencing, Java retry delays and local retention after both send-back paths fail pass |
| P10-V04 | Startup rollback at every acquisition step; slow callback shutdown preserves unresolved work and joins owned tasks | Local fault-injection tests | UNIT_VERIFIED — startup-stage rollback plus slow-listener cancellation, unresolved-work retention and owned-worker shutdown pass |
| P10-U01 | Private fixed assignment with Java-produced message fixtures and scripted send-back results for concurrent listener/retry decisions | Local unit tests; no production fixed-assignment API | UNIT_VERIFIED — fixture-backed success/partial/failure, null/panic handling, retry metadata and scripted transport outcomes pass; live Broker DLQ transition remains P10-V03 |
| P10-V03 | Live 5.3.4 Broker retry/DLQ transition | Phase 19; not run or passed by Phase 10 | PENDING_PHASE_19 |

Fixture provenance for newly added cases must name the pinned Java source or
Java-produced fixture and its SHA-256 in the test/fixture metadata. This matrix
records local evidence only; live Broker compatibility remains a Phase 19
check.
