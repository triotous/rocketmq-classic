# Phase 11 offline method and allocation matrix

Java behavior source: RocketMQ client 5.5.1, commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`. Allocation fixtures invoke the
published Java client strategies directly; no Broker was contacted.

## Java method to Rust implementation and check

| Java 5.5.1 source and method | Rust owner and check | Status |
| --- | --- | --- |
| `AllocateMessageQueueStrategy.allocate/getName`; `AbstractAllocateMessageQueueStrategy.check` | `rebalance::strategy::AllocateMessageQueueStrategy` and `RebalanceError`; `tests/rebalance.rs` | UNIT_VERIFIED — exact input-validation precedence, missing-current result and all strategy names match Java fixtures |
| `AllocateMessageQueueAveragely.allocate` | `AllocateMessageQueueAveragely`; `tests/rebalance.rs::average_allocation_matches_java_551_fixtures_and_partition_invariants` | UNIT_VERIFIED — empty input errors, missing current client, 1/N queues and clients, 10 queues/3 clients, Java UTF-16 ordering, disjointness and coverage |
| `AllocateMessageQueueAveragelyByCircle.allocate` | `AllocateMessageQueueAveragelyByCircle`; P11-V01 Java differential | UNIT_VERIFIED — queue modulo consumer assignment matches Java |
| `AllocateMessageQueueByConfig.allocate/getMessageQueueList/setMessageQueueList` | `AllocateMessageQueueByConfig`; P11-V01 Java differential | UNIT_VERIFIED — configured queues are returned without filtering; unset list maps to empty assignment |
| `AllocateMessageQueueByMachineRoom.allocate/getConsumeridcs/setConsumeridcs` | `AllocateMessageQueueByMachineRoom`; P11-V01 Java differential and split edge checks | UNIT_VERIFIED — Java room filtering, sorting and quotient/remainder assignment match |
| `AllocateMessageQueueConsistentHash.allocate`; `ConsistentHashRouter.routeNode`; `VirtualNode.getKey`; default MD5 and custom `HashFunction` | `AllocateMessageQueueConsistentHash`; P11-V01 default/custom hash Java differential | UNIT_VERIFIED — ten virtual nodes, signed custom hashes, ring collisions and queue-string hash inputs match |
| `DefaultMQPullConsumer.get/setAllocateMessageQueueStrategy`, `isEnableRebalance/setEnableRebalance`, queue-listener and registration methods | `PullConsumerConfig` plus `PullConsumer::{register_message_queue_listener, set_message_queue_listener, register_topics, set_register_topics, add_register_subscriptions, clear_register_subscriptions, enable_rebalance}`; P11-U02 default, namespace, selector and runtime tests | UNIT_VERIFIED — average allocation and enabled rebalance are defaults; listeners, registered topics/subscriptions and runtime enablement are wired |
| `MQPullConsumer.registerMessageQueueListener`; `DefaultMQPullConsumerImpl.fetchMessageQueuesInBalance` | `PullConsumer::{register_message_queue_listener, fetch_message_queues_in_balance}`; P11-U02 lifecycle and namespace tests | UNIT_VERIFIED — callbacks carry the all/assigned sets; the in-balance query requires a running consumer and removes the configured namespace from returned queues |
| `RebalanceImpl.doRebalance/rebalanceByTopic/updateProcessQueueTableInRebalance` | Push and Pull `ConsumerRuntime::try_rebalance`; P11-U01/P11-U02 membership, empty-route, broadcast, listener, persistence and generation-fencing tests | UNIT_VERIFIED — route absence/emptiness is handled before membership lookup, unavailable membership preserves assignments, and LiteSelective follows Java's no-op default branch |
| `RebalancePushImpl.computePullFromWhereWithException/removeUnnecessaryMessageQueue/messageQueueChanged` | Push initial offset and assignment lifecycle; P11-V02/P11-U01 | UNIT_VERIFIED — Java initial-offset fixtures match, query failures do not fall back to zero, failed removal persistence retains the dropped generation, and assignment changes notify the listener |
| `RebalancePullImpl.removeUnnecessaryMessageQueue/messageQueueChanged` | Pull assignment table reconciliation; P11-U02 broadcast/clustering lifecycle tests | UNIT_VERIFIED — removed offsets persist before removal, newly assigned queues clear stale offsets, callback failures are isolated, and unavailable membership keeps the old assignment |
| `RebalanceService.run` | Runtime-owned coalesced rebalance worker and Push/Pull `try_rebalance`; P11-U01/P11-U02 serialization, membership-transition, race and shutdown tests | UNIT_VERIFIED — startup, topic updates and membership notifications reach serialized reconciliation; retries retain safe queue state and owned workers stop on shutdown |

## Java allocation fixture

`fixtures/rebalance/allocation.json` is generated and drift-checked by
`compat/java-reference/src/main/java/compat/RebalanceFixtures.java` using the
actual 5.5.1 classes. It covers all five Java strategies plus the custom-hash
constructor; zero/one/ten queues;
one/three consumers; the 10q/3c case; missing current client; invalid empty
inputs; Unicode topic, broker and client ordering; config-selected queues;
machine-room filters; and default consistent hashing. Java source paths are
recorded in fixture metadata. Fixture SHA-256:
`9a0cfabcf4234e955f19c5d5cebd438de25a0b5f45b5386d4c880549c91a0d8b`.

`fixtures/rebalance/initial-offsets.json` is generated and drift-checked by
`compat/java-reference/src/main/java/compat/RebalanceInitialOffsetFixtures.java`
against the same pinned Java client. Fixture SHA-256:
`6db5b5ddc9ba68c85dd51ef769394ae8551f678412db5f1cae83b05f118435a4`.

## Case status

| Case | Local evidence | Status |
| --- | --- | --- |
| P11-V01 | Java fixture plus direct Rust differential for all five strategies and custom consistent-hash function | UNIT_VERIFIED |
| P11-V02 | Java initial-offset fixture differential; failed query/persist and replacement-generation fencing | UNIT_VERIFIED — `java_initial_offset_fixtures_match_policy_and_retry_topic_selection`, `initial_offsets_prefer_store_and_never_turn_query_failures_into_zero`, `failed_offset_persist_retains_the_dropped_generation_for_retry`, and `activation_fences_old_generations_and_rolls_back_scheduler_failure` |
| P11-U01 | Membership transitions, bounded retry, pull/listener barrier races, concurrent triggers and shutdown ownership | UNIT_VERIFIED — consumer library tests cover join/leave convergence, missing-route notifications, in-flight pull/listener fencing, persist retry, serialized triggers, startup and shutdown |
| P11-U02 | Pull rebalance settings and public registration/query API; broadcasting and clustering assignment, listener callbacks, safe offset removal and retry on unavailable membership | UNIT_VERIFIED — `pull_rebalance_settings_default_to_java_values_and_registration_sets_listener`, `pull_runtime_rebalance_assigns_all_broadcast_queues_and_persists_removals`, `pull_clustering_rebalance_uses_membership_and_keeps_assignment_when_unavailable`, and `startup_registration_and_shutdown_obey_consumer_lifecycle` |
| P11-V03/P11-V04 | Live mixed-client and bidirectional Push scenarios | PENDING_PHASE_19 |

Orderly assignment remains PARTIAL until Phase 12 activates Broker queue locks.

## Validation record

On 2026-10-12, these checks passed:

- `cargo fmt --all -- --check` and `git diff --check`
- `cargo test -p rocketmq-classic-consumer --locked` (consumer library and integration targets)
- `cargo build -p rocketmq-classic --examples --locked`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `python3 tests/differential/compare.py --self-test`
- Maven execution `exec:java@verify-java-rebalance-initial-offset-fixtures`

The full offline Maven `verify` stopped at a separate drift in
`fixtures/runtime/client-config.json`; the Phase 11 initial-offset fixture
verification passed independently. `cargo test --workspace --locked` reached
the core crate and failed two host-interface-dependent tests: this runner
reports `192.168.18.113` where the existing fixtures expect `10.10.10.111`.
The complete consumer package test run passed, including the allocation and
initial-offset fixtures; the two core failures are unrelated to Phase 11.
