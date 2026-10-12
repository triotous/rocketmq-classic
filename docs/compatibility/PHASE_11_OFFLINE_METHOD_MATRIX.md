# Phase 11 offline method and allocation matrix

Java behavior source: RocketMQ client 5.5.1, commit
`efa1be59d6fc747ad220f036c86030f05f0419f9`. Allocation fixtures invoke the
published Java client strategies directly; no Broker was contacted.

## Java method to Rust implementation and check

| Java 5.5.1 source and method | Rust owner and check | Status |
| --- | --- | --- |
| `AllocateMessageQueueStrategy.allocate/getName`; `AbstractAllocateMessageQueueStrategy.check` | `rebalance::strategy::AllocateMessageQueueStrategy` and `RebalanceError`; P11-V01 Java validation fixtures | PARTIAL — Rust contract and average validation are checked; differential validation of other strategies remains open |
| `AllocateMessageQueueAveragely.allocate` | `AllocateMessageQueueAveragely`; `tests/rebalance.rs::average_allocation_matches_java_551_fixtures_and_partition_invariants` | UNIT_VERIFIED — empty input errors, missing current client, 1/N queues and clients, 10 queues/3 clients, Java UTF-16 ordering, disjointness and coverage |
| `AllocateMessageQueueAveragelyByCircle.allocate` | `rebalance::strategy`; P11-V01 fixture cases | PARTIAL — Java fixtures recorded; Rust implementation and differential remain open |
| `AllocateMessageQueueByConfig.allocate/getMessageQueueList/setMessageQueueList` | `rebalance::strategy`; P11-V01 fixture cases | PARTIAL — Java fixtures recorded; Rust implementation and differential remain open |
| `AllocateMessageQueueByMachineRoom.allocate/getConsumeridcs/setConsumeridcs` | `rebalance::strategy`; P11-V01 fixture cases | PARTIAL — Java fixtures recorded; Rust implementation and differential remain open |
| `AllocateMessageQueueConsistentHash.allocate`; `ConsistentHashRouter.routeNode`; `VirtualNode.getKey`; default MD5 hash | `rebalance::strategy`; P11-V01 fixture cases | PARTIAL — Java fixtures recorded; Rust implementation and differential remain open |
| `RebalanceImpl.doRebalance/rebalanceByTopic/updateProcessQueueTableInRebalance` | Push registration rebalance capability; P11-U01 concurrency and membership tests | NOT IMPLEMENTED |
| `RebalancePushImpl.computePullFromWhereWithException/removeUnnecessaryMessageQueue/messageQueueChanged` | Push initial offset and assignment lifecycle; P11-V02/P11-U01 | NOT IMPLEMENTED |
| `RebalanceService.run` | Existing runtime-owned coalesced rebalance worker and Push `try_rebalance`; P11-U01 timing/shutdown tests | PARTIAL — runtime signal owner exists; Push reconciliation is not wired |

## Java allocation fixture

`fixtures/rebalance/allocation.json` is generated and drift-checked by
`compat/java-reference/src/main/java/compat/RebalanceFixtures.java` using the
actual 5.5.1 classes. It covers all five Java strategies plus the custom-hash
constructor; zero/one/ten queues;
one/three consumers; the 10q/3c case; missing current client; invalid empty
inputs; Unicode topic, broker and client ordering; config-selected queues;
machine-room filters; and default consistent hashing. Java source paths are
recorded in fixture metadata. Fixture SHA-256:
`4a4ad4b705c2afdee6daf92c3aa6972ac1455782c070ac7345885ed15a4169dc`.

## Case status

| Case | Local evidence | Status |
| --- | --- | --- |
| P11-V01 | Java fixture plus direct Rust average-strategy differential | PARTIAL — all-strategy differential remains open |
| P11-V02 | Initial-offset policy and failed query/persist replacement fencing | NOT IMPLEMENTED |
| P11-U01 | Membership transitions, bounded retry, barrier races and shutdown ownership | NOT IMPLEMENTED |
| P11-V03/P11-V04 | Live mixed-client and bidirectional Push scenarios | PENDING_PHASE_19 |

Orderly assignment remains PARTIAL until Phase 12 activates Broker queue locks.
