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
`9a0cfabcf4234e955f19c5d5cebd438de25a0b5f45b5386d4c880549c91a0d8b`.

## Case status

| Case | Local evidence | Status |
| --- | --- | --- |
| P11-V01 | Java fixture plus direct Rust differential for all five strategies and custom consistent-hash function | UNIT_VERIFIED |
| P11-V02 | Initial-offset policy and failed query/persist replacement fencing | NOT IMPLEMENTED |
| P11-U01 | Membership transitions, bounded retry, barrier races and shutdown ownership | NOT IMPLEMENTED |
| P11-V03/P11-V04 | Live mixed-client and bidirectional Push scenarios | PENDING_PHASE_19 |

Orderly assignment remains PARTIAL until Phase 12 activates Broker queue locks.
