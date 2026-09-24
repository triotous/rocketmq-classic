# RocketMQ Java client 5.5.1 request/response code inventory

The numeric source of truth is the pinned Java tag `rocketmq-all-5.5.1`, especially `remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RequestCode.java`, `ResponseCode.java`, and `RemotingSysResponseCode.java`. Phase 0 records required early fixture families; Phase 1 must complete the audited registry before coding Rust constants. A registered code does not imply an RPC implementation.

| Code | Java name | Client operation | Rust implementation status |
|---:|---|---|---|
| 10 | SEND_MESSAGE | Producer send | TODO |
| 11 | PULL_MESSAGE | Consumer pull | TODO |
| 14 | QUERY_CONSUMER_OFFSET | Query offset | TODO |
| 15 | UPDATE_CONSUMER_OFFSET | Update offset | TODO |
| 34 | HEART_BEAT | Heartbeat | TODO |
| 35 | UNREGISTER_CLIENT | Lifecycle | TODO |
| 37 | END_TRANSACTION | Transaction | TODO |
| 41/42 | LOCK_BATCH_MQ / UNLOCK_BATCH_MQ | Orderly consumption | TODO |
| 105 | GET_ROUTEINFO_BY_TOPIC | NameServer route | TODO |
| 200050+ | POP family | POP consumer | TODO |

This is an initial checklist, not the full Java numeric registry. See `JAVA_5_5_1_COMPATIBILITY.md` for status semantics.
