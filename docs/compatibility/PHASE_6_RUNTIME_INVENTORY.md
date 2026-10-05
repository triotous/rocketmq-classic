# Phase 6 source and acceptance inventory

Baseline: client 5.5.1 `efa1be59d6fc747ad220f036c86030f05f0419f9`. Paths below are under Java `client/src/main/java/org/apache/rocketmq/client/`, unless stated otherwise. This is an in-progress inventory, not phase acceptance.

| Java source / call site | Rust owner / planned executable evidence | Current status |
| --- | --- | --- |
| `ClientConfig.buildMQClientId`, called by `MQClientManager.getOrCreateMQClientInstance` | `config.rs`, `tests/config.rs`, 73 invoked Java identity cases including Unicode whitespace/null/STREAM | UNIT_VERIFIED for explicit IP identity |
| `ClientConfig` fields, `cloneClientConfig`, `resetClientConfig` | `config.rs`; invoked defaults/clone, property and reset checks | PARTIAL: explicit-IP model; automatic IP/PID/namespace methods not closed |
| `MQClientManager.getOrCreateMQClientInstance`, `removeClientFactory(id, instance)` | `runtime/manager.rs`; concurrent identity, per-key parallelism, failure/removal identity, incompatible settings and cancelled initialization checks | UNIT_VERIFIED construction only; P6-V01 running task/socket gate remains open |
| `MQClientInstance` construction/start/cleanup/shutdown | `runtime/runtime.rs`; P6-V03 each resource boundary fault and terminal cleanup | TODO |
| `registerProducer/registerConsumer/unregister*` | identity leases, duplicate group and independent shared-user shutdown tests | TODO |
| `topicRouteData2TopicPublishInfo/topicRouteData2TopicSubscribeInfo/updateTopicRouteInfoFromNameServer` | existing Phase 1 conversions + coherent `route/cache.rs`; concurrent readers/failed refresh tests | TODO |
| `findBrokerAddressInPublish/Subscribe/Admin`, common `MixAll.brokerVIPChannel` | pure resolution/VIP functions with IPv6/checked-port tests | TODO |
| `startScheduledTask` | owned cancellation/join, fixed-rate route 10ms/30000ms, heartbeat 1000ms/30000ms, persist 10000ms/5000ms; discovery 10000ms/120000ms and adjustment 60000ms/60000ms; paused-time tests | TODO |
| `prepareHeartbeatData`, V1/V2/concurrent send, channel-active listener, fingerprint/version tables | `heartbeat.rs`; P6-U01 scripted bytes/subscription/unregister and negotiation tests | TODO |
| `ClientRemotingProcessor.processRequest`, `RebalanceService`, `PullMessageService` | group/client inbound slots; explicit unsupported error until owning phase; narrow registration/wakeup contracts | TODO |
| P6-V04 live heartbeat/subscription/unregister/shared user | original scenario ID, server 5.3.4 | PENDING_PHASE_19 |

The plan's local serve address setting has no named counterpart in pinned common/client/remoting (`git grep localServe/LOCAL_SERVE`); no new public setting is invented. Actual IP initialization is `common/.../utils/NetworkUtil.getLocalAddress` and explicit `ClientConfig.setClientIP`.

Runtime ownership will be an explicit shareable manager, not a process singleton. Runtime-affecting transport/NameServer/security incompatibility is rejected as required by the plan; producer/consumer-specific settings are not silently promoted into manager keys. Private failure-injection seams must not enter the public API.

Construction implementation: `3079e28`; design rationale: `docs/architecture/ADR-0004-client-runtime-sharing.md` (ADR-0002 was already assigned to NameServer response peer). ClientConfig fixture SHA-256: `dead2b64b2989d84b809465bc00f7914887b5750ce9d55a89cb25a0454b41ab7`. Maven verify regenerates and checks the same pinned invoked JSON. No global phase acceptance is inferred from the construction-only evidence.
