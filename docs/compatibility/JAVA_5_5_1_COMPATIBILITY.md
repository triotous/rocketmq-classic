# Java 5.5.1 compatibility evidence

## Baseline

- Java client: `org.apache.rocketmq:rocketmq-client:5.5.1`
- Source tag/commit: `rocketmq-all-5.5.1` / `efa1be59d6fc747ad220f036c86030f05f0419f9`
- Server baseline: 5.3.4 (`V5_3_4`), reserved for live validation
- Source paths, artifact hashes and tool versions: `compat/java-reference/README.md`
- Status vocabulary: implementation `TODO / IN_PROGRESS / PARTIAL / UNIT_VERIFIED / COMPATIBLE`; live `PENDING_PHASE_19 / PASS / FAIL / BLOCKED`. Fixture existence is not implementation or live-server evidence.

## Phase 1 model and registry status

Phase 1 provides the initial Rust 2024 workspace (`rocketmq-classic-common` and `rocketmq-classic-protocol`), pinned to Rust 1.85.0/MSRV 1.85. The common crate contains message/property models and message/pull flags; protocol contains the full pinned 166-request/68-response integer registry, language and serialization tags, route conversion models, and heartbeat JSON models. Unit coverage cross-checks the Phase 0 fixture sidecars, Java UTF-16 queue ordering, malformed property handling, static/ordered route cases, Java-compatible JSON field names, signed values, and flag preservation. Registry and offline-fixture coverage are not client feature completion or live Broker validation.

## Initial bidirectional inventory

The matrix starts with the source areas required by the master plan. Later phase commits must add each public method/overload, inherited/deprecated setting, source branch, configuration default/type/unit/range/validation, Rust mapping, fixture and unit-test ID. Phase 18 closes both directions, including proof that Rust exposes no Java-incompatible public setting. `TODO` means not yet implemented; no feature completion is claimed here.

| Java class/source (5.5.1 tag) | Java method/setting scope | Rust mapping / test ID | Request code / fixture | Implementation | Live / server |
|---|---|---|---|---|---|
| `remoting/.../protocol/RemotingCommand.java` | encode/decode, response/oneway flags, opaque, ext fields | Phase 2 | protocol fixture families | TODO | PENDING_PHASE_19 / 5.3.4 |
| `remoting/.../protocol/RocketMQSerializable.java` | private header encoding/decoding | Phase 2 | JSON + ROCKETMQ self-test | TODO | PENDING_PHASE_19 / 5.3.4 |
| `remoting/.../protocol/RequestCode.java` | complete numeric request registry | Phase 1 | `REQUEST_CODES.md` | TODO | PENDING_PHASE_19 / 5.3.4 |
| `remoting/.../protocol/ResponseCode.java` | complete response registry and aliases | Phase 1 | `REQUEST_CODES.md` | TODO | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/MQClientAPIImpl.java` | typed RPC calls, response interpretation | Phase 5 | route/send/pull/offset/lock | TODO | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/factory/MQClientInstance.java` | shared runtime, routing, heartbeat, lifecycle | Phase 6 | `HEARTBEAT-001` | TODO | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/producer/DefaultMQProducerImpl.java` | validation, send modes, retries, callbacks | Phase 7 | `PROD-*` | TODO | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/consumer/PullAPIWrapper.java` | pull results, decoding, filtering | Phase 8 | `CONS-*` | TODO | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/consumer/ProcessQueue.java` | message ownership, offsets, lifecycle | Phase 10 | consumer pipeline fixtures | TODO | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/consumer/RebalanceImpl.java` | assignment add/remove and triggers | Phase 11 | `REBAL-*` | TODO | PENDING_PHASE_19 / 5.3.4 |
| `client/.../consumer/store/RemoteBrokerOffsetStore.java` | remote offset lifecycle | Phase 9 | `CONS-001/004` | TODO | PENDING_PHASE_19 / 5.3.4 |
| `client/.../consumer/store/LocalFileOffsetStore.java` | local persistence/recovery | Phase 9 | local store unit cases | TODO | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/consumer/ConsumeMessageConcurrentlyService.java` | concurrent callbacks/retry/send-back | Phase 10 | concurrent consumer cases | TODO | PENDING_PHASE_19 / 5.3.4 |
| `client/.../impl/consumer/ConsumeMessageOrderlyService.java` | orderly callbacks and locks | Phase 12 | `ORDER-001` | TODO | PENDING_PHASE_19 / 5.3.4 |
| transaction producer/check APIs | half message, check, commit/rollback | Phase 14 | `TX-*` | TODO | PENDING_PHASE_19 / 5.3.4 |
| request/reply APIs | correlation, callback completion and responder | Phase 15 | `REQ-001` | TODO | PENDING_PHASE_19 / 5.3.4 |
| Lite/POP APIs and configuration | complete public methods and receipt lifecycle | Phase 13 | `LITE-*`, `POP-*` | TODO | PENDING_PHASE_19 / 5.3.4 |
| namespace, hooks, ACL/TLS, trace | settings, validation, ordering and lifecycle | Phase 16 | source-derived cases | TODO | PENDING_PHASE_19 / 5.3.4 |
| MQAdmin/query public API | methods, result models, fan-out | Phase 17 | source-derived cases | TODO | PENDING_PHASE_19 / 5.3.4 |

## Phase 0 evidence

The Java harness emits offline Remoting request frames for route, heartbeat, send, pull, offset query/update and lock operations. Each `.bin` has a JSON sidecar with source/client/server version labels, scenario ID, codec, code, normalized decoded fields, body length and frame SHA-256. These are protocol command fixtures, not executed RPC results. Live Broker scenarios in `tests/differential/scenarios.yaml` remain `PENDING_PHASE_19`.
