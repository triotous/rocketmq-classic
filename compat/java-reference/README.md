# Java 5.5.1 offline compatibility oracle

This standalone Maven project pins `org.apache.rocketmq:rocketmq-client:5.5.1`. The local source checkout was inspected read-only at tag `rocketmq-all-5.5.1`, commit `efa1be59d6fc747ad220f036c86030f05f0419f9` (2026-08-20); the active Java checkout remains unchanged at 5.3.3.

## Verified local artifacts and tools

- Client binary SHA-256: `1c39c94a28e59b292e29d699872d693cdc2ce30bd54c4e0f95a4c377fd9dff84`
- Client source SHA-256: `de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`
- JDK: OpenJDK 21.0.2; Maven 3.9.16
- Source paths verified in tag: `remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RemotingCommand.java`, `RocketMQSerializable.java`, `RequestCode.java`, `ResponseCode.java`, and `client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java`.
- Maven dependency is pinned and build validation rejects a changed client version. Resolve/check source artifacts with `mvn -f compat/java-reference/pom.xml dependency:sources` when regenerating evidence.

## Commands

```sh
mvn -f compat/java-reference/pom.xml verify
mvn -f compat/java-reference/pom.xml exec:java -Dexec.args='fixtures --output fixtures/remoting'
mvn -f compat/java-reference/pom.xml exec:java -Dexec.args='decode --input fixtures/remoting/route-001-get-existing-topic.bin'
mvn -f compat/java-reference/pom.xml exec:java -Dexec.args='scenario --id HEARTBEAT-001 --output target/heartbeat.json'
python3 tests/differential/compare.py --self-test
mvn -f compat/java-reference/pom.xml exec:java -Dexec.args=invoked-rpc-oracle
```

The fixture command emits offline Remoting command frames (no live Broker). A frame is accompanied by JSON with version provenance, scenario, request code, decoded semantic fields, body length, full-frame SHA-256 and the command used to generate it. Live scenarios in `tests/differential/scenarios.yaml` explicitly remain `PENDING_PHASE_19`; command-frame fixtures are not claims of successful Broker operations.

## Phase 5 offline reply differential

Run `compat/java-reference/run-rpc-differential.sh` from any directory. It regenerates 15 deterministic Java 5.5.1 response frames under `fixtures/rpc/`, decodes them in the Rust typed-header test, emits Rust frames under `target/p5-rust-rpc-frames/`, and uses the pinned Java decoder to check response code, response flag, ext fields and body bytes. `cargo test -p rocketmq-classic-core --test rpc_java_fixtures --locked` also checks the checked-in evidence without Maven. The labeled outcomes follow `MQClientAPIImpl.processSendResponse`, `processPullResponse`, and `queryConsumerOffset` in pinned source; they are not executions of those methods. `server_baseline_not_contacted` explicitly denotes no server interaction.

Frame coverage: send (four status codes), pull (four status codes with raw FOUND bytes), query offset (success and not-found), unknown send code, route error, and missing success headers. These frame fixtures do not exercise `MQClientAPIImpl` and are not P5-U01 or P5-V01 completion claims; keep Phase 19 cases pending.

Additional invoked-oracle checks exercise V1, V2, reply V1/V2 and batch send in sync, async and oneway modes, including four send statuses and broker errors for sync/async and transport failures for async. Route, send, pull, query offset and assignment also have executable unknown-error and malformed-success cases. Four pinned Java invoked cases (route success, V2 send success, offset success/not-found) are persisted in `fixtures/rpc-invoked/`; Maven `verify` checks for drift and `cargo test -p rocketmq-classic-core --test rpc_invoked_java_fixtures --locked` compares the same requests, responses and outcomes using a Rust scripted peer. This is a subset, not P5-U01 completion; see `docs/compatibility/PHASE_5_OFFLINE_METHOD_MATRIX.md`.

The separate `invoked-rpc-oracle` command (also run by Maven `test`) executes pinned `MQClientAPIImpl` with an injected mock `RemotingClient`. It asserts send V2 oneway, synchronous send statuses (success, disk/slave timeout, slave unavailable) and broker error, async send success/error callbacks driven by the mock, reply V2 and batch oneway dispatch, pull synchronous statuses (found, not found, retry, moved) and broker error, and async pull callbacks for all four statuses, broker error and transport failure. It also checks query-offset success, `OffsetNotFoundException` and generic broker error; update-offset sync success/error and oneway; heartbeat and unregister success/error; route decoded success and topic-not-exist; lock success/error, unlock sync success/error and oneway; consumer send-back success/error; request-mode success/error; consumer-list populated and empty success, absent-body success (throws), and broker error; assignment empty and populated success/error; heartbeat V2 ext-field success, absent-field defaults and error; end-transaction oneway; and POP, POP Lite, ACK, ACK Lite, batch ACK and change-invisibility async dispatch and success/error callbacks. The change-invisibility callback decodes the response header before inspecting its status; absent primitive header fields decode as zero on both success and error. Request codes, transport modes, selected headers/bodies and return values are checked at invocation. ACK Lite actually dispatches `ACK_MESSAGE` (200051), not `ACK_LITE_MESSAGE` (200072). No Broker or NameServer is contacted. This command writes no fixture files; the 15 response frames and their `java_outcome` labels remain source-derived, not executed method results. Pinned `MQClientAPIImpl.sendMessage` returns `SendResult` for SYNC and `null` for ASYNC, not a future. Pinned 5.5.1 `MQClientAPIImpl` has no `notificationAsync`, `pollingInfoAsync`, or dedicated ACK Lite request-code method; these cannot be invoked through this API. Lite subscription control has no identified pinned `MQClientAPIImpl` method invocation; its model/frame evidence does not establish client method parity. Only four invoked cases have persisted Java request/response fixtures and an executable Java-to-Rust method-outcome comparison, so P5-U01 remains open; other methods/branches not enumerated here remain unverified by invocation. No server behavior is tested.

## Licensing

RocketMQ client artifacts are Apache License 2.0. This harness uses the published dependency and does not copy RocketMQ implementation sources. Preserve Apache NOTICE/license obligations if future work copies source-derived code or fixtures.
