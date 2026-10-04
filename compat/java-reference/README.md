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

The separate `invoked-rpc-oracle` command (also run by Maven `test`) executes pinned `MQClientAPIImpl` with an injected mock `RemotingClient`: it asserts send V2 oneway request/body, query-offset synchronous request and decoded success offset, and route synchronous request plus topic-not-exist exception. No Broker or NameServer is contacted. This command writes no fixture files; the 15 response frames and their `java_outcome` labels remain source-derived, not executed method results. Invocation coverage is limited to these three branches; async send, send response processing, route success, offset not-found, and all other RPC branches remain unverified by invocation.

## Licensing

RocketMQ client artifacts are Apache License 2.0. This harness uses the published dependency and does not copy RocketMQ implementation sources. Preserve Apache NOTICE/license obligations if future work copies source-derived code or fixtures.
