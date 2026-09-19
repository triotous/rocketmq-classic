# RocketMQ Classic — Phase Implementation Index

## 1. Scope and verified repository state

This directory expands [the master plan](00-rocketmq-classic-plan.md) into **19 sequential phase plans (Phase 0–18)**, each with an English primary and equivalent Traditional Chinese translation. Numbered filenames `01`–`19` are document numbers; the `phase-00`–`phase-18` components preserve the original phase numbers. The September 2026 directory is intentionally retained from the requested parent plan.

Repository inspection found only the master plan and its SVG diagrams. There is no Cargo workspace, Java harness, source code, dependency lockfile or test suite yet. All source paths, signatures, test targets and commands in the child plans are proposed implementation contracts, not claims about existing code. This planning work did not download/verify the Java release or run client tests. Phase 0 must verify the requested tag `rocketmq-all-5.5.1`, commit `efa1be59d6fc747ad220f036c86030f05f0419f9` and Maven artifact `org.apache.rocketmq:rocketmq-client:5.5.1` before generating authoritative fixtures. Stop for review if they disagree; never silently switch baselines.

The master plan retains overall scope and compatibility definitions. Child plans add task order, concrete mechanics and acceptance evidence; they do not authorize silently dropping features. Original Pn-NN task IDs are preserved, sometimes grouped under one implementation step. Added IDs fill bootstrap/testing/lifecycle gaps. Phase 16–18 task IDs map to the master’s numbered feature sections.

## 2. Sequential reading and implementation order

Read the English primary as authoritative; use its paired Chinese file for the same requirements. Finish the current phase’s gate before the next, with the explicit cumulative Push gate described in section 3.

| Order | Phase | English primary | 繁體中文 |
| ---: | ---: | --- | --- |
| 01 | 0 | [Freeze the reference and test oracle](01-phase-00-reference-oracle.md) | [凍結參考基準與測試 Oracle](01-phase-00-reference-oracle.zh-tw.md) |
| 02 | 1 | [Common types, constants and protocol models](02-phase-01-common-protocol-models.md) | [共用型別、常數與協定模型](02-phase-01-common-protocol-models.zh-tw.md) |
| 03 | 2 | [Remoting frame codec](03-phase-02-remoting-codec.md) | [Remoting frame codec](03-phase-02-remoting-codec.zh-tw.md) |
| 04 | 3 | [TCP/TLS remoting transport](04-phase-03-remoting-transport.md) | [TCP/TLS Remoting transport](04-phase-03-remoting-transport.zh-tw.md) |
| 05 | 4 | [Typed request headers and protocol bodies](05-phase-04-typed-headers-bodies.md) | [Typed request header 與 protocol body](05-phase-04-typed-headers-bodies.zh-tw.md) |
| 06 | 5 | [MQClientApi typed RPC facade](06-phase-05-typed-rpc-api.md) | [MQClientApi typed RPC facade](06-phase-05-typed-rpc-api.zh-tw.md) |
| 07 | 6 | [Client runtime, routing and heartbeat](07-phase-06-runtime-routing-heartbeat.md) | [Client runtime、routing 與 heartbeat](07-phase-06-runtime-routing-heartbeat.zh-tw.md) |
| 08 | 7 | [Producer core](08-phase-07-producer-core.md) | [Producer 核心](08-phase-07-producer-core.zh-tw.md) |
| 09 | 8 | [Message binary decode and pull results](09-phase-08-message-decode-pull-results.md) | [Message binary decode 與 pull result](09-phase-08-message-decode-pull-results.zh-tw.md) |
| 10 | 9 | [Pull consumer and offset stores](10-phase-09-pull-offset-store.md) | [Pull consumer 與 offset store](10-phase-09-pull-offset-store.zh-tw.md) |
| 11 | 10 | [Push consumer core pipeline](11-phase-10-push-consumer-pipeline.md) | [Push consumer 核心 pipeline](11-phase-10-push-consumer-pipeline.zh-tw.md) |
| 12 | 11 | [Rebalance and assignment lifecycle](12-phase-11-rebalance.md) | [Rebalance 與 assignment lifecycle](12-phase-11-rebalance.zh-tw.md) |
| 13 | 12 | [Orderly consumption and queue locks](13-phase-12-orderly-consumption.md) | [Orderly consumption 與 queue lock](13-phase-12-orderly-consumption.zh-tw.md) |
| 14 | 13 | [Lite Pull, assignment and POP APIs](14-phase-13-lite-assignment-pop.md) | [Lite Pull、assignment 與 POP API](14-phase-13-lite-assignment-pop.zh-tw.md) |
| 15 | 14 | [Transaction producer](15-phase-14-transaction-producer.md) | [Transaction producer](15-phase-14-transaction-producer.zh-tw.md) |
| 16 | 15 | [Request/reply and exactly-once completion](16-phase-15-request-reply.md) | [Request/reply 與單次完成語意](16-phase-15-request-reply.zh-tw.md) |
| 17 | 16 | [Namespace, hooks, security and operational features](17-phase-16-namespace-security-operations.md) | [Namespace、hook、安全與維運功能](17-phase-16-namespace-security-operations.zh-tw.md) |
| 18 | 17 | [Admin/query public surface](18-phase-17-admin-query.md) | [Admin/query public surface](18-phase-17-admin-query.zh-tw.md) |
| 19 | 18 | [Compatibility, performance and release gate](19-phase-18-compatibility-release-gate.md) | [相容性、效能與發布門檻](19-phase-18-compatibility-release-gate.zh-tw.md) |

## 3. Dependency clarifications required for sequential work

1. **Bootstrap:** Phase 0 creates only the Java oracle/fixtures/environment. Phase 1 creates common/protocol workspace crates; remoting arrives in Phase 2, core/integration tests in Phase 5, producer/facade in Phase 7 and consumer in Phase 8. Do not create empty future crates.
2. **No crate cycle:** HeaderMap, ProtocolError and header conversion traits live in protocol from Phase 2. Remoting uses them; Phase 4 headers never import remoting. Dependency direction remains common ← protocol ← remoting ← core ← producer/consumer ← facade.
3. **Models are not supported APIs:** Phase 4 models and Phase 5 lower-level Lite/POP calls do not imply a public consumer exists. Phase 13 classifies and completes the approved public surface.
4. **Message decode:** Phase 5 returns raw pull bytes; Phase 8 supplies the single bounded decoder. Do not duplicate parsing to unblock earlier RPC tests.
5. **Offset dependency:** Phase 9 implements search/min/max/earliest-time primitives needed by Phase 11 start-offset policies; Phase 17 only adds/reuses their approved public admin exposure.
6. **Push/rebalance gap:** Phase 10 tests the pipeline through a private assignment seam and test-only fixed assignment. Phase 11 supplies real startup/periodic/notification rebalance and closes the cumulative public Push gate. Do not ship a fake fixed-assignment production Push implementation.
7. **Orderly dependency:** Phase 11 prepares lock-aware lifecycle seams; Phase 12 activates Broker locking/renewal/expiry and orderly callbacks. Until then orderly compatibility is PARTIAL.
8. **Release labels are not phase gates:** Namespace is implemented in Phase 16 despite appearing in the master’s v0.1 matrix. A Phase 7 Producer milestone must not claim that entire release column is complete. TLS starts in Phase 3 and is finalized in Phase 16. All advertised release columns require their actual feature evidence.
9. **Optional scope:** Only the owner can approve Lite/POP exposure, trace, target auth topology and public admin exclusions. Keep undecided rows TODO; do not fabricate INTENTIONAL_DIFFERENCE approval.
10. **External environments:** Locally runnable Broker, TLS/auth and two-Broker checks remain in each feature phase. Only environment-dependent target-deployment acceptance is deferred to owner-executed Phase 18 P18-08.

## 4. Shared implementation and evidence rules

### 4.1 Read the exact Java source

Use the verified checkout/source artifacts recorded in `compat/java-reference/README.md`, never `develop`. Resolve short class references in each phase against:

- `common/src/main/java/org/apache/rocketmq/common/` for messages, flags, namespace and common values;
- `remoting/src/main/java/org/apache/rocketmq/remoting/` for framing, codes, headers and protocol bodies;
- `client/src/main/java/org/apache/rocketmq/client/` for APIs, runtime, Producer, Consumer and allocation behavior;
- additional auth/ACL/trace modules only as located in the verified 5.5.1 source tree.

Record the actual file, method and call site in the compatibility matrix before porting. Inspect both header validation and construction/response interpretation paths. Numbers, defaults, field order, requiredness and serialization limits come from this evidence, not guesses or unrelated SDKs. If a named class/path differs at the verified tag, record the actual mapping and stop for review if behavior is ambiguous.

### 4.2 Implement one reviewable task at a time

1. Read the phase’s prerequisites, contract and linked Java methods.
2. Add the named failing test or Java fixture; link it to the matrix before writing behavior.
3. Make the minimum change in the listed owning files, using standard Rust collections, installed dependencies and existing lower layers first. Add dependencies only when the phase actually uses them.
4. Run focused checks and common gates; keep all earlier suites passing.
5. Update `docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md`, `REQUEST_CODES.md`, `KNOWN_DIFFERENCES.md` where applicable and record commands/results in that phase’s implementation handoff/log. Do not invent an extra logging service or framework.
6. Commit reviewable work only under the implementation workflow agreed with the owner. This documentation task itself does not implement, commit, tag or publish software.

No production-input `unwrap`/`expect`, unchecked length allocation, sync guard across await, unowned task, credential logging or silent offset reset. Preserve unknown codes/ext fields. Every retry consumes one original monotonic deadline. Every stateful task/registration has identity-aware rollback, cancellation and join ownership. User callbacks run outside shared locks/readers. Document deliberate scope ceilings with a `ponytail:` code comment naming the upgrade trigger; this never excuses missing security, input validation or required compatibility.

Use built-in Rust tests and small table-driven fixtures; a new testing framework is not required. Reuse loopback peers for transport failure simulation. A trait is justified for actual multiple implementations (for example local/remote OffsetStore), not merely to wrap a single concrete service for tests.

### 4.3 Evidence and test commands

Phase-local commands are **to be implemented and run during coding**, not runnable in today’s documentation-only tree. Paths without a leading slash are repository-root-relative.

From Phase 1 onward, run these local gates after every completed phase:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
```

From Phase 0 onward, Java/fixture changes also run:

```bash
mvn -f compat/java-reference/pom.xml verify
python3 tests/differential/compare.py --self-test
```

Phase 0 establishes local profiles and deterministic setup. Phase 5 adds the integration package and differential runner. Before their Broker commands, start the required profile, configure `RMQ_NAMESRV_ADDR` and run its documented setup. The runner must read the scenario’s profile requirements, fail on missing prerequisites and isolate scenario topic/group state; it must not mutate an external cluster by accident.

```bash
docker compose -f integration-tests/compose.yaml up -d
bash integration-tests/setup.sh
export RMQ_NAMESRV_ADDR=127.0.0.1:9876
cargo test -p rocketmq-classic-integration --test broker --locked -- --ignored
bash tests/differential/run.sh --phase 5
```

Replace the phase number for later suites; `--all` is added for Phase 18. Specialized two-Broker/TLS/auth profiles require their own recorded addresses/configuration, not blindly the basic address above. Maven verify must validate Rust-produced protocol fixtures after Phase 2; a same-language round trip alone is insufficient. Golden metadata follows the master’s Appendix B with SHA-256/provenance. Do not normalize away protocol errors, ordering, retry counts, duplicate-delivery relationships or safe offset advancement.

## 5. Handoff and stop conditions

A phase handoff lists changed files, evidence/test IDs, exact commands and results, actual profile/version, remaining status rows and the next phase. Keep assertions executable and deterministic, including startup failure and terminal-owner race checks. One successful connection is not a compatibility gate.

Stop and request review when the verified baseline disagrees, a field width narrows, Java requiredness/behavior is ambiguous, a completed lower-layer contract must change, ownership cannot be safely transferred or differential results disagree without explanation. Add the failing fixture first; follow the master plan’s ADR rule for lower-layer contract changes rather than silently rewriting earlier phases. Deployment-only checks that have not run remain BLOCKED, never PASS.

## 6. Owner decisions to resolve before the affected phase

- **Phase 13:** Which Lite Pull/public POP APIs are v1.0 requirements, and which may be lower-level-only? See [Phase 13 section 7](14-phase-13-lite-assignment-pop.md#7-open-questions--decisions-required).
- **Phase 16:** Which 5.5.1 auth/ACL mode, CA/SNI/mTLS/proxy topology is required, and is message trace parity mandatory? See [Phase 16 section 7](17-phase-16-namespace-security-operations.md#7-open-questions--decisions-required).
- **Phase 17:** Which MQAdmin operations must be public in the first stable release? See [Phase 17 section 7](18-phase-17-admin-query.md#7-open-questions--decisions-required).

These decisions do not block reading or Phase 0–12 baseline work. They do block the dependent public API and final compatibility claim. No implementation or test success is asserted by the creation of these plans.
