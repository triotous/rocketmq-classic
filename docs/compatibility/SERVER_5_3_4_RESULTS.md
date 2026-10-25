# Apache RocketMQ Server 5.3.4 results

This report tracks Broker interoperability separately from the Java client
5.5.1 API and offline fixture audit. A passing row proves only its listed
scenario and profile.

## Pinned basic profile

- Official ASF image: [`apache/rocketmq:5.3.4` Docker Hub tag](https://hub.docker.com/layers/apache/rocketmq/5.3.4/images/sha256-a6976cd5841facfda294616a0b8797e7f41fa0292d51ad4e46b2ff224ce64995), tied to the Apache [5.3.4 release](https://rocketmq.apache.org/release-notes/2025/11/26/5.3.4/).
  Multi-platform index digest
  `sha256:d57ee4ebbd9d7bb6e185775bde41c128e98f185b25bf0b05c77d4e6c010572b8`.
- Image inspection and runtime: Ubuntu 24.04, Eclipse Temurin OpenJDK 8u472,
  `linux/arm64`; Docker Engine 29.8.2 and Compose 5.5.0 on the host. The pinned
  image ID matches the configured digest, and `mqadmin clusterList` reported
  server version `V5_3_4`.
- Observed host runtime: Docker Engine 29.8.2, Compose 5.5.0, `linux/aarch64`;
  `mqadmin clusterList` reported server version `V5_3_4`.
- NameServer and Broker health checks passed. Broker `broker-a` advertised
  `broker:10911` on the isolated Compose network.
- The scenario topic is created explicitly with four read/write queues;
  Broker auto-creation remains disabled.

## Executed scenarios

| Scenario | Result | Evidence |
| --- | --- | --- |
| `ROUTE-001-get-existing-topic` | PASS — Java 5.5.1 and Rust returned the same four route rows for `compat_phase0_route`, broker `broker-a`, queue IDs 0–3. The owned topic was deleted and the Compose project, network, and volume were removed. | [`20261017T052003Z`](evidence/phase-19/ROUTE-001-get-existing-topic/20261017T052003Z/) |
| `ROUTE-002-topic-not-exist` | PASS — Java 5.5.1 and Rust both returned `NO_ROUTE` for `compat_phase0_missing`. The isolated Compose project was removed after the run. | [`20261017T051903Z`](evidence/phase-19/ROUTE-002-topic-not-exist/20261017T051903Z/) |
| `HEARTBEAT-001` | PASS — Java 5.5.1 and Rust sent the same encoded producer heartbeat and both received Broker response version `481`. The isolated Compose project was removed after the run. | [`20261017T053144Z`](evidence/phase-19/HEARTBEAT-001/20261017T053144Z/) |
| `PROD-001-sync-send` | PASS — Java and Rust both received `SEND_OK`; Rust read the Java-produced message and Java read the Rust-produced message. Both readbacks matched topic `compat_phase0_send`, queue 0, returned queue offsets, and body `fixed`. The owned topic and isolated Compose project were removed after the run. | [`20261017T054628Z`](evidence/phase-19/PROD-001-sync-send/20261017T054628Z/) |
| `PROD-002-async-send` | PASS — Java's async callback fired once and Rust's Future resolved once; both returned `SEND_OK`. Each client read the other client's message and matched topic `compat_phase0_send`, queue 0, the returned queue offset, and body `fixed`. The owned topic and isolated Compose project were removed after the run. | [`20261017T055737Z`](evidence/phase-19/PROD-002-async-send/20261017T055737Z/) |
| `PROD-003-oneway-send` | PASS — Java and Rust oneway calls returned without a Broker response. Broker queue max offsets advanced from 0 to 1 and then 2; Rust read the Java message at queue offset 0, and Java read the Rust message at offset 1. The owned topic and isolated Compose project, network, and volume were removed after the run. | [`20261017T063258Z`](evidence/phase-19/PROD-003-oneway-send/20261017T063258Z/) |
| `PROD-004-batch-send` | PASS — Java and Rust both returned `SEND_OK` for two messages. Rust read both Java messages by their physical offset IDs at queue offsets 0 and 1; Java read both Rust messages at offsets 2 and 3. Bodies matched `batch-0` and `batch-1`. The owned topic and isolated Compose project, network, and volume were removed after the run. | [`20261017T065458Z`](evidence/phase-19/PROD-004-batch-send/20261017T065458Z/) |

The first batch attempt at source revision `2f9396e3` sent successfully from
Java but failed in the Rust probe because it read only the first returned
physical offset ID and treated the stored message body as a compact batch. The
diagnostic run is retained at
[`20261017T064439Z`](evidence/phase-19/PROD-004-batch-send/20261017T064439Z/);
the runner was corrected to read every returned ID before the passing run above.

The two route runs used source commit `91ecd510`; the heartbeat run used
`1a77ac68`; the sync-send run used `95e38101`; the async-send run used
`6f2a0c12`; the oneway-send run used `48ed3f58`; the batch-send run used
`8262d722`. Evidence metadata records the
full revision, toolchains, Docker platform, Compose/configuration hashes,
commands, duration, client outputs, and cleanup results. Local home/repository
paths and the host name are redacted from archived tool output. Current
results cover route lookup, heartbeat, synchronous/asynchronous/oneway/batch
sends with cross-language readback, consumer offsets and pulls, queue lock
contention, average assignment over two live Brokers, and the listed
transaction commit/rollback visibility cases. They do not complete
request/reply, retry, TLS, ACL, failover, or stress coverage.

## Pending local server coverage

The scenario manifest contains 37 live cases. The seven rows above, four
consumer rows, the P5 route row, `ORDER-001-lock-unlock`,
`REBAL-001-average-10q-3c`, and the two transaction rows are implemented and
passed by the current runner; 21 remain pending. The two-Broker profile covers
lock/unlock and average queue assignment; retry, query, and broader
interoperability coverage, TLS, ACL, and feature-specific profiles remain
incomplete. No server limitation is inferred from an unrun case. Phase 19
acceptance remains open until each locally reproducible case has Java/Rust
evidence or a source-backed matched server limitation plus the required
success-path evidence.

## Current-candidate rerun — 2026-10-25

The seven implemented cases were rerun against the pinned image on candidate
revision `553564a21e4d2a40148c60637f4817c7d6f26b74`, using fresh isolated
Compose projects, recreated topics, Java 5.5.1 Maven probes, and Rust 1.85.0
probes. All seven passed and their new evidence replaced the manifest pointers:

| Scenario | Result | Evidence |
| --- | --- | --- |
| `ROUTE-001-get-existing-topic` | PASS — four matching route rows | [`20261025T021228Z`](evidence/phase-19/ROUTE-001-get-existing-topic/20261025T021228Z/) |
| `ROUTE-002-topic-not-exist` | PASS — both clients returned no route | [`20261025T021336Z`](evidence/phase-19/ROUTE-002-topic-not-exist/20261025T021336Z/) |
| `HEARTBEAT-001` | PASS — encoded heartbeat and response version match | [`20261025T021430Z`](evidence/phase-19/HEARTBEAT-001/20261025T021430Z/) |
| `PROD-001-sync-send` | PASS — send results and bidirectional readback match | [`20261025T021526Z`](evidence/phase-19/PROD-001-sync-send/20261025T021526Z/) |
| `PROD-002-async-send` | PASS — single completion and bidirectional readback match | [`20261025T021643Z`](evidence/phase-19/PROD-002-async-send/20261025T021643Z/) |
| `PROD-003-oneway-send` | PASS — no-response sends reached queues and cross-readback matched | [`20261025T021802Z`](evidence/phase-19/PROD-003-oneway-send/20261025T021802Z/) |
| `PROD-004-batch-send` | PASS — both messages from each client were readable by the other | [`20261025T021932Z`](evidence/phase-19/PROD-004-batch-send/20261025T021932Z/) |

The evidence records the image digest, runtime platform, server startup version,
toolchains, commands, and project cleanup. The remaining 30 scenario IDs have
no live runner, and no two-Broker, TLS, ACL, or feature-specific profile was
executed. These seven results do not establish complete client parity or final
release acceptance.

## Current consumer probes — 2026-10-25

Four basic-profile consumer cases ran on source revision
`cb351d8ae6a67b9bdb4cbf9fdacd497602e0b9a4`. Each run used a fresh Compose
project and reports `source_code_dirty=false`; topic and profile cleanup
completed successfully.

| Scenario | Result | Evidence |
| --- | --- | --- |
| `CONS-001-query-offset` | PASS — Rust read Java offset 17; Java read Rust offset 23 | [`20261025T025233Z`](evidence/phase-19/CONS-001-query-offset/20261025T025233Z/) |
| `CONS-002-pull-found` | PASS — both clients returned `FOUND`, offsets 0–1, and the same `pull-seed` response body | [`20261025T031409Z`](evidence/phase-19/CONS-002-pull-found/20261025T031409Z/) |
| `CONS-003-pull-no-new-msg` | PASS — both clients returned `NO_NEW_MSG` with empty response bodies | [`20261025T031529Z`](evidence/phase-19/CONS-003-pull-no-new-msg/20261025T031529Z/) |
| `CONS-004-update-offset` | PASS — Rust read Java offset 31; Java read Rust offset 47 | [`20261025T031646Z`](evidence/phase-19/CONS-004-update-offset/20261025T031646Z/) |

One clean `CONS-004` attempt timed out on the Java update RPC and was cleaned
up without a pass; a later fresh project passed. An early `CONS-002` attempt
also exposed that direct pulls need the Java `PullSysFlag` subscription bit;
the probe now uses the same flag in Java and Rust. Neither diagnostic run is
used as a passing manifest result. The 26 remaining live cases still prevent
full Phase 19 acceptance.

## P5 existing and missing topic routes — 2026-10-25

`P5-V02-route` passed on committed revision
`6300029907b78a03cb1276a434393972330bdf0d` with
`source_code_dirty=false`. Java and Rust matched four route rows for
`compat_p5_route` and both returned `NO_ROUTE` for `compat_p5_missing`. The
scenario-owned topic and Compose project were cleaned after evidence capture:
[`20261025T081906Z`](evidence/phase-19/P5-V02-route/20261025T081906Z/).
At that checkpoint, 25 cases remained and prevented full Phase 19 acceptance.

## Two-Broker lock and unlock — 2026-10-25

The `two-broker` profile starts separate `broker-a` and `broker-b` instances
from the pinned 5.3.4 image and waits for both to register with the NameServer.
`ORDER-001-lock-unlock` passed on committed revision
`6684ff8c22c92fdb0fc3af8f97574fd3e34c9a82` with
`source_code_dirty=false`. Java and Rust both acquired the same unlocked
`broker-a` queue; Rust was denied while Java held it, then acquired it after
Java's synchronous unlock. Rust's unlock allowed Java to acquire the queue
again. The scenario topic, group, project, network, and volumes were cleaned:
[`20261025T083111Z`](evidence/phase-19/ORDER-001-lock-unlock/20261025T083111Z/).

This validates two-Broker profile startup and the listed lock behavior; it is
not replicated HA or failover evidence. Live coverage at that checkpoint was
13/37, with 24 cases pending.

## Two-Broker average queue assignment — 2026-10-25

`REBAL-001-average-10q-3c` passed on committed revision
`b347d11c5e33c18d3e443c852bc8680e83892c2f` with
`source_code_dirty=false`. Java 5.5.1 and Rust fetched the same live route of
five queues from each Broker, then produced identical assignments for three
sorted consumer IDs: 4, 3, and 3 queues. The assertion checked that all ten
unique queue IDs across `broker-a` and `broker-b` were covered. The topic,
Compose project, containers, network, and volumes were removed after capture:
[`20261025T090131Z`](evidence/phase-19/REBAL-001-average-10q-3c/20261025T090131Z/).

An earlier rehearsal at `20261025T085632Z` was a false positive: the runner
provisioned and cleaned up but did not execute either probe. Its manifest PASS
pointer was removed and the directory is retained with a diagnostic note. A
corrected dirty-source replay at `20261025T085935Z` passed the assertions but
is diagnostic-only; the clean run above is authoritative. Live coverage is
At that checkpoint, live coverage was 14/37, with 23 cases pending.

## Transaction commit and rollback visibility — 2026-10-25

`TX-001-commit` and `TX-002-rollback` passed on committed revision
`1cc67ce358d4f191b20dc33e71b59a175a57eb59`; both runs record
`source_code_dirty=false`. A dedicated transaction profile advertises its
Broker at `127.0.0.1:30911` so Java 5.5.1 and Rust can reach the same isolated
Broker from the host. Each run creates an owned topic and consumer group.

For TX-001, both transactional producers returned `COMMIT_MESSAGE`; Java and
Rust pulls matched byte-for-byte, returned `FOUND` with offsets 0–2, and
contained both `transaction-body` messages. The Broker queue max offset was
2. For TX-002, both producers returned `ROLLBACK_MESSAGE`; matching pulls
returned `NO_NEW_MSG` with empty bodies and all queue offsets at 0. Both topics
and Compose projects, networks, and volumes were removed after evidence capture:
[`TX-001`](evidence/phase-19/TX-001-commit/20261025T091920Z/) and
[`TX-002`](evidence/phase-19/TX-002-rollback/20261025T092046Z/).

An earlier transaction replay at `20261025T091320Z` and `20261025T091433Z`
checked callbacks and queue offsets but not consumer visibility; the later
clean-source runs above include the stronger pull assertions and are the
manifest evidence. Live coverage is now 16/37, with 21 cases pending. The last
full local gate predates the transaction probes.
