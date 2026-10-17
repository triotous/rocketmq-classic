# Apache RocketMQ Server 5.3.4 results

This report tracks Broker interoperability separately from the Java client
5.5.1 API and offline fixture audit. A passing row proves only its listed
scenario and profile.

## Pinned basic profile

- Image: `apache/rocketmq:5.3.4`, multi-platform index digest
  `sha256:d57ee4ebbd9d7bb6e185775bde41c128e98f185b25bf0b05c77d4e6c010572b8`.
- Observed runtime: Docker Engine 29.8.2, Compose 5.5.0, `linux/aarch64`;
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
paths and the host name are redacted from archived tool output. The current
results cover route lookup, one heartbeat, synchronous, asynchronous, oneway,
and batch sends with cross-language message readback. They do not exercise
Broker-side consumer operations, TLS, ACL, transactions, retries, failover, or
stress behavior.

## Pending local server coverage

The scenario manifest contains 37 live cases. The seven cases above are the
only ones implemented and passed by the current runner; 30 remain pending.
The two-Broker, TLS, ACL, and feature-specific profiles and full local matrix
are not complete. No server limitation is inferred from an unrun case. Phase 19
acceptance remains open until each locally reproducible case has Java/Rust
evidence or a source-backed matched server limitation plus the required
success-path evidence.
