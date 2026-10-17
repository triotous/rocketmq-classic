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
| `ROUTE-001-get-existing-topic` | PASS — Java 5.5.1 and Rust returned the same four route rows for `compat_phase0_route`, broker `broker-a`, queue IDs 0–3. The owned topic was deleted and the Compose project, network, and volume were removed. | [`20261017T051047Z`](evidence/phase-19/ROUTE-001-get-existing-topic/20261017T051047Z/) |

The run used source commit `adae9d1f`; the evidence metadata records the full
revision, toolchains, Docker platform, Compose/configuration hashes, commands,
duration, client outputs, and cleanup results. Local home/repository paths and
the host name are redacted from archived tool output. This is route lookup evidence;
it does not exercise message send/receive, Broker-side consumer operations,
TLS, ACL, transactions, retries, failover, or stress behavior.

## Pending local server coverage

The scenario manifest contains 37 live cases. The route case above is the only
one implemented and passed by the current runner; 36 remain pending. The
two-Broker, TLS, ACL, and feature-specific profiles and full local matrix are
not complete. No server limitation is inferred from an unrun case. Phase 19
acceptance remains open until each locally reproducible case has Java/Rust
evidence or a source-backed matched server limitation plus the required
success-path evidence.
