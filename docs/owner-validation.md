# Owner validation handoff

Status date: 2026-10-27. The Phase 19 production code candidate is
`413f735ad0ab41717acb9d76e2cb664b42076f4d`. The current local aggregate and
manifest refresh are recorded under
`docs/compatibility/evidence/phase-19/run-local/20261026T190920Z/`. This
document is a handoff, not owner approval or release evidence. The owner is the
RocketMQ service owner plus the relevant network/security/operations owner;
names and authorized resources have not been supplied.

Production writes, credentials, certificate enrollment, fault injection, and
load tests require owner-provided disposable resources and explicit
authorization. Do not point the Docker test runner at a production NameServer.
Synthetic TLS/authentication, restart/recovery, mixed-client, and bounded
request-reply scenarios have local Phase 19 evidence in
[SERVER_5_3_4_RESULTS.md](compatibility/SERVER_5_3_4_RESULTS.md). They do not
replace the owner-specific network, security, HA, or workload checks below.

| ID | Status | Owner input and topology | Command and expected result | Cleanup and evidence |
| --- | --- | --- | --- | --- |
| MAN-01 | BLOCKED | Service/network owner must provide an approved non-production endpoint that reproduces production DNS/VIP/proxy/firewall paths, plus an existing read-only test topic and the host/network from which to run the checks. | On that approved endpoint only, set `NAMESRV_ADDR` and `EXISTING_TEST_TOPIC`, then run the Java route probe and Rust route probe shown below. Their sorted route rows must match. The probes do not create a topic or send a message; both clients start and shut down a producer identity. | No topic cleanup is needed. Confirm producer registration has expired/been unregistered per the owner’s environment. Save redacted stdout, endpoint class (no secrets), host/network, date, and revision under `docs/compatibility/evidence/owner/MAN-01/`. |
| MAN-02 | BLOCKED | Security owner must provide organization-issued CA/SNI/mTLS material, short-lived test identities, ACL policy, and disposable topic/group names. Production credentials must not be copied here. | Local `P16-V02` passed synthetic ACL and Broker-side mTLS checks on an isolated Apache 5.3.4 profile; `P3-V04` covers the separate TLS-terminating proxy path. Neither establishes the owner issuer, SNI profile, ACL policy, or endpoint behavior. Run the owner-issued profile only after the approved disposable resources are supplied; expected allowed and denied outcomes must match Java 5.5.1. | Revoke test identities, remove only owner-approved disposable ACL/topic/group resources, and archive redacted command/output plus certificate fingerprints (never keys) under `docs/compatibility/evidence/owner/MAN-02/`. |
| MAN-03 | BLOCKED | Service/operations owner must provide the approved HA topology, maintenance window, fault-injection procedure, and rollback contact. This requires production-specific multi-zone or network-partition behavior. | No executable failover scenario is registered yet. Only after written authorization and a disposable replica of the topology exists, run the future manifest scenario command for the approved failure and recovery sequence. Expected routing, delivery/duplication, offset recovery, and service restoration must be compared with Java 5.5.1. | Restore the topology using the approved rollback procedure. Archive the authorization reference, topology/version, exact scenario command, timestamps, Java/Rust results, recovery measurements, and cleanup confirmation under `docs/compatibility/evidence/owner/MAN-03/`. |
| MAN-04 | BLOCKED | Service/SRE owner must provide representative message sizes, concurrency, throughput shape, duration, resource quotas, success SLOs, and approved disposable capacity. | The local Broker-backed P7-V04 fixed-seed scenario now passes: Java and Rust each sent 8,192 variable-size messages with at most 32 concurrent sends, matching byte totals/checksums and contiguous offsets under a 120-second bound; both clients returned to zero tracked resources after three additional shutdown cycles. This bounded local case does not establish representative production capacity or SLOs. Run an owner-approved production-like soak only after its workload parameters and disposable capacity are supplied. | Stop load generators, verify owned clients/tasks/sockets have returned to baseline, and remove only approved test resources. Save redacted workload parameters, raw/normalized results, resource telemetry, command, revision, and cleanup evidence under `docs/compatibility/evidence/owner/MAN-04/`. |
| MAN-05 | N/A | The source-backed client feature/API audit and current Phase 19 local matrix are complete. No additional feature-specific scenario requires an owner-only service. MAN-01 through MAN-04 separately track owner-network, security, HA, and workload validation. | The pinned Java 5.5.1 source/API matrices and all 37 Phase 19 local scenarios were reviewed. The `run-local.sh --all` replay on `413f735a` passed all 23 gates; reconciliation verified all 37 scenarios have current clean-source PASS evidence and exit status 0. | Audit: [MAN-05 current-tree evidence](compatibility/evidence/owner/MAN-05/current-tree-audit-20261026.md); [current aggregate evidence](compatibility/evidence/phase-19/run-local/20261026T190920Z/). This N/A is limited to unrepresented feature-specific validation and is not owner production approval. |

## MAN-01 route commands

Run these commands only after the service/network owner has approved the
non-production endpoint and topic. Set both values to that environment:

```bash
export NAMESRV_ADDR='approved-non-production-nameserver:9876'
export EXISTING_TEST_TOPIC='owner-approved-existing-topic'
mvn -q -f compat/java-reference/pom.xml \
  -Dexec.args="$NAMESRV_ADDR $EXISTING_TEST_TOPIC" \
  exec:java@live-route-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
  route "$NAMESRV_ADDR" "$EXISTING_TEST_TOPIC"
```

Compare the Java and Rust `QUEUE`/`EMPTY_ROUTE` rows. This command sequence is
read-only with respect to topics and messages, but it starts producer clients;
the owner must permit those producer registrations on the approved test
endpoint. Captured outputs must omit private hostnames and credentials where
needed. These commands have not been executed against any owner environment.

## Current disposition

MAN-01 through MAN-04 remain BLOCKED pending the owner inputs listed above:
approved non-production network paths, organization-issued security material,
authorized HA fault topology, and representative workload/SLO capacity.

MAN-05 is N/A because the source-backed feature audit found no additional
feature-specific behavior that needs an owner-only service, and all 37
registered local scenarios have passing evidence. The current local aggregate on `413f735a` passed all 23 gates, with all 37
scenarios reconciled to current clean-source PASS evidence and exit status 0.
The current-tree audit is
[docs/compatibility/evidence/owner/MAN-05/current-tree-audit-20261026.md](compatibility/evidence/owner/MAN-05/current-tree-audit-20261026.md).
GitHub Actions CI was removed per the user's 2026-10-27 direction and is not a
release gate. Historical run `40383732405`, which started no job steps because
of an account payment/spending-limit block, remains archived in the Phase 19
evidence. This does not approve production use or substitute for MAN-01
through MAN-04.
