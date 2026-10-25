# Owner validation handoff

Status date: 2026-10-25. The Phase 19 local candidate is
`d673e6ff270d696a76c5cebb40d6349b32d5bfb7`; this document is a handoff, not
owner approval or release evidence. The owner is the RocketMQ service owner
plus the relevant network/security/operations owner; names and authorized
resources have not been supplied.

Production writes, credentials, certificate enrollment, fault injection, and
load tests require owner-provided disposable resources and explicit
authorization. Do not point the Docker test runner at a production NameServer.
Synthetic TLS/authentication, ordinary restart/recovery, mixed-client, and
bounded stress scenarios remain automatic Phase 19 work and cannot be waived
through this checklist.

| ID | Status | Owner input and topology | Command and expected result | Cleanup and evidence |
| --- | --- | --- | --- | --- |
| MAN-01 | BLOCKED | Service/network owner must provide an approved non-production endpoint that reproduces production DNS/VIP/proxy/firewall paths, plus an existing read-only test topic and the host/network from which to run the checks. | On that approved endpoint only, set `NAMESRV_ADDR` and `EXISTING_TEST_TOPIC`, then run the Java route probe and Rust route probe shown below. Their sorted route rows must match. The probes do not create a topic or send a message; both clients start and shut down a producer identity. | No topic cleanup is needed. Confirm producer registration has expired/been unregistered per the owner’s environment. Save redacted stdout, endpoint class (no secrets), host/network, date, and revision under `docs/compatibility/evidence/owner/MAN-01/`. |
| MAN-02 | BLOCKED | Security owner must provide synthetic organization-issued CA/SNI/mTLS material, short-lived test identities, ACL policy, and disposable topic/group names. Production credentials must not be copied here. | `P3-V04` covers client TLS against a local TLS-terminating proxy only; no live Broker TLS/ACL profile or owner-issued certificate/ACL probe is registered. Keep this work blocked until those probes and the approved disposable profile exist, then run locally generated and owner-issued material separately. Expected allowed and denied outcomes must match Java 5.5.1. | Revoke test identities, remove only owner-approved disposable ACL/topic/group resources, and archive redacted command/output plus certificate fingerprints (never keys) under `docs/compatibility/evidence/owner/MAN-02/`. |
| MAN-03 | BLOCKED | Service/operations owner must provide the approved HA topology, maintenance window, fault-injection procedure, and rollback contact. This requires production-specific multi-zone or network-partition behavior. | No executable failover scenario is registered yet. Only after written authorization and a disposable replica of the topology exists, run the future manifest scenario command for the approved failure and recovery sequence. Expected routing, delivery/duplication, offset recovery, and service restoration must be compared with Java 5.5.1. | Restore the topology using the approved rollback procedure. Archive the authorization reference, topology/version, exact scenario command, timestamps, Java/Rust results, recovery measurements, and cleanup confirmation under `docs/compatibility/evidence/owner/MAN-03/`. |
| MAN-04 | BLOCKED | Service/SRE owner must provide representative message sizes, concurrency, throughput shape, duration, resource quotas, success SLOs, and approved disposable capacity. | No Broker-backed sustained-load or capacity scenario is registered yet. The offline microbenchmark `cargo bench -p rocketmq-classic --bench offline --locked` is not a substitute. After the local stress runner is implemented, run its fixed-seed bounded scenario first on the disposable production-like topology; run the separately approved production soak only with owner authorization. | Stop load generators, verify owned clients/tasks/sockets have returned to baseline, and remove only approved test resources. Save redacted workload parameters, raw/normalized results, resource telemetry, command, revision, and cleanup evidence under `docs/compatibility/evidence/owner/MAN-04/`. |
| MAN-05 | BLOCKED | Compatibility owner must finish the source-backed feature audit and identify any behavior requiring infrastructure unavailable in this environment. No such disposition has been established yet because 12 live scenarios remain unimplemented. | No case-specific command can be selected until a concrete feature and its evaluated local/supplementary alternatives are recorded. Once selected, run the exact manifest command against the owner-approved disposable topology and compare Java 5.5.1 and Rust results. | Archive the source evidence, alternatives evaluated, topology/version, command, expected and observed outcomes, cleanup, owner, date, and revision under `docs/compatibility/evidence/owner/MAN-05/`. If every required behavior proves locally runnable, replace BLOCKED with N/A and link that evidence. |

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

All five manual rows are `BLOCKED`. MAN-01 through MAN-04 need owner-supplied
deployment inputs and/or unimplemented live probes. MAN-05 cannot be classified
as N/A until the local feature matrix and source audit are complete. Required
blocked items prevent final compatibility and release approval.
