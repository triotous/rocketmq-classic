# Phase 19 owner validation handoff

Status: owner-environment checks are **BLOCKED pending environment inputs**.
No production endpoint, identity, resource, workload, or maintenance procedure
has been supplied, so no owner-environment test has run. Local synthetic TLS,
auth, restart, and failover scenarios remain automatic Phase 19 work and are
not waived by this handoff.

The release owner must provide the named owner, approved disposable resources,
topology/version, access method, cleanup procedure, and evidence destination
before an item is scheduled. Do not use production topics/groups or perform
fault injection without explicit resource and maintenance authorization.

## Items

| ID | Status / owner | Required input | Command and expected result | Cleanup / evidence |
| --- | --- | --- | --- | --- |
| MAN-01 | BLOCKED; owner TBD | Approved NameServer address, DNS/VIP/proxy/firewall path, and an existing read-only route topic. | On the approved host, run `mqadmin clusterList -n "$ROCKETMQ_NAMESRV_ADDR"`; then compare Java and Rust route output with the read-only commands below. Expect the same route rows and record advertised endpoints. This does not by itself prove Broker reachability. | No server mutation. Save sanitized output and network-path details under the owner-approved evidence location. |
| MAN-02 | BLOCKED; owner TBD | Organization CA/SNI or mTLS material, a provisioned test identity, and explicitly authorized allowed/denied disposable resources. | No command is approved yet. Finalize Java/Rust TLS and ACL commands after the organization supplies the connection properties and ACL resource names. Expect Java and Rust to agree on allowed and denied operations, with valid certificates accepted and invalid identity/CA cases rejected. | Revoke test credentials and remove only approved disposable ACL resources; preserve redacted outcomes. |
| MAN-03 | BLOCKED; owner TBD | Production HA topology, approved maintenance window, and the owner’s fault-injection/recovery procedure. | No fault command is approved yet. Use only the owner-supplied procedure after maintenance approval. Expect routing, retry, recovery, and shutdown outcomes to match the Java 5.5.1 client against the production 5.3.4 topology. | Restore the topology using the owner runbook and capture timestamps, route changes, and recovery evidence. |
| MAN-04 | BLOCKED; owner TBD | Representative workload shape, capacity/SLO targets, resource limits, soak duration, and approved environment. | No workload command can be fixed until those parameters are supplied. Run the agreed bounded workload and restart/shutdown procedure against the approved environment; compare delivery, offsets, resource use, and recovery with Java. | Stop the workload, remove only test resources, and retain redacted metrics and logs. |
| MAN-05 | Pending classification; owner TBD | A specific source-confirmed Java 5.5.1 feature and proof that no local or supplementary unmodified server profile can exercise it. | No item-specific command exists until the Phase 19 local matrix identifies such a feature. If identified, record the exact Java/Rust command and expected matched outcomes here before owner execution. | Record why local profiles cannot reproduce it, the owner-approved cleanup, and evidence. Do not mark N/A because a case is merely unrun. |

MAN-01 route query command form, once the owner supplies a disposable existing
topic and endpoint:

```bash
export ROCKETMQ_NAMESRV_ADDR='<approved-name-server-address>'
export ROCKETMQ_OWNER_ROUTE_TOPIC='<approved-existing-read-only-topic>'
mvn -q -f compat/java-reference/pom.xml \
  -Dexec.args="$ROCKETMQ_NAMESRV_ADDR $ROCKETMQ_OWNER_ROUTE_TOPIC" \
  exec:java@live-route-probe
cargo run --locked --quiet -p rocketmq-classic-integration \
  --bin live_client -- route \
  "$ROCKETMQ_NAMESRV_ADDR" "$ROCKETMQ_OWNER_ROUTE_TOPIC"
```

These probes query NameServer route data and do not create topics or send
messages. They do not, by themselves, validate Broker reachability or the
remaining MAN-01 network paths. Replace the placeholders only with owner-
approved values; do not save credentials in this document.
