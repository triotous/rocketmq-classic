# Local RocketMQ integration environment

The `basic` profile provisions the pinned Apache RocketMQ 5.3.4 multi-platform
image. The image index digest is
`sha256:d57ee4ebbd9d7bb6e185775bde41c128e98f185b25bf0b05c77d4e6c010572b8`.
The Broker advertises the Compose service name `broker`, so clients must run on
the same Compose network. Host port mappings bind only to `127.0.0.1` and are
for diagnostics; they do not make the advertised Broker address host-routable.

Provisioning requires an explicit disposable-test marker and profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
bash integration-tests/setup.sh
```

The setup waits for NameServer and Broker health checks and then polls
`mqadmin clusterList` until `broker-a` is registered. Broker topic and
subscription-group auto-creation are disabled so scenarios must create their
own resources. Data is stored in a Compose-project-scoped volume.

Tear down only the matching local test project and its volume:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
bash integration-tests/cleanup.sh
```

`COMPOSE_PROJECT_NAME` may be set to isolate concurrent runs; it must start
with `rocketmq-classic-p19-`. Run one of the implemented live cases:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario ROUTE-001-get-existing-topic
```

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario ROUTE-002-topic-not-exist
```

The TLS lifecycle handoff uses the isolated TLS profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=tls \
tests/differential/run.sh --scenario P3-V04
```

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario HEARTBEAT-001
```

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario PROD-001-sync-send
```

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario PROD-002-async-send
```

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario PROD-003-oneway-send
```

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario PROD-004-batch-send
```

The runner creates a unique Compose project, provisions a manifest-owned
topic when required, then compares Java 5.5.1 and Rust route outcomes,
heartbeat bodies and Broker response versions, or synchronous/asynchronous/
oneway/batch send results, async completion counts, Broker queue offsets,
consumer-offset writes and cross-client queries, pull status/offsets/message
bodies, batch message bodies read by every returned physical offset ID,
bidirectional cross-language Broker readback, queue-lock acquisition,
contention and unlock release, live average queue assignment, and transaction
commit/rollback visibility through Java and Rust consumer pulls. `P6-V04`
checks live subscription changes, Broker-side tag filtering, producer
unregister, and continued use by the remaining producer. `P9-V03` covers
explicit pull statuses, suggested-broker selection, offset-boundary RPCs, and
resuming from a broker-persisted offset after a consumer restart.
`REQ-001-request-reply-success` checks Java request/Rust reply and Rust
request/Java reply on separate scenario-owned topics, including the correlated
reply fields and each responder's observed request.
`P3-V04` invokes a raw route on the local NameServer, then sends Java and Rust
RocketMQ requests through a locally generated TLS terminator that forwards to
the disposable plain Broker. Java 5.5.1 verifies the CA chain but accepts a
trusted wrong-host certificate; Rust verifies the host and rejects it. Both
clients reject an untrusted CA and report zero retained remoting resources at
shutdown.
`PROD-005-send-timeout` delays each direct Broker response beyond the reviewed
100 ms deadline and verifies the timed-out sends were each committed once by
cross-language pull readback.
`P5-V02-producer` compares raw producer V1, V2, batch, reply V1/V2, and
end-transaction calls on the pinned Broker.
`P5-V02-consumer` compares raw pull and Lite pull, offset query/update,
consumer-list, send-back, and queue lock/unlock calls on the two-Broker profile.
It deletes any owned topic and tears down only that project. Toolchain,
configuration, output, and cleanup evidence is stored under
`docs/compatibility/evidence/phase-19/`; a manifest row becomes `PASS` only
after comparison and cleanup both succeed. The runner implements 25 of 37
cases and all 25 have passed: the two Phase 0 route cases, `P0-V03`, `P3-V04`,
`P5-V02-route`, `P5-V02-producer`, `P5-V02-consumer`, `P6-V04`, `P7-V02`, `P9-V03`,
`ORDER-001-lock-unlock`, `REBAL-001-average-10q-3c`, `HEARTBEAT-001`,
`PROD-001-sync-send` through `PROD-004-batch-send`, `PROD-005-send-timeout`, and
`CONS-001-query-offset` through `CONS-004-update-offset`, plus
`TX-001-commit`, `TX-002-rollback`, and `REQ-001-request-reply-success`.
`--phase N` and `--all` fail closed when any selected case has no runner.

The two-Broker lock case can be replayed with:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=two-broker \
tests/differential/run.sh --scenario ORDER-001-lock-unlock
```

The two-Broker average rebalance scenario can be replayed with:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=two-broker \
tests/differential/run.sh --scenario REBAL-001-average-10q-3c
```

The local transaction visibility cases use the host-routed `transaction`
profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=transaction \
tests/differential/run.sh --scenario TX-001-commit
```

Run the rollback case by replacing the scenario ID with `TX-002-rollback`.
The checks compare Java/Rust transaction results and pulls; commit requires two
visible messages and rollback requires an empty queue.

The high-level producer overload matrix uses the host-routed profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=host-routed \
tests/differential/run.sh --scenario P7-V02
```

It checks sync, async, oneway, explicit-queue, selector, valid-batch, and
mixed-topic batch rejection behavior against the same isolated Broker.

The shared-runtime lifecycle check also uses the host-routed profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=host-routed \
tests/differential/run.sh --scenario P6-V04
```

It verifies that changing the consumer filter takes effect at the Broker and
that producer B remains functional after producer A unregisters.

The explicit pull and offset-resume checks use the same host-routed profile:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=host-routed \
tests/differential/run.sh --scenario P9-V03
```

They compare all four pull statuses, suggested broker ID, min/max/search
offsets, and the next offset after a new consumer instance starts.

Live runs require the pinned runner toolchains: Maven on JDK 27 and Rust 1.85.0,
matching the repository CI configuration.

The full local entry point is:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
bash integration-tests/run-local.sh --all
```

It records and runs all server-free gates first. It then checks every Phase 19
manifest row before starting any Docker resource. Missing scenario runners
produce a `BLOCKED` result with the missing IDs in the archived log; the full
matrix cannot report success until all IDs have passing evidence. Currently
25 of 37 live cases have runners and passed; 12 remain without runners:
`P5-V02-extensions`, `P7-V03`, `P7-V04`, `P10-V03`, `P11-V03`, `P11-V04`,
`P12-V03`, `P13-V02`, `P14-V03`, `P15-V03`, `P16-V02`, and `P17-V02`.
The remaining two-Broker cases, ACL/feature profiles, and a fresh
`run-local.sh --all` pass remain pending. The last full local run predates
several later runner additions and must be rerun against the current revision.
Starting a profile by itself does not mark a scenario passed.
