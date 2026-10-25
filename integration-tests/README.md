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
bidirectional cross-language Broker readback, and queue-lock acquisition,
contention, and unlock release.
It deletes any owned topic and tears down only that project. Toolchain,
configuration, output, and cleanup evidence is stored under
`docs/compatibility/evidence/phase-19/`; a manifest row becomes `PASS` only
after comparison and cleanup both succeed. The runner implements 13 of 37
cases and all 13 have passed: the two Phase 0 route cases, `P5-V02-route`,
`ORDER-001-lock-unlock`, `HEARTBEAT-001`,
`PROD-001-sync-send` through `PROD-004-batch-send`, and
`CONS-001-query-offset` through `CONS-004-update-offset`.
`--phase N` and `--all` fail closed when any selected case has no runner.

The two-Broker lock case can be replayed with:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=two-broker \
tests/differential/run.sh --scenario ORDER-001-lock-unlock
```

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
13 of 37 live cases have runners; the other 24, two-Broker coverage beyond
ORDER-001, TLS/ACL/feature profiles, and the full `run-local.sh --all` pass
remain pending. Starting a profile by itself does not mark a scenario passed.
