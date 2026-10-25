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
batch message bodies read by every returned physical offset ID, and bidirectional
cross-language Broker readback.
It deletes any owned topic and tears down only that project. Toolchain,
configuration, output, and cleanup evidence is stored under
`docs/compatibility/evidence/phase-19/`; a manifest row becomes `PASS` only
after comparison and cleanup both succeed. The runner implements seven of 37
cases and all seven have passed: the two route cases, `HEARTBEAT-001`, and
`PROD-001-sync-send`, `PROD-002-async-send`, `PROD-003-oneway-send`, and
`PROD-004-batch-send`.
`--phase N` and `--all` fail closed when any selected case has no runner.

Live runs require the pinned runner toolchains: Maven on JDK 27 and Rust 1.85.0,
matching the repository CI configuration.

The full local entry point is:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
bash integration-tests/run-local.sh --all
```

It first checks that every manifest row has a runner, then runs the offline
workspace gates and each profile-specific live case in its own Compose
project. It reconciles every Phase 19 manifest row against a `PASS` result and
archived evidence before reporting success. It refuses to start Docker when
any selected scenario is unimplemented. Currently seven of 37 live cases have
runners; the other 30, the two-Broker/TLS/ACL/feature profiles, and the full
`run-local.sh --all` pass remain pending. Starting the basic profile by itself
does not mark a scenario passed.
