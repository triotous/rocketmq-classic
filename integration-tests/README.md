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
with `rocketmq-classic-p19-`. To run the currently implemented live case:

```bash
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable \
ROCKETMQ_PROFILE=basic \
tests/differential/run.sh --scenario ROUTE-001-get-existing-topic
```

The runner creates a unique Compose project, provisions the manifest-owned
topic with four queues, queries the route using Java client 5.5.1 and the Rust
client, compares the sorted queue rows, deletes the topic, and tears down only
that project. It stores toolchain/configuration/output/cleanup evidence under
`docs/compatibility/evidence/phase-19/` and marks the manifest row `PASS` only
after comparison and cleanup both succeed. The differential inventory has 37
cases; `ROUTE-001-get-existing-topic` is the sole implemented and passed case
in the current runner. `--phase N` and `--all` fail closed when any selected
case has no runner, so they currently report the unimplemented cases instead
of skipping them.

Live runs require the pinned runner toolchains: Maven on JDK 27 and Rust 1.85.0,
matching the repository CI configuration.

The two-Broker, TLS, ACL, and feature-specific profiles; remaining 36 live
cases; and a `run-local.sh --all` orchestration entry point are still pending.
Starting this basic profile by itself does not mark a scenario passed.
