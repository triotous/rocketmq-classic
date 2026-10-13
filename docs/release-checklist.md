# Release preparation checklist

This checklist records Phase 18 preparation only. It does not authorize a
compatibility claim, registry publication, release tag, or deployment. The
Java client baseline is RocketMQ 5.5.1; the Phase 19 Broker baseline is 5.3.4.

## Compatibility and validation

- [x] Keep the Java client and Broker baselines separate in docs and fixture
  provenance.
- [x] Record the server-free tests, fuzz/race smoke runs, benchmark observations,
  and CI commands in the Phase 18 execution log and compatibility docs.
- [ ] Close P18-V01: complete bidirectional Java source/API/input reconciliation
  and reverse-audit all Rust public APIs and settings. The broad Phase 6 audit
  remains `PARTIAL`; the Phase 9 deprecated implementation-accessor audit now
  records remaining raw implementation methods and lifecycle/rebalance
  integration APIs. Pull `sendMessageBack`/`maxReconsumeTimes` now covers the
  3000 ms timeout and logical Broker route lookup offline; its live Broker
  handoff remains pending.
- [x] Register live cases in the Phase 19 scenario manifest and validate the
  manifest locally.
- [ ] Obtain an Ubuntu CI result for the Linux-specific Java address profile.
- [ ] Complete Phase 19 Broker/Docker scenarios before final compatibility or
  release acceptance. The owner must provide any deployment-only credentials,
  topology, and maintenance inputs at that phase.

## Package graph and metadata

Current Cargo metadata reports seven workspace packages, all at version
`0.1.0`, all with `publish = false`, and all declaring `Apache-2.0`. Repository
URL and package descriptions are now present. The package dependency order for
a future leaf-to-facade publication is:

1. `rocketmq-classic-common`
2. `rocketmq-classic-protocol`
3. `rocketmq-classic-remoting`
4. `rocketmq-classic-core`
5. `rocketmq-classic-producer`
6. `rocketmq-classic-consumer` (depends on producer)
7. `rocketmq-classic` (facade)

Before any publication:

- [ ] Choose release versions and replace workspace path dependencies with
  registry-compatible version requirements in dependency order.
- [x] Add package descriptions and repository metadata; verify them with
  `cargo metadata --locked`.
- [ ] Review package contents and dependency license/provenance requirements;
  local-path workspace dependencies do not prove registry resolution.
- [ ] Keep each package unpublished until all of its dependencies are available
  and its package contents have been inspected.

## Apache license files

- [ ] Add the full Apache License 2.0 text at the repository root. The manifests
  declare `Apache-2.0`, but no root `LICENSE` file is present.
- [ ] Review whether this repository's Java reference fixtures or other
  included material require attribution in a root `NOTICE` file. No root
  `NOTICE` file is currently present; its absence is not evidence that no
  attribution is required.

## Final release boundary

- [ ] Keep `publish = false` until Phase 19 has passed and package/legal review is
  complete.
- [ ] Do not create or push a release tag as part of Phase 18.
- [ ] Re-run the full release checks from the final revision before publication.
