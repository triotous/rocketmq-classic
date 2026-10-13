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
  remains `PARTIAL`; Phase 9 now has fixture/unit coverage for raw Pull offset
  entrypoints, `consumerRunningInfo`, the construction-time start timestamp,
  `get/setPullAPIWrapper`, and Pull's `get/setServiceState`. The Push
  `getDefaultMQPushConsumerImpl()` alias, service-state, pause,
  consume-orderly, exception-delay, and message-queue-listener accessors,
  effective retry-limit getter,
  offset-store accessors, direct/periodic/shutdown offset persistence scoped to
  active pull queues, timestamp offset reset, `queryConsumeTimeSpan` direct
  route lookup and ordered per-Broker requests with VIP handling, consumer
  settings/subscriptions (including the class-filter overload's namespace,
  source retention and heartbeat path), and direct consume-hook entrypoints now
  have Java fixture/Rust evidence. Push filter-hook registration and POP invocation,
  `parseSubscribeMessageQueues`, `tryResetPopRetryTopic`, and
  `resetRetryAndNamespace` also have invoked Java fixture/Rust checks. Push
  raw listener registration/getter and facade-owner identity also have direct
  Java fixture/Rust checks. Push subscribe-route update and paused `tryRebalance` entrypoints now have direct
  fixture checks; active assignment behavior is covered by Phase 11 tests. The
  `pullMessage` row also now covers Java's one-time
  first-lock offset refresh for orderly clustering pulls; its Java fixture and
  Rust regression pass. Push reset-offset behavior now also matches Java's
  active-queue selection, null-offset drop/clear, callback drain and orderly
  lock windows, conditional offset-store cleanup, and late-work generation
  fencing; paused-time reset regressions cover the timeout and removal paths.
  The returned Push implementation's 71 declared public
  signatures are now captured from the pinned Java fixture and count-pinned in
  Rust. Its `consumerRunningInfo` method now has a direct Rust owner API and
  fixture-backed running-flow checks. `adjustThreadPool` has a direct owner
  mapping and fixture evidence for the pinned built-in services' no-op
  behavior. The raw `executeTask`/`executeTaskLater` methods map to the
  owner-scoped runtime scheduler and now have invoked-fixture plus lifecycle /
  delay regressions. The raw consume-service, stats-manager, and client-factory
  accessors have source-backed internal-entrypoint dispositions: their owner
  flows are covered, while mutable Java service/factory handles stay internal
  to the Rust lifecycle owner. `RebalancePushImpl.messageQueueChanged` has an
  invoked Java fixture and Rust regression for global queue/size threshold
  scaling across multiple topics, later assignment changes, empty-table
  retention, pull backpressure, and running-info values; fixture SHA-256
  `c531359f6121cd35087a50150bebc14064f4a9d423825f2fdbb16aa7629adf67`. The
  raw Push inventory now has a method-level mapping or reviewed disposition.
  Heartbeat Fastjson input coverage now includes 1086 invoked Java cases with
  systematic whitespace positions and raw control characters in string keys
  and values; the Rust differential and Maven fixture-drift gate pass for that
  corpus. Route DTO Fastjson input coverage now includes 527 invoked Java
  cases, including 128 raw U+0000–U+001F probes across values, field names,
  and filter-map keys/values; the Rust differential and fixture-drift gate
  pass. These bounded inputs do not close the broad Phase 6
  source/API/input/reverse-export audit. Pull `getRebalanceImpl`, both
  `pullKernelImpl` overloads, `popAsync`, and the `RebalanceImpl`/`RebalancePullImpl`
  surface now have source-based owner-flow mappings or internal-entrypoint
  dispositions. Pull
  `sendMessageBack`/`maxReconsumeTimes` covers the 3000 ms timeout and logical
  Broker route lookup offline; its live Broker handoff remains pending.
  The public `TopicValidator.ValidateResult(boolean, String)` constructor,
  including null remarks, also has direct pinned-Java fixture and Rust
  differential coverage. `TopicValidator` null-input behavior and nullable
  system/send-block set members now have fixture-backed Rust differentials too;
  this closes those specific branches only, while the broad API/source/input
  audit remains open. A re-export-aware Rustdoc scan now has exact crosswalks
  for all producer and consumer inherent method names; Java-side reverse
  exports, inherited/trait methods, and source/input branch equivalence remain
  part of P18-V01. The previously unmapped Producer
  `setCallbackExecutor`/`setAsyncSenderExecutor` surface now has pinned Java
  fixture coverage, Rust facade methods, rejection/backpressure/deadline tests,
  and a Phase 7 matrix entry. Rust keeps caller-Tokio execution as its default;
  Java's private fixed-size sender pool is not reproduced. The runtime
  backpressure setters, in-flight permit adjustment, mode toggle, live retry
  response-code set and public adjustment/lock helpers found by the reverse scan
  now have Rust APIs and fixture-backed unit evidence. `getCompressor`,
  `setCompressType`, `initProduceAccumulator`, and runtime auto-batch state now
  have Producer APIs, invoked Java fixtures and Rust differential tests. The
  runtime `setCompressMsgBodyOverHowmuch` and `setCompressLevel` settings also
  have live Producer APIs and Java/Rust regression coverage. The
  broad Java reverse-export, inherited/trait API, and source/input branch
  reconciliation remains local P18-V01 work, not manual validation.
- [x] Register live cases in the Phase 19 scenario manifest and validate the
  manifest locally.
- [x] Re-run full workspace/all-feature P18-V02 gates on the latest Producer
  revision `da683076`. Default/all-feature workspace tests, strict workspace
  Clippy, fmt/diff, Maven verify (37 scenarios), comparator self-test and
  37-scenario manifest validation, the 16-case race/leak smoke repeated three
  times, bounded fuzz smoke, release examples and benchmark compilation pass.
- [ ] Obtain a current GitHub Ubuntu CI result for the Linux-specific Java
  address profile. A native Ubuntu ARM64 container passes the Core RPC
  integration suite; this does not replace the required GitHub Ubuntu result.
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

- [x] Set internal workspace dependency requirements to the currently aligned
  `0.1.0` package versions in dependency order while retaining local paths for
  workspace builds. This prepares crate manifests for registry publication;
  it does not prove registry availability.
- [ ] Confirm `0.1.0` remains the intended release version set and validate
  registry dependency resolution before publication.
- [x] Add package descriptions and repository metadata; verify them with
  `cargo metadata --locked`.
- [x] Review package contents and dependency license/provenance metadata.
  `cargo package --workspace --list --locked --allow-dirty` lists 7 crate
  archives with per-crate `LICENSE` and `NOTICE`; contents are crate source,
  examples/benches, tests, and synthetic TLS/fuzz fixtures. Locked Cargo
  metadata reports 207 external crates, all from crates.io registry sources
  with license metadata and no git/path overrides. The manifest license
  expressions use permissive alternatives. Internal workspace dependencies
  now pair local paths with `0.1.0` version requirements. This does not establish
  future registry resolution; confirm the version set and validate the registry
  graph before publication.
- [ ] Keep each package unpublished until all of its dependencies are available
  and its package contents have been inspected.

## Apache license files

- [x] Add the full Apache License 2.0 text at the repository root, matching
  the workspace manifests' `Apache-2.0` declaration.
- [x] Preserve the Apache RocketMQ reference `NOTICE` at the repository root
  for the Java 5.5.1 compatibility harness and its source-derived fixtures.
  Package contents and external dependency provenance are reviewed above;
  registry resolution of workspace dependencies remains release preparation.

## Final release boundary

- [ ] Keep `publish = false` until Phase 19 has passed and package/legal review is
  complete.
- [ ] Do not create or push a release tag as part of Phase 18.
- [ ] Re-run the full release checks from the final revision before publication.
