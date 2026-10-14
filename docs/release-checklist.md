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
  runtime batch delay, per-batch byte threshold, and aggregate hold cap also
  have invoked Java fixture coverage and live Producer APIs. Rust tests compare
  pre-init/default/runtime values, preserve effective settings after invalid
  updates, flush pending batches after a lower byte threshold, reschedule a
  pending delay, and apply aggregate-cap changes to later enqueue decisions.
  Reinitializing after an invalid runtime setter retains the already active
  accumulator values, matching Java. Fixture SHA-256 is
  `4b166c789a8698ac5600667e2594d7025f7c1b90dd67b2949719269cb6d8160c`.
  The
  runtime `setCompressMsgBodyOverHowmuch` and `setCompressLevel` settings also
  have live Producer APIs and Java/Rust regression coverage. The
  broad Java reverse-export, inherited/trait API, and source/input branch
  reconciliation remains local P18-V01 work, not manual validation. Producer
  `EndTransactionHook` registration, context and direct execution now have
  fixture-backed local and Broker-check path coverage, including ordered
  execution and hook-failure isolation; this closes that API slice only and
  leaves broad P18-V01 reconciliation open. The pinned
  `DefaultMQProducerImpl` fixture now records its 2 public constructors and 82
  declared public methods. Direct CheckForbidden/SendMessage presence and
  execution helpers now map to public Rust methods, with fixture-backed tests
  for hook ordering, rejection short-circuit, context mutation and panic
  isolation. Producer service-state getters/setters now map to a nullable Rust
  enum with fixture-backed Java names/ordinals and lifecycle transitions;
  `isUnitMode` maps to the Producer config view. `getTopicPublishInfoTable`
  now returns a shared `TopicPublishInfoTable`; Java fixture/Rust tests cover
  live map/value/list/route/cursor aliases and a later scripted send observes a
  changed queue list. Its `ConcurrentMap` operations, nullable callback/key/
  value/result branches, ordered `putAll` null-row behavior, and retained
  key/value/entry view single, bulk, `removeIf`, and `setValue` mutations now
  have invoked Java fixture and Rust differential evidence. Fixture SHA-256 is
  `3815a09b647c39d68ae6bb3538e3d026eb48dd9dc12135c16d50c2a1c9f04ca2`.
  `TopicPublishInfo.selectOneMessageQueue(QueueFilter...)` also has direct
  null/empty array, null filter, null/empty queue-list and non-short-circuit
  filter-order differentials in that fixture.
  The pinned fixture now inventories the `MQClientInstance` returned by the
  Producer client-factory getter and the `MQFaultStrategy`/`BrokerFilter`
  surfaces. Their raw mutable factory, filter, and ThreadLocal objects remain
  owned by Rust runtime/Producer lifecycle owners; each pinned Producer call
  site maps to an owner flow. This closes those surface-disposition slices.
  The boolean `start`/`shutdown` overloads and raw
  `executeAsyncMessageSend` helper now also have pinned call-site and owner
  dispositions in the Phase 7 matrix; their use is confined to shared factory
  lifecycle and typed async Producer send paths.
  The facade inventory now pins 11 `DefaultMQProducer` constructors,
  107 declared/183 callable methods, 34 declared/42 callable `MQProducer`
  methods, and 8 `MQAdmin` methods. Rust also exposes direct-send and forced
  accumulator paths, with scripted coverage for both choices under opposite
  `autoBatch` settings. All 11 constructor signatures are individually pinned,
  and their group, RPC-hook, topic, namespace and trace-field mappings are now
  invoked against Java and differentially checked against Rust configuration,
  including nullable group, namespace, topic and trace values. `ProducerConfig`
  preserves nullable groups with an `Option<String>` representation, and null
  group construction/setter/start behavior matches Java's blank-group error
  and `START_FAILED` state through the nullable `set_producer_group` API.
  Public Producer hook contexts also preserve nullable group fields.
  Java's retained topic-list identity maps to
  Rust-owned values. This closes the constructor and null-group lifecycle
  slice; malformed-input and broad P18-V01 reconciliation remain open.
  Per-overload input/branch mappings, Java reverse-export, inherited/trait API,
  and broad source/input reconciliation remain local P18-V01 work.
  A targeted trait pass now maps the custom consistent-hash constructor,
  `CommandHeader`, consumer runtime/request adapters, TLS decrypt strategy, and
  listener/transaction executor callbacks; no missing capability was confirmed
  in those slices. The full inherited and Rust/Java reverse-export audit is
  still open.
  Runtime Producer scalar settings for create-topic key/queue count, send and
  per-request timeouts, sync/async retry counts, retry-after-non-OK-store, and
  message-size limit now have live `Producer` setters and fixture-backed send,
  retry, deadline, and validation regressions (code commit `781bf6f0`; fixture
  SHA-256 `46c9c9522317d72d2942fab4abae0caf9c6fd9bf76a179ea90a6ec453b041f3c`).
  Live Producer group and topics setters now have fixture-backed behavior:
  group changes reach subsequent headers (including null-field omission), and
  topic getters reflect setter calls while the started runtime retains its
  startup topic snapshot. VIP getter mutation is also covered; the active
  runtime keeps the ClientConfig snapshot copied at start, matching Java's
  `MQClientManager.getOrCreateMQClientInstance`. The Producer fixture SHA-256
  is `1ed0060666e8ee2a6b5b82b3088b22cb41a9a690bc291a96e56e51d16e7f7d76`.
  Post-start Producer-group mutation now has direct Java lifecycle fixture and
  Rust differential coverage: shutdown unregisters by the current group key,
  including the Java null-group exception and service-state result; pending
  request entries remain after producer shutdown. Fixture
  `fixtures/producer/lifecycle.json` SHA-256 is
  `9d5063e8ae538f7da3f957c874dfb95dd81a6a85bf113a947a796de1815001d8`.
  Per-overload input/branch mappings, Java reverse-export, inherited/trait API,
  and broad source/input reconciliation remain local P18-V01 work.
- [x] Map runtime Producer fault-strategy settings: live latency-fault and
  detector toggles, mutable `long[]` table aliasing/replacement, and subsequent
  Broker selection. Null setter/getter behavior, deferred null-table NPEs,
  short duration-table AIOOBE, and ignored extra duration entries now have
  invoked Java fixtures and Rust differentials; see the Phase 7 matrix and
  Phase 6 audit.
- [x] Register live cases in the Phase 19 scenario manifest and validate the
  manifest locally.
- [x] Re-run full workspace/all-feature P18-V02 gates on Producer revision
  `1845c55a`. Default/all-feature workspace tests, the 112-unit-test
  Producer suite and integration targets, strict workspace Clippy, fmt/diff,
  Maven verify (37 scenarios), comparator self-test and 37-scenario manifest validation, the
  16-case race/leak smoke repeated three times, 1,824-mutation fuzz smoke,
  release examples and benchmark compilation pass. The new runtime fault
  fixture SHA-256 is
  `2f8c6d40c4669a6a662085e8cddef749338c91fcbb1bf64829ef1f59f77a284e`.
- [x] Repeat full workspace/all-feature P18-V02 gates on current revision
  `6c62ebf2`. Sequential default and all-feature workspace tests pass; the
  Producer suite passes 120 unit tests and all integration/doc targets. Strict
  workspace Clippy, Maven verify (37 scenarios), comparator self-test and
  manifest validation, 16 race/leak cases repeated three times, 1,824-mutation
  fuzz smoke, release examples, benchmark compilation, formatting and diff
  checks pass. Workspace metadata and all seven package contents also pass.
- [x] Re-run server-free P18-V02 gates with the runtime Producer accumulator
  parity change on the worktree based on `454d57cf`, before its feature commit.
  Default and all-feature workspace tests, Producer tests (123 unit tests and
  all integration/doc targets), strict workspace Clippy, default Maven verify
  with fixture drift checking (37 scenarios), comparator self-test and
  37-scenario manifest validation, 16 race/leak cases repeated three times,
  1,824-mutation fuzz smoke, release examples, benchmark compilation, package
  metadata/content checks, formatting and diff checks all pass. The updated
  Producer fixture SHA-256 is
  `4b166c789a8698ac5600667e2594d7025f7c1b90dd67b2949719269cb6d8160c`.
- [x] Re-run the server-free P18-V02 gates on revision `281dd9e0` after
  nullable Producer group compatibility. Default and all-feature workspace
  tests, strict workspace Clippy, formatting/diff checks, Maven fixture drift
  and Java/Rust cross-decodes (37 scenarios), comparator self-test,
  37-scenario manifest validation, 16 deterministic race/leak cases repeated
  three times, 1,824-mutation fuzz smoke, release facade examples, offline
  benchmark compilation, and workspace metadata/license/dependency checks pass.
  The Producer fixture SHA-256 is
  `b0a59a0c084fc24ddff5c4947e7064d20311fe0dcf38306b8ac45ee15546c4fc`.
- [x] Re-run current-revision server-free P18-V02 gates on `f3d5b44d` after
  Producer group, VIP-channel and topic setter compatibility updates. Default
  and all-feature workspace tests, strict all-target/all-feature Clippy,
  formatting/diff checks, Maven verify (37 scenarios, fixture drift and Java /
  Rust cross-decodes), comparator self-test, 37-scenario manifest validation,
  16 race/leak cases repeated three times, 1,824-mutation fuzz smoke, release
  example build, offline benchmark compilation, and workspace metadata checks
  pass. Producer fixture SHA-256:
  `1ed0060666e8ee2a6b5b82b3088b22cb41a9a690bc291a96e56e51d16e7f7d76`.
- [x] Re-run server-free P18-V02 gates on lifecycle revision `9834d96d`.
  Default and all-features workspace tests, strict all-target /
  all-feature Clippy, formatting/diff checks, Maven verify (37 scenarios with
  fixture drift and Java/Rust cross-decodes), comparator self-test, 37-scenario
  manifest validation, 16 race/leak cases repeated three times, 1,824-mutation
  fuzz smoke, release example build, offline benchmark compilation, and
  workspace/package metadata checks pass. The lifecycle fixture SHA-256 is
  `9d5063e8ae538f7da3f957c874dfb95dd81a6a85bf113a947a796de1815001d8`.
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
