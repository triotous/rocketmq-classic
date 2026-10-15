# Release preparation checklist

This checklist records Phase 18 preparation only. It does not authorize a
compatibility claim, registry publication, release tag, or deployment. The
Java client baseline is RocketMQ 5.5.1; the Phase 19 Broker baseline is 5.3.4.

## Compatibility and validation

- [x] Keep the Java client and Broker baselines separate in docs and fixture
  provenance.
- [x] Record the server-free tests, fuzz/race smoke runs, benchmark observations,
  and CI commands in the Phase 18 execution log and compatibility docs.
- [x] Reconcile the Phase 15–17 local matrices and execution records against
  current artifacts. Phase 15's P15-V01/V02/U01 local gates are verified;
  P15-V03 remains Phase 19. Phase 16 records all local acceptance gates as
  passed; Broker and owner-environment cases remain Phase 19. Phase 17 records
  P17-V01/V03/U01 local evidence; `jstackEnable` enumerates threads on macOS,
  Linux and Windows, with Linux runtime coverage on both architectures and
  Windows cross-target compile evidence. Native Windows stack runtime and
  Broker-backed query checks remain later validation. Plans remain unchanged.
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
  fixture-backed running-flow checks. Its `ConsumerRunningInfo` body and
  `ProcessQueueInfo`/`PopProcessQueueInfo`/`ConsumeStatus` models now also match
  inherited Java object-identity equality instead of exposing structural
  `PartialEq`/`Eq`. `QueryAssignmentRequestBody`,
  `SetMessageRequestModeRequestBody`, `GetConsumerStatusBody`,
  `ResetOffsetBody`, and `ConsumeMessageDirectlyResult` now also omit Rust
  structural equality, matching inherited Java `Object.equals/hashCode`; their
  invoked identity fixture is `fixtures/protocol/dto-identity.json`.
  Sixteen typed response-header DTOs now also omit structural equality,
  matching the pinned Java classes' inherited Object identity; their fixture
  is `fixtures/protocol/header-response-identity.json`. The 31 modeled request
  headers inheriting `RpcRequestHeader.equals/hashCode` now retain and encode
  all four nullable base fields and compare/hash only those fields; the invoked
  Java fixture is `fixtures/protocol/request-header-identity.json`. The
  `ConsumeMessageDirectlyResultRequestHeader` base `bname` is separate from its
  subclass `brokerName`, and Lock/Unlock request headers have distinct Rust
  types. This closes the inherited request-header equality slice only; the
  broad P18-V01 audit remains open.
  `adjustThreadPool` has a direct owner
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
  including null remarks and inherited identity-based `Object.equals`/
  `hashCode`, also has direct pinned-Java fixture and Rust API-boundary
  coverage. Rust intentionally omits structural `PartialEq`/`Eq` for this
  Java identity-valued result. `TopicValidator` null-input behavior and nullable
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
  The implementation-level `setSemaphoreAsyncSendNum/Size` helpers now also
  have direct Rust APIs and invoked fixture evidence for raw replacement,
  negative permits, releases into the replacement semaphore, later facade
  resize accounting, and unchanged facade limits. The updated Producer core
  fixture SHA-256 is
  `defd34c1ebf1fc81bb6cdbed4406c40bf5794c73bdf7ef6a85d1e37b84cecbe5`.
  Reinitializing after an invalid runtime setter retains the already active
  accumulator values, matching Java. Fixture SHA-256 is
  `4b166c789a8698ac5600667e2594d7025f7c1b90dd67b2949719269cb6d8160c`.
  The
  runtime `setCompressMsgBodyOverHowmuch` and `setCompressLevel` settings also
  have live Producer APIs and Java/Rust regression coverage. The
  broad Java reverse-export, inherited/trait API, and source/input branch
  reconciliation remains local P18-V01 work, not manual validation. Producer
  selector failures now have invoked Java evidence for selector-only and sync
  send: both wrap the thrown selector failure with the Java client exception
  and cause. Rust catches selector panics as `ProducerError::SelectorPanicked`;
  the scripted regression compares the exact Java message and cause text and
  confirms that neither path sends a Broker request. `fixtures/producer/core.json`
  SHA-256: `d1b05a04ed53dde2820647bd964ea2092592b35d51de90273f5b22f77ded0fe7`.
  This closes that selector failure branch only; broad P18-V01 reconciliation
  remains open. Reply-host conversion and `MessageExt` host getter/setter types
  now have invoked Java differentials for resolved/unresolved addresses,
  IPv4/unbracketed IPv6, Unicode decimal ports, range boundaries, missing
  separators, invalid/out-of-range ports, the socket-address constructor,
  host-byte methods and `socketAddress2String`; these close only the host API
  and reply-processing slices. `fixtures/runtime/inbound-routing.json` SHA-256:
  `614adec3fe0eb41a261000744602f0247aa3ddaf03fdd42fdabf9ee9b8785eb2`.
  A follow-up Java `messageExtSystemFlagsApi` fixture now covers topic/group
  transient system-flag get/set/clear and malformed/Unicode integer inputs,
  V6 flag setters, and topic-filter parsing. Rust matches those cases in
  `message_ext_system_flag_helpers_match_java_551`, which also compares
  `MessageExt.toString()` for a deterministic one-property message. For
  messages with multiple properties, Rust uses its stable map order because
  Java `HashMap` order is unspecified. The broad source/API/input/reverse-export
  reconciliation remains open.
  The Java `Message` user API now also has direct Rust mappings for property
  lookup/mutation, Buyer ID and instance ID, collection-based keys, delay-level
  and timer-delay accessors, signed Java integer parsing and `toString()`;
  `MessageExt` exposes inherited Message methods through Rust deref. The
  invoked `messagePublicApi` fixture checks default null topic/body/properties,
  default plus all four parameterized constructor forms, nullable setter values, lazy property-map
  creation after null-name lookups, null-valued system properties, user-property
  null rejection, null collection/null-element keys, nullable JSON round-trips,
  numeric bounds, Unicode digits, malformed/overflow values and map mutation;
  fixture SHA-256 is
  `6ec3f130f86c25b12b08f5d5d51f5b43c0bab1a2e77738208909959bbef79db5`.
  This closes the Message constructor/null-state slice only; broader inherited
  API and reverse-export reconciliation remain local P18-V01 work. The pinned
  Common API reverse scan also removed Rust-only `Message::{take_body,
  take_body_nullable,set_property,properties_mut}` and
  `MessageQueue::new_nullable_broker`; compile-fail doctests guard those five
  public boundaries. The exact Java user-property and nullable queue APIs
  remain available.
  The public-surface reverse scan also found caller-configurable decoder limits
  and a limited decompression function with no Java API counterpart. Those
  controls are now internal fixed safety bounds; decoder and pull-result public
  calls no longer accept them. Low-cap regressions run inside the common crate.
  This closes the identified decoder API difference only; full P18-V01 remains
  open. The reverse-scan finding that `serde_fastjson` was an externally public
  helper module with no matching Java utility API is now closed: both the common
  and protocol adapters are crate-private, and rustdoc compile-fail cases check
  that downstream imports fail. Code commit: `b62e66c1`. Producer
  `EndTransactionHook` registration, context and direct execution
  now have fixture-backed local and Broker-check path coverage, including ordered
  execution and hook-failure isolation; this closes that API slice only and
  leaves broad P18-V01 reconciliation open. The pinned
  `DefaultMQProducerImpl` fixture now records its 2 public constructors and 82
  declared public methods. Direct CheckForbidden/SendMessage presence and
  execution helpers now map to public Rust methods, with fixture-backed tests
  for hook ordering, rejection short-circuit, context mutation and panic
  isolation. The latest fixture adds default-route ASYNC CheckForbidden
  wrapping and explicit-queue ASYNC pass-through cases. Producer service-state
  getters/setters now map to a nullable Rust
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
  in those slices. Phase 10 and 12 now include method-level crosswalks for the
  public concurrent and orderly listener traits, their returned status/context,
  null/panic disposition, and owning callback tests. The full inherited and
  Rust/Java reverse-export audit is
  still open.
  Runtime Producer scalar settings for create-topic key/queue count, send and
  per-request timeouts, sync/async retry counts, retry-after-non-OK-store, and
  message-size limit now have live `Producer` setters and fixture-backed send,
  retry, deadline, and validation regressions (code commit `781bf6f0`; fixture
  SHA-256 `46c9c9522317d72d2942fab4abae0caf9c6fd9bf76a179ea90a6ec453b041f3c`).
  Follow-up oracle and regressions now cover per-request timeout setter inputs
  `-2`, `-1`, `0`, `125`, and `6000`; a deterministic zero-cap case confirms
  sync retries continue while the outer timeout remains. This closes that
  timeout/retry slice only; broad P18-V01 reconciliation remains open.
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
- [x] Re-run current-tree P18-V02 server-free gates at revision `450ed704`
  after Producer send-overload coverage. Default and all-features workspace
  tests, strict all-target/all-feature workspace Clippy, formatting/diff,
  Maven verify (37 scenarios, fixture drift and cross-decodes), comparator
  self-test, manifest validation (37 scenarios), deterministic race/leak smoke
  (16 cases × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824
  mutations), release examples, offline benchmark compilation, workspace
  metadata, and seven package-content listings all completed successfully.
- [x] Re-run server-free P18-V02 gates for the CheckForbidden synchronous
  retry slice on the worktree based on `3695b821` (2026-10-14). Default and
  all-features workspace tests, strict all-target/all-feature workspace
  Clippy, fmt/diff checks, Maven verify (37 scenarios with fixture drift and
  Java/Rust cross-decodes), comparator self-test and 37-scenario manifest,
  16 deterministic race/leak cases repeated three times, 1,824-mutation fuzz
  smoke, release examples, offline benchmark compilation, and workspace
  metadata all pass. `fixtures/producer/core.json` SHA-256:
  `ec20ff084ee066fc71de15e9d744bc091aaced07d1038a8389f12a79d5985b08`.
- [x] Re-run current-worktree server-free P18-V02 gates after default-route
  ASYNC and explicit-queue ASYNC CheckForbidden parity (`39afef81`, based on
  `051c01a3`, 2026-10-14). Default and all-features workspace tests, the
  Producer suite (138 unit tests and 55 integration/doc targets), strict
  all-target/all-feature workspace Clippy, fmt/diff, Maven verify (37
  scenarios with fixture drift and cross-decodes), comparator self-test and
  37-scenario manifest validation, deterministic race/leak smoke (16 cases ×
  3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release
  examples, offline benchmark compilation, workspace metadata/license checks,
  and seven package-content listings pass. `fixtures/producer/core.json`
  SHA-256: `d4ef907983f88f7948c4b143fbcd061115cca4ebdc03c19e6acae52e0dc30408`.
- [x] Re-run Producer-scoped checks after the final ASYNC fault-entry assertion
  (`f314116a`, 2026-10-14): `cargo test -p rocketmq-classic-producer
  --locked` passes (138 unit tests and 55 integration/doc targets), as do
  strict all-target/all-feature Producer Clippy, formatting and diff checks.
  The full workspace/all-feature, Maven, race/leak, fuzz and package gates above
  were run on parent `39afef81`; this follow-up only changes a Producer test.
- [x] Close the missing Broker publish-address branch (`e48fb323`, 2026-10-14).
  The default send path now retries the Java `MQClientException` and returns the
  response-code-10003 wrapper with attempt/Broker metadata; explicit queue and
  selector sends expose Java's `The broker[broker] not exist` error. The route
  fixture preserves the writable queue with a null master address, and asserts
  no redundant NameServer fetch. Default/all-feature workspace tests, strict
  workspace Clippy, Maven fixture/differential verification, comparator and
  37-scenario manifest checks, race/leak and fuzz smoke, release examples,
  benchmark compilation, metadata and package-content checks all pass on
  `e48fb323`; the selector regression and full Producer suite also pass on
  follow-up test commit `28d28377`. Producer core fixture SHA-256:
  `725af3b3887f7a4ec6f6d72a23188e4fb80206e50c98b113b015d76917fedfd8`.
- [x] Re-run Producer checks after caller-visible `UNIQ_KEY` mutation parity
  (`4b311231`, 2026-10-14). `cargo test -p rocketmq-classic-producer --locked`
  passes (139 unit tests and 60 integration tests); strict all-target/all-feature
  workspace Clippy, Maven verify (37 scenarios, fixture drift and Java/Rust
  cross-decodes), formatting, and diff checks pass. The invoked Java fixture
  verifies accumulator mutation before a failed dispatch and direct-send
  service-state failure before mutation; scripted Rust tests cover these
  boundaries, direct-send timing, retry ID reuse, auto-batch IDs, and
  caller-supplied ID preservation. Producer fixture SHA-256:
  `079e9fcac47f97f4921a823c10b02a3b0ec882c42b85c85695663530047cd23d`.
- [ ] Obtain a current GitHub Ubuntu CI result for the Linux-specific Java
  address profile. A native Ubuntu ARM64 container passes the Core RPC
  integration suite; this does not replace the required GitHub Ubuntu result.
- [ ] Complete Phase 19 Broker/Docker scenarios before final compatibility or
  release acceptance. The local Docker runner and scenario execution are still
  implementation/automation work and cannot be reassigned to the manual list.
  The owner must provide any deployment-only credentials, topology, and
  maintenance inputs at that phase.

- P18-V01 ClientConfig reverse-surface follow-up (`2026-10-15`, pinned Java
  source `efa1be59d6fc747ad220f036c86030f05f0419f9`): all 33 Rust
  `ClientConfig` backing fields are crate-private; the mapped getter/setter
  surface remains public. Consumer running-info uses a read-only hidden bridge
  for Java `MixAll.object2Properties`, retaining raw values and null omission.
  The external compile-fail doctest passes. Core config differential passes
  (21/21); Consumer all-feature tests pass (219 unit tests and all integration
  and doc targets); Producer all-feature tests pass (142 unit tests and all
  integration/doc targets). This closes the ClientConfig field-visibility
  discrepancy only; broad P18-V01 remains open.

- P18-V01 Remoting per-channel setting timing (`b37d8522`, 2026-10-15):
  `clientChannelMaxIdleTimeSeconds` and `disableNettyWorkerGroup` now have live
  adapters sampled for each new channel. Rust preserves the initialized idle
  handler and dispatch path on existing channels; its worker pool starts once
  so later channels can select either worker dispatch or direct dispatch.
  Rust loopback tests cover post-start mutation and old-channel retention; the
  Java `RemotingLiveConfigProbe` verifies the corresponding idle handler and
  event executor for channels created after mutation. The two setting-timing
  rows are closed; broad P18-V01 remains open.
- PASS on `b37d8522`: `cargo test -p rocketmq-classic-remoting
  --all-features --locked` (89 unit, 6 codec, 1 fuzz regression, 5 security,
  95 transport, 5 doctests), strict Remoting all-target/all-feature Clippy,
  `cargo fmt --all -- --check`, Maven `verify` (37 scenarios, live-config
  probe, Java/Rust cross-decodes and fixture checks), and `git diff --check`.
  These are scoped checks, not the final full-workspace P18-V02 rerun.

## Phase 18 verification disposition

- [ ] **P18-V01** — Broad Java/Rust source, overload input/error/flow, inherited
  API, and reverse-export reconciliation remains open; this is local parity
  work, not final manual validation. Earlier Phase 3 notes that accepted
  unmatched Rust-only APIs, immutable config snapshots, or selected observable
  differences are historical and do not waive the current parity contract.
  Remoting `clientCloseSocketIfTimeout` now has a live Rust update adapter and
  Java/Rust post-start timeout evidence; `clientWorkerThreads`, `enableReconnectForGoAway`,
  `disableCallbackExecutor`, and `isScanAvailableNameSrv` also have live
  adapters and timing regressions. These close their setting rows only.
  The producer send mutation branch is now
  oracle-backed, and surfaced Rust-only publish-info constructors/snapshot
  helpers are crate-private. Producer publish-info queue/cursor/selector and
  live-view callback adapters now have exact Rust-to-Java matrix rows guarded
  by Producer tests. The full 17-method `TopicPublishInfo` and 3-method
  `ThreadLocalIndex` Java surfaces are fixture-pinned; cursor overflow and
  `toString()` formatting are checked against Java. The broad reverse-export
  and overload/input/error/flow audit remains open. The Phase 16 ACL reverse
  scan also removed duplicate public module helpers, the non-Java one-argument
  `AclUtils::combine_request_content`, and `SessionCredentials` builder/YAML
  exports while preserving the Java static `AclUtils`/`AclSigner` mapping and
  pre-encoded Java-Charset signer adapter; compile-fail doctests guard the
  removed boundaries. Code commit `6893f14b`; the Phase 16 ACL matrix and
  security usage guide name the retained Java-mapped entrypoints. The full
  Core locked test suite, strict Core Clippy, format and diff checks pass for
  this slice. Core `AdminQueryRequest` now keeps derived route/LMQ metadata
  private, preserves Java's nullable cluster/index-type/pagination inputs with
  builders, and matches the null-index result filter; LMQ and wheel-timer route
  tests cover the derived behavior. Code commits `e7dcdf63` and `daef67eb`;
  Core/Producer/Consumer locked tests, strict workspace Clippy, format, diff,
  and rustdoc JSON surface inspection pass for this slice. `QueryResult` now
  also matches Java's private fields, public constructor/getters, nullable
  list/elements, shared mutable list alias, and `toString`; invoked Java
  reflection/behavior fixtures guard the surface. Code commits `ba2589ec` and
  `57fe1527`. Default and all-feature workspace tests, strict workspace
  Clippy, Maven fixture verification, and Core/Producer/Consumer rustdoc JSON
  generation pass; Core JSON shows stripped fields. These bounded corrections
  leave P18-V01 open. The Pull facade's Rust-only singular `register_topic`
  helper is now private, and the combined `unregister_topic` convenience
  method was removed because pinned Java has no corresponding public API; the
  Java `registerMessageQueueListener`, register-topic set, subscription, and
  clear-subscriptions entrypoints remain available. Code commit `2bfa4c16`;
  Consumer locked tests/doctests, strict Consumer Clippy, formatting, and diff
  checks pass. `LocalFileOffsetStore::group_name` and `storage_path` were also
  removed from the public surface because pinned Java exposes neither getter;
  compile-fail doctests guard both boundaries. Code commit `e8582a22`; Consumer
  locked tests/doctests, strict Clippy, rustdoc JSON, formatting, and diff
  checks pass. The pinned Java Push/Pull facade inventory now records declared
  and inherited signatures and confirms neither facade declares a separate
  `queryMessageByUniqKey`; Rust's extra `query_message_by_unique_key` methods
  are removed. All three Rust `view_message` facades now fall back from an
  invalid physical ID or failed physical lookup to Java's unique-key query
  defaults (32 results, three-day lower bound, `Long.MAX_VALUE` upper bound),
  returning the first nullable result. Scripted tests cover fallback fields,
  namespaced topics, physical lookup, and terminal errors. The Pull example
  now registers through the Java-mapped listener API. This closes those facade
  methods and branches only; broad P18-V01 reconciliation remains open.
- Common message formatting reverse-surface slice: `Message::java_to_string`,
  `MessageExt::java_to_string`, and `MessageHostAddress::java_to_string` are
  now internal helpers; Java `toString()` behavior remains exposed through
  Rust `Display`/`ToString`. Compile-fail doctests guard all three boundaries.
  Common all-feature tests, strict Clippy, formatting, and diff checks pass.
  Constructor/property accessors and the wider Common/Core/API reconciliation
  remain open P18-V01 work.
- Common message equality reverse-surface slice: Java `Message` and
  `MessageExt` inherit identity-based `Object.equals`/`hashCode`; Rust no longer
  exposes structural equality. `MessageExt` equality now uses a hidden identity
  token; Java and Rust fixtures cover same-instance/clone and distinct-default
  outcomes. A compile-fail doctest guards `Message`'s lack of structural
  equality. Default/all-feature workspace tests, strict workspace Clippy,
  Maven verify (37 scenarios and cross-decodes), formatting, and diff checks
  pass for this slice. The full P18-V02 rerun, including race/fuzz, example,
  benchmark, metadata, and package-content gates, remains required on the final
  revision. Broader Common/Core/API reconciliation remains open P18-V01 work.
- Common `putUserProperty` whitespace input slice: Rust now follows Java
  `String.trim()` (U+0000–U+0020 only), preserving Java acceptance of
  non-breaking-space keys and em-space values. Invoked Java and Rust fixture
  cases cover those values and rejection of ASCII control/space-only input.
  This input boundary is closed; broad Message overload/input/API
  reconciliation remains open P18-V01 work.
- Common `MessageConst.STRING_HASH_SET` mutation: Rust now exposes a
  lock-protected mutable set with nullable members, and `putUserProperty`
  checks live membership before blank validation. Invoked Java and Rust
  fixtures verify dynamic additions/removals plus null/empty-string entries;
  null and empty reserved keys produce Java's reserved-property failure before
  blank-name validation. Fixture SHA-256:
  `cd48ccada3c107facb4ec332316ef29e569cca7c50cd3b0711152b7234cd6b98`.
  This closes that mutable-set behavior slice; broad Common reverse-surface
  reconciliation remains open P18-V01 work.
- Common `Message` priority accessors: `priority`/`set_priority` now match the
  pinned Java getter's `-1` fallback and decimal parsing, and setter rejection
  for negative values. Invoked Java/Rust fixtures cover map initialization,
  malformed/overflow/Unicode-digit inputs and no mutation after rejection.
  This closes that API slice only; P18-V01 remains open.
- Common MessageId API slice: Rust now exposes both Java `createMessageId`
  address forms, `decodeMessageId`, and the mutable nullable `MessageId` model.
  The invoked Java fixture covers IPv4/IPv6, negative 64-bit offsets, invalid
  hex conversion, trailing bytes, short input and invalid ports. This closes
  the MessageId surface only; fixture SHA-256 is
  `f58a8b10bf498cf00e3914a9fbc2a83016fb6b3bcbb8db9933403d834c1c121a`;
  P18-V01 remains open.
- Common CRC property helper slice: `append_crc32_property` matches both Java
  `createCrc32` overloads for zero, positive and negative inputs. The invoked
  fixture checks ByteBuffer and ByteBuf output equality; P18-V01 remains open.
- Common record-property decoder slice: `decode_record_properties` maps
  Java's public `decodeProperties(ByteBuffer)` behavior for the pinned V1/V2
  records, including null when no properties are present; P18-V01 remains open.
- Common compact-message API slice: public encode/decode, concatenated batch,
  inner-batch expansion and count helpers now match the invoked Java 5.5.1
  fixture, including copied outer metadata and checked malformed inputs.
  P18-V01 remains open for the other MessageDecoder methods and broad audit.
- Common `MessageExt` record encoder slice: public standard and unique encoders
  match the invoked Java 5.5.1 bytes for IPv4/IPv6, configured-size padding and
  undersized buffers; compressed ZLIB/LZ4/ZSTD records decode to Java's expected
  body. Rust regression and the Java fixture are updated. This closes those two
  encoder methods only; at that checkpoint, the remaining decoder overloads
  and broad P18-V01 audit remained open. Current message fixture SHA-256:
  `c7bf73f5baa095242502d97e06953e2ef2a2c4ea774ed69923aeef586fde940a`.
- Common client decode and `MessageDecoder` constant slice: Rust now exposes
  client ID fallback helpers, client batch decoding, Java record-offset and
  magic constants, and the UTF-8 name. Invoked fixture checks `clientDecode`,
  `decodes`, `isClient` IDs, and all public decoder constants. Regression tests
  pass for the client decode overloads and constants. This slice does not close
  the remaining P18-V01 audit. Current message fixture SHA-256:
  `756381ff5802566bf8236bae22c9e8ea1b1d008a27a728436ed6bb8e384a1746`.
- Common message property codec slice: nullable Java map/string inputs and
  nullable key/value entries now have explicit Rust adapters. The invoked
  fixture covers null maps, skipped null values, Java's literal `null` key,
  null decode input and permissive duplicate/malformed entries. This closes the
  property codec input slice only; P18-V01 remains open. Current message
  fixture SHA-256:
  `b4ae563511220dbf008a4ef0bc30d82f2c325005e36eb52b15a5ee87bff59d40`.
- Common `MessageExt` broker-name accessors now have direct Java fixture and
  Rust getter/setter coverage for default, non-null and cleared values. This
  closes this accessor slice only; P18-V01 remains open. Fixture at this slice
  SHA-256: `bb5fd8ba6d671739bb352e7780f29661e759c53dce7fe3887e1726149660ad1e`.
- Common `MessageVersion` now exposes Java's enum metadata and V1/V2 signed
  topic-length reads/writes. The invoked fixture and Rust regression cover enum
  order/names, magic conversion, signed narrowing, read/write cursors and
  truncated-read errors. Current message fixture SHA-256:
  `3d867fbd1ace33d2c95561ec12d7ac2f9f709fddc742c754459c7f75e08d563d`.
- Common message constants now have a direct Java-to-Rust value audit: the
  fixture reflects all 76 public `MessageConst` string fields, pins the two
  `MixAll` reply constants, and records the initial mutable reserved-property
  set. `message_constants_match_java_551` compares every value and reserved
  name. P18-V01 remains open for the broader source/API/input/flow audit.
- Common flag handling no longer exports Rust-only `has_compressed_flag`.
  Producer checks use Java's `MessageSysFlag.check(flag, COMPRESSED_FLAG)`
  counterpart, and a compile-fail rustdoc case guards the removed export.
  Broader P18-V01 reconciliation remains open.
- The five `PullSysFlag` bit masks are private in Rust, matching Java. Consumer
  and Core call sites now build flags through the mapped Java helper methods;
  a compile-fail rustdoc case guards the private constants. Broader P18-V01
  reconciliation remains open.
- `MessageExt::set_message` is removed: Java inherits `Message` state directly,
  and Rust already exposes those inherited setters through `DerefMut`. A
  compile-fail rustdoc case guards the removed Rust-only method; broader P18-V01
  reverse-export reconciliation remains open.
- `MessageAccessor.setProperties` now maps to `Message::set_properties` for
  replacing, emptying, and clearing the property map, with pinned Java fixture
  coverage. The selector path now matches `cloneMessage`'s observable fields:
  it gives callbacks an unnamespaced snapshot with the source body, flag and
  properties, and no transaction ID. Java's shared body/property references
  are recorded in the fixture; Rust callbacks receive an immutable reference,
  so a deep-owned snapshot preserves the observable values. Null-key maps now use `NullableMessageProperties`
  and `Message::set_nullable_properties`, with nullable lookup/clear APIs and
  encoded-key coverage across compact/stored records, producer send/batch, and
  consumer retry-topic fallback. Current message fixture SHA-256:
  `7c61fee7311b7926b92b845899083e5f80a2ba8e67105ed7656de3d5e3a17a22`.
  The same invoked fixture now calls Java's transfer/correction/origin-ID/MQ2/
  reconsume/max-reconsume/consume-start helper pairs and compares the Rust
  property-constant mappings, including a present null transfer flag. This
  closes those helper getter/setter value paths; broad P18-V01 remains open.
  `MessageExt` scalar getters/setters also now have a pinned Java fixture for
  zero/null defaults and signed boundary values, compared against all Rust
  scalar owner methods. This closes that API value slice only.
  Message tags, string keys, wait-store and transaction-ID setters also have
  direct Java/Rust getter checks, including nullable state and overwrite
  behavior.
  A Rust `MessageClientExt` view now preserves the Java no-op `setMsgId`, live
  `UNIQ_KEY` getter and mutable offset-ID fallback semantics. Its direct
  fixture/differential closes that subclass API slice only; broader P18-V01
  remains open.
  The Java `MessageQueue` constructors, fields, copy, equality/hash/text and
  ordering methods now have a method-level Phase 8 crosswalk to their existing
  invoked fixtures and Rust differential tests; broad P18-V01 remains open.
  `MessageClientIDSetter`'s public ID decoding helpers now map to Rust APIs and
  Java differential cases; the generator and near-time reconstruction use the
  process-local month boundary. The focused differential also passes with
  `TZ=Asia/Taipei`. The Java fixture now stores the near-time delta from its
  selected month start instead of an absolute epoch, avoiding timezone and
  month-rollover drift; the focused Rust differential passes under UTC and
  `TZ=Asia/Taipei`. CI runs the non-UTC case explicitly. This closes that
  helper slice only; broad P18-V01 remains open.
- [x] **P18-V02** — Re-run the complete server-free suite on revision
  `51be0d20` (2026-10-15). Default workspace tests under UTC, all-feature
  workspace tests, strict all-target/all-feature Clippy, Maven `verify` (37
  scenarios, fixture checks, and Java/Rust cross-decodes), comparator
  self-test and 37-scenario manifest, 16 deterministic race/leak cases × 3,
  fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release
  examples, workspace benchmark compilation, locked Cargo metadata, workspace
  metadata/license/dependency checks, seven package-content listings, format,
  and diff checks pass. The near-time ClientID differential also passes under
  UTC and Asia/Taipei. P18-V01, current-revision GitHub Ubuntu address-profile
  evidence, and Phase 19 Broker/Docker validation remain open.
- [x] Re-run the full server-free P18-V02 gates on current revision `15f8494b`
  after the Push runtime-setting correction (2026-10-14): default and
  all-feature workspace tests, strict all-target/all-feature Clippy, Maven
  verify (37 scenarios and Java/Rust fixture cross-decodes), comparator
  self-test, 37-scenario manifest, 16 race/leak cases × 3, fuzz smoke (104
  frame seeds, 10 message seeds, 1,824 mutations), release examples, offline
  benchmark compilation, workspace metadata/license/dependency checks, seven
  package-content listings, formatting, and diff checks all pass.
- Follow-up commit `7ece0e66` pins Java's per-pull reads of `pullBatchSize` and
  `pullBatchSizeInBytes` and adds a scripted wire assertion for post-construction
  updates. Default and all-feature workspace tests, strict Clippy, Maven
  verification (37 scenarios), format, and diff checks pass for this correction.
- Follow-up commit `f0eaf53e` reconciles Push runtime setting read timing against
  the pinned Java call sites. Pull/flow-control/POP/consume/heartbeat settings
  now read live values where Java does; `PullAPIWrapper` unit mode and the
  rebalance message model/allocation strategy take Java's start-time snapshots.
  Queue listener and `clientRebalance` changes after consumer construction
  reach later rebalance calls. `cargo test -p rocketmq-classic-consumer
  --all-features --locked` passes (205 unit tests, all consumer integration
  targets and doctests), as does strict all-target/all-feature Consumer Clippy.
  Formatting and diff checks pass. These setting-timing rows are closed; the
  broader P18-V01 audit remains open.
- [x] **P18-V03** — Facade examples, package graph/content/provenance, license
  files, compatibility baselines, and existing usage/behavior documentation
  have been reviewed. No release or compatibility claim is made.
- [ ] **P18-V04** — The 37-scenario handoff manifest validates and server-free
  suites pass locally. A current GitHub Ubuntu result for the address profile
  is still absent.

The Push runtime-setting timing rows listed in the prior audit checkpoint were
reconciled in commit `f0eaf53e`. P18-V01 remains open for the full Java/Rust
source, public API/settings, overload/input/error/control-flow and reverse
export audit; the current consumer-scoped test and Clippy pass do not replace
the required full Phase 18 gates on the final revision.

- Push consumer-group follow-up (`bd869fae`, 2026-10-14): the retained facade
  group is nullable and live; startup writes back the namespaced group,
  heartbeat preparation and shutdown use the current group, and consume-hook
  context preserves Java null. Pinned Java heartbeat and Push implementation
  fixtures cover mutation, unregister-by-current-group, and nullable context.
  Consumer all-feature tests (209 unit tests plus integration targets and
  doctests), strict Consumer Clippy, Maven verify (37 scenarios, fixture drift
  and Java/Rust cross-decodes), formatting, and diff checks pass for this
  slice. This does not close P18-V01 or replace full workspace gates on the
  final revision.
- Query-consume-time-span nullable group follow-up (`633b94bc`, 2026-10-14):
  the public request header retains nullable topic/group fields, and Push uses
  the current group. Pinned Java header conversion and the Rust loopback
  request both verify that a null group is omitted from ext fields. Protocol
  tests (including header inventory/validation), the Core API regression, the
  Consumer loopback case, strict Clippy for Protocol/Core/Consumer, Maven
  verify (37 scenarios and fixture/cross-decode checks), formatting, and diff
  checks pass. P18-V01 remains open beyond this path.
- Pull statistics group follow-up (`b05dff72`, 2026-10-14): Push now indexes a
  successful pull's statistics with the group retained by its `PullRequest`,
  matching Java's `incPullRT`/`incPullTPS` call sites. The invoked Java fixture
  mutates the facade group after creating the request and confirms the request
  keeps the original group; the Rust regression confirms statistics use that
  request group after facade mutation. Consumer all-feature tests (210 unit
  tests plus integration targets and doctests), strict Consumer Clippy, Maven
  verify (37 scenarios and fixture/cross-decode checks), formatting, and diff
  checks pass. This closes only the Pull statistics key path; consume-service
  statistics and the broad P18-V01 audit remain open.
- Consume-service statistics group follow-up (`edef3bd3`, 2026-10-14): Rust
  snapshots the namespaced group when Push services are created and uses it for
  concurrent/orderly consume metrics and POP-concurrent batch metrics. The
  pinned Java fixture constructs all four services, mutates the facade group,
  and confirms their retained group. POP-orderly consume metrics remain absent,
  matching its Java service flow; RunningInfo continues to look up the current
  facade group. Consumer all-feature tests (213 unit tests plus integration
  targets and doctests), strict Consumer Clippy, Maven verify (37 scenarios and
  fixture/cross-decode checks), formatting, and diff checks pass. This closes
  the identified consume-stat group timing paths only; P18-V01 remains open.
- Live consumer-group flow follow-up (2026-10-14): Push callback hook contexts
  and trace records now read the current nullable facade group; Broker send-back
  uses the live group while fallback retry topics apply the configured
  namespace. Pull send-back keeps the current/default or explicit group in the
  Broker request, while a failed request falls back to Pull's default facade
  group even when the request used a custom override. Invoked Java fixtures and
  Rust regressions cover those paths, including retry-topic fallback after
  facade mutation. Consumer all-feature tests (219 unit tests, all integration
  targets and doctests), strict Consumer Clippy, Maven verify (37 scenarios,
  fixture drift and Java/Rust cross-decodes), formatting and diff checks pass.
  Push fixture SHA-256 `4d98b6989130933d91831cbe20c671ba389e4e61f129153843d5fe5be8c981e5`;
  Pull fixture SHA-256 `1090617637533587bc425acaf1862521b7d2826c8747839a14a6e985fbdd4c78`.
  This closes only the audited group-timing paths; the broad P18-V01 audit and
  full workspace gates on the final revision remain open.
- Current-revision server-free gate rerun (`2c87e4ed`, 2026-10-14): default and
  all-feature workspace tests, strict all-target/all-feature workspace Clippy,
  `cargo fmt --all -- --check`, pinned Maven `verify` (37 scenarios, fixture
  drift and Java/Rust cross-decodes), differential self-test and 37-scenario
  manifest, fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations),
  16 deterministic race/leak cases repeated three times, release examples,
  offline benchmark compilation, workspace metadata/license/dependency checks,
  seven package-content listings, and `git diff --check` all pass. This closes
  the current-revision P18-V02 server-free rerun; broad P18-V01 and the required
  GitHub Ubuntu address-profile result remain open.
- Current-revision server-free gate rerun (`8f530402`, 2026-10-14): default and
  all-feature workspace tests, strict all-target/all-feature workspace Clippy,
  formatting and diff checks, Maven `verify` (37 scenarios, fixture drift and
  Java/Rust cross-decodes), differential self-test and 37-scenario manifest,
  fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), 16
  deterministic race/leak cases repeated three times, release examples, offline
  benchmark compilation, workspace metadata/license/dependency checks, and
  seven package-content listings all pass. This closes the current-revision
  P18-V02 server-free rerun only; P18-V01, the GitHub Ubuntu address-profile
  result, and Phase 19 Broker/Docker validation remain open.
- Current-revision server-free gate rerun (`7a4bfef5`, 2026-10-15), after the
  ACL public-surface correction: default and all-feature workspace tests,
  strict all-target/all-feature workspace Clippy, formatting and diff checks,
  Maven `verify` (37 scenarios, fixture drift and Java/Rust cross-decodes),
  differential self-test and 37-scenario manifest, 16 deterministic race/leak
  cases repeated three times, fuzz smoke (104 frame seeds, 10 message seeds,
  1,824 mutations), release examples, offline benchmark compilation, workspace
  metadata/license/dependency checks, and seven package-content listings all
  pass. P18-V02 server-free gates pass on this revision. P18-V01, the required
  current-revision GitHub Ubuntu address-profile result, and Phase 19
  Broker/Docker validation remain open.
- Current-revision server-free gate rerun (`133d4f54`, 2026-10-15), after the
  mutable `MessageConst.STRING_HASH_SET` correction: default and all-feature
  workspace tests, strict all-target/all-feature workspace Clippy,
  format/diff checks, Maven `verify` (37 scenarios, fixture drift and
  Java/Rust cross-decodes), comparator self-test and 37-scenario manifest,
  fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), 16
  deterministic race/leak cases × 3, release examples, offline benchmark
  compilation, workspace metadata and locked Cargo metadata, and all seven
  package-content listings pass. P18-V02 server-free gates pass on this
  revision. P18-V01, the current-revision GitHub Ubuntu address-profile result,
  and Phase 19 Broker/Docker validation remain open.
- Selector message snapshot (`2026-10-15`, pinned Java 5.5.1): the invoked
  `ProducerCoreFixtures.implementationSelectorMessageSnapshot` case confirms
  that `MessageAccessor.cloneMessage` passes a distinct selector argument,
  removes the configured namespace, retains body/flag/properties, shares body
  and property references, and leaves `transactionId` null. Rust now prepares
  the same readable selector view and preserves the input message; its closure
  receives `&Message`, so reference identity and mutation cannot be observed.
  The Java/Rust fixture and focused scripted-selector regression pass. Fixture
  SHA-256: `41dd9cca0b559e68397eb2d7bd41769596417ac0d0be605b8cd586423524b1d1`.
  This closes only the selector snapshot slice; broad P18-V01, the full gate on
  this revision, current-revision GitHub Ubuntu evidence, and Phase 19 local
  Broker/Docker automation remain open.
- The public `MessageAccessor.cloneMessage` helper now maps directly to
  `Message::clone_message`. Its invoked Java `messagePublicApi` fixture and Rust
  differential check copied topic/body/flag/property values, distinct message
  identity, omitted transaction ID, and unchanged source transaction ID.
  Producer selector preparation uses this same projection. The Java fixture
  confirms shared body-array/property-map identity, bidirectional mutation
  visibility, and per-message detachment after `setBody`/`setProperties`.
  Rust `clone_message()` now shares synchronized body/property storage and the
  replacement setters detach the receiver; ordinary Rust `Clone` remains a
  deep, independent copy. Rust accessors use borrow guards or owned property
  strings to keep those alias semantics safe. This closes the direct alias
  slice only; broad P18-V01 remains open. Fixture SHA-256 at that checkpoint:
  `b9f6f527720c425b17af57367a524f65e1f16da20acfc369502d64fa37b821e7`.
  The current-revision P18-V02 rerun is recorded below; broad P18-V01, GitHub
  Ubuntu evidence, and Phase 19 gates remain open.
- Current-revision server-free P18-V02 rerun (`1aae4772`, 2026-10-15):
  `TZ=UTC cargo test --workspace --locked` and
  `cargo test --workspace --all-features --locked` pass; strict workspace
  Clippy, Maven verify (37 scenarios and Java/Rust cross-decodes), comparator
  self-test and manifest validation, race/leak smoke (16 cases × 3), fuzz
  smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release examples,
  workspace benchmark compilation, locked Cargo metadata, workspace metadata,
  all seven package listings, format and diff checks pass. The near-time
  ClientID differential passes under UTC and Asia/Taipei. This closes P18-V02
  server-free gates on this revision only; P18-V01, current-revision GitHub
  Ubuntu address-profile evidence, Phase 19 Broker/Docker validation, and
  release version/registry checks remain open.
- P18-V02 scoped follow-up (2026-10-15): after protocol DTO identity changes,
  `cargo test -p rocketmq-classic-protocol --all-features --locked` passes all
  115 unit tests, integration targets, and 25 doctests; strict Protocol Clippy,
  pinned Maven verify (37 scenarios and fixture/cross-decode checks), format,
  and diff checks pass. The complete workspace P18-V02 gate must still be
  rerun on the final P18 revision.
- P18-V02 scoped response-header follow-up (2026-10-15): after removing
  structural equality from 16 Java-identity response headers,
  `cargo test -p rocketmq-classic-protocol --all-features --locked` passes 116
  unit tests, all integration targets, and 41 doctests; strict Protocol Clippy,
  Maven verify (37 scenarios and fixture/cross-decodes), format, and diff
  checks pass. The complete workspace gate remains pending on the final P18
  revision.
- Current-revision server-free P18-V02 follow-up (`0a7c94ca`,
  `8418d163`, 2026-10-15): after reconciling the 31 modeled request headers
  with `RpcRequestHeader` equality/hash behavior and updating their call sites,
  default workspace tests under UTC and all-feature workspace tests pass.
  Strict all-target/all-feature workspace Clippy, Maven verify (37 scenarios
  and Java/Rust cross-decodes), comparator self-test and manifest validation,
  race/leak smoke (16 cases × 3), fuzz smoke (104 frame seeds, 10 message
  seeds, 1,824 mutations), release examples, workspace benchmark compilation,
  locked Cargo metadata, metadata/license/dependency checks, seven package
  listings, UTC/Asia-Taipei near-time ClientID differential, formatting, and
  diff checks pass. This closes the server-free P18-V02 gates on these commits
  only; broad P18-V01, current-revision GitHub Ubuntu evidence, P18-V04 CI
  evidence, and Phase 19 Broker/Docker scenarios remain open.

- Current-revision server-free P18-V02 run (`cfed06f6`, 2026-10-15): default
  and all-feature workspace tests, strict all-target/all-feature Clippy, pinned
  Maven `verify` (37 scenarios and fixture/cross-decodes), differential
  comparator self-test and 37-scenario manifest, deterministic race/leak smoke
  (16 cases × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824
  mutations), release examples, workspace benchmark compilation, locked Cargo
  metadata, workspace metadata/license/dependency checks, seven package
  listings, Asia/Taipei unique-ID regression, formatting, and diff checks pass.
  The direct `cloneMessage` alias behavior is covered in this run. Broad
  P18-V01, current-revision GitHub Ubuntu/P18-V04 CI evidence, Phase 19 local
  Broker/Docker automation, and pre-publication version/registry resolution
  checks remain open.

- P18-V01 Common API reverse-scan follow-up (`fcab764e`, 2026-10-15): the
  pinned Java `MessageExt` inherits `Message` methods and exposes no
  `message()`/`message_mut()` composition accessors. Rust removes those two
  extra public methods and migrates internal callers to inherited methods via
  `Deref`/`DerefMut`; compile-fail doctests guard both removed boundaries.
  Default and all-feature workspace tests, strict all-target/all-feature
  Clippy, formatting, and diff checks pass. This closes the surfaced
  `MessageExt` reverse-export discrepancy only; P18-V01 remains open for the
  complete Java/Rust source, overload/input/error/flow, inherited API, and
  reverse-export audit.

- P18-V02 fixture isolation follow-up (`21f13891`, 2026-10-15): a final
  all-features workspace rerun exposed interference between the message
  constants fixture and the message API fixture, which temporarily mutates
  Java's process-wide reserved-property set. A test mutex now serializes those
  two fixtures. The focused 19-test message suite, default and all-features
  workspace suites, strict all-target/all-feature Clippy, formatting, and diff
  checks pass on the corrected test harness.
- Current-revision server-free P18-V02 rerun (`c8bdae9d`, 2026-10-15):
  default and all-feature workspace tests, strict all-target/all-feature
  Clippy, Maven verify (37 scenarios and Java/Rust cross-decodes), comparator
  self-test and 37-scenario manifest, deterministic race/leak smoke (16 cases
  × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations),
  release examples, workspace benchmark compilation, locked Cargo metadata,
  workspace metadata/license/dependency checks, all seven package listings,
  the `Asia/Taipei` unique-ID fixture, formatting, and diff checks pass.
  P18-V02 server-free gates are complete on this revision. P18-V01, current
  GitHub Ubuntu/P18-V04 CI evidence, Phase 19 Broker/Docker automation, and
  pre-publication version/registry resolution checks remain open.
- Current-revision server-free P18-V02 rerun (`78bf82ca`, 2026-10-15), after
  the Netty frame-limit parity change: default and all-feature workspace tests,
  strict all-target/all-feature Clippy, Maven verify (37 scenarios and
  Java/Rust cross-decodes, including default/configured frame-limit probes),
  comparator self-test and 37-scenario manifest, deterministic race/leak smoke
  (16 cases × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824
  mutations), release examples, workspace benchmark compilation, locked Cargo
  metadata, workspace metadata/license/dependency checks, all seven package
  listings, unique-ID fixture tests under UTC and Asia/Taipei, formatting, and
  diff checks pass. P18-V02 server-free gates are complete on `78bf82ca`.
  Broad P18-V01, current GitHub Ubuntu/P18-V04 CI evidence, Phase 19 local
  Broker/Docker automation, and pre-publication version/registry resolution
  checks remain open.
- Common `MessageExt` source-surface sweep (2026-10-15): all public Java
  declarations, including both socket-address buffer helpers and inherited
  `Message` behavior, have Rust owner mappings and invoked fixture coverage in
  the Phase 8 matrix. `cargo test -p rocketmq-classic-common --test
  message_decode --locked message_ext_` passes all four matching fixture
  tests. No additional gap surfaced in this class inventory; this closes one
  bounded P18-V01 audit slice only.
- `MessageClientExt` decoded offset-ID correction (`e15f0417`, 2026-10-15):
  wrapping client-decoded messages now preserves Java's physical offset ID
  separately from the live `UNIQ_KEY` getter, including setter and key-clear
  fallback behavior. Workspace default/all-feature tests and the current-tree
  server-free P18-V02 gates pass; broad P18-V01 remains open.
- Protocol header helper reverse-export correction (`60a62324`, 2026-10-15):
  parser helpers and the synthetic `ProofHeader` used only by tests are no
  longer public. Remoting codec and fuzz tests use the Java-mapped
  `GetRouteInfoRequestHeader`; a compile-fail doctest guards the removed exports.
  Default/all-feature workspace tests, strict all-target/all-feature Clippy,
  formatting, and diff checks pass. This closes those header exports only;
  broad P18-V01 remains open.
- Physical message-ID decoder parity (`fa6c0ff0`, 2026-10-15): Producer, Push,
  and Pull `view_message` now use the Java-mapped Common decoder. Pinned Java
  `UtilAll.string2bytes` turns an invalid nibble into `0xFF`; scripted facade
  tests mutate the physical ID's final nibble to `G` and verify `VIEW_MESSAGE_BY_ID`
  with offset `255`, including the Producer's later unique-key fallback. The
  duplicate Core parser and Rust-only DTO/errors are removed with a compile-fail
  export check. Default/all-feature workspace tests, strict Clippy, Maven verify
  (37 scenarios/cross-decodes), comparator/manifest, race/leak (16 × 3), fuzz
  smoke (1,824 mutations), release examples, benchmark compilation, metadata,
  format, and diff checks pass on this revision. This closes only the decoder
  and view-path slice; broad P18-V01 and current-revision GitHub Ubuntu/P18-V04
  evidence remain open.

- Netty decoder frame-limit setting (`754d5050`, corrected by `a06853ba`,
  2026-10-15): Rust reads the Java `com.rocketmq.remoting.frameMaxLength`
  equivalent when creating its first connection, defaults to `16777216`, and
  applies the selected limit to inbound decoding only. The pinned Java
  `NettyEncoder` has no such limit; Rust allows outbound frames above the
  configured inbound limit while respecting the signed Java length field.
  Separate Java JVM probes verify default/configured `4096` decoder limits and
  that the Java encoder emits a larger frame; a Rust child-process loopback
  test verifies the same inbound/outbound behavior.
  PASS: `cargo test -p rocketmq-classic-remoting --locked` (84 unit, 6 codec,
  1 fuzz regression, 5 security, 86 transport tests); strict all-target/all-
  feature Remoting Clippy; Maven verify (37 scenarios, both frame-limit probes,
  and Rust-to-Java cross-decodes); format and diff checks. This closes the
  frame-limit setting slice only; broad P18-V01 remains open.

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
  metadata reports 208 external crates, all from crates.io registry sources
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

- Remoting codec API reverse-export correction (`c27b60e4`, 2026-10-15):
  public frame and header encoders no longer accept caller-selected size bounds;
  public frame decoding no longer exposes a per-call bound, and Java's private
  16 MiB default is crate-private. The live connection decoder still applies
  the Java property to inbound frames only; outbound framing keeps the signed
  Java length bound. Compile-fail doctests guard the removed signatures and
  constant, and an internal unit test preserves bounded transport behavior.
  This closes the remoting codec API disposition slice only; broad P18-V01
  remains open.
- Current-revision server-free P18-V02 rerun on `c27b60e4` (2026-10-15): all
  server-free P18-V02 gates pass after the codec API change, including default
  and all-feature workspace suites, strict Clippy, Maven verify (37 scenarios
  and cross-decodes), differential self-test/manifest, race/leak smoke,
  fuzz smoke, examples, benchmark compilation, metadata/package checks,
  UTC/Asia-Taipei differential, formatting, and diff checks. P18-V01, current
  GitHub Ubuntu address-profile/P18-V04 evidence, Phase 19 local Docker/Broker
  automation, and pre-publication version/registry checks remain open.

- Current-revision P18-V02 server-free rerun (`d165ba45`, 2026-10-15):
  formatting, strict all-target/all-feature workspace Clippy, UTC workspace
  tests, all-feature workspace tests, Maven verify (37 scenarios and Java/Rust
  cross-decodes including frame-limit probes), differential self-test and
  manifest validation, deterministic race/leak smoke (16 cases × 3), fuzz
  smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release examples,
  workspace benchmark compilation, locked Cargo metadata, workspace
  metadata/license/dependency checks, seven package listings, and the Java
  unique-ID fixture under UTC and Asia/Taipei all pass. `git diff --check`
  passes. This closes P18-V02 server-free gates on `d165ba45`; P18-V01,
  current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker
  automation, and pre-publication version/registry resolution remain open.

- Current-worktree P18-V02 rerun (`95548e49`, 2026-10-15): after the live
  `clientCloseSocketIfTimeout` setting change, default and all-feature workspace
  tests, strict workspace Clippy, Maven verify (37 scenarios plus the Java
  live-config probe), differential self-test/manifest, deterministic race/leak
  smoke (16 cases × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824
  mutations), release examples, benchmark compilation, locked Cargo metadata,
  workspace metadata/license/dependency checks, seven package listings, UTC
  and Asia/Taipei unique-ID fixture runs, formatting, and `git diff --check` all
  pass. This closes P18-V02 server-free gates on `95548e49`; broad P18-V01,
  current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker
  automation, and pre-publication version/registry resolution remain open.

- P18-V01 Remoting callback executor timing slice (`9881c230`, 2026-10-15):
  `disableCallbackExecutor` is now read live for the callback-executor getter
  and callback completion dispatch. Setting a custom executor while disabled
  retains it; the getter remains hidden and dispatch runs inline until the
  setting is re-enabled. The Rust regression exercises both completion paths
  and executor identity; Maven's pinned Java live-config probe verifies the
  matching getter and retention behavior. This closes that remoting setting
  slice only; the broad P18-V01 audit remains open.
- P18-V01 Remoting GO_AWAY reconnect timing slice (`f506067d`, 2026-10-15):
  the retry decision now reads the live `enableReconnectForGoAway` value when a
  GO_AWAY response arrives, with a Rust post-start mutation regression and the
  Java `RemotingLiveConfigProbe`. The current-revision server-free gates on
  `9881c230` revalidate this slice. The Phase 3 inventory now records the live
  setting adapter; this closes the setting row only.
- PASS on `9881c230`: formatting, strict all-target/all-feature workspace
  Clippy, UTC default and all-feature workspace suites, full Remoting tests,
  Maven verify (37 scenarios and live remoting config probe), differential
  self-test/37-scenario manifest, deterministic race/leak smoke (16 cases × 3),
  fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release
  examples, benchmark compilation, workspace metadata/license/dependency
  checks, seven package listings, Asia/Taipei unique-ID differential and diff
  checks. Locked Cargo metadata reports 208 external packages, all registry
  sourced with license metadata.
- P18-V01 Remoting NameServer scan timing slice (`6ffccd1b`, 2026-10-15):
  `isScanAvailableNameSrv` now has a live atomic Rust adapter, read by `start()`
  when deciding whether to schedule reachability scans. A Rust loopback test
  and the Java `RemotingLiveConfigProbe` both change the value after client
  construction and before `start()`, then verify the scan connects. This
  closes that setting row only; scan cadence/topology and broad P18-V01 remain
  separately described in the remoting inventory.
- PASS on `6ffccd1b`: UTC default and all-feature workspace tests, strict
  all-target/all-feature Clippy, Maven verify (37 scenarios, live remoting
  config probe and Java/Rust cross-decodes), differential self-test and
  37-scenario manifest, deterministic race/leak smoke (16 cases × 3), fuzz
  smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release examples,
  benchmark compilation, locked Cargo metadata, metadata/license/dependency
  checks, seven package listings, Asia/Taipei unique-ID differential, format,
  and diff checks all pass. Full Remoting results include 85 unit, 6 codec,
  1 fuzz regression, 5 security, 90 transport, and 5 doctest cases.
- P18-V01 Remoting worker-count timing slice (`e7e5728a`, 2026-10-15):
  `clientWorkerThreads` now has a live atomic Rust adapter, read at `start()`
  when creating request workers. The Rust regression covers post-construction
  mutation, startup validation and worker count; the Java live-config probe
  mutates `NettyClientConfig` after client construction and checks the executor
  group created by `start()`. This closes that setting row only.
- PASS on `e7e5728a`: UTC default and all-feature workspace tests, strict
  all-target/all-feature workspace Clippy, Maven verify (37 scenarios, Java
  fixtures, live remoting config probe and Java/Rust cross-decodes), comparator
  self-test/37-scenario manifest, deterministic race/leak smoke (16 cases × 3),
  fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release
  examples, benchmark compilation, locked Cargo metadata, workspace
  metadata/license/dependency checks, seven package listings, Asia/Taipei
  unique-ID differential, format and diff checks all pass.
- Remaining non-manual work: complete broad bidirectional P18-V01 source/API,
  setting, input/error/control-flow, inherited API and reverse-export
  reconciliation; obtain current-revision GitHub Ubuntu address-profile
  evidence for P18-V04; and implement/run Phase 19 local Docker/Broker
  automation. Confirm the release version and registry dependency resolution
  before publication. Owner-run deployment/live Broker checks remain separate
  Phase 19 manual validation.
- P18-V01 Remoting `connectTimeoutMillis` Bootstrap timing slice (`aca955cd`,
  2026-10-15): direct connections retain the value captured by `start()`;
  each matched proxy CIDR captures the live timeout on first use and reuses
  that cached value, while later CIDRs read the newer value. Reachability scans
  use the selected path's connection timeout without imposing a second
  operation deadline; zero disables the separate connection cap. Rust tests
  cover direct start-time capture, proxy first-use/cache behavior, scan timing,
  and zero-timeout scanning. Java `RemotingLiveConfigProbe` verifies the direct
  Bootstrap and first/reused/new-CIDR proxy Bootstraps.
- PASS on `aca955cd`: Remoting all-feature tests (88 unit, 6 codec, 1 fuzz
  regression, 5 security, 93 transport, 5 doctests), strict Remoting
  all-target/all-feature Clippy, formatting, Maven `verify` (37 scenarios,
  live-config probe and cross-decodes), and `git diff --check`.
- This closes the `connectTimeoutMillis` Bootstrap timing row only. Remaining
  work includes broad bidirectional P18-V01 reconciliation (including the
  `clientChannelMaxIdleTimeSeconds` and `disableNettyWorkerGroup` timing rows),
  full workspace P18-V02 gates on the eventual final revision, current-revision
  GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  release version/registry dependency checks. Owner-run deployment/live Broker
  checks remain Phase 19 manual validation.

- P18-V01 Remoting Bootstrap-option timing (`7b00a52f`, 2026-10-15): socket
  send/receive buffers now use live adapters; direct options are captured by
  `start()`, while proxy values are captured and cached per CIDR on first use.
  Direct watermarks and the Rust frame-pool switch are also captured at start.
  Proxy connections retain default watermarks and disable the frame pool,
  matching options omitted by Java's proxy Bootstrap. Rust unit coverage and
  the Java `RemotingLiveConfigProbe` verify direct, cached-proxy, and new-CIDR
  behavior.
- PASS on `7b00a52f`: Remoting all-feature tests (91 unit, 6 codec, 1 fuzz
  regression, 5 security, 95 transport, 5 doctests), strict Remoting
  all-target/all-feature Clippy, formatting, Maven `verify` (37 scenarios,
  live-config probe and Java/Rust cross-decodes), and `git diff --check`.
  The frame-limit child test was updated to call `start()` before its internal
  connection helper; the initial failure was test setup, and the targeted
  regression passed after cleanup was added.

- P18-V01 Remoting `useTLS` setting timing (`04c219dc`, 2026-10-15): the TLS
  context remains captured at construction, while the live `useTLS` adapter is
  sampled for each new connection before handshake. Existing channels retain
  their mode; enabling TLS without a captured context leaves new connections
  plain. Rust loopback tests cover post-start mutation and absent-context
  behavior; Maven `RemotingLiveConfigProbe` verifies Java's post-start disable
  on a newly created channel.
- PASS on `04c219dc`: Remoting all-feature tests (91 unit, 6 codec, 1 fuzz
  regression, 5 security, 97 transport, 5 doctests), strict Remoting
  all-target/all-feature Clippy, `cargo fmt --all -- --check`, Maven `verify`
  (37 scenarios, live-config probe and Java/Rust cross-decodes), and
  `git diff --check`.
- This closes the Remoting `useTLS` setting-timing row only. Broad P18-V01,
  final-revision full-workspace P18-V02, current-revision GitHub Ubuntu/P18-V04,
  Phase 19 local Docker/Broker automation, and release version/registry checks
  remain open. Owner-run deployment/live Broker checks remain Phase 19 manual
  validation.

- Current-revision server-free P18-V02 rerun on code revision `04c219dc`
  (2026-10-15; documentation-only follow-up `44d69079`): UTC default and
  all-feature workspace tests, strict all-target/all-feature Clippy, Maven
  `verify` (37 scenarios, live Remoting config probe and Java/Rust cross-decodes),
  comparator self-test and 37-scenario manifest, 16 race/leak cases × 3,
  fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations), release
  examples, workspace benchmark compilation, full locked Cargo metadata,
  workspace metadata/license/dependency checks, seven package listings, and
  unique-ID fixture checks under UTC and Asia/Taipei all pass. Formatting and
  diff checks pass. The 208 external Cargo packages are registry-sourced and
  carry license metadata.
- This closes the current-revision P18-V02 server-free gate. Broad P18-V01,
  current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker
  automation, and release version/registry-resolution checks remain open.
  Owner-run deployment/live Broker checks remain Phase 19 manual validation.

- P18-V01 Remoting callback/channel API slice (`a74171df`, 2026-10-15): Rust
  callbacks now receive a Java-shaped `ResponseFuture` with mutable response
  state, Java error-category selection, callback-once execution and owned
  permit release. Channel event callbacks receive a `ChannelHandle` that can
  inspect, close and write through its connection; closing a pending handle
  cancels connection setup. Callback timeout metadata now follows the Java
  success and exceptional-wrapper paths, and result closures receive connect
  failures exactly once. This closes these bounded Remoting API/flow rows only.
- PASS on `a74171df`: Remoting all-feature tests (96 unit, 6 codec, 1 fuzz
  regression, 5 security, 99 transport, 5 doctests); Core all-feature tests
  (159 unit and all integration/doctests); strict workspace all-target/all-
  feature Clippy; Maven `verify` (37 scenarios, Java/Rust cross-decodes and
  live Remoting config probe); format and diff checks.
- Broad P18-V01, the final full-workspace P18-V02 rerun, current-revision GitHub
  Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and release
  version/registry-resolution checks remain open. Owner-run deployment/live
  Broker checks remain Phase 19 manual validation.

- P18-V01 Remoting NameServer live-list slice (`a5ef99ae`, 2026-10-15): Java
  returns the nullable backing list from `getNameServerAddressList()` and later
  selection/scanning observes caller mutations. Rust now preserves `None` versus
  `Some(empty)` and provides `with_name_server_address_list` for lock-scoped
  mutation of the stored list. `p18_nameserver_getter_exposes_live_nullable_list_with_lock_scope`
  checks live mutation, getter state, empty-update behavior, operation behavior,
  and replacement. Remoting all-feature tests pass (96 unit, 6 codec, 1 fuzz
  regression, 5 security, 100 transport, 5 doctests); strict Remoting Clippy,
  formatting and `git diff --check` pass. Phase 3 inventory also now reflects
  the `ResponseFuture` and `ChannelHandle` callback API slice from `a74171df`.
  This closes only the listed Remoting rows; broad P18-V01, current-revision
  full-workspace P18-V02, GitHub Ubuntu/P18-V04 evidence, Phase 19 local
  Docker/Broker automation, and release version/registry-resolution checks
  remain open. Owner-run deployment/live Broker checks remain manual.

- Current-revision server-free **P18-V02** gates on `ae08e42d` (2026-10-15):
  default and all-feature workspace tests, strict all-target/all-feature
  Clippy, pinned Maven `verify` (37 scenarios and Java/Rust cross-decodes),
  differential self-test and 37-scenario manifest, race/leak smoke (16 cases
  × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations),
  release examples, workspace bench compilation, locked Cargo metadata,
  workspace metadata/license/dependency checks, package listings, UTC default
  fixture tests and the unique-ID fixture under Asia/Taipei all pass. Rustfmt
  and `git diff --check` pass. This closes server-free P18-V02 only on
  `ae08e42d`; any later implementation revision needs another full rerun.
  Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19
  local Docker/Broker automation, and release version/registry-resolution
  checks remain open. Owner-run deployment/live Broker checks remain manual.

- Current-tree server-free **P18-V02** rerun after the ClientConfig visibility
  slice (2026-10-15; `fc8ce548` plus the tested worktree): default and
  all-feature workspace tests, workspace check, strict all-target/all-feature
  Clippy, Maven verify (37 scenarios and Java/Rust cross-decodes), comparator
  self-test and 37-scenario manifest, deterministic race/leak smoke (16 cases
  × 3), fuzz smoke (104 frame seeds, 10 message seeds, 1,824 mutations),
  release examples, workspace benchmark compilation, locked Cargo metadata,
  workspace metadata/license/dependency checks, all seven package listings,
  UTC and Asia/Taipei unique-ID tests, formatting, and diff checks pass. This
  closes server-free P18-V02 on the tested tree. Broad P18-V01, current-revision
  GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  pre-publication version/registry-resolution checks remain open. Owner-run
  deployment/live Broker checks remain manual validation.

- P18-V01 Common `MessageDecoder` overload/body-null slice (2026-10-15): the
  Java 5.5.1 fixture now invokes every generic `decode` overload and both
  `clientDecode` variants against eleven records, including an empty body.
  Rust now preserves Java's null body when body reading is disabled or the
  record body is empty, and preserves a null properties map when the record has
  no properties section. The Common crate suite passes (21 unit tests, 20
  `message_decode` integration tests, one topic-validator test, and 17 doctests),
  as do strict Common Clippy and the full offline Maven verify (37 scenarios,
  Java/Rust cross-decodes and fixture checks). This closes this decoder slice
  only; broad P18-V01 remains open. At this checkpoint, full-workspace P18-V02
  gates still needed a rerun; the rerun is recorded below. Current GitHub Ubuntu/P18-V04 evidence,
  Phase 19 local Docker/Broker automation, and release version/registry checks
  remain open. Owner-run deployment/live Broker checks remain Phase 19 manual
  validation. Current message fixture SHA-256:
  `761ffe884d0a7c907bb694969ce0730fbb60390cc1fa92151c310f6894483dfd`.

- Current-revision server-free **P18-V02** gates on `f14d7c6a` (2026-10-15):
  default and all-feature workspace tests; workspace all-target check; strict
  all-target/all-feature Clippy; pinned offline Maven verify (37 scenarios,
  Java/Rust cross-decodes and fixture checks); differential self-test and
  37-scenario manifest; deterministic race/leak smoke (16 cases × 3); fuzz
  smoke (104 frame seeds, 11 message seeds, 1,840 bounded mutations); release
  examples; workspace bench compilation; locked Cargo metadata; workspace
  metadata/license/dependency checks; seven package listings; Asia/Taipei
  unique-ID fixture; rustfmt and diff checks all pass. This closes server-free
  P18-V02 on `f14d7c6a`. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04
  evidence, Phase 19 local Docker/Broker automation, and release version/
  registry-resolution checks remain open. Owner-run deployment/live Broker
  checks remain Phase 19 manual validation.

- P18-V01 Remoting private-header narrowing (`c0878056`, 2026-10-15): pinned
  Java 5.5.1 `RocketMQSerializable` writes request code and version with
  `writeShort`, retaining the low 16 bits. The Rust private-header encoder now
  matches this behavior. The new invoked Java fixture uses `987654` for both
  fields and records `4614` after decode; Rust encoding matches the complete
  Java frame byte-for-byte. Before the fix, the regression failed with
  `FieldTooWide("code")`. Remoting all-feature tests pass (96 unit, 7 codec,
  1 fuzz regression, 5 security, 100 transport and 5 doctests); strict
  all-target/all-feature Remoting Clippy, Maven verify (37 scenarios),
  formatting, diff checks and fixture regeneration comparison pass. This
  closes the private-header narrowing slice only; broad P18-V01, current
  GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation and
  release version/registry resolution remain open. Fixture SHA-256:
  `033e89778d92def6f5e1875211ea4ca7b59facaf813a72a57de8c386c5e4acf8`.

- P18-V01 Remoting private key-length narrowing (`c62e0740`, 2026-10-15):
  pinned Java 5.5.1 `RocketMQSerializable.writeStr(true, key)` stores the UTF-8
  key length with `setShort`. A 32,768-byte key therefore writes `0x8000`
  while retaining all key bytes. Rust now matches the emitted Java frame;
  before the fix its encoder returned `FieldTooWide("ext field key")`. The
  direct Java fixture lives under `fixtures/remoting/edge-cases/` because its
  signed length is negative and the case verifies encoding. Remoting
  all-feature tests pass (96 unit, 8 codec, 1 fuzz regression, 5 security,
  100 transport and 5 doctests); strict Remoting Clippy, Maven verify (37
  scenarios), formatting, diff checks and fresh-fixture comparison pass.
  This closes the private key-length slice only; broad P18-V01, current GitHub
  Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation and release
  version/registry resolution remain open. Fixture SHA-256:
  `7af64d29f396b2e7a1dd70a57066920890559fce1f170844ca8d4e1ba3a453ca`.

- P18-V01 Common formatted-string reverse map (2026-10-15): the Phase 8
  Rust-to-Java table now lists `Message`/`MessageExt` `Display` and blanket
  `ToString` as the public mapping for Java `toString()`. It no longer lists
  private `java_to_string` implementation helpers as public exports. This is
  a documentation correction only; broader inherited API and reverse-export
  reconciliation remains open.

- Current-revision server-free **P18-V02** gates on code revision `ae416072`
  (tested at HEAD `a6f3010c`, whose only later change is this run's execution
  log; 2026-10-15): UTC default and all-feature workspace tests pass when run
  serially; strict all-target/all-feature Clippy and workspace all-target
  check pass. Two concurrent full test processes initially collided on shared
  offset-store fixture paths (`OffsetStore(AlreadyOpen)`); both suites pass on
  the serialized rerun. Pinned Maven `verify` passes (37 scenarios and
  Java/Rust cross-decodes); comparator self-test and 37-scenario manifest pass;
  deterministic race/leak smoke passes (16 cases × 3); fuzz smoke passes (105
  frame seeds, 11 message seeds, 1,856 bounded mutations); release examples and
  workspace benches compile; locked metadata reports seven workspace crates
  and 208 crates.io dependencies; metadata/license/dependency checks and all
  seven package listings pass with LICENSE/NOTICE; the unique-ID fixture passes
  under UTC and Asia/Taipei; rustfmt and `git diff --check` pass. This closes
  server-free P18-V02 on the tested code. Broad P18-V01, current-revision
  GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  release version/registry-resolution checks remain open. Owner-run deployment
  and live Broker checks remain Phase 19 manual validation.

- P18-V01 `MessageClientExt` constructor API (`8b91e933`, 2026-10-15): pinned
  Java 5.5.1 reflection confirms exactly one public no-argument constructor.
  Rust `MessageClientExt::new()` and `Default` provide that construction path;
  the Rust-only `new(MessageExt)` and `From<MessageExt>` conversion are removed.
  Compile-fail doctests guard both absent conversion surfaces, and the invoked
  Java fixture checks the public constructor count/signature. The constructor
  parity slice passes Common all-feature tests (21 unit, 20 message-decode
  integration, 1 topic-validator and 20 doctests), strict Common Clippy,
  Maven verify (37 scenarios), formatting and diff checks. Fixture SHA-256:
  `cd7329a631de04393ceb4fdb7eff701ceec05728213605e9efd07eceaf184ccb`.
  This closes the constructor slice only; broad P18-V01 remains open, and the
  full-workspace P18-V02 gate must be rerun after the remaining code changes.

- P18-V01 Common public inherent-method crosswalk (`e4593202`, 2026-10-15): a
  re-export-aware Rustdoc JSON name sweep covered 26 public aggregate types.
  The Common compatibility matrices now explicitly map mutable body access,
  nullable property lookup/user-property validation, `MessageHostAddress`
  adapters, `MessageId` setters, and the Java `TopicValidator` mutable-set
  getters. The scan found no public inherent method name absent from the
  compatibility docs; it does not close trait/inherited API or input/error/
  control-flow reconciliation. Common all-feature tests (21 unit, 20
  message-decode integration, 1 topic-validator and 20 doctests), strict Common
  Clippy, formatting and diff checks pass.

- P18-V01 `MessageExt` constructor parity (`7665b769`, 2026-10-15): the pinned
  Java fixture now reflects both public constructors: no-argument and
  `(int, long, SocketAddress, long, SocketAddress, String)`. Rust `new()` and
  `Default` map to the no-argument constructor; the prior public
  `new(Message)` conversion was removed. Common retains a crate-private
  `from_message` helper for decoder internals, while Consumer/Core product paths
  construct through `new()` and inherited setters.
  A compile-fail rustdoc example protects the absent conversion API. This
  closes the constructor slice only; broad P18-V01 remains open. Fixture SHA-256:
  `9400beefa451ed1e29cdda0cb1171387aacd6929d5e2a144de3a837f6ca840e0`.

- Current-worktree server-free P18-V02 rerun at `bb93560e` (2026-10-15):
  workspace tests pass with default and all features; workspace all-target
  check and strict all-target/all-feature Clippy pass; pinned Maven `verify`
  passes all 37 scenarios and fixture drift checks. Differential self-test and
  37-scenario manifest validation pass. Offline race/leak smoke passes 16 cases
  × 3; fuzz smoke passes 105 frame seeds, 11 message seeds and 1,856 bounded
  mutations. Release examples, workspace benches, locked Cargo metadata,
  workspace metadata/license/dependency checks and package listings pass; all
  seven package contents include LICENSE/NOTICE. The Java-backed Unique ID
  fixture passes under `Asia/Taipei`; the full workspace runs used the host UTC
  timezone. Formatting and diff checks pass. This closes local server-free
  P18-V02 only. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 local Docker/Broker automation, and pre-publication version/registry
  resolution remain open. Owner-run deployment/live Broker checks remain
  Phase 19 manual validation.

- P18-V01 TLS decryptor registration (`d163a143`, 2026-10-15, pinned Java
  source `efa1be59d6fc747ad220f036c86030f05f0419f9`): Rust now exposes the
  Java-shaped process-wide `register_decryption_strategy` API; the callback
  takes a key path and client/server role, and null registration makes a later
  key load fail. Invoked Java fixtures pin the public nested interface,
  registration method, callback signature, default strategy, callback values,
  and null behavior. TLS configuration no longer exposes per-config strategy
  injection. This closes the decryptor registration API slice only. Java
  property-file reload timing and broad P18-V01 reconciliation remain open.
  TLS fixture SHA-256 values: defaults
  `1ef41b86ef30bb65c912889b091924c7c15390a460eaf0c74aef6bd10e539db1`,
  system properties
  `c0267cd68a2dd32cb6adb9b9bdc17d6340220928d2090d970a5223df9de8b5a7`.

- Current-revision server-free P18-V02 rerun on code revision `d163a143`
  (2026-10-15): default and all-feature workspace tests, all-target/all-feature
  workspace check and strict Clippy, Maven `verify` (37 scenarios and fixture
  checks), comparator self-test and 37-scenario manifest, deterministic
  race/leak smoke (16 cases × 3), fuzz smoke (105 frame seeds, 11 message
  seeds, 1,856 bounded mutations), release examples, facade offline benchmark
  compilation, locked Cargo metadata, workspace metadata/license/dependency
  checks, seven package listings with LICENSE/NOTICE, Asia/Taipei Unique ID
  fixture, formatting, and diff checks pass. This closes local server-free
  P18-V02 only. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 local Docker/Broker automation, and pre-publication version/registry
  resolution remain open. Owner-run deployment/live Broker checks remain
  Phase 19 manual validation.

- P18-V01 TLS property-file reload timing (`83afee2a`, 2026-10-15, pinned
  Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the invoked Java
  fixture changes `tls.test.mode.enable` across three context builds and
  confirms `true → false → true`; it also confirms that an omitted
  `tls.client.authServer` property retains its previous value. Rust reloads a
  retained file source immediately before each new RemotingClient builds its
  TLS connector; an existing client keeps its constructed settings. Clones of
  one `TlsConfig` share the last-loaded values. Java's TLS values are
  process-static, while separately created Rust `TlsConfig` sources retain
  independent explicit property maps; that cross-config boundary stays in the
  broad P18-V01 audit. The Rust differential
  `java_tls_property_file_is_reloaded_for_each_client_context` matches the
  fixture. Updated TLS fixture SHA-256 values: defaults
  `c5db30257df68c1e1926a811549bb534210a4e3a70df95aed1cde3195c764528`,
  system properties
  `099916a84c78f2b5b1f4b434eec904cc556779271e43db39831ae7208f7d8951`.

- Current-revision server-free P18-V02 rerun on code revision `83afee2a`
  (2026-10-15): default and all-feature workspace tests, all-target/all-feature
  workspace check and strict Clippy, Maven `verify` (37 scenarios and fixture
  checks), comparator self-test and 37-scenario manifest, deterministic
  race/leak smoke (16 cases × 3), fuzz smoke (105 frame seeds, 11 message
  seeds, 1,856 bounded mutations), release examples, offline benchmark
  compilation, locked Cargo metadata, workspace metadata/license/dependency
  checks, seven package listings with LICENSE/NOTICE, Asia/Taipei Unique ID
  fixture, formatting, and diff checks pass. This closes local server-free
  P18-V02 only. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 local Docker/Broker automation, and pre-publication version/registry
  resolution remain open. Owner-run deployment/live Broker checks remain
  Phase 19 manual validation.

- P18-V01 TLS process-wide property state (`2d16427f`, 2026-10-15, pinned Java
  source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the invoked Java fixture
  builds contexts from two successive property files; the second omits
  `tls.client.authServer` and retains `true` from the first. Rust's
  Java-compatible property sources now share retained values process-wide in
  normal builds, so distinct `TlsConfig` sources inherit omitted settings;
  existing RemotingClient instances keep their constructed connector. The
  new `tls_property_file_settings_are_process_wide_across_config_sources`
  integration test compares both property values with Java. This closes the
  TLS cross-source state slice only; broad P18-V01 remains open. TLS fixture
  SHA-256 values: defaults
  `48e1ac15883eaa795a7c3a2eefbf71752c3a9415d1f01171348e0a8b4d9d48a6`,
  system properties
  `81a4325d704f37713033dc5a6d7f6a2c8ffe5f24aa8e49bececf9ca5c33acd4a`.
- PASS on `2d16427f`: Remoting all-feature tests (97 unit, 8 codec, 1 fuzz,
  6 security, 100 transport and 7 doctests); strict all-target/all-feature
  Remoting Clippy; Maven `verify` (37 scenarios and Java fixture checks),
  formatting and diff checks.
