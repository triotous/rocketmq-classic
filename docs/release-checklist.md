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
  Windows cross-target compile evidence. The CI workflow now includes a native
  Windows stack runtime job; its current-revision result and Broker-backed
  query checks remain pending. Plans remain unchanged.
- [ ] Close P18-V01: complete bidirectional Java source/API/input reconciliation
  and reverse-audit all Rust public APIs and settings. Remoting after-response
  hooks now mutate request/response commands on outbound and inbound paths;
  request-pipeline stages mutate requests before later stages/processors, with
  the mutated opaque verified through the response. Java fixture verification
  and the focused Rust regression pass. The Java `UtilAll.exceptionSimpleDesc`
  versus Rust error-remark difference is accepted as diagnostic text only under
  the core flow-parity rule; response code and control-flow paths are mapped,
  while text-for-text equivalence is not claimed.
  The broad Phase 6 audit
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
  Rust. Its `consumerRunningInfo` method now has direct Rust owner coverage
  during runtime and after shutdown; the latter compares retained properties,
  subscription/status state and shutdown-cleared queue tables with Java. Before
  startup, the Rust owner returns typed `NotStarted`; pinned Java throws
  `NullPointerException` for its absent consume service. Its
  `ConsumerRunningInfo` body and
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
- Common message equality reverse-surface slice (initial implementation): Java
  `Message` and `MessageExt` inherit identity-based `Object.equals`/`hashCode`;
  Rust equality used a hidden `MessageExt` identity token, while `Message` had
  no equality trait. The later 2026-10-16 identity/hash slice below supersedes
  this implementation and its compile-fail doctest. Default/all-feature
  workspace tests, strict workspace Clippy, Maven verify (37 scenarios and
  cross-decodes), formatting, and diff checks pass for this slice. The full
  P18-V02 rerun, including race/fuzz, example, benchmark, metadata, and
  package-content gates, remains required on the final revision. Broader
  Common/Core/API reconciliation remains open P18-V01 work.
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
- [ ] **P18-V04** — The 37-scenario handoff manifest and server-free local
  gates pass on code revision `de2d1a8f` (documentation HEAD `15f9705c`):
  default/all-feature workspace tests, strict workspace Clippy/check, release
  examples, offline benchmark compilation, locked metadata for all seven
  `0.1.0` packages, and package-content listings with `LICENSE`/`NOTICE`.
  Current-revision GitHub Ubuntu evidence for the Linux address profile and
  native Windows stack-runtime evidence are still absent, so P18-V04 remains
  open.

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
  Push fixture SHA-256 `fdc94afde6f4fdd74ffeab6fae12fad521a831178e070c4e072c25ef718a1f82`;
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
  `d11f8c88e44711cffbd574beab5946943b00a8d350fe05ec40ff1b744320f321`.

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
- P18-V01 TLS static-property initialization (`e314b5e1`, 2026-10-15, pinned
  Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the Java fixture
  initializes its TLS class state, then changes `tls.client.authServer` and
  verifies later contexts retain the original static value. Rust now seeds its
  process-wide property state only on first access, including when the first
  TLS mode selection is Plain; later maps cannot replace it, while subsequent
  file reloads still update present keys and retain omitted keys. The invoked
  Java fixture and Rust cross-source regression pass. Fixture SHA-256 values:
  defaults `4e1603df11cdb4054f8f3950ddc27cb5e219ad89f93d91071c9478d0d80ad648`,
  system properties `b4ca8ef0433b88014c28168be8bdb335ff93f2eeccae24f467ef5c72cb79c2b9`.
  Validation on the code commit: Remoting all-feature tests (97 unit, 8 codec,
  1 fuzz, 6 security, 100 transport, 7 doctests), strict all-target/all-feature
  Remoting Clippy, pinned Maven `verify` (37 scenarios), formatting and diff
  checks pass. This closes only the TLS initialization slice; broad P18-V01,
  final-revision workspace P18-V02, current-revision GitHub Ubuntu/P18-V04,
  Phase 19 local Docker/Broker automation, and pre-publication version/registry
  resolution remain open. Owner-run deployment and production checks remain
  Phase 19 manual validation.

- Current-revision server-free **P18-V02** gate on code revision `e314b5e1`
  (2026-10-15): `cargo test --workspace --locked` and
  `cargo test --workspace --all-features --locked` pass;
  `cargo check --workspace --all-targets --all-features --locked` and strict
  all-target/all-feature workspace Clippy pass; pinned Maven `verify` passes
  all 37 differential scenarios and fixtures; comparator self-test, release
  facade example build, formatting, and diff checks pass. This closes
  server-free P18-V02 on `e314b5e1`. Broad P18-V01, current-revision GitHub
  Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  pre-publication version/registry resolution remain open. Owner-run
  deployment and production checks remain Phase 19 manual validation.
- P18-V01 TLS mode selection from static properties (`746c35bc`, 2026-10-15):
  the Java oracle keeps `TlsSystemConfig.tlsEnable=false` after a later
  `tls.enable=true` system-property change, and ignores `tls.enable` in TLS
  files. Rust `TransportMode` now reads `tls.enable` from the one-time initial
  property snapshot; the default `tls.config.file` path uses that same
  snapshot. The cross-source regression reproduced the mismatch before the
  fix and passes now. Fixture SHA-256: defaults
  `223ed13d44d2a448b9c5193435a5dcb8c04940c42a9109196ad55eebeacbbc82`, system
  properties `67bab119ca6084783ee19bdca2badd2bd62fb9534a3b44382fcf16c5e918dba2`.
  Broad P18-V01 remains open.
- Current-revision server-free **P18-V02** gate on code revision `746c35bc`
  (2026-10-15): default and all-feature locked workspace tests, strict
  all-target/all-feature workspace Clippy, all-target workspace check, pinned
  Maven `verify` (37 scenarios and fixture checks), differential self-test,
  release facade example build, formatting, and diff checks pass. This closes
  server-free P18-V02 on `746c35bc`. Broad P18-V01, current-revision GitHub
  Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  pre-publication version/registry resolution remain open. Owner-run
  deployment and production checks remain Phase 19 manual validation.
- P18-V01 request/reply unregister flow (`2026-10-15`, pinned Java source
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): Java
  `MQClientInstance.unregisterProducer` removes the producer table entry and
  unregisters it with Brokers; it does not clear
  `RequestFutureHolder.getRequestFutureTable()`. Rust lease-based producer
  unregister now preserves pending correlations, matching group-based
  unregister and the Java path. Core registry and inbound regressions verify
  that an outstanding correlation accepts a late reply after unregister.
  Rust future-drop cancellation maps to Java synchronous request interruption
  and its `finally` removal; explicit cancellation maps to removing an entry
  from Java's public live correlation map. Core tests, Producer tests, strict
  Core Clippy, formatting, and diff checks pass. This closes normal unregister
  and cancellation mappings only. Rust `ProducerShutdown` on startup-failure
  registry clear remains a ClientRuntime-only path for the broad P18-V01
  reverse audit.

- P18-V01 oneway completion boundary (19989176, 2026-10-15; pinned Java
  source efa1be59d6fc747ad220f036c86030f05f0419f9): Rust invoke_oneway
  now returns only call-boundary errors while the client owns scheduled
  transmission; later connection, admission, and socket-write outcomes are not
  returned, matching Java's caller-visible boundary. The Core call sites and
  scripted transport expectations were updated, and the inventory, ADR, and
  compatibility overview now describe the same contract. Remoting tests pass
  (100 transport tests and 7 doctests); Core tests pass (159 unit tests, every
  integration target, and 13 doctests); strict all-target/all-feature Clippy
  passes for both crates, as do format and diff checks. This closes the scoped
  oneway result-surface difference only. Broad P18-V01 remains open, including
  the pre-start ClientRuntime pending-reply ProducerShutdown path. The full
  final-revision P18-V02 workspace rerun and current-revision GitHub Ubuntu
  address-profile result remain outstanding.

- P15 request/reply startup rollback follow-up (182f8f69, 2026-10-15,
  pinned Java source efa1be59d6fc747ad220f036c86030f05f0419f9): reply
  registration now requires a Running ClientRuntime, so a failed startup cannot
  leave a request to receive a Rust-only ProducerShutdown completion.
  GroupRegistry clear drops internal senders, and the public
  RequestReplyShutdown error and ProducerShutdown completion variant are
  removed. The Phase 15 matrix marks this local lifecycle slice verified; the
  full P18-V01 reverse API audit remains open. cargo test for Core and Producer
  passes (Core: 160 unit tests, all integration targets, 13 doctests; Producer:
  142 unit tests, all integration targets, 6 doctests). Strict all-target,
  all-feature Clippy for both crates, format, and diff checks pass.

- Current-revision server-free **P18-V02** rerun on code revision `182f8f69`
  (2026-10-15): the default and all-feature workspace tests, all-target
  workspace check, strict all-target/all-feature Clippy, pinned Maven verify
  (37 scenarios and Java/Rust cross-decodes), comparator self-test and
  37-scenario manifest validation pass. The 16-case race/leak smoke passes at
  three repetitions; fuzz smoke covers 105 frame seeds, 11 message seeds, and
  1,856 bounded mutations; release examples and workspace benchmarks compile.
  Locked Cargo metadata, workspace metadata/license/dependency checks, all
  seven package listings (including LICENSE and NOTICE), and the Java-backed
  Unique ID differential under `TZ=Asia/Taipei` pass. Formatting and diff
  checks pass. This closes the server-free P18-V02 gate on `182f8f69`;
  P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19 local
  Docker/Broker automation, and pre-publication version/registry resolution
  remain open. Owner-run deployment and production checks remain Phase 19
  manual validation.

- Current-revision server-free **P18-V02** rerun on code revision `4c142334`
  (2026-10-15): on macOS ARM64, UTC default and all-feature workspace tests,
  all-target/all-feature workspace check and strict Clippy, pinned Maven verify
  (37 scenarios and Java/Rust cross-decodes), comparator self-test and
  37-scenario manifest validation all pass. Race/leak smoke passes (16 cases ×
  3); fuzz smoke passes (105 frame seeds, 11 message seeds, 1,856 mutations);
  release examples and workspace benchmark compilation pass. Locked metadata,
  workspace metadata/license/dependency checks, seven package listings with
  LICENSE/NOTICE, the Asia/Taipei Unique ID fixture, formatting and diff checks
  pass. This closes the macOS server-free P18-V02 gate on `4c142334`. The
  Linux-only signal fallback is type-checked in an isolated target harness but
  has not been run on Linux; current-revision GitHub Ubuntu/P18-V04 evidence,
  broad P18-V01, Phase 19 Docker/Broker automation, and pre-publication
  version/registry resolution remain open. Owner-run deployment and production
  checks remain Phase 19 manual validation.

- Current-revision server-free **P18-V02** gate on `526a0b34` (2026-10-16,
  macOS ARM64): default and all-feature locked workspace tests, strict
  all-target/all-feature workspace Clippy, formatting, and diff checks pass.
  The pinned Maven `verify` passes its 37-scenario manifest, Java fixture
  probes, fixture drift checks, and Java/Rust cross-decodes. Comparator
  self-test and manifest validation pass; deterministic race/leak smoke passes
  16 cases × 3; fuzz smoke covers 105 frame seeds, 11 message seeds, and 1,856
  bounded mutations. Release examples, offline benchmark compilation, and
  workspace metadata/license/dependency checks pass. This closes server-free
  P18-V02 on `526a0b34`. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04
  evidence, Phase 19 local Docker/Broker automation, and pre-publication
  version/registry resolution remain open. Owner-run deployment and production
  checks remain Phase 19 manual validation.

- P18-V01 Remoting config validator reverse-surface follow-up (`375aeff5`,
  2026-10-16, pinned Java source
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): `ClientConfig::validate` is
  internal because Java `NettyClientConfig` has no public standalone
  validator; `RemotingClient::with_config` still validates before construction.
  A compile-fail rustdoc example protects the public boundary, and transport
  validation tests now exercise the public constructor. Remoting all-feature
  tests pass (97 unit, 8 codec, 1 fuzz regression, 7 security, 100 transport,
  8 doctests), as do strict all-target/all-feature Remoting Clippy, formatting,
  and diff checks. This closes this API mismatch only; broad P18-V01 remains
  open.

- Current-revision server-free **P18-V02** rerun on `375aeff5`
  (2026-10-16, macOS ARM64): default and all-feature locked workspace tests,
  all-target/all-feature workspace check and strict Clippy, pinned Maven
  `verify` (37 scenarios and Java/Rust cross-decodes), differential self-test
  and 37-scenario manifest validation all pass. Race/leak smoke passes (16
  cases × 3); fuzz smoke covers 105 frame seeds, 11 message seeds, and 1,856
  bounded mutations. Release examples and workspace benchmarks compile.
  Locked Cargo metadata reports 215 packages, including 208 registry packages
  with license metadata; workspace metadata/license/dependency checks and all
  seven package listings with LICENSE/NOTICE pass. The Asia/Taipei Unique ID
  fixture, formatting and diff checks pass. This closes server-free P18-V02 on
  `375aeff5`. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 Docker/Broker automation, and pre-publication version/registry
  resolution remain open. Owner-run deployment and production checks remain
  Phase 19 manual validation.

- Primary facade mapped-type availability (2026-10-16): the main
  `rocketmq-classic` crate now re-exports the phase-owned consumer/producer
  states and default groups, POP result/status, end-transaction hook API,
  `QueryResult`, Producer publish-info maps/views/adapters, and route DTO/alias
  handle. `tests/facade_public_types.rs` checks the root import paths and
  Producer state/table accessors. This closes that facade reachability slice
  only; broad P18-V01 and current-revision release gates remain open.

- Current-revision server-free **P18-V02** rerun on `fd809302`
  (2026-10-16, macOS ARM64): UTC default and all-feature locked workspace tests,
  all-target/all-feature check and strict Clippy pass. Pinned Maven `verify`
  passes the 37-scenario manifest and all Java/Rust fixture checks; comparator
  self-test and manifest validation pass. Offline race/leak smoke passes 16
  cases × 3; fuzz smoke passes 105 frame seeds, 11 message seeds, and 1,856
  mutations. Release examples and workspace benchmarks compile. Locked
  metadata reports 7 workspace packages, 215 total packages, and 208 registry
  packages with license metadata; workspace metadata/dependency/license checks
  and all seven package listings with LICENSE/NOTICE pass. The Asia/Taipei
  Unique ID fixture, formatting, and diff checks pass. This closes the
  server-free P18-V02 gate on `fd809302`. Broad P18-V01, current-revision GitHub
  Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  pre-publication version/registry resolution remain open. Owner-run deployment
  and production checks remain Phase 19 manual validation.

- P18-V01 Remoting timeout-property follow-up (`b5106ebc`, 2026-10-16): the
  pinned Java fixture now records `connectTimeoutMillis=-13` and
  `clientChannelMaxIdleTimeSeconds=-7` from their system properties. Rust
  seeds these signed values from the same process environment keys on first
  `ClientConfig::default()`, retaining the process-wide result. The isolated
  subprocess test checks these values alongside the semaphore and
  close-socket properties. Remoting all-feature tests pass (99 unit, 8 codec,
  1 fuzz regression, 7 security, 100 transport, 8 doctests); strict Remoting
  Clippy, Maven verify (37 scenarios, fixture drift, and Java/Rust
  cross-decodes), formatting, and diff checks pass. This closes these two
  property-source rows only. Worker-count, socket-buffer, and write-watermark
  property sources and broad P18-V01 remain open, as do final-revision
  full-workspace P18-V02, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 local Docker/Broker automation, and pre-publication version/registry
  resolution. Owner-run deployment and production checks remain Phase 19
  manual validation.

- P18-V01 Remoting close-socket property-source follow-up (`280bcfe0`,
  2026-10-16): the pinned Java fixture now sets
  `com.rocketmq.remoting.client.closeSocketIfTimeout=false` and records the
  resulting `NettyClientConfig` value. Rust reads the same process environment
  key on first `ClientConfig::default()` and matches Java's case-insensitive
  `Boolean.parseBoolean` behavior. The isolated subprocess test checks this
  alongside the semaphore properties. Remoting all-feature tests pass (99
  unit, 8 codec, 1 fuzz regression, 7 security, 100 transport, 8 doctests);
  strict all-target/all-feature Remoting Clippy, Maven verify (37 scenarios,
  fixture drift, and Java/Rust cross-decodes), formatting, and diff checks pass.
  This closes this property-source slice only. Other `NettySystemConfig`
  default seeds and broad P18-V01 remain open, as do final-revision
  full-workspace P18-V02, current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 local Docker/Broker automation, and pre-publication version/registry
  resolution. Owner-run deployment and production checks remain Phase 19
  manual validation.

- P18-V01 request/reply helper behavior fixture (2026-10-16): the invoked
  Java 5.5.1 fixture now exercises `RequestResponseFuture` timeout, response,
  latch, send-status, cause and callback behavior, including its private
  exactly-once callback guard, and verifies `RequestFutureHolder` singleton/live
  table identity and mutation. The pinned commit source and Maven 5.5.1 artifact
  were used because the separate local Java checkout is at `2238256d` and has a
  newer extra method. Rust fixture assertions and Maven drift verification pass.
  This adds evidence only: Rust still lacks the public mutable future/table API,
  so P18-V01 remains open.

- P18-V01 request/reply future/table implementation follow-up (2026-10-16):
  Core now exposes mutable `RequestResponseFuture` state and a live
  `RequestFutureTable` over the same process-wide correlation entries used by
  Producer registration, inbound replies, send failures, and timeout scans.
  Producer callback requests use the future callback guard, while Producer
  startup/shutdown manages the shared three-second/one-second scanner. Scoped
  runtime timeout scans preserve registry isolation; the latch countdown is
  atomic under concurrent callers. The focused Core all-feature suite passes
  (163 unit tests and all Core integration/doc targets); Producer and facade
  all-feature suites pass (142 Producer unit tests, request/reply timeout/send
  failure/shutdown tests, facade table tests, all Producer integration/doc
  targets); strict workspace all-target/all-feature Clippy, formatting and
  diff checks pass. This closes the public mutable future/live-table request/
  reply slice only. Broad P18-V01, a full current-revision P18-V02 rerun,
  current GitHub Ubuntu/P18-V04 evidence, Phase 19 Docker/Broker automation,
  and pre-publication version/registry resolution remain open; owner-run
  deployment and production checks remain Phase 19 manual validation.

- P18-V01 request-future scheduler clock-scope follow-up (2026-10-16): each
  Producer timeout scanner is associated with its Tokio runtime ID, and each
  scan expires only entries registered on that runtime. This prevents an
  independent paused/runtime clock from expiring another runtime's future. A
  two-runtime paused-clock regression passes. The combined Core/Producer/
  facade all-feature gate passes (164 Core unit tests, 142 Producer unit tests,
  and all selected integration/doc targets); strict workspace Clippy,
  formatting, and diff checks pass. The full current-tree P18-V02 suite and
  other release gates listed above remain open.

- Current-revision server-free **P18-V02** gate on `479b9a7b` (2026-10-16,
  macOS arm64): default and all-feature workspace tests, workspace check,
  strict all-target/all-feature Clippy, Maven verify (37 scenarios and Java/Rust
  cross-decodes), comparator self-test and manifest validation, 16 deterministic
  race/leak cases × 3, fuzz smoke (105 frame seeds, 11 message seeds, 1,856
  mutations), release examples, workspace benchmark compilation, locked Cargo
  metadata/package listings, the Asia/Taipei Unique ID differential, formatting,
  and diff checks all pass. Broad P18-V01, current GitHub Ubuntu/P18-V04 evidence,
  Phase 19 Docker/Broker automation, and pre-publication version/registry
  resolution remain open. Owner-run deployment and production checks remain
  Phase 19 manual validation.

- P18-V01 Remoting signed semaphore config follow-up (`e4fd4062`, code
  `c0f5c13d`, 2026-10-16): Rust async/oneway config fields now use Java's signed
  `i32` range and accept negative values. The invoked Java fixture records
  defaults, setter/getter round trips, and timed-acquire outcomes for
  `Integer.MIN_VALUE`, its neighbor, `-2`, `-1`, zero, one, and `Integer.MAX_VALUE`.
  Rust maps ordinary negative capacities to closed admission and reproduces
  Java's `Integer.MIN_VALUE` first-acquire overflow as an effective `2^31`
  permits. `signed_semaphore_settings_match_java_fixture` checks both async and
  oneway admission, immediate saturation and positive-deadline timeout.
  Remoting all-feature tests pass (98 unit, 8 codec, 1 fuzz regression, 7
  security, 100 transport, 8 doctests); strict Remoting Clippy, formatting,
  diff checks and Maven verify (37 scenarios and fixture/cross-decode checks)
  pass. This closes the signed semaphore value/admission slice only. Java
  system-property seeding for these fields and the broad P18-V01 audit remain
  open, as do current GitHub Ubuntu/P18-V04 evidence, Phase 19 Docker/Broker
  automation, and release version/registry resolution. Owner-run deployment
  and production checks remain Phase 19 manual validation.

- P18-V01 Remoting semaphore property-source follow-up (`6497d5b4`,
  2026-10-16): the pinned Java fixture now records the async and oneway
  semaphore values when their Java system properties are set. Rust seeds
  `ClientConfig::default()` from the same process environment keys on first
  use, applies Java signed-decimal integer parsing, and retains the first
  result process-wide. An isolated subprocess test covers the fixture values
  and malformed leading-whitespace input. Remoting all-feature tests pass
  (99 unit, 8 codec, 1 fuzz regression, 7 security, 100 transport, 8 doctests),
  as do strict all-target/all-feature Remoting Clippy, formatting, diff checks,
  and Maven verify (37 scenarios, fixture drift, and Java/Rust cross-decodes).
  This closes the property-source slice for these two settings only. Broad
  P18-V01, final-revision full-workspace P18-V02, current-revision GitHub
  Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  pre-publication version/registry resolution remain open. Owner-run deployment
  and production checks remain Phase 19 manual validation.

- P18-V01 Remoting worker-thread setting/source follow-up (`85dccf7f`,
  2026-10-16): Java fixture coverage now includes the `clientWorkerThreads`
  system-property value `-3` and setter/getter round trips for
  `Integer.MIN_VALUE`, `-1`, zero, one, and `Integer.MAX_VALUE`. Rust's public
  `ClientConfig.worker_threads` and live `RemotingClient` getter/setter now use
  Java's signed `i32` domain; nonpositive values remain observable and fail
  when `start()` creates workers. The default reads the same process
  environment key on first construction with Java integer parsing. Remoting
  all-feature tests pass (99 unit, 8 codec, 1 fuzz regression, 7 security,
  100 transport, 8 doctests); strict Remoting Clippy, Maven verify (37
  scenarios, fixture drift, and Java/Rust cross-decodes), formatting, and diff
  checks pass. This closes the worker-count API/domain/property slice only.
  Socket-buffer and write-watermark property sources and signed API domains,
  broad P18-V01, final-revision full-workspace P18-V02, current-revision
  GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and
  pre-publication version/registry resolution remain open. Owner-run deployment
  and production checks remain Phase 19 manual validation.

- P18-V01 Remoting socket-buffer and write-watermark setting/source follow-up
  (`45ea5b54`, 2026-10-16): Java property fixtures now cover
  `com.rocketmq.remoting.socket.sndbuf.size`,
  `com.rocketmq.remoting.socket.rcvbuf.size`, and both write-watermark
  properties, plus signed setter/getter round trips at
  `Integer.MIN_VALUE`, `-1`, zero, one, and `Integer.MAX_VALUE`. Rust's
  `ClientConfig` fields and live `RemotingClient` accessors now use signed
  `i32`, seed defaults from those same process environment keys with Java
  `Integer.parseInt` behavior, preserve the values, and match Java's direct
  start-time versus proxy-CIDR first-use settings. Enabled invalid watermark
  ordering is rejected at `start()`. Remoting all-feature tests pass (102
  unit, 8 codec, 1 fuzz regression, 7 security, 100 transport, 8 doctests);
  strict all-target/all-feature Remoting Clippy, Maven verify (37 scenarios,
  fixture drift, and Java/Rust cross-decodes), formatting, and diff checks
  pass. This closes these four setting rows only. Broad P18-V01, a final-revision
  full-workspace P18-V02 rerun, current-revision GitHub Ubuntu/P18-V04
  evidence, Phase 19 local Docker/Broker automation, and pre-publication
  version/registry resolution remain open. Owner-run deployment and production
  checks remain Phase 19 manual validation.

- P18-V01 Remoting channel capability identity (`463c1460`, 2026-10-16):
  pinned Java `ResponseFuture.getChannel()` and `ChannelEventListener` channel
  callbacks are represented by Rust `ResponseFuture::channel()` and
  identity-preserving `ChannelHandle` values. Unit and loopback assertions now
  cover future identity plus shared CONNECT/ACTIVE callback identity. Corrected
  stale Java 5.5.1, Phase 3/6/16 inventory, known-difference and ADR claims that described
  the API as address-only or absent. The Remoting all-feature suite (102 unit,
  8 codec, 1 fuzz regression, 7 security, 101 transport, 8 doctests), strict
  Remoting Clippy, formatting and diff checks pass. Broad P18-V01 remains open;
  final-revision full-workspace P18-V02, GitHub Ubuntu/P18-V04 evidence, Phase
  19 Docker/Broker automation, and release version/registry checks remain
  pending.

- P18-V01 Remoting request-context slice (`eadcdb55`, 2026-10-16): pinned Java
  `NettyRequestProcessor.processRequest` and `RequestPipeline.execute` both
  receive `ChannelHandlerContext`. Rust processors and each piped pipeline stage
  now receive the same identity-preserving `RequestContext`, with peer address,
  channel close, and immediate no-deadline write enqueue. Existing address-only
  processor/pipeline implementations keep their default adapters. The loopback
  regression verifies shared handle identity, an extra context write, and the
  regular processor response; a unit regression verifies the legacy processor
  adapter. Default and all-feature workspace tests, strict all-target/all-feature
  workspace Clippy, formatting and diff checks pass on this change. This closes
  only this context slice: the broader P18-V01 bidirectional audit, complete
  Netty context surface, final P18-V02 evidence, GitHub Ubuntu/P18-V04 result,
  Phase 19 Docker/Broker matrix, and release version/registry resolution remain
  open.

- P18-V01 Remoting channel no-deadline write follow-up (`0b873da1`, 2026-10-16): Java 5.5.1 exposes immediate `Channel.writeAndFlush` through the channel supplied to remoting callbacks. Rust `ChannelHandle` now provides `write_and_flush_without_deadline`; `RequestContext::write_and_flush` delegates to it. The local result future reports socket-write completion and does not imply remote receipt. The loopback tests cover deadline and no-deadline channel writes, context channel state/close, shared identity, context-write completion, and the regular processor response. Pinned source: `NettyRequestProcessor.processRequest(ChannelHandlerContext, RemotingCommand)`, `RequestPipeline.execute(ChannelHandlerContext, RemotingCommand)`, and `NettyRemotingAbstract.writeResponse(Channel, ...)`. Focused Remoting all-feature tests, strict all-target/all-feature Remoting Clippy, formatting, and diff checks pass. This closes the no-deadline channel-write capability slice; generic Netty handler-context APIs and the broad bidirectional P18-V01 audit remain open, as do the final-revision full P18-V02 gates, GitHub Ubuntu/P18-V04 evidence, Phase 19 local Docker/Broker automation, and release version/registry checks.

- Current-revision server-free **P18-V02** rerun on source commit `4d49cd7e`
  (2026-10-16, macOS ARM64): default and all-feature locked workspace tests,
  strict all-target/all-feature workspace Clippy, format/diff checks, Maven
  `verify` (37 scenarios and Java/Rust fixture cross-decodes), comparator
  self-test and 37-scenario manifest validation, 16 race/leak cases × 3,
  fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations), release
  examples, offline benchmark compilation, locked Cargo metadata, workspace
  metadata/license/dependency checks, seven package listings with LICENSE and
  NOTICE, and the Asia/Taipei Unique ID fixture all pass. Metadata reports 7
  workspace packages, 215 packages total, and 208 registry packages with
  license metadata. This closes the server-free P18-V02 rerun on this source
  revision. Broad P18-V01 and current-revision GitHub Ubuntu/P18-V04 evidence
  remain open. The latest listed branch CI run is for older commit `cc79eb6e`
  and failed in the Linux consumer stack-capture test; it is not evidence for
  `4d49cd7e`. Phase 19 Docker/Broker automation, the owner-validation handoff,
  and pre-publication version/registry checks remain open; owner-run deployment
  and production checks remain Phase 19 manual validation.

- P18-V01 Remoting request-context API disposition (2026-10-16): pinned Java
  `NettyRemotingAbstract.buildProcessRequestHandler` reads `ctx.channel()` for
  peer metadata and responses and passes the same context into the
  `RequestPipeline` and `NettyRequestProcessor` callbacks. Maven dependency
  resolution pins Netty `netty-transport` 4.1.130.Final. Its generic
  `ChannelHandlerContext` handler metadata, event propagation, allocator,
  attributes, raw socket operations, arbitrary-object writes and promise
  factories are framework facilities; the Rust API instead maps the
  RocketMQ-level contract through shared `RequestContext`/`ChannelHandle`,
  typed command writes, `RequestExecutor`, and ordered `RequestPipeline` stages.
  Existing request-context, pipeline-order and executor-rejection tests cover
  these mappings. This closes the context framework disposition only; broad
  P18-V01 source/API/input/error/control-flow and reverse-export reconciliation
  remains open.

- P18-V01 RequestResponseFuture nullable latch slice (`310b6cd6`, fixture
  commit `3be59a09`, 2026-10-16): the pinned Java fixture confirms that a null
  latch getter returns null, `putResponseMessage` stores its response before
  throwing `NullPointerException`, and `waitResponseMessage` also throws. Rust
  uses `Option` and `RequestFutureError`; inbound reply processing leaves the
  pending table entry in place and returns `SYSTEM_ERROR` after preserving the
  response. Core's 167 unit tests, Producer request/reply's 3 tests, the facade
  request/reply's 5 tests, strict Clippy for Core/Producer/facade targets,
  formatting, diff checks, and pinned Maven `verify` (37 scenarios and fixture
  drift checks) pass. This closes this latch slice only. Broad P18-V01, a full
  P18-V02 gate rerun on this code revision, current-revision GitHub Ubuntu /
  P18-V04 evidence, Phase 19 Docker/Broker automation and scenario execution,
  `docs/owner-validation.md`, and pre-publication version/registry checks
  remain incomplete. Owner deployment and production checks remain Phase 19
  manual validation.
- P18-V01 request/reply public message type (`c6d852df`, 2026-10-16): the
  pinned Java signatures use base `Message` for Producer request results,
  `RequestCallback.onSuccess`, and `RequestResponseFuture` response methods.
  Rust now uses `Message` on those public boundaries. A later reverse-export
  review found public `From<MessageExt>` and `From<&MessageExt>` implementations
  that contradicted the documented boundary; the follow-up below removes them
  and records its scoped validation. At `c6d852df`, `cargo fmt --all -- --check`,
  strict workspace all-target/all-feature Clippy, default and all-feature
  workspace tests, and `git diff --check` passed. This closes that type slice
  only; broad P18-V01 and the full P18-V02 gate suite remain open, along with
  the Ubuntu/P18-V04 result, Phase 19 Docker/Broker automation, owner-validation
  instructions, and pre-publication version/registry checks.
- [x] **P18-V02** — Complete server-free gate rerun on revision `3b0af61d`
  (2026-10-16, macOS ARM64): default/all-feature locked workspace tests,
  strict all-target/all-feature Clippy, format/diff checks, pinned Maven verify
  (37 scenarios, fixture drift and cross-decodes), differential self-test and
  37-scenario manifest, deterministic race/leak smoke (16 cases × 3), fuzz
  smoke (105 frame seeds, 11 message seeds, 1,856 mutations), release examples,
  offline benchmark compilation, Cargo/workspace metadata and dependency/license
  checks, all seven package listings with LICENSE/NOTICE, and the
  `TZ=Asia/Taipei` Unique ID fixture all pass. Broad P18-V01 and the current
  GitHub Ubuntu/P18-V04 result remain open; this entry makes no live Broker,
  release, or compatibility claim.

- P18-V01 Push `updateCorePoolSize` slice (`8105fd3b`, Java fixture SHA-256
  `29ed1e78da3f3dfe6f0d334d78e8f97e4178537a7105419c6fd907fc0aa9a023`): Rust
  no longer exposes Java's void operation as a boolean. It ignores invalid
  facade-bound values, reports an uncreated service as `NotStarted`, maps a
  raised facade maximum exceeding the constructed executor capacity to
  `InvalidSetting`, handles concurrent and orderly services, and keeps the
  effective pool size updateable after shutdown. The pinned fixture checks the
  Java null-service failure, valid/invalid boundaries, live maximum mutation,
  executor-capacity exception, and post-shutdown paths. The final consumer
  suite passes (220 unit tests plus all integration and doc targets); strict
  all-target/all-feature Consumer Clippy, formatting, diff checks, and pinned
  Maven `verify` (37 scenarios, fixture drift and cross-decodes) pass. This
  closes this API slice only; broad P18-V01 remains open, and the complete
  workspace P18-V02 gate must run again on the final revision.

- P18-V01 request/reply table removal semantics (`aef6b86a`, `65becc27`,
  2026-10-16): the pinned Java 5.5.1 fixture confirms that removing a
  correlation from `RequestFutureHolder` leaves the future latch unsignaled
  and its own timed wait returns null. Rust now preserves the original
  `PendingReplyRequest` deadline when public table removal, replacement, or
  clear drops the lookup sender; explicit owner cancellation and registry
  clear close the receiver and map to `RequestReplyCancelled` without adding
  a public completion variant. Regressions cover removal-to-deadline behavior,
  cancellation, and the Java fixture observations. Core/Producer/facade
  all-feature tests pass (168 Core unit tests, 142 Producer unit tests, all
  scoped integration and doc targets); strict all-target/all-feature Clippy,
  formatting, `git diff --check`, and pinned Maven `verify` (37 scenarios,
  fixture drift and Java/Rust cross-decodes) pass. Fixture SHA-256:
  `b5c6e39d24e197d678fd214418f4f18b4fabb9f63ef27e74d3e152c7616ef5ef`.
  This closes the table-removal waiter slice only; broad P18-V01 and final
  workspace P18-V02 remain open.

- P18-V01 request/reply completion API reverse-surface correction
  (`2026-10-16`): the first implementation added a `Cancelled` variant to the
  public `PendingReplyCompletion` enum. The pinned Java API has no such public
  completion, so cancellation now uses private state and preserves the
  existing receiver-error mapping to `RequestReplyCancelled`; the public enum
  shape is unchanged. Core/Producer/facade all-feature tests pass (168 Core
  unit tests, 142 Producer unit tests, all scoped integration and doc targets),
  as do strict scoped all-target/all-feature Clippy, format and diff checks.
  This closes the extra-variant discrepancy only; broad P18-V01 and final
  workspace P18-V02 remain open.

- [x] **P18-V02** — Revalidated the complete server-free gate on revision
  `ac26a5ae` (2026-10-16, macOS ARM64): default and all-feature locked workspace
  tests, strict all-target/all-feature Clippy, workspace formatting, pinned
  Maven `verify` (37 scenarios, fixture drift and Java/Rust cross-decodes),
  differential self-test and 37-scenario manifest, deterministic race/leak
  smoke (16 cases × 3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856
  mutations), release examples, offline benchmark compilation, locked Cargo
  metadata, workspace metadata/license/dependency checks, seven package
  listings with LICENSE/NOTICE, and the `TZ=Asia/Taipei` Unique ID fixture all
  pass. P18-V01, current GitHub Ubuntu/P18-V04 evidence, Phase 19 automation,
  and release preflight remain open; this makes no live Broker, release, or
  compatibility claim.

- P18-V01 Common `MessageUtils` slice (2026-10-16): the four pinned Java
  public methods now map to Rust Common helpers. Invoked Java fixture coverage
  and the Rust differential cover Murmur3 sharding indexes, message-property
  fallback, index deduplication, and Java encoded-property deletion. Fixture
  SHA-256: `e01bed78a2a2290a82991fd68d308d77c283475e82b32d32d3939d045099f5f8`.
  Default/all-feature locked workspace tests, strict all-target/all-feature
  Clippy, formatting, diff checks, and pinned Maven `verify` (37 scenarios and
  the new fixture) pass on the current worktree. This closes that API slice
  only; broad P18-V01 and current-revision GitHub Ubuntu/P18-V04 evidence remain
  open.

- [x] **P18-V02** — Revalidated the full server-free gate on the worktree
  based on `12db9def` with the Common `MessageUtils` parity update. Default and
  all-feature locked workspace tests, strict all-target/all-feature Clippy,
  formatting and diff checks, Maven `verify` (37 scenarios and fixture
  cross-decodes), differential self-test and manifest, deterministic race/leak
  smoke (16 cases × 3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856
  mutations), release examples, offline benchmark compilation, workspace
  metadata/license/dependency checks, seven package listings, and the
  Asia/Taipei Unique ID fixture all pass. Broad P18-V01 and current-revision
  GitHub Ubuntu/P18-V04 evidence remain open; this makes no Broker, release, or
  compatibility claim.

- P18-V01 Common `CorrelationIdUtil` slice (2026-10-16): Rust Common now exports
  `utils::create_correlation_id`, Producer request preparation uses it, and the
  Producer no longer directly depends on `rand`. The invoked Java 5.5.1
  request/reply fixture records `java.lang.String createCorrelationId()`;
  the Common integration test checks the signature and lowercase UUID v4 text
  format. Common and Producer focused tests, Maven fixture generation, and
  format/diff checks pass. This closes this API slice only; broad P18-V01 and a
  full P18-V02 rerun on the resulting commit remain open.

- P18-V01 Common `NameServerAddressUtils` slice (2026-10-16): Common now
  exposes all five Java public values and four methods, with direct Java fixture
  inventory and Rust input/output checks. Core ClientConfig shares the Common
  matcher and extraction implementation. Common fixture tests, all Core
  configuration and library tests, and Java fixture generation pass. This
  closes this utility slice only; broad P18-V01 remains open.

- [x] **P18-V02** — Revalidated the complete server-free gate on revision
  `787157a0` (2026-10-16, macOS ARM64), after the Common API additions and
  lifecycle/thread-helper audit. Default and all-feature locked workspace
  tests, strict all-target/all-feature Clippy, formatting, pinned Maven
  `verify` (37 scenarios, fixture drift and Java/Rust cross-decodes),
  differential self-test and 37-scenario manifest, race/leak smoke (16 cases
  × 3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations),
  release examples, offline workspace benchmark compilation, locked Cargo
  metadata, workspace metadata/license/dependency checks, seven package
  listings with LICENSE/NOTICE, and the `TZ=Asia/Taipei` Unique ID fixture all
  pass. Broad P18-V01, current GitHub Ubuntu/P18-V04 evidence, Phase 19
  Broker/Docker automation, and release preflight remain open.

- P18-V01 platform cross-target continuation (2026-10-16): the production Core
  target passes Windows GNU all-target/all-feature `cargo check`, strict
  Windows-target Clippy, and `cargo test --no-run` with all Core test binaries
  linked. A temporary crate including the actual local-address module passes
  FreeBSD-target check and strict Clippy. The full Core FreeBSD check lacks a
  FreeBSD C sysroot, and neither Windows nor BSD binaries ran. Windows runtime
  enumeration, non-macOS BSD runtime behavior, and a real IPv6-only host remain
  open P18-V01 evidence; broad API/source/input/flow reconciliation remains
  open as well.

- P18-V01 Common `Message`/`MessageAccessor` declared-surface slice (2026-10-16):
  the pinned Java fixture now records all 5 public `Message` constructors, 34
  declared public `Message` methods, and 20 declared public `MessageAccessor`
  methods; Rust pins those signature lists and differentially checks
  `MessageAccessor.setLiteTopic` through `PROPERTY_LITE_TOPIC`. Fixture SHA-256:
  `d11f8c88e44711cffbd574beab5946943b00a8d350fe05ec40ff1b744320f321`.
  Pinned offline Maven `verify` (37 scenarios), the focused Common fixture test,
  Common all-target/all-feature strict Clippy, formatting, and diff checks pass.
  This closes only this API inventory slice. Broad P18-V01, a full P18-V02 run
  on the eventual final revision, current-revision GitHub Ubuntu/P18-V04
  evidence, Phase 19 local Docker/Broker automation, and release
  version/registry checks remain open. Owner-run deployment and live Broker
  checks remain Phase 19 manual validation.

- P18-V01 Common `MessageExt` reverse-export correction (2026-10-16): the
  pinned Java class inherits `Message`; Rust already models that access through
  `Deref`, but public `From<MessageExt>` and `From<&MessageExt>` implementations
  also exposed lossy conversions to the base type. Both conversions are removed,
  Core clones the inherited `Message` for its internal response-future boundary,
  and compile-fail doctests guard the absent conversions. Common all-feature
  tests pass (23 unit tests, all integration targets, 24 doctests); Core
  all-feature tests and strict Common/Core all-target Clippy also pass, as do
  formatting and diff checks. This closes only this reverse-export discrepancy;
  broad P18-V01, final-revision P18-V02, current-revision GitHub Ubuntu/P18-V04,
  Phase 19 local Docker/Broker automation, and release preflight remain open.

- P18-V01 Common inherited `Object.equals/hashCode` parity (2026-10-16): Java
  `Message`, `MessageExt`, and `MessageClientExt` inherit identity equality and
  stable identity hash codes from `Object`. Rust now uses a shared hidden
  identity token for `Message`/`MessageExt`/`MessageClientExt` `PartialEq`/`Eq`/
  `Hash`, including comparisons through base/subclass views. Rust `Clone`
  preserves its handle token; `Message::clone_message`, decode, and
  deserialization create new identities. Mutation leaves the hash stable. The
  pinned fixture records the declaring class, self/distinct equality, hash
  stability, and inherited-view comparisons; Rust covers the same behavior and
  checks clone token preservation, distinct `clone_message`/decoded/deserialized
  identities, and identity-hash stability across mutation. Fixture SHA-256:
  `5158dad83a14f0de57ba842134466e82331716f2c171b963e31ae227d9d66890`.
  Maven `verify` (37 scenarios), Common all-feature tests (23 unit, 21 message
  decode, 6 other integration, 24 doctests), Core all-feature tests (168 unit,
  all integration targets, 13 doctests), strict Common/Core Clippy, formatting,
  and diff checks pass. This closes only this inherited trait slice; broad
  P18-V01 and final-revision P18-V02 remain open.

- P18-V01 Common `MessageExt`/`MessageClientExt` declared method inventory
  (2026-10-16): the pinned Java fixture now records all 44 declared public
  `MessageExt` methods and all four declared public `MessageClientExt` methods.
  Rust's Common fixture tests pin both sorted signature lists alongside the
  constructor, scalar, host-buffer, and client-ID behavior checks. Fixture
  SHA-256: `2118277b7aad68e1077d92371d5ec14bd8cb77535e272dca406bc3e716d85792`.
  Maven `verify` passes all 37 scenarios. Common all-feature tests pass (23
  unit tests, 21 `message_decode`, six other integration, 24 doctests), as do
  strict Common Clippy, formatting, checksum, and diff checks. This closes the
  method inventory slice only; broad P18-V01 remains open.

- P18-V01 identity-handle follow-up (2026-10-16): the first default workspace
  rerun after removing `From<MessageExt> for Message` found a stale Producer
  `.into()` call; after that was fixed, Consumer filter-hook coverage showed
  that Rust `Clone` must preserve the message identity token when it snapshots
  the same Java object. Producer now clones through `Deref`, ordinary `Clone`
  preserves the token, and `Message::clone_message` creates a distinct object.
  Producer all-feature tests/strict Clippy and the focused Common/Consumer
  regressions pass; the full workspace rerun is pending.

- P18-V02 identity-handle final-gate follow-up (2026-10-16; worktree based on
  `0f1435db`): the default and all-feature locked workspace tests, strict
  workspace Clippy, formatting, and diff checks pass after the Producer clone
  correction. Offline Maven `verify` passes all 37 scenarios and message fixture
  drift checks. The differential self-test and 37-scenario manifest pass; the
  deterministic race/leak smoke passes 16 cases × 3, and fuzz smoke passes 105
  frame seeds, 11 message seeds, and 1,856 mutations. Release examples and the
  offline benchmark compile. Workspace metadata reports seven members and 215
  total packages; metadata/license/dependency checks and seven package-content
  listings pass. The Asia/Taipei Unique ID fixture passes. This closes the
  server-free P18-V02 gates on this worktree only; broad P18-V01, current-revision
  GitHub Ubuntu/P18-V04 evidence, Phase 19 Docker/Broker automation and scenario
  execution, owner-validation instructions, and pre-publication version/registry
  checks remain open. Owner-run deployment and live Broker checks remain Phase
  19 manual validation.

- P18-V01 Common `MessageDecoder` declared-surface inventory (`9004bcc9`,
  2026-10-16): the pinned Java 5.5.1 fixture now records its public no-argument
  constructor and all 26 declared public method signatures. The Common
  differential pins both sorted lists alongside the existing constant checks;
  the Phase 8 matrix maps each method family to its Rust owner and behavior
  evidence. Fixture SHA-256:
  `6cead986404276aa6ff3f11e73c04ba69d936619160351703e3e782205a6a73d`.
  Common `message_decode` tests pass (21 tests), strict all-target/all-feature
  Common Clippy passes, and pinned Maven `verify` passes all 37 scenarios and
  fixture drift checks. Formatting, checksum, and diff checks pass. This closes
  the declaration-inventory slice only; broad P18-V01 remains open.

- P18-V01 Common `MessageId` declared-surface inventory (`cc961818`,
  2026-10-16): the pinned Java 5.5.1 fixture now records its public
  `MessageId(SocketAddress, long)` constructor and all four declared public
  methods. Rust's `MessageId::new`, address/offset getters, and setters map to
  those signatures; the fixture test pins the exact sorted lists. Fixture
  SHA-256: `c2591c4d8f477becd4a96150df971da9ab09dcd6458be83ce0e9cc3c33759bc8`.
  Common `message_decode` tests pass (21 tests), strict all-target/all-feature
  Common Clippy passes, and pinned Maven `verify` passes all 37 scenarios and
  fixture drift checks. Formatting, checksum, and diff checks pass. This closes
  only the MessageId declaration-inventory slice; broad P18-V01 remains open.

- P18-V01 Common `MessageVersion` declared-surface inventory (`84462565`,
  2026-10-16): the pinned Java 5.5.1 fixture now records all eight declared
  public methods and confirms the enum has no public constructors. The Rust
  fixture test pins the sorted signatures alongside the existing enum values,
  comparison, lookup, byte-width, and error-boundary cases. Fixture SHA-256:
  `ddef0ea5e007f41e8e03c983081de27fe53de2df2d369dfefb98dab775a19516`.
  Common `message_decode` tests pass (21 tests), strict all-target/all-feature
  Common Clippy passes, and pinned Maven `verify` passes all 37 scenarios and
  fixture drift checks. Formatting, checksum, and diff checks pass. This closes
  only the MessageVersion declaration-inventory slice; broad P18-V01 remains
  open.

- P18-V01 Common `MessageSysFlag` declared-surface inventory (`1e6b07bd`,
  2026-10-16): the pinned Java fixture records all 14 public integer constants,
  the no-argument utility constructor, and all five declared public methods.
  Rust's `sysflag::message` constants and functions match the listed values and
  method behavior; the stateless Java constructor is represented by this Rust
  namespace rather than a constructible object. Common `message_decode` tests
  pass (22 tests), strict all-target/all-feature Common Clippy passes, and
  pinned Maven `verify` passes all 37 scenarios and fixture drift checks.
  Fixture SHA-256: `1b9590233baae11bb8705795bc357c84b12fa8cf2be978d69267d2f4e2fc1fe2`.
  Formatting, checksum, and diff checks pass. This closes only this utility
  declaration slice; broad P18-V01 remains open.

- P18-V01 Common `PullSysFlag` declared-surface inventory (`d0a36f9e`,
  2026-10-16): the pinned Java fixture records its empty public-constant map,
  public no-argument constructor, and all 10 declared public methods. Rust pins
  these signatures and checks the two flag builders, predicates, clearers, and
  subscription builder through `sysflag::pull`; the stateless Java utility
  constructor is represented by that Rust namespace. Common `message_decode`
  passes (23 tests), strict all-target/all-feature Common Clippy passes, and
  pinned Maven `verify` passes all 37 scenarios, fixture drift, and Java/Rust
  cross-decodes. Formatting, checksum, and diff checks pass. Fixture SHA-256:
  `be0af6c701fbebef014ba1724670d126d5ec70ff73060c990d934e1721d7a3b9`. This
  closes only the PullSysFlag declaration slice; broad P18-V01 remains open.

- P18-V01 Common `MessageConst` declared-surface inventory (`337160d5`,
  2026-10-16): the pinned Java fixture now records all 77 public fields (76
  `String` fields and `HashSet<String> STRING_HASH_SET`), the public
  no-argument constructor, and zero declared public methods. Rust pins the
  field signatures, constructor disposition, method list, all 76 constant
  values, and the initial reserved-property names; the stateless constructor is
  represented by the `message::constants` namespace. Common `message_decode`
  passes (23 tests), strict all-target/all-feature Common Clippy passes, and
  pinned Maven `verify` passes all 37 scenarios, fixture drift, and Java/Rust
  cross-decodes. Formatting, checksum, and diff checks pass. Fixture SHA-256:
  `59309d6426aa55b398addea84e0e1fdab886bc8daa3388b32c4e27005eae07c5`. This
  closes only the MessageConst declaration slice; broad P18-V01 remains open.

- P18-V01 Common `MessageQueue` declared-surface inventory (`d16f7f19`,
  2026-10-16): the pinned Java fixture records all three public constructors,
  all 11 declared public methods (including the compiler bridge
  `compareTo(Object)`), and no public fields. Rust pins these declarations and
  maps the stateless construction/copy/comparison forms to `Default`, `Clone`,
  and `java_compare_to`; existing route differentials cover method behavior.
  Common `message_decode` passes (24 tests), strict all-target/all-feature
  Common Clippy passes, and pinned Maven `verify` passes all 37 scenarios,
  fixture drift, and Java/Rust cross-decodes. Formatting and diff checks pass.
  Fixture SHA-256: `85feb19197afb046354bb4c88aed4ad1008dcfe235f7bf781817ffa7eb1edd2f`.
  This closes only the MessageQueue declaration slice; broad P18-V01 remains
  open.

- P18-V01 Producer Common `MessageClientIDSetter` declared-surface inventory
  (`9329f424`, 2026-10-16): the pinned Java fixture records its public
  no-argument constructor, no public fields, and all eight declared/callable
  public methods. The Rust test pins the signatures while retaining the
  existing IPv4/IPv6, PID, near-time, invalid-hex and fake-IP differentials;
  the stateless Java constructor maps to the `unique_id` namespace. Core's 168
  library tests pass, strict all-target/all-feature Core Clippy passes, and
  pinned Maven `verify` passes all 37 scenarios and fixture drift/cross-decodes.
  Formatting and diff checks pass. `fixtures/producer/core.json` SHA-256:
  `af16e63df4a3506daaf22162afdf7b52655861bbf0cffea072983a4dacfa25b6`. This
  closes only the MessageClientIDSetter declaration slice; broad P18-V01 remains
  open.

- P18-V01 Producer `MessageBatch` declared-surface inventory and implementation
  (`28253d80`, 2026-10-16): the pinned Java 5.5.1 fixture records the public
  `List<Message>` constructor and all three declared methods. Rust exposes
  `MessageBatch::{new,generate_from_list,encode}`, inherited `Message` access,
  and `IntoIterator`; Java-compatible validation and encoded record bytes are
  covered by Producer tests and a facade import test. Fixture SHA-256:
  `f8acdeae600bef3a56250c4f07c144860c6c22fbd9f84fe8bfe14d930a497b8d`.
  Producer/facade tests, strict crate Clippy, and Maven `verify` (37 scenarios,
  fixture drift, and Java/Rust cross-decodes) pass.
- The server-free P18-V02 gate passes on `28253d80`: default and all-feature
  workspace tests, all-target workspace check and strict Clippy, comparator
  self-test and 37-scenario manifest validation, 16 race/leak cases × 3,
  bounded fuzz smoke, release-example and benchmark compilation, locked
  metadata and package-content checks, and the Asia/Taipei Unique ID check all
  pass. Maven `verify`, formatting, and diff checks pass. This is server-free
  evidence only; it does not close broad P18-V01, current-revision Ubuntu CI /
  P18-V04, Phase 19 Docker/Broker automation, or pre-publication registry
  resolution. Owner deployment and production checks remain manual.

- P18-V01 Producer `MessageBatch` nullability/validation follow-up
  (`8f94b206`, 2026-10-16): pinned Java 5.5.1 fixtures now invoke null and empty
  collections, null elements, and all four delay properties, recording exact
  exception classes/messages. Rust's nullable adapters preserve constructor
  failure timing and iterator null elements; batch generation matches the
  delay, retry-topic, topic and wait-store rejection messages. Producer tests,
  facade import test, strict Producer Clippy, and pinned Maven `verify` pass.
  Fixture SHA-256: `3d8ae4a75e76e1c2636de3633dbf2f04c61c8058076e2c639d63fec17fc5c88d`.
- Current-revision server-free P18-V02 rerun (`8f94b206`, 2026-10-16): default
  and all-feature workspace tests, all-target workspace check and strict
  Clippy, Maven `verify` (37 scenarios and fixture/cross-decode checks),
  differential self-test and 37-scenario manifest, 16 deterministic race/leak
  cases × 3, fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations),
  release examples, workspace benchmark compilation, locked metadata and
  package-content checks, and the Asia/Taipei Unique ID differential pass.
  Broad P18-V01 remains open; current-revision GitHub Ubuntu/P18-V04 evidence,
  Phase 19 Docker/Broker automation and scenario execution, and pre-publication
  version/registry resolution remain outstanding.
- P18-V01 Producer collection-send mutation follow-up (`3624d0b8`, 2026-10-16):
  the pinned Java 5.5.1 oracle confirms that `send(Collection<Message>)` assigns
  missing unique IDs and namespaces each caller message before downstream
  dispatch. Rust `send_batch*` now accepts mutable message slices and preserves
  Java's structural precheck and per-element validation mutation timing. The
  fixture covers dispatch failure after preparation, mixed-topic rejection
  before mutation, and a later element validation failure after an earlier
  element was mutated. Producer all-feature tests (146 unit, 65 integration,
  and 6 doc tests), strict Producer Clippy, all-target/all-feature workspace
  check, Maven `verify` (37 scenarios and fixture/cross-decodes), formatting,
  and diff checks pass. Current Producer core fixture SHA-256:
  `fd92f9e88ab615288cfc5b25b8ed594c000fc2b3c07b9952d6bc0fd790cdbde4`. This
  closes the collection-send mutation slice only; broad P18-V01 and the final
  full-workspace P18-V02 rerun remain open.
- P18-V01 Producer batch error/order follow-up (`c4251219`, 2026-10-16): Java
  wraps batch-construction failures with `Failed to initiate the MessageBatch`;
  Rust now preserves that message and exposes the typed source error. Batch
  encoding no longer applies the aggregate-size check before producer-state
  validation, matching Java's call order. The invoked fixture covers wrapper
  messages, per-element partial mutation, and an unstarted oversized batch.
  Producer all-feature tests (146 unit, 65 integration, and 6 doc tests), strict
  Producer Clippy, all-target/all-feature workspace check, the pinned Producer
  core fixture verification, formatting, and diff checks pass. Current Producer
  core fixture SHA-256: `bfa2d5f8f411c4648670d024e44b0e637ab793873a9eeb0c7ff75bb2a82651c5`.
  This closes only these collection-send error/order slices; broad P18-V01 and
  the final full-workspace P18-V02 rerun remain open.
- P18-V01 Producer auto-batch aggregate-size ordering follow-up (2026-10-16):
  explicit and auto-batch sends now share the batch encoder and apply aggregate
  message-size validation after Producer-state validation, before Broker I/O.
  Producer all-feature tests (147 unit, 65 integration, and 6 doc tests),
  strict all-target/all-feature workspace Clippy, all-target/all-feature
  workspace check, formatting, and diff checks pass. The final full-workspace
  P18-V02 rerun remains open.
- Current-revision server-free P18-V02 rerun (`00254e6d`, 2026-10-16): default
  and all-feature workspace tests, all-target/all-feature workspace check and
  strict Clippy, Maven `verify` (37 scenarios, fixture drift checks and
  Java/Rust cross-decodes), comparator self-test and 37-scenario manifest,
  16 deterministic race/leak cases × 3, fuzz smoke (105 frame seeds, 11
  message seeds, 1,856 mutations), release example build, offline benchmark
  compilation, workspace metadata/license/dependency checks, seven package
  listings, the Asia/Taipei Unique ID differential, formatting, and diff checks
  pass. Broad P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19
  Docker/Broker runner and scenario execution, and pre-publication
  dependency/version resolution remain outstanding.
- P18-V01 Common `MessageAccessor` reverse-surface slice (2026-10-16):
  `MessageAccessor`'s 20 public methods now have an explicit Java-to-Rust
  crosswalk to `Message`, nullable property operations, message property
  constants, and Rust `Clone`. The existing invoked Java fixture pins every
  declared signature and its Rust differential covers helper values, nullable
  maps, deep-copy independence, and `cloneMessage` aliasing. The focused Common
  API test passes; broad Common/P18-V01 reconciliation remains open.
- P17 native Windows stack runtime CI path (2026-10-16): `.github/workflows/ci.yml`
  now runs `jstack_includes_current_and_other_named_threads` on
  `windows-latest` and fails if the exact test does not report one pass. The
  local host cannot execute Windows binaries; the workflow result remains
  pending a run on the current revision.
- P18-V01 Common `MessageAccessor.setProperties` caller-map alias follow-up
  (2026-10-16, pinned Java client source `efa1be59d6fc747ad220f036c86030f05f0419f9`):
  Java retains the supplied `Map` object and exposes mutations through both the
  caller and `Message`. Rust now provides `SharedMessageProperties`,
  `Message::set_properties_shared`, and `Message::shared_properties`, and the
  primary facade re-exports the shared handle. The invoked fixture checks
  identity and bidirectional mutation; Common and facade regressions exercise
  the mapped behavior. Fixture SHA-256:
  `02be176d528051f874a159a7fc2867484a8e1c2f8a7cffcf92c588ec8853db91`.
  Focused Common API and facade tests, all Common tests/Clippy, pinned Maven
  verify (37 scenarios, fixture checks, and Java/Rust cross-decodes), format,
  and diff checks pass. Broader P18-V01 reconciliation remains open.
- Current-revision server-free P18-V02 rerun (`830969e8`, 2026-10-16): default
  and all-feature workspace tests, all-target/all-feature check and strict
  Clippy, Maven `verify` (37 scenarios, fixture checks, Java/Rust cross-decodes),
  comparator self-test and 37-scenario manifest, 16 deterministic race/leak
  cases × 3, fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations,
  146 frame decodes, 113 message decodes), release examples, benchmark
  compilation, locked metadata/package checks, all seven package listings,
  UTC and Asia/Taipei Unique ID tests, formatting, and diff checks pass. Broad
  P18-V01, current-revision GitHub Ubuntu/P18-V04 evidence, and Phase 19
  Docker/Broker automation and scenario execution remain open.
- P18-V01 Common `Message.setBody(byte[])` caller-array alias follow-up
  (`d3532c6e`, 2026-10-16; pinned Java source
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): the invoked fixture verifies
  retained array identity, bidirectional mutation visibility, and detachment
  after replacement. Rust exposes `SharedMessageBody` with lock-scoped mutable
  bytes, `Message::set_body_shared`, and `Message::shared_body`; the facade
  re-exports the handle. Fixture SHA-256:
  `efb8b02c5eb9fec2e78459ec63e97144fdf633d964bd4a2d6e276fc60b13e39d`.
- Current-revision server-free P18-V02 rerun (`d3532c6e`, 2026-10-16): default
  and all-feature workspace tests, all-target/all-feature check and strict
  Clippy, pinned Maven `verify` (37 scenarios and Java/Rust cross-decodes),
  comparator self-test and 37-scenario manifest, deterministic race/leak smoke
  (16 cases × 3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856
  mutations), release examples, workspace benchmark compilation, locked
  metadata and seven package listings, Asia/Taipei Unique ID tests (4),
  formatting, and diff checks pass. Broad P18-V01, current-revision GitHub
  Ubuntu/P18-V04 evidence, native Windows runtime CI result, Phase 19
  Docker/Broker automation, `docs/owner-validation.md`, and pre-publication
  version/dependency resolution remain open.
- P18-V01 Common `Message` constructor body-array aliases (`b03d7fe9`, 2026-10-16;
  pinned Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the invoked
  fixture verifies the two-argument, tags, tags-and-keys, and six-argument
  constructors retain their caller's exact array, including mutation after
  construction. Rust now exposes shared-body variants for each constructor
  shape and nullable fields. Fixture SHA-256:
  `0346a246d0f22440ed71773ac1854ad510acf857954d85f7ee9b8a451afc6471`.
- Current-revision server-free P18-V02 rerun (`b03d7fe9`, 2026-10-16): default
  and all-feature workspace tests, all-target/all-feature check and strict
  Clippy, pinned Maven `verify` (37 scenarios and updated fixture/cross-decodes),
  comparator self-test and 37-scenario manifest, deterministic race/leak smoke
  (16 cases × 3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856
  mutations), release examples, workspace benchmark compilation, locked
  metadata and seven package listings, Asia/Taipei Unique ID tests (4),
  formatting and diff checks pass. The first default workspace run exposed a
  flaky paused-time test; `f04c4384` makes its three-second initial delay and
  one-second scan period deterministic, and the corrected test passes in the
  repeated smoke and full workspace suites. Broad P18-V01, current-revision
  Ubuntu/Windows CI evidence, Phase 19 Docker/Broker automation,
  `docs/owner-validation.md`, and pre-publication version/dependency resolution
  remain open.
- P18-V01 Remoting request-context capability disposition (`b03d7fe9`,
  2026-10-16, pinned Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`):
  `NettyRequestProcessor` declares `processRequest(ChannelHandlerContext,
  RemotingCommand)` and default `rejectRequest()`. Rust maps those operations
  to the shared identity-preserving `RequestContext` and `reject_request()`;
  generic allocator, handler propagation, arbitrary-object write, promise, and
  raw Netty APIs remain framework-specific under the Rust/Tokio API contract.
  The Phase 3 source/call-site mapping and Common API map now record this
  disposition. Remoting all-feature `p18_` tests pass (11), including context
  identity and no-deadline channel writes; processor executor/rejection and
  pipeline-order regressions pass (1 each). This closes the Netty context
  capability question only; processor error-remark formatting is accepted as
  diagnostic-only under the core flow-parity rule. Broad P18-V01 reconciliation
  remains open.
- P18-V01 Common shared-alias reverse-export disposition (`b03d7fe9`,
  2026-10-16, pinned Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`):
  the Phase 8 method matrix now records all public `SharedMessageBody` and
  `SharedMessageProperties` operations, the shared `Message` constructors and
  accessors, and their mapping to Java array/Map alias behavior. These are
  synchronized Rust ownership handles for existing Java capabilities, not
  additional RocketMQ operations. The Common fixture differential and facade
  public-type test pass (1 each); fixture SHA-256:
  `0346a246d0f22440ed71773ac1854ad510acf857954d85f7ee9b8a451afc6471`.
  This closes the named alias-handle reverse-export slice only; broad P18-V01
  reconciliation remains open.
- P18-V01 Remoting GO_AWAY post-swap retry follow-up (`bc970e10`, 2026-10-16,
  pinned Java
  client source `efa1be59d6fc747ad220f036c86030f05f0419f9`):
  `NettyRemotingClient.invokeImpl` looks up the wrapper by the responding old
  channel; after `ChannelWrapper.reconnect` swaps its current future, a later
  GO_AWAY from that old channel sees the wrapper's different current channel
  and retries through it. Rust now retries that retired generation through the
  shared replacement. The regression receives the first replacement retry
  before sending the second old-channel GO_AWAY, then verifies both requests
  complete through that one replacement. Java's `tryLock` can still fail for
  a callback racing before the current-channel swap; this test does not claim
  that every simultaneous ordering retries. Remoting all-features tests pass
  (104 unit, 8 codec, 1 fuzz regression, 7 security, 102 transport, 8 doctests),
  as do strict Remoting Clippy, formatting, and diff checks. This closes the
  deterministic post-swap behavior slice only; broad P18-V01 and final-revision
  full-workspace P18-V02 remain open.
- Current-revision server-free P18-V02 rerun (code `bc970e10`; evidence tree
  `464fad73`, 2026-10-16): default and all-features locked workspace tests,
  all-target/all-feature locked check and strict Clippy, pinned Maven `verify`
  (37 scenarios, Java fixture checks and Java/Rust cross-decodes), comparator
  self-test and 37-scenario manifest, offline race/leak smoke (16 cases × 3),
  and fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations, 146 frame
  decodes, 113 message decodes) pass. Release examples and workspace benches
  compile; locked metadata resolves 215 packages including 208 registry
  packages with license metadata; all seven package listings include LICENSE
  and NOTICE; workspace metadata/dependency checks pass. UTC and Asia/Taipei
  Unique ID fixture tests pass (4 each); formatting and diff checks pass. This
  closes P18-V02 server-free gates on the tested code. Broad P18-V01,
  current-revision GitHub Ubuntu/P18-V04 and native Windows runtime CI evidence,
  Phase 19 Docker/Broker automation and scenario execution,
  `docs/owner-validation.md`, and pre-publication version/dependency resolution
  remain open.
- Current-revision server-free P18-V02 rerun (code `ff138fa3`, 2026-10-16):
  default and all-feature locked workspace tests, strict all-target/all-feature
  Clippy, pinned Maven `verify` (37 scenarios, updated mutable-hook/pipeline
  fixture and Java/Rust cross-decodes), comparator self-test and manifest,
  offline race/leak smoke (16 cases × 3), fuzz smoke (105 frame seeds, 11
  message seeds, 1,856 mutations, 146 frame decodes, 113 message decodes),
  release examples, workspace bench compilation, formatting, and diff checks
  pass. Remoting after-response mutation now covers request/response commands;
  request-pipeline mutation now checks response opaque propagation. Broad
  P18-V01 remains open for the broad public API/settings and source-flow audit.
  The Java `UtilAll.exceptionSimpleDesc` versus Rust error-remark difference is
  accepted as diagnostic text only; text-for-text equivalence is not claimed.
  Current-revision GitHub Ubuntu/P18-V04 and
  native Windows runtime CI evidence, Phase 19 Docker/Broker automation and
  scenario execution, `docs/owner-validation.md`, and pre-publication
  version/dependency resolution remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T174604Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T174604Z.rlog).
- Current-revision server-free P18-V02 rerun (code `15c6ca4f`, 2026-10-16;
  compatibility-document disposition committed separately as `ebeb6460`):
  formatting, default and all-features locked workspace tests, strict
  all-target/all-feature workspace Clippy, pinned Maven `verify` (37 scenarios,
  Java runtime probes and Java/Rust fixture cross-decodes), comparator self-test,
  37-scenario manifest validation, offline race/leak smoke (16 cases × 3), fuzz
  smoke (105 frame seeds, 11 message seeds, 1,856 mutations, 146 frame and
  113 message decodes), release example build, workspace benchmark compilation,
  locked Cargo metadata, and final diff checks all pass. This closes the
  current-revision P18-V02 server-free gates. Broad P18-V01 source/API/settings,
  overload/input/error/control-flow and reverse-export reconciliation remains
  open. Current-revision GitHub Ubuntu address-profile and native Windows
  runtime CI evidence, Phase 19 Docker/Broker automation and live scenario
  execution, `docs/owner-validation.md`, and pre-publication version/registry
  dependency resolution remain open.
- Phase 18 offline performance measurement (`15c6ca4f`, 2026-10-16): ran the
  release benchmark on the recorded Apple M5 Max / Rust 1.85.0 environment for
  all five fixed-input operations and allocation sampling. Input sizes,
  iteration counts, timing and allocation results are recorded in
  [PHASE_18_OFFLINE_BENCHMARK_BASELINE.md](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
  These are machine-specific observations without a pass/fail threshold.
- P18-V01 Remoting `RemotingClient` public inherent-method scan (`f853cb69`,
  2026-10-16): all-feature Rustdoc JSON enumerated 64 public inherent methods.
  The unused public `invoke_name_server_with_peer` helper was made private; a
  compile-fail doctest guards that boundary. The compatibility crosswalk now
  names every remaining inherent method, including the connect-timeout and
  GO_AWAY live accessors and both owned-processor registration methods. The
  exact-name scan reports zero unlisted `RemotingClient` methods. Remoting
  doctests (9), strict Remoting Clippy, and Core route RPC regressions (5) pass.
  This closes that bounded method-name/public-surface slice only; the broader
  Java/Rust source, overload/input/error/control-flow, inherited API, and
  cross-crate reverse-export audit remains open. See
  [PHASE_3_REMOTING_INVENTORY.md](compatibility/PHASE_3_REMOTING_INVENTORY.md).
- P18-V01 Remoting semaphore helper API slice (`978791b3`, 2026-10-16, pinned
  Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): added the
  Rust `client::SemaphoreReleaseOnlyOnce` equivalent for Java's public
  constructor, `release`, and `getSemaphore`; `ResponseFuture::new` and
  `new_with_release_once` now map the pinned Java constructor overloads, while
  the Tokio-only `new_with_permit` remains private under a compile-fail doctest.
  `fixtures/remoting/response-future-api.json` pins both Java constructor/method
  inventories and release/null/identity behavior (SHA-256
  `4fc0509fee67637d2e1655a39dfb810ecac19e264289c6634448fa85f38aab4c`). The
  Remoting all-feature suite passes (105 unit, 8 codec, 1 fuzz, 7 security,
  107 transport, and 10 doctests), as do strict Remoting Clippy, Maven verify
  (37 scenarios and the new reflection fixture), formatting, and diff checks.
  This closes the helper and constructor slice only; broad P18-V01 and the
  final-revision workspace gates remain open.
- P18-V01 Remoting `ResponseFuture` callback failure continuation
  (`b597ccda`, 2026-10-16):
  the pinned Java fixture covers cause retention for send, timeout, and generic
  callback failures, fail-before-complete order, and the null-request generic
  failure before callback dispatch. Rust now retains the cause in all three
  typed error paths and preserves the null-request failure. Remoting all-feature
  tests pass (106 unit, 8 codec, 1 fuzz regression, 7 security, 107 transport,
  and 10 doctests); strict Remoting Clippy, formatting, diff checks, and full
  Maven `verify` (37 scenarios and runtime fixtures) pass. This closes this
  callback-failure slice only. `ResponseFuture.toString`, `waitResponse`
  interruption semantics, broad P18-V01, final-revision workspace gates,
  current-revision GitHub Ubuntu/P18-V04 evidence, Phase 19 Docker/Broker
  automation, and release version/registry checks remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T185506Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T185506Z.rlog).
- P18-V01 Remoting `ResponseFuture.toString` and `waitResponse` disposition
  (`db03c206`, 2026-10-16, pinned Java source
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): Rust `Display` now includes
  Java's stable state fields in the same order and follows the response-latch
  count. The fixture records Java's external-thread `InterruptedException`
  and cleared interrupt flag; Rust's standard Condvar has no external thread
  interrupt facility, while the separate `ResponseFuture::interrupt()` remains
  mapped. Stable display/latch and interruption-boundary fixtures pass.
  Fixture SHA-256: `4739b82304af6d251fe963f87f90f829215add80632976ccd6d9ef411c6494f4`.
  Remoting all-feature tests pass (108 unit, 8 codec, 1 fuzz regression, 7
  security, 107 transport, 10 doctests); strict Remoting Clippy, format/diff
  checks, and Maven `verify` (37 scenarios, including the new Java fixture)
  pass. Broad P18-V01 and full workspace P18-V02 remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T190618Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T190618Z.rlog).
- P18-V01 Remoting `TransportError` construction-helper surface (`c3e669d6`,
  2026-10-16, pinned Java source `efa1be59d6fc747ad220f036c86030f05f0419f9`):
  `timeout`, `timeout_with_cause`, `connect_source`, and `tls_source` are now
  crate-internal helpers; the redundant `send_source` factory was removed.
  Public typed variants remain available. `ErrorSource` is mapped to Java
  `Throwable` message/cause/type-inspection capability, and the Rust value
  equality difference is recorded as non-flow diagnostic behavior. Five
  compile-fail doctests guard helper visibility. Remoting all-feature tests
  pass (108 unit, 8 codec, 1 fuzz regression, 7 security, 107 transport, and
  15 doctests); strict all-target/all-feature Remoting Clippy, workspace format
  check, and diff check pass. This closes only the identified error-factory
  visibility slice; broad P18-V01 and final-revision full-workspace P18-V02
  remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T191756Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T191756Z.rlog).
- P18-V01 Remoting `ResponseFuture` declared-method crosswalk (2026-10-16,
  pinned source `efa1be59d6fc747ad220f036c86030f05f0419f9`): the inventory now
  lists the Rust mapping for every Java-declared method, including nullable
  command/channel values, mutable-command closures, timeout/latch methods, and
  the `Display` mapping for `toString`. The source file at the pinned commit and
  the current Java worktree have the same SHA-256
  `63789320d327604995dadfa80ae126f9b321e50571fca68d67d371fb5fa065bf`; the
  invoked fixture pins both constructors and all 20 declared public methods
  (fixture SHA-256
  `4739b82304af6d251fe963f87f90f829215add80632976ccd6d9ef411c6494f4`). This
  closes the method-crosswalk documentation slice only; broad P18-V01 and the
  final-revision full-workspace P18-V02 remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T192429Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T192429Z.rlog).
- P18-V01 ACL `AclUtils` reflected public surface (`abde18c6`, 2026-10-16,
  pinned Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): the
  invoked Java fixture records the public no-argument constructor and all 22
  declared public static signatures. The Core regression asserts that exact
  list and exercises each corresponding Rust wrapper; whitespace and out-of-
  range `isScope(String)` inputs are included. Pinned and current Java
  `AclUtils.java` SHA-256: `86a4a1f968685a6ef3392a0d8b67f0af8301a70e2dbace74de0908a4be7f7fa3`;
  fixture SHA-256: `bb2e97058c0d8509d8143e0a21bd67bad51c0079d8753627db86086973667c4f`.
  Focused Core test, strict all-target/all-feature Core Clippy, workspace format
  and diff checks pass; Maven `verify` passes. This closes the declared
  `AclUtils` surface slice only; broad P18-V01 and final-revision full-workspace
  P18-V02 remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T194232Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T194232Z.rlog).
- P18-V01 request/reply timeout-helper reverse scan (`aa4ce064`, 2026-10-16,
  pinned Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): the
  public Rust `RequestResponseFuture::is_timeout_at(Instant)` helper was only
  used by the Core runtime registry and had no Java public counterpart. It is
  now runtime-internal; a Core compile-fail doctest guards the public boundary,
  and the Phase 15 matrix records the disposition. The pinned Java
  `RequestResponseFuture.java` SHA-256 is
  `08f2a1eec7e492d95fd9502e3b942960df948674182de5632b7b02d0514d95d4`; the
  pinned source and Maven 5.5.1 artifact both omit the newer-worktree-only
  `acquireCountDownLatch` method. Core all-feature tests (168 unit plus all
  integration tests and 14 doctests), strict Core Clippy, Core Rustdoc JSON,
  format and diff checks pass. This closes only this Rust-only helper surface;
  broad P18-V01 and final-revision full-workspace P18-V02 remain open. Execution
  log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T195635Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T195635Z.rlog).
- P18-V01 `RequestResponseFuture` method-level crosswalk (2026-10-16): the
  Phase 15 matrix now maps the constructor and every one of the 18 public Java
  methods to the exact Rust API, including the `CountDownLatch` adapter and
  async wait representation. The pinned Java source SHA-256 is
  `08f2a1eec7e492d95fd9502e3b942960df948674182de5632b7b02d0514d95d4`; the
  invoked fixture SHA-256 is
  `295307a1f559b7afa30179b48a83b0ac008716a8a1b421872b440c6ff097153c`.
  This closes the declared-method crosswalk slice only; broad P18-V01 and the
  final-revision P18-V02 workspace gate remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T195832Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T195832Z.rlog).

- P18-V01 BatchAck inherited serialization (`bd228919`, 2026-10-16, pinned
  Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`):
  `BatchAckMessageRequestBody` now maps inherited compact/pretty JSON, encode,
  decode/list decode, fromJson and their nullable results. The fixture pins
  both constructors, all 19 declared `BatchAck` methods, the four declared body
  methods, and all 13 callable body methods including inherited overloads.
  Three serializer cases and the 111-case body input matrix pass through the
  Java-compatible Rust adapters. Protocol all-feature tests (118 unit tests,
  integration targets and 42 doctests), strict Protocol Clippy, Maven verify
  (37 scenarios), formatting and diff checks pass. Empty
  `JSONWriter.Feature[]` is covered; non-empty writer features remain outside
  this adapter. Broad P18-V01 and the final-revision full-workspace P18-V02
  gate remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T201832Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T201832Z.rlog).
- P18-V01 CheckClient inherited serialization continuation (2026-10-16, pinned
  Java source commit `efa1be59d6fc747ad220f036c86030f05f0419f9`): Rust now
  exposes inherited compact/pretty JSON, instance/static encode, decode/list
  decode, `fromJson`, and nullable adapters for `CheckClientRequestBody`. The
  invoked fixture pins all nine inherited Java signatures and covers default,
  explicit-null, populated nested subscription data, and Java null utility
  results including a null list member. Protocol all-feature tests (119 unit
  tests, integration targets and 42 doctests), strict Protocol Clippy, Maven
  verify (37 scenarios), formatting and diff checks pass. Empty
  `JSONWriter.Feature[]` is recorded; non-empty writer features and the broader
  inherited DTO audit remain open. Pinned Java source SHA-256:
  `769c533d9d6940c97ffcca009b6b2089a1e2c6eb251b517b9dbb247f5085505a`;
  fixture SHA-256:
  `b40aca9ee7eecbcfccb272757fd0f7371f953ffece7634352b0bcd91079d24c0`.
  The final-revision full-workspace P18-V02 rerun remains open.
- P18-V01 inherited RemotingSerializable body adapters (`96756a8e`,
  2026-10-16, pinned Java source commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): adapters now cover the mapped
  body DTOs, including `GetConsumerStatusBody` and `ResetOffsetBody` through
  their existing MessageQueue-key codecs. `QueueTimeSpan` timestamp getters
  and derived JSON fields match Java across UTC and Asia/Taipei, including
  signed-millisecond extremes. Protocol all-feature tests (120 unit tests,
  integration targets, and 42 doctests), strict Protocol Clippy, Maven verify
  (37 scenarios), and the Core focused regression under both timezones pass.
  Empty `JSONWriter.Feature[]` is covered; non-empty writer features remain
  unresolved. Fixture SHA-256:
  `468bcf8a39cfb5f8c9706a474501897f370c42dd8f747b414c3a1d1d91a2402e`.
  This closes only the no-feature inherited serializer slice. Broad P18-V01,
  the final-revision full-workspace P18-V02 run, current-revision platform CI
  evidence, and Phase 19 local Docker/Broker automation remain open.
- P18-V01 inherited RemotingSerializable writer-feature inventory (worktree,
  2026-10-16, pinned Java source commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): the invoked fixture enumerates
  all 48 `JSONWriter.Feature` values and records feature-specific output for 32
  representative cases across 15 inherited body DTO types. Twenty-two
  features changed at least one sampled output; the other 26 made no change
  in these samples, which does not establish that they are inert for every
  valid input. The probe explicitly passes a `Feature[]` to resolve the Java
  instance varargs overload. Pinned offline Maven verify passes all 37
  scenarios. Fixture SHA-256:
  `5a10759a1b73504c0476b0d08c0f922ab74c5aa04f41b9203e6bfd8a8a7d2d6c`;
  Java fixture source SHA-256:
  `b5bdc4a43c7f28d05ef6b91b546c62e3b48124b683e8440176d419689b3962e7`.
  This is probe evidence only: Rust has no feature-aware encoder yet, and the
  P18-V01 serializer API/behavior slice remains open. Broad P18-V01, the
  final-revision P18-V02 workspace gate, current-revision platform CI evidence,
  and Phase 19 local Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T210508Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T210508Z.rlog).
- P18-V01 inherited RemotingSerializable writer-feature implementation
  (commit `2261c5b1`, 2026-10-16; pinned Java source commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9`): added the public
  `JavaJsonWriterFeature` inventory with all 48 Java names and ordinals, and
  `java_encode_with_features` adapters for 15 body DTOs, both route DTOs, and
  `HeartbeatData`. `WriteNonStringValueAsString` now matches invoked Java
  output, including special `MessageQueue` and route numeric map keys. The
  other 47 features are explicitly rejected as unmapped; enum inventory and
  probe coverage do not close their behavior. The current worktree passes
  `cargo test -p rocketmq-classic-protocol --all-features --locked` (121 unit
  tests, integration targets, 42 doctests), strict all-target/all-feature
  Protocol Clippy, pinned Maven `verify` (37 scenarios), `cargo fmt --all
  -- --check`, and `git diff --check`. Fixture SHA-256 values: `lookup.json`
  `5a10759a1b73504c0476b0d08c0f922ab74c5aa04f41b9203e6bfd8a8a7d2d6c`,
  `route.json` `189b34d28d4719b44839a7cc08ab8e209f711b613285a6a6baad194378829cd4`,
  and `heartbeat.json`
  `459adaf81a93ca4d1cb08075fcc65c4db7b75c6a9582ffd0500b499283585bda`.
  This closes one writer-feature behavior slice only. Broad P18-V01, the
  final-revision full-workspace P18-V02 gate, current-revision platform CI
  evidence, package/pre-publication review, and Phase 19 local Docker/Broker
  automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T214928Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T214928Z.rlog).
- P18-V01 inherited RemotingSerializable `PrettyFormat` feature continuation
  (commit `cf51df04`, 2026-10-16): `java_encode_with_features` now applies the
  Java tab-indented pretty output on all 15 body DTOs, both route DTOs, and
  `HeartbeatData`. The Java writer-feature output fixture is compared byte for
  byte, including `QueryConsumeTimeSpanBody` timezone projections. Protocol
  all-feature tests (121 unit tests, all integration targets, 42 doctests),
  strict Protocol Clippy, formatting, and diff checks pass. Two of 48 writer
  features now have behavior mappings; 46 remain unmapped. The final-revision
  full-workspace P18-V02 gate must be rerun after this commit. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T220515Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T220515Z.rlog).
- P18-V01 Fastjson2 space-indent writer features (commit `c9d502a3`,
  2026-10-16): `PrettyFormatWith2Space` and `PrettyFormatWith4Space` now match
  the invoked outputs byte for byte across the 15 body DTOs, both route DTOs,
  and `HeartbeatData`. The source-backed precedence test confirms 4-space
  formatting wins when both options are supplied. Protocol all-feature tests
  (121 unit tests, all integration targets, 42 doctests), strict Protocol
  Clippy, format, and diff checks pass. Four of 48 features now have behavior
  mappings; 44 remain unmapped. The final-revision full-workspace P18-V02 gate
  is still pending after this change. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T221138Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T221138Z.rlog).
- P18-V01 `WriteBooleanAsNumber` and combined feature precedence (2026-10-16):
  Java fixtures now pin the individual and combined outputs across inherited
  body, route, and heartbeat serializers. Fastjson2 stringifies booleans when
  both `WriteBooleanAsNumber` and `WriteNonStringValueAsString` are supplied;
  Rust matches that precedence. Five of 48 writer features now have behavior
  mappings; the other 43 remain unmapped. Protocol all-feature tests (121 unit
  tests, all integration targets, 42 doctests), strict Protocol Clippy, Maven
  verify (37 scenarios), format, and diff checks pass. Fixture SHA-256 values:
  `lookup.json` `c0398f15a506db742fff5bc9c019ffe5b944649c08cce8b97847cc6403bffcdf`,
  `route.json` `ad46456edced9bd7152c27ad4af06c45273f9a3ea715067d507fc732c551b4d7`,
  `heartbeat.json`
  `b9ffb50d9624ca531076444b2529e3a729baacf0edfc8361d93a19d1ab7df797`.
  Broad P18-V01, final-revision full-workspace P18-V02, current-revision
  platform CI evidence, package/pre-publication review, and Phase 19 local
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T222453Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T222453Z.rlog).
- P18-V01 `EscapeNoneAscii` writer feature (commit `2dcbb98e`, 2026-10-16):
  inherited body, route, and heartbeat serializers now match the pinned Java
  output. Invoked Unicode cases cover a Latin-1 character that remains raw, a
  high BMP character escaped as `\uXXXX`, and supplementary characters emitted
  as surrogate pairs. Six of 48 writer features now have behavior mappings;
  the other 42 remain unmapped. Protocol all-feature tests (122 unit tests, all
  integration targets, 42 doctests), strict Protocol Clippy, Maven verify (37
  scenarios), format, and diff checks pass. Fixture SHA-256 values: `lookup.json`
  `ad6ab8868be11ad924d10ff34a9542e59018fb883a355dc6ac2fe0fca7182977`,
  `route.json` `e7199dea0c92a60fecbc569a048981511c74ba6281e873e2a7d1ea84135d7e56`.
  Broad P18-V01, final-revision full-workspace P18-V02, current-revision
  platform CI evidence, package/pre-publication review, and Phase 19 local
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T223610Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T223610Z.rlog).
- P18-V01 Fastjson empty-value writer features (code commit `dc791e63`,
  2026-10-16): `NotWriteEmptyArray` now omits empty collection and map
  properties; `IgnoreEmpty` also omits empty strings. Invoked body, route, and
  heartbeat fixture cases cover both features, including empty route maps and
  nested DTO collections. Eight of 48 writer features now have behavior
  mappings; the other 40 remain unmapped. Protocol all-feature tests (122 unit
  tests, all integration targets, 42 doctests), strict Protocol Clippy, Maven
  verify (37 scenarios), formatting, and diff checks pass. The prior
  full-workspace default/all-feature runs were on parent `c7c26cc0`; the final
  workspace P18-V02 gate must be rerun after this code commit. Broad P18-V01,
  current-revision platform CI evidence, package/pre-publication review, and
  Phase 19 local Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T225647Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T225647Z.rlog).
- P18-V01 `NotWriteDefaultValue` writer feature (code commit `9f7870fe`,
  2026-10-16): inherited body, route, and heartbeat serialization now omits
  Java default-valued bean fields while retaining map entry values and the
  pinned `autoCommit` output. The Java fixtures cover populated nested DTOs,
  false/zero primitive defaults, route mapping IDs, and explicit class defaults.
  Nine of 48 writer features now have behavior mappings; the other 39 remain
  unmapped. Protocol all-feature tests (122 unit tests, all integration
  targets, 42 doctests), strict Protocol Clippy, Maven verify (37 scenarios),
  formatting, and diff checks pass. The final full-workspace P18-V02 gate must
  be rerun after this code commit. Broad P18-V01, current-revision platform CI
  evidence, package/pre-publication review, and Phase 19 local Docker/Broker
  automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T230936Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T230936Z.rlog).
- P18-V01 Fastjson writer feature continuation (commits `c3842532` and
  `de2d1a8f`, 2026-10-16): `WriteNonStringKeyAsString` now quotes numeric route
  map keys; `WriteLongAsString` stringifies mapped Java long-valued DTO fields
  and offset-map values, quotes long-keyed route `brokerAddrs` entries, and
  leaves integer-keyed `currIdMap` entries numeric. Eleven of 48 writer
  features now have behavior mappings; the other 37 remain unmapped. Protocol
  all-feature tests (122 unit tests, all integration targets and 42 doctests),
  strict all-target/all-feature Protocol Clippy, pinned Maven verify (37
  scenarios), formatting and `git diff --check` pass on `de2d1a8f`. The final
  workspace P18-V02 gate, broad P18-V01 audit, current-revision platform CI,
  package/pre-publication review and Phase 19 Docker/Broker automation remain
  open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T232526Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T232526Z.rlog).
- P18-V01 Fastjson field-name writer features (code commit `9026c715`,
  2026-10-16): `UseSingleQuotes` and `UnquoteFieldName` now match the invoked
  outputs for all mapped body DTOs, route DTOs, and `HeartbeatData`. The writer
  preserves Fastjson's boolean/enum property-name behavior, distinguishes map
  entries from bean properties, and handles single-quote escaping together
  with `EscapeNoneAscii`. Thirteen of 48 writer features now have behavior
  mappings; the other 35 remain unmapped. Protocol all-feature tests (123 unit
  tests, all integration targets, 42 doctests), strict all-target/all-feature
  Protocol Clippy, Maven verify (37 scenarios), formatting, and diff checks
  pass. The final-revision workspace P18-V02 rerun remains due after the
  remaining Phase 18 code changes. Broad P18-V01, current-revision platform CI,
  Phase 19 Docker/Broker automation, owner-validation handoff, and pre-release
  version/registry checks remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T235429Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261016T235429Z.rlog).
- P18-V01 Fastjson non-serializable writer features (code commit `bc5642df`,
  2026-10-17): `IgnoreNoneSerializable` returns Java's fixture-pinned `null`,
  `ErrorOnNoneSerializable` maps the fixture-pinned `JSONException` failure,
  and `IgnoreNoneSerializable` takes precedence when both are set. Body, route,
  and heartbeat fixtures cover default, null, and populated DTO cases. Fifteen
  of 48 writer features now have behavior mappings; 33 remain unmapped.
  Protocol tests (123 unit tests, all integration targets, 42 doctests),
  default/all-feature workspace tests, strict workspace Clippy, pinned Maven
  verify (37 scenarios), differential self-test/manifest, race/leak smoke
  (16 cases x 3), fuzz smoke (1,856 mutations), release examples, workspace
  bench compilation, metadata/package checks, formatting, and diff checks pass.
  P18-V02 server-free local gates are complete on `bc5642df`; broad P18-V01,
  current-revision Ubuntu/Windows CI results, Phase 19 Docker/Broker automation,
  owner-validation handoff, and pre-publication version/registry checks remain
  open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T001244Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T001244Z.rlog).
- P18-V01 `WriteEnumUsingOrdinal` (code commit `5f08a240`, 2026-10-17): enum-valued
  protocol bean fields now serialize as Java ordinals; same-named string values
  in map properties are preserved, including nested `messageRequestModeMap`.
  Sixteen of 48 writer features have behavior mappings; 32 remain unmapped.
  Protocol all-feature tests (125 unit tests, all integration targets, and 42
  doctests), strict Protocol Clippy, Maven verify (37 scenarios), formatting,
  and diff checks pass on `5f08a240`. The full-workspace P18-V02 gate remains
  due after the remaining Phase 18 code changes. Broad P18-V01, current-revision
  Ubuntu/Windows CI, package/provenance review, owner-validation handoff, and
  Phase 19 Docker/Broker automation and scenario execution remain open.
  Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).
- P18-V01 Fastjson `BrowserCompatible` writer feature (code commit `8c22e355`,
  2026-10-17): mapped serializers now quote numeric route-map keys and emit
  mapped integral values outside JavaScript's exact integer range as strings.
  Invoked Java fixtures cover both safe boundaries and values just outside
  them, plus route-map keys. Seventeen of 48 writer features now have behavior
  mappings; 31 remain unmapped. Protocol all-feature tests (125 unit tests, all
  integration targets, and 42 doctests), strict Protocol Clippy, Maven verify
  (37 scenarios), formatting, and diff checks pass. Broad P18-V01, final
  full-workspace P18-V02, current-revision Ubuntu/Windows CI, P18-V03 package
  review, P18-V04 handoff, and Phase 19 Docker/Broker automation remain open.
  Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).
- P18-V01 Fastjson `OptimizedForAscii` writer feature (code commit `8a9939b9`,
  2026-10-17): accepted as an output-preserving option. Fastjson 2.0.63 uses a
  UTF-8 writer path for the option; Rust's protocol JSON writer already emits
  UTF-8. Java/Rust fixture comparisons remain byte-identical for ASCII and
  Unicode values across body, route, and heartbeat serializers. Eighteen of 48
  writer features now have behavior mappings; 30 remain unmapped. Protocol
  all-feature tests (125 unit tests, all integration targets, and 42 doctests),
  strict Protocol Clippy, Maven verify (37 scenarios), formatting, and diff
  checks pass. Broad P18-V01, final full-workspace P18-V02, current-revision
  Ubuntu/Windows CI, P18-V03/V04, and Phase 19 Docker/Broker automation remain
  open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).

- P18-V01 Fastjson `WriteNameAsSymbol` mapping (code commit `4d07ad5a`,
  2026-10-17): pinned RocketMQ 5.5.1 `RemotingSerializable.encode` writes
  UTF-8 text JSON through Fastjson2 `JSON.toJSONBytes`/`JSONWriter.ofUTF8`;
  `WriteNameAsSymbol` affects binary JSON type-name paths, so this text output
  remains byte-identical. Existing invoked Java feature fixtures cover the
  mapped body DTOs, route DTOs, and HeartbeatData; Rust assertions compare the
  output against each Java baseline. No fixture regeneration was needed.
- Validation on `4d07ad5a`: Protocol all-feature suite passed (125 unit tests,
  all integration targets, 42 doctests); strict all-target/all-feature
  Protocol Clippy, formatting, `git diff --check`, and Maven verify (37
  scenarios, fixture drift and cross-decodes) passed. Writer feature mapping is
  now 19/48; 29 remain unmapped. Broad P18-V01, the final full-workspace
  P18-V02 rerun, current-revision Ubuntu/Windows CI, P18-V03/V04, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).

- P18-V01 Fastjson `NotWrite*ClassName` writer flags (code commit `812a3ff9`,
  2026-10-17): the four suppression flags are output-preserving when passed
  alone because `WriteClassName` remains disabled in this UTF-8 text encoder.
  Pinned Fastjson 2.0.63 checks three flags only while writing type metadata;
  `NotWriteSetClassName` has no writer callsite. Existing Java fixture output
  cases cover CheckClient, BatchAck, all mapped body adapters, both route DTOs,
  and HeartbeatData. The Rust differential compares each output to its Java
  baseline. Combinations with the still-unmapped `WriteClassName` remain
  unsupported.
- Validation on `812a3ff9`: Protocol all-feature suite passed (125 unit tests,
  all integration targets, 42 doctests); strict all-target/all-feature
  Protocol Clippy, formatting, `git diff --check`, and Maven verify (37
  scenarios, fixture drift and cross-decodes) passed. Writer feature mapping is
  now 23/48; 25 remain unmapped. Broad P18-V01, the final full-workspace
  P18-V02 rerun, current-revision Ubuntu/Windows CI, P18-V03/V04, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).
- P18-V01 CheckClient/BatchAck writer-flag test follow-up (code commit
  `4ae3735a`, 2026-10-17): their dedicated inherited-serialization tests now
  compare Java fixture baselines for `WriteNameAsSymbol` and all four
  `NotWrite*ClassName` flags across default, explicit-null, and populated
  cases. The Java oracle probes every pinned writer feature and records changed
  output or exceptions; an absent override therefore represents the unchanged
  Java baseline. The focused Protocol command
  `cargo test -p rocketmq-classic-protocol --all-features --locked
  inherited_remoting_serialization_matches_invoked_java` passed (3 matching
  tests, including the two dedicated DTO tests); formatting and diff checks
  passed. This closes the named test-coverage slice only. The final full-workspace
  P18-V02 rerun, broad P18-V01 audit, 25 unmapped writer features,
  current-revision Ubuntu/Windows CI, P18-V03/V04, and Phase 19 Docker/Broker
  automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).
- P18-V01 inert DTO writer-feature mapping (code commit `b627c15e`,
  2026-10-17): `WriteByteArrayAsBase64`, `WritePairAsJavaBean`,
  `WriteThrowableClassName`, and `WriterUtilDateAsMillis` preserve output for
  the mapped body, route, and heartbeat DTO graph. Pinned Fastjson2 call sites
  limit these options to byte arrays, Apache Pair values, Throwable values,
  and `java.util.Date`; the mapped graph has none of those fields. BatchAck's
  BitSet uses RocketMQ's custom serializer, and QueueTimeSpan has long fields
  with derived date strings. Invoked Java fixtures show no output change, and
  Rust now compares each adapter against those baselines. Protocol all-feature
  tests (125 unit tests, all integration targets, 42 doctests), strict Protocol
  Clippy, Maven verify (37 scenarios, fixture checks and cross-decodes),
  formatting, and diff checks pass. Mapping count is now 27/48; 21 writer
  features remain unmapped. The final full-workspace P18-V02 rerun, broad
  P18-V01 audit, current-revision Ubuntu/Windows CI, P18-V03/V04, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).
- P18-V01 numeric writer-format mapping (code commit `b25f6a79`, 2026-10-17):
  `WriteBigDecimalAsPlain` and `WriteFloatSpecialAsString` preserve output for
  the mapped body, route, and heartbeat DTO graph. Pinned Fastjson2 uses these
  options only for BigDecimal, Float, and Double values; none appear in this
  graph's fields or map-value types, and the invoked Java fixture records no
  output changes. Rust now compares every mapped adapter against the Java
  baseline. Protocol all-feature tests (125 unit tests, all integration
  targets, 42 doctests), strict Protocol Clippy, formatting, and diff checks
  pass. Mapping count is now 29/48; 19 writer features remain unmapped. The
  final full-workspace P18-V02 rerun, broad P18-V01 audit, current-revision
  Ubuntu/Windows CI, P18-V03/V04, and Phase 19 Docker/Broker automation remain
  open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T002853Z.rlog).
- P18-V01 Fastjson `BrowserSecure` writer-feature mapping (code commit
  `02ae239d`, 2026-10-17): standalone `BrowserSecure` escapes `<`, `>`, `(`,
  and `)` in mapped string values and quoted field names as uppercase Unicode
  escapes. The invoked body, route, and heartbeat fixtures also cover
  `BrowserSecure+UnquoteFieldName`: arbitrary route map keys remain raw, while
  string values keep the Java escapes. The `UnquoteFieldName` route case pins
  Fastjson's unquoted output for a non-identifier map key. Protocol all-feature
  tests (126 unit tests, all integration targets, 42 doctests), strict Protocol
  Clippy, format/diff checks, and Maven verify (37 scenarios, fixture checks,
  and cross-decodes) pass on `02ae239d`. The writer-feature mapping count is now
  30/48; 18 remain unmapped. The final full-workspace P18-V02 rerun, broad
  P18-V01 audit, current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase
  19 Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T014126Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T014126Z.rlog).
- P18-V01 Fastjson enum-name writer flags (code commit `d13636e8`,
  2026-10-17): `WriteEnumsUsingName` and `WriteEnumUsingToString` preserve the
  pinned Java output across body, route, and heartbeat DTO cases: the covered
  Java enum name and `toString()` values match their existing encoded strings.
  Rust now accepts both flags and checks their output against each applicable
  Java fixture. Protocol all-feature tests (126 unit tests, all integration
  targets, 42 doctests), strict Protocol Clippy, formatting, and diff checks
  pass. Maven verify passed on fixture revision `02ae239d`; this change adds no
  Java fixture changes. Mapping count is now 32/48; 16 writer features remain
  unmapped. Final full-workspace P18-V02, broad P18-V01, current-revision
  Ubuntu/Windows CI, P18-V04 handoff, and Phase 19 Docker/Broker automation
  remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T014736Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T014736Z.rlog).

- P18-V01 Fastjson `IgnoreErrorGetter` writer-feature mapping (code commit
  `cceed2ab`, 2026-10-17): the pinned Java oracle explicitly invokes the flag
  for 41 body, 8 route, and 3 heartbeat DTO cases; every encoding equals its
  default output. Rust now accepts the flag and compares each applicable DTO
  adapter with those Java fixture values. Protocol all-feature tests (126 unit
  tests, all integration targets, and 42 doctests), strict Protocol Clippy,
  formatting, and diff checks pass. Maven verify passes with 37 scenarios,
  fixture drift checks, and cross-decodes. The mapping count is now 33/48; 15
  writer features remain unmapped. Final full-workspace P18-V02, broad P18-V01,
  current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog).

- P18-V01 Fastjson `IgnoreNonFieldGetter` writer-feature mapping (code commit
  `333e24a8`, 2026-10-17): the Java oracle records the feature's filtered output
  for getter-only `QueueTimeSpan` timestamp strings and for `MessageQueue` and
  numeric route-map keys. Rust omits the three derived timestamp properties,
  serializes the affected map keys using the Java string form, and compares
  those results across the body, route, and heartbeat fixture suites. Protocol
  all-feature tests (126 unit tests, all integration targets, and 42 doctests),
  strict Protocol Clippy, formatting, diff checks, and Maven verify (37
  scenarios, fixture drift, and cross-decodes) pass. The mapping count is now
  34/48; 14 writer features remain unmapped. Final full-workspace P18-V02,
  broad P18-V01, current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase
  19 Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog).

- P18-V01 Fastjson `FieldBased` writer-feature mapping (code commit `6bce768d`,
  2026-10-17): the pinned Java fixture generator invokes every writer feature
  for 41 body, 8 route, and 3 heartbeat cases; `FieldBased` has no output
  overrides, including for `QueueTimeSpan`'s getter-derived timestamps. Rust
  accepts the flag and compares it with the default fixture bytes in those
  serializer suites. Protocol all-feature tests (126 unit tests, all
  integration targets, and 42 doctests), strict Protocol Clippy, formatting,
  diff checks, and Maven verify (37 scenarios, fixture drift, and cross-decodes)
  pass. Mapping count is now 35/48; 13 writer features remain unmapped. Final
  full-workspace P18-V02, broad P18-V01, current-revision Ubuntu/Windows CI,
  P18-V04 handoff, and Phase 19 Docker/Broker automation remain open. Execution
  log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T021935Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T021935Z.rlog).
- P18-V01 Fastjson `WriteNullBooleanAsFalse` writer-feature mapping (code
  commit `bd3e7ced`, 2026-10-17): the pinned Java oracle probes the feature for
  41 body, 8 route, and 3 heartbeat DTO cases; none records an output override.
  Rust accepts the flag and compares each current adapter's output with the
  corresponding fixture. The no-op claim is limited to these adapters; Java
  `BrokerReplicasInfo` contains a boxed Boolean, but its
  `getInSyncStateData` entrypoint belongs to the `rocketmq-tools` `MQAdminExt`
  extension, which Phase 17 explicitly excludes. Protocol all-feature tests
  (126 unit tests, all
  integration targets, and 42 doctests), strict Protocol Clippy, formatting,
  diff checks, and Maven verify (37 scenarios, fixture drift, and cross-decodes)
  pass. Mapping count is now 36/48; 12 writer features remain unmapped. Broad
  P18-V01, final full-workspace P18-V02, current-revision Ubuntu/Windows CI,
  P18-V04 handoff, and Phase 19 Docker/Broker automation remain open. Execution
  log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T022954Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T022954Z.rlog).
- P18-V01 Fastjson map-sorting writer flags (code commit `b1a071fd`,
  2026-10-17): `MapSortField` and `SortMapEntriesByKeys` sort serialized map
  entries by key. Rust's `serde_json::Value` maps are key-ordered in the
  configured dependency, so the current body, route, and heartbeat adapters
  already produce the feature output. The Java oracle records no overrides
  across 41 body, 8 route, and 3 heartbeat cases, including populated route
  maps. Protocol all-feature tests (126 unit tests, all integration targets,
  and 42 doctests), strict Protocol Clippy, formatting, diff checks, and Maven
  verify (37 scenarios, fixture drift, and cross-decodes) pass. Mapping count
  is now 38/48; 10 writer features remain unmapped. Broad P18-V01, final
  full-workspace P18-V02, current-revision Ubuntu/Windows CI, P18-V04 handoff,
  and Phase 19 Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T022954Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T022954Z.rlog).
- P18-V01 Fastjson `LargeObject` writer flag (code commit `fc4d2e30`,
  2026-10-17): Fastjson2 raises its writer array-capacity ceiling from 64 MiB
  to 1 GiB for this flag. The Rust adapter uses a growable `String` with no
  corresponding fixed ceiling, so accepting the flag does not alter its output
  or allocation policy; Rust's default large-output capacity is more permissive
  than Fastjson's default. The Java oracle records no output overrides across
  41 body, 8 route, and 3 heartbeat cases. Protocol all-feature tests (126 unit
  tests, all integration targets, and 42 doctests), strict Protocol Clippy,
  formatting, diff checks, and Maven verify (37 scenarios, fixture drift, and
  cross-decodes) pass. Mapping count is now 39/48; 9 writer features remain
  unmapped. Broad P18-V01, final full-workspace P18-V02, current-revision
  Ubuntu/Windows CI, P18-V04 handoff, and Phase 19 Docker/Broker automation
  remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T022954Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T022954Z.rlog).
- P18-V01 Fastjson `BeanToArray` writer flag (code commit `acd40aff`,
  2026-10-17): the adapters serialize mapped DTOs in Java bean-property order,
  including nested DTOs in collections and typed maps, while preserving user
  maps as maps. The transform retains null fields, applies enum ordinal/name
  behavior, and preserves queue-table map-key encoding. Body, route, and
  heartbeat fixture loops compare the feature output with invoked Java 5.5.1.
  Protocol all-feature tests (126 unit tests, all integration targets, and 42
  doctests), strict Protocol Clippy, formatting, diff checks, and Maven verify
  (37 scenarios, fixture checks, and Java/Rust cross-decodes) pass. Mapping
  count is now 40/48; the eight remaining flags are `WriteNulls`,
  `WriteMapNullValue`, `NullAsDefaultValue`, `WriteClassName`,
  `ReferenceDetection`, `WriteNullListAsEmpty`, `WriteNullStringAsEmpty`, and
  `WriteNullNumberAsZero`. Broad P18-V01, final full-workspace P18-V02,
  current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `WriteMapNullValue` writer flag (code commit `af0baada`,
  2026-10-17): the pinned Java oracle emits nullable DTO fields with this flag,
  matching its `WriteNulls` output for the mapped body, route, and heartbeat
  fixture cases. The schema-backed adapter restores those nullable fields;
  map values already represented as JSON null remain present in the Rust value
  tree. Protocol all-feature tests (126 unit tests, all integration targets,
  and 42 doctests), strict Protocol Clippy, formatting, and diff checks pass.
  Mapping count is now 42/48; six flags remain unmapped:
  `NullAsDefaultValue`, `WriteClassName`, `ReferenceDetection`,
  `WriteNullListAsEmpty`, `WriteNullStringAsEmpty`, and
  `WriteNullNumberAsZero`. Broad P18-V01, final full-workspace P18-V02,
  current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `WriteNulls` writer flag (code commit `e7b0acde`,
  2026-10-17): schema-backed serialization restores omitted nullable fields as
  JSON `null` across mapped body, route, and heartbeat DTOs, including nested
  typed DTOs. The pinned Java fixture comparisons cover all current feature
  samples in those adapters. Protocol all-feature tests (126 unit tests, all
  integration targets, and 42 doctests), strict Protocol Clippy, formatting,
  diff checks, and Maven verify (37 scenarios, fixture checks, and Java/Rust
  cross-decodes) pass. Mapping count is now 41/48; seven flags remain unmapped:
  `WriteMapNullValue`, `NullAsDefaultValue`, `WriteClassName`,
  `ReferenceDetection`, `WriteNullListAsEmpty`, `WriteNullStringAsEmpty`, and
  `WriteNullNumberAsZero`. Broad P18-V01, final full-workspace P18-V02,
  current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `WriteNullListAsEmpty` writer flag (code commit `038bf5f8`,
  2026-10-17): schema-backed traversal converts nullable collection properties
  to empty arrays, including nested typed DTO collections, without emitting
  unrelated missing nullable fields. Protocol all-feature tests (126 unit
  tests, all integration targets, and 42 doctests), strict Protocol Clippy,
  formatting, and diff checks pass. Mapping count is now 43/48; five flags
  remain unmapped: `NullAsDefaultValue`, `WriteClassName`,
  `ReferenceDetection`, `WriteNullStringAsEmpty`, and `WriteNullNumberAsZero`.
  Broad P18-V01, final full-workspace P18-V02, current-revision Ubuntu/Windows
  CI, P18-V04 handoff, and Phase 19 Docker/Broker automation remain open.
  Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `WriteNullStringAsEmpty` writer flag (code commit
  `68941489`, 2026-10-17): schema-backed serialization replaces nullable
  String fields and nullable String members in typed collections with empty
  strings across mapped body, route, and heartbeat DTOs. Protocol all-feature
  tests (126 unit tests, all integration targets, and 42 doctests), strict
  Protocol Clippy, formatting, and diff checks pass. Mapping count is now
  44/48; four flags remain unmapped: `NullAsDefaultValue`, `WriteClassName`,
  `ReferenceDetection`, and `WriteNullNumberAsZero`. Broad P18-V01, final
  full-workspace P18-V02, current-revision Ubuntu/Windows CI, P18-V04 handoff,
  and Phase 19 Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `WriteNullNumberAsZero` writer flag (code commit
  `63a90423`, 2026-10-17): this is output-preserving for the mapped DTO graph.
  The mapped bean schemas contain no nullable numeric fields; pinned Fastjson2
  map and collection writers serialize null entries with `writeNull()` without
  applying the number-null feature. Java feature fixtures record no output
  overrides. Protocol all-feature tests (126 unit tests, all integration
  targets, and 42 doctests), strict Protocol Clippy, formatting, and diff
  checks pass. Mapping count is now 45/48; three flags remain unmapped:
  `NullAsDefaultValue`, `WriteClassName`, and `ReferenceDetection`. Broad
  P18-V01, final full-workspace P18-V02, current-revision Ubuntu/Windows CI,
  P18-V04 handoff, and Phase 19 Docker/Broker automation remain open. Execution
  log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `NullAsDefaultValue` writer flag (code commit `a54512da`,
  2026-10-17): DTO-schema traversal emits empty strings for nullable String
  fields, empty arrays for nullable collections, and empty objects for nullable
  maps and nested DTO references. Nullable enum fields remain `null`, matching
  Fastjson2 2.0.63. Invoked Java writer fixtures cover body, route, and
  heartbeat defaults, explicit nulls, nested DTOs, and nullable String
  collection members. Protocol all-feature tests (126 unit tests, all
  integration targets, and 42 doctests), strict all-target/all-feature Protocol
  Clippy, formatting, and diff checks pass. Mapping is now 46/48; `WriteClassName`
  and `ReferenceDetection` remain unmapped. Broad P18-V01, final full-workspace
  P18-V02, current-revision Ubuntu/Windows CI, P18-V04 handoff, and Phase 19
  Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `ReferenceDetection` writer flag (code commit `eb991847`,
  2026-10-17): serialization tracks shared `SharedValue` handles and emits the
  pinned Fastjson `$ref` path for repeated DTO instances. The invoked Java route
  fixture reuses one `BrokerData` object in two list positions; Rust matches
  `$.brokerDatas[0]`. Protocol all-feature tests (127 unit tests, all
  integration targets, and 42 doctests), strict all-target/all-feature Protocol
  Clippy, formatting, diff checks, and Maven verify (37 scenarios, fixture
  checks, and Java/Rust cross-decodes) pass. The final internal-marker refactor
  also passes the focused shared-route reference test and strict Protocol
  checks. Mapping is now 47/48; `WriteClassName` remains unmapped. Broad P18-V01,
  final full-workspace P18-V02, current-revision Ubuntu/Windows CI, P18-V04
  handoff, and Phase 19 Docker/Broker automation remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
- P18-V01 Fastjson `WriteClassName` writer flag (code commit `07d2fba1`,
  2026-10-17): serialization now emits Java root and polymorphic DTO type
  metadata, typed map metadata and MessageQueue map keys, `Set` markers, and
  `L` suffixes for small values held in Java `long` fields. The combined
  `NotWriteRootClassName`, `NotWriteHashMapArrayListClassName`, and
  `NotWriteSetClassName` policies are also mapped. Body, route, and heartbeat
  fixture loops compare the feature output with invoked Java 5.5.1. Protocol
  all-feature tests pass (127 unit tests, all integration targets, 42 doctests);
  strict all-target/all-feature Protocol Clippy, formatting, and diff checks
  pass. Writer feature inventory is now 48/48. The broad P18-V01 API/source/
  input/reverse-export audit, final full-workspace P18-V02, current-revision
  Ubuntu/Windows CI evidence, and Phase 19 local Docker/Broker automation
  remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).
