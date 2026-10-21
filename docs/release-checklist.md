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
  broad P18-V01 audit remains open. The compact `SendMessageRequestHeaderV2`
  slice (`227876c1`) now also matches Java's nullable compact fields, inherited-field
  handling by fast versus reflective decode, malformed-number behavior, and
  V1 conversion of `brokerName` to `n`; the invoked fixture is
  `fixtures/remoting/send-message-request-v2-decode-modes.json`. This closes
  only that header slice and leaves the broad P18-V01 audit open.
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

- [x] **P18-V02 current-revision rerun** — Code commit `e9fe95cd` (2026-10-17):
  default and all-feature workspace tests, strict workspace Clippy, Maven
  `verify` (37 scenarios, fixture drift checks, Java/Rust cross-decodes),
  comparator self-test and 37-scenario manifest, race/leak smoke (16 cases ×
  3), fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations), release
  examples, workspace bench compilation, workspace metadata/dependency/license
  checks, seven package listings with `LICENSE`/`NOTICE`, format and diff checks
  all pass. The race rerun first exposed drift between a request's captured
  absolute deadline and its mutable future timeout. Registry expiration now
  honors the captured deadline; the focused regression and full smoke pass.
  This closes P18-V02's server-free local gates on this revision. Broad P18-V01,
  current-revision Ubuntu/Windows CI evidence, P18-V04, Phase 19 Docker/Broker
  automation and local scenario execution remain open. The pinned official
  RocketMQ 5.3.4 image digest was verified; basic Compose setup/readiness was
  subsequently checked as recorded below.
  Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T024941Z.rlog).

- Phase 19 initial basic Docker profile (`2026-10-17`): Compose uses the pinned
  official RocketMQ 5.3.4 image and a project-scoped network/store volume,
  loopback-bound host ports, explicit disposable-test marker/profile, bounded
  readiness polling, and project-only volume cleanup. The NameServer and Broker
  became healthy; `mqadmin clusterList` reported Broker `broker-a` at
  `broker:10911`, version `V5_3_4`. Cleanup removed this run's containers,
  network, and volume. A root-owned fresh-volume issue was fixed with a
  one-shot volume owner initialization service. This verifies only the basic
  environment startup and cleanup path; Java/Rust client scenarios, runner,
  resource reset/evidence reconciliation, and two-Broker/TLS/auth profiles
  remain automatic Phase 19 work. No scenario had been marked as passed at this initial setup checkpoint. Evidence:
  [`integration-tests/README.md`](../integration-tests/README.md),
  [`rlogs/2026/10/17-phase-19-basic-broker-profile__20261017T043317Z.rlog`](../rlogs/2026/10/17-phase-19-basic-broker-profile__20261017T043317Z.rlog).

## Phase 19 local progress (2026-10-17)

- [x] Add an unpublished `rocketmq-classic-integration` workspace package,
  Java 5.5.1 route probe, and fail-closed differential selector. Code commits:
  `72266b44`, `0fa704f8`, `21ef9f4e`, `adae9d1f`, and `1a77ac68` (Maven JDK 27 /
  Rust 1.85.0 gate, evidence path redaction, and live heartbeat support).
- [x] Run `ROUTE-001-get-existing-topic` against the pinned Apache RocketMQ
  5.3.4 profile. Java and Rust returned the same four `broker-a` queue rows
  (IDs 0–3); scenario-topic deletion and project-only Compose cleanup passed.
  The runner marked this manifest row `PASS`. Latest evidence:
  [`20261017T052003Z`](compatibility/evidence/phase-19/ROUTE-001-get-existing-topic/20261017T052003Z/).
- [x] Run `ROUTE-002-topic-not-exist`; Java and Rust both returned `NO_ROUTE`,
  and project-only Compose cleanup passed. Evidence:
  [`20261017T051903Z`](compatibility/evidence/phase-19/ROUTE-002-topic-not-exist/20261017T051903Z/).
- [x] Run `HEARTBEAT-001`; Java and Rust sent the same encoded producer
  heartbeat and received Broker version `481`. Project-only Compose cleanup
  passed. Evidence:
  [`20261017T053144Z`](compatibility/evidence/phase-19/HEARTBEAT-001/20261017T053144Z/).
- [x] Run `PROD-001-sync-send` against the pinned basic profile. Java and Rust
  both returned `SEND_OK`; each client read the other's message by physical offset
  and matched topic, queue, queue offset, and body. Topic and project-only
  Compose cleanup passed. Code/evidence commits: `95e38101`, `398c8ebd`. Evidence:
  [`20261017T054628Z`](compatibility/evidence/phase-19/PROD-001-sync-send/20261017T054628Z/).
- [x] Run `PROD-002-async-send` against the pinned basic profile. Java's
  callback fired once; Rust's Future resolved once; both returned `SEND_OK` and
  cross-read the other's message by physical offset. Topic and project-only
  Compose cleanup passed. Code/evidence commits: `6f2a0c12`, `aa35cc45`. Evidence:
  [`20261017T055737Z`](compatibility/evidence/phase-19/PROD-002-async-send/20261017T055737Z/).
- [x] Run `PROD-003-oneway-send` against the pinned basic profile. Both clients
  returned without a Broker response; the queue max offset advanced to 1 and 2,
  and each language read the other's message at queue offsets 0 and 1. Topic,
  containers, network, and volume cleanup passed. Code commit: `48ed3f58`.
  Evidence: [`20261017T063258Z`](compatibility/evidence/phase-19/PROD-003-oneway-send/20261017T063258Z/).
- [x] Run `PROD-004-batch-send` against the pinned basic profile. Both languages
  returned `SEND_OK` with two physical offset IDs; each read both messages from
  the other client with matching bodies and queue offsets. Topic and Compose
  project/network/volume cleanup passed. Code commit: `8262d722`.
  Evidence: [`20261017T065458Z`](compatibility/evidence/phase-19/PROD-004-batch-send/20261017T065458Z/).
- [x] After adding the integration package, rerun default and all-feature
  workspace tests, strict workspace Clippy, Maven `verify`, format, workspace
  metadata/license checks, comparator self-test, and 37-row manifest
  validation. Seven publishable packages each list `LICENSE` and `NOTICE`; the
  integration runner remains `publish = false`.
- [ ] Close broad P18-V01 Java source/API/settings/input/error/control-flow and
  reverse-export audit. The 48/48 JSON writer-feature mapping is complete, but
  it does not close the remaining API and behavior inventory.
- [x] Rerun the remaining Phase 18 offline checks on the revision containing
  the integration package: `bash tests/stress/run-offline-smoke.sh` passed 16
  race/leak cases × 3; fuzz smoke passed 105 frame seeds, 11 message seeds,
  1,856 mutations, 146 frame decodes, and 113 message decodes; release examples
  built; the offline bench target compiled. This closes the local server-free
  P18-V02 rerun on the integration-package revision.
- [x] Recheck the server-free P18-V02 gates after the batch probe change on
  `5e677be3` (probe source `8262d722`): default and all-feature workspace tests,
  all-target/all-feature strict Clippy, format, timezone-sensitive unique-ID
  test, workspace metadata/license check, 16 race/leak cases × 3, fuzz smoke,
  release examples, and offline bench compilation all passed. Maven `verify`
  passed on the same probe source commit; final comparator and 37-row manifest
  validation passed after evidence reconciliation.
- [ ] Obtain current-revision Ubuntu/Windows CI evidence and complete P18-V04.
- [ ] Implement and run the remaining 30 of 37 Phase 19 live scenarios, the
  required two-Broker/TLS/ACL/feature profiles, and full `run-local.sh --all`
  evidence reconciliation. `tests/differential/run.sh --all` currently fails
  closed on unimplemented IDs.
- [ ] Complete pre-publication version and registry dependency-resolution
  checks. No publish, tag, or push was performed.
- [ ] Owner checks MAN-01–MAN-04 are blocked pending approved topology,
  identity/resources, maintenance procedure, and workload inputs; MAN-05 is
  pending classification after local matrix execution. See
  [`docs/owner-validation.md`](owner-validation.md).

The seven live PASS results do not constitute Phase 19 acceptance. All
remaining locally reproducible cases stay automatic work; only the
owner-specific checks belong in the manual handoff.

## Phase 18 API audit update (2026-10-17)

- P18-V01 Remoting command flag-helper slice (test commit `f017a1df`): the
  pinned Java 5.5.1 fixture now invokes response/oneway marking and predicates
  for default, oneway, combined, repeated, and unrelated-high-bit cases. The
  Rust fixture test checks flags, command type, and both predicates; only
  `remotingEnumMethodCases.commandFlags` was added to the generated fixture.
  The full Remoting all-feature suite passed (108 unit, 8 codec, 1 fuzz, 7
  security, 107 transport tests, 15 doctests), strict Remoting Clippy, format,
  diff, and Maven `verify` (37 scenarios and fixture/cross-decode checks).
  This closes the flag-helper test slice only. The broad P18-V01 source/API/
  settings/input/error/control-flow and reverse-export audit remains open.
  Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog).
- P18-V01 `RemotingCommand` suspended-state API slice (code commit `811d6dda`):
  Rust now exposes the Java false-default boolean state with getter/setter
  methods, and the invoked Java fixture compares default, true, and reset-to-
  false outcomes. Encoding keeps this non-wire state out of command headers.
  Remoting all-feature tests, strict Clippy, format, diff, and Maven `verify`
  passed. The pinned Java `writeResponse` flow also emits the suspended value
  as the long-polling RPC metric label; Rust has no corresponding remoting
  metric emission yet, so that behavior and the broader P18-V01 audit remain
  open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog).

- P18-V01 `RemotingCommand` public method inventory (test commit `b6129174`):
  the pinned Java fixture now reflects all 57 declared public methods and
  confirms there is no public constructor. The audit identifies remaining
  factory/request-ID helpers, serialization metadata/header helpers, attached
  custom-header accessors, process timer/callback-list methods, command display
  behavior, and long-poll RPC metric emission. The matching crosswalk marks
  each family OPEN or PARTIAL; the inventory is a guard against omission, not
  compatibility evidence for those families. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T015123Z.rlog).
- P18-V01 `RemotingCommand` protocol-helper slice (code commit `1d0d50b7`):
  Rust now exposes request-ID generation, protocol-type marking/lookup, header
  length extraction, and serialization-type configuration matching the pinned
  Java setting precedence and validation. Invoked Java fixtures compare the
  helper results, including unknown protocol bytes and isolated property/env
  cases. Remoting all-feature tests, strict Clippy, formatting, diff checks,
  and Maven `verify` (37 scenarios, fixture drift and Java/Rust cross-decodes)
  pass. This closes those helper rows only: Java command factories and opaque
  initialization, attached custom-header behavior, process timer/callback-list,
  long-poll RPC metric emission, and the broad P18-V01 audit remain open. The
  complete workspace P18-V02 gate must be rerun after this revision. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T075137Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T075137Z.rlog).
- P18-V01 `RemotingCommand.toString()` slice (code commit `dfcfc890`): Rust
  `Display` matches invoked Java output for null fields, populated state,
  single-entry ext-fields, and negative 32-bit flag formatting. Remoting
  all-feature tests (110 unit, 8 codec, 1 fuzz regression, 7 security, 107
  transport, 15 doctests), strict Clippy, format/diff checks, and Maven `verify`
  (37 scenarios and fixture/cross-decode gates) pass. Multi-entry ext-field
  rendering still follows Rust's sorted map order instead of Java's
  `HashMap` iteration order, so this method remains PARTIAL. Process timer and
  callback-list APIs, factories, long-poll metric emission, the broad P18-V01
  audit, and the final full-workspace P18-V02 rerun remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T075137Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T075137Z.rlog).

- P18-V01 `RemotingCommand` factory slice (code/test commit `a0fe9c3e`,
  2026-10-17): Rust now has request, response, and error factories, typed
  request/response header overloads, class-header `Default` forms, configured
  version handling, and process-wide request-ID increments. Eight invoked Java
  fixture cases compare flags, fields, version, ext-fields, serialization
  type, and request-ID deltas. Full Remoting all-feature tests passed (111
  unit, 8 codec, 1 fuzz regression, 7 security, 107 transport, and 15 doctests);
  strict all-target/all-feature Clippy, formatting, diff checks, and Maven
  `verify` passed (37 scenarios, fixture drift, and Java/Rust cross-decodes).
  `Default` still has Rust-specific language/opaque initialization, custom
  header retention/reflection failure behavior is not equivalent, and the
  multi-entry `toString()` ordering, process timer/callback-list, long-poll
  metric, and broad P18-V01 audit remain open. This is a method-family slice,
  not closure of P18-V01 or the final P18-V02 gate. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T083404Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T083404Z.rlog).
- P18-V02 Remoting TLS test portability follow-up (test commit `48482b49`,
  2026-10-17): the property-file reload test now uses the repository's pinned
  TLS certificate as a local trust anchor. This keeps the test focused on
  property reload and lets it run on hosts without native system trust roots.
  The test and the complete Remoting all-feature suite pass with this change.
- P18-V01 `RemotingCommand` custom-header slice (code/test commit `d0ecaf1e`,
  2026-10-17): typed headers remain attached until explicit materialization or
  encoding; both JSON and RocketMQ codecs include the header fields without
  changing the pre-encode ext-field map. Rust mutable borrows preserve the
  attached header's in-command mutation flow, and cached decode returns the
  same typed instance when requested. Invoked Java cases cover attach/read,
  null clearing, post-attach mutation, direct and cached decoding, ext-field
  merge, `addExtField`, `addExtFieldIfNotExist`, and its null-map exception.
  Rust maps the null-map failure to `ProtocolError` and uses owned typed
  headers with a compile-time trait contract instead of Java aliases and
  reflection. The fixture SHA-256 is
  `48b1ac80746dbb12d22b4da2c6a100b0a19f19e78b65035e5d88243c82d6e582`.
  Remoting all-feature tests pass (112 unit, 8 codec, 1 fuzz regression, 7
  security, 107 transport, and 15 doctests); strict all-target/all-feature
  Remoting Clippy, workspace all-feature check, formatting, diff checks, and
  Maven `verify` pass (37 scenarios, fixture drift, and Java/Rust cross-decodes).
  `Default` initialization, process timer/callback-list, long-poll metric,
  multi-entry `toString()` ordering, and broad P18-V01 remain open. The final
  full-workspace P18-V02 run is still due after the remaining Phase 18 changes.
  Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T090444Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T090444Z.rlog).
- P18-V01 `RemotingCommand` process timer and callback-list slice (pending code
  and documentation commits, 2026-10-17): Rust now exposes timer and callback
  getters/setters, maps the Java stopwatch start/stop/reset state behavior to
  `ProcessTimer`, and attaches a running timer to each inbound decoded command.
  The invoked Java fixture covers null defaults/clears, same-instance access,
  timer state/errors, callback-list mutation, and callback invocation. The Rust
  transport regression confirms inbound requests reach processors with a
  running timer. Fixture SHA-256:
  `8a7ae4f5b258cff9ad87462c5568fe363d079605c487e5327dc672d69981b4c8`.
  Remoting all-feature tests pass (113 unit, 8 codec, 1 fuzz regression, 7
  security, 107 transport, and 15 doctests); focused transport coverage,
  strict Remoting Clippy, workspace all-feature check, formatting, diff checks,
  and Maven `verify` pass (37 scenarios, fixture drift and Java/Rust
  cross-decodes). This closes the timer/callback-list method slice only.
  `Default` initialization, long-poll RPC metric emission, multi-entry
  `toString()` ordering, custom-header representation details, broad P18-V01,
  and final-revision full-workspace P18-V02 remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T091405Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T091405Z.rlog).
- P18-V01 `RemotingCommand::default()` follow-up (pending code and
  documentation commits, 2026-10-17): default commands now match the pinned
  Java constructor's `JAVA` language, zero-valued fields, configured
  serialization type, null transient fields, and process-wide opaque ID
  increment. Factory construction now consumes one opaque ID per command.
  The invoked fixture reflects the protected Java constructor and the Rust
  child-process regression compares its defaults without global request-ID or
  serialization-property interference. Fixture SHA-256:
  `b1dc81c3c70894dfd4e669a59027590bb6d79499a8c34e4c87ca1b6d60cb70ab`.
  Remoting all-feature tests pass (113 unit, 8 codec, 1 fuzz regression, 7
  security, 107 transport, and 15 doctests); strict Remoting Clippy, workspace
  all-feature check, format/diff checks, and Maven `verify` pass (37 scenarios,
  fixture drift, and Java/Rust cross-decodes). This closes the default-state
  difference only. Long-poll RPC metric emission, multi-entry `toString()` map
  ordering, custom-header representation details, broad P18-V01, and final
  full-workspace P18-V02 remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T092123Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T092123Z.rlog).
- P18-V01 Remoting RPC-latency metrics and code-description slice (code commit
  `0d72386c`, 2026-10-17): Rust now has a per-client metrics manager and records
  inbound oneway, successful/failed/cancelled response writes, executor-submit
  failures, long-poll state, and Java-compatible request/response code labels.
  The invoked Java 5.5.1 fixture covers the manager API, metric-view boundaries,
  result labels, 166 request codes, 68 response codes, and unknown-code decimal
  fallback. The Rust fixture differential and loopback test cover supplied
  attributes and observed response metrics. Full Remoting all-feature tests
  pass (115 unit, 8 codec, 1 fuzz regression, 7 security, 107 transport, and 15
  doctests); strict all-target/all-feature Remoting Clippy, format/diff checks,
  and Maven `verify` pass (37 scenarios, fixture drift, and Java/Rust
  cross-decodes). This closes only the Remoting metrics/code-description slice;
  multi-entry `toString()` ordering, custom-header representation details,
  broad P18-V01, and final-revision full-workspace P18-V02 remain open. Execution
  log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T100912Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T100912Z.rlog).
- [x] Run the current-revision server-free P18-V02 gate on `bdf56557` after
  Remoting metrics: default and all-feature workspace tests, workspace
  all-target/all-feature strict Clippy, format/diff, pinned Maven verify (37
  scenarios, fixture drift and cross-decodes), comparator self-test, 37-row
  scenario manifest, race/leak smoke (16 cases × 3), bounded fuzz smoke,
  release examples, workspace bench compilation and benchmark, locked Cargo
  metadata, metadata/license/dependency-direction checks, and workspace package
  file lists all passed. This is current-revision evidence; rerun the gate after
  any later source change. The integration test package remains unpublished;
  Cargo emitted a non-blocking missing-description warning for it during the
  package-list command. Broad P18-V01, current-revision platform CI/P18-V04,
  P18-V03 registry resolution, and locally runnable Phase 19 Broker/Docker
  scenarios remain open. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T100912Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T100912Z.rlog).
- P18-V01 RemotingCommand display-order audit (test commit `a0ee6aef`,
  2026-10-17): an invoked Java 5.5.1 fixture now includes two ext-fields whose
  observed `HashMap` rendering is `{a=lower, Z=upper}`. The Rust differential
  verifies that Java's rendered entries equal the field map, then compares the
  complete display after normalizing only entry order. The pinned source uses
  `HashMap` and its iteration order is unspecified; Rust retains deterministic
  `BTreeMap` order. The API crosswalk now records this as a verified
  representation disposition. It also now reflects completed command defaults,
  process timer/callback APIs, and long-poll metrics. The broad P18-V01 audit
  remains open; direct Java null ext-field key/value inputs are a newly
  identified surface Rust's current `HeaderMap<String, String>` cannot
  represent. Full workspace P18-V02 must be rerun after this test/fixture
  revision. Execution log:
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T103126Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T103126Z.rlog).
- P18-V03 unpublished integration package metadata (commit `9e8476c4`,
  2026-10-17): added a package description to remove Cargo's package-list
  warning. Integration package tests, locked workspace metadata, the
  dependency-direction/license check, and workspace package file listing pass;
  `rocketmq-classic-integration` remains unpublished. Registry dependency
  resolution remains pending after Phase 19.
- P18-V01 Remoting nullable ext-field input slice (code/test commit `c07d3ac2`,
  2026-10-17): `ExtFields` now preserves Java `HashMap` null-key/null-value
  entries, nullable `addExtField` adapters match `putIfAbsent`, JSON encodes a
  null key as the string `"null"` and omits null values, and RocketMQ binary
  map encoding omits null entries. The invoked Java fixture and Rust
  differential pass; the targeted Remoting all-feature test passes, and all
  119 Remoting unit tests pass. The complete Remoting test command then stalled
  while launching the `codec` integration binary at macOS `_dyld_start`; a
  process sample confirmed the launch stall, so the suite is not recorded as a
  pass. Maven `verify` passes all 37 scenarios, fixture checks, and Java/Rust
  cross-decodes. `cargo test --workspace --all-features --locked --no-run`
  completed successfully on this code revision in 38 minutes, compiling every
  workspace test target including `live_client`. Full test execution and the
  current-revision P18-V02 gate remain open. Broad P18-V01 remains open.
- [x] Current-revision server-free P18-V02 rerun on code commit `b6da737e`
  (documentation HEAD `2b059963`): default and all-feature workspace tests,
  all-target/all-feature strict workspace Clippy, formatting, Maven `verify`
  (37 scenarios, fixture drift, and Java/Rust cross-decodes), comparator
  self-test, 37-scenario manifest, 16 deterministic race/leak cases × 3,
  bounded fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations),
  release examples, offline benchmark, locked metadata, workspace
  metadata/license/dependency checks, seven package file lists, and the
  Asia/Taipei Unique ID tests all pass. Rust tests ran with Rust 1.85.0 in an
  offline Linux AArch64 container; `--init` was required to restore unblocked
  signal defaults inherited by the container runtime. The host macOS workspace
  test launch remains blocked by AMFI rejecting its newly built unsigned
  `codec` test binary, so that launch is not counted as a pass. Broad P18-V01
  and current-revision GitHub Ubuntu/native Windows evidence for P18-V04 remain
  open. No Broker, release, push, tag, or publication action occurred.

## Current code revision gate — 2026-10-17

- [x] P18-V02 server-free gate passed on code revision `05448c25`, containing
  `40ebb7ba` (custom-header validation), `fcc001a7` (header-prefix encoders),
  and `05448c25` (heartbeat-tolerant admin test). Default and all-feature
  workspace tests passed with Rust 1.85.0 in `rust:1.85.0-bookworm` using
  `docker run --init` and the offline cargo cache. Workspace all-target,
  all-feature strict Clippy, formatting, and diff checks passed. Maven
  `verify` passed on host OpenJDK 27, including 37 scenarios, Java fixture
  drift checks, and Java/Rust cross-decodes. The differential comparator
  self-test and 37-row manifest validation passed; deterministic race/leak
  smoke passed 16 cases × 3, and fuzz smoke passed 105 frame seeds, 11 message
  seeds, and 1,856 mutations. Release examples and the offline benchmark ran
  successfully. Locked workspace metadata, metadata/license/dependency checks,
  seven publishable-package file lists, and four Asia/Taipei Unique ID tests
  passed. Detailed commands and benchmark observations are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T141001Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T141001Z.rlog).
- These code changes close only the custom-header validation and Remoting
  header-prefix API slices. Broad P18-V01 source/API/settings/input/error/flow
  and reverse-export reconciliation remains open. Current-revision GitHub
  Ubuntu/native Windows evidence for P18-V04 is still absent. The Phase 19
  manifest has 37 cases; 30 local Broker/Docker scenarios and required
  two-Broker/TLS/ACL profiles remain automatic work. Release version and
  registry dependency-resolution checks remain pending before publication.
  Owner-specific MAN checks remain separate final validation. No plan files
  changed; no push, tag, publication, or deployment occurred.

- P18-V01 RemotingCommand identity equality (`4eb99671`, 2026-10-17): the
  pinned Java 5.5.1 fixture resolves `equals` and `hashCode` to `java.lang.Object`
  and shows two distinct commands with the same visible state remain unequal
  and occupy two `HashSet` entries. Rust no longer derives structural
  `PartialEq`/`Eq`; a compile-fail doctest guards that API, and affected codec,
  response-future, and transport assertions now compare wire fields or error
  variants. Fixture SHA-256:
  `a539bc204e2f2c03077af07dad37bc4a58c631ce2fa24d8e9e69f50999e4c9b0`.
  Remoting tests pass (123 unit, 8 codec, 1 fuzz regression, 7 security, 107
  transport, 17 doctests), default/all-feature workspace tests pass on
  `rust:1.85.0-bookworm` with `--init`, strict workspace Clippy, formatting,
  diff checks, and Maven `verify` (37 scenarios, fixture drift, and
  cross-decodes) pass. The final P18-V02 gate remains subject to rerun after the
  remaining Phase 18 implementation work. Broad P18-V01, current-revision
  Ubuntu/Windows P18-V04 evidence, Phase 19 live scenarios, and pre-publication
  checks remain open.

- P18-V01 `ProcessTimer` identity equality (`4858ebdd`, 2026-10-17): Guava
  `Stopwatch` inherits identity equality from `Object`; the invoked fixture
  verifies equal stopped state, getter identity, and two entries for distinct
  timers in a `HashSet`. Rust omits structural `PartialEq`/`Eq` from
  `ProcessTimer` and guards the public trait boundary with a compile-fail
  doctest. Fixture SHA-256:
  `2680e925aa7c21cd5f2f5ea8055ee986bec5fc174601c612cb8ae469481d2cf0`.
  Linux Remoting all-feature tests, strict Remoting Clippy, format/diff checks,
  and Maven verify pass. The macOS all-feature workspace run stalled in Rust
  compilation and was interrupted; the later Linux ARM64 current-revision
  rerun is recorded below. Details for this identity slice are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T144935Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T144935Z.rlog).

## Current code revision gate — 2026-10-17 (`ac0a16f3`)

- [x] P18-V02 server-free local gates pass on code revision `ac0a16f3`,
  including ProcessTimer identity parity (`4858ebdd`). Default and all-feature
  workspace tests and strict all-target/all-feature workspace Clippy pass on
  Rust 1.85.0 in a Linux ARM64 `rust:1.85.0-bookworm` container with
  `--init`. The initial default run exposed a transient `/proc/self/task`
  `ESRCH` race in Linux jstack enumeration; `ac0a16f3` skips entries for
  disappeared tasks, and both workspace modes pass after the fix. Format and
  diff checks pass.
- Maven `verify` passes all 37 scenarios, fixture drift checks and Java/Rust
  cross-decodes. Differential comparator self-test and the 37-row manifest
  validate; deterministic race/leak smoke passes 16 cases × 3; fuzz smoke
  passes 105 frame seeds, 11 message seeds, 1,856 mutations, 146 frame decodes
  and 113 message decodes. Release examples build and the offline benchmark
  runs. Locked Cargo metadata, workspace metadata/license/dependency checks,
  all seven package file lists, and four Asia/Taipei Unique ID tests pass.
  Detailed output and benchmark observations are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T144935Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T144935Z.rlog).
- The macOS workspace test process still stalled during compilation; it is not
  counted as a pass. The Linux ARM64 run closes P18-V02's local server-free
  gate on this code revision. Broad P18-V01 reconciliation, current-revision
  GitHub Ubuntu/native Windows evidence for P18-V04, 30 remaining Phase 19
  Docker/Broker scenarios and required multi-Broker/TLS/ACL profiles, and
  release version/registry checks remain open. Owner-specific final checks
  remain a separate manual handoff. No plan, Java oracle, remote branch, tag,
  or publication target was changed.

## Current code revision gate — 2026-10-17 (`56ca072f`)

- [x] P18-V02 server-free local gate passed on code commit `56ca072f`. Default
  and all-feature workspace tests, all-target/all-feature workspace check and
  strict Clippy, formatting, and diff checks pass. Rust tests/checks ran with
  Rust 1.85.0 in a Linux ARM64 `rust:1.85.0-bookworm` container with `--init`;
  workspace tests were serialized with `--test-threads=1`.
- Maven `verify` passes all 37 scenarios, fixture drift checks and Java/Rust
  cross-decodes. Differential comparator self-test and the 37-row manifest
  validate; deterministic race/leak smoke passes 16 cases × 3; fuzz smoke
  passes 105 frame seeds, 11 message seeds, 1,856 mutations, 146 frame decodes
  and 113 message decodes. Release examples build and the offline benchmark
  runs. Locked Cargo metadata, workspace metadata/license/dependency checks,
  all seven package file lists, and UTC plus Asia/Taipei Unique ID tests pass.
  Maven emitted existing JDK `Unsafe` and reflective-final-field warnings but
  exited successfully. Detailed commands and benchmark observations are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T152044Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T152044Z.rlog).
- Broad P18-V01 source/API/settings/input/error/flow and reverse-export
  reconciliation remains open. Current-revision GitHub Ubuntu/native Windows
  evidence for P18-V04, 30 Phase 19 Docker/Broker scenarios and required
  multi-Broker/TLS/ACL profiles, and release version/registry checks remain
  open. Owner-specific deployment/live Broker checks remain a separate manual
  handoff. No plan, Java oracle, remote branch, tag, publication, Broker
  operation, or deployment was changed.

## Current code revision gate — 2026-10-17 (`86e7cb34`)

- [x] P18-V02 server-free gates pass on `86e7cb34`. The default and
  all-feature workspace test suites, all-target/all-feature workspace check
  and strict Clippy, formatting, and diff checks passed. Rust checks ran with
  Rust 1.85.0 in Linux ARM64 `rust:1.85.0-bookworm` using `--init`.
- `PullMessageRequestHeader` no longer exposes Java-absent `lite_topic`, and
  encoding omits `liteTopic`; decoding ignores an incoming value as an unknown
  field. The protocol crate tests pass, including the updated Java-header
  inventory. Maven `verify` passes all 37 scenarios, fixture checks and
  Java/Rust cross-decodes; comparator self-test and 37-row manifest validation
  pass. Race/leak smoke passes 16 cases × 3 and fuzz smoke passes 105 frame
  seeds, 11 message seeds, 1,856 mutations, 146 frame decodes and 113 message
  decodes. Release examples and the offline benchmark run. Locked metadata,
  workspace metadata/license/dependency checks, all seven package listings
  with `LICENSE`/`NOTICE`, and four Unique ID tests each under UTC and
  Asia/Taipei pass. Detailed results are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T153927Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T153927Z.rlog).
- Broad P18-V01 source/API/settings/input/error/flow and reverse-export
  reconciliation remains open. In particular, the Rust direct custom-header
  decoder still ignores the Java `useFastEncode` hint; Java's reflection path
  catches per-field missing/conversion failures before `checkFields()`, while
  Rust decoding is strict. Current-revision GitHub Ubuntu/native Windows
  evidence for P18-V04, 30 remaining Phase 19 Docker/Broker scenarios and the
  required multi-Broker/TLS/ACL/feature profiles, and pre-publication version
  and registry dependency-resolution checks remain open. Owner-specific
  deployment/live Broker checks remain the separate final manual handoff. No
  plan, Java oracle, remote branch, tag, publication, Broker operation, or
  deployment was changed.

## Current code revision gate — 2026-10-17 (`1db3a1c4`)

- [x] P18-V02 server-free gates pass on code commit `1db3a1c4`. Default and
  all-feature workspace tests, all-target/all-feature workspace check and
  strict Clippy pass on Rust 1.85.0 in Linux ARM64 `rust:1.85.0-bookworm`
  using `--init`. Host format and diff checks pass.
- Maven `verify` passes all 37 scenarios, fixture drift checks, and Java/Rust
  cross-decodes, including the new Pull header fast/reflection fixture for
  missing fields, valid fields, and invalid numeric conversions. Differential
  comparator self-test and manifest validation pass (37 scenarios). Race/leak
  smoke passes 16 cases × 3; fuzz smoke passes 105 frame seeds, 11 message
  seeds, 1,856 mutations, 146 frame decodes, and 113 message decodes. Workspace
  examples and benchmarks compile; the offline benchmark runs on macOS ARM64.
  Locked Cargo metadata, workspace metadata/license/dependency checks, seven
  package listings with `LICENSE`/`NOTICE`, and four Core Unique ID tests under
  both UTC and Asia/Taipei pass. The Java output includes existing JDK
  `Unsafe` and reflective-final-field warnings but exits successfully.
- Pull fast/reflection mode handling closes the specific decoder discrepancy
  recorded under `86e7cb34`; other header mode differences and the broad
  bidirectional P18-V01 source/API/input/error/flow and reverse-export audit
  remain open. Current-revision Ubuntu/native Windows evidence for P18-V04,
  30 remaining Phase 19 Docker/Broker scenarios and required profiles, and
  pre-publication version/registry checks remain open. Owner-specific final
  production checks remain a separate manual handoff. No plan, Java oracle,
  remote branch, tag, publication, Broker operation, or deployment was changed.

## Current code revision gate — 2026-10-17 (`01e02a5c`)

- [x] P18-V02 server-free local gates pass on code commit `01e02a5c`. Default
  and all-feature workspace tests pass in a clean Linux ARM64
  `rust:1.85.0-bookworm` container with serialized tests. All-target,
  all-feature check and strict Clippy pass with Rust 1.85.0 on macOS ARM64;
  formatting and diff checks pass.
- `SendMessageResponseHeader` now preserves Java nullable `msgId`, `queueId`,
  and `queueOffset` on direct decode, including Java's fast/reflection
  conversion behavior. The send owner still requires queueId and queueOffset;
  `SendResult.offset_msg_id` preserves absent `msgId` as `None`. Its invoked
  Java fixture SHA-256 is
  `c2962fc0c34868c36dd67ef7ef33e4694c59904b679de665ef401d302fe3818a`.
- Maven `verify` passes all 37 scenarios, fixture-drift checks, and Java/Rust
  cross-decodes. Differential self-test and the 37-row manifest validate;
  race/leak smoke passes 16 cases × 3, and fuzz smoke passes 105 frame seeds,
  11 message seeds, 1,856 mutations, 146 frame decodes, and 113 message
  decodes. Release examples and workspace benches compile; the offline
  benchmark runs. Locked metadata, package/license/dependency checks, all
  seven package listings with `LICENSE`/`NOTICE`, and four Unique ID tests
  under both UTC and Asia/Taipei pass. A first all-feature run had one stack
  capture test fail to find an unblocked signal; the isolated test, full
  consumer suite, and final default/all-feature workspace reruns passed.
  Detailed results are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T184946Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T184946Z.rlog).
- Broad P18-V01 bidirectional Java source/API/settings/input/error/flow
  reconciliation and Rust reverse-export audit remain open. Current-revision
  GitHub Ubuntu/native Windows evidence for P18-V04, 30 remaining Phase 19
  Docker/Broker scenarios and required multi-Broker/TLS/ACL profiles, and
  pre-publication version/registry dependency checks remain open. Owner-only
  production checks are the separate final manual handoff. No plan, Java
  oracle, remote branch, tag, publication, Broker operation, or deployment
  was changed.

## Latest code revision follow-up — 2026-10-17 (`227876c1`)

- [x] Default and all-feature workspace tests pass in the Linux
  `rust:1.85.0-bookworm` container. All-target/all-feature `cargo check` and
  strict Clippy pass there; formatting and diff checks pass.
- [x] Maven `verify` passed all 37 scenarios and fixture-drift/cross-decode
  checks on the worktree containing this code revision.
- [ ] Refresh the remaining P18-V02 server-free evidence on this revision:
  differential comparator and manifest checks, race/leak and fuzz smoke,
  release examples and benchmark build, locked metadata/package checks, and
  timezone-specific Unique ID checks. The previous complete run is recorded on
  `01e02a5c`; these later V2 header edits have not yet had those gates repeated.

## Pull response header follow-up — 2026-10-17 (`fcc0f3f1`)

- [x] P18-V01 `PullMessageResponseHeader` nullable and mode-specific decoding:
  the Java 5.5.1 fixture covers absent wrappers, valid numbers, and malformed
  numeric fields in fast and reflection modes. Rust preserves nullable wrapper
  values on direct decode, rejects invalid numbers in fast mode, and ignores an
  individual reflection conversion failure as Java does. The Pull RPC validates
  `nextBeginOffset`, `minOffset`, `maxOffset`, and `suggestWhichBrokerId` at the
  Java-equivalent unboxing boundary. Fixture SHA-256:
  `9ba19f1b5394e7bd6e60366770990e034e75e98f2ecff04c82059d712dc8db59`.
  Maven verify (37 scenarios, fixture drift and cross-decodes), Linux default
  and all-feature workspace tests, all-target/all-feature check and strict
  Clippy, formatting, and diff checks pass. Two test expectations were corrected
  to match Java: direct decode preserves null, and the wrapper fields are
  nullable in the validation matrix. This closes this header slice only.
- [x] P18-V01 `FastCodesHeader` production-source inventory: pinned Java
  `remoting` contains exactly four implementations, and all four have invoked
  fast/reflection fixtures plus Rust comparisons: Pull request, Pull response,
  Send response, and Send V2 request headers. Rust provides per-mode decoders
  for each; Send V2 also covers inherited fields and V1 conversion. This closes
  that bounded inventory only. Other headers' generic reflection decoding and
  broad P18-V01 reconciliation remain open.
- [x] P18-V02 server-free gates complete for code revision `fcc0f3f1`
  (documentation HEAD `29b38b4b`): comparator self-test and 37-row manifest;
  16 race/leak cases × 3; fuzz smoke (105 frame seeds, 11 message seeds, 1,856
  mutations, 146 frame decodes, 113 message decodes); release examples;
  workspace benchmark compile and offline run; locked Cargo metadata, workspace
  metadata/license/dependency checks, and all seven product package listings;
  four Core Unique ID tests under each of UTC and Asia/Taipei. Formatting and
  diff checks pass. The benchmark ran on macOS ARM64, Apple M5 Max, Rust 1.85.0:
  frame encode/decode 1,954.6 ns/iteration, Zstd message decode 1,038.8 ns,
  Producer request construction 1,181.7 ns, route lookup 15.5 ns, and loopback
  pending completion 64,728.4 ns. These are observations, not thresholds.
- Broad P18-V01 reconciliation, current-revision GitHub Ubuntu/native Windows
  P18-V04 evidence, 30 Phase 19 local Docker/Broker scenarios and required
  profiles, and pre-publication version/registry checks remain open. Owner-only
  production checks remain separate final manual items.

## Consumer offset header follow-up — 2026-10-17 (3beabab0)

- [x] P18-V01 typed consumer-offset decoder slice: the Java 5.5.1 fixture compares query/update request and query response headers with missing fields, malformed numbers, malformed setZeroIfNotFound, and valid values under both useFastEncode hints. Rust retains nullable wrapper fields, skips per-field reflection failures, and validates response offset at the RPC result boundary. Fixture SHA-256: 2e911e6369ef5260ac73ad7ecbd3db9b38d190f8b080b5d36fc46997e7f20ddb. Code commit: b59431e4.
- [x] P18-V02 server-free gates pass on code revision 3beabab0. Linux ARM64 rust:1.85.0-bookworm default and all-feature workspace tests, all-target/all-feature check and strict Clippy, and Maven verify (37 scenarios, fixture drift and Java/Rust cross-decodes) pass. Comparator self-test and the 37-row manifest pass; 16 race/leak cases × 3 and fuzz smoke (105 frame seeds, 11 message seeds, 1,856 mutations, 146 frame decodes, 113 message decodes) pass. Release examples and the offline benchmark compile and run. Locked Cargo metadata, workspace metadata/license/dependency checks, seven package-content listings, UTC/Asia-Taipei Unique ID tests, format, and diff checks pass. The benchmark ran on Linux ARM64 with Rust 1.85.0; results are observations, not thresholds.
- [x] Stabilized the live-timeout regression after the first parallel workspace run exposed a route-refresh timeout: the test now loads and checks the Broker route before mutating the timeout, then applies the Java fixture's 150 ms value to the Broker RPC. All 17 runtime_lookup integration tests pass. Test-only commit: 3beabab0.
- A macOS Remoting package run was stopped after its codec test child remained in dyld startup before Rust main; a live process sample showed _dyld_start. The complete default/all-feature workspace suites passed in the Linux container on 3beabab0.
- Broad P18-V01 bidirectional Java/Rust source/API/settings/input/error/flow and reverse-export audit remains open. Current-revision GitHub Ubuntu/native Windows P18-V04 evidence, 30 Phase 19 Docker/Broker scenarios and required profiles, and pre-publication version/registry checks remain open. Owner-specific production checks remain the separate final manual handoff. Plans and the read-only Java oracle were unchanged.

## Offset-boundary reflection follow-up and current gate — 2026-10-17 (`9513e4da`)

- [x] P18-V01 offset-boundary response-header decoding: Java 5.5.1 direct
  reflection leaves absent or malformed boxed `Long` fields null per field,
  regardless of the fast-encode hint. Rust now preserves those nullable values
  for `SearchOffsetResponseHeader`, `GetMaxOffsetResponseHeader`,
  `GetMinOffsetResponseHeader`, and `GetEarliestMsgStoretimeResponseHeader`;
  each RPC owner checks the required value at the Java-equivalent unboxing
  boundary. Invoked fixture SHA-256:
  `c4891f046639eb0f9a82e221b1605d296aed1314fb70e37c22ea514d240a9e95`.
- [x] P18-V02 server-free local gates pass on code revision `9513e4da` in a
  serialized Linux ARM64 `rust:1.85.0-bookworm` container: default/all-feature
  workspace tests, all-target/all-feature check and strict Clippy, Maven verify
  (37 scenarios, fixture drift and Java/Rust cross-decodes), differential
  self-test/manifest validation, 16 race/leak cases × 3, fuzz smoke, release
  examples, benchmark build/run, locked metadata/package checks, workspace
  metadata/license/dependency checks, seven package listings with `LICENSE` and
  `NOTICE`, UTC/Asia-Taipei Unique ID tests, formatting, and diff checks. A
  first parallel workspace run had one Consumer stack-capture failure because
  the environment exposed no unblocked signal; serialized default/all-feature
  reruns passed. Benchmark values are observations, not thresholds.
- Broad P18-V01 source/API/settings/overload/input/error/flow and Rust
  reverse-export reconciliation remains open. Current-revision GitHub Ubuntu
  and native Windows P18-V04 evidence, 30 Phase 19 Docker/Broker scenarios and
  required multi-Broker/TLS/ACL profiles, and pre-publication version/registry
  checks remain open. Owner-only production connectivity, credentials/CA,
  approved HA/fault injection, and capacity/soak checks remain the final manual
  handoff. No plan, Java oracle, remote branch, tag, publication, Broker
  operation, or deployment was changed.

## Remoting suspended response metric — 2026-10-17 (`0fdbc1d4`)

- [x] Closed the P18-V01 long-poll metric slice. Pinned Java
  `NettyRemotingAbstract.writeResponse` records `request.isSuspended()` in the
  `is_long_polling` RPC latency attribute. Rust's inbound `send_response`
  builds the same attribute from the request and records it after response
  writes or for oneway requests. The transport integration test marks an
  inbound request suspended in a hook and verifies the recorded response
  metric contains `is_long_polling=true`.
- Focused verification passed:
  `cargo test --locked -p rocketmq-classic-remoting --test transport
  p3_v03_inbound_dispatch_and_reordered_parallel_responses`, formatting, and
  `git diff --check`. Test-only commit: `0fdbc1d4`.
- Earlier checklist notes that listed long-poll metric emission as open are
  superseded by this evidence. Broad P18-V01 remains open for its other API and
  behavior rows.

## POP change-invisibility response header — 2026-10-17 (`dff482d9`)

- [x] Closed the missing `ChangeInvisibleTimeResponseHeader` public typed
  header slice. Pinned Java 5.5.1 declares its constructor, `get/setPopTime`,
  `get/setInvisibleTime`, and `get/setReviveQid`, with identity-based inherited
  equality. Rust now exports and directly decodes/encodes the header, retains
  Java's independent primitive zero defaults for absent or malformed fields,
  and matches signed limits and BMP decimal digits. The invoked fixture covers
  both decode hints, null/empty ext-field maps, malformed and valid fields,
  identity, accessors, and encoded fields. SHA-256:
  `967d3146d0da1467714c96a1fa7464c7375888e2ba09f6fc0c097126bab67c2a`.
- [x] Current-revision P18-V02 server-free gates pass on `dff482d9`:
  default/all-feature workspace tests, all-target/all-feature check and strict
  Clippy, UTC Maven `verify` (37 scenarios, fixture drift and cross-decodes),
  differential self-test and 37-row manifest, 16 deterministic race/leak cases
  × 3, fuzz smoke (106 frame seeds, 11 message seeds, 1,872 mutations, 156
  frame decodes, 110 message decodes), release examples, all workspace bench
  compilation and offline benchmark run, locked Cargo metadata, metadata/
  license/dependency-direction checks, seven package-content listings,
  UTC/Asia-Taipei Unique ID tests, formatting, and diff checks.
- The benchmark ran on macOS ARM64, Apple M5 Max, Rust 1.85.0: frame encode /
  decode 2,140.8 ns per iteration, Zstd message decode 1,104.1 ns, Producer
  request construction 1,191.8 ns, route lookup 16.4 ns, and loopback pending
  completion 64,665.5 ns. Allocation observations were 4,676; 197,493; 3,665;
  0; and 9,546 bytes per iteration. These are observations, not thresholds.
- Broad P18-V01 source/API/settings/overload/input/error/flow and Rust
  reverse-export reconciliation remains open. Current-revision GitHub Ubuntu
  and native Windows P18-V04 evidence, 30 Phase 19 Docker/Broker scenarios and
  required profiles, and pre-publication version/registry checks also remain
  open. Owner-only production checks remain the final manual handoff. No plan,
  remote branch, tag, publication, Broker operation, or deployment was touched.

## Heartbeat request header API follow-up — 2026-10-17

- [x] Closed the P18-V01 `HeartbeatRequestHeader` slice against the pinned
  Java 5.5.1 source. `sendHeartbeat` and `sendHeartbeatV2` use this
  `RpcRequestHeader` subtype, with inherited namespace, broker-name, and
  oneway fields. Rust now exports the typed header, preserves all four ext
  fields and inherited equality/hash/toString behavior, and uses it for both
  heartbeat RPCs. Java typed-header fixture SHA-256:
  `d52b9cfbb0e640702e0eb6cea45430a237ac7120513854b36f677447fc3cfe46`.
- [x] Pinned Java Maven `verify` passes all 37 scenarios and cross-decodes 56
  Rust typed-header frames. Focused Rust verification passes the 3 header
  inventory tests, the request-header identity test, the 14 Core extension
  tests, and strict all-target/all-feature Clippy for Protocol and Core.
- Broad P18-V01 reconciliation remains open. Current-revision GitHub Ubuntu
  and native Windows P18-V04 evidence, 30 Phase 19 local Broker scenarios and
  required profiles, and pre-publication registry resolution remain open.

## Current-revision P18-V02 gate — 2026-10-17 (`5e5775fa`)

- [x] The current-revision server-free gate passes after adding
  `HeartbeatRequestHeader` to the Protocol header-validation matrix. The first
  full default workspace run exposed the missing row; the focused matrix test
  and both complete default and all-feature workspace suites passed after the
  correction. Commit: `5e5775fa`.
- [x] Also passed all-target/all-feature workspace check and strict Clippy,
  format and diff checks, UTC Maven `verify` (37 scenarios, fixture drift,
  56 typed-header and 14 typed-body cross-decodes), differential comparator
  self-test and 37-row manifest validation, 16 race/leak cases × 3, fuzz smoke
  (107 frame seeds, 11 message seeds, 1,888 mutations, 165 frame decodes,
  116 message decodes), release examples, workspace bench compilation and
  offline benchmark, locked Cargo metadata, workspace metadata/license/
  dependency-direction checks, workspace package-content listing, and
  UTC/Asia-Taipei unique-ID tests. Benchmark observations and toolchain details
  are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T230853Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T230853Z.rlog).
- Broad P18-V01 Java/Rust source/API/settings/input/error/flow and
  reverse-export audit, current-revision Ubuntu/native Windows P18-V04 CI
  evidence, final P18-V03 package review, and Phase 19 live scenarios/profiles
  remain open. This rerun closes P18-V02 only.

## Current-revision P18-V02 and OpenTelemetry Attributes API — 2026-10-17 (`7e7689f6`)

- [x] P18-V01 OpenTelemetry 1.47 `AttributeKey`, immutable `Attributes`, and
  `AttributesBuilder` surface compared with the invoked Java 5.5.1 fixture.
  Rust maps all eight scalar/array kinds, typed lookup, `to_builder`, `put_all`,
  type-aware `remove`, and `remove_if`. Fixture SHA-256:
  `e15d03aac196916a0592a21a8deb2440fbce694e20bc3b0e050ba88e798a966e`.
- [x] Full P18-V02 server-free rerun on source commit `7e7689f6`: default and
  all-feature locked offline workspace tests; all-target/all-feature workspace
  check and strict Clippy; Remoting all-feature tests; Maven `verify` (37
  scenarios, fixture checks, 56 typed-header and 14 typed-body cross-decodes);
  comparator self-test and 37-row manifest; 16 race/leak cases × 3; fuzz smoke
  (107 frame seeds, 11 message seeds, 1,888 mutations); release examples,
  workspace benchmark build/run, locked Cargo and workspace metadata/package
  checks, UTC/Asia/Taipei Unique ID tests, formatting, and diff checks. The
  benchmark ran on macOS ARM64 Apple M5 Max with Rust 1.85.0; observations are
  recorded in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T235517Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261017T235517Z.rlog).
- Broad P18-V01 source/API/settings/input/error/control-flow and reverse-export
  reconciliation remains open. Current-revision Ubuntu/native Windows CI
  evidence for P18-V04 and final P18-V03 package/provenance review remain open.
  The Phase 19 live Broker matrix remains a separate phase handoff; owner-only
  production checks remain in the final manual handoff.

## CreateTopicRequestHeader reflection decoding — 2026-10-18 (`4f7f9435`)

- [x] Closed a P18-V01 header-decoding slice against the pinned Java 5.5.1
  implementation. `CreateTopicRequestHeader` does not implement
  `FastCodesHeader`, so both decode hints use reflection. Conversion failures
  are caught per field, nullable Java fields remain null, Boolean parsing maps
  non-`true` text to false, and `checkFields()` runs after the loop. Rust now
  models the corresponding nullable payload fields as `Option`, skips failed
  numeric conversions, preserves the Java default values for `order`/`force`,
  and validates `topicFilterType` after decoding. The invoked fixture covers
  both mode hints, valid/missing/malformed fields, and final validation. SHA-256:
  `20e0a1c2dd78af55b220976ad6faecc37673d0d6951954a4a0c2bc433a7053a7`.
- [x] Refreshed current-revision P18-V02 server-free gates on `4f7f9435`:
  default/all-feature workspace tests, all-target/all-feature check and strict
  Clippy, Maven `verify` (37 scenarios, fixture checks, 56 typed-header and 14
  typed-body cross-decodes), differential self-test and 37-row manifest,
  deterministic race/leak smoke (16 cases × 3), bounded fuzz smoke (107 frame
  seeds, 11 message seeds, 1,888 mutations), release examples, workspace bench
  compilation and offline benchmark, locked Cargo/workspace metadata and
  package-content checks, UTC/Asia-Taipei Unique ID tests, formatting, and diff
  checks. Full command/results are in
  [`rlogs/2026/09/19-phase-18-compatibility-release-gate__20261018T002552Z.rlog`](../rlogs/2026/09/19-phase-18-compatibility-release-gate__20261018T002552Z.rlog).
- The broader bidirectional P18-V01 audit remains open. Current-revision
  Ubuntu/native Windows P18-V04 evidence and the final P18-V03 package and
  provenance review remain open. Phase 19 Broker scenarios/profiles and owner
  checks remain separate obligations; no live Broker was contacted.

## EndTransactionRequestHeader reflection decoding — 2026-10-18 (`0961d24d`)

- [x] Closed this P18-V01 header-decoding slice against the pinned Java 5.5.1
  implementation. Both decode-mode hints use the reflection path. Per-field
  missing/invalid conversion failures are skipped; nullable offset, group, and
  message-ID values remain absent; `fromTransactionCheck` defaults to false;
  malformed Boolean values decode as false; and commit state is validated
  after field decoding. The invoked fixture covers 13 cases in both modes.
  Fixture SHA-256: `78605a0c0cd70f33553813559e2e6b4c9fe82589a08acd83ca18a9ebc616ab5d`.
- [x] Protocol header tests (12), Producer tests, Core invoked-producer fixture,
  Remoting mode fixture, Maven `verify`, affected-crate strict Clippy, format,
  and diff checks pass on code commit `0961d24d`. The Maven run verified the
  new Java fixture, 37 differential scenarios, 56 typed-header cross-decodes,
  and 14 typed-body cross-decodes.
- This closes only the EndTransaction decode slice. Broad P18-V01 remains open;
  the full workspace P18-V02 gate must be refreshed after remaining Phase 18
  implementation work. Current-revision Ubuntu/native Windows P18-V04 evidence
  and final P18-V03 package/provenance review also remain open. The Phase 19
  runner worktree changes were excluded from this commit.

## Query/admin header reflection decoding — 2026-10-18 (`66334aa9`)

- [x] Closed a P18-V01 query/admin decoder slice against pinned Java 5.5.1. `QueryMessageRequestHeader`, `QueryMessageResponseHeader`, and `ViewMessageRequestHeader` use Java's reflection decoder under both `useFastEncode` hints; Rust now preserves nullable wrapper fields and per-field malformed numeric outcomes. `ViewMessageRequestHeader` keeps Java identity equality, and Core checks the nullable query index timestamp at the Java-equivalent unboxing boundary. The invoked fixture covers both modes and valid, missing, and malformed fields. SHA-256: `abc37ff23edd774910af9093b96fce95ce5e2270c5db7996fb0607df1ef3cc68`.
- [x] Maven `verify` passed (37 offline scenarios, fixture drift, 56 typed-header cross-decodes, 14 typed-body cross-decodes). Protocol all-feature tests passed (128 unit tests, header matrix/inventory, 44 doctests); Remoting all-feature tests passed (137 unit, 8 codec, 1 fuzz-regression, 7 security, 107 transport, 18 doctests). Core `admin_rpc` (19), Producer admin (11), and Consumer admin (9) tests passed; strict all-target/all-feature Clippy passed for the five affected crates. `cargo fmt --all -- --check` and `git diff --check` pass.
- [ ] Refresh full-workspace P18-V02 gates after the latest code slice. Broad P18-V01 remains open; current-revision Ubuntu/native Windows P18-V04 evidence and final P18-V03 package/provenance review remain open. Phase 19 still lacks 30 of 37 local scenario runners, the required profiles, and a passing `run-local.sh --all` reconciliation. Pre-publication release-version and registry dependency checks remain pending. Owner-specific manual checks remain a separate handoff.

## P18-V01 `MqClientAdmin` surface inventory discovery — 2026-10-18

- [ ] Reconcile the 19 public methods declared by Java 5.5.1
  `client/MqClientAdmin.java`, exposed through
  `MQClientAPIExt.getMqClientAdmin()`. The existing Phase 17 `MQAdmin` matrix
  did not cover this separate interface. Some operations have similar Rust
  facade/Core methods, but no complete source/API/input/error/flow crosswalk
  exists. Add missing typed request/response support and invoked Java/Rust
  evidence for every method before closing P18-V01. See the inventory and
  source path in `docs/compatibility/PHASE_17_ADMIN_MATRIX.md`. This is local
  implementation work, not final manual validation.

## P18-V01 `queryTopicsByConsumer` slice — 2026-10-18

- [x] Code commit `07865bde` adds `MQClientApi::query_topics_by_consumer`,
  `QueryTopicsByConsumerRequestHeader`, and Java-compatible `TopicList` defaults,
  null handling, Set aliasing, and JSON fields. The invoked Java fixture covers
  both decoder mode hints, inherited request fields, and `TopicList` accessor
  identity/default behavior. Rust covers the decoded fields and a scripted
  successful request/response exchange.
- [x] Targeted Protocol, Remoting, and Core tests pass; strict all-target,
  all-feature Clippy passes for those three crates; Maven `verify` passes all
  37 scenarios and Java/Rust cross-decodes 57 typed headers and 14 typed bodies.
  Formatting and diff checks pass.
- [x] Follow-up code commit `59c0d8fa` adds a scripted `TOPIC_NOT_EXIST`
  response test for the error code, peer address, and remark mapping. The Core
  test, strict all-target/all-feature Core Clippy, format, and diff checks pass.
- The other 18 `MqClientAdmin` methods and broad P18-V01 reverse/API/settings
  reconciliation remain open. Refresh the full current-revision P18-V02
  workspace gates after these changes. Current-revision Ubuntu/Windows CI and
  final P18-V03 package/provenance review also remain open.

## P18-V01 `querySubscriptionByConsumer` slice — 2026-10-18 (`2ac9351b`)

- [x] Added `MQClientApi::query_subscription_by_consumer`, the typed request
  header, and `QuerySubscriptionResponseBody`. The body retains Java's nullable
  `SubscriptionData`, null-omitting JSON, and shared getter alias. Invoked Java
  fixtures cover the response DTO and both query-header decode hints, including
  missing and malformed Boolean fields. The Rust header inventory and
  validation matrix include both `queryTopicsByConsumer` and
  `querySubscriptionByConsumer`.
- [x] Protocol all-feature tests pass (130 unit tests, header inventory and
  validation, 46 doctests); Remoting all-feature tests pass (137 unit tests,
  107 transport tests, 18 doctests); Core admin RPC tests pass (22 cases).
  Strict all-target/all-feature Clippy passes for Protocol, Remoting, and Core.
  Maven `verify` passes 37 scenarios, fixture drift checks, and Java/Rust
  cross-decodes for 58 typed headers and 14 typed bodies. Formatting and diff
  checks pass.
- The other 17 `MqClientAdmin` methods and broad P18-V01 reconciliation remain
  open. Refresh the full-workspace P18-V02 gate after remaining implementation
  work. Current-revision Ubuntu/native Windows P18-V04 evidence, final P18-V03
  package/provenance review, Phase 19 local Docker/Broker scenarios and
  required profiles, and pre-publication checks also remain open. Owner-only
  production checks stay in the separate manual handoff.

## P18-V01 `queryTopicConsumeByWho` admin RPC — 2026-10-18 (`c091d507`)

- [x] Added `MQClientApi::query_topic_consume_by_who`, the typed request
  header, and Java-compatible `GroupList` body for request code 300. Invoked
  Java fixtures cover the header under both decode hints and the DTO's empty
  default, nullable Set/elements, JSON shape, and getter alias identity.
  Scripted Core RPC cases cover success, nullable response data, and Broker
  error mapping. Fixture SHA-256 values: `group-list.json`
  `d033bf2e2cf2e866ba854bf2152ab21f66f43a2bb0212caaf9dbdc4d88f5c38e`;
  request-header identity `2849daef72f0bbc29c64f311b8dc41761602845ef1256f12c6937e2ccdbea48f`.
- [x] Protocol all-feature tests pass (131 unit tests, header inventory and
  validation, 47 doctests); the Remoting query/admin decode-mode fixture and
  Core `admin_rpc` suite (25 tests) pass. Strict all-target/all-feature
  Clippy passes for Protocol, Remoting, and Core. Maven `verify` passes all
  37 scenarios and fixture checks, with 59 typed-header and 14 typed-body
  cross-decodes. Formatting and diff checks pass.
- The `RpcRequestHeader` identity fixture now covers 35 classes. This brings
  the `MqClientAdmin` source-backed method count to three complete slices,
  with 16 methods remaining to reconcile.
- [ ] Refresh full-workspace P18-V02 gates after this code slice. Broad
  P18-V01 reconciliation remains open; current-revision Ubuntu/native Windows
  P18-V04 evidence and final P18-V03 package/provenance review remain open.
  Phase 19 still needs the remaining 30 local scenario runners, required
  profiles, and passing `run-local.sh --all` reconciliation. Pre-publication
  version/registry checks remain pending; owner-environment checks stay in the
  separate manual handoff.

## P18-V01 `getTopicStatsInfo` admin RPC — 2026-10-18 (`e53ad60c`)

- [x] Added `MQClientApi::get_topic_stats_info`, request code 202's typed
  `GetTopicStatsInfoRequestHeader`, and Java-compatible `TopicStatsTable` and
  `TopicOffset` DTOs. Invoked Java fixtures cover public methods, primitive
  defaults, nullable map/accessor aliasing, MessageQueue object-key JSON and
  null map-value omission. Rust RPC regressions cover success, absent/JSON-null
  bodies, Broker errors, and malformed JSON. Fixture SHA-256:
  `12c3a17f32ffb2b3b60e1e27d57ce8a96a199cb3ba0c7b1201790461b5ecd669`.
- [x] Protocol all-feature tests passed (132 unit tests, all integration
  targets, 49 doctests); Core `admin_rpc` passed (28 tests). Strict
  all-target/all-feature Clippy passed for Protocol, Remoting, and Core after
  deriving the private fixture fields' `Default` implementation. The first
  Clippy run reported `derivable_impls`; the correction removed the warning.
  Formatting and `git diff --check` pass.
- [x] Pinned Maven `verify` passed all 37 scenarios, fixture drift checks,
  Java/Rust typed-header cross-decodes (60) and typed-body cross-decodes (14).
  Code commit: `e53ad60c`.
- The source-backed `MqClientAdmin` count is now 4/19; the other 15 methods
  and the broad P18-V01 public API/settings/input/error/flow/reverse-export
  audit remain open. Full-workspace P18-V02 must be refreshed after the
  remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and `run-local.sh --all`, and pre-publication
  version/registry checks remain open. Owner-only production validation stays
  in the separate manual handoff.

## P18-V01 `queryConsumeTimeSpan` admin RPC — 2026-10-18 (`72a37b17`)

- [x] Added an invoked Java 5.5.1 `MqClientAdminImpl.queryConsumeTimeSpan`
  fixture for request code 303. It records the direct broker address and
  timeout, all typed request fields, the decoded `QueueTimeSpan` result, and
  `MQClientException` code/remark. Fixture SHA-256:
  `5689eea4d53809d6ce2caa6bebc14ba4ebeb8b49b8fffe3000432bbe73dfaa4e`.
- [x] The Rust Core method now sends to the supplied address directly, as the
  Java `MqClientAdmin` adapter does. Push Consumer applies the configured VIP
  mapping before invoking Core, preserving the Java push facade's Broker
  address behavior. Tests cover the direct method with VIP enabled, success
  decoding and Broker error mapping, plus Push Consumer's VIP route.
- [x] Core query-time-span tests passed (2); Core `admin_rpc` passed (29); the
  focused Consumer VIP route test passed. Strict all-target/all-feature
  Clippy passed for Core and Consumer. Pinned Maven `verify` passed all 37
  scenarios and fixture checks, including 60 typed-header and 14 typed-body
  cross-decodes. Formatting and `git diff --check` pass. Code commit:
  `72a37b17`.
- The source-backed `MqClientAdmin` method count is now 5/19; the remaining 14
  methods and broad P18-V01 API/settings/input/error/flow/reverse-export audit
  remain open. Full-workspace P18-V02, current-revision Ubuntu/native Windows
  P18-V04, final P18-V03 package/provenance review, Phase 19 local Docker
  scenarios/profiles and full runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation remains the
  separate manual handoff.

## P18-V01 `updateOrCreateTopic` admin RPC — 2026-10-18 (`24eae6bf`)

- [x] Extended the invoked Java `MqClientAdminImpl` fixture with
  `updateOrCreateTopic`, request code 17, full `CreateTopicRequestHeader`
  fields, direct address, null request body, successful completion and
  `MQClientException` code/remark. The fixture is
  `fixtures/admin/query-consume-time-span.json` (shared with the preceding
  method slice), SHA-256
  `16939c20d844e02921a2634dc32af32521b8a36b5d0de27b256ae88c777056aa`.
- [x] Rust's existing `MQClientApi::create_topic` matches the Java request
  and response paths. Core admin RPC tests now compare the successful header
  and Broker error to the Java fixture; the existing permission-error test
  remains. Core `admin_rpc` passed all 30 tests; strict all-target/all-feature
  Core Clippy, formatting, and diff checks passed.
- [x] Pinned Maven `verify` passed after fixture regeneration: 37 scenarios,
  fixture drift checks, 60 typed-header cross-decodes and 14 typed-body
  cross-decodes. Test/fixture commit: `24eae6bf`.
- The source-backed `MqClientAdmin` method count is now 6/19; 13 methods and
  the broad P18-V01 public API/settings/input/error/flow/reverse-export audit
  remain open. Full-workspace P18-V02, current-revision Ubuntu/native Windows
  P18-V04, final P18-V03 package/provenance review, Phase 19 local Docker
  scenarios/profiles and full runner reconciliation, and pre-publication
  version/registry checks remain open. Owner-only validation stays separate.

## P18-V01 `deleteTopicInBroker` admin RPC — 2026-10-18 (`74c52143`)

- [x] Added `DeleteTopicRequestHeader` with Java's nullable topic and inherited
  `RpcRequestHeader`/`TopicRequestHeader` fields. Java's `checkFields()` is a
  no-op, and inherited equality/hash behavior only uses `ns`, `nsd`, `bname`,
  and `oway`. Added `MQClientApi::delete_topic_in_broker` using request code
  215, the direct address, and no request body.
- [x] The invoked Java 5.5.1 fixture covers `deleteTopicInBroker` request
  fields, timeout, success completion, and `MQClientException` response code
  and remark. Rust scripted Core tests compare the request and Broker error to
  the fixture. Java/Rust typed-header cross-decode and the request-header
  identity inventory include the new header. Shared admin fixture SHA-256:
  `2a113cbc01a9e5bdce353b6705c6ebd734a5dd33832b147135eebdf6030d177b`.
- [x] Protocol all-feature tests passed (132 unit tests, all integration
  targets, 49 doctests); Core `admin_rpc` passed all 32 tests. Strict
  all-target/all-feature Clippy passed for Protocol, Remoting, and Core.
  Pinned Maven `verify` passed all 37 scenarios and fixture checks, including
  61 typed-header and 14 typed-body cross-decodes. Formatting and diff checks
  passed.
- The source-backed `MqClientAdmin` method count is now 7/19; 12 methods and
  broad P18-V01 reconciliation remain open. Refresh full-workspace P18-V02
  after the remaining implementation work. Current-revision Ubuntu/native
  Windows P18-V04 evidence, final P18-V03 package/provenance review, Phase 19
  local Docker scenarios/profiles and runner reconciliation, and
  pre-publication version/registry checks also remain open. Owner-only
  validation stays in the separate manual handoff.

## P18-V01 `deleteTopicInNameserver` admin RPC — 2026-10-18 (`fb1e43b5`)

- [x] Added `DeleteTopicFromNamesrvRequestHeader` with nullable `topic`,
  `clusterName`, and inherited topic/RPC fields. Java's `checkFields()` is a
  no-op, and inherited equality/hash behavior uses only `ns`, `nsd`, `bname`,
  and `oway`. Added `MQClientApi::delete_topic_in_nameserver` using request
  code 216, the supplied NameServer address, and no request body.
- [x] The invoked Java 5.5.1 fixture covers the exact method signature, header
  fields, timeout, success completion, and `MQClientException` response code
  and remark. Rust scripted Core tests compare the request and error result to
  the fixture. The header is included in Java/Rust typed-header cross-decode
  and request-header identity inventory. Shared admin fixture SHA-256:
  `ee0c842383a4150957dddd40baa9a948cf536b0e00a72989ee8ee62fc71cdcc3`.
- [x] Protocol all-feature tests passed (132 unit tests, all integration
  targets, 49 doctests); Core `admin_rpc` passed all 34 tests. Strict
  all-target/all-feature Clippy passed for Protocol, Remoting, and Core.
  Pinned Maven `verify` passed all 37 scenarios and fixture checks, including
  62 typed-header and 14 typed-body cross-decodes. Formatting and diff checks
  passed.
- The source-backed `MqClientAdmin` method count is now 8/19; 11 methods and
  broad P18-V01 reconciliation remain open. Refresh full-workspace P18-V02
  after remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation stays in
  the separate manual handoff.

## P18-V01 `deleteKvConfig` admin RPC — 2026-10-18 (`7cb6fb49`)

- [x] Added `DeleteKvConfigRequestHeader` with nullable `namespace` and `key`,
  matching Java's no-op `checkFields()`. Added `MQClientApi::delete_kv_config`
  using request code 102, the supplied address, and no request body.
- [x] The invoked Java 5.5.1 fixture covers the method signature, header
  fields, timeout, success completion, and `MQClientException` response code
  and remark. Rust scripted Core tests compare the request and error mapping
  to the fixture. Java/Rust typed-header cross-decode and header inventory
  coverage include the new header. Shared admin fixture SHA-256:
  `ceafece5e4c70efab31e34760b8485b631a5691a46000c8d6bff32448fedbb0c`.
- [x] Protocol all-feature tests passed (132 unit tests, all integration
  targets, 49 doctests); Core `admin_rpc` passed all 36 tests. Strict
  all-target/all-feature Clippy passed for Protocol, Remoting, and Core.
  Pinned Maven `verify` passed all 37 scenarios and fixture checks, including
  63 typed-header and 14 typed-body cross-decodes. Formatting and diff checks
  passed. Code commit: `7cb6fb49`.
- The source-backed `MqClientAdmin` method count is now 9/19; 10 methods and
  broad P18-V01 reconciliation remain open. Refresh full-workspace P18-V02
  after remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation stays in
  the separate manual handoff.

## P18-V01 `deleteSubscriptionGroup` admin RPC — 2026-10-18 (`ec8b7c41`)

- [x] Added `DeleteSubscriptionGroupRequestHeader` with nullable `groupName`,
  Java primitive `cleanOffset` defaulting to `false`, and inherited nullable
  RPC fields. Its equality and hash behavior uses the four inherited RPC
  fields. Added `MQClientApi::delete_subscription_group` using request code
  207, the supplied Broker address, and no request body.
- [x] The invoked Java 5.5.1 fixture covers the method signature, typed header,
  timeout, success completion, and `MQClientException` response code and
  remark. Rust scripted Core tests compare the exact request and error mapping.
  Java/Rust typed-header cross-decode, the header validation matrix, and the
  request-header identity fixture cover the new header. Shared admin fixture
  SHA-256: `d2da8387de557de716a1a8eb87c4aabde190d6409ef677ddacc735c5467cac8c`;
  identity fixture SHA-256:
  `81325ef5905ee81e3fbc6af951568afdf88851220a7a0d0ecb0c3b3989d1f2ef`.
- [x] Protocol all-feature tests passed (132 unit tests, all integration
  targets, 49 doctests); Core `admin_rpc` passed all 38 tests. Strict
  all-target/all-feature Clippy passed for Protocol, Remoting, and Core.
  Pinned Maven `verify` passed all 37 scenarios and fixture checks, including
  64 typed-header and 14 typed-body cross-decodes. Formatting and diff checks
  passed. Code commit: `ec8b7c41`.
- The source-backed `MqClientAdmin` method count is now 10/19; nine methods
  and broad P18-V01 reconciliation remain open. Refresh full-workspace P18-V02
  after remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation stays in
  the separate manual handoff.

## P18-V01 `invokeBrokerToResetOffset` admin RPC — 2026-10-18 (`859c152a`)

- [x] Added `MQClientApi::invoke_broker_to_reset_offset` with request code 222
  and Java `ResetOffsetRequestHeader`. It decodes the Java `ResetOffsetBody`
  object-key map into shared queue/offset entries, preserves a nullable
  offset table, and maps a successful response without a body to Java's
  `MQClientException` response code 0.
- [x] The invoked Java fixture covers the method signature, full request
  fields, timeout, successful offset table, Broker error, and success-without-
  body error. Core scripted tests compare request fields, result contents,
  response code/remark, and the null-body branch. Shared admin fixture
  SHA-256: `dc98c4a44b5994bb30bdf55b72abae40727e852e6b7dafdd4e7b4b8e8747b9c0`.
- [x] Core `admin_rpc` passed all 41 tests. Strict all-target/all-feature
  Clippy passed for Protocol, Remoting, and Core. Pinned Maven `verify` passed
  all 37 scenarios and fixture checks, including 64 typed-header and 14
  typed-body cross-decodes. Formatting and diff checks passed. Code commit:
  `859c152a`.
- The source-backed `MqClientAdmin` method count is now 11/19; eight methods
  and broad P18-V01 reconciliation remain open. Refresh full-workspace P18-V02
  after remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation stays in
  the separate manual handoff.

## P18-V01 `queryMessage` direct admin adapter — 2026-10-18 (`ae420dc6`)

- [x] Added the Rust `MqClientAdmin` adapter and
  `MQClientApi::mq_client_admin().query_message`. It sends Java's request code
  12 through asynchronous remoting, preserves `_UNIQUE_KEY_QUERY`, applies the
  caller's body-decompression setting, filters by topic and exact key or
  unique message ID, returns an empty list for `QUERY_NOT_FOUND`, and preserves
  other Broker response codes and remarks.
- [x] The invoked Java 5.5.1 fixture covers matching and nonmatching messages,
  compressed and decompressed bodies, key and unique-key query modes,
  `QUERY_NOT_FOUND`, and Broker failure. Fixture SHA-256:
  `09f2aac28d54e335d591f92359863f5ebae152e19a9d806881bddb6c5c93c320`.
  Core scripted differential compares request fields and decoded message
  fields/body against the fixture.
- [x] Core `admin_rpc` passed all 42 tests. Core all-feature tests passed 171
  unit tests, all integration targets, and 14 doctests. Strict all-target,
  all-feature Core Clippy passed. Pinned Maven `verify` passed all 37 scenarios
  and fixture checks, including 64 typed-header and 14 typed-body cross-decodes.
  Formatting and `git diff --check` passed. Code commit: `ae420dc6`.
- The source-backed `MqClientAdmin` count is now 12/19. Seven methods remain:
  `updateOrCreateSubscriptionGroup`, `viewMessage`, `getBrokerClusterInfo`,
  `getConsumerConnectionList`, `getConsumeStats`, `getConsumerRunningInfo`,
  and `consumeMessageDirectly`. Broad P18-V01 API/settings/input/error/flow and
  reverse-export reconciliation remains open. Refresh full-workspace P18-V02
  after remaining implementation work; current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation remains the
  separate manual handoff.

## P18-V01 `viewMessage` direct admin adapter — 2026-10-18 (`4d6cb941`)

- [x] Added `MQClientApi::mq_client_admin().view_message` using Java's direct
  asynchronous request code 33 path and `ViewMessageRequestHeader`. The API
  decodes one Broker message and preserves response-code/remark errors.
- [x] The invoked Java 5.5.1 fixture records the method signature, timeout,
  request fields, encoded response body, decoded message fields/body, and
  Broker error. The scripted Core differential compares request and result
  against that fixture. Fixture SHA-256:
  `7786a6c5d13b32a109f17f90878ab953b5a7a25392f4cebb514f9c0c8bc148d4`.
- [x] Core `admin_rpc` passed all 43 tests. Core all-feature tests passed 171
  unit tests, all integration targets, and 14 doctests. Strict all-target,
  all-feature Core Clippy passed. Pinned Maven `verify` passed all 37 scenarios
  and fixture checks, including 64 typed-header and 14 typed-body cross-decodes.
  Formatting and `git diff --check` passed. Code commit: `4d6cb941`.
- The source-backed `MqClientAdmin` count is now 13/19. Six methods remain:
  `updateOrCreateSubscriptionGroup`, `getBrokerClusterInfo`,
  `getConsumerConnectionList`, `getConsumeStats`, `getConsumerRunningInfo`,
  and `consumeMessageDirectly`. Broad P18-V01 API/settings/input/error/flow and
  reverse-export reconciliation remains open. Refresh full-workspace P18-V02
  after remaining implementation work; current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation remains the
  separate manual handoff.

## P18-V01 `consumeMessageDirectly` admin RPC — 2026-10-18 (`183660a5`)

- [x] Added `MQClientApi::mq_client_admin().consume_message_directly` using
  Java's asynchronous request code 309 path and
  `ConsumeMessageDirectlyResultRequestHeader`. It decodes the typed result and
  preserves Broker response-code/remark errors.
- [x] The invoked Java 5.5.1 fixture records the method signature, timeout,
  direct and inherited request fields, response body/result, and Broker error.
  Scripted Core tests compare the request/result and reject malformed success
  bodies. Fixture SHA-256:
  `604d2e9f75cfb87e23e7e7fd230a5307ddb51f2356370eda2d3778ae4e7316ef`.
- [x] Core `admin_rpc` passed all 45 tests. Core all-feature tests passed 171
  unit tests, all integration targets, and 14 doctests. Strict all-target,
  all-feature Core Clippy passed. Pinned Maven `verify` passed all 37 scenarios
  and fixture checks, including 64 typed-header and 14 typed-body cross-decodes.
  Formatting and `git diff --check` passed. Code commit: `183660a5`.
- The source-backed `MqClientAdmin` count is now 14/19. Five methods remain:
  `updateOrCreateSubscriptionGroup`, `getBrokerClusterInfo`,
  `getConsumerConnectionList`, `getConsumeStats`, and
  `getConsumerRunningInfo`. Broad P18-V01 API/settings/input/error/flow and
  reverse-export reconciliation remains open. Refresh full-workspace P18-V02
  after remaining implementation work; current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation remains the
  separate manual handoff.

## P18-V01 `getBrokerClusterInfo` admin RPC — 2026-10-18 (`b274784c`)

- [x] Added `MQClientApi::mq_client_admin().get_broker_cluster_info` using
  Java's headerless asynchronous request code 106. The typed `ClusterInfo`
  decoder handles unquoted numeric Broker-address keys and maps a null
  response body to `None`.
- [x] The invoked Java 5.5.1 fixture records the method signature, direct
  address/timeout, response tables, null-body outcome, and Broker error.
  Scripted Core tests compare request shape and decoded tables and cover null
  and error outcomes. Fixture SHA-256:
  `3c3c9e711dbb34e9eb97824cbec676e131990e4a03f9628ab76a119d2b9356d5`.
- [x] Core `admin_rpc` passed all 46 tests; Core all-feature tests passed 171
  unit tests, all integration targets, and 14 doctests. Protocol all-feature
  tests passed 132 unit tests, all integration targets, and 49 doctests.
  Strict all-target/all-feature Core and Protocol Clippy passed. Pinned Maven
  `verify` passed all 37 scenarios and fixture checks, including 64 typed-header
  and 14 typed-body cross-decodes. Formatting and `git diff --check` passed.
  Code commit: `b274784c`.
- The source-backed `MqClientAdmin` count is now 15/19. Four methods remain:
  `updateOrCreateSubscriptionGroup`, `getConsumerConnectionList`,
  `getConsumeStats`, and `getConsumerRunningInfo`. Broad P18-V01
  API/settings/input/error/flow and reverse-export reconciliation remains
  open. Refresh full-workspace P18-V02 after remaining implementation work;
  current-revision Ubuntu/native Windows P18-V04 evidence, final P18-V03
  package/provenance review, Phase 19 local Docker scenarios/profiles and
  runner reconciliation, and pre-publication version/registry checks also
  remain open. Owner-only validation remains the separate manual handoff.

## P18-V01 `getConsumerConnectionList` admin RPC — 2026-10-18 (`eb627fcc`)

- [x] Added `MQClientApi::mq_client_admin().get_consumer_connection_list`
  using Java's asynchronous request code 203 and
  `GetConsumerConnectionListRequestHeader`. The typed response preserves the
  nullable body, connection set, subscription table, and consumer enums.
- [x] The invoked Java 5.5.1 fixture records request code/fields, the encoded
  response body, decoded connection and subscription values, null-body result,
  and Broker error. Core scripted tests compare the request and result and
  cover null bodies, Broker errors, and malformed successful bodies. Fixture
  SHA-256: `63fedb0e501c7539fcbf9495a639c41ec59aa8e3e8db39a14a75674c1a1f2a05`.
- [x] Added bidirectional header fixtures for `GetConsumerConnectionList`
  and the in-progress `GetConsumeStats` request header. Protocol all-feature
  tests passed (133 unit tests, all integration targets, 49 doctests); Core
  `admin_rpc` passed all 47 tests. Strict all-target/all-feature Clippy passed
  for Protocol, Remoting and Core. Pinned Maven `verify` passed all 37
  scenarios, 66 typed-header cross-decodes and 14 typed-body cross-decodes.
  Formatting and `git diff --check` passed.
- The source-backed `MqClientAdmin` count is now 16/19. Three methods remain:
  `updateOrCreateSubscriptionGroup`, `getConsumeStats`, and
  `getConsumerRunningInfo`. Broad P18-V01 API/settings/input/error/flow and
  reverse-export reconciliation remains open. Refresh full-workspace P18-V02
  after remaining implementation work; current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks remain open. Owner-only validation remains the
  separate manual handoff.

## P18-V01 `getConsumeStats` admin RPC — 2026-10-18 (`7375c283`)

- [x] Added `MQClientApi::mq_client_admin().get_consume_stats` for Java 5.5.1
  request code 208. `ConsumeStats` and `OffsetWrapper` preserve Java defaults,
  nullable MessageQueue-keyed offset tables, shared map/entry mutation, public
  accessors, and both wrapping signed-long diff calculations.
- [x] The invoked Java fixture records the interface signature, request fields,
  Fastjson response body/result, DTO public methods/defaults/alias behavior,
  computed totals, null-body result, and Broker error. Scripted Core tests
  compare request/result data and cover null bodies, Broker errors, and
  malformed successful bodies. Fixture SHA-256:
  `707e5c298cab50f1f985bbe41d5da2de70db34670ec67742fbb8fc65b84da9c7`.
- [x] Protocol all-feature tests passed (134 unit tests, all integration
  targets, 51 doctests); Core `admin_rpc` passed all 48 tests. Strict
  all-target/all-feature Clippy passed for Protocol, Remoting, and Core.
  Pinned Maven `verify` passed all 37 scenarios, 66 typed-header cross-decodes,
  and 15 typed-body cross-decodes. `cargo fmt --all -- --check` and
  `git diff --check` passed. Rust body fixture SHA-256:
  `60927f5f6dc23903346f62856021ca84bbf0bc37ae90c17978a2b064dff705e9`.
- The source-backed `MqClientAdmin` count is now 17/19. The remaining methods
  are `updateOrCreateSubscriptionGroup` and `getConsumerRunningInfo`; broad
  P18-V01 reconciliation remains open. Refresh full-workspace P18-V02 after
  remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19 local
  Docker scenarios/profiles and runner reconciliation, and pre-publication
  version/registry checks also remain open. Owner-only validation stays in
  the separate manual handoff.

## P18-V01 `updateOrCreateSubscriptionGroup` admin RPC — 2026-10-18 (`10db6f10`)

- [x] Added `MQClientApi::mq_client_admin().update_or_create_subscription_group`
  for Java request code 200. It sends the provided address, no ext-fields, and
  the Fastjson `SubscriptionGroupConfig` body; success returns `()` and Broker
  errors preserve the response code/remark.
- [x] Added `SubscriptionGroupConfig` and its retry-policy DTOs with the Java
  public accessor surface, defaults, equality/hash, Lite attribute helpers,
  alias behavior, display and serialization. `SubscriptionDataSet` now has
  insertion-ordered Java Set membership: equality ignores subscription
  `version`, duplicate equal values and duplicate nulls collapse, and Set
  equality ignores iteration order.
- [x] The invoked pinned Java fixture covers the interface method, request
  body, defaults, DTO methods, alias semantics, and Set duplicate/null cases.
  Core `admin_rpc` passed all 49 tests; Protocol all-feature tests passed 141
  unit tests, all integration targets and 51 doctests. Strict all-target/all-
  feature Clippy passed for Protocol, Remoting and Core. Maven `verify` passed
  all 37 scenarios, 66 typed-header cross-decodes and 15 typed-body
  cross-decodes. Formatting and `git diff --check` passed. Fixture SHA-256:
  `21985c21ea6f73efda1c5eceeff45359b937f112d181321b8237a1f431fee8a2`.
- Code commit: `10db6f10`. The source-backed `MqClientAdmin` count is now
  18/19; `getConsumerRunningInfo` is the remaining method slice. Broad P18-V01
  reconciliation, current-revision full-workspace P18-V02, P18-V03 package/
  provenance review, P18-V04 Ubuntu/Windows evidence, and Phase 19 local
  Docker/Broker scenarios and profiles remain open. Owner-only validation
  remains in the separate manual handoff.

## P18-V01 `getConsumerRunningInfo` admin RPC — 2026-10-18 (`f05833a7`)

- [x] Added `MQClientApi::mq_client_admin().get_consumer_running_info` for Java
  request code 307. The adapter sends `GetConsumerRunningInfoRequestHeader`,
  decodes Java's object-valued `MessageQueue` map keys into the typed running-
  info snapshot, returns `None` for a success response without a body, and
  preserves Broker response codes and remarks.
- [x] Extended the pinned Java fixture with the direct async RPC signature,
  request fields, existing rich running-info body, decoded result, null-body
  result and Broker error. Rust tests verify properties, subscriptions,
  normal/POP queue rows, status, user info and jstack; they also cover missing
  body, Broker error and malformed success body. Fixture SHA-256:
  `ae970b21d7822f9d213477bf47a227125b5d6242634a18c490001837b5828851`.
- [x] Protocol all-feature tests passed (141 unit tests, all integration
  targets and 51 doctests); Core `admin_rpc` passed all 50 tests. Strict
  all-target/all-feature Clippy passed for Protocol, Remoting and Core. Pinned
  Maven `verify` passed all 37 scenarios, 66 typed-header cross-decodes and 15
  typed-body cross-decodes. Formatting and `git diff --check` passed.
- Code commit: `f05833a7`. All 19 source-backed `MqClientAdmin` RPC slices are
  present. Broad P18-V01 public DTO/API, settings, input/error/flow and reverse-
  export audit remains open, as do current-revision full-workspace P18-V02,
  P18-V03 package/provenance, P18-V04 Ubuntu/Windows evidence and Phase 19
  local Docker/Broker scenarios and profiles. Owner-only validation remains
  in the separate manual handoff.

## P18-V01 ConsumerRunningInfo property-key correction — 2026-10-18 (`a56ee59d`)

- [x] Added Rust associated constants for all six pinned Java
  `ConsumerRunningInfo` property keys. Push/Pull Running Info generation uses
  these constants; the orderly property now uses Java's exact value,
  `PROP_CONSUMEORDERLY`.
- [x] Expanded the Java Running Info fixture and its dependent MqClientAdmin
  fixture. The Protocol body test checks the constant values and encoded
  fields; the Push generator regression checks the correct key and absence of
  the former misspelling. Fixture SHA-256 values:
  `fixtures/consumer/running-info.json`:
  `54ba7d8dba8b942528eed61d3d15a273a23b944ba4653824300d09c579ef026e`;
  `fixtures/admin/query-consume-time-span.json`:
  `ae161fbf6d2ba14453c7ff91b99ca5be3ae9aaae7f55d46026276ade1de6fe60`.
- [x] Protocol and Push targeted tests passed. Strict all-target/all-feature
  Clippy passed for Protocol and Consumer; formatting and `git diff --check`
  passed. Pinned Java `mvn -f compat/java-reference/pom.xml verify` passed 37
  scenarios, 66 typed-header cross-decodes and 15 typed-body cross-decodes.
- `analyzeSubscription`, `isPushType`, `analyzeRebalance`,
  `analyzeProcessQueue`, and `formatString` are used by `rocketmq-tools`, which
  Phase 17 excludes; pinned-source review found no client call sites. Broader
  P18-V01 API/settings/input/error/flow/reverse-export reconciliation remains
  open, as do current-revision workspace/P18-V02, P18-V03 package/provenance,
  P18-V04 platform evidence and the Phase 19 local scenarios.

## P18-V01 `ConsumerConnection.computeMinVersion` DTO method — 2026-10-18 (`d8f84f2c`)

- [x] Added the Rust `ConsumerConnection::compute_min_version` API for the
  pinned Java method. Empty sets return `i32::MAX`, versions use Java's signed
  minimum rule, and null sets/elements return typed errors at the Java
  `NullPointerException` boundaries. The invoked Java fixture covers defaults,
  empty sets, default/mixed versions, integer limits, nulls, and the reflected
  method signature. Fixture SHA-256:
  `748138c8020551f397ab68bbb24e8e858b5345e95fb78998d0051cea5bbd5f20`.
- [x] Protocol all-feature tests pass (142 unit tests, all integration targets
  and 51 doctests); strict Protocol Clippy passes. Pinned Maven `verify` passes
  all 37 scenarios, 66 typed-header and 15 typed-body cross-decodes. Formatting
  and diff checks pass.
- [x] Current-worktree Linux ARM64 Rust 1.85.0 default workspace tests pass.
  All-feature workspace unit and integration tests pass when the temporary
  Cargo runner clears its inherited signal mask before launching test binaries.
  Workspace all-target/all-feature check and strict Clippy also pass. A full
  all-feature workspace test rerun was stopped during Consumer doctests after
  the unchanged Common compile-fail doctests took 306 seconds; this is not
  recorded as a full P18-V02 pass. The latest complete P18-V02 gate remains
  `411d4efb`, before this DTO slice.
- The P18-V01 broad source/API/settings/input/error/flow and reverse-export
  audit remains open. Current-revision P18-V02 full rerun, final P18-V03
  package/provenance review, P18-V04 Ubuntu/native Windows CI evidence, Phase
  19 local runner/profile completion, and pre-publication release-version and
  registry dependency checks remain open. Owner-environment checks stay in the
  separate manual handoff.

## P18-V02 current-worktree server-free gate — 2026-10-18 (`d8f84f2c`)

- [x] The current code revision passes the full Linux ARM64 Rust 1.85.0
  default workspace tests, the full all-feature workspace tests including
  integration tests and doctests, workspace all-target/all-feature `cargo
  check`, and strict workspace Clippy. The full all-feature test used
  `CARGO_TARGET_DIR=/tmp/rocketmq-phase18-target` inside the container and a
  temporary test runner that clears the inherited signal mask; it exited 0.
- [x] On macOS ARM64 Apple M5 Max with Rust 1.85.0, differential comparator
  self-test and 37-scenario manifest validation pass. The deterministic
  race/leak suite passes 16 cases × 3; fuzz smoke passes 120 frame seeds, 11
  message seeds, 2,096 bounded mutations, 164 frame decodes, and 120 message
  decodes. Release examples build and the offline benchmark completes.
- [x] Maven 3.10.0 / OpenJDK 27 `verify` passes all 37 scenarios, fixture
  checks, 66 typed-header and 15 typed-body cross-decodes. Workspace metadata,
  license and dependency-direction checks pass; each of the seven publishable
  package listings includes `LICENSE` and `NOTICE`. UTC and Asia/Taipei
  Unique ID fixture tests each pass. Formatting and `git diff --check` pass.
- The offline benchmark ran on macOS ARM64 Apple M5 Max with Rust 1.85.0:
  frame encode/decode 2,552 ns/iteration, zstd message decode 1,193 ns,
  producer request construction 1,274 ns, route lookup 14.6 ns, and loopback
  pending completion 77,248 ns. Allocation observations were recorded by the
  harness; these are measurements, not acceptance thresholds.
- This closes the current-revision server-free P18-V02 rerun. Broad P18-V01
  bidirectional source/API/settings/input/error/flow and reverse-export audit,
  final P18-V03 package/provenance review, current-revision P18-V04 Ubuntu and
  native Windows evidence, Phase 19 local runners/profiles, and pre-publication
  release-version and registry dependency checks remain open. Owner checks
  MAN-01–04 remain in the separate handoff; MAN-05 awaits local-matrix
  classification. The pre-existing uncommitted Phase 19 runner files were
  left untouched.

## P18-V01 optional numeric header reflection decoding — 2026-10-18 (`1a4924a3`)

- [x] Pinned Java 5.5.1 fixture covers both decode hints for five non-fast
  headers. Malformed/overflow nullable numeric values are skipped per field,
  valid Arabic-Indic digits decode like Java, and malformed primitive
  `ResetOffsetRequestHeader.queueId` preserves Java's initial `-1`. Fixture
  SHA-256: `f4bc4d7c18fc82a43a07f45f052f710644dcd950f7a5c5c0a0333a25435580f5`.
- [x] Protocol all-feature tests passed (142 unit tests, all integration
  targets, 51 doctests); strict all-target/all-feature Protocol Clippy passed.
  Maven `verify` passed 37 scenarios, fixture checks, 66 typed-header and 15
  typed-body cross-decodes. Formatting and `git diff --check` passed.
- This closes only the named header fields. Other primitive/required numeric
  header branches and broad P18-V01 remain open. Refresh full-workspace P18-V02
  after remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19's 30
  remaining local scenarios and required profiles, and pre-publication version/
  registry checks remain open. Owner-only checks stay in the separate handoff.

## P18-V01 `ChangeInvisibleTimeRequestHeader` nullable reflection fields — 2026-10-18 (`8f64dde3`)

- [x] Pinned Java 5.5.1 fixture covers both decode hints. Nullable strings and
  boxed numeric fields remain unset for missing or invalid input; Arabic-Indic
  decimal digits decode, and primitive `suspend` defaults to false. Fixture
  SHA-256: `01b6c6fdbcc09ef278509f92e58e117c025222ca1e99755ff23168b570c95890`.
- [x] Rust header and POP request construction preserve the nullable fields.
  Core returns typed missing-field errors for absent success-path `offset`,
  `topic`, or `queueId`; non-success responses return `NoExist` without reading
  those fields. The regression covers each missing-field branch.
- [x] Four affected crates pass all-feature unit, integration, and doctests in
  a Rust 1.85.0 Linux container. Strict all-target/all-feature Clippy passes
  for those crates; Maven `verify` passes 37 scenarios, 66 typed-header and 15
  typed-body cross-decodes. Formatting and diff checks pass. The initial
  macOS run stalled at a launched-suspended codec test process; the Linux
  container run completed the full affected-crate test command.
- This closes only the named header and callback paths. Other header branches
  and broad P18-V01 remain open. Refresh the full-workspace P18-V02 gate after
  remaining implementation work. Current-revision Ubuntu/native Windows
  P18-V04 evidence, final P18-V03 package/provenance review, Phase 19's 30
  remaining local scenarios and required profiles, and pre-publication
  version/registry checks remain open. Owner-only checks stay in the separate
  handoff.

## P18-V01 `PopMessageRequestHeader` reflection defaults — 2026-10-18

- [x] The pinned Java 5.5.1 oracle confirms both decode hints use reflection:
  the header does not implement `FastCodesHeader`, its `checkFields()` is a
  no-op, nullable group/topic remain null, primitive numeric fields retain zero
  on absent or malformed input, and primitive `order` defaults to false.
  Fixtures cover defaults, malformed/overflow values, and valid Arabic-Indic
  numerals. Fixture: `fixtures/remoting/optional-numeric-header-decode-modes.json`.
- [x] Rust now represents nullable group/topic as `Option<String>`, preserves
  Java primitive defaults, and the consumer POP request path supplies present
  values explicitly.
- [x] Current-tree default and all-feature workspace tests, strict
  all-target/all-feature Clippy, Maven `verify` (37 scenarios, 66 typed-header
  and 15 typed-body cross-decodes), formatting, and diff checks pass.
- This closes only the `PopMessageRequestHeader` decode/default slice. The
  complete P18-V02 supplementary race/fuzz/example/benchmark/package gate must
  be refreshed after implementation work. Broad P18-V01, final P18-V03
  package/provenance and pre-publication checks, current-revision P18-V04
  Ubuntu/native Windows CI evidence, and Phase 19's remaining local scenarios
  and profiles remain open. Owner-only checks stay in the separate handoff.

## P18-V01 Lite POP header parity — 2026-10-18

- [x] `PopLiteMessageRequestHeader` now follows Java reflection defaults:
  nullable strings remain null; primitive numbers default to zero or retain
  zero after invalid/overflow input; the four inherited `RpcRequestHeader`
  fields are represented and encoded. The invoked Java fixture covers both
  decode hints, Arabic-Indic numerals, nullable fields, inherited identity, and
  `isTimeoutTooMuch()` elapsed/future cases. The matching timeout helper is
  implemented for both POP request header types using Java signed arithmetic.
  Fixture hashes: `fixtures/remoting/optional-numeric-header-decode-modes.json`
  (`2c7095818dfb39cfd892f84e585b57c4a61cf09f60f4d6dfc5489674ec340424`) and
  `fixtures/protocol/request-header-identity.json`
  (`cb0a9c871e95bdb006f7bdb5a8a54c2cf74a214ee79729a6e7470f8b62bce9c1`).
- [x] Protocol all-feature tests, workspace all-feature tests, strict
  all-target/all-feature workspace Clippy, Maven `verify` (37 scenarios, 66
  typed-header and 15 typed-body cross-decodes), formatting, and diff checks
  pass.
- This closes only these POP header decode/default/inherited-equality and
  timeout-helper slices. Broad P18-V01 API/input/flow/reverse-export
  reconciliation, the complete P18-V02 supplementary gates, final P18-V03
  package/provenance and pre-publication checks, current-revision P18-V04
  Ubuntu/native Windows CI evidence, and Phase 19 local runners/profiles remain
  open. Owner-only checks stay in the separate handoff.

## P18-V01 POP header public helper methods — 2026-10-18

- [x] Pinned Java behavior is now fixture-backed for negative `queueId`
  normalization, nullable `order` getter/predicate behavior, and both POP
  request headers' custom `toString()` output. Rust exposes matching
  `get_queue_id`, `get_order`, `is_order`, `set_order`, and Java-format string
  helpers. The expanded optional-header fixture SHA-256 is
  `5876a0b734633ff2488d5487df7104fb35ee350b970e1556b0e8c881346a3150`.
- [x] Protocol all-feature tests and the focused Java-fixture differential
  pass; strict all-target/all-feature Protocol Clippy, Maven `verify` (37
  scenarios, 66 typed headers and 15 typed bodies), formatting, and diff checks
  pass.
- This closes only these POP public helper branches. Broad P18-V01 API,
  settings, input/error/flow and reverse-export reconciliation; complete P18-V02
  supplementary gates; final P18-V03 package/provenance and pre-publication
  checks; current-revision P18-V04 Ubuntu/native Windows CI evidence; and Phase
  19 local runners/profiles remain open. Owner-only checks stay in the separate
  handoff.

## P18-V01 POP response header reflection defaults — 2026-10-18 (`f5a924fa`)

- [x] Pinned Java 5.5.1 fixture now covers both decode hints for `PopMessageResponseHeader` and `PopLiteMessageResponseHeader`: absent, malformed, and overflowing primitive values retain zero; Arabic-Indic decimal digits decode; default and valid response fields encode identically. Fixture SHA-256: `1a6bd80ad46c645132412e217517de98e7242e88d457034215c921aca4a4e23f`.
- [x] Rust response-header reflection decoding and the header validation matrix now preserve Java's primitive defaults. Protocol all-feature tests pass (143 unit tests, all integration targets, 51 doctests); strict all-target/all-feature Protocol Clippy passes; Maven `verify` passes the 37-scenario manifest, fixture checks, 66 typed-header and 15 typed-body cross-decodes. Formatting and `git diff --check` pass.
- This closes only POP response header primitive defaults. Broad P18-V01 source/API/settings/input/error/flow/reverse-export reconciliation remains open. The full-workspace P18-V02 supplement, current-revision P18-V04 Ubuntu/native Windows evidence, final P18-V03 package/provenance and version/registry preflight, and Phase 19 local scenario runners/profiles remain open. MAN-01–04 remain owner-environment handoff items; MAN-05 awaits local-matrix classification.

## Current-worktree P18-V02 server-free gate — 2026-10-18 (`f5a924fa`)

- [x] Linux ARM64, Rust 1.85.0: default and all-feature workspace tests pass,
  including integration targets and doctests. All-target/all-feature workspace
  `cargo check` and strict Clippy pass. The container used `--init`, offline
  dependencies, a temporary runner to clear the inherited signal mask, and the
  cached `/workspace` mount path. Initial attempts without the runner or with a
  mismatched cached path were environment failures; neither is counted as a
  gate pass.
- [x] Maven `verify` passes all 37 scenarios, fixture checks, and Java/Rust
  cross-decodes for 66 typed headers and 15 typed bodies. Differential
  comparator self-test and 37-row manifest validation pass.
- [x] Deterministic race/leak smoke passes 16 cases × 3. Fuzz smoke passes 120
  frame seeds, 11 message seeds, 2,096 bounded mutations, 164 frame decodes,
  and 120 message decodes. Release examples build; workspace benchmarks
  compile and the offline benchmark runs.
- [x] Locked Cargo metadata, workspace license/dependency checks, seven
  publishable package listings with `LICENSE` and `NOTICE`, UTC and
  Asia/Taipei Unique ID tests, formatting, and `git diff --check` pass.
- Linux ARM64 Rust 1.85.0 benchmark observations: frame encode/decode 1,217.0
  ns/iteration; Zstd message decode 711.2 ns; producer request construction
  906.9 ns; 128-entry route lookup 14.1 ns; pending completion 5,894.3 ns.
  These are observations, not acceptance thresholds.
- P18-V02 server-free local gates are complete on this revision. Broad P18-V01
  source/API/settings/input/error/flow and reverse-export reconciliation,
  final P18-V03 package/provenance and pre-publication checks, current-revision
  Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30 remaining local
  scenario runners and required profiles remain open. MAN-01–04 remain owner
  checks; MAN-05 awaits local-matrix classification.

## P18-V01 pinned `Message` declaration crosswalk — 2026-10-18

- The pinned Java 5.5.1 `Message.java` declares five public constructors and
  34 public methods. The compatibility map now records a Rust counterpart and
  observed behavior for every declaration, including nullable input, the
  `String`/`Collection` key overloads, lazy property-map state, property/body
  aliases, numeric parse/error paths, and `toString()`.
- On code revision `f5a924fa`,
  `cargo test -p rocketmq-classic-common --test message_decode
  message_public_api_matches_java_551_fixture` passed. Offline Maven `verify`
  passed with 37 scenarios, 11 Java message decode fixtures, fixture drift
  checks, and Java/Rust cross-decodes. The fixture hash is
  `0346a246d0f22440ed71773ac1854ad510acf857954d85f7ee9b8a451afc6471`.
- This closes the declared `Message` API slice. `MessageExt` inherited API and
  workspace-wide P18-V01 reconciliation remain open.

## P18-V01 `MessageExt` and `MessageClientExt` surface crosswalk — 2026-10-18

- The pinned Java 5.5.1 fixtures enumerate both `MessageExt` constructors and
  all 44 declared methods, plus the `MessageClientExt` no-argument constructor
  and four declared methods. The compatibility map now reconciles scalar and
  inherited accessors, socket/address and byte-buffer overloads, system flags,
  client message-ID selection, formatting, and identity behavior.
- On code revision `f5a924fa`, four `message_ext_*` differential tests and
  `message_client_ext_api_matches_java_551_fixture` passed. The current-revision
  offline Maven `verify` passed with 37 scenarios, 11 Java message decode
  fixtures, fixture drift checks, and Java/Rust cross-decodes. Fixture SHA-256:
  `0346a246d0f22440ed71773ac1854ad510acf857954d85f7ee9b8a451afc6471`.
- This closes the `MessageExt`/`MessageClientExt` declared and inherited API
  slice. Broad P18-V01 bidirectional reconciliation remains open.

## P18-V01 `MessageExt` host-byte failure ordering — 2026-10-18

- [x] Pinned Java 5.5.1 confirms absent born/store hosts fail with
  `NullPointerException`. Caller buffers sized 7 bytes for IPv4 and 19 bytes
  for IPv6 overflow while writing the port after the address, retaining the
  first 4/16 bytes and advancing to that position. The fixture also covers
  constructor preservation of null hosts.
- [x] Rust returns typed missing-address/short-buffer errors and preserves the
  address prefix on short-buffer failure. The focused differential and Common
  all-feature suite pass; strict all-target/all-feature Common Clippy, format,
  `git diff --check`, and offline Maven `verify` pass. Fixture SHA-256:
  `6b1a19fef78c1f88f52d7e0851905a6c4a1d6adb98de90fe2028642880a513b3`.
- This closes only this host-byte error-ordering slice. Broad P18-V01 remains
  open; the current-revision full-workspace P18-V02 rerun is still required
  after this source change.

## Current-worktree checks after `ClusterInfo` DTO slice — 2026-10-18 (`6b40e86c`)

- [x] Locked default and all-feature workspace tests, all-target/all-feature
  workspace check and strict Clippy pass on macOS ARM64. `cargo fmt --all --
  --check` and `git diff --check` pass.
- [x] Offline Maven `verify` passes the 37-scenario fixture manifest and
  Java/Rust cross-decodes (66 typed headers and 15 typed bodies).
- The current default/all-feature test and lint gates include the
  `ClusterInfo` DTO change. The P18-V02 supplementary race/fuzz/example/
  benchmark/package gate has not been refreshed after the latest Phase 18
  implementation and remains open.
- Broad P18-V01 bidirectional source/API/settings/input/error/flow and
  reverse-export reconciliation, final P18-V03 package/provenance and
  version/registry preflight, and current-revision Ubuntu/native Windows
  P18-V04 evidence remain open. Phase 19 still needs 30 local Docker/Broker
  scenario runners and required profiles. MAN-01–04 remain owner-environment
  checks; MAN-05 awaits the completed local matrix.

## P18-V02 current-revision supplementary gate — 2026-10-18 (`6b40e86c`)

- [x] On macOS ARM64 Apple M5 Max with Rust 1.85.0, default and all-feature
  workspace tests pass, including integration targets and doctests. Workspace
  all-target/all-feature `cargo check` and strict Clippy pass.
- [x] Maven offline `verify` passes all 37 scenarios, fixture checks, 66
  typed-header and 15 typed-body Java/Rust cross-decodes. Differential
  comparator self-test and 37-row manifest validation pass.
- [x] Deterministic race/leak smoke passes 16 cases × 3. Bounded fuzz smoke
  passes 120 frame seeds, 11 message seeds, 2,096 mutations, 164 frame decodes,
  and 120 message decodes. Release examples and all workspace benchmark targets
  build; the offline benchmark runs.
- [x] Locked workspace metadata, package dependency/license checks and seven
  package-content listings pass. Four Unique ID tests pass under both UTC and
  Asia/Taipei. Formatting and `git diff --check` pass.
- Benchmark observations (not thresholds): frame encode/decode 2,367.5
  ns/iteration; Zstd message decode 1,220.3 ns; Producer request construction
  1,244.3 ns; route lookup 17.1 ns; loopback pending completion 28,162.1 ns.
  Allocation observations are recorded by the benchmark harness.
- P18-V02 server-free gates are now complete on this code revision. Broad
  P18-V01 bidirectional reconciliation, final P18-V03 package/provenance and
  version/registry preflight, and current-revision Ubuntu/native Windows
  P18-V04 evidence remain open. Phase 19 still needs 30 local Docker/Broker
  scenario runners and required profiles. MAN-01–04 remain owner-environment
  checks; MAN-05 awaits the completed local matrix.

## P18-V01 `Connection` and `ConsumerConnection` DTO API — 2026-10-18 (`160f099a`)

- The pinned Java 5.5.1 oracle fixture now inventories the public no-argument
  constructors and all 8 declared methods on `Connection` and 11 declared
  methods on `ConsumerConnection`. Differential coverage records their default
  values, setter/getter values, nullable table behavior, and collection/value
  identity and mutation visibility. Rust `SharedValue` accessors preserve the
  mutable set/map and nested DTO aliases.
- The focused Rust Protocol differential passed. Offline Maven `verify` passed
  all 37 scenarios, fixture drift checks, 66 typed-header and 15 typed-body
  cross-decodes. `cargo fmt --all -- --check` and `git diff --check` passed.
  Fixture SHA-256: `a2702341491aea02ece8494c65ac0dc5544e69947d42987414c171f2fb96cc31`.
- This closes only the two DTO API slices. Broad P18-V01 bidirectional
  reconciliation remains open, as does the full current-revision P18-V02
  rerun. Final P18-V03 package/provenance and version/registry preflight,
  current-revision Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30
  local Docker/Broker scenario runners and required profiles remain open.
  MAN-01–04 remain owner-environment checks; MAN-05 awaits local-matrix
  completion.

## P18-V01 `QueueTimeSpan` DTO API — 2026-10-18 (`423bcdbc`)

- [x] Aligned Rust field visibility with Java: `QueueTimeSpan` fields are
  private, and `QueryConsumeTimeSpanBody.consumeTimeSpanSet` is private. Its
  default empty list, explicit null, nullable rows, and shared identity/mutation
  behavior are covered by an invoked Java fixture and Rust differentials.
- [x] Core preserves its owned response type by snapshotting decoded shared
  DTO handles. Protocol doctests passed (53); focused Protocol/Core Java fixture
  and admin RPC tests passed; strict all-target/all-feature Clippy passed for
  Protocol and Core. Offline Maven `verify` passed all 37 scenarios, fixture
  checks, 66 typed-header and 15 typed-body cross-decodes. Formatting and diff
  checks passed. Fixture SHA-256:
  `3012f8dfb49f4bd404e1d29a7e27cdbe14d47435347bcb55ac807e5649085af5`.
- Code commit: `423bcdbc`. Broad P18-V01 bidirectional reconciliation remains
  open. P18-V02 must be rerun on this newer revision; final P18-V03
  package/provenance and version/registry preflight, current-revision Ubuntu
  and native Windows P18-V04 evidence, and Phase 19's 30 local Docker/Broker
  scenario runners and required profiles remain open. MAN-01–04 remain
  owner-environment checks; MAN-05 awaits completion of the local matrix.

## P18-V01 `GroupList` DTO API inventory — 2026-10-18 (`3b8a62b3`)

- [x] The invoked Java fixture now captures its public no-argument constructor,
  declared getter/setter signatures and empty public-field set. Rust asserts
  that inventory, calls the matching accessors, and compile-fail checks that
  the DTO field is private. Existing alias/default/null/nullable-element
  differentials remain in place. Fixture SHA-256:
  `5961395fa36f199d8b3745f91d1dcc4941c1d2d8748f272e119ed9c3ae1ad73a`.
- [x] The focused Protocol differential, 54 Protocol doctests, strict
  all-target/all-feature Protocol Clippy, pinned Maven `verify` (37 scenarios,
  fixture checks, 66 typed-header and 15 typed-body cross-decodes), formatting,
  and diff checks passed. Test/fixture commit: `3b8a62b3`.
- Broad P18-V01 reconciliation remains open. P18-V02 needs a final rerun on
  this newer revision; P18-V03 package/provenance and version/registry
  preflight, current-revision Ubuntu/native Windows P18-V04 evidence, and Phase
  19's 30 local Docker/Broker runners and required profiles also remain open.

## P18-V01 `TopicList` DTO API inventory — 2026-10-18 (`5233eb2f`)

- [x] Added Java constructor reflection to the existing API fixture. The Rust
  Protocol differential now checks the constructor, four declared accessor
  signatures, empty public-field list, mapped accessors and private field
  visibility, alongside existing default/null/alias behavior. Fixture
  SHA-256: `11d0a8919e272d3f3ed347695a06220a3688c3437805a0e5737ba4fccd43726b`.
- [x] The focused Protocol differential, 55 Protocol doctests, strict
  all-target/all-feature Protocol Clippy, pinned Maven `verify` (37 scenarios,
  fixture checks, 66 typed-header and 15 typed-body cross-decodes), formatting,
  and diff checks passed. Test/fixture commit: `5233eb2f`.
- Broad P18-V01 reconciliation remains open; P18-V02 requires another final
  rerun on this newer revision. P18-V03 package/provenance and registry
  preflight, current-revision Ubuntu/native Windows P18-V04 evidence, and Phase
  19's 30 local Docker/Broker runners and required profiles remain open.

## P18-V01 `TopicStatsTable` and `TopicOffset` API inventory — 2026-10-18 (`93412b9a`)

- [x] Pinned Java fixtures now record public no-argument constructors for both
  DTOs. Rust asserts the constructor, method and field inventories, guards
  field privacy, and exercises all `TopicOffset` getters/setters at signed
  64-bit boundaries. Existing offset-table JSON/null/alias checks remain.
  Fixture SHA-256:
  `9b1bd6d1cebd844db4415a45e60cee14442e76dc235662d5189056977da0aeb6`.
- [x] The focused Protocol differential, 57 Protocol doctests, strict
  all-target/all-feature Protocol Clippy, pinned Maven `verify` (37 scenarios,
  fixture checks, 66 typed-header and 15 typed-body cross-decodes), formatting,
  and diff checks passed. Test/fixture commit: `93412b9a`.
- Broad P18-V01 remains open. P18-V02 needs a final rerun on this latest code
  revision; P18-V03 package/provenance and registry preflight,
  current-revision Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30
  local Docker/Broker runners and required profiles remain open.

## P18-V01 `QuerySubscriptionResponseBody` API inventory — 2026-10-18 (`67edffe7`)

- [x] The invoked Java fixture now includes the public no-argument constructor,
  six declared accessor signatures and empty public-field list. Rust asserts
  those declarations, checks field privacy, directly exercises the setters and
  null reset, and retains the shared `SubscriptionData` identity/mutation
  differential. Fixture SHA-256:
  `e5c6405810fbe550a58bd7ce3b843b3e21410fc3e1674c1692f18836a2e969af`.
- [x] Focused Protocol differential, 58 Protocol doctests, strict all-target /
  all-feature Protocol Clippy, pinned Maven `verify` (37 scenarios, fixture
  checks, 66 typed-header and 15 typed-body cross-decodes), formatting and diff
  checks passed. Test/fixture commit: `67edffe7`.
- Broad P18-V01 remains open. P18-V02 requires another final rerun on this
  latest revision. P18-V03 package/provenance and registry preflight,
  current-revision Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30
  local Docker/Broker runners and required profiles also remain open.

## P18-V02 current-worktree server-free gate — 2026-10-18 (`117ae342`)

- [x] Default and all-feature `cargo test --workspace --locked` pass, including
  workspace integration targets and doctests. Strict all-target/all-feature
  workspace Clippy and `cargo check` pass.
- [x] Maven `verify` passes all 37 scenarios, fixture checks, 66 typed-header
  and 15 typed-body cross-decodes. Differential comparator self-test and
  37-row manifest validation pass.
- [x] Offline race/leak smoke passes 16 cases × 3. Fixed-seed fuzz smoke passes
  120 frame seeds, 11 message seeds, 2,096 mutations, 164 frame decodes, and
  120 message decodes. Release examples and all workspace benchmark targets
  build.
- [x] The offline benchmark runs on macOS ARM64 Apple M5 Max with Rust 1.85.0.
  Observations per iteration: frame encode/decode 2,353.0 ns; Zstd message
  decode 1,145.1 ns; Producer request construction 1,178.5 ns; 128-entry route
  lookup 14.4 ns; loopback pending completion 65,594.1 ns. Allocation
  observations are emitted by the benchmark harness; these measurements are
  not thresholds.
- [x] Workspace metadata and seven product package-content listings pass;
  every listed product package includes `LICENSE` and `NOTICE`. All eight
  workspace packages report version `0.1.0`. Four Core Unique ID tests pass
  under both UTC and Asia/Taipei. Formatting and `git diff --check` pass.
- The P18-V02 local server-free gate is complete on this revision. All 19
  source-backed `MqClientAdmin` RPC slices are present; an earlier unchecked
  19-method inventory note is superseded by the Phase 17 matrix. Broad P18-V01
  API/settings/input/error/flow and reverse-export reconciliation remains
  open. Final P18-V03 version/registry and provenance preflight and current-
  revision Ubuntu/native Windows P18-V04 CI evidence also remain open. Phase
  19 still needs 30 of 37 local Docker/Broker scenario runners, the required
  profiles, and passing `run-local.sh --all` reconciliation. MAN-01–04 remain
  owner-environment checks; MAN-05 awaits the local matrix classification.
- The consumer test call-site adjustment is in code commit `117ae342`. Existing
  Phase 19 runner/README changes remain unstaged and outside this Phase 18
  gate. No plan, Java oracle, remote CI, Broker/Docker state, publication,
  release tag, or push changed.

## P18-V01 `GetConsumerListByGroupResponseBody` API inventory — 2026-10-18 (`e5887718`)

- [x] The invoked Java 5.5.1 fixture records the public no-argument constructor,
  two declared public accessor signatures, and empty public-field inventory.
  Rust asserts this inventory and compile-fail checks that `consumer_id_list`
  is private. Existing default/null, alias mutation, and Java identity checks
  remain covered. Fixture SHA-256:
  `21f6d58affb63528aad0ccc25ac21e4b8e2131c8521ccbdffeb8c68eccf8408c`.
- [x] Protocol/core tests passed; all 59 Protocol doctests passed; strict
  all-target/all-feature Clippy passed for Protocol, Core, and Remoting; the
  Remoting fixture generator example compiled; Maven `verify` passed all 37
  scenarios, fixture drift checks, 66 typed-header and 15 typed-body
  cross-decodes. Formatting and diff checks passed. Code commit: `e5887718`.
- Broad P18-V01 bidirectional reconciliation remains open. P18-V02's full
  server-free gate, P18-V03 provenance/version/registry preflight, current
  Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30 local Docker/Broker
  scenario runners and required profiles remain open.

## P18-V01 LockBatchResponseBody API inventory — 2026-10-18 (43a7cebb)

- [x] The invoked Java 5.5.1 fixture records the public no-argument
  constructor, getter/setter signatures, and empty public-field inventory.
  Rust asserts those declarations and compile-fail checks that
  lock_okmq_set is private. Existing default/null, shared Set/element alias,
  and Java identity behavior remain covered. Fixture SHA-256:
  bf612d3a20c17748bd2026256383e92dfb8f0dcde455677583036fcdbc92543c.
- [x] Protocol/Core tests and doctests pass; strict all-target/all-feature
  Clippy passes for Protocol, Core, and Remoting; the Remoting fixture
  generator example compiles; pinned Maven verify passes all 37 scenarios,
  fixture checks, 66 typed-header and 15 typed-body cross-decodes. Formatting
  and diff checks pass.
- Broad P18-V01 bidirectional reconciliation remains open. P18-V02's complete
  workspace server-free gate must be refreshed after this change. P18-V03
  provenance/version/registry preflight, current Ubuntu/native Windows P18-V04
  evidence, and Phase 19's 30 local Docker/Broker scenario runners and required
  profiles remain open.

## P18-V02 current-worktree full server-free gate — 2026-10-18 (43a7cebb)

- [x] Default and all-feature locked workspace tests, all-target/all-feature
  workspace check and strict Clippy pass. Maven verify passes all 37
  scenarios, fixture drift checks, 66 typed-header and 15 typed-body
  cross-decodes. Differential comparator self-test and 37-scenario manifest
  validation pass.
- [x] Offline race/leak smoke passes 16 cases × 3. Fixed-seed fuzz smoke
  passes 120 frame seeds, 11 message seeds, 2,096 mutations, 164 frame
  decodes, and 120 message decodes. Release facade examples and all workspace
  benchmark targets compile.
- [x] The offline benchmark ran on macOS ARM64 Apple M5 Max (18 logical CPUs)
  with Rust 1.85.0. Per-iteration observations: frame encode/decode
  2,237.8 ns; Zstd message decode 1,143.4 ns; Producer request construction
  1,195.5 ns; 128-entry route lookup 14.8 ns; loopback pending completion
  65,795.8 ns. Allocation counts/bytes per iteration were respectively
  40/4,676; 17/197,493; 54/3,665; 0/0; and 81/9,546. These are observations,
  not thresholds.
- [x] Locked Cargo metadata resolves 216 packages (208 registry packages);
  workspace metadata/license/dependency checks and seven product package
  listings pass, with LICENSE and NOTICE in each listing. Four Unique ID tests
  pass under both UTC and Asia/Taipei. Formatting and diff checks pass.
- This closes the P18-V02 server-free gate on code revision 43a7cebb. Broad
  P18-V01 bidirectional API/settings/input/error/flow reconciliation and Rust
  reverse-export audit remain open. P18-V03 Java artifact/source provenance,
  version and registry preflight, and current Ubuntu/native Windows P18-V04
  evidence remain open. Phase 19's remaining local scenario runners and
  profiles are separate work.

## P18-V03 Java provenance and package preparation — 2026-10-18

- [x] Reconfirmed the pinned Maven dependency tree is Java client 5.5.1,
  remoting 5.5.1, and common 5.5.1. Local binary JAR, source JAR and POM
  checksums match Maven Central's published SHA-1 values. Four representative
  source-JAR files byte-match the exact Apache RocketMQ 5.5.1 tag commit.
  Details and SHA-256 values are in
  [PHASE_18_JAVA_PROVENANCE.md](compatibility/PHASE_18_JAVA_PROVENANCE.md).
- [x] Reviewed the locked Rust package graph, product package contents,
  LICENSE/NOTICE inclusion, dependency direction, package order and compatibility
  baselines. Facade examples and Phase 18 server-free checks pass. The source
  tag's OpenPGP signature is present but could not be verified because gpg is
  unavailable in this environment; the exact tag target and source-file
  comparisons are recorded.
- P18-V03 preparation review is complete. Workspace crates remain explicitly
  unpublished at 0.1.0. Confirming the release version, checking registry
  resolution, enabling publication, publishing or tagging remains
  post-Phase-19 work. Broad P18-V01 and current Ubuntu/native Windows P18-V04
  evidence remain open.

## P18-V01 Java body DTO public-field inventory — 2026-10-18 (`2b15d393`)

- [x] Reconciled ten body DTOs whose Rust fields were public while the Java
  fields are private: `CheckClientRequestBody`, `QueryAssignmentRequestBody`,
  `QueryAssignmentResponseBody`, `SetMessageRequestModeRequestBody`,
  `LockBatchRequestBody`, `UnlockBatchRequestBody`,
  `ConsumeMessageDirectlyResult`, `MessageRequestModeSerializeWrapper`,
  `GetConsumerStatusBody`, and `ResetOffsetBody`. Rust callers now use the
  Java-equivalent accessors. Compile-fail doctests guard representative
  private fields; the invoked Java fixture records empty public-field lists.
  Fixture SHA-256:
  `dd9fb47b7ee42cf67e602e691baf52d0a1b4ba7dffdb0bef0fa158412a2aba16`.
- [x] Code and fixture commit: `2b15d393`.
- This closes the identified body DTO field-visibility slice. Broad P18-V01
  source/API/settings/input/error/flow and reverse-export reconciliation
  remains open.

## P18-V02 current-worktree server-free gate — 2026-10-18 (`2b15d393`)

- [x] Default and all-feature locked workspace tests, all-target/all-feature
  workspace check, and strict workspace Clippy pass. Offline Maven `verify`
  passes all 37 scenarios, fixture drift checks, and Java/Rust cross-decodes
  (66 typed headers and 15 typed bodies). Differential comparator self-test
  and 37-row scenario-manifest validation pass.
- [x] Deterministic race/leak smoke passes 16 cases × 3. Fixed-seed fuzz
  smoke passes 120 frame seeds, 11 message seeds, 2,096 mutations, 164 frame
  decodes, and 120 message decodes. Release facade examples and the offline
  benchmark compile; the benchmark runs on macOS ARM64 Apple M5 Max
  (18 logical CPUs), Rust 1.85.0. Observations: frame encode/decode 2,058.9
  ns; Zstd message decode 1,079.7 ns; Producer request construction 1,189.2
  ns; 128-entry route lookup 15.6 ns; loopback pending completion 67,962.2
  ns. These are observations, not thresholds.
- [x] Locked Cargo metadata resolves 216 packages (208 registry packages).
  Workspace metadata/license/dependency checks and seven package listings
  pass with `LICENSE` and `NOTICE`; four Unique ID tests pass under both UTC
  and Asia/Taipei. Formatting and diff checks pass.
- P18-V02 server-free gates are complete on `2b15d393`. Broad P18-V01,
  current-revision GitHub Ubuntu address-profile evidence, and native Windows
  stack-runtime evidence remain open. Phase 19 still needs 30 of 37 local
  Docker/Broker scenario runners and required profiles; this is Phase 19
  automation work.

## P18-V01 `MessageQueueAssignment` field visibility — 2026-10-18 (`40ccbae8`)

- [x] Pinned Java 5.5.1 reflection confirms `MessageQueueAssignment` exposes
  no public fields, a no-argument constructor, and getters/setters for its
  three properties. Rust fields are private; existing shared alias, null,
  default, and JSON behavior now uses the matching accessors. The invoked Java
  runtime fixture records the public API inventory. Fixture SHA-256:
  `b264398de11245733fc53f99db5573b7e8a28bf58fb2e32d5b383993bcfa60f3`.
- [x] All-feature tests passed for Protocol, Consumer, and Core, including
  71 Protocol doctests; strict all-target/all-feature Clippy passed for those
  three crates. Pinned offline Maven `verify` passed all 37 scenarios, fixture
  checks, 66 typed-header and 15 typed-body cross-decodes. Formatting and diff
  checks passed. Code/fixture commit: `40ccbae8`.
- This closes only the `MessageQueueAssignment` field-visibility slice. Broad
  P18-V01 source/API/settings/input/error/flow and Rust reverse-export
  reconciliation remains open. The complete current-revision P18-V02 gate,
  current Ubuntu/native Windows P18-V04 evidence, and remaining Phase 18
  acceptance work are still pending.

## P18-V01 POP batch ACK DTO field visibility — 2026-10-18 (`a300224f`)

- [x] Pinned Java 5.5.1 reflection confirms `BatchAck` and
  `BatchAckMessageRequestBody` expose no public fields, with their existing
  constructors and getter/setter inventories. Rust fields are now private;
  producer code, test fixtures, and integration call sites use the existing
  Java-equivalent accessors. Compile-fail doctests guard both DTOs. Fixture
  SHA-256: `1eea96b0635b4a8f8c8f2a82187c1702ac0335f529fa2b6790d0ca2ddab2c53f`.
- [x] All-feature tests passed for Protocol, Consumer, Remoting, and Core;
  Protocol passed 143 unit tests and 73 doctests. Strict all-target/all-feature
  Clippy passed for the four crates. Pinned offline Maven `verify` passed all
  37 scenarios, fixture checks, 66 typed-header and 15 typed-body
  cross-decodes. Formatting and diff checks passed. Code/fixture commit:
  `a300224f`.
- This closes only the two POP batch ACK DTO field-visibility rows. Broad
  P18-V01 API/settings/input/error/flow and Rust reverse-export reconciliation,
  the final workspace P18-V02 gate, and current Ubuntu/native Windows P18-V04
  evidence remain open.

## P18-V01 Consumer Running Info DTO field visibility — 2026-10-18 (`8e9b8821`)

- [x] Pinned Java 5.5.1 reflection confirms `ConsumerRunningInfo` exposes its
  six documented public constants while `ProcessQueueInfo`,
  `PopProcessQueueInfo`, and `ConsumeStatus` expose no public fields. Rust
  instance fields are private and callers use accessors. The invoked fixture
  records constructor, public-field, declared-method, and callable-method
  inventories plus identity semantics. Fixture SHA-256:
  `728c824865b335c102c653e36f19676d6bfd935bad1b473f6ce0676f461091de`.
- [x] All-feature locked/offline tests passed for Protocol, Consumer, and Core;
  strict all-target/all-feature Clippy passed for Protocol, Consumer, Remoting,
  and Core. Offline Maven `verify` passed all 37 scenarios, fixture checks,
  66 typed-header and 15 typed-body cross-decodes. Formatting and diff checks
  passed. Code commit: `8e9b8821`.
- This closes only these DTO field-visibility rows. The captured method
  inventory is evidence for continued API reconciliation, not a claim that the
  broad P18-V01 API/settings/input/error/flow and Rust reverse-export audit is
  complete. The final current-revision P18-V02 gate and current Ubuntu/native
  Windows P18-V04 evidence remain open.

## P18-V01 Running Info nested DTO `toString` — 2026-10-18 (`4953cde5`)

- [x] Added Java-compatible Rust `Display`/`ToString` implementations for
  `ProcessQueueInfo` and `PopProcessQueueInfo`. The pinned Java fixture records
  both outputs, including local timestamp formatting for `ProcessQueueInfo`;
  the Protocol differential checks exact strings. Fixture SHA-256:
  `cefcb0d2f68f88acc12996a5068e9f12449676d8313b502fe8d2e9eeccbc69c5`.
- [x] The focused Protocol test and strict all-target/all-feature Protocol
  Clippy passed. Offline Maven `verify` passed all 37 scenarios, fixture
  checks, 66 typed-header and 15 typed-body cross-decodes. Formatting and diff
  checks passed. Code commit: `4953cde5`.
- This closes only the two nested DTO text methods. Full P18-V01 and the final
  workspace P18-V02 gate remain open.

## P18-V01 consumer-offset header visibility and API — 2026-10-18 (`b71406fa`)

- [x] Pinned Java 5.5.1 inventories confirm that
  `QueryConsumerOffsetRequestHeader`, `UpdateConsumerOffsetRequestHeader`, and
  `QueryConsumerOffsetResponseHeader` expose no public fields. Rust fields are
  crate-private, Java-equivalent accessors are available, and the two request
  headers match Java `toString()`. Update requests also encode/decode inherited
  `lo`. Fixture SHA-256:
  `2a62dd6f4171bd043a21c9f9fbf3f79299a07723d165540a1589d5d7312abab1`.
- [x] Workspace all-target/all-feature `cargo check`, focused Protocol/Core/
  Consumer offset and RPC tests, strict Clippy for those three crates, offline
  Maven `verify` (37 scenarios, fixture drift and 66 typed-header/15 typed-
  body cross-decodes), formatting, and diff checks pass. Code commit:
  `b71406fa`.
- This closes only the three consumer-offset header rows. Broad P18-V01
  bidirectional source/API/settings/input/error/flow and reverse-export
  reconciliation, the final current-revision P18-V02 gate, current GitHub
  Ubuntu and native Windows P18-V04 evidence, and Phase 19's 30 missing local
  Broker/Docker scenario runners/profiles remain open. P18-V03 tag-signature
  verification is unavailable without GPG; release version/registry preflight
  is deferred until after Phase 19.

## P18-V01 query/admin header field visibility — 2026-10-18 (`78ac649f`)

- [x] Pinned Java 5.5.1 inventories confirm that
  `QueryTopicsByConsumerRequestHeader`,
  `QuerySubscriptionByConsumerRequestHeader`, and
  `QueryTopicConsumeByWhoRequestHeader` expose no public fields. Rust fields
  are crate-private and the Java-equivalent getters/setters cover owned and
  inherited request values. Compile-fail doctests guard representative fields;
  decode-mode fixture checks both reflection modes and the Java API inventory.
  Fixture SHA-256:
  `1a526e62792d4207d3d0455ff47d9ba86e23a162a368255b59656701eb1f8e1b`.
- [x] Workspace all-target/all-feature check, focused Remoting and Core admin
  tests, Protocol doctests, strict Clippy for Protocol/Remoting/Core, offline
  Maven `verify` (37 scenarios, fixture drift and 66 typed-header/15 typed-body
  cross-decodes), formatting, and diff checks pass. Code commit: `78ac649f`.
- This closes only these three header visibility rows. Broad P18-V01
  bidirectional reconciliation, the final current-revision P18-V02 gate,
  current GitHub Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30
  missing local Broker/Docker scenario runners/profiles remain open.

## P18-V01 admin header field visibility and inherited `lo` — 2026-10-18 (`3b475951`)

- [x] Pinned Java 5.5.1 inventories confirm no public fields on
  `QueryConsumeTimeSpanRequestHeader`, `GetTopicStatsInfoRequestHeader`,
  `DeleteTopicRequestHeader`, `DeleteTopicFromNamesrvRequestHeader`, and
  `DeleteSubscriptionGroupRequestHeader`. Rust fields are crate-private with
  Java-mapped getters/setters and compile-fail guards. The inherited `lo`
  property on `QueryConsumeTimeSpanRequestHeader` is now preserved by Rust
  encode/decode and included in the Java typed-header fixture and validation
  matrix. Query-admin inventory fixture SHA-256:
  `6e92e3d073dcb3b13abbc9c93b94955fd306a5b47576b5c8c52c472a7f668655`.
- [x] Workspace all-target/all-feature check; Protocol header tests and 88
  doctests; Remoting query-admin differential; Core's 50 admin RPC tests and
  consume-time-span tests; Consumer consume-time-span test; strict Clippy for
  Protocol/Remoting/Core/Consumer; offline Maven `verify` (37 scenarios,
  fixture drift, 66 typed-header and 15 typed-body cross-decodes); format and
  diff checks pass. Code commit: `3b475951`.
- This closes only these five header rows and the `QueryConsumeTimeSpan.lo`
  mapping. Broad P18-V01 reconciliation, the final current-revision P18-V02
  gate, current GitHub Ubuntu/native Windows P18-V04 evidence, and Phase 19's
  30 missing local Broker/Docker scenario runners/profiles remain open.

## P18-V01 `DeleteKVConfigRequestHeader` visibility — 2026-10-18 (`cd3742dc`)

- [x] Pinned Java 5.5.1 inventory confirms `DeleteKVConfigRequestHeader`
  exposes no public fields. Rust `namespace` and `key` are crate-private with
  Java-equivalent accessors; a compile-fail doctest guards field privacy. The
  invoked fast/reflection decode fixture now captures this API and both decode
  modes. Query-admin fixture SHA-256:
  `645d41ced3b7bb8ccb355be4546867207335bf7eb2f40aab889d2583aac5d461`.
- [x] Workspace all-target/all-feature check, Protocol header validation and
  89 doctests, Remoting query/admin differential, both Core Delete KV RPC
  regressions, strict Clippy for Protocol/Remoting/Core, offline Maven
  `verify` (37 scenarios, fixture checks, 66 typed-header and 15 typed-body
  cross-decodes), formatting, and diff checks pass. Code commit: `cd3742dc`.
- This closes only the `DeleteKVConfigRequestHeader` visibility row. Broad
  P18-V01 bidirectional reconciliation, the final current-revision P18-V02
  gate, current GitHub Ubuntu/native Windows P18-V04 evidence, and Phase 19's
  30 missing local Broker/Docker scenario runners/profiles remain open.

## P18-V01 offset-boundary header field visibility — 2026-10-18 (`c648552f`)

- [x] Pinned Java 5.5.1 inventories confirm no public fields on the four
  offset-boundary request headers and four response headers. Rust fields are
  crate-private with Java-equivalent getters/setters, including inherited
  topic/queue/request properties and the Search boundary getter's `LOWER`
  fallback. The invoked fast/reflection fixture now captures full Java public
  API inventories and asserts the empty public-field lists. Fixture SHA-256:
  `ab124c979a93556d65660bcc8b9aeaf680649a3c7eb787dc21cb1ef843c167ad`.
- [x] Workspace all-target/all-feature check; offset-boundary Protocol tests
  and 97 doctests; Remoting request/response decode-mode differentials; Core
  invoked offset RPC and admin regressions; Producer and Consumer offset facade
  regressions; strict workspace Clippy; offline Maven `verify` (37 scenarios,
  fixture checks, 66 typed-header and 15 typed-body cross-decodes); formatting
  and diff checks pass. Code commit: `c648552f`.
- This closes only these eight header field-visibility rows. Broad P18-V01
  bidirectional reconciliation, the final current-revision P18-V02 gate,
  current GitHub Ubuntu/native Windows P18-V04 evidence, and Phase 19's 30
  missing local Broker/Docker scenario runners/profiles remain open.

## P18-V01 `QueryMessageResponseHeader` field visibility — 2026-10-18 (`85c9bae5`)

- [x] Pinned Java 5.5.1 inventory confirms
  `QueryMessageResponseHeader` exposes no public fields. Rust watermark fields
  are crate-private with Java-equivalent getters/setters; Core, Consumer,
  Remoting and Protocol callers use the accessors. The invoked Java API inventory
  and Rust field-visibility differential include the class. Query-admin fixture
  SHA-256: `482d0d317162c15b8d159326f5ef59822b3142241c0dad00ea2ba293121d35e0`.
- [x] Workspace all-target/all-feature check, Protocol's 15 header integration
  tests and 98 doctests, Remoting query/admin differential, six Core query RPC
  regressions, Consumer query facade regression, strict workspace Clippy,
  offline Maven `verify` (37 scenarios, fixture checks, 66 typed-header and 15
  typed-body cross-decodes), formatting, and diff checks pass. Code commit:
  `85c9bae5`.
- This closes only the `QueryMessageResponseHeader` visibility row. Broad
  P18-V01 bidirectional reconciliation, the final current-revision P18-V02
  gate, current GitHub Ubuntu/native Windows P18-V04 evidence, and Phase 19's
  30 missing local Broker/Docker scenario runners/profiles remain open.

## P18-V01 remaining response-header field visibility — 2026-10-19

- [x] Pinned Java 5.5.1 inventories confirm that
  `PullMessageResponseHeader`, `SendMessageResponseHeader`,
  `CheckTransactionStateResponseHeader`, `NotificationResponseHeader`,
  `PopMessageResponseHeader`, `PopLiteMessageResponseHeader`, and
  `PollingInfoResponseHeader` expose no public fields. The separate
  `ChangeInvisibleTimeResponseHeader` fixture also reports zero public fields.
  Their Rust backing fields are now crate-private with the Java getter/setter pairs public;
  nullable wrappers remain nullable, and primitive defaults are preserved.
  The response identity fixture records the 16-header inventory; the separate
  change-invisibility fixture records its exact method inventory.
  Compile-fail doctests guard field visibility, and Protocol tests exercise
  accessors/null defaults while Remoting/Core callers use them.
- [x] `cargo fmt --all -- --check`, `git diff --check`, and pinned offline
  Maven `verify` pass (37 scenarios, 66 typed-header and 15 typed-body
  cross-decodes). The full Remoting all-feature suite passes (138 unit, 8
  codec, 1 fuzz-regression, 7 security, 107 transport, and 18 doctests),
  including the corrected Query Admin and Offset Boundary API-inventory
  assertions. Protocol and full-workspace gates remain to be rerun before this
  slice is finalized.
- This closes the response-header field-visibility/API slice only. Broad
  P18-V01 source/API/settings/input/error/flow and Rust reverse-export
  reconciliation, current-revision Ubuntu/native Windows evidence, and Phase
  19 live Broker validation remain separate gates.

## P18-V01 Lite topic-info header inventory follow-up — 2026-10-19

- [x] The Rust `GetLiteTopicInfoRequestHeader` and Core RPC were implemented,
  but the Rust-to-Java typed-header fixture inventory did not include the
  public Java header. Added its Java API/nullable-field fixture assertions,
  typed-frame generation, reverse decode, and validation-matrix row. Code
  commits: `d578db50`, `d4d7588e`.
- [x] Maven `verify` passes all 37 differential scenarios, fixture checks, 67
  exact typed-header frames, and 15 typed-body frames. Protocol header
  inventory (5 tests), header/API/validation tests (19 tests), rustfmt, and
  `git diff --check` pass. Frame SHA-256:
  `92e15800cdfb810908a8a016582189b5523bdc2d7dbbfc8ff69523f2a7184884`.
- This closes one previously unrecorded header fixture. Full P18-V01
  source/API/settings/input/error/flow and reverse-export reconciliation plus
  final workspace validation remain open.

## P18-V01 `ClientGroup` public-final field parity — 2026-10-19

- [x] The pinned Lite topic-info fixture reports `ClientGroup.clientId` and
  `ClientGroup.group` as `public final String`, with a public two-argument
  constructor and no setters. Rust fields are now private with read-only
  `client_id()`/`group()` accessors; two compile-fail doctests reject direct
  field access and mutation. The existing Protocol differential also asserts
  both accessor values for the populated constructor case.
- [x] `cargo fmt --all -- --check` passed. The locked Protocol `--tests` suite
  passed 145 unit tests and all 39 integration tests. The `ClientGroup`-filtered
  rustdoc run passed both new compile-fail guards. Workspace Rustdoc JSON
  generation succeeded and reports 243 remaining public fields after this
  correction. Code commit: `f44b942a`.
- This closes only the `ClientGroup` mutability discrepancy. Broad P18-V01
  bidirectional API/settings/flow and reverse-export reconciliation remains
  open.

## P18-V02/V03 current-worktree server-free gate — 2026-10-19 (`aaff724e`)

- [x] On code revision `aaff724edb116c87e73c2040b73c56841d18ab31`, the locked
  default and all-feature workspace tests, `cargo fmt --all -- --check`, strict
  all-target/all-feature Clippy, the Asia/Taipei unique-ID fixture, differential
  comparator self-test and 37-scenario manifest validation passed.
- [x] The fixed-seed fuzz smoke passed with 121 frame seeds, 11 message seeds,
  2,112 bounded mutations, 170 frame decodes and 109 message decodes. Offline
  deterministic race/leak smoke passed all 16 cases three times each.
- [x] Offline Maven `verify` passed all 37 scenarios and fixture checks,
  including 67 exact typed-header and 15 typed-body cross-decodes. The facade
  examples built in release mode; the offline benchmark compiled and ran. Its
  current-revision observations are in
  [PHASE_18_OFFLINE_BENCHMARK_BASELINE.md](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
- [x] Workspace metadata/dependency-direction checks and package listings for
  all seven product crates passed; each listing includes `LICENSE`, `NOTICE`,
  and `src/lib.rs`. The Java tag still resolves to pinned source commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9`, and the harness pins client
  `5.5.1`. No registry resolution, publication or release claim is made.
- This closes the local server-free P18-V02 gate and current-tree package
  preparation slice. Broad P18-V01 bidirectional source/API/settings/input/
  error/flow and reverse-export reconciliation remains open. Current-revision
  GitHub Ubuntu and native Windows P18-V04 evidence is not available from this
  local run; no CI was triggered. Phase 19 live Broker/Docker validation was
  not run. See the execution log for command and environment details.

## P18-V02 current-revision refresh — 2026-10-21 (`bbcc88ae`)

- [x] Default and all-feature locked workspace tests, strict workspace Clippy,
  formatting, Maven `verify` (37-scenario manifest and fixture drift checks),
  differential comparator self-test, 37-scenario manifest validation, and
  the Asia/Taipei Unique ID fixture pass on the current revision.
- [x] Offline deterministic race/leak smoke passes all 16 cases three times.
  Bounded fuzz smoke passes with 121 frame seeds, 11 message seeds, 2,112
  mutations, 170 frame decodes and 109 message decodes. Release facade examples
  build; the fixed-input offline benchmark runs. Current sample is recorded in
  [PHASE_18_OFFLINE_BENCHMARK_BASELINE.md](compatibility/PHASE_18_OFFLINE_BENCHMARK_BASELINE.md).
- [x] Locked Cargo metadata, workspace license/dependency-direction checks and
  all seven product package listings pass; each listing contains `LICENSE`,
  `NOTICE`, and library source. The package graph remains unpublished at
  version `0.1.0`; no registry or release check is claimed.
- The 37 live handoff rows validate and the checked-in CI workflow defines
  server-free Linux gates. No remote workflow or native Windows run was
  triggered. Broad P18-V01 remains open because Rust still does not expose the
  pinned Java mutable nested rebalance tables and concrete process queue API;
  Phase 19 Broker/Docker validation remains separate.


## P18-V01 nested POP API slice — 2026-10-21

- Exposed the Java `PopProcessQueue` methods and added a pinned direct Java
  behavior fixture for its signed counter updates, ACK return, queue-info fill
  and text representation. Push/Pull owners expose live subscription/topic
  tables; Push exposes the live POP table; both create the public POP queue.
- The regenerated facade API fixture is stable across two generations and
  passes its focused Java drift verifier. SHA-256:
  `e9690f9738fb8ccbfe81b8187e4b92b4676deca106a29b2dced86774e88a97e4`.
- Full pinned Maven `verify`, default and all-feature locked workspace tests,
  strict workspace Clippy, Consumer all-feature tests (236 unit tests plus
  integration/doctest targets), the focused facade API suite, rustfmt and
  `git diff --check` pass on this worktree.
- This closes only the exposed POP queue slice. P18-V01 remains open for the
  public `ProcessQueue` API, live process-queue tables, Pull's POP table and
  complete inherited rebalance method correspondence. Refresh the remaining
  P18-V02 supplementary gates after implementation work. P18-V03 package and
  provenance review and current Ubuntu/native Windows P18-V04 evidence remain
  open; Phase 19 Broker/Docker validation was not run.

## P18 ProcessQueue API slice — 2026-10-21

- [x] Exposed Java-shaped `ProcessQueue` methods, live message-tree and atomic
  counter handles, and the same consume lock used by orderly processing. Push
  and Pull owners expose live process-queue tables; Pull assignment updates its
  process table as queues are added and removed. Added direct Java behavior and
  Rust facade tests. The regenerated fixture SHA-256 is
  `d2a38d2f6f18436517520b28213e81f8d7b267895b0b6ba194f31fa4a4b4b3b6`.
- [x] Consumer all-feature tests (236 unit tests plus integration and doctest
  targets), strict all-target/all-feature Consumer Clippy, focused Java fixture
  drift verification, pinned Maven `verify` (37 scenarios and fixture checks),
  formatting and `git diff --check` pass on this worktree.
- P18-V01 remains open for inherited rebalance call correspondence and Pull POP
  assignment flow. Refresh the full current-revision P18-V02 supplementary
  gates after implementation; P18-V03 review and current Ubuntu/native Windows
  P18-V04 evidence remain open. Phase 19 Broker/Docker validation was not run.

## P18 Pull rebalance behavior slice — 2026-10-21

- [x] Matched pinned `RebalancePullImpl` behavior: client rebalance is enabled,
  both pull-offset helpers return zero, `consumeType` is active Pull, process
  queue creation succeeds, POP queue creation returns null, and POP init mode
  reports `no initMode for Pull`. The Pull POP table remains empty. Java fixture
  SHA-256: `fe4523a2b16b7737e977dd6b5ff7beec9b4cb2e6c16b15aac6b43a6cd1f16d3a`.
- [x] The seven-test Consumer facade API suite, focused Java fixture drift
  verifier, formatting and `git diff --check` pass.
- P18-V01 remains open for the remaining inherited rebalance method/settings
  correspondence and the other full client audit rows. P18-V02 refresh and
  P18-V03/V04 review remain outstanding; Phase 19 Broker/Docker validation was
  not run.

## P18 Pull rebalance settings slice — 2026-10-21

- [x] Pull rebalance model and allocation-strategy values now live in shared
  state used by the active registration. Their setters accept a shared owner
  and request an immediate rebalance after startup. The pinned Java fixture
  now checks Pull group/model/strategy setter results and strategy identity;
  regenerated `fixtures/consumer/facade-api.json` SHA-256:
  `7a7234d87c260b398f73bbf4b868714f87c5c9f27a6f0659cb0b93a7e9e834f0`.
- [x] Focused facade tests (9), Consumer all-feature tests (236 unit tests,
  all integration targets and 13 doctests), strict Consumer Clippy, pinned Java
  fixture drift verification, formatting and `git diff --check` pass.
- This closes the Pull model/strategy mutability slice only. Broad P18-V01
  source/API/settings/input/error/flow and reverse-export reconciliation,
  current-revision full-workspace P18-V02, final P18-V03 package/provenance
  review and current Ubuntu/native Windows P18-V04 evidence remain open.
  Phase 19 Broker/Docker validation was not run.

## P18 inherited rebalance method crosswalk — 2026-10-21

- [x] Added a method-level crosswalk for all 32 pinned Java
  `RebalanceImpl` callable methods, including the raw factory replacement and
  mutable dispatch-list adaptations. A Rust fixture test now rejects any
  inherited Java method absent from the crosswalk or its owner source mapping.
- [x] Focused Consumer facade API suite passes all 10 tests. The full Consumer
  all-feature suite, strict Consumer Clippy, pinned fixture drift check,
  formatting and diff checks passed immediately before this crosswalk-only
  follow-up.
- This closes the inherited rebalance crosswalk inventory. Broad P18-V01 still
  covers the rest of the Java client source, input/error/control-flow and
  reverse-export audit.

## P18 `MQHelper` timestamp reset overloads — 2026-10-21

- [x] Added the public `MqHelper` utility to the Consumer crate and top-level
  facade. Its deprecated overload selects instance `DEFAULT`; the explicit
  overload configures the requested model and instance, searches queue offsets
  in Java `TreeSet` order, updates only nonnegative offsets, persists the
  fetched queue set, and shuts down after the operation.
- [x] Pinned Java reflection now records both `MQHelper.resetOffsetByTimestamp`
  overloads in `fixtures/consumer/facade-api.json`; SHA-256:
  `6d38837ca80e7012a35069800144888fa0980c5e2783785b7e998c1d2efa2b1c`.
  Rust tests cover sorted queue visitation, negative-offset filtering and
  stopping at the first search/update error.
- [x] Consumer all-feature tests (239 unit tests, integration targets and 13
  doctests), strict Consumer Clippy, the facade public-type test, Java fixture
  drift verification, formatting and diff checks pass.
- This closes the `MQHelper` utility API slice only. Broad P18-V01 remains open
  for Java source/API/settings/input/error/flow and reverse-export
  reconciliation; current-revision full-workspace P18-V02, final P18-V03
  review, and current Ubuntu/native Windows P18-V04 evidence remain outstanding.
  Phase 19 Broker/Docker validation was not run.

## P18-V01 Validator permission and Broker config RPC slice — 2026-10-21

- [x] Pinned Java reflection now records all seven `Validators` public methods,
  all eight `PermName` methods and permission constants, and numeric/string
  validation outcomes. The Producer fixture SHA-256 is
  `8216bf9c1a66b57a61e88af448f61ab38dc84015ffd872ee340d68896d3ec9b5`.
- [x] Common now exposes the mapped permission masks, predicates, formatter
  and Java-compatible string parser. Producer validation maps topic and Broker
  config checks, including code-16 exception text and malformed-integer
  behavior. Core validates supplied topic permission and implements
  `update_broker_config` with the empty-map no-op, UTF-8 Properties body,
  configured VIP offset and Broker response path.
- [x] Common/Core/Producer all-feature test suites, focused permission and
  Broker RPC regressions, strict all-target/all-feature Clippy for Common,
  Core, Producer and the facade, formatting, and pinned Maven `verify` (37
  scenarios, fixture drift and Java/Rust cross-decodes) pass.
- This closes the listed permission methods and Broker-config operation only.
  Broad P18-V01 remains open, and the full current-revision P18-V02 refresh,
  P18-V03 package/provenance review and current Ubuntu/native Windows P18-V04
  evidence remain outstanding. No remote CI or Phase 19 Broker/Docker run was
  triggered.

## P18-V01 `getBrokerConfig` Properties read slice — 2026-10-21

- [x] Added the Java 5.5.1 `MQClientAPIImpl.getBrokerConfig(String,long)`
  mapping to Core request 26. The request has no header/body, preserves the
  supplied address with VIP enabled, and decodes Java Properties behavior for
  escaped separators, Unicode escapes, malformed escapes, raw non-ASCII text,
  and a final backslash. Missing response bodies map to a structured Rust
  `MalformedResponse`; Java dereferences null before parsing.
- [x] At the read-operation checkpoint, `fixtures/producer/core.json` SHA-256:
  `53f8d3ae387f5df155d0c10484815f3a71d00debafb70c82ed0912f3848bca2f`.
- [x] Core all-feature locked tests (176 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy for Common/Core/
  Producer/facade, Producer fixture comparison, format checks, and pinned Java
  Maven `verify` (37 scenarios, fixture drift and cross-decodes) pass.
- This closes only the Broker config read operation and Properties parser
  inputs. Broad P18-V01, the final current-revision P18-V02 refresh, P18-V03
  package/provenance review and current Ubuntu/native Windows P18-V04 evidence
  remain open. Phase 19 Broker/Docker validation was not run.

## P18-V01 cold-data flow and CommitLog read-ahead RPC slice — 2026-10-21

- [x] Added the four pinned `MQClientAPIImpl` low-level operations: request
  2001 updates cold-data group Properties; request 2002 removes a group;
  request 2003 reads nullable status text; request 2004 sets CommitLog
  read-ahead mode and returns a nullable remark. Tests cover no-op inputs,
  exact request bodies/ext fields, VIP transformation for mutations, direct
  address for reads, empty/null results and Broker errors. The null mode is
  omitted from the remoting wire fields, matching Java's serializer.
- [x] Java invocation fixture and Rust scripted differential cover all four
  methods and their named response branches. Current producer core fixture
  SHA-256 at this slice checkpoint:
  `c20f4474d674bd4e0001f19c82fed8c5e2dfbe6f5c911ee6eade0fb1842c7cb0`.
- [x] Core all-feature locked suite (177 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy for Common/Core/
  Producer/facade, Producer fixture comparison, pinned Java Maven `verify`
  (37 scenarios, fixture drift and cross-decodes), formatting, and scoped
  whitespace checks pass.
- The high-level `rocketmq-tools` facade remains outside Phase 17. This closes
  the Core transport-operation mappings only; broad P18-V01, current-revision
  P18-V02, P18-V03 package/provenance review and current Ubuntu/native Windows
  P18-V04 evidence remain open. No remote CI or Phase 19 Broker/Docker run was
  triggered.

## P18-V01 `registerClient` heartbeat result slice — 2026-10-21

- [x] Added `MQClientApi::register_client` for pinned Java
  `MQClientAPIImpl.registerClient(String, HeartbeatData, long)`: request 34,
  encoded heartbeat body, empty header, supplied address, and synchronous
  boolean result (`true` only for `SUCCESS`; any other Broker code returns
  `false`).
- [x] Direct Java signature/invocation fixture and Rust scripted RPC compare
  both response branches, request code, exact body and empty ext fields.
  Fixture SHA-256 at this slice checkpoint:
  `434a3fe8b1cba71732f41ccb710e601e0d8ce4cbcf196265695cef7641b90905`.
- [x] Core all-feature locked suite (177 unit tests, all integration targets,
  and 16 doctests), focused Producer fixture comparison, strict all-target/
  all-feature Clippy for Common/Core/Producer/facade, pinned Java Maven
  `verify` (37 scenarios, fixture drift, 67 typed-header frames, 15 typed-body
  fixtures and cross-decodes), formatting, and scoped whitespace checks pass.
- This closes the direct registration method only. Broad P18-V01,
  current-revision P18-V02, P18-V03 and P18-V04 remain open; no remote CI or
  Phase 19 Broker/Docker validation ran.

## P18-V01 NameServer topic-list RPC slice — 2026-10-21

- [x] Added `MQClientApi::get_topic_list_from_nameserver` for the pinned
  `MQClientAPIImpl.getTopicListFromNameServer(long)` method. It sends request
  206 with no header/body through the NameServer selector, decodes `TopicList`,
  and preserves error responses. The Java direct invocation fixture records
  the exact method signature, null address, request envelope, topic set,
  Broker address and error result.
- [x] Rust NameServer differential checks success, error, code, Java language,
  empty ext fields and missing request body. The protocol `TopicList` now has
  Java `RemotingSerializable` encode/decode adapters. Fixture SHA-256 at this
  slice checkpoint:
  `6e0a6f94f934f9e173aa76a5e4e3ab18c70ee1121f41f1dfbb52fbbac303a2ef`.
- [x] Core all-feature locked suite (177 unit tests, all integration targets,
  and 16 doctests), Producer fixture comparison, strict all-target/all-feature
  Clippy for Common/Core/Producer/facade, pinned Java Maven `verify` (37
  scenarios, fixture drift, 67 typed-header frames, 15 typed-body fixtures
  and Java/Rust cross-decodes), formatting, and scoped whitespace checks pass.
- At this NameServer-only checkpoint, the composite `getSystemTopicList`
  operation remained open. This closes the direct topic-list RPC only; broad
  P18-V01, current-worktree P18-V02, P18-V03 and P18-V04 remain open.

## P18-V01 Broker system-topic-list RPC slice — 2026-10-21

- [x] Added `MQClientApi::get_system_topic_list_from_broker` for pinned Java
  `MQClientAPIImpl.getSystemTopicListFromBroker(String,long)`: request 305,
  VIP address transform, empty header/body, Java `TopicList` decode, and Broker
  errors. The direct Java invocation fixture and Rust scripted RPC compare the
  exact signature, address, timeout, response topic set, Broker error and
  `SUCCESS` without a body (Java `MQClientException` code 0).
- [x] Core VIP-enabled loopback confirms this operation connects through the
  configured two-port address offset.
- [x] Fixture SHA-256 at this Broker-only slice checkpoint:
  `fbe6f70615e8004c26bdba90fdc824241415d19557b57853a6a563f24becbd32`.
- [x] Core all-feature locked suite (178 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, and Producer pinned
  fixture comparison passed.
- [x] Pinned Java Maven `verify` passed (37 scenarios, 67 typed-header frames,
  15 typed-body fixtures, and Java/Rust cross-decodes); formatting and scoped
  `git diff --check` passed.
- At this Broker-only slice checkpoint, the composite `getSystemTopicList`
  call remained open. This closes the direct Broker operation only; broad
  P18-V01 and P18-V02/P18-V03/P18-V04 gates remain open.

## P18-V01 composite system-topic-list RPC slice — 2026-10-21

- [x] Added `MQClientApi::get_system_topic_list` for pinned Java
  `MQClientAPIImpl.getSystemTopicList(long)`: request 304 through NameServer
  selection, conditional request 305 through the returned Broker address, and
  in-place merge of the nonempty Broker topic set.
- [x] The Java invocation fixture and Rust differential cover both request
  codes and envelopes, timeout and call order, merge, empty/null topic sets,
  blank Broker address, NameServer/Broker errors and missing NameServer body.
- [x] Fixture SHA-256 at this composite slice checkpoint:
  `38b2eac88f25aa04674f469b4e7a685dc599834969f6a1d24613de8c98efdb23`.
- [x] Core all-feature locked suite (178 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, and Producer pinned
  fixture comparison passed.
- [x] Pinned Java Maven `verify` passed (37 scenarios, 67 typed-header frames,
  15 typed-body fixtures, and Java/Rust cross-decodes); formatting and scoped
  `git diff --check` passed.
- The composite system-topic-list operation is closed. Broad P18-V01 remains
  open, as do P18-V02/P18-V03/P18-V04 gates.

## P18-V01 batched subscription-group creation RPC slice — 2026-10-21

- [x] Added `MQClientApi::create_subscription_group_list` for pinned Java
  `MQClientAPIImpl.createSubscriptionGroupList(String,List,long)`: request 225,
  Java `SubscriptionGroupList` body, configured VIP transform, and Broker
  error handling.
- [x] Invoked Java fixture and Rust differential compare the exact encoded
  body for populated and empty lists, timeout, success and Broker-error
  outcomes; a VIP-enabled loopback verifies the two-port address offset.
- [x] Current producer core fixture SHA-256:
  `2479e9af49895298bf807b04775db0e3acef6ad2b6a0a6e2cf59a830ca89169e`.
- [x] Core all-feature locked suite (179 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, and Producer pinned
  fixture comparison passed.
- [x] Pinned Java Maven `verify` passed (37 scenarios, 67 typed-header frames,
  15 typed-body fixtures, and Java/Rust cross-decodes); formatting and scoped
  `git diff --check` passed.
- The batch subscription-group method is closed. The sibling batch topic
  method and broader P18-V01 API inventory remain open; P18-V02/P18-V03/P18-V04
  gates remain open.

## P18-V01 cluster topic-list RPC slice — 2026-10-21

- [x] Added `MQClientApi::get_topics_by_cluster` for pinned Java
  `MQClientAPIImpl.getTopicsByCluster(String,long)`: request 224 through
  NameServer selection with the `cluster` header field. The nullable adapter
  preserves Java's accepted null cluster and omits the null wire field.
- [x] The invoked Java fixture and Rust differential compare the signature,
  request envelope, cluster field, decoded topic list, response error and
  successful reply without a body, including null cluster behavior.
- [x] Fixture SHA-256 at this slice checkpoint:
  `ac3462b7351f327839c4a79db5a8a499f6fdbbd56f667f74b2ac5f18fb368fee`.
- [x] Core all-feature locked suite, strict all-target/all-feature Clippy,
  Producer pinned fixture comparison, pinned Java Maven `verify` (37 scenarios,
  67 typed-header frames, 15 typed-body fixtures and Java/Rust cross-decodes),
  formatting and scoped `git diff --check` passed.
- The cluster topic-list RPC is closed. Broader P18-V01 and P18-V02/P18-V03/
  P18-V04 gates remain open.

## P18-V01 batched topic creation RPC slice — 2026-10-21

- [x] Added `MQClientApi::create_topic_list` for pinned Java
  `MQClientAPIImpl.createTopicList(String,List,long)`: request 18 with a Java
  `CreateTopicListRequestBody`, configured VIP transform and Broker-error
  handling. The nullable adapter preserves Java's null-list and null-element
  body encoding.
- [x] Invoked Java fixture and Rust differential compare populated/default/
  custom, empty, null-list and null-element body bytes, request code, empty
  header, success and Broker-error outcomes; a VIP-enabled loopback verifies
  the two-port transform.
- [x] Current producer core fixture SHA-256:
  `fcf27463aa5fde04867de50543847ad53dabc1b5c3b526ae8ac2406481514ec7`.
- [x] Core all-feature locked suite (180 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, and Producer pinned
  fixture comparison passed.
- [x] Pinned Java Maven `verify` passed (37 scenarios, 67 typed-header frames,
  15 typed-body fixtures and Java/Rust cross-decodes); formatting and scoped
  `git diff --check` passed.
- The batch topic method is closed. Broader P18-V01 and P18-V02/P18-V03/P18-V04
  gates remain open.

## P18-V01 batch topic and subscription-group deletion RPC slice — 2026-10-21

- [x] Added `delete_topic_in_broker_list` for pinned Java
  `deleteTopicInBrokerList(String,List,long)`, request 5002, and
  `delete_subscription_group_list` for
  `deleteSubscriptionGroupList(String,List,boolean,long)`, request 5003. Both
  use Java-compatible request bodies and the configured VIP address; nullable
  adapters preserve null lists and list elements.
- [x] Invoked Java fixtures and Rust differentials compare populated, empty,
  null-list, null-element, success and Broker-error cases; group deletion also
  covers both `cleanOffset` values. A VIP-enabled loopback verifies both
  two-port transforms.
- [x] Current producer core fixture SHA-256:
  `33b7788c15092caaeb2e5cc42ac69eb7bcda5baf5ebc27c2caf01aac9c5e9ff3`.
- [x] Core all-feature locked suite (181 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, and Producer pinned
  fixture comparison passed.
- [x] Pinned Java Maven `verify` passed (37 scenarios, 67 typed-header frames,
  15 typed-body fixtures and Java/Rust cross-decodes); formatting and scoped
  `git diff --check` passed.
- The batch deletion RPCs are closed. Broader P18-V01 and P18-V02/P18-V03/
  P18-V04 gates remain open.

## P18-V01 unit topic-list RPC slice — 2026-10-21

- [x] Added the three pinned Java methods `getUnitTopicList`,
  `getHasUnitSubTopicList`, and `getHasUnitSubUnUnitTopicList` as NameServer
  requests 311, 312 and 313. Rust filters `%RETRY%` topics only when
  `isUnitMode` is false.
- [x] Invoked Java fixtures and Rust differential compare all three request
  envelopes, both filtering modes, response errors and missing-body behavior.
- [x] Current producer core fixture SHA-256:
  `bb5a19cb131f922828ff5effb8eb52ac4a0d95e8bae2863775214da4d6664e5b`.
- [x] Core all-feature locked suite (181 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, and Producer pinned
  fixture comparison passed.
- [x] Pinned Java Maven `verify` passed (37 scenarios, 67 typed-header frames,
  15 typed-body fixtures and Java/Rust cross-decodes); formatting and scoped
  `git diff --check` passed.
- These three RPC mappings are closed. Broad P18-V01 and P18-V02/P18-V03/
  P18-V04 gates remain open.

## P18-V01 NameServer configuration RPC slice — 2026-10-21

- [x] Added `update_name_server_config` for pinned Java request 318 and
  `get_name_server_config` for request 319, including explicit/configured
  NameServer selection, empty input behavior and Java Properties conversion.
- [x] Invoked Java fixtures and Rust differential compare per-server call
  order, fanout/short-circuit errors, property values, malformed content and
  missing-body behavior.
- [x] Current producer core fixture SHA-256:
  `4276123552ab57632da4a10177b1b529266707234e95785668cfcd12177e783c`.
- [x] Core all-feature locked suite, strict all-target/all-feature Clippy,
  Producer pinned fixture comparison, pinned Java Maven `verify`, formatting
  and `git diff --check` passed.
- These two RPC mappings are closed. Broad P18-V01 and P18-V02/P18-V03/P18-V04
  gates remain open.

## P18-V01 transaction resume-check RPC slice — 2026-10-21

- [x] Added `resume_check_half_message` and its nullable adapter for pinned
  Java `MQClientAPIImpl.resumeCheckHalfMessage(String,String,String,long)`,
  request 323. Null header fields are omitted; the request uses the configured
  VIP Broker address and returns `false` for every non-success response.
- [x] Invoked Java fixture and Rust loopback differential compare the method
  signature, request fields and empty body, success/non-success results,
  individually nullable fields and both-null input, and the VIP port offset.
- [x] Current producer core fixture SHA-256:
  `bbeea411ebb9a1cbf177a4f0efddbf85023a475288bd123764959acca248efc6`.
- [x] Core all-feature locked suite (182 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, Producer pinned
  fixture comparison, pinned Java Maven `verify`, formatting and scoped
  `git diff --check` passed.
- The named RPC mapping is closed after validation. Broad P18-V01 and
  P18-V02/P18-V03/P18-V04 gates remain open.

## P18-V01 timer-engine switch RPC slice — 2026-10-21

- [x] Added `MQClientApi::switch_timer_engine` for pinned Java
  `MQClientAPIImpl.switchTimerEngine(String,String,long)`, request 5001. It
  sends `E_T` including an empty string, uses the supplied address unchanged
  when VIP is enabled, and preserves Broker errors.
- [x] Invoked Java fixture and Rust VIP-enabled loopback compare signature,
  field value, empty body, address behavior, success and Broker rejection.
- [x] Current producer core fixture SHA-256:
  `bb28b038fdd66160007d8c757e53ed4cb78ebcad0a91b4ff9d950fa04eccee6c`.
- [x] Core all-feature locked suite (184 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, Producer pinned
  fixture comparison, pinned Java Maven `verify` (37 scenarios, 67 typed-header
  frames, 15 typed-body fixtures and Java/Rust cross-decodes), formatting and
  scoped `git diff --check` passed.
- The named RPC mapping is closed after validation. Broad P18-V01 and
  P18-V02/P18-V03/P18-V04 gates remain open.

## P18-V01 POP record export RPC slice — 2026-10-21

- [x] Added `MQClientApi::export_pop_record` for pinned Java
  `MQClientAPIImpl.exportPopRecord(String,long)`, request 200056
  (`POP_ROLLBACK`). The request has no header or body, uses the supplied
  address unchanged even when VIP is enabled, and reports non-success replies.
- [x] Invoked Java fixture and Rust VIP-enabled loopback compare the exact
  signature, request envelope, address behavior, success and Broker rejection.
- [x] Current producer core fixture SHA-256:
  `d94fd1f95eba2caf6ce77e312cd53a4109be74048b23db5ad694a2c5b7109242`.
- [x] Core all-feature locked suite (183 unit tests, all integration targets,
  and 16 doctests), strict all-target/all-feature Clippy, Producer pinned
  fixture comparison, pinned Java Maven `verify` (37 scenarios, 67 typed-header
  frames, 15 typed-body fixtures and Java/Rust cross-decodes), formatting and
  scoped `git diff --check` passed.
- The named RPC mapping is closed after validation. Broad P18-V01 and
  P18-V02/P18-V03/P18-V04 gates remain open.
