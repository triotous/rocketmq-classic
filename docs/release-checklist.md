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

## Phase 18 verification disposition

- [ ] **P18-V01** — Broad Java/Rust source, overload input/error/flow, inherited
  API, and reverse-export reconciliation remains open; this is local parity
  work, not final manual validation. The producer send mutation branch is now
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
  lock-protected mutable set and `putUserProperty` reads its current contents.
  Invoked Java and Rust fixtures verify add/remove mutations affect reserved
  property validation. This closes the set-mutation behavior only; broad
  Common reverse-surface reconciliation remains open P18-V01 work.
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
  encoder methods only; the remaining decoder overloads and broad P18-V01 audit
  remain open. Current message fixture SHA-256:
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
  `bb82ac0033f97b109251a178330b278f86257ed2ad34a8f198fe597002dd7902`.
- [ ] **P18-V02** — Re-run the complete server-free suite on the final revision.
  The latest complete run is recorded below for `133d4f54`; later commits are
  not covered by that full run.
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
