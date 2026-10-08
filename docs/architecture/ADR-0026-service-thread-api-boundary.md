# ADR-0026: Map the inherited Java ServiceThread API boundary

Status: audited in `afb1811`; whole-phase source and API reconciliation remains
open.

## Source contract

In Java 5.5.1, both `RebalanceService` and `PullMessageService` inherit the
public `ServiceThread` methods `start`, `shutdown()`, `shutdown(boolean)`,
`getJoinTime`, `makeStop`, `wakeup`, `isStopped`, `isDaemon`, and
`setDaemon(boolean)`. `getServiceName()` is overridden on both classes and is
mapped in [ADR-0025](ADR-0025-runtime-service-name-methods.md). The pinned
bytecode returns a 90,000ms join time; new threads default to non-daemon and
not stopped. `makeStop()` does nothing before the worker is started.
`PullMessageService.shutdown()` also shuts down its separate scheduled
executor.

The generated `serviceThreadCases` fixture directly invokes state/configuration
methods on both unstarted service instances. It checks the default
join/daemon/stopped values, `setDaemon(true)` followed by `isDaemon()`, the
observed state after `wakeup()` and pre-start `makeStop()`, and the unchanged
stopped flag after pre-start `shutdown()`. It also records that the
PullMessageService scheduled executor is closed.

The `serviceLifecycleCases` fixture uses subclasses that override `run()` with
a latch-controlled body. It directly invokes `start()` twice, verifies only
one run, calls `makeStop()` while the thread is blocked and verifies that the
stopped flag changes without joining, then releases the run body and invokes
`shutdown(true)`. The fixture observes the stopped flag, thread liveness, and
PullMessageService executor closure. These probes exercise the inherited
thread mechanics without executing either RocketMQ worker loop or contacting
a Broker.

## Rust mapping and limits

Rust does not expose standalone `RebalanceService`, `PullMessageService`, or
JDK-thread objects. `ClientRuntime::start`, `shutdown`, and `state` manage the
aggregate runtime; `rebalance_immediately` signals the owned rebalance task,
and immediate pull/pop calls enqueue work. `OwnedPullWorker::finish_gracefully`
preserves the one-second scheduled-work grace before cancellation and join.
Existing lifecycle, rebalance-wakeup, and pull-dispatch tests cover those
owned-task paths.

The Java daemon flag, `Thread.join` timeout getter, interrupt boolean, and
started-but-not-joined `makeStop()` operation have no direct Rust counterpart.
Rust tasks are explicitly owned and joined through the runtime control task;
`RuntimeState` is richer than the per-thread `isStopped()` boolean. These are
documented worker-model differences, not claims of JDK thread parity.
`waitForRunning(long)` and `onWaitEnd()` are protected methods, so they remain
implementation details rather than public API entries.

The current `fixtures/runtime/client-config.json` SHA-256 is
`631b1b9537c13cf1f5943b0e958b214a6bf4099b4edc6cd8643a02f4c207d4c0`.
`inherited_service_thread_oracle_records_java_thread_controls` and
`inherited_service_thread_oracle_records_java_start_stop_contract` pin the
invoked values. Full workspace tests, strict Clippy, pinned offline Maven
verification, comparator self-test, manifest validation, formatter, and diff
checks pass. Whole Phase 6 reconciliation and P6-V04 Phase 19 evidence remain
open.
