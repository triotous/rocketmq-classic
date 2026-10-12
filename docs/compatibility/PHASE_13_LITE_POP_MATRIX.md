# Phase 13 Lite Pull and POP compatibility matrix

Java reference: `rocketmq-client:5.5.1`, source tag commit `efa1be59d6fc747ad220f036c86030f05f0419f9`. The checkout at `/Users/bruce.tsai/Projects/3rd/rocketmq` is on 5.3.3; phase source inspection must use `git show <pinned-commit>:<path>`. Maven fixtures execute the pinned 5.5.1 artifact.

This matrix tracks Phase 13 implementation evidence. A lower-level request/header model does not establish completion of the high-level consumer flow.

| Java source/API | Behavior owned by Phase 13 | Rust mapping | Local evidence | Status |
| --- | --- | --- | --- | --- |
| `LitePullConsumer` / `DefaultLitePullConsumer` lifecycle | Constructors, start/shutdown/running state, namespace, group and lifecycle validation | `LitePullConsumer` facade and config | `fixtures/consumer/lite-pop.json`; P13-U01 | In progress |
| Lite subscription and heartbeat | Tag/SQL selector conversion, `buildSubscriptionsForHeartbeat`, subscription/assignment exclusivity, subscribe/unsubscribe and metadata change callback | Lite subscription registry and existing heartbeat/rebalance runtime | P13-U01, P13-V01 | In progress |
| Lite assigned queue and polling | Assignment replacement, bounded asynchronous prefetch, poll timeout/batch, pause/resume, late result invalidation | Generation-scoped queue state and polling buffer | P13-U01, P13-V01 | In progress |
| Lite offsets | Commit set/map/single/all and deprecated `commitSync` aliases, auto-commit deadline, committed read, timestamp/min/max seek helpers | Lite offset state over existing offset store and admin RPCs | P13-U01, P13-V01 | In progress |
| Lite public settings | Defaults and validation for pull threads, batch, flow thresholds, poll timeout, long-poll timeout, consume origin/timestamp, offset store, selector strategy, unit mode, trace controls, broker selection | `LitePullConsumerConfig` and existing `ClientConfig` | P13-U01, P13-V03 | In progress |
| Push POP assignment | `QUERY_ASSIGNMENT`, per-queue PULL/POP assignment, mode transitions, POP retry-topic subscription and assignment callback | Existing typed assignment RPC plus POP queue runtime | P13-U01, P13-V02, P13-V03 | In progress |
| Push POP pull/consume | POP scheduling, invisible-time bounds/default, batch size, queue flow control, subscription filtering, retry cutoff and listener dispatch | POP request worker and consumer service | P13-U01, P13-V02 | In progress |
| POP receipt operations | Preserve `PROPERTY_POP_CK`, parse checkpoint/pop/invisible/revive/retry/Broker/queue/offset data, ACK, source-used batch ACK and change-invisibility receipt replacement | Opaque `PopReceipt` with validated accessors and typed API adapter | `fixtures/consumer/lite-pop.json`, P13-U01, P13-V02 | In progress |

## Pinned Java public surface and defaults

The fixture records all `LitePullConsumer` interface signatures and all declared public `DefaultLitePullConsumer` method signatures. It invokes selected public getters/setters and captures config validation. Defaults observed from the 5.5.1 artifact include clustering mode, auto-commit enabled, 20 pull threads, a 5-second auto-commit interval, batch 10, all-queue threshold 10000, queue count threshold 1000, queue body-size threshold 100 MiB, max consume span 2000, 5-second default poll timeout, 30-second metadata polling, last-offset consumption and a timestamp 30 minutes in the past. Setting an auto-commit interval below 1000 ms is ignored. The heartbeat builder replaces its retained subscription set with the selector map's built subscriptions.

The Push POP defaults are queue wait-ACK threshold 96, invisibility 60000 ms, and POP batch count 32. The Java `ExtraInfoUtil` fixture records the unmodified receipt string and each decoded routing/offset field. `PopResult` exposes `FOUND`, `NO_NEW_MSG`, `POLLING_FULL`, and `POLLING_NOT_FOUND` plus pop time, invisibility, remaining count and message list.

Typed request/response bodies and low-level POP/ACK RPCs are already present in protocol/core crates. This matrix remains `In progress` until the Lite scheduling path, broker-assignment POP path, receipt operations, method/config audit, and local cases all pass.
