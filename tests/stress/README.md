# Offline deterministic race and leak smoke

Run the bounded repeat suite with:

```bash
bash tests/stress/run-offline-smoke.sh
```

The script runs 17 existing barrier-, paused-time-, and scripted-peer cases
three times. It fails if a named test filter matches zero tests or any test
fails. It contacts no Broker and requires no Docker daemon.

The selected assertions cover these resource and ownership boundaries:

| Area | Test evidence |
| --- | --- |
| Remoting pending entries and opaque generations | Pending/timeout/cancel/shutdown terminal races retain one owner and leave no pending entries. |
| Remoting tasks and sockets | Start/shutdown and connection-publication races close published or unpublished sockets and join owned tasks. |
| Remoting shutdown admission | Public-executor submissions remain accepted through the channel-event worker join wait and are rejected after the wait boundary. |
| Reconnect and callbacks | GO_AWAY retry obtains a fresh connection; pending callbacks drain on shutdown. |
| Shared runtime construction | Barrier-synchronized first lookups share one runtime and transport socket; worker counters return to zero after shutdown. |
| Request/reply registry | Reply/timeout/send-failure/cancellation races claim one terminal path; admission slots become available after cleanup. |
| Consumer scheduling and offsets | Membership join/leave converges; reset fences slow callbacks and late pulls; stale lock generations cannot restore ownership. |
| Orderly shutdown | Slow listener/rebalance work is bounded and dropped work cannot advance committed offsets. |

The test assertions, not process memory, are the evidence for pending entries,
connections, owned tasks, admission permits, queue generations and offsets.
Server-backed sustained load, restart and Broker failover remain Phase 19
obligations.
