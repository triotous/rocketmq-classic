# ADR-0004: Explicit per-client runtime ownership

Status: accepted for construction/sharing. Lifecycle, group leases and task acceptance are still in progress in Phase 6.

The Phase 6 plan proposed ADR-0002, but that number already belongs to the accepted NameServer response-peer decision. This decision uses the next free number; it does not change the planned ownership contract.

Pinned Java 5.5.1 `MQClientManager.getOrCreateMQClientInstance` owns a `computeIfAbsent` key before constructing resources. `removeClientFactory(id, instance)` is identity-aware. `MQClientInstance` construction does not start its remoting transport; start failure later removes only that instance. Java uses a singleton; the Rust plan explicitly requires an injectable/shareable manager instead.

`ClientRuntimeManager` clones share a table. The table contains per-ID entries; each entry owns an async initialization mutex and a write-once runtime cell. The short table lock never spans construction or await. Only the winning key owner constructs a transport; no losing construction is created and discarded. Different IDs initialize concurrently. A failed constructor conditionally removes its entry. Waiters on that old entry must recheck the authoritative mapping before construction. Cancelled initialization releases its per-key guard and drops unpublished resources, allowing a later retry.

Runtime removal reads the immutable identity cell, not a best-effort `try_lock` on initialization: concurrent lookup cannot spuriously suppress required cleanup. It compares the mapped runtime pointer before removing the entry. An old runtime/entry cannot remove a replacement.

Sharing compares settings that govern the common transport, schedules and heartbeat behavior, TLS property snapshot and RPC hook identity. Conflicts return a typed error without credentials. Consumer decoder policy is not a transport key; clients retain their own policy. No public mock/failure-injection interface or extra retry/config setting is introduced. `ClientConfig::clone_client_config` follows Java's copy omissions; Rust `Clone` remains an exact value copy.

Executable evidence: `tests/runtime.rs` concurrent identity/configuration/retry/removal cases and manager unit tests for independent-key rendezvous, stale entry removal, cleanup during a lookup lock and cancelled unpublished initialization. These prove construction ownership, not yet the P6-V01 running task/socket gate. Resource start/shutdown, registration identity leases, joined periodic work and startup-failure rollback must complete before Phase 6 acceptance.

## Owned lifecycle/control extension

The runtime now owns one command worker for registration/start/unregister/shutdown. A weak reference is upgraded only while processing a command; the idle worker does not keep a runtime/manager cycle alive. Registry producer/consumer handles are weak, while callers own actual implementations. Explicit group identities, not runtime Arc counts, guard normal stop. Admin groups participate in the stop guard but not heartbeat preparation.

Java's synchronized unregister includes Broker RPCs; a local pointer check alone cannot prevent a late unregister from invalidating replacement Broker state. Rust registration APIs therefore become async, queued behind the same owned control worker. No synchronous guard crosses await. Caller cancellation drops its response receiver, not the in-flight command. A private pending-delivery guard undoes registration when a caller never accepts the returned token. Accepted lease cloning/dropping does not implicitly change registration; owning facades must unregister explicitly.

The control JoinHandle stays stored during polling. An async join gate serializes wakers; a cancelled joiner releases the gate without detaching the handle, and another caller can complete the same join. Startup failure closes current resources, clears unusable group registrations, retains Failed on old handles and removes only the manager's current identity. Normal stop unregisters the actual Java internal producer group before closing transport. Registration/transport fault checkpoints, parallel start/stop, pending-delivery cancellation, cancelled terminal join and scripted external/internal unregister ordering are executable checks. Periodic/inbound resources and their checkpoints remain pending; this extension does not close the phase gate.
