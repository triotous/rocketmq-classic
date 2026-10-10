# ADR-0082: Serialize raw route-table remapping per topic

Status: implemented for the invoked raw-table race; broader Phase 6 acceptance remains open.

## Source contract

`MQClientInstance.getTopicRouteTable()` exposes a Java `ConcurrentMap`. The pinned Java 5.5.1 oracle distinguishes compute-style operations from `replaceAll`:

- A blocked `compute` callback reads the current mapping and runs once. A same-key writer waits until compute publishes its result, then receives that result as its previous value and leaves the writer value current.
- `replaceAll` invokes its callback without excluding a same-key writer. If the writer changes the mapping before the conditional replacement, Java compares the observed values and calls the callback again with the new value.
- `clear` waits for a blocked same-key `compute` callback, then removes that computed mapping. The callback still runs once and returns its computed value.

Different keys do not share this per-key ordering requirement.

## Native implementation

`RouteCache` owns a weak registry of mutation gates keyed by exact topic. Compute-style `TopicRouteTable` remapping callbacks (`compute_if_absent`, `compute_if_present`, `compute`, and `merge`), direct and conditional writes, collection-view conditional removals, and normal route-refresh publication use the same gate. These callbacks run without holding the route snapshot lock, so reads can inspect the current mapping while they run. `replace_all` takes a key snapshot but does not hold the key gate during its callback; it gates only its compare-and-replace, uses Java route-value equality, and retries the callback after a conflicting write. It is not an all-map transaction. `put_all` applies independent per-key writes and never holds gates for multiple keys at once.

The route refresh path performs route comparison and conversion before taking a topic gate, then holds the gate across the raw-map observation and coherent snapshot publication. This avoids making an unrelated conversion hold up a direct table mutation while ensuring the publication and a same-topic remap have a single order. `clear` snapshots the current keys and removes them one at a time under their topic gates, so it waits for a same-topic compute without becoming a whole-table transaction. Concurrent insertions after the key snapshot are not covered by that clear pass. Gates are weakly retained and expired entries are pruned when the registry grows.

Reentrant same-topic writes from a remapping callback return `RuntimeError::RecursiveRouteTableMutation` instead of waiting on their own gate.

## Evidence

The `runtimeTopicRouteTable` Java fixture records four compute-style race outcomes, four `replaceAll` race outcomes, and four clear/compute race outcomes. Its complete `runtimeTopicRouteTable.primitives` map has 77 outcomes; whole-file `fixtures/runtime/route.json` SHA-256 is `f2c35bdda057ec82d400af935b9c16d91a62ec987e9e7e8a8a7b545fb84ea341`. The native `retained_raw_topic_table_matches_invoked_java_primitives_and_cached_queries` test compares all primitive/view outcomes, confirms `replaceAll` retries after a writer runs during its callback, checks clear waiting/removal after compute, and directly checks that a recursive compute-style same-topic write is rejected without deadlock.

Focused route-table coverage, `cargo test --workspace --all-targets --locked`, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify, formatting, and diff checks pass. The broader Phase 6 source/API/input/platform reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.
