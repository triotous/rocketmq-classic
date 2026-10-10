# ADR-0082: Serialize raw route-table remapping per topic

Status: implemented for the invoked raw-table race; broader Phase 6 acceptance remains open.

## Source contract

`MQClientInstance.getTopicRouteTable()` exposes a Java `ConcurrentMap`. The pinned Java 5.5.1 oracle runs `compute` with a blocked callback and a concurrent same-key `put`. The callback reads the current mapping and runs once. The writer waits until the callback publishes its result, receives that result as its previous value, and then leaves the writer value current. Different keys do not share this per-key ordering requirement.

## Native implementation

`RouteCache` owns a weak registry of mutation gates keyed by exact topic. `TopicRouteTable` remapping callbacks, direct and conditional writes, collection-view conditional removals, and normal route-refresh publication use the same gate. The callback runs without holding the route snapshot lock, so reads can inspect the current mapping while it runs. `replace_all` takes a key snapshot and gates each key separately; it is not an all-map transaction. `put_all` applies independent per-key writes and never holds gates for multiple keys at once.

The route refresh path performs route comparison and conversion before taking a topic gate, then holds the gate across the raw-map observation and coherent snapshot publication. This avoids making an unrelated conversion hold up a direct table mutation while ensuring the publication and a same-topic remap have a single order. Gates are weakly retained and expired entries are pruned when the registry grows.

Reentrant same-topic writes from a remapping callback return `RuntimeError::RecursiveRouteTableMutation` instead of waiting on their own gate. `clear` remains a map-wide update and is not represented as a transaction across every topic.

## Evidence

The `runtimeTopicRouteTable` Java fixture records all four same-key race outcomes. Its complete `runtimeTopicRouteTable.primitives` map now has 69 outcomes; whole-file `fixtures/runtime/route.json` SHA-256 is `ab38d35b9e87150471b6e001ee8291578c8daac75b523ec13a1c798c90bdeb12`. The native `retained_raw_topic_table_matches_invoked_java_primitives_and_cached_queries` test compares all primitive/view outcomes and directly checks that a recursive same-topic write is rejected without deadlock.

Focused route-table coverage, `cargo test --workspace --all-targets --locked`, strict workspace/all-target/all-feature Clippy, pinned offline Maven verify, formatting, and diff checks pass. The broader Phase 6 source/API/input/platform reconciliation remains open, and P6-V04 remains `PENDING_PHASE_19`.
