# ADR-0010: Live raw topic routes independent of derived publication

Status: implemented for tested Phase 6 primitives; final acceptance remains open.

## Source contract

Pinned Java MQClientInstance.getTopicRouteTable returns its live ConcurrentMap. Actual oracle19 primitive outcomes include retained-view/getter freshness, equal-copy conditional replacement/removal, empty keys and clear/repopulation. Direct map writes do not fetch, validate, convert, invoke owners or rebuild global Broker/producer/consumer metadata.

Observed actual runtime behavior: replacement changes raw topic-based address but not global publish address; clear removes raw getter/topic selection but Broker data persists until offline cleanup. Cached query returns direct raw data without fetching, including a null Broker address map. Query after clear refetches once and retained table sees publication. Refresh of unchanged invalid raw data returnsfalse before conversion. The reviewed oracle fixes an earlier stale counter baseline after metadata refresh; it did not demonstrate an actual cached-query fetch.

## Native implementation

`dae27fb` exposes ClientRuntime.topic_route_table and cloneable TopicRouteTable. Reads and atomic synchronous put/put_if_absent/replace/replace_if_equal/remove/remove_if_equal/clear operate on the shared raw map. snapshot is explicitly detached. Conditional comparisons use source route content equality (Broker name/addresses only), not owner identity or broad lower DTO Eq.

RouteSnapshot.raw_routes is separate from topics, the last converted publication. Normal refresh publishes raw/converted/Broker state atomically. Explicit raw writes update only raw_routes in a new coherent immutable snapshot; old snapshots remain unchanged. Raw getters, topic selection, refresh change detection and offline cleanup consume raw_routes. This represents intentional source raw/derived divergence, not partially completed conversion. Unchanged raw comparison precedes conversion validation; owner-forced publication still validates/rebuilds.

Handles own cache data and plain DTO Arcs, never runtime/control/transport. No worker, RPC, setting or dependency is added. Primitive writes contain no await or owner callbacks. Retained tables work after runtime/transport teardown. Plain immutable Rust DTO values are owned strongly; in-place JVM object alias mutation is not claimed emulated. Additional map default operations and malformed raw cleanup/DTO inputs still require source/API reconciliation, not a typed-bound exclusion.

## Evidence

Subsequent cleanup evidence `75c2729` closes the specific null raw Broker map branch: actual source helper has a null guard, and both real runtime cases succeed, preserve raw object/null map and remove no-longer-live historical addresses when present. Native private golden agrees without production change. Earlier malformed-cleanup uncertainty was an audit item, not a confirmed null-map scan bug. Null lists/other DTO inputs still require their own reconciliation. Current route hash `a1e55afbdda8db505efe05f13eb20687a21aa88ecd108bcfc1df6676a20ef558`; reviewed core74pass/1ignored/runtime7/topicTable3/full workspace/exact Clippy and Maven exit0 (`/tmp/p6-raw-offline-cleanup-*.log` reviewed cargo/Java verification variants), fmt/diff/comparator/manifest22 pass.

Three topic_route_table integrations cover19 golden primitives, cached/invalid queries, metadata equality, 64 OS-thread atomic competitors, owned DTO/detached snapshot lifetime, runtime release, two actual NameServer refresh frames and EOF with no third cached-query frame. The wire test starts only lower transport, explicitly shuts it down, and never starts runtime periodic work or contacts fixture Broker names. Private golden verifies raw/derived separation, clear/offline cleanup, republish after raw absence, unchanged invalid no-op and historical snapshot identity. Existing raw lookup oracle setup now uses raw_routes, preserving all original candidate/error assertions.

Observed exit0: focused core73pass/1lower-review-ignored/topicTable3/runtime7/lookup5/check3, full workspace and exact all-feature Clippy (`/tmp/p6-raw-topic-table-focused-reviewed-final.log`, `-workspace-final.log`, `-clippy-reviewed-final.log`), reviewed Maven drift (`/tmp/p6-raw-topic-table-java-reviewed-verify.log`). fmt/diff/comparator/manifest22 pass. Route SHA-256 `dbea24db04ab83de918333ec35e4356ee6e610523051eeb77f7d4d477c10e0e3` (`fe71e82`). Both lower nullable proposals/ignored Broker failure remain unclosed. No live Broker/Docker or Phase7 handoff.
