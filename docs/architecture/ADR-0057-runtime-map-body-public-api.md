# ADR-0057: Map consumer status and reset offset body methods

- Status: Accepted
- Date: 2026-10-09

## Context

Pinned RocketMQ 5.5.1 `GetConsumerStatusBody` exposes getters and setters for `messageQueueTable` and `consumerTable`. `ResetOffsetBody` exposes getters and setters for `offsetTable`. Existing direct inputs exercised their Fastjson behavior but did not invoke these public methods on constructed objects.

## Decision

Keep the existing Rust snake_case accessors and add direct Java invocation oracles plus Rust differentials. Both Java constructors initialize their maps empty. The cases cover getter values after assignment, explicit-null setter results, nullable Long entries, empty nested maps, null nested maps, and Java object identity for the assigned outer and nested maps.

Rust stores the data as owned entry vectors and a `BTreeMap`; getters lend immutable views. Values, ordering where defined by the native entry representation, null states, and defaults match the fixture, but Java mutable Map alias identity does not.

## Consequences

The public methods and constructor defaults now have direct coverage in addition to the DTO input matrix. The mutable Map alias boundary remains explicit; overall Phase 6 source/API/input reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.

## Evidence

- Pinned `javap -public` confirms the four `GetConsumerStatusBody` methods and two `ResetOffsetBody` methods.
- Direct Java cases: `consumerStatusBodyApiCase` and `resetOffsetBodyApiCase` in `fixtures/runtime/lookup.json`.
- Rust differential: `consumer_status_and_reset_offset_public_methods_match_invoked_java`.
- Oracle/test commit: `471b7160`; no production change was required.
- Aggregate lookup fixture SHA-256: `7b2ce204660f804147471cb2d6d220e669e33991285b64cc68faf699c15a8aff`.
- Validation: protocol all-target tests (107 unit tests plus integration tests); pinned offline Maven verify (22 scenarios, fixture drift and Java cross-decodes); formatting and diff checks.
- Oracle uses pinned RocketMQ 5.5.1 artifacts; the requested alternate source checkout was unavailable.
