# ADR-0042: Match Java check-client body Fastjson inputs

Status: Accepted

## Context

`CheckClientRequestBody` is a public protocol DTO with optional `clientId`, `group`, `subscriptionData`, and `namespace` fields. RocketMQ 5.5.1 decodes it with Fastjson 2.0.63, which keeps the last duplicate property and coerces primitive/object values assigned to String fields. The derived Rust decoder rejected duplicate properties and did not apply those String coercions.

## Decision

Keep the existing nullable public field types and serialization shape. Deserialize the DTO through the shared raw-object last-wins helper; use the Fastjson-compatible nullable String converter for the three String fields, and decode nested `SubscriptionData` through its existing Java-compatible decoder. Missing and explicit-null fields remain absent on re-encode. Scalar `subscriptionData` remains rejected, and unknown properties remain ignored.

This records only the invoked DTO input boundary. It does not claim Java exception-class equivalence or complete coverage of other protocol body inputs.

## Evidence

Eight direct pinned Java 5.5.1 inputs cover last-wins decoding for all four fields, nested subscription replacement, scalar-to-string conversion, explicit null, unsupported scalar subscription data, and ignored unknown properties. Native differential test `check_client_body_inputs_match_invoked_java_fastjson` failed on duplicate `clientId` before the decoder change, then matched all eight accepted/rejected outcomes. Oracle/test commit `3cba9ba`; implementation commit `516f9e0`; client-check fixture SHA-256 `ef84e70eb27c60a9ceab9cfe81f2922295a0ff71bead0159eb606085b7e61277`.

Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify and fixture drift checks (22 scenarios), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. Whole Phase 6 source/API/input reconciliation remains open; P6-V04 stays `PENDING_PHASE_19`.
