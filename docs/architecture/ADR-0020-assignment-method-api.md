# ADR-0020: Map MessageQueueAssignment public methods

Status: Accepted

## Context

The pinned Java `MessageQueueAssignment` exposes bean getters/setters, `equals`, and `toString`. Rust had public fields and derived equality but no accessor pair, no Java-shaped text, and no method that represented Java's queue-only equality and its null-queue failure.

## Decision

Add idiomatic getters/setters, `Display` and `java_to_string`, and a typed `java_equals` method. The explicit equality method compares only `message_queue`, returns false for a null other value, returns true for the same object reference, and reports Java's null `messageQueue` dereference for distinct objects. Rust's derived `PartialEq` and `Hash` continue to compare all fields so Rust hash collections retain a lawful Eq/Hash pair.

## Evidence

Two actual Java 5.5.1 method cases cover default and populated accessors/text plus queue-only equality when mode and attachments differ, different queues, self/null/wrong-type inputs, and the null-queue exception. `assignment_methods_match_invoked_java_public_api` compares the Rust accessor values, `Display`, and explicit equality outcomes.

Current combined lookup fixture SHA-256: `8cb7bcefaaeefd5a4b72371ca113c8a334823bef50936c26c7417f11491d2d7a`.

Java `MessageRequestMode` inherits identity-based `Object.hashCode`, so its numeric hash changes across JVM runs and has no stable enum-value equivalent. Rust keeps standard `Hash`; reproducing Java's hash-bucket collision effects is not claimed. This closes the recorded accessor/equality/text cases only; the wider Phase 6 API/input audit remains open.
