# ADR-0033: Map Java NettyEventType enum API

Status: implemented; full Phase 6 reconciliation remains open.

## Source contract

RocketMQ 5.5.1 exposes `org.apache.rocketmq.remoting.netty.NettyEventType`
with `CONNECT`, `CLOSE`, `IDLE`, `EXCEPTION`, and `ACTIVE` in that order. The
enum has generated `values()` and `valueOf(String)` plus inherited name,
ordinal, text, and comparison behavior. `NettyEvent.getType()` returns one of
these values.

## Rust mapping

The public `client::NettyEventType` enum matches the Java constants, ordinals,
lookup errors, display names, and comparisons. The connection event queue now
uses this public type directly, so its declaration order and event identity
share one definition. Listener methods continue to receive the remote address
through their event-specific callbacks.

## Evidence and limits

`nettyEventTypeMethodCases` in `fixtures/runtime/client-config.json` invokes all
five constants, defensive `values()` copies, and null/unknown/case-mismatched
name lookups. The native differential checks 12 Java cases. `NettyEvent` also
exposes a Java Netty `Channel`; the native transport does not expose a Tokio
socket handle through its callback API, so this enum mapping does not claim a
direct carrier-object equivalent. Full Phase 6 source/API reconciliation
remains open.
