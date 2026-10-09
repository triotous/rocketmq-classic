# ADR-0045: Match Java automatic client-IP ordering on macOS

Status: Accepted

## Context

RocketMQ 5.5.1 initializes `ClientConfig.clientIP` with `NetworkUtil.getLocalAddress()`. The helper filters loopback addresses, prefers IPv4, selects the first address classified as external, and otherwise selects the final IPv4 candidate. The native automatic constructor uses the same selection policy over OS interface addresses.

On the pinned macOS/JDK host, Java and Rust received the same private IPv4 candidates in opposite order. Java selected `10.10.10.111`; Rust `ClientConfig::new_auto()` selected `192.168.139.3`.

## Decision

On macOS, reverse only the IPv4 candidate sequence returned by `getifaddrs` before applying the existing selection policy. Keep IPv6 order and the generic selection function unchanged. Other operating systems retain their current interface adapters.

## Evidence

The Java fixture directly records `NetworkUtil.getLocalAddress()`, ordered eligible candidates, and the no-argument `ClientConfig.getClientIP()`. The native `automatic_client_ip_matches_invoked_java_network_util_and_constructor` regression failed with `192.168.139.3` before implementation `5791e28`; oracle/test commit `482e02d`. A macOS-only unit test verifies that the adapter reverses IPv4 candidates while leaving IPv6 entries in place. Fixture `client-config.json` SHA-256: `138164047171a6e1bb649d3b70239a023245341852cd4036148d80d3c722b5ea`.

Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and deterministic fixture checks), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes the observed pinned macOS IPv4 case. Non-macOS enumeration and IPv6-only fallback remain under reconciliation; this is not Phase 6 acceptance, and P6-V04 remains `PENDING_PHASE_19`.
