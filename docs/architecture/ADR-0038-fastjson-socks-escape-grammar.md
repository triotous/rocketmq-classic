# ADR-0038: Match Fastjson SOCKS string escape parsing

Status: Accepted

## Context

The pinned Java 5.5.1 SOCKS constructor parses `socksProxyConfig` with Fastjson2. Its string escape handling accepts complete `\x` and `\u` escapes even when an ASCII digit is not hexadecimal, converting each invalid digit to zero. It rejects escapes that reach the string terminator before the required number of digits, and it rejects backslash line continuations.

## Decision

Keep the raw SOCKS JSON setting unchanged. During native normalization, rewrite complete `\xNN` and `\uNNNN` sequences to valid JSON Unicode escapes. Each non-hex ASCII digit in a complete escape maps to zero, matching the invoked Java parser. Incomplete sequences remain parse errors. Preserve the existing U+FFFD adaptation for isolated UTF-16 surrogates because Rust `String` cannot represent those code units.

This is an input-grammar adapter only. It adds no public configuration and does not claim Java exception-class parity.

## Evidence

The pinned Java `RuntimeConfigFixtures.socksConstruction` directly constructs `MQClientInstance` and reflects the parsed `NettyRemotingClient.proxyMap`. The 126-case fixture includes malformed escape positions, short escapes, escaped quote forms, line continuations, and a trailing backslash. Java maps `\xG0` and `\x0G` to U+0000, `\xG1` to U+0001, and `\u12G4`, `\u1G34`, and `\uG234` to U+1204, U+1034, and U+0234. Native parsing matches the observed values and accept/reject outcomes.

The current `client-config.json` SHA-256 is `2457d9c7c181805b46c9ae0ca848e1357f3a652f235be56e9f3d82ef834ccde0`. The focused SOCKS differential, full locked workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verify (manifest22), comparator self-test, formatter, and diff checks pass.

This closes only the invoked escape cases. Other Fastjson grammar, address/transport variants, and Phase 6 source/API/input reconciliation remain open; P6-V04 remains PENDING_PHASE_19.
