# ADR-0038: Match Fastjson SOCKS string escape parsing

Status: Accepted

## Context

The pinned Java 5.5.1 SOCKS constructor parses `socksProxyConfig` with Fastjson2. Its string escape handling accepts complete `\x` and `\u` escapes even when an ASCII digit is not hexadecimal, converting each invalid digit to zero. It rejects escapes that reach the string terminator before the required number of digits, and it rejects backslash line continuations.

## Decision

Keep the raw SOCKS JSON setting unchanged. During native normalization, rewrite complete `\xNN` and `\uNNNN` sequences to valid JSON Unicode escapes. Each non-hex ASCII digit in a complete escape maps to zero, matching the invoked Java parser. Incomplete sequences remain parse errors. Preserve the existing U+FFFD adaptation for isolated UTF-16 surrogates because Rust `String` cannot represent those code units.

This is an input-grammar adapter only. It adds no public configuration and does not claim Java exception-class parity.

## Evidence

The pinned Java `RuntimeConfigFixtures.socksConstruction` directly constructs `MQClientInstance` and reflects the parsed `NettyRemotingClient.proxyMap`. The 254-case fixture includes malformed escape positions, short escapes, escaped quote forms, line continuations, a trailing backslash, and one probe for each ASCII escape follower from U+0000 through U+007F. Java maps `\xG0` and `\x0G` to U+0000, `\xG1` to U+0001, and `\u12G4`, `\u1G34`, and `\uG234` to U+1204, U+1034, and U+0234. The ASCII matrix adds 12 accepted identity escapes (`\ `, `\#`, `\&`, `\(`, `\)`, `\,`, `\.`, `\@`, `\[`, `\]`, `\_`, and `\~`), octal `\1` through `\7`, and uppercase `\F` as form feed. The native differential was red on the first accepted identity escape and exposed 20 previously rejected cases; the correction in `8ac0ea3` now matches each acceptance result and parsed proxy value.

The current `client-config.json` SHA-256 is `34e3b6600611e97fe5ac5c3009bcd5d95e2c9489456ac9cc91e9b5f5a9ab2f25`. The focused SOCKS differential and runtime construction test pass for all 254 cases; full locked workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verify (manifest22), comparator self-test, formatter, and diff checks are recorded in the Phase 6 log.

This closes only the invoked escape cases. Other Fastjson grammar, address/transport variants, and Phase 6 source/API/input reconciliation remain open; P6-V04 remains PENDING_PHASE_19.
