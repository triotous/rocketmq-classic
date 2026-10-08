# ADR-0038: Match Fastjson SOCKS string escape parsing

Status: Accepted

## Context

The pinned Java 5.5.1 SOCKS constructor parses `socksProxyConfig` with Fastjson2. Its string escape handling accepts complete `\x` and `\u` escapes even when an ASCII digit is not hexadecimal, converting each invalid digit to zero. It rejects escapes that reach the string terminator before the required number of digits, and it rejects backslash line continuations.

## Decision

Keep the raw SOCKS JSON setting unchanged. During native normalization, rewrite complete `\xNN` and `\uNNNN` sequences to valid JSON Unicode escapes. The escape forms follow different Fastjson 2.0.63 paths: `\x` indexes a bounded lookup table, mapping in-range invalid ASCII digits to zero and rejecting code units above U+0066; `\u` uses a packed UTF-16 decoder that consumes four code units and can produce non-ASCII nibble values. Preserve the existing U+FFFD adaptation for isolated UTF-16 surrogates because Rust `String` cannot represent those code units.

This is an input-grammar adapter only. It adds no public configuration and does not claim Java exception-class parity.

## Evidence

The pinned Java `RuntimeConfigFixtures.socksConstruction` directly constructs `MQClientInstance` and reflects the parsed `NettyRemotingClient.proxyMap`. The 263-case fixture includes malformed escape positions, short escapes, escaped quote forms, line continuations, a trailing backslash, one probe for each ASCII escape follower from U+0000 through U+007F, and nine Unicode/control-unit probes. Java maps `\xG0` and `\x0G` to U+0000, `\xG1` to U+0001, and `\u12G4`, `\u1G34`, and `\uG234` to U+1204, U+1034, and U+0234. The ASCII matrix adds 12 accepted identity escapes (`\ `, `\#`, `\&`, `\(`, `\)`, `\,`, `\.`, `\@`, `\[`, `\]`, `\_`, and `\~`), octal `\1` through `\7`, and uppercase `\F` as form feed. New cases confirm `\u12λ4` becomes U+12B4, `\u12١4` becomes U+12A4, `\u12１4` becomes U+1214, raw tab/newline code units participate in `\u` decoding, and supplementary characters consume two UTF-16 units; `\xλ0` and `\x0١` reject. Regression `scalar_socks_fields_match_invoked_java_parsed_proxy_values` first failed on `\u1λ34` (native U+1243 versus Java U+1B34), then passed with the UTF-16-aware correction in `7754497`.

The current `client-config.json` SHA-256 is `b3bfd8a5491de753cc2567618712a8375dae007c31e8a0116e1624b0297fbde4`. The focused SOCKS differential passes for all 263 cases and all 67 remoting library tests pass. Full locked workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verify (manifest22), comparator self-test/manifest validation22, formatter, and diff checks pass.

This closes only the invoked escape cases. Other Fastjson grammar, address/transport variants, and Phase 6 source/API/input reconciliation remain open; P6-V04 remains PENDING_PHASE_19.
