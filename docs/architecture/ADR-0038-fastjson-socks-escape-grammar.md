# ADR-0038: Match Fastjson SOCKS string escape parsing

Status: Accepted

## Context

The pinned Java 5.5.1 SOCKS constructor parses `socksProxyConfig` with Fastjson2. Its string escape handling accepts complete `\x` and `\u` escapes even when an ASCII digit is not hexadecimal, converting each invalid digit to zero. It rejects escapes that reach the string terminator before the required number of digits, and it rejects backslash line continuations. For a non-ASCII character immediately after a backslash, Fastjson's `char1` lookup masks the first UTF-16 code unit with `0x7f` before indexing its escape table.

## Decision

Keep the raw SOCKS JSON setting unchanged. During native normalization, rewrite complete `\xNN` and `\uNNNN` sequences to valid JSON Unicode escapes. The escape forms follow different Fastjson 2.0.63 paths: `\x` indexes a bounded lookup table, mapping in-range invalid ASCII digits to zero and rejecting code units above U+0066; `\u` uses a packed UTF-16 decoder that consumes four code units and can produce non-ASCII nibble values. For other non-ASCII escape followers, map the first UTF-16 unit through the same low-seven-bit `char1` table. A supplementary scalar therefore consumes the escaped high surrogate as the lookup input and leaves its low surrogate as raw string content. Preserve the U+FFFD adaptation for isolated UTF-16 surrogate code units because Rust `String` cannot represent them.

This is an input-grammar adapter only. It adds no public configuration and does not claim Java exception-class parity.

## Evidence

The pinned Java `RuntimeConfigFixtures.socksConstruction` directly constructs `MQClientInstance` and reflects the parsed `NettyRemotingClient.proxyMap`. The 392-case fixture includes malformed escape positions, short escapes, escaped quote forms, line continuations, a trailing backslash, one probe for each UTF-16 code unit U+0000 through U+00FF after a backslash, and ten additional Unicode/control-unit probes. Java maps `\xG0` and `\x0G` to U+0000, `\xG1` to U+0001, and `\u12G4`, `\u1G34`, and `\uG234` to U+1204, U+1034, and U+0234. The ASCII matrix adds 12 accepted identity escapes (`\ `, `\#`, `\&`, `\(`, `\)`, `\,`, `\.`, `\@`, `\[`, `\]`, `\_`, and `\~`), octal `\1` through `\7`, and uppercase `\F` as form feed. Across U+0080 through U+00FF, 31 followers are accepted and map through the low-seven-bit table (for example U+00A0 becomes space and U+00B0 becomes U+0000). The `\u` probes cover packed nibble conversion, UTF-16-unit counting, supplementary input, and raw tab/newline code units; `\xλ0` and `\x0١` reject. A direct supplementary follower maps its high surrogate to U+0005 and leaves a low surrogate; native parsing reproduces the Java UTF-16 sequence and maps the unpaired code unit to U+FFFD. Oracle/test commit `5f6e48c`, supplementary comparison test `823cc02`, and implementation `e59475f` extend the prior UTF-16 correction `7754497`.

The current `client-config.json` SHA-256 is `309eec166aa25207b91c3bac843fb5e2e354cbd477cede6bb215a90b881df700`. The focused SOCKS differential passes for all 392 cases, the core runtime-construction test passes, and all 67 remoting library tests pass. Full locked workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (manifest22), comparator self-test/manifest validation22, formatter, and diff checks pass.

This closes only the invoked escape cases. Other Fastjson grammar, address/transport variants, and Phase 6 source/API/input reconciliation remain open; P6-V04 remains PENDING_PHASE_19.
