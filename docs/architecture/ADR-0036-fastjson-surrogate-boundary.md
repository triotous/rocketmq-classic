# ADR-0036: Record the Fastjson lone-surrogate boundary

Status: Accepted

## Context

Pinned Java 5.5.1 accepts SOCKS proxy JSON whose `addr` contains either a lone high surrogate (`\uD800`) or a lone low surrogate (`\uDC00`). Its parsed Java `String` retains the corresponding UTF-16 code unit. Rust `String` stores valid Unicode scalar values and cannot represent either isolated surrogate.

## Decision

Keep the raw `ClientConfig.socks_proxy_config` text unchanged. Before JSON5 decoding, native input normalization converts an isolated surrogate escape in a string literal to U+FFFD. Valid high/low surrogate pairs pass through unchanged and decode to the same Unicode scalar as Java. Native construction therefore preserves Java's successful parse outcome for isolated surrogates while documenting the parsed address value as U+FFFD rather than claiming exact Java `String` parity.

## Evidence

The pinned Java `RuntimeConfigFixtures.socksConstruction` directly constructs `MQClientInstance`, inspects `NettyRemotingClient.proxyMap`, and records the parsed UTF-16 code units for the two isolated-surrogate cases. Rust's `scalar_socks_fields_match_invoked_java_parsed_proxy_values` checks both Java inputs, native construction success, and the U+FFFD representation. Existing escaped supplementary-character cases continue to verify that valid surrogate pairs decode identically.

The updated `client-config.json` SHA-256 is `daf340c6136e5081901763a3af9ab93839584a05e5b7873e9752ebd5549ddb29`; the constructor corpus now has 111 inputs. Focused and full workspace tests, strict all-target/all-feature Clippy, pinned offline Maven verification, comparator/manifest checks, formatting, and diff checks pass.

This records one UTF-16 representation limit, not full Fastjson grammar or Phase 6 acceptance. Broader Fastjson and source/API/input reconciliation remain open; P6-V04 remains PENDING_PHASE_19.
