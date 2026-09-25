# Known differences and evidence gaps

## Phase 2 Remoting codec

- Java's 5.5.1 private RocketMQ serializer narrows request code and version to signed 16-bit values. The Rust private-header encoder rejects values outside that range instead of silently wrapping. JSON encoding retains full i32 request codes and is the supported path for such values. Java-generated fixture `private-large-code` documents the Java narrowing behavior; it is decoded as the narrowed signed value.
- Java's private serializer writes both absent and empty remark as a zero-length string; Rust preserves the distinction in its command model but the wire decoder returns `None` for either representation. An absent versus empty body similarly collapses to no body when the frame body length is zero.
- Java's `LanguageCode.valueOf(byte)` returns null for unknown private-header values; Rust preserves the raw byte as `LanguageCode::Unknown` rather than losing it. JSON headers use enum names and reject names Java cannot map.

- No Rust client implementation exists at the start of Phase 0; all Rust mappings are pending their owning phase.
- Java harness fixtures are offline protocol-command evidence, not live NameServer/Broker outcomes.
- Live Broker behavior, TLS/auth profiles and cross-language cases remain `PENDING_PHASE_19` until executed against a verified server artifact.
- Phase 0 harness emits selected request codes and is not yet the complete Java public API/configuration inventory; completion is required by Phases 18–19.
