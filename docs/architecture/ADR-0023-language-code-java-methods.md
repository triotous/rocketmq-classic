# ADR-0023: Map Java LanguageCode methods and nullable wire decoding

Status: implemented in `d5cbd5b`; full Phase 6 reconciliation remains open.

## Source contract

The pinned RocketMQ 5.5.1 `LanguageCode` enum exposes `getCode()` for its byte
code, `getCode(String)` for exact enum-name lookup, and `valueOf(byte)` as a
custom byte-code lookup. Java returns null for unknown names and byte values.
Actual Java cases cover all 14 constants, boundary and negative byte inputs,
null and case-mismatched names, and unknown names. `RemotingSerializable.decode`
also returns a command with null language for an unknown JSON language string.

## Rust mapping

`LanguageCode` contains exactly the 14 Java constants. `code()` and `name()` map
the instance methods `getCode()` and `Enum.name()`. `from_java_name()` maps
`getCode(String)`, and `from_java_byte(i8) -> Option<LanguageCode>` maps the
custom `valueOf(byte)` lookup, including its null result for unknown values.
There is no public raw-code variant or separate byte conversion API.

JSON and private-header decoding now use the nullable Java lookup behavior, so
unknown wire names and byte codes decode to `None`. Since every representable
`LanguageCode` has a Java enum name and code, JSON and private-header encoders
only receive Java-defined values.

## Evidence and limits

`languageCodeMethodCases` in `fixtures/runtime/client-config.json` is generated
by invoking the pinned Java methods directly. The fixture also records the
actual Java unknown-name decode result. Its SHA-256 is
`33441a65926214a006702b46369bfab8f44b35fc1951c1ab17a45166f0408fdf`.

Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned
offline Maven verification, formatter, and diff checks pass. This closes
the recorded enum lookup and unknown-wire decode cases, not the remaining Phase
6 source/API/input audit or P6-V04 Phase 19 evidence.
