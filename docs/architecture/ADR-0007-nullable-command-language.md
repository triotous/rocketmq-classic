# ADR-0007: Nullable command language

Status: proposed; owner review required before changing the completed lower-layer public contract.

## Evidence

Pinned client 5.5.1 ClientConfig.setLanguage accepts null; clone/reset retain it,
and MQClientInstance construction succeeds. An actual unstarted runtime and
RemotingCommand.encode invocation in RuntimeConfigFixtures prove:

- JSON encoding succeeds, omits the language key, and decoding the omitted key
  produces Java's default JAVA language.
- ROCKETMQ binary encoding throws NullPointerException for null language.

The native ClientConfig and RemotingCommand currently require LanguageCode.
JSON encoding always emits a language. Treating null as Java would send a
different header; treating null as unconditional failure would reject Java's
successful JSON path. Mapping it to an unknown numeric language is also incorrect.

## Proposed decision

Use Option<LanguageCode> for ClientConfig.language and RemotingCommand.language.
Preserve existing defaults as Some(Java) and Some(Rust), respectively. Core typed
RPCs keep Java-default language except heartbeat, which uses the nullable client
setting. JSON omits None, decodes an absent key to Some(Java), and must distinguish
an explicitly supplied null according to the Java decoder. Binary encoding of
None returns a native codec error, never a fabricated language byte.

Update hooks, request/response models, codecs and all construction/tests together.
Before/after hooks must observe the actual nullable field, allowing an owner hook
to replace it as Java permits; a core-only early rejection or hidden sentinel
would alter that control flow. Existing valid/unknown language behavior remains
subject to its source-backed lower-layer contract.

## Review boundary

RemotingCommand is a completed Phase 2 public model. The execution index requires
review and an ADR before changing it. The owner-approved earlier heartbeat Vec
change does not automatically approve this unrelated lower public field change.
Approval was requested on 2026-10-06; no implementation or acceptance is claimed.
Independent Phase 6 nullable AccessChannel/SOCKS corrections do not require this
lower command change and are separately implemented and verified.
