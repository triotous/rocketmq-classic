# Plan: Phase 16 — Namespace, hooks, security and operational features

> Parent: [`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md), Phase 16; execution index: [`README.md`](README.md).
> Translation: [`17-phase-16-namespace-security-operations.zh-tw.md`](17-phase-16-namespace-security-operations.zh-tw.md). This is an implementation plan, not a report of completed or verified implementation.

## 1. Goal and scope

Complete cross-cutting public configuration and behavior without embedding authentication policy in socket code. Validate locally with fixtures and TLS/auth test profiles; deployment-specific checks belong to Phase 18.

## 2. Prerequisites and current-state analysis

Phases 0–15 are accepted. TLS transport already exists; this is completion/interoperability, not a replacement. RPC/consume hook seams and trace/auth scope require review before public API stabilization.

- Read/complete first: [Phase 15](16-phase-15-request-reply.md).

### 2.1 Java references to inspect

- Before coding, resolve these classes/methods and their call sites using the pinned-source rules in the index; the Java artifact has not been independently verified by this planning task.
- `NamespaceUtil`
- `ClientConfig namespace methods`
- `RPCHook`
- `SendMessageHook`
- `ConsumeMessageHook`
- `5.5.1 auth/ACL signing call sites`
- `trace dispatcher/hooks`
- `ChannelEventListener`
- `TLS configuration`

## 3. Target files and implementation contract

These are proposed paths relative to the repository root, not existing Rust/Java implementation files. Reuse files established by prior phases; do not create parallel implementations.

```text
crates/rocketmq-classic-common/src/namespace.rs
crates/rocketmq-classic-remoting/src/client/hooks.rs
crates/rocketmq-classic-core/src/auth.rs
crates/rocketmq-classic-core/src/config.rs
crates/rocketmq-classic-producer/src/trace.rs
crates/rocketmq-classic-consumer/src/consume/
crates/rocketmq-classic-remoting/tests/security.rs
integration-tests/compose.yaml
docs/security.md
docs/compatibility/KNOWN_DIFFERENCES.md
```

Namespace wrapping happens once at the logical wire boundary; user-visible results are unwrapped according to Java. Hooks see the final request fields/body before signing/encoding, and signing runs again for each retry after route/queue mutations. Unknown ext fields remain intact. Credentials/signatures/message bodies are redacted from logs. TLS verifies peer identity by default; no plaintext or invalid-certificate fallback. Public configuration must flow through the facade, not require internal-crate access.

## 4. Ordered implementation tasks

### 4.1 P16-01 (16.1) — Apply namespace uniformly

Fixture normal topics/groups, retry and DLQ prefixes, already-wrapped names, empty namespace and Unicode. Implement pure wrap/unwrap helpers and use them for producer sends, subscriptions, route lookup, offsets, heartbeat, rebalance, transaction/reply and admin inputs. Keep logical caller names separate from wire names to avoid double wrapping across retries. Test response topic/queue unwrapping and explicit queue selectors.

### 4.2 P16-02 (16.2) — Implement ordered hooks with clear error semantics

Expose before-request/after-response hooks and producer/consumer hook contexts required by the approved Java surface. Source-fixture the invocation order for success, error, timeout, retry and oneway; do not invent an after-response event where none exists. Before-request failures abort sending without leaking pending entries. User hooks run outside shared locks and reader loops; document whether hooks may block and how bounded execution is enforced. Auth hooks may modify extFields but not bypass frame limits.

### 4.3 P16-03 (16.3) — Implement the approved pinned auth path

First identify the exact 5.5.1 client signer, canonicalization, body inclusion, excluded fields, algorithm and server configuration. Implement only that approved path through hooks; use a maintained compatible crypto dependency already present if possible, never handwritten cryptography. Golden-test signatures with synthetic credentials and both serializations, including non-ASCII fields, retries and oneway. Wrong secret/missing credentials/tampered body must fail against the local auth profile. Do not commit real credentials or silently downgrade to unauthenticated transport.

### 4.4 P16-04 (16.4) — Finish facade TLS configuration and failure tests

Expose trusted CA, server-name behavior and client identity if required by the approved mTLS profile. Validate unreadable/malformed credentials before starting tasks. Test CA trust, wrong name, expired certificate, unsupported handshake and required client-certificate absence. Preserve IPv6/DNS address identity and original deadlines. Document local test CA versus production trust setup without an accept-all switch.

### 4.5 P16-05 (16.5) — Implement or explicitly defer trace parity

After owner scope approval, implement Java trace event fields/order and trace message encoding with golden fixtures. Use a bounded trace queue and isolated internal producer with tracing disabled to prevent recursion; define source-compatible overflow and shutdown-flush behavior. Trace failure must not silently change business-send results. If deferred, add an approved user-visible gap and do not expose a no-op trace option.

### 4.6 P16-06 (16.6) — Expose owned connection events and operational docs

Expose connect/close/exception/idle observations with connection generation and redacted peer details. Deliver via bounded owned dispatch outside the reader; document overflow semantics separately for optional telemetry versus correctness-critical hooks. Test slow observers and shutdown drain. Document facade examples for namespace, security, hooks, trace selection and certificate provisioning; collect target deployment-only validation tasks for Phase 18.

## 5. Verification

Write the failing check/oracle fixture before its implementation. The cases below must become runnable checks, not manual checklist-only claims. Shared commands, evidence rules and integration prerequisites are in the execution index.

- [ ] **P16-V01** — Namespace fixtures pass across send/consume/retry/DLQ/offset/transaction/reply; existing no-namespace behavior is unchanged.
- [ ] **P16-V02** — Golden auth signatures and local TLS/auth positive/negative tests pass without insecure bypass; hook retry/order/cancellation tests leak no pending entries.
- [ ] **P16-V03** — If trace is supported, events cross-decode with Java, recursion is absent and bounded flush works; slow channel observers cannot block transport.

### 5.1 Local commands after implementation

Run after this phase or earlier phases create the named entry points. This documentation task does not execute these commands or claim their tests pass.

```bash
cargo test -p rocketmq-classic-remoting --test security --locked
cargo test --workspace --all-features --locked
bash tests/differential/run.sh --phase 16
```

## 6. Acceptance and handoff

All advertised production settings are reachable through the facade. Security and optional trace scope are recorded before release claims; actual production topology acceptance is owner-executed in Phase 18.

- Handoff includes changed paths, case IDs, actual commands/results, fixture provenance and matrix status/gaps; follow the shared rules in the index.
- Next: [Phase 17](18-phase-17-admin-query.md).

## 7. Open questions / decisions required

1. Which pinned 5.5.1 ACL/auth mode and TLS topology must v1.0 support (server TLS only or mTLS, custom CA, proxies)? This controls P16-03/04 and the final owner-run security profile.
2. Is Java message trace parity required for v1.0? If not, approve an explicit documented gap rather than a no-op implementation; P16-05 depends on this.
