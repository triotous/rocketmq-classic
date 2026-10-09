# ADR-0044: Match ClientConfig TLS property default

Status: Accepted

## Context

RocketMQ 5.5.1 keeps two TLS inputs. `TlsSystemConfig.tlsEnable` reads `tls.enable` with a false default, while `tlsMode` reads `tls.server.mode`. `ClientConfig.useTLS` initializes from `tlsEnable`. A standalone `NettyClientConfig` derives its own default from whether `tlsMode` is `ENFORCING`, but `MQClientInstance` explicitly copies `ClientConfig.useTLS` into its Netty config.

Rust `ClientConfig::with_properties` previously used the enforcing mode as the fallback for `use_tls`, so `tls.server.mode=enforcing` incorrectly enabled ClientConfig TLS when `tls.enable` was absent.

## Decision

Derive `ClientConfig.use_tls` only from the `tls.enable` property, using false when absent or malformed. Keep `tls.server.mode` handling for the separate transport-mode/standalone Netty default. Runtime construction continues to use the ClientConfig value copied into the lower configuration.

## Evidence

The isolated `ClientConfigTlsModeProbe` sets `tls.server.mode=enforcing`, clears `tls.enable`, and constructs both Java configuration classes in a fresh JVM. The pinned 5.5.1 fixture records `tlsEnable=false`, `tlsMode=ENFORCING`, `ClientConfig.useTLS=false`, standalone `NettyClientConfig.useTLS=true`, and the runtime-copied value false. The native `client_tls_enable_uses_global_enable_not_server_mode` regression failed before implementation `4084689`; oracle/test commit `cbac242`; fixture `client-config-tls.json` SHA-256 `8ac6b07475ea0af746b70541678355e3f244fffcf6b6bb0bd70d9a7e60a14b46`.

Full workspace/all-target tests, strict all-target/all-feature Clippy, pinned offline Maven verify (22 scenarios and deterministic fixture checks), comparator self-test/manifest validation (22 scenarios), formatting, and diff checks pass. This closes one ClientConfig constructor input. TLS credential provisioning and live acceptance remain Phase 19 work; other Phase 6 source/API/input reconciliation remains open and P6-V04 stays `PENDING_PHASE_19`.
