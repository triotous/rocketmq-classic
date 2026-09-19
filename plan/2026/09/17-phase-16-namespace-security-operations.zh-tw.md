# Plan: Phase 16 — Namespace、hook、安全與維運功能

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 16；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`17-phase-16-namespace-security-operations.md`](17-phase-16-namespace-security-operations.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 1. 目標與範圍

完成跨功能 public configuration 與行為，不將 authentication policy 寫死於 socket code。以 fixture 與本機 TLS/auth profile 驗證；部署專屬檢查留至 Phase 18。

## 2. 前置條件與現況分析

Phase 0–15 已驗收。TLS transport 已存在，本階段補齊與驗證 interoperability，不重寫。public API 穩定前需審查 RPC/consume hook 接點與 trace/auth scope。

- 先完成: [Phase 15](16-phase-15-request-reply.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `NamespaceUtil`
- `ClientConfig namespace methods`
- `RPCHook`
- `SendMessageHook`
- `ConsumeMessageHook`
- `5.5.1 auth/ACL signing call sites`
- `trace dispatcher/hooks`
- `ChannelEventListener`
- `TLS configuration`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

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

namespace 在 logical wire boundary 只 wrapping 一次；user-visible result 依 Java unwrapping。hook 在 signing/encoding 前看到最終 request field/body，每次 retry 的 route/queue mutation 後重新 signing。保留 unknown ext field。log 遮蔽 credential/signature/message body。TLS 預設驗證 peer identity，不允許 plaintext 或 invalid-certificate fallback。public configuration 經 facade 傳入，不要求存取 internal crate。

## 4. 依序實作步驟

### 4.1 P16-01 (16.1) — 一致套用 namespace

以 fixture 測 normal topic/group、retry 與 DLQ prefix、already-wrapped name、empty namespace 與 Unicode。實作 pure wrap/unwrap helper，套用 producer send、subscription、route lookup、offset、heartbeat、rebalance、transaction/reply 及 admin input。caller logical name 與 wire name 分開，避免 retry double wrapping。測 response topic/queue unwrapping 與 explicit queue selector。

### 4.2 P16-02 (16.2) — 實作有順序與明確 error semantics 的 hook

提供 approved Java surface 所需 before-request/after-response hook 及 producer/consumer hook context。以 source fixture 記錄 success、error、timeout、retry 與 oneway 的呼叫順序；不存在 after-response 的情況不可虛構。before-request failure 終止 send，不遺留 pending entry。user hook 在 shared lock 與 reader loop 外執行；記錄是否允許 blocking 及 bounded execution。auth hook 可修改 extFields，但不可繞過 frame limit。

### 4.3 P16-03 (16.3) — 實作核准且固定版本的 auth 路徑

先確認精確 5.5.1 client signer、canonicalization、body inclusion、excluded field、algorithm 與 server configuration。只透過 hook 實作核准路徑；盡可能重用已存在且維護中的相容 crypto dependency，不手寫密碼演算法。使用 synthetic credential 對兩種 serialization 進行 signature golden test，包含 non-ASCII field、retry 與 oneway。wrong secret/missing credential/tampered body 在本機 auth profile 必須失敗。不得提交真實 credential 或靜默降級為 unauthenticated transport。

### 4.4 P16-04 (16.4) — 補齊 facade TLS configuration 與 failure test

提供 trusted CA、server-name 行為，以及核准 mTLS profile 需要時的 client identity。啟動 task 前驗證 unreadable/malformed credential。測 CA trust、wrong name、expired certificate、unsupported handshake 與缺少必要 client certificate。保留 IPv6/DNS identity 與 original deadline。記錄本機 test CA 與 production trust setup 差異，不提供 accept-all switch。

### 4.5 P16-05 (16.5) — 實作或明確延後 trace parity

owner 核准 scope 後，依 Java 實作 trace event field/order 與 trace message encoding，附 golden fixture。使用 bounded trace queue 與停用 tracing 的 isolated internal producer，避免 recursion；定義 source-compatible overflow 與 shutdown-flush。trace failure 不可靜默改變 business-send result。若延後，記錄核准的 user-visible gap，不提供 no-op trace option。

### 4.6 P16-06 (16.6) — 提供受管理 connection event 與維運文件

提供 connect/close/exception/idle observation，包含 connection generation 與經遮蔽 peer detail。經 reader 外的 bounded owned dispatch 投遞；optional telemetry 與 correctness-critical hook 分別定義 overflow semantics。測 slow observer 與 shutdown drain。記錄 namespace、security、hook、trace selection 與 certificate provisioning 的 facade example；目標部署專屬驗證集中至 Phase 18。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。共通命令、證據規則與 integration 前置設定見執行索引。

- [ ] **P16-V01** — namespace fixture 通過 send/consume/retry/DLQ/offset/transaction/reply；既有 no-namespace 行為不變。
- [ ] **P16-V02** — auth signature golden 與本機 TLS/auth 正反例在無不安全 bypass 下通過；hook retry/order/cancellation 不遺留 pending entry。
- [ ] **P16-V03** — 支援 trace 時，event 可與 Java 交叉解碼、無 recursion 且 bounded flush 正常；slow channel observer 不阻塞 transport。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-remoting --test security --locked
cargo test --workspace --all-features --locked
bash tests/differential/run.sh --phase 16
```

## 6. 驗收與交接

所有對外宣稱 production setting 皆可透過 facade 設定。release claim 前記錄 security 與 optional trace scope；實際 production topology 驗收由 owner 在 Phase 18 執行。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 17](18-phase-17-admin-query.zh-tw.md).

## 7. 待決問題／必要決策

1. v1.0 必須支援哪一種 pinned 5.5.1 ACL/auth mode 與 TLS topology（server TLS 或 mTLS、custom CA、proxy）？這決定 P16-03/04 及最終 owner-run security profile。
2. v1.0 是否必須具備 Java message trace parity？若否，需核准明確 documented gap，不做 no-op implementation；P16-05 依賴此決策。
