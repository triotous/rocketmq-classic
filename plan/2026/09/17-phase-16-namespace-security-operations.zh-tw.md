# Plan: Phase 16 — Namespace、hook、安全與維運功能

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 16；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`17-phase-16-namespace-security-operations.md`](17-phase-16-namespace-security-operations.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

完成全部 Java 5.5.1 namespace、hook、auth/TLS、trace 與 channel-event 設定／流程。本階段使用 unit test、signing fixture 與 scripted TLS peer；Docker interoperability 與部署專屬驗證屬 Phase 19。

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
docs/security.md
docs/compatibility/KNOWN_DIFFERENCES.md
```

namespace 在 logical wire boundary 只 wrapping 一次；user-visible result 依 Java unwrapping。hook 在 signing/encoding 前看到最終 request field/body，每次 retry 的 route/queue mutation 後重新 signing。保留 unknown ext field。log 遮蔽 credential/signature/message body。TLS 預設驗證 peer identity，不允許 plaintext 或 invalid-certificate fallback。public configuration 經 facade 傳入，不要求存取 internal crate。

## 4. 依序實作步驟

### 4.1 P16-01 (16.1) — 一致套用 namespace

以 fixture 測 normal topic/group、retry 與 DLQ prefix、already-wrapped name、empty namespace 與 Unicode。實作 pure wrap/unwrap helper，套用 producer send、subscription、route lookup、offset、heartbeat、rebalance、transaction/reply 及 admin input。caller logical name 與 wire name 分開，避免 retry double wrapping。測 response topic/queue unwrapping 與 explicit queue selector。

### 4.2 P16-02 (16.2) — 實作有順序與明確 error semantics 的 hook

提供 Java 5.5.1 存在的 before-request/after-response hook 及 producer/consumer hook context。以 source fixture 記錄 success、error、timeout、retry 與 oneway 的呼叫順序；不存在 after-response 的情況不可虛構。before-request failure 終止 send，不遺留 pending entry。user hook 在 shared lock 與 reader loop 外執行；記錄是否允許 blocking 及 bounded execution。auth hook 可修改 extFields，但不可繞過 frame limit。

### 4.3 P16-03 (16.3) — 實作固定版本的全部 Java client auth 路徑

先確認精確 5.5.1 client signer、canonicalization、body inclusion、excluded field、algorithm 與 server configuration。透過等價 hook 實作全部 source 確認的 Java client auth 路徑；盡可能重用已存在且維護中的相容 crypto dependency，不手寫密碼演算法。使用 synthetic credential 對兩種 serialization 進行 signature golden test，包含 non-ASCII field、retry 與 oneway。wrong secret/missing credential/tampered body 在本機 auth profile 必須失敗。不得提交真實 credential 或靜默降級為 unauthenticated transport。

### 4.4 P16-04 (16.4) — 補齊 facade TLS configuration 與 failure test

提供 trusted CA、server-name 行為，以及Java client identity 語意需要時的 client identity。啟動 task 前驗證 unreadable/malformed credential。測 CA trust、wrong name、expired certificate、unsupported handshake 與缺少必要 client certificate。保留 IPv6/DNS identity 與 original deadline。記錄本機 test CA 與 production trust setup 差異，不提供 accept-all switch。

### 4.5 P16-05 (16.5) — 實作完整 trace parity

以 golden fixture 實作全部 Java trace configuration、event field/order 與 encoding。採用符合 Java 的 queue/overflow 及 shutdown-flush 行為，使用 isolated internal producer 避免 recursion。trace failure 必須符合 Java business-send 語意。trace 為必要，不可延後形成缺口或提供 no-op option。

### 4.6 P16-06 (16.6) — 提供受管理 connection event 與維運文件

提供 connect/close/exception/idle observation，包含 connection generation 與經遮蔽 peer detail。經 reader 外的 bounded owned dispatch 投遞；optional telemetry 與 correctness-critical hook 分別定義 overflow semantics。測 slow observer 與 shutdown drain。記錄 namespace、security、hook、trace selection 與 certificate provisioning 的 facade example；目標部署專屬驗證集中至 Phase 19。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [ ] **P16-V01** — namespace fixture 通過 send/consume/retry/DLQ/offset/transaction/reply；既有 no-namespace 行為不變。
- [ ] **P16-V03** — trace event 可與 Java 交叉解碼、無 recursion 且 bounded flush 正常；slow channel observer 不阻塞 transport。

- [ ] **P16-U01** — signing fixture 與 scripted TLS peer 覆蓋安全正反例；hook order/retry/cancellation 與 trace flush 可重現。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-remoting --test security --locked
cargo test --workspace --all-features --locked
```

### 5.2 Phase 19 live 測試交接

保留以下 case ID 及行為要求，於 Phase 19 使用 5.3.4 Docker server 執行；不是本 phase 的通過條件，不可提前標 PASS。

- [ ] **P16-V02** — auth signature golden 與本機 TLS/auth 正反例在無不安全 bypass 下通過；hook retry/order/cancellation 不遺留 pending entry。

## 6. 驗收與交接

每個 Java client 設定均有等價 Rust API／設定途徑，不含 Rust 專有公開選項。security/trace unit 證據完整；Docker interoperability 與 owner production topology 驗證屬 Phase 19。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 17](18-phase-17-admin-query.zh-tw.md).
