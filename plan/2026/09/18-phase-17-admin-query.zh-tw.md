# Plan: Phase 17 — Admin/query public surface

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 17；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`18-phase-17-admin-query.md`](18-phase-17-admin-query.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

公開全部 Java 5.5.1 rocketmq-client MQAdmin/query operation，完成全部 client-side administrative processor。不移植獨立 rocketmq-tools suite、Broker server 或 Dashboard。

## 2. 前置條件與現況分析

Phase 0–16 unit gate 通過。重用 Phase 9 offset RPC、Phase 8 decoder、Phase 6 dispatch 與 Phase 16 namespace/hook。crate packaging 屬內部設計；全部 Java client admin API 仍為必要。

- 先完成: [Phase 16](17-phase-16-namespace-security-operations.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `MQAdmin`
- `MQAdminImpl`
- `MQClientAPIImpl query/admin methods`
- `ClientRemotingProcessor reset/status/running-info/direct-consume methods`
- `QueryResult`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-core/src/api/admin.rs
crates/rocketmq-classic-protocol/src/header/
crates/rocketmq-classic-protocol/src/body/
crates/rocketmq-classic-consumer/src/admin.rs
crates/rocketmq-classic/src/lib.rs
crates/rocketmq-classic-core/tests/admin.rs
docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md
```

每個 operation 遵循 typed model→single-attempt MQClientApi→public facade→Java differential evidence。query result 保留 Java ordering、pagination/time range、not-found 與 per-Broker failure 行為。administrative reset 不等於一般 increase-only offset update：必須 fence pull/listener generation 並安全 persist reset。direct-consume request 不可靜默推進正常 consumer offset。

## 4. 依序實作步驟

### 4.1 P17-01 — 稽核完整 client admin surface

盤點 pinned rocketmq-client MQAdmin 全部 method 及設定，不納入獨立 tools artifact。包含 create topic、publish/subscribe queue、search/min/max offset、earliest store time、view/query message 及 source 確認的全部 operation。納入 inbound reset/status/running-info/direct-consume 義務。全部映射 Rust 及 unit test，不可選擇 supported/deferred。

### 4.2 P17-02 — 重用 model 並補齊 RPC 證據

全部缺少的 Java operation，先新增 request/response model 與 fixture，再寫 network method。重用 route conversion 與 offset-boundary API，不複製常數。驗證 topic/key/time range/max count，依 Java source 區分 unique client ID lookup 與 offset message ID lookup。view/query message 用既有有界 decoder，統一 namespace stripping。

### 4.3 P17-03 — 實作 query fan-out 與 public result

multi-Broker query 依 Java 的 broker selection、merge/dedup/order 與 partial-error semantics。透過既有 runtime 限制 concurrency，全部 query 共用 deadline。topic creation 驗證 permission/queue count 並送到正確 Broker；不可把建立 topic 當作 read fallback 的副作用。透過 facade 公開 typed result，保留 raw Broker error 與 stable message ID。

### 4.4 P17-04 — 安全啟用 administrative consumer processor

reset-offset 暫停相關 queue、fence generation、協調 in-flight work，依 Java replace/freeze-update store，persist 後只恢復一致的新 generation。status/running-info 回 bounded snapshot，不阻塞 reader 或洩漏 credential。direct-consume 在有界執行中使用正確 group/listener mode，映射 result/timing，不執行正常 ProcessQueue commit。unknown group、malformed body 與 shutdown 回 source-compatible error，不假裝成功空結果。

### 4.5 P17-05 — 稽核剩餘 unsupported processor

將每個 Java 支援的 Phase 6 processor stub 換成符合 source 且已測試的實作。不可保留刻意功能差異或 generic success stub。新增 public query/create example、permission requirement 及 source/unit evidence ID。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [ ] **P17-V01** — 每個 Java client operation 有 success、not-found/permission/error 與 malformed-response；既有 offset/route 行為不變。
- [ ] **P17-V03** — reset versus slow callback/late pull 不會恢復舊 offset；direct-consume 不改 regular offset，status request 維持可回應。

- [ ] **P17-U01** — scripted multi-Broker query response 驗證 duplicate-key merge、有界 time range、partial failure 及精確 Java error。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-core --test admin --locked
cargo test -p rocketmq-classic-consumer --locked
```

### 5.2 Phase 19 live 測試交接

保留以下 case ID 及行為要求，於 Phase 19 使用 5.3.4 Docker server 執行；不是本 phase 的通過條件，不可提前標 PASS。

- [ ] **P17-V02** — 本機 multi-Broker query 在 duplicate key、有界 time range 與 partial failure 下符合 Java。

## 6. 驗收與交接

Phase 18 接收完整 public-method、設定及 inbound-processor mapping 與通過的 unit 證據。不可排除 Java client API 或設定；live evidence 留待 Phase 19。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 18](19-phase-18-compatibility-release-gate.zh-tw.md).
