# Plan: Phase 13 — Lite Pull、assignment 與 POP API

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 13；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`14-phase-13-lite-assignment-pop.md`](14-phase-13-lite-assignment-pop.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

完成全部 Java 5.5.1 額外 consumer 能力與設定，保留 POP receipt，精確對齊 Java public/lower-level exposure。不可發明 Rust 專有 high-level POP framework。

## 2. 前置條件與現況分析

Phase 4–5 model/RPC、Phase 8 decoding 與 Phase 11 assignment 已備妥。P13-01 是完整性稽核，不是允許延後 Java API。

- 先完成: [Phase 12](13-phase-12-orderly-consumption.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `DefaultLitePullConsumer`
- `DefaultLitePullConsumerImpl`
- `AssignedMessageQueue`
- `MQClientAPIImpl POP/ACK methods`
- `PopResult`
- `ExtraInfoUtil`
- `assignment/request-mode bodies`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-consumer/src/lite/
crates/rocketmq-classic-consumer/src/pop.rs
crates/rocketmq-classic-core/src/api/consumer.rs
crates/rocketmq-classic-protocol/src/body/
crates/rocketmq-classic-consumer/tests/lite_pop.rs
examples/lite_pull.rs
```

classic pull offset 與 POP receipt 分開。POP receipt 保留原 extra-info bytes 及經驗證的 parsed field；ACK/change-invisibility 使用精確 Broker identity、queue、offset 與 receipt 語意。change-invisibility response 依 Java 可能替換 handle；不得重建 pop time 或靜默沿用 stale handle。DefaultLitePullConsumer 行為由 call graph 定義，不可只看同名相近的 LITE_PULL_MESSAGE 常數。

## 4. 依序實作步驟

### 4.1 P13-01 — 稽核完整 method/configuration 表

盤點 Lite subscribe/unsubscribe、assign、poll、seek、pause/resume、offset、auto/manual commit、timestamp/queue helper、assignment/request-mode 及每個實際 POP call。每個 Java method、設定與流程都映射 Rust 及 unit evidence。對齊 Java visibility；只有 Java 本身無 high-level API 才可 lower-level-only。若 Java 有 Push POP 路徑，必須完整實作；不可缺少功能或提供 no-op option。

### 4.2 P13-02 — 實作 Lite mode 與有界 polling

依 Java 禁止不支援的 subscription/explicit-assignment 混用。重用 pull scheduling/decoding，不經 listener dispatch。分開 fetched、delivered 與 committed position；seek 以 generation 使 queued/in-flight result 失效，清除 obsolete cache。poll timeout、max batch、empty result、pause/resume 與 shutdown 需符合 fixture。auto-commit 只依 Java 規則執行，不可因 prefetch bytes 就提交。

### 4.3 P13-03 — 轉換 Broker assignment 與 request mode

使用 QUERY_ASSIGNMENT/SET_MESSAGE_REQUEST_MODE typed RPC，保留 static-topic mapping 與 mode metadata。public boundary 驗證 group/topic/mode。unsupported-Broker outcome 與 valid empty assignment 分開；透過既有 assignment generation 更新，不原地修改 active queue state。

### 4.4 P13-04 / P13-05 — 完成 POP receipt operation

依 source 的 delimiter/field rule 解碼 pop result 與 extra-info，numeric parsing 必須有界。精確保留 pop time、invisibility、revive/retry 與 queue offset mapping，包含 batched/mixed-queue result。實作 ACK、source 有使用的 batch ACK 與 change-invisibility；expired/malformed/duplicate receipt outcome 依 Java/Broker fixture 定義。對 caller 以 opaque receipt 搭配安全 accessor，預設不記錄 receipt。不把 lower-level POP call 宣稱為完整 Push POP consumer。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [ ] **P13-V01** — 每個 supported Lite method 具 success/error differential；seek versus late pull、pause/resume 與 commit/restart 不跳過 prefetched message。
- [ ] **P13-V03** — 每個 Java consumer-facing API／設定均完成實作與 unit 證據，適用時附 Phase 19 live scenario；不可延後或增加公開能力。

- [ ] **P13-U01** — Java Lite/POP fixture 與可控時鐘覆蓋每個 method、receipt expiry/replacement、redelivery decision 及 mixed-queue mapping。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-consumer --test lite_pop --locked
```

### 5.2 Phase 19 live 測試交接

保留以下 case ID 及行為要求，於 Phase 19 使用 5.3.4 Docker server 執行；不是本 phase 的通過條件，不可提前標 PASS。

- [ ] **P13-V02** — 本機 POP/ACK/change-invisibility 測 success、expiry/redelivery、changed receipt、malformed extra-info 與 mixed-queue mapping。

## 6. 驗收與交接

全部本階段 Java 能力完成實作並通過 unit/fixture check 後，才進入 Phase 14。live evidence 留待 Phase 19；不可透過核准豁免實作缺口。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 14](15-phase-14-transaction-producer.zh-tw.md).
