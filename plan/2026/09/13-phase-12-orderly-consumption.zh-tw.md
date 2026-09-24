# Plan: Phase 12 — Orderly consumption 與 queue lock

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 12；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`13-phase-12-orderly-consumption.md`](13-phase-12-orderly-consumption.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

加入 per-queue 有序 listener、Broker lock renewal/expiry，以及 rebalance-safe offset commit。不宣稱跨 queue 全域排序或 exactly-once side effect。

## 2. 前置條件與現況分析

Phase 10–11 concurrent pipeline 與 reconciliation 已驗收。重用 Phase 5 lock/unlock RPC 及 ProcessQueue lock field，不新增第二套 assignment registry。

- 先完成: [Phase 11](12-phase-11-rebalance.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `ConsumeMessageOrderlyService`
- `ProcessQueue orderly methods`
- `RebalanceImpl lock/unlock branches`
- `ConsumeOrderlyStatus`
- `ConsumeOrderlyContext`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-consumer/src/consume/orderly.rs
crates/rocketmq-classic-consumer/src/process_queue.rs
crates/rocketmq-classic-consumer/src/rebalance/mod.rs
crates/rocketmq-classic-consumer/tests/orderly.rs
examples/orderly_consumer.rs
```

分開 available、in-flight 與 committed orderly state。queue-specific async consume gate 序列化 callback；短 state lock 保護 transition，user code 前釋放。不同 queue 可並行。Broker lock ownership 與 local serialization 是不同要求。lock loss/drop 後的 callback success 不可在較新 assignment generation 下 commit；cancellation 無法撤回 user side effect，因此必須記錄可能 redelivery。

## 4. 依序實作步驟

### 4.1 P12-01 — 套用 batch lock result 並管理 renewal

依 Broker 分組呼叫 LOCK_BATCH_MQ/UNLOCK_BATCH_MQ。只有 response 中已鎖定 queue 可啟用；partial success 不可視為全部成功或全部失敗。逐 generation 記錄 monotonic freshness，依 Java configured interval renewal。單一受管理 renewal task 使用 current assignment snapshot；stale renewal response 不可重新鎖定 dropped/replaced queue。broadcasting 在適用時採 Java 無 distributed lock 分支。

### 4.2 P12-02 / P12-03 — 序列化 consumption 並驗證 freshness

取得 per-queue consume gate，驗證 dropped/generation/lock freshness，將下一個有序 batch 移至 in-flight，再於無 global state lock 下呼叫 listener。lock stale 時 suspend/reacquire，不可繼續。callback 後、commit 前再驗 ownership。使用 paused time 測 exact expiry boundary；duration 由 source/configuration 取得，不猜常數。

### 4.3 P12-04 — 映射全部 listener result 與 context mode

以 fixture 測 autoCommit true/false 搭配 SUCCESS、SUSPEND_CURRENT_QUEUE_A_MOMENT 及 source 支援的 COMMIT/ROLLBACK；若對外宣稱 deprecated mode，也須測試。success 只移除 acknowledged ordered prefix；suspend/rollback 還原相同順序。套用 source-defined retry limit、send-back/fallback 與 suspend delay bound。不可跳過先前失敗 message 而處理同 queue 後續 message。

### 4.4 P12-05 — 完成 lock-aware rebalance 與 shutdown

gain 時先 lock，才啟用正常 pull/consume。loss 時先 dropped、停止 queue work、persist safe offset 再 unlock；cleanup 未完成則 retry，不可復活 queue。slow listener 不可拖住整個 rebalance service；採 Java 相容的有界 retry/cleanup。shutdown 在安全嘗試 unlock/persist 後取消 renewal 並 join。unlock failure 必須可見，不可回報已確認 release。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [ ] **P12-V01** — table test 涵蓋 partial lock success、renewal response race、exact expiry 與全部 supported listener/context result。
- [ ] **P12-V02** — 不同 queue 可獨立前進，各 queue 在 suspend、rollback、retry 與 lock loss 中維持順序。

- [ ] **P12-U01** — scripted lock result、paused time 與 slow-callback barrier 驗證競爭、renewal/expiry、rebalance 及 shutdown 不產生 stale commit。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-consumer --test orderly --locked
cargo build -p rocketmq-classic --examples --locked
```

### 5.2 Phase 19 live 測試交接

保留以下 case ID 及行為要求，於 Phase 19 使用 5.3.4 Docker server 執行；不是本 phase 的通過條件，不可提前標 PASS。

- [ ] **P12-V03** — 本機 mixed Java/Rust orderly consumer 競爭、slow callback rebalance 與 shutdown 不產生 stale commit；Broker lock state 在 cleanup/expiry 後收斂。

## 6. 驗收與交接

現在可對外宣稱已測試 mode 的 orderly public API。lock-related matrix row 必須連結 race 與 mixed-group 證據，不能只有成功 lock RPC。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 13](14-phase-13-lite-assignment-pop.zh-tw.md).
