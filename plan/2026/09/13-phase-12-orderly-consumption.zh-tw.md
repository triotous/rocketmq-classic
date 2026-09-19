# Plan: Phase 12 — Orderly consumption 與 queue lock

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 12；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`13-phase-12-orderly-consumption.md`](13-phase-12-orderly-consumption.md)。此文件是實作計畫，不是已完成／已驗證的報告。

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

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。共通命令、證據規則與 integration 前置設定見執行索引。

- [ ] **P12-V01** — table test 涵蓋 partial lock success、renewal response race、exact expiry 與全部 supported listener/context result。
- [ ] **P12-V02** — 不同 queue 可獨立前進，各 queue 在 suspend、rollback、retry 與 lock loss 中維持順序。
- [ ] **P12-V03** — 本機 mixed Java/Rust orderly consumer 競爭、slow callback rebalance 與 shutdown 不產生 stale commit；Broker lock state 在 cleanup/expiry 後收斂。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-consumer --test orderly --locked
cargo build -p rocketmq-classic --examples --locked
bash tests/differential/run.sh --phase 12
```

## 6. 驗收與交接

現在可對外宣稱已測試 mode 的 orderly public API。lock-related matrix row 必須連結 race 與 mixed-group 證據，不能只有成功 lock RPC。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 13](14-phase-13-lite-assignment-pop.zh-tw.md).
