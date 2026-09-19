# Plan: Phase 9 — Pull consumer 與 offset store

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 9；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`10-phase-09-pull-offset-store.md`](10-phase-09-pull-offset-store.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 1. 目標與範圍

在 Push scheduler 前提供 explicit pull 與 crash-safe offset persistence。拉取 batch 不可隱含 acknowledge。

## 2. 前置條件與現況分析

Phase 8 decoding 與 Phase 6 runtime registration 已驗收。現在實作 min/max/search/earliest-time RPC，因 Phase 11 start-offset policy 需要；Phase 17 重用，不可延到該階段。

- 先完成: [Phase 8](09-phase-08-message-decode-pull-results.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `Pull consumer implementations`
- `PullAPIWrapper`
- `OffsetStore`
- `RemoteBrokerOffsetStore`
- `LocalFileOffsetStore`
- `MQClientAPIImpl offset boundary methods`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-consumer/src/pull/
crates/rocketmq-classic-consumer/src/offset/mod.rs
crates/rocketmq-classic-consumer/src/offset/remote.rs
crates/rocketmq-classic-consumer/src/offset/local.rs
crates/rocketmq-classic-core/src/api/consumer.rs
crates/rocketmq-classic-protocol/src/header/
crates/rocketmq-classic-consumer/tests/offsets.rs
examples/pull_consumer.rs
```

OffsetStore 有兩個實際 implementation，因此使用 trait。內部分開 Found(offset)、NotFound 與 failure，只在 compatibility boundary 轉換 Java sentinel。remote/local store 依 Java 共用 update/read-mode/increase-only 語意。freeze 或 reset 與 offset update 以同一短鎖原子轉換，不跨 RPC/file I/O 持鎖。persist snapshot，不 persist 仍上鎖的 live map。

## 4. 依序實作步驟

### 4.1 P9-01 — 定義並測試 offset transition

實作 load、update、source 要求時的 update-and-freeze、依 mode read、persist one/all、remove、snapshot/clone 與 update-to-Broker。建立 memory-only、store-only、memory-first、missing value、stale increase-only update 與 reset/freeze 順序表。提供 typed outcome，避免將 timeout 誤認為沒有 stored offset。

### 4.2 P9-02 — 實作 remote store

使用 Phase 5 query/update RPC 與 runtime broker resolution。read mode 決定何時填 cache、何時查 Broker。配合 sync 與 oneway persistence call site；oneway 成功只代表 dispatched，不是 durable acknowledgement。persist 失敗仍保留 in-memory progress 供 retry，不可提早移除 offset entry。

### 4.3 P9-03 — 持久化 local offset，避免破壞舊檔

以 fixture 記錄 Java JSON layout、default path derivation 與 backup recovery。支援 explicit storage directory；group 與 client-ID path component 需驗證／編碼以防 traversal。同目錄寫入 unique temporary file，flush/sync，保留上一份 valid backup，再 atomic rename，平台支援時 sync directory。primary 畸形時使用 valid backup；兩者都無法讀取時回 error，不可重設為零。定義 single-writer ownership，或拒絕同檔並行 writer。

### 4.4 P9-04 — 提供 explicit pull 與 offset control

I/O 前驗證 queue、expression、非負 requested offset/count 與 timeout/suspend 關係；Java sentinel handling 僅保留在 offset-store API。逐 queue 解析 suggested broker，依 Java 套用 pull flag/commit-offset 規則。回傳 Phase 8 decoded outcome，不自動 commit。乾淨註冊／解除 pull consumer，並透過 facade 提供 explicit persist/commit。

### 4.5 P9-05 — 新增後續 rebalance 所需 offset-boundary primitive

為 search offset、min/max offset 與 earliest store time 新增 typed header/response fixture 與 single-attempt RPC。timestamp 採 Java signed milliseconds；區分 empty queue 與 lookup failure。可執行 example 依序 pull、成功處理、update next offset、persist；說明 persist 前 crash 的 at-least-once recovery。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。共通命令、證據規則與 integration 前置設定見執行索引。

- [ ] **P9-V01** — 所有 read/update/freeze mode 符合 Java，包含 missing offset、increase-only race 與 transport failure 區分。
- [ ] **P9-V02** — 在 local-file write/rename 各步前後注入 failure，保留可恢復的舊或新 snapshot；測 corrupt primary/backup 與 path traversal。
- [ ] **P9-V03** — 本機 Broker manual pull/restart 從 persisted next offset 繼續；所有 pull status、suggested broker 與 boundary RPC 符合 Java。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-consumer --test offsets --locked
cargo build -p rocketmq-classic --examples --locked
bash tests/differential/run.sh --phase 9
```

## 6. 驗收與交接

Phase 10 接收 explicit pull wrapper 與 durable store。Phase 11 可計算 initial offset，不需依賴 Phase 17。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 10](11-phase-10-push-consumer-pipeline.zh-tw.md).
