# Plan: Phase 11 — Rebalance 與 assignment lifecycle

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 11；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`12-phase-11-rebalance.md`](12-phase-11-rebalance.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 1. 目標與範圍

完成 public Push startup 與可重現 queue assignment，包含並行 pull/callback 下安全移除／新增 queue。

## 2. 前置條件與現況分析

Phase 10 private activation/removal 接點與 Phase 9 offset-boundary RPC 已備妥。orderly lock-specific integration 到 Phase 12 完成，目前不得宣稱 orderly ownership。

- 先完成: [Phase 10](11-phase-10-push-consumer-pipeline.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `RebalanceImpl`
- `RebalancePushImpl`
- `RebalanceService`
- `AllocateMessageQueueAveragely`
- `AllocateMessageQueueAveragelyByCircle`
- `consistent-hash and machine-room allocation classes`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-consumer/src/rebalance/mod.rs
crates/rocketmq-classic-consumer/src/rebalance/strategy/
crates/rocketmq-classic-consumer/src/rebalance/service.rs
crates/rocketmq-classic-consumer/src/push/consumer.rs
crates/rocketmq-classic-consumer/tests/rebalance.rs
examples/push_consumer.rs
```

每個 consumer 同時間只允許一次 rebalance 修改；wakeups 合併。strategy 前先以 Java 相容比較排序 MessageQueue 與 client ID。由一致 snapshot 建立 desired assignment，再與 current queue generation reconcile。consumer-list query 失敗不等於 authoritative empty group，不可因此任意重分配。ownership/offset 義務完成前不得移除 local state。

## 4. 依序實作步驟

### 4.1 P11-01 / P11-02 — 以 pure logic 實作 average allocation

使用主計畫 strategy signature；以 fixture 確認 invalid input/current-client-missing 的 error 或 empty result。精確移植 Java index/start/range 算術，包含 queue 少於 consumer。一起測全部 client：assignment 互斥且涵蓋 eligible queue，不只測 current client。

### 4.2 P11-03 — 盤點並實作其他 advertised strategy

盤點 pinned public allocation class。實作納入 parity 的 circle 及 source 確認的 consistent-hash/machine-room variant，精確重現 hash input byte、virtual node、comparator 與 error。適當時重用已安裝 hashing dependency，不以 Rust DefaultHasher 代替。延後 strategy 在 matrix 明示不對外宣稱，任何宣稱中的差異需 owner 核准。

### 4.3 P11-04 / P11-05 — 解析 clustering 與 broadcasting assignment

broadcasting 分配全部 subscribed queue，使用 local offset。clustering 取得 consumer ID、排序後呼叫所選 strategy；依 fixture 區分 unavailable、empty 與 missing-current-client。更新 subscription snapshot；fetch 失敗於下一 owned cycle 重試。接上 startup 第 12 步，以 real rebalance 取代 Phase 10 test assignment。

### 4.4 P11-06 — 依 ownership 順序移除 assignment

取消 pending pull 前先標 dropped；停止新增 consume work，保留 unresolved offset 並 persist safe progress。cleanup 失敗需保留 removal state 供 retry，不可直接刪除證據。replacement generation 不得接收舊 listener/pull completion。現在安裝 orderly unlock 接點，Phase 12 啟用。cleanup 成功後才以 identity 移除 registry state。

### 4.5 P11-07 — 計算安全 initial offset 並啟用

只在 Java 定義的時點移除 stale in-memory offset，再讀 authoritative stored offset。不存在時，透過 Phase 9 RPC 實作各 ConsumeFromWhere policy，包含 Java fixture 中的 retry-topic 與 timestamp/time-zone 行為。無法取得 start offset 不可變成零。建立一個 ProcessQueue generation、啟用一個 PullRequest，activation 失敗則 rollback。Phase 12 會在 orderly activation 前插入 Broker locking。

### 4.6 P11-08 / P11-09 — 接上 trigger 並證明 race safety

管理單一 periodic service，支援 immediate startup、subscription-change 與 NOTIFY_CONSUMER_IDS_CHANGED wakeup。採 source 確認的 timing，以及 bounded/coalesced signal。shutdown 開始後拒絕新工作。以 barrier 測 remove-versus-pull、remove-versus-listener、concurrent-trigger 與 rapid join/leave；完成真正 public Push example 與 facade export。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。共通命令、證據規則與 integration 前置設定見執行索引。

- [ ] **P11-V01** — Java/Rust allocation 在 0/1/N queue、1/N client、10q/3c、missing client、Unicode order 與各 advertised strategy 相同。
- [ ] **P11-V02** — initial-offset policy 符合 Java；query/persist failure 不得重設為零或移除 live replacement。
- [ ] **P11-V03** — 本機 mixed Java/Rust group join/leave 在文件化 timeout 內收斂，無 persistent duplicate queue ownership；仍允許 transient redelivery。
- [ ] **P11-V04** — 真正 public Java producer→Rust Push 與 Rust producer→Java Push 通過 retry/offset 驗證；反覆 shutdown 不遺留 scheduler task。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-consumer --test rebalance --locked
cargo build -p rocketmq-classic --examples --locked
bash tests/differential/run.sh --phase 11
```

## 6. 驗收與交接

完成 Phase 10–11 累積 public Push 門檻。orderly 項目維持 PARTIAL，直到 Phase 12 的 lock renewal、expiry 與 callback completion fencing 驗收。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 12](13-phase-12-orderly-consumption.zh-tw.md).
