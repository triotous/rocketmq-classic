# Plan: Phase 13 — Lite Pull、assignment 與 POP API

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 13；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`14-phase-13-lite-assignment-pop.md`](14-phase-13-lite-assignment-pop.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 1. 目標與範圍

完成明確選定的額外 consumer surface，保留 Broker 發出的 POP receipt metadata。不得擴張成未核准的新 high-level POP framework。

## 2. 前置條件與現況分析

Phase 4–5 extension model/RPC、Phase 8 decoding 與 Phase 11 assignment 已備妥。主計畫允許 supported/deferred/lower-level-only API，因此 P13-01 是 scope gate；不依賴決策的 inventory 可先做。

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

### 4.1 P13-01 — 核准完整 method-level scope 表

盤點 Lite Pull 的 subscribe/unsubscribe、assign、poll、seek、pause/resume、committed offset、auto/manual commit 與 timestamp/queue helper；另外盤點 assignment/request-mode 及全部實際 POP call。每項選擇 v1 supported、later 或 lower-level-only，需 owner 核准並記錄 user-visible gap。不可降低 Phase 5 已要求的 baseline lower-level RPC。未核准項目維持 TODO，不可標 intentional difference。

### 4.2 P13-02 — 實作 Lite mode 與有界 polling

依 Java 禁止不支援的 subscription/explicit-assignment 混用。重用 pull scheduling/decoding，不經 listener dispatch。分開 fetched、delivered 與 committed position；seek 以 generation 使 queued/in-flight result 失效，清除 obsolete cache。poll timeout、max batch、empty result、pause/resume 與 shutdown 需符合 fixture。auto-commit 只依 Java 規則執行，不可因 prefetch bytes 就提交。

### 4.3 P13-03 — 轉換 Broker assignment 與 request mode

使用 QUERY_ASSIGNMENT/SET_MESSAGE_REQUEST_MODE typed RPC，保留 static-topic mapping 與 mode metadata。public boundary 驗證 group/topic/mode。unsupported-Broker outcome 與 valid empty assignment 分開；透過既有 assignment generation 更新，不原地修改 active queue state。

### 4.4 P13-04 / P13-05 — 完成 POP receipt operation

依 source 的 delimiter/field rule 解碼 pop result 與 extra-info，numeric parsing 必須有界。精確保留 pop time、invisibility、revive/retry 與 queue offset mapping，包含 batched/mixed-queue result。實作 ACK、source 有使用的 batch ACK 與 change-invisibility；expired/malformed/duplicate receipt outcome 依 Java/Broker fixture 定義。對 caller 以 opaque receipt 搭配安全 accessor，預設不記錄 receipt。不把 lower-level POP call 宣稱為完整 Push POP consumer。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。共通命令、證據規則與 integration 前置設定見執行索引。

- [ ] **P13-V01** — 每個 supported Lite method 具 success/error differential；seek versus late pull、pause/resume 與 commit/restart 不跳過 prefetched message。
- [ ] **P13-V02** — 本機 POP/ACK/change-invisibility 測 success、expiry/redelivery、changed receipt、malformed extra-info 與 mixed-queue mapping。
- [ ] **P13-V03** — 每個 Java consumer-facing API 均分類；supported 項有 integration 證據，deferred 項不列入對外 parity。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-consumer --test lite_pop --locked
bash tests/differential/run.sh --phase 13
```

## 6. 驗收與交接

在完成核准 surface，或記錄 owner-approved gap 且通過 lower-level baseline 前，不進入 Phase 14。不得靜默省略。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 14](15-phase-14-transaction-producer.zh-tw.md).

## 7. 待決問題／必要決策

1. v1.0 必須支援哪些 Lite Pull/public POP API？是否允許部分只提供 lower-level？這決定 P13-01/02 與 public facade example；Phase 5 lower-level call 仍為必要。
