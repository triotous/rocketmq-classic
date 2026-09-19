# Plan: Phase 17 — Admin/query public surface

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 17；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`18-phase-17-admin-query.md`](18-phase-17-admin-query.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 1. 目標與範圍

公開核准的 MQAdmin/query operation，完成 client-side administrative processor。不擴張為 rocketmq-tools command suite、Broker management server 或 Dashboard。

## 2. 前置條件與現況分析

Phase 0–16 已驗收。重用 Phase 9 offset-boundary RPC、Phase 8 message decoder、Phase 6 processor dispatch 與 Phase 16 namespace/hook。只有核准實際 operation 才建立 optional admin crate；否則在 core/public crate 放小型 facade。

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

### 4.1 P17-01 — 核准精確 client admin surface

由 pinned rocketmq-client MQAdmin 盤點，不使用獨立 tools artifact 的範圍。create topic、fetch publish/subscribe queue、search/min/max offset、earliest store time、view message 與 query-by-key/topic 逐項記錄 supported/deferred 與 owner 核准。納入 client inventory 的 inbound reset-offset、status、running-info 與 direct-consume 義務。即使 optional admin exposure 延後，已使用的 offset primitive 仍需支援。

### 4.2 P17-02 — 重用 model 並補齊 RPC 證據

核准但缺少的 operation，先新增 request/response model 與 fixture，再寫 network method。重用 route conversion 與 offset-boundary API，不複製常數。驗證 topic/key/time range/max count，依 Java source 區分 unique client ID lookup 與 offset message ID lookup。view/query message 用既有有界 decoder，統一 namespace stripping。

### 4.3 P17-03 — 實作 query fan-out 與 public result

multi-Broker query 依 Java 的 broker selection、merge/dedup/order 與 partial-error semantics。透過既有 runtime 限制 concurrency，全部 query 共用 deadline。topic creation 驗證 permission/queue count 並送到正確 Broker；不可把建立 topic 當作 read fallback 的副作用。透過 facade 公開 typed result，保留 raw Broker error 與 stable message ID。

### 4.4 P17-04 — 安全啟用 administrative consumer processor

reset-offset 暫停相關 queue、fence generation、協調 in-flight work，依 Java replace/freeze-update store，persist 後只恢復一致的新 generation。status/running-info 回 bounded snapshot，不阻塞 reader 或洩漏 credential。direct-consume 在有界執行中使用正確 group/listener mode，映射 result/timing，不執行正常 ProcessQueue commit。unknown group、malformed body 與 shutdown 回 source-compatible error，不假裝成功空結果。

### 4.5 P17-05 — 稽核剩餘 unsupported processor

重新檢視全部 Phase 6 stub。各項必須成為已測 handler，或有文件與 owner 核准的 intentional difference；不可保留 generic success stub。為核准 query/create operation 加最小 public example，明列 permission 需求。以直接 evidence ID 更新 REQUEST_CODES.md 與 method-level matrix。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。共通命令、證據規則與 integration 前置設定見執行索引。

- [ ] **P17-V01** — 每個 approved operation 有 success、not-found/permission/error 與 malformed-response；既有 offset/route 行為不變。
- [ ] **P17-V02** — 本機 multi-Broker query 在 duplicate key、有界 time range 與 partial failure 下符合 Java。
- [ ] **P17-V03** — reset versus slow callback/late pull 不會恢復舊 offset；direct-consume 不改 regular offset，status request 維持可回應。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-core --test admin --locked
cargo test -p rocketmq-classic-consumer --locked
bash tests/differential/run.sh --phase 17
```

## 6. 驗收與交接

Phase 18 接收完整分類的 public-method 與 inbound-processor 清單。owner-approved exclusion 未記錄前，不可宣稱全部 Java public API 相容。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 18](19-phase-18-compatibility-release-gate.zh-tw.md).

## 7. 待決問題／必要決策

1. 首個 stable release 必須公開哪些 MQAdmin operation？請於 P17-01 核准 method list；既有 offset primitive 與安全處理 inbound administrative request 不可直接刪除。
