# Plan: Phase 15 — Request/reply 與單次完成語意

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 15；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`16-phase-15-request-reply.md`](16-phase-15-request-reply.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

實作 message-level correlation、reply construction 與 Broker-pushed reply delivery，與 Remoting opaque matching 分開。

## 2. 前置條件與現況分析

Phase 7–8 producer/decoder 與 Phase 6 reply processor slot 已備妥；transaction 已驗收。重用 Phase 4 reply header/property，補齊 Phase 5 reply-send method，不重複 transport。

- 先完成: [Phase 14](15-phase-14-transaction-producer.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `DefaultMQProducerImpl request methods`
- `RequestFutureHolder`
- `RequestResponseFuture`
- `RequestCallback`
- `MessageUtil.createReplyMessage`
- `ClientRemotingProcessor.receiveReplyMessage`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-producer/src/request_reply.rs
crates/rocketmq-classic-core/src/api/producer.rs
crates/rocketmq-classic-producer/tests/request_reply.rs
examples/request_reply.rs
```

pending correlation map 由 shared-client dispatch boundary 管理，每筆帶 producer-registration owner token；共享 runtime 的 producer 不可互換 reply handler 或清除彼此 request。entry 包含 deadline 與 oneshot sender；atomic removal 代表 completion。Remoting opaque 與 RocketMQ correlation ID 分開。TTL 是 Java wire duration property，內部等待採單一 monotonic deadline。

## 4. 依序實作步驟

### 4.1 P15-01 / P15-02 — 準備 correlation 與 lifecycle ownership

使用精確 Java correlation/reply-to/TTL property constant 與 ID format 語意。依 Java preparation，在 request delivery 前確保 runtime client identity 與 Broker heartbeat registration 已備妥。以既有 admission control 限制 pending request。註冊一個 shared reply dispatcher 與受管理 timeout scheduler，不可每個 request 建 callback executor。

### 4.2 P15-03 — 先註冊再 send，保留單一 deadline

send 前插入 correlation。route acquisition、heartbeat preparation、producer retry 與 reply wait 共用 original request deadline。send failure 只有在仍擁有 entry 時才 remove-and-fail；fast reply 可能已完成。caller cancellation 移除自身 entry；producer shutdown 只 drain 自身 token。late send completion 不可重建 timed-out request。

### 4.3 P15-04 — 實作 responder construction 與 inbound reply

新增符合 MessageUtil 的 public reply-message helper，保留 correlation、reply-to、TTL/topic/cluster metadata，驗證 missing request property。依 Java 使用 SEND_REPLY_MESSAGE/V2 送 reply，不可當 ordinary request send。以有界 body handling 解碼 PUSH_REPLY_MESSAGE_TO_CLIENT，claim correlation，對 duplicate/late reply 也依 Java 回相容 Remoting acknowledgement。unknown correlation 不得建立新 pending entry。

### 4.4 P15-05 / P15-06 — 讓 5.5.1 regression 可確定重現

reply、timeout、cancellation、send failure 與 shutdown 共用 remove-and-complete。以 barrier 讓 reply 與 timeout 同時符合條件，觀測 terminal ownership，不只計 Future poll。測 duplicate reply frame 與 response-before-send-completion。deterministic test 後再做重複 stress。單次 completion 不代表 request processing 或 reply delivery 恰好一次。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [ ] **P15-V01** — property/reply-header golden fixture 符合 Java；invalid request metadata 在 send 前失敗。
- [ ] **P15-V02** — 所有 terminal race 只有一個 owner，pending 歸零；涵蓋兩個 producer 共享 runtime 且各自 shutdown。

- [ ] **P15-U01** — Java request/reply fixture 與注入 reply/no-reply/late-reply event 驗證雙方角色、correlation 與 timeout 語意。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-producer --test request_reply --locked
```

### 5.2 Phase 19 live 測試交接

保留以下 case ID 及行為要求，於 Phase 19 使用 5.3.4 Docker server 執行；不是本 phase 的通過條件，不可提前標 PASS。

- [ ] **P15-V03** — 本機 Broker 上 Java requester↔Rust responder 與 Rust requester↔Java responder 可運作；no responder 與 late reply 產生相容 timeout。

## 6. 驗收與交接

5.5.1 multiple-callback regression 具備可執行 deterministic test 與 stress 證據。correlation task 與 registration 納入 release leak inventory。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 16](17-phase-16-namespace-security-operations.zh-tw.md).
