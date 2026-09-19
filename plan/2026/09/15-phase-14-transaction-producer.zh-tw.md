# Plan: Phase 14 — Transaction producer

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 14；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`15-phase-14-transaction-producer.md`](15-phase-14-transaction-producer.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 1. 目標與範圍

使用既有 Producer 與 inbound transport 實作 half-message、local decision 與 Broker transaction-check。half send 成功不代表已 commit 可見。

## 2. 前置條件與現況分析

Phase 7–8 producer/decoder 與 Phase 6 processor registry 已驗收；Phase 13 scope gate 已關閉。重用 Phase 4/5 end/check transaction header 與 RPC。

- 先完成: [Phase 13](14-phase-13-lite-assignment-pop.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `TransactionMQProducer`
- `DefaultMQProducerImpl.sendMessageInTransaction`
- `TransactionListener`
- `LocalTransactionState`
- `EndTransactionRequestHeader`
- `ClientRemotingProcessor.checkTransactionState`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-producer/src/transaction.rs
crates/rocketmq-classic-producer/src/producer.rs
crates/rocketmq-classic-core/src/runtime/runtime.rs
crates/rocketmq-classic-producer/tests/transaction.rs
examples/transaction_producer.rs
```

提供獨立 Commit/Rollback/Unknown decision，以及分開的 local-execute/check callback。callback 在有界且 Producer-owned 工作中執行，不在 socket reader。transaction context 保留精確 half-send result、original message 與 producer identity。不可由錯誤 message ID 製造 queue/commit-log offset。error/panic/cancellation 不得隱含 Commit；明確記錄 source-compatible Unknown/rollback policy。

## 4. 依序實作步驟

### 4.1 P14-01 / P14-02 — 新增 validated API 與 half-send construction

start 時要求 local/check callback，依 producer group 註冊，拒絕重複 listener ownership。以 Java fixture 驗證 transactional-message restriction。複製 message 以設定 prepared property/sysFlag，使用正常 route/compression/retry；不修改 caller message。SendResult 與 transaction result 保留 transaction/unique ID。

### 4.2 P14-03 — 在正確時點執行 local transaction

依 Java 定義各 half-send status：只在允許路徑執行 user callback，確定 half-send 失敗後不得執行。將 callback result/error/panic 轉為明確 decision，保留通知 Broker 所需 result。half-send 後 caller cancellation 不可抹去 Broker recovery：記錄 Unknown/check recovery，已接受 owned task 保留至安全 terminal state。

### 4.3 P14-04 — 依精確資料建構 end-transaction command

由 source fixture 映射 commit/rollback/unknown flag 與 producerGroup/tranStateTableOffset/commitLogOffset/fromTransactionCheck/msgId/transactionId。以正確 helper 解碼 offsetMsgId 與 unique ID。必要時採 Java oneway；write 成功不是 Broker commit acknowledgement。end command transport failure 時仍保留 local decision，使後續 check 一致。

### 4.4 P14-05 / P14-06 — 啟用 Broker check 並管理 shutdown

以既有有界 codec 解碼 inbound header/message，找到正確 producer listener，送入有界 check callback queue。missing/unknown group 行為由 Java 確認。END_TRANSACTION 使用 fromTransactionCheck 與 inbound check offset。重複 check 可合法再次呼叫 user callback；要求 business check 冪等，不建立無界永久 dedup map。shutdown 停止 admission，依文件化有界 policy 處理 queued/in-flight check，只解除相符 generation。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。共通命令、證據規則與 integration 前置設定見執行索引。

- [ ] **P14-V01** — fixture 驗證 half property/flag，以及 commit、rollback、unknown 與 non-OK send status 的全部 end/check field。
- [ ] **P14-V02** — error/panic/cancellation、repeated check、slow callback 與 shutdown race 不產生意外 commit 或 reader blockage。
- [ ] **P14-V03** — 本機 Broker commit 可消費、rollback 不可見、Unknown 透過真正 Broker check 解決；以有界 configured check interval 比對 Java 行為。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-producer --test transaction --locked
bash tests/differential/run.sh --phase 14
```

## 6. 驗收與交接

matrix 連結 cross-language visibility 與 shutdown 證據。不可將 END_TRANSACTION transport 成功等同已證明 transaction compatibility。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 15](16-phase-15-request-reply.zh-tw.md).
