# Plan: Phase 8 — Message binary decode 與 pull result

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 8；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`09-phase-08-message-decode-pull-results.md`](09-phase-08-message-decode-pull-results.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 1. 目標與範圍

解碼 Broker message record 並處理 pull result；不排程 consumption 或提交 offset。

## 2. 前置條件與現況分析

Phase 0–7 已驗收。重用 MessageExt、property/flag constant、compression backend 與 raw PullResultExt；現在新增 consumer crate，先只實作 result processing。

- 先完成: [Phase 7](08-phase-07-producer-core.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `MessageDecoder`
- `MessageExt`
- `MessageSysFlag`
- `PullResultExt`
- `PullAPIWrapper.processPullResult`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-common/src/message/decoder.rs
crates/rocketmq-classic-common/src/compression/
crates/rocketmq-classic-consumer/Cargo.toml
crates/rocketmq-classic-consumer/src/pull/result.rs
crates/rocketmq-classic-common/tests/message_decode.rs
fixtures/messages/
```

分開 decode_one(record, options, limits) 與 decode_many(body, options, limits)。每筆 record 受宣告 store size 限制且必須往前推進。options 保留 Java decodeReadBody/decodeDecompressBody 行為。limits 限制 input record、decompressed bytes 與 aggregate result size；invalid/oversized data 在無界配置前回 structured error。不可混淆 store message format、batch-producer format 與 Remoting framing。

## 4. 依序實作步驟

### 4.1 P8-01 — 解析前先凍結 record layout

產生 Broker/Java binary fixture 與 source-derived field-width/order 表，包含 magic/version variant、CRC、offset、timestamp、host flag、reconsume count、prepared offset、body、topic 與 property。以有界 cursor 與 checked arithmetic 解析。born/store host 分別支援 IPv4/IPv6、topic-length variant 與 concatenated record；拒絕 unknown magic、truncated field 與 impossible length。保留 CRC 表示；是否驗證 CRC 依 Java path/config，不虛構預設值。

### 4.2 P8-02 — 遵守 body 與 compression option

解碼 pinned client 使用的所有 compression type 與支援 flag；要求解壓時遇 unknown compression 必須回明確 error。以 byte cap 限制 streaming decompression，防止 decompression bomb。禁止讀 body 時直接跳過且不配置；禁止解壓時保留 compressed byte 與 flag。Java message ID 由 host/offset 組成，與 producer unique ID 分開進行 fixture test。

### 4.3 P8-03 — 將 decoding 接到 pull outcome

Phase 5 PullResultExt 完成 status mapping 後才處理。FOUND 解碼 message；NO_NEW_MSG/NO_MATCHED_MSG/OFFSET_ILLEGAL 即使沒有 body 仍保留 next/min/max offset 與 suggested broker。依 source 套用 Java wrapper enrichment，例如 broker/queue metadata 與 min/max property。只有 public result 需要時才保留 raw byte，否則處理後釋放以避免重複 buffer。

### 4.4 P8-04 — 只實作正確的 post-pull filtering

移植 PullAPIWrapper 實際執行的 tag filtering 與 class-filter 相關行為。SQL92 留在 Broker。不可嘗試在 Rust 執行任意 Java class-filter bytecode；public class-filter API 應列為範圍決策，不可假稱相容。本機過濾後 batch 可能為空，但 nextBeginOffset 仍須採 Broker 提供值。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。共通命令、證據規則與 integration 前置設定見執行索引。

- [ ] **P8-V01** — single/multiple record、全部支援的 magic/host/compression 與 body-option 組合，Java decoded JSON 與 Rust 相等。
- [ ] **P8-V02** — 在每個 byte 截斷 valid record；變造 size/magic/property 並測超量 decompression output：不得 panic、overread 或無界配置。
- [ ] **P8-V03** — FOUND/empty-filter/no-new/no-match/illegal-offset 保留 Java next-offset 與 suggested-broker 語意。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-common --test message_decode --locked
cargo test -p rocketmq-classic-consumer --locked
bash tests/differential/run.sh --phase 8
```

## 6. 驗收與交接

Phase 9 接收具明確 limit、無隱藏 offset mutation 的 decoded pull result。新增 binary fixture 與 normalization rule 必須加入 evidence manifest。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 9](10-phase-09-pull-offset-store.zh-tw.md).
