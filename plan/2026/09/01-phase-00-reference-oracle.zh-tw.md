# Plan: Phase 0 — 凍結參考基準與測試 Oracle

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 0；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`01-phase-00-reference-oracle.md`](01-phase-00-reference-oracle.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 1. 目標與範圍

在實作 Rust 協定前，建立可執行的 Java Oracle、可重現的本機 Broker 環境及證據清單。本階段建立驗證基礎，不實作 Rust client。

## 2. 前置條件與現況分析

儲存庫目前只有主計畫與 SVG 圖，尚無 Cargo workspace、Java harness、fixtures 或測試。主計畫的 release/tag/commit 是指定基準，不是已獨立驗證的證據。

- 先完成: [執行索引](README.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `RemotingCommand`
- `RocketMQSerializable`
- `MQClientAPIImpl`
- `MQClientInstance`
- `DefaultMQProducerImpl`
- `PullAPIWrapper`
- `ProcessQueue`
- `RebalanceImpl`
- `RemoteBrokerOffsetStore`
- `LocalFileOffsetStore`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
compat/java-reference/pom.xml
compat/java-reference/README.md
compat/java-reference/src/main/java/compat/Main.java
fixtures/README.md
fixtures/remoting/
fixtures/messages/
integration-tests/compose.yaml
integration-tests/setup.sh
integration-tests/README.md
tests/differential/scenarios.yaml
tests/differential/compare.py
docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md
docs/compatibility/REQUEST_CODES.md
docs/compatibility/KNOWN_DIFFERENCES.md
```

Oracle CLI 提供 `fixtures --output <dir>`、`decode --input <file>`、`scenario --id <id> --output <file>` 與 `self-test`。使用 Maven exec 執行 `compat.Main`；每個情境輸出一份 JSON。`scenarios.yaml` 使用 JSON 相容的 YAML 文件，讓 Python 標準函式庫不必加入 YAML dependency 即可讀取。每筆記錄 ID、所需 profile、負責 phase、受控輸入及精確的正規化規則。Python comparator 比較物件時忽略 key 順序，陣列則維持順序，除非特定欄位明確屬於集合。不得正規化掉錯誤碼、queue 分配、重試次數、順序或 offset 關係。

## 4. 依序實作步驟

### 4.1 P0-00 — 產生證據前先驗證基準

將 `rocketmq-all-5.5.1` 解析為 peeled commit，與 `efa1be59d6fc747ad220f036c86030f05f0419f9` 比對。解析 Maven `org.apache.rocketmq:rocketmq-client:5.5.1` 及對應 source artifacts。在 harness README 記錄 artifact SHA-256、tag/commit、JDK/Maven 版本及 source paths。若無法取得，不得改用 `develop`、其他 artifact 版本或未驗證的 Docker image；停止協定實作並請 owner 審查。

### 4.2 P0-01 — 建立證據與範圍清單

依主計畫建立完整 matrix 欄位，包含 source/Rust mapping、request code、golden/unit/integration/differential 識別資訊與狀態值。盤點 pinned source 的 public client API 及所有 RequestCode/ResponseCode 數值；區分「已有常數」與「已實作操作」。加入 source class/method 連結與負責 phase。實作項目一律從 TODO 開始；納入 transaction、reply、Lite/POP、admin、namespace、hooks 與 security。選配範圍未決時記為 pending，不可標為 COMPATIBLE。

### 4.3 P0-02 — 實作協定 fixture 輸入輸出

使用 Java public API；若 release 必須存取 package scope，採用有文件說明的最小同 package adapter。盡可能明確設定 opaque、timestamps 與 properties，產生 JSON 與 ROCKETMQ commands。輸出完整 raw frame、解碼後 semantic JSON、body length 與 body SHA-256。decoder 接受兩種 serialization，遇到畸形輸入必須失敗。將 self-test 接入 Maven verify，不可依賴預設關閉的 Java assertions。

### 4.4 P0-03 — 建立可重現情境與 comparator

登錄主計畫所有 Phase 0 scenario ID，並補上 heartbeat、offset update 及 header 反例。先實作 route/send/pull/offset/lock；採用隔離的 topic/group 及明確初始 offset。純 allocation 與 header 情境不需要 Broker。comparator self-test 必須證明非決定性 ID 可正規化，但 status 或 queue 順序改變仍會失敗。未來 scenario ID 可先登錄，但未實作時必須明示，不可回報成功。

### 4.5 P0-04 — 提供本機 cluster 與可重現 setup

以已驗證的 5.5.1 distribution/image 建立基本 NameServer + Broker Compose profile；記錄 checksum 或 image digest，以及映射與 advertised address。readiness 必須使用實際 route/admin 操作，不可固定 sleep。setup.sh 在隔離測試 cluster 以冪等方式建立固定 queue/topic/group，說明 reset/cleanup，且不得操作外部資料。加入 two-Broker 與本機 TLS CA profile；確認 pinned auth 路徑後才建立 auth profile。TLS profile 不可關閉憑證驗證。

### 4.6 P0-05 — 提交可重現證據

使用附錄 B 的 fixture 命名。每個 .bin 配一份 .json，包含 source version、scenario、serialization、request code、normalized fields、payload SHA-256 及 creation command；raw capture 與 normalized result 分開保存。本階段必須具備 route、heartbeat、send、pull、offset query/update 及 lock/unlock families。後續 phase 實作功能前先擴充清單。將 dependency/version check 接入 Maven verify，並記錄 fixture 重建方式及 Java 授權／NOTICE 來源。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。共通命令、證據規則與 integration 前置設定見執行索引。

- [ ] **P0-V01** — 產生 fixtures 兩次；metadata hash 符合 raw file，正規化 semantic result 一致。
- [ ] **P0-V02** — Java 可解碼自身產生的兩種 codec；畸形 fixture 失敗；comparator self-test 能找出有意義差異。
- [ ] **P0-V03** — 基本本機 cluster：Java producer/consumer、route、heartbeat、pull、offset 與 lock 操作產生非空的 machine-readable 證據。
- [ ] **P0-V04** — 修改 Maven client 版本會使驗證失敗；缺少前置條件的 scenario 不得回報 PASS。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
mvn -f compat/java-reference/pom.xml verify
mvn -f compat/java-reference/pom.xml exec:java -Dexec.mainClass=compat.Main -Dexec.args="fixtures --output target/java-fixtures"
python3 tests/differential/compare.py --self-test
docker compose -f integration-tests/compose.yaml up -d
bash integration-tests/setup.sh
```

## 6. 驗收與交接

一併提交已驗證基準、fixture manifest、可執行本機 cluster 步驟與 TODO matrix。必要 Oracle 證據可重現後才能開始 Phase 1；僅能在部署環境進行的測試收集至 Phase 18，不可取代本機門檻。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 1](02-phase-01-common-protocol-models.zh-tw.md).
