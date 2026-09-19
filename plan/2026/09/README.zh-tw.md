# RocketMQ Classic — 分階段實作索引

## 1. 範圍與已確認的儲存庫現況

本目錄將[主計畫](00-rocketmq-classic-plan.md) 展開為 **19 個依序實作的 phase 計畫（Phase 0–18）**，每份皆有英文主版本與等價繁體中文版本。檔名 `01`–`19` 是文件編號；`phase-00`–`phase-18` 保留原始 phase 編號。依指定主計畫沿用 2026 年 9 月目錄。

檢查儲存庫後，確認目前只有主計畫及 SVG 圖，尚無 Cargo workspace、Java harness、source code、dependency lockfile 或 test suite。子計畫的 source path、signature、test target 與 command 都是預定實作契約，不代表既有程式。本次規劃沒有下載／驗證 Java release，也沒有執行 client test。Phase 0 必須先驗證指定 tag `rocketmq-all-5.5.1`、commit `efa1be59d6fc747ad220f036c86030f05f0419f9` 與 Maven artifact `org.apache.rocketmq:rocketmq-client:5.5.1`，才能產生 authoritative fixture。若不一致，停止並請求審查，不可靜默改換基準。

主計畫保留整體範圍及 compatibility 定義。子計畫補足工作順序、具體實作與驗收證據，不授權靜默刪減功能。原始 Pn-NN task ID 皆保留，部分合併於同一實作步驟；新增 ID 補足 bootstrap/testing/lifecycle 缺口。Phase 16–18 的 task ID 對應主計畫編號 feature section。

## 2. 依序閱讀與實作

英文為 authoritative 主版本；繁中配對檔具有相同要求。完成目前 phase 門檻才開始下一階段；Push 的明確累積驗收例外見第 3 節。

| 順序 | Phase | 英文主版本 | 繁體中文 |
| ---: | ---: | --- | --- |
| 01 | 0 | [Freeze the reference and test oracle](01-phase-00-reference-oracle.md) | [凍結參考基準與測試 Oracle](01-phase-00-reference-oracle.zh-tw.md) |
| 02 | 1 | [Common types, constants and protocol models](02-phase-01-common-protocol-models.md) | [共用型別、常數與協定模型](02-phase-01-common-protocol-models.zh-tw.md) |
| 03 | 2 | [Remoting frame codec](03-phase-02-remoting-codec.md) | [Remoting frame codec](03-phase-02-remoting-codec.zh-tw.md) |
| 04 | 3 | [TCP/TLS remoting transport](04-phase-03-remoting-transport.md) | [TCP/TLS Remoting transport](04-phase-03-remoting-transport.zh-tw.md) |
| 05 | 4 | [Typed request headers and protocol bodies](05-phase-04-typed-headers-bodies.md) | [Typed request header 與 protocol body](05-phase-04-typed-headers-bodies.zh-tw.md) |
| 06 | 5 | [MQClientApi typed RPC facade](06-phase-05-typed-rpc-api.md) | [MQClientApi typed RPC facade](06-phase-05-typed-rpc-api.zh-tw.md) |
| 07 | 6 | [Client runtime, routing and heartbeat](07-phase-06-runtime-routing-heartbeat.md) | [Client runtime、routing 與 heartbeat](07-phase-06-runtime-routing-heartbeat.zh-tw.md) |
| 08 | 7 | [Producer core](08-phase-07-producer-core.md) | [Producer 核心](08-phase-07-producer-core.zh-tw.md) |
| 09 | 8 | [Message binary decode and pull results](09-phase-08-message-decode-pull-results.md) | [Message binary decode 與 pull result](09-phase-08-message-decode-pull-results.zh-tw.md) |
| 10 | 9 | [Pull consumer and offset stores](10-phase-09-pull-offset-store.md) | [Pull consumer 與 offset store](10-phase-09-pull-offset-store.zh-tw.md) |
| 11 | 10 | [Push consumer core pipeline](11-phase-10-push-consumer-pipeline.md) | [Push consumer 核心 pipeline](11-phase-10-push-consumer-pipeline.zh-tw.md) |
| 12 | 11 | [Rebalance and assignment lifecycle](12-phase-11-rebalance.md) | [Rebalance 與 assignment lifecycle](12-phase-11-rebalance.zh-tw.md) |
| 13 | 12 | [Orderly consumption and queue locks](13-phase-12-orderly-consumption.md) | [Orderly consumption 與 queue lock](13-phase-12-orderly-consumption.zh-tw.md) |
| 14 | 13 | [Lite Pull, assignment and POP APIs](14-phase-13-lite-assignment-pop.md) | [Lite Pull、assignment 與 POP API](14-phase-13-lite-assignment-pop.zh-tw.md) |
| 15 | 14 | [Transaction producer](15-phase-14-transaction-producer.md) | [Transaction producer](15-phase-14-transaction-producer.zh-tw.md) |
| 16 | 15 | [Request/reply and exactly-once completion](16-phase-15-request-reply.md) | [Request/reply 與單次完成語意](16-phase-15-request-reply.zh-tw.md) |
| 17 | 16 | [Namespace, hooks, security and operational features](17-phase-16-namespace-security-operations.md) | [Namespace、hook、安全與維運功能](17-phase-16-namespace-security-operations.zh-tw.md) |
| 18 | 17 | [Admin/query public surface](18-phase-17-admin-query.md) | [Admin/query public surface](18-phase-17-admin-query.zh-tw.md) |
| 19 | 18 | [Compatibility, performance and release gate](19-phase-18-compatibility-release-gate.md) | [相容性、效能與發布門檻](19-phase-18-compatibility-release-gate.zh-tw.md) |

## 3. 依序實作所需的依賴釐清

1. **Bootstrap：**Phase 0 只建立 Java Oracle/fixture/environment。Phase 1 建 common/protocol workspace crate；Phase 2 加 remoting、Phase 5 加 core/integration test、Phase 7 加 producer/facade、Phase 8 加 consumer。不先建未來的空 crate。
2. **禁止 crate cycle：**HeaderMap、ProtocolError 與 header conversion trait 從 Phase 2 起放在 protocol。remoting 使用它們；Phase 4 header 不可 import remoting。dependency direction 維持 common ← protocol ← remoting ← core ← producer/consumer ← facade。
3. **Model 不等於 supported API：**Phase 4 model 與 Phase 5 lower-level Lite/POP call 不代表已有 public consumer。Phase 13 才分類並完成核准 public surface。
4. **Message decode：**Phase 5 回 raw pull byte；Phase 8 提供唯一有界 decoder。不得為了解鎖早期 RPC test 而重複 parsing。
5. **Offset 依賴：**Phase 9 實作 Phase 11 start-offset policy 所需 search/min/max/earliest-time primitive；Phase 17 只補充／重用核准的 public admin exposure。
6. **Push/rebalance 缺口：**Phase 10 經 private assignment 接點與 test-only fixed assignment 測 pipeline。Phase 11 提供真正 startup/periodic/notification rebalance，完成累積 public Push gate。不可發布假的 fixed-assignment production Push。
7. **Orderly 依賴：**Phase 11 準備 lock-aware lifecycle 接點；Phase 12 啟用 Broker locking/renewal/expiry 與 orderly callback。此前 orderly compatibility 維持 PARTIAL。
8. **Release label 不是 phase gate：**namespace 雖出現在主計畫 v0.1 matrix，實作在 Phase 16。Phase 7 Producer milestone 不可宣稱該 release column 全部完成。TLS 從 Phase 3 開始，Phase 16 補齊。所有 advertised release column 都需要對應 feature evidence。
9. **Optional scope：**Lite/POP exposure、trace、target auth topology 與 public admin exclusion 只有 owner 可核准。未決項目保持 TODO，不得虛構 INTENTIONAL_DIFFERENCE 核准。
10. **外部環境：**可在本機執行的 Broker、TLS/auth 與 two-Broker check 保留於各 feature phase。只有 environment-dependent target-deployment acceptance 留待 owner 於 Phase 18 P18-08 執行。

## 4. 共通實作與證據規則

### 4.1 閱讀精確 Java source

使用 `compat/java-reference/README.md` 記錄且已驗證的 checkout/source artifact，不可用 `develop`。各 phase 的簡稱 class 依下列位置查找：

- `common/src/main/java/org/apache/rocketmq/common/`：message、flag、namespace 與 common value；
- `remoting/src/main/java/org/apache/rocketmq/remoting/`：framing、code、header 與 protocol body；
- `client/src/main/java/org/apache/rocketmq/client/`：API、runtime、Producer、Consumer 與 allocation behavior；
- 額外 auth/ACL/trace module 只依經驗證的 5.5.1 source tree 實際位置查找。

移植前，在 compatibility matrix 記錄實際 file、method 與 call site。同時檢查 header validation 以及 construction/response interpretation。數值、預設值、field order、requiredness 與 serialization limit 都由證據取得，不猜測、不採無關 SDK 行為。若 verified tag 的 class/path 不同，記錄實際 mapping；行為不明時停止並請求審查。

### 4.2 一次完成一個可審查工作

1. 閱讀 phase 前置條件、contract 與連結的 Java method。
2. 先加指定 failing test 或 Java fixture，寫行為前先連結 matrix。
3. 在列出的 owning file 做最小修改，優先使用 Rust standard collection、已安裝 dependency 與既有 lower layer。dependency 只在 phase 實際用到時新增。
4. 執行 focused check 與共通 gate，保持全部既有 suite 通過。
5. 依影響更新 `docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md`、`REQUEST_CODES.md`、`KNOWN_DIFFERENCES.md`，在該 phase 的實作 handoff/log 記錄 command/result。不新增額外 logging service 或 framework。
6. 只依與 owner 約定的 implementation workflow 提交可審查工作。本次文件任務不實作、不 commit/tag/publish 軟體。

production input 路徑禁止 `unwrap`/`expect`、unchecked length allocation、sync guard 跨 await、無 owner task、credential logging 或 silent offset reset。保留 unknown code/ext field。每次 retry 消耗同一 original monotonic deadline。每個 stateful task/registration 有 identity-aware rollback、cancellation 與 join owner。user callback 在 shared lock/reader 外執行。刻意的範圍上限以 `ponytail:` code comment 記錄升級條件；不能用來豁免 security、input validation 或必要 compatibility。

使用 Rust built-in test 與小型 table-driven fixture，不需要新 testing framework。transport failure simulation 重用 loopback peer。trait 應用於真正多 implementation（例如 local/remote OffsetStore），不是為了測試而包裝單一 concrete service。

### 4.3 證據與測試命令

phase-local command **需於實作時建立並執行**，目前只有文件的 tree 還不能執行。未以斜線開頭的 path 都相對 repository root。

從 Phase 1 起，每個 phase 完成後執行本機共通 gate：

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
```

從 Phase 0 起，Java/fixture 變更另外執行：

```bash
mvn -f compat/java-reference/pom.xml verify
python3 tests/differential/compare.py --self-test
```

Phase 0 建 local profile 與 deterministic setup；Phase 5 加 integration package 與 differential runner。Broker command 前，先啟動 required profile、設定 `RMQ_NAMESRV_ADDR`，並執行文件指定 setup。runner 必須讀取 scenario 的 profile requirement，缺前置條件時失敗，隔離 scenario topic/group state；不可誤改外部 cluster。

```bash
docker compose -f integration-tests/compose.yaml up -d
bash integration-tests/setup.sh
export RMQ_NAMESRV_ADDR=127.0.0.1:9876
cargo test -p rocketmq-classic-integration --test broker --locked -- --ignored
bash tests/differential/run.sh --phase 5
```

後續 suite 替換 phase number；Phase 18 加入 `--all`。特殊 two-Broker/TLS/auth profile 需使用其記錄的 address/config，不可盲用上方 basic address。Phase 2 後 Maven verify 必須驗證 Rust 產生的 protocol fixture；只有同語言 round trip 不足。golden metadata 依主計畫附錄 B，包含 SHA-256/provenance。不得正規化掉 protocol error、ordering、retry count、duplicate-delivery relationship 或 safe offset advancement。

## 5. 交接與停止條件

phase handoff 列出 changed file、evidence/test ID、exact command/result、actual profile/version、remaining status row 與 next phase。assertion 必須可執行且可重現，包含 startup failure 與 terminal-owner race。一個成功 connection 不算 compatibility gate。

verified baseline 不符、field width 窄化、Java requiredness/behavior 不明、需更動已完成 lower-layer contract、無法安全轉移 ownership，或 differential result 不一致且原因不明時，停止並請求審查。先加入 failing fixture；lower-layer contract 變更遵循主計畫 ADR 規則，不靜默重寫先前 phase。尚未執行的 deployment-only check 維持 BLOCKED，不可標 PASS。

## 6. 受影響 phase 前需由 owner 決定的事項

- **Phase 13：**哪些 Lite Pull/public POP API 為 v1.0 必要項，哪些可只提供 lower-level？見 [Phase 13 第 7 節](14-phase-13-lite-assignment-pop.zh-tw.md#7-待決問題必要決策)。
- **Phase 16：**需要哪一種 5.5.1 auth/ACL mode、CA/SNI/mTLS/proxy topology？message trace parity 是否必要？見 [Phase 16 第 7 節](17-phase-16-namespace-security-operations.zh-tw.md#7-待決問題必要決策)。
- **Phase 17：**首個 stable release 必須公開哪些 MQAdmin operation？見 [Phase 17 第 7 節](18-phase-17-admin-query.zh-tw.md#7-待決問題必要決策)。

這些決策不阻擋閱讀或 Phase 0–12 baseline 工作，但會阻擋相依 public API 與最終 compatibility claim。建立這些計畫不代表已實作或測試通過。
