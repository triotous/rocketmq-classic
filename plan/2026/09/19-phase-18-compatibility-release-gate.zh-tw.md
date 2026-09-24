# Plan: Phase 18 — 完成 unit parity 並準備最終驗證

> 主計畫：[Master plan](00-rocketmq-classic-plan.md)，Phase 18；執行索引：[README.zh-tw.md](README.zh-tw.md)。
> 英文為主版本：[English](19-phase-18-compatibility-release-gate.md)。此為計畫，不是已完成的測試證據。保留既有檔名以維持連結；最終發布驗收移至 Phase 19。

## 1. 目標與範圍

以 **unit test 為主要門檻**，補齊 Java client **5.5.1** source／設定／功能／控制流程覆蓋，輔以離線 golden/differential、deterministic race、fuzz-smoke 與 microbenchmark。不要求 live Broker 或 Docker。只實作 client；server **V5_3_4（5.3.4）**是 Phase 19 production interoperability 基準。

全部 Java client 設定及功能皆必要，default、validation、error、callback、retry、routing、offset 與 lifecycle 語意必須一致。Rust 專有公開設定／功能、缺少 Java 能力或刻意行為差異都不能通過。Rust 慣用內部實作不代表可以改變可觀察流程。

## 2. 前置條件與現況分析

完成 [Phase 17](18-phase-17-admin-query.zh-tw.md) 及全部 Phase 0–17 unit gate。目前 repository 只有計畫；以下路徑及命令均為預定實作。再次核對 Phase 0 Java artifact/source provenance，不以 server 版本替換。

檢查全部映射的 Java class、method、overload、inherited/deprecated API、client 設定途徑與 call site。涵蓋 5.5.1 request/reply、runtime construction、blank-address、header encoding 與 batch-validation regression。此時發現的清單缺口必須在 Phase 19 前實作並測試，即使前面計畫未具名列出。

## 3. 目標檔案與實作契約

- `docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md`：雙向 feature/settings/flow coverage 及證據。
- `docs/compatibility/REQUEST_CODES.md`：全部 client operation 與 inbound processor，不只是數值常數。
- `docs/compatibility/KNOWN_DIFFERENCES.md`：未解決 blocker、server 限制與內部表示註記，不是功能排除豁免。
- `tests/differential/scenarios.yaml`、`tests/differential/compare.py`：完整 scenario 清單及離線比較。
- `fuzz/`、`tests/stress/`、crate-local tests/benches：有界離線 harness。
- `docs/release-checklist.md`、`README.md`、workspace/crate manifest：僅準備，不發布。

分開 implementation status 與 live-test status。`UNIT_VERIFIED` 不代表最終 `COMPATIBLE`；缺少 live 證據維持 `PENDING_PHASE_19`。Java 5.5.1 與 server 5.3.4 使用不同 metadata 欄位。server 限制不能刪除 Rust 的 Java 功能。

## 4. 依序實作步驟

### 4.1 P18-01 — 稽核完整雙向 parity

每個 Java feature/method/configuration 記錄 source path、method/call site、type/default/unit/range、validation、控制流程分支、Rust 實作與 unit ID。反向稽核每個 Rust public API/configuration，包含 environment variable、feature flag、limit 及 test injection point：不可有無法對照的公開設定或功能。移植缺少的 Java 能力，不可縮小稽核範圍。private safety/ownership 機制不可改變合法輸入結果、Java configuration 語意或 public flow 行為。

### 4.2 P18-02 — 補齊離線 Oracle 與 scenario 覆蓋

執行全部 unit、Java golden cross-decode 及離線 differential case。比較 default、合法／非法輸入、呼叫順序、callback count、retry decision 及 offset/ownership transition。只正規化已證實的非決定性；comparator 反例必須偵測 status、ordering、retry count 及 duplicate/offset relationship 差異。將前面全部 live handoff ID 收集到 `scenarios.yaml`，包含 source phase、execution_phase=19、profile、input、assertion、cleanup 及 Java/Rust direction。缺 case 時 inventory check 失敗。

### 4.3 P18-03 — Fuzz protocol trust boundary

使用既有 fixture 作為 Remoting/private-header/ext-field/message/property fuzz seed。一般 `cargo test` 納入 deterministic bounded mutation regression；isolated fuzz harness 補上固定 budget、可重現的 smoke run。驗證 checked length、無 panic／無界 decompression 及 unknown raw code 保留。每個失敗先存 fixture 再修正。不可新增 Rust 專有公開 limit knob；公開 limit 由 Java 設定推導。

### 4.4 P18-04 — 證明 deterministic race 與 leak 行為

使用 paused time、barrier、scripted peer 與暫存 offset file，測 near-deadline response、reconnect、opaque wrap、concurrent runtime construction、start/stop、slow callback、assignment churn 及 duplicate reply。計數 task、socket、pending entry 與 queue work，不只看記憶體。反覆 shutdown 回到零或文件指定 shared-runtime baseline。需要 server 的 sustained load 屬 Phase 19。

### 4.5 P18-05 — 量測離線效能

以固定 fixture input 在 release mode 量測 codec/message decode、send construction、pending completion、route lookup 與 allocation。記錄 hardware/toolchain、input size、iteration、latency 及 allocation。先用小型 `harness=false` bench 與 `black_box`，再考慮 framework。只最佳化已量測瓶頸，並重跑 Oracle。不可虛構 throughput 驗收門檻或額外公開 tuning control。

### 4.6 P18-06 — 強制 server-free CI

要求 fmt、clippy、locked unit test、Java version pin、golden cross-decode、comparator self-test、deterministic race/leak、fuzz smoke 及 example compilation。測 default/all-features 及支援設定組合。Phase 0–18 CI 在沒有 Docker daemon 或 server address 時也必須通過。不可將略過 live test 當成成功證據；Phase 19 加上必要 live CI job。

### 4.7 P18-07 — 準備 package 與文件

記錄 Java 等價 public usage、timeout/duplicate-delivery 與 shutdown 語意、client 5.5.1/server 5.3.4 基準及待執行 live 證據。審查 Apache LICENSE/NOTICE 與 package dependency direction。準備 package metadata 及 leaf-to-facade 發布順序；packaging check 不可宣稱未發布的 registry dependency 已能解析。實際 registry name availability、publish 與 tag 需 Phase 19 後另外授權。

### 4.8 P18-08 — 交接最終驗證需求

驗證完整 Phase 19 scenario manifest，列出 deployment-only 候選檢查並說明無法本地執行的理由。本階段不執行 production check。可由本機 container 執行的 TLS/auth、restart、failover、跨語言與 stress，仍是 Phase 19 自動測試義務，不是 manual exception。

## 5. 驗證

- [ ] **P18-V01** — 每個 Java client 設定／功能／流程均映射已實作 Rust 與 unit 證據；反向稽核無額外公開能力或設定。
- [ ] **P18-V02** — unit/golden/offline differential/race/fuzz suite 通過，包含全部五項 5.5.1 regression；shutdown counter 回 baseline。
- [ ] **P18-V03** — facade example 可編譯，package graph/provenance/docs 已審查，未宣稱最終發布驗收。
- [ ] **P18-V04** — 全部 live handoff ID 已登錄 Phase 19；unit-only CI 在無 Docker 或執行中 Broker 時通過。

### 5.1 實作完成後的命令

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo test --workspace --all-features --locked
mvn -f compat/java-reference/pom.xml verify
python3 tests/differential/compare.py --self-test
cargo build -p rocketmq-classic --examples --release --locked
```

fuzz/microbenchmark target 存在後記錄精確有界命令。目前只有文件的 tree 無法執行這些命令。

## 6. 驗收與交接

全部 Java client 實作及 unit coverage 完整，不含透過核准豁免的缺口或額外公開設定／功能。live 證據仍待執行；本階段不可授權相容性宣稱或發布。交接包含 revision、source mapping、test ID/result、fixture hash 及完整 live/manual manifest。

下一階段：[Phase 19 — Docker 本地驗證與最終驗收](20-phase-19-docker-local-validation.zh-tw.md)。
