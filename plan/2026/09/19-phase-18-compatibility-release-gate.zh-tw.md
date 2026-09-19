# Plan: Phase 18 — 相容性、效能與發布門檻

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 18；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`19-phase-18-compatibility-release-gate.md`](19-phase-18-compatibility-release-gate.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 1. 目標與範圍

為每個對外相容性宣稱產生可稽核證據，驗證 performance/leak 邊界，準備真正 release，但不自動 publish 或 tag。

## 2. 前置條件與現況分析

Phase 0–17 已驗收，包含 optional API/security/trace/admin 的 owner 決策。重新以 Phase 0 artifact 驗證主計畫基準；前面全部測試仍必要，不以單一 end-to-end smoke 取代。

- 先完成: [Phase 17](18-phase-17-admin-query.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `Master-plan sections 18–30 and final acceptance checklist`
- `All pinned Java source and fixture provenance recorded by Phases 0–17`
- `5.5.1 request/reply, runtime-construction, blank-address, optimized-header encoding and batch-validation regressions`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md
docs/compatibility/REQUEST_CODES.md
docs/compatibility/KNOWN_DIFFERENCES.md
tests/differential/run.sh
tests/stress/
fuzz/
integration-tests/README.md
docs/release-checklist.md
docs/owner-validation.md
README.md
Cargo.toml
crates/*/Cargo.toml
```

supported matrix row 需依適用範圍連結實際 source、fixture/unit/integration/differential 證據；N/A 必須有理由。TODO/PARTIAL/unapproved gap 不可為發布而改標 COMPATIBLE。intentional difference 需 owner 核准並說明 user-visible impact。machine-readable result 記錄 client/reference version、source commit、profile、seed 與 normalization rule。必要 suite 缺失／略過時 release gate 失敗。performance result 是量測基準，不虛構 throughput 承諾。

## 4. 依序實作步驟

### 4.1 P18-01 (18.1 / 18.2) — 完成 evidence matrix 並執行全部測試層

稽核 API/class/request-code coverage 與 source provenance，包含 alias 與 intentionally unsupported operation。執行 pure unit、golden cross-codec、本機 Broker integration 與 Java/Rust differential。對 generated ID/offset 只正規化已記錄的非決定性，保留相等關係、duplicate count、queue order 與 commit 行為。加入 comparator 反例 self-test，避免過度正規化隱藏 regression。

### 4.2 P18-02 (18.1 Layer E) — 執行完整跨語言矩陣

執行 Java producer→Rust consumer、Rust producer→Java consumer、mixed clustering、mixed orderly、Rust transaction commit/rollback/check 與雙向 request/reply。使用 one-Broker/two-Broker profile，每個 scenario 之間受控 reset topic/group；驗證 delivery/offset/ownership invariant，不只看 process exit。納入已核准 Lite/POP/admin/security/trace profile，前置條件不足必須明確失敗。

### 4.3 P18-03 (18.3) — fuzz protocol trust boundary

以既有 Remoting/private-header/ext-field/message/property fixture 與 malformed variant 為 fuzz seed。需要時僅在隔離 fuzz harness 加 cargo-fuzz dependency，不影響 production。ordinary cargo test 加 deterministic bounded mutation test，記錄固定 budget/corpus 的可重現 fuzz-smoke command。驗證無 panic、checked length、有界 decompression 與 raw unknown code 保留。修正前先把每個 crash 保存為小型 regression fixture。

### 4.4 P18-04 (18.4 / 18.6) — 執行 deterministic race 與 stress/leak loop

保留 barrier/paused-time test，再 stress 大量 send、near-deadline response、reconnect、opaque wrap、concurrent runtime creation、start/stop race、slow listener、rebalance churn、offset persistence 與 duplicate reply。明確計數 owned task/connection/pending entry/queue work，不能只用 memory usage 證明。反覆 shutdown 後比對 baseline：所有 owner count 歸零，仍有其他使用者時則回文件指定 shared-runtime baseline。加入 fixed seed、有界 duration 的 integration stress target。

### 4.5 P18-05 (18.5) — 先量測再最佳化

以 release mode 分別 benchmark JSON/private header、frame encode/decode、message decode、send construction、pending completion、route lookup 與 allocation。重用 fixture input，記錄 CPU/toolchain/config、input size、iteration 與 latency/allocation observation。初期用 std::time 與 black_box 的小型 harness=false bench 即可，不需 benchmark framework。輸出仍須符合 Oracle。只有量測到瓶頸才最佳化，並重跑 differential；owner 未提供需求前不設定任意 throughput pass/fail threshold。

### 4.6 P18-06 — 讓 release CI 強制驗證實際範圍

release-branch CI 納入 fmt、clippy all targets/features、locked unit test、Java version pin、golden cross-decode、one/two-Broker integration、本機 TLS/auth、differential、fuzz smoke、leak stress 與 facade example。除了 all-features，還需測 default feature 與文件中的 supported combination。缺 Docker/auth profile 或略過 ignored test，必要 job 必須失敗，不可 green build。保存附 revision metadata 的證據並遮蔽 secret。

### 4.7 P18-07 — 準備 package、文件與明確發布核准

更新 README example、compatibility limit、timeout/duplicate-delivery semantics、supported Broker/security version 與 ownership/shutdown 指引。審查 Apache LICENSE/NOTICE provenance。真正 release 前再次確認全部 crates.io name；若被占用，停止並協調整體 prefix rename。只為實際 package 設 publish/version/license/repository metadata，workspace path 同時提供 registry version。確認 leaf-to-facade dependency 順序；cargo package/dry-run 必須能解析 registry 已存在的 dependency。不可宣稱 facade dry-run 能驗證未發布 path-only dependency。實際 publish/tag 是另外需 owner 核准的操作，絕非 automatic test。

### 4.8 P18-08 — 最後才執行 owner 部署驗收

建立 docs/owner-validation.md，寫明核准 target topology/profile、disposable test topic/group、out-of-band credential、command、expected result 與 cleanup。需要外部 infrastructure 的 real CA/SNI/mTLS/auth/namespace connectivity、allowed/denied operation、代表性 failover/load 與 graceful shutdown 由 owner 執行。未取得明確授權，不得修改 production topic/group 或注入 failure。記錄 date、revision、environment 與 evidence；必要 deployment check 未執行時持續阻擋 release approval。可在本機執行的 TLS/auth 與 Broker test 留在 P18-01/02/06，不可延到此處。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。共通命令、證據規則與 integration 前置設定見執行索引。

- [ ] **P18-V01** — 每個 advertised row 都是附證據的 COMPATIBLE，或已核准且說明影響的 INTENTIONAL_DIFFERENCE；必要 suite 無缺失或略過。
- [ ] **P18-V02** — golden/differential/cross-language/fuzz/stress 全通過，五項 5.5.1 regression 均存在，shutdown counter 回 baseline。
- [ ] **P18-V03** — published API example 只使用 rocketmq-classic 即可編譯；核准發布前已審查 package graph、licensing、docs 與 name availability。
- [ ] **P18-V04** — 必要 owner deployment 證據獨立記錄；不可用環境回報 BLOCKED 而非 PASS。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
mvn -f compat/java-reference/pom.xml verify
python3 tests/differential/compare.py --self-test
RMQ_NAMESRV_ADDR=127.0.0.1:9876 cargo test -p rocketmq-classic-integration --test broker --locked -- --ignored
RMQ_NAMESRV_ADDR=127.0.0.1:9876 cargo test -p rocketmq-classic-integration --test stress --locked -- --ignored
bash tests/differential/run.sh --all
cargo build -p rocketmq-classic --examples --release --locked
```

## 6. 驗收與交接

產出具證據的 release candidate checklist，不以可連線宣稱相容。owner 分別核准 external acceptance 與 publishing；必要 scope 未決或 gate 失敗時，專案維持 experimental。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
