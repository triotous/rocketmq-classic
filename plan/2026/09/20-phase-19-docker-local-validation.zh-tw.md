# Plan: Phase 19 — Docker 本地驗證與最終驗收

> 主計畫：[Master plan](00-rocketmq-classic-plan.md)；索引：[README.zh-tw.md](README.zh-tw.md)。
> 英文為主版本：[English](20-phase-19-docker-local-validation.md)。檔名 `20` 是文件編號，本計畫為 Phase 19。以下實作路徑／命令皆為規劃，不代表已有 infrastructure 或完成結果。

## 1. 目標與範圍

建立必要 Docker 測試環境，執行截至 Phase 18 累積的**全部可本地執行測試**。以 **Java client 5.5.1** 為行為／source Oracle，**RocketMQ server V5_3_4（5.3.4）**為 production 基準。只實作／修正 client；使用未修改的 Apache server distribution，不以 Rust server 或 server patch 掩蓋不相容。

完整 parity 包含全部 Java client 設定、default、功能、控制流程及 error；不可新增 Rust 公開設定／功能，也不可省略 Java 能力。Docker/test-runner option 屬測試 infrastructure，不是 client 設定。非本地驗證以獨立 owner 手動清單追蹤，不是略過可本地重現測試的理由。

## 2. 前置條件與現況分析

完成 [Phase 18](19-phase-18-compatibility-release-gate.zh-tw.md)。重用 source matrix、離線 Java harness、fixture 及 `tests/differential/scenarios.yaml`，不建立平行 Oracle。目前 repository 尚無實際 workspace 或 Docker 環境。5.3.4 image name/digest、runtime architecture 及 server 支援的 profile 必須於 P19-01 驗證，不在此猜測。

審查 Java 5.5.1 `MQClientAPIImpl`、producer/consumer/transaction/reply 流程及全部 source mapping。只為環境設定及解釋 server response 而查閱 server 5.3.4 source/configuration；不可替換 client Oracle，也不授權實作 server。

## 3. 目標檔案與必要環境

- `integration-tests/compose.yaml`、`integration-tests/config/`、`integration-tests/setup.sh`、`integration-tests/cleanup.sh`、`integration-tests/README.md`：隔離 Docker profile、readiness、seed data 與 teardown。
- `integration-tests/Cargo.toml`、`integration-tests/tests/broker.rs`、`integration-tests/tests/stress.rs`：不發布的 `rocketmq-classic-integration` workspace package。
- `tests/differential/run.sh`、`tests/differential/scenarios.yaml`、`tests/differential/compare.py`：依 profile 執行完整 Java/Rust 測試與比較。
- `integration-tests/run-local.sh`：全部本地 suite 與 evidence inventory 的單一入口。
- `docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md`、`docs/compatibility/SERVER_5_3_4_RESULTS.md`、`docs/release-checklist.md`、`docs/owner-validation.md`：分開 client parity、server result、manual evidence 及 release decision。

必要 profile：基本 NameServer + Broker、供 route/retry/query/rebalance 使用的 two-Broker、暫存 CA 與反例 certificate 的 TLS、source 確認的 auth/ACL，以及實際 Java 路徑所需 trace/POP/transaction/filter-server 或 failover topology。只新增清單中 scenario 需要的 profile。兩台獨立 Broker 不能證明 replicated HA failover；scenario 需要時設定真正 source 支援的 HA topology。任何 profile 都不可靜默替換 5.3.4 基準。

## 4. 依序實作步驟

### 4.1 P19-01 — 固定並驗證 Docker artifact

確認官方 5.3.4 server image，或由已驗證 Apache 5.3.4 binary distribution 建立本地 image。記錄 immutable digest 或 distribution SHA-256、source version、image build input、JDK、OS/architecture 及實際 startup version。Java client artifact/source 獨立固定為 5.5.1 並記錄 checksum。Java/Rust runner toolchain 與 dependency 分開固定；image 內建 client 不可靜默替換 Oracle。image/platform 或 artifact 不可用時明確失敗。

### 4.2 P19-02 — 建立隔離環境與可重現 setup

使用專屬 Compose project/network/volume，對外 port 綁定 loopback。設定兩種 test runner 都能到達的 Broker advertised address；container-local 與 host-mapped address 不可混用。優先讓兩種語言 runner 使用相同 Compose network；支援 host 執行時記錄其位址。以有界 polling 驗證 route lookup 及實際 send/pull readiness，不用固定 sleep。

冪等建立 scenario 專屬 topic、group、queue count、retry/DLQ resource、ACL identity 與可控 initial offset。Java/Rust 在等價 reset state 執行；只有 assertion 是 mixed-client interaction 時才共同執行。任何 mutation 前要求 test-environment identity marker 及明確 profile。cleanup 只操作本 Compose project 資源；teardown 前保存 log/result，不刪 external/production data。產生暫存 TLS key 與 synthetic credential，不提交 production secret，也不關閉 verification 讓測試通過。

### 4.3 P19-03 — 建立缺項即失敗的 runner 並移入 live 義務

收集前面全部 `P*-V*` live handoff 及 P18-02/08 要求；保留 source ID，未涵蓋的 Java 功能／設定另加 ID。manifest 每列記錄 source phase、execution_phase=19、profile/server version、Java/Rust command、isolated input、有界 timeout/seed、invariant、normalization 及 cleanup。以 unit test 驗證 manifest coverage、version check、missing scenario 與 comparator failure。

依 manifest 實作 `run.sh --phase N` 與 `--all`。Broker test 可在一般 server-free cargo run 中 ignored，但 `run-local.sh --all` 必須明確執行全部必要 ignored target，並比對 executed ID 與 expected ID。缺 Docker/profile/auth prerequisite、nonzero exit、略過 scenario 或空 evidence 都使 local gate 失敗；不可提早 skip 後回成功。也重跑全部離線 suite，包含 fuzz smoke、deterministic race、benchmark 及 example compilation。保存 revision、兩種 client version、server digest/configuration、toolchain、seed、command、log、raw/normalized result 與 duration，遮蔽 secret。

### 4.4 P19-04 — 執行完整本地 scenario matrix

| 類別 | 必要行為與證據 |
| --- | --- |
| Protocol/transport/runtime | 兩種 codec、header、malformed/unknown response、TCP/TLS、timeout/cancellation/oneway、route refresh/failure、heartbeat/unregister、shared lifecycle 及 reconnect |
| Producer | Sync/async/oneway/batch、explicit queue/selector、全部 Java configuration boundary、compression、retry/fault selection、two-Broker failure、original deadline 及 duplicate-delivery relationship |
| Pull/Push/offset | 全部 pull status、manual persistence/restart、local/remote offset store、listener partial/failure、send-back/fallback/retry/DLQ、broadcasting/clustering、flow control 及 orderly lock renewal/loss |
| Rebalance/Lite/POP | 全部 Java allocation strategy、mixed Java/Rust join/leave、initial-offset policy、Lite assignment/subscription/seek/commit/pause、POP receipt/ACK/batch ACK/change-invisibility/expiry 及完整 Java POP client flow |
| Transaction/request-reply | Commit/rollback/unknown、實際 Broker check/visibility、duplicate check、雙語言 requester/responder、no responder、late/duplicate reply 及 exactly-one completion |
| Cross-cutting/admin | Namespace、class-filter client path、hook/order/signature、trace delivery/flush、auth allowed/denied、TLS failure、全部 MQAdmin/query operation、reset/status/running-info/direct-consume 及 partial Broker failure |
| Interoperability | Java producer 至 Rust consumer 及反向、mixed clustering/orderly、transaction consumption 及相同 server profile 的雙向 request/reply |
| Stress/recovery | Fixed-seed bounded sustained load、disconnect/restart、實際設定的 failover、slow listener、rebalance churn、offset recovery 及反覆 shutdown 後 task/socket/pending count 回 baseline |

每個公開設定都透過實際 facade／設定途徑測試，不只用 internal test setter。以 Java 5.5.1 結果作 assertion，包含 unsupported-server response。效能 baseline 記錄 hardware/resource 與 load shape；production capacity/SLO 證據分開處理。每個發現的 client defect 都新增 unit regression fixture，修正所屬 layer，再重跑 unit、受影響及完整 local suite。

### 4.5 P19-05 — 區分 server 限制與 client parity 缺漏

將問題歸因 server 前，先在相同 5.3.4 profile 執行 Java 5.5.1 與 Rust。相同拒絕結果只證明 rejection path，不代表功能成功運作。分別記錄 server limitation、Java/Rust result、source evidence 及缺少的 success-path evidence；Rust 功能仍須完整實作並具 unit/golden coverage。

5.3.4 無法執行的成功路徑，先確認已驗證的額外未修改 server version/topology 能否本地執行。可以時，新增獨立命名／固定版本的 Docker profile 並在此執行；保留必要 5.3.4 結果，不宣稱額外 profile 證明 production 支援。只有真正非本地需求才列 manual 並附理由。原本可本地執行卻無法 provision 的 profile 是 BLOCKED，不是 manual waiver 或 PASS。必要行為未證明時，阻擋完整相容性／發布核准。

### 4.6 P19-06 — 列出並交接非本地手動驗證

建立 `docs/owner-validation.md`。每項記錄 ID、非本地理由、owner、所需 topology/version/access、授權 disposable resource、精確 command、預期 Java/Rust outcome、cleanup、revision/date 及 evidence location。未明確授權，不做 production write 或 fault injection。

| ID | 無法本地重現時由 owner 執行的項目 |
| --- | --- |
| MAN-01 | 實際 production 5.3.4 NameServer/Broker discovery、DNS/VIP/proxy/firewall 及跨網路 connectivity |
| MAN-02 | 組織核發 CA/SNI/mTLS、真實 identity/ACL provisioning 及 rotation、實際 namespace/topic/group allowed/denied permission |
| MAN-03 | Production 專屬 HA/failover、network partition、multi-zone routing 及核准 maintenance/fault-injection 程序下的 recovery |
| MAN-04 | 代表性 production workload、capacity/SLO、長時間 soak、resource quota 及維運 restart/shutdown 行為 |
| MAN-05 | Source 確認且需要本地無法取得 infrastructure 的功能，附已評估本地／額外 Docker 替代方案的證據 |

本地 synthetic TLS/auth、一般 restart/failure、mixed-client 及 bounded stress 仍為自動測試，不可只為方便而改列以上項目。未執行 manual check 維持 BLOCKED；N/A 需實際不適用的證據，不可用於缺少實作。owner 核准不能豁免 Java 功能／設定 parity。

### 4.7 P19-07 — 強制最終驗收與發布授權

CI 執行 server-free gate 及完整 Docker local matrix。release branch 的必要 profile/test 不可選配。再次驗證雙向 source/settings coverage、完整 evidence ID、無額外公開 control 及無缺少 Java flow。分開 client implementation、5.3.4 interoperability、supplementary-server success 與 manual deployment status。不可用 `INTENTIONAL_DIFFERENCE` 豁免 public API/configuration/behavior 缺口。

最終 candidate 需要全部 local gate 通過且必要 manual evidence 完整。未解決差異、必要環境不可用或缺少 success-path evidence 都會阻擋。publish/tag 仍為另外授權操作：leaf-to-facade 發布前再次檢查 crate-name availability、package graph/registry dependency 與 LICENSE/NOTICE。測試不可自動發布。

## 5. 驗證

- [ ] **P19-V01** — 乾淨 Docker setup 可重現；實際 5.3.4 version/digest 與 Java 5.5.1 pin 已驗證；readiness 與安全 cleanup 正常。
- [ ] **P19-V02** — executed local ID 等於完整必要清單；missing/ignored/skipped case 不算 PASS，可執行的本地測試沒有延至 manual。
- [ ] **P19-V03** — 每個適用類別的 Java/Rust 跨語言及 differential 結果一致；server limitation 與額外 success evidence 分開記錄。
- [ ] **P19-V04** — configuration/feature/flow 反向稽核無額外 Rust 公開能力，也無缺少 Java 能力；新 defect 有 unit regression。
- [ ] **P19-V05** — 非本地項目具可執行 owner 指令及 PASS/BLOCKED/N/A 證據；必要 BLOCKED 項目阻擋最終核准。

### 5.1 預定本地入口

```bash
docker version
docker compose version
bash integration-tests/run-local.sh --all
```

實作 `run-local.sh` 以驗證設定、provision profile、執行 setup 及全部 offline/live/differential/stress/fuzz/example suite、比對 evidence ID，最後以 trap teardown 並保留失敗狀態與 log。在 `integration-tests/README.md` 記錄各 profile replay command 及 address。此計畫不宣稱命令已存在或通過。

## 6. 驗收與交接

產出可稽核報告，分開完整 Java 5.5.1 client parity、production 基準 5.3.4 結果、額外本地證據及 owner manual result。全部可本地執行測試都已實際執行；不可接受缺少功能／設定／流程或額外公開行為。必要本地及非本地證據通過前，最終相容性／發布核准維持阻擋。本任務不包含 server 實作、自動發布或 production mutation。

## 7. 手動執行前需要的 owner 輸入

受影響 manual case 執行前，取得實際 production topology、auth/CA provisioning、允許測試的資源、maintenance authorization 及 workload/SLO parameter。這些是環境輸入，不是能否省略 Java 功能的決策；不阻擋建立隔離本地 profile。
