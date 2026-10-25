#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
manifest="$repo_root/tests/differential/scenarios.yaml"
scenario_ids=()
scenario_id_count=0
selection=""

usage() {
    cat <<'EOF'
Usage:
  tests/differential/run.sh --scenario ID
  tests/differential/run.sh --phase N
  tests/differential/run.sh --all
  tests/differential/run.sh --check-all
  tests/differential/run.sh --list

Only scenarios with a complete local runner are executable. --phase and --all
fail closed if the selected inventory contains any unimplemented scenario.
EOF
}

while (($#)); do
    case "$1" in
        --scenario)
            [[ $# -ge 2 ]] || { usage >&2; exit 2; }
            selection="scenario"
            scenario_ids+=("$2")
            scenario_id_count=$((scenario_id_count + 1))
            shift 2
            ;;
        --phase)
            [[ $# -ge 2 && -z "$selection" ]] || { usage >&2; exit 2; }
            selection="phase"
            phase_number="$2"
            shift 2
            ;;
        --all)
            [[ -z "$selection" ]] || { usage >&2; exit 2; }
            selection="all"
            shift
            ;;
        --check-all)
            [[ -z "$selection" ]] || { usage >&2; exit 2; }
            selection="check-all"
            shift
            ;;
        --list)
            [[ -z "$selection" ]] || { usage >&2; exit 2; }
            selection="list"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

[[ -n "$selection" ]] || { usage >&2; exit 2; }
if [[ "$selection" == "scenario" && $scenario_id_count -eq 0 ]]; then
    echo "At least one scenario ID is required" >&2
    exit 2
fi
if [[ "$selection" != "scenario" && $scenario_id_count -ne 0 ]]; then
    echo "--scenario cannot be combined with another selector" >&2
    exit 2
fi

selected_file="$(mktemp)"
trap 'rm -f "$selected_file"' EXIT
python_args=("$manifest" "$selection" "${phase_number:-}")
if ((scenario_id_count)); then
    python_args+=("${scenario_ids[@]}")
fi
if ! python3 - "${python_args[@]}" >"$selected_file" <<'PY'
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
selection = sys.argv[2]
phase = sys.argv[3]
scenario_ids = sys.argv[4:]
data = json.loads(manifest_path.read_text())
scenarios = data.get("scenarios")
if not isinstance(scenarios, list) or not scenarios:
    raise SystemExit("scenario manifest is empty or malformed")
if data.get("java_client_version") != "5.5.1" or data.get("server_version") != "5.3.4":
    raise SystemExit("scenario manifest version pins must remain Java 5.5.1 / server 5.3.4")

ids = [scenario.get("id") for scenario in scenarios]
if any(not isinstance(item, str) or not item for item in ids) or len(ids) != len(set(ids)):
    raise SystemExit("scenario manifest IDs must be nonempty and unique")

if selection in ("all", "check-all"):
    selected = scenarios
elif selection == "phase":
    if not phase.isdecimal():
        raise SystemExit("--phase requires a nonnegative integer")
    selected = [scenario for scenario in scenarios if scenario.get("phase") == int(phase)]
elif selection == "scenario":
    by_id = {scenario["id"]: scenario for scenario in scenarios}
    missing = [scenario_id for scenario_id in scenario_ids if scenario_id not in by_id]
    if missing:
        raise SystemExit("scenario ID not present in manifest: " + ", ".join(missing))
    selected = [by_id[scenario_id] for scenario_id in scenario_ids]
else:
    selected = scenarios

if not selected:
    raise SystemExit("selector matched no scenarios")
for scenario in selected:
    scenario_id = scenario.get("id")
    profile = scenario.get("profile")
    inputs = scenario.get("inputs", {})
    topic = inputs.get("topic", "-")
    producer_group = inputs.get("producer_group", inputs.get("transaction_group", inputs.get("group", "-")))
    consumer_group = inputs.get("consumer_group", inputs.get("group", "-"))
    body = inputs.get("body", "-")
    key_prefix = inputs.get("key_prefix", "-")
    java_expected_max_offset = inputs.get("java_expected_max_offset", "-")
    rust_expected_max_offset = inputs.get("rust_expected_max_offset", "-")
    java_expected_queue_offset = inputs.get("java_expected_queue_offset", "-")
    rust_expected_queue_offset = inputs.get("rust_expected_queue_offset", "-")
    message_count = inputs.get("messages", "-")
    queue_id = inputs.get("queue_id", "-")
    java_offset = inputs.get("java_offset", "-")
    rust_offset = inputs.get("rust_offset", "-")
    pull_offset = inputs.get("pull_offset", "-")
    missing_topic = inputs.get("missing_topic", "-")
    queue_count = inputs.get("queues", "-")
    consumer_ids = inputs.get("consumers", "-")
    if not isinstance(profile, str) or not profile:
        raise SystemExit(f"{scenario_id}: missing profile")
    if not isinstance(key_prefix, str) or not key_prefix:
        raise SystemExit(f"{scenario_id}: invalid key prefix")
    print(
        f"{scenario_id}\t{profile}\t{topic}\t{producer_group}\t{body}\t{key_prefix}\t"
        f"{java_expected_max_offset}\t{rust_expected_max_offset}\t"
        f"{java_expected_queue_offset}\t{rust_expected_queue_offset}\t{message_count}\t"
        f"{java_offset}\t{rust_offset}\t{queue_id}\t{pull_offset}\t{missing_topic}\t"
        f"{queue_count}\t{consumer_ids}\t{consumer_group}"
    )
PY
then
    exit 2
fi

if [[ "$selection" == "list" ]]; then
    cat "$selected_file"
    exit 0
fi

implemented=("ROUTE-001-get-existing-topic" "ROUTE-002-topic-not-exist" "HEARTBEAT-001" "PROD-001-sync-send" "PROD-002-async-send" "PROD-003-oneway-send" "PROD-004-batch-send" "CONS-001-query-offset" "CONS-002-pull-found" "CONS-003-pull-no-new-msg" "CONS-004-update-offset" "ORDER-001-lock-unlock" "REBAL-001-average-10q-3c" "TX-001-commit" "TX-002-rollback" "P5-V02-route" "P0-V03" "P6-V04" "P7-V02" "P9-V03")
unsupported=()
while IFS=$'\t' read -r scenario_id profile topic producer_group body key_prefix java_expected_max_offset rust_expected_max_offset java_expected_queue_offset rust_expected_queue_offset message_count java_offset rust_offset queue_id pull_offset missing_topic queue_count consumer_ids consumer_group; do
    is_implemented=false
    for implemented_id in "${implemented[@]}"; do
        if [[ "$scenario_id" == "$implemented_id" ]]; then
            is_implemented=true
            break
        fi
    done
    if [[ "$is_implemented" != true ]]; then
        unsupported+=("$scenario_id")
    fi
done <"$selected_file"

if ((${#unsupported[@]})); then
    printf 'Refusing to skip unimplemented scenarios (%d):\n' "${#unsupported[@]}" >&2
    printf '  %s\n' "${unsupported[@]}" >&2
    echo "No Docker resources were started and no selected scenario counts as passed." >&2
    exit 3
fi

if [[ "$selection" == "check-all" ]]; then
    row_count="$(wc -l <"$selected_file" | tr -d ' ')"
    printf 'Every selected live scenario has a local runner (%s rows); no Docker resources were started.\n' \
        "$row_count"
    exit 0
fi

if [[ "$selection" != "scenario" ]]; then
    while IFS=$'\t' read -r scenario_id profile _; do
        printf 'Running manifest scenario %s with profile %s\n' "$scenario_id" "$profile"
        ROCKETMQ_PROFILE="$profile" "$repo_root/tests/differential/run.sh" --scenario "$scenario_id"
    done <"$selected_file"
    exit 0
fi

IFS=$'\t' read -r scenario_id profile topic producer_group body key_prefix java_expected_max_offset rust_expected_max_offset java_expected_queue_offset rust_expected_queue_offset message_count java_offset rust_offset queue_id pull_offset missing_topic queue_count consumer_ids consumer_group <"$selected_file"
case "$profile:$scenario_id" in
    basic:*) ;;
    two-broker:ORDER-001-lock-unlock|two-broker:REBAL-001-average-10q-3c) ;;
    transaction:TX-001-commit|transaction:TX-002-rollback) ;;
    host-routed:P6-V04|host-routed:P7-V02|host-routed:P9-V03) ;;
    *)
        echo "Scenario metadata differs from the implemented profile" >&2
        exit 3
        ;;
esac
case "$scenario_id:$topic" in
    ROUTE-001-get-existing-topic:compat_phase0_route|ROUTE-002-topic-not-exist:compat_phase0_missing) ;;
    HEARTBEAT-001:-)
        if [[ "$producer_group" != "compat_phase0" ]]; then
            echo "Scenario metadata differs from the reviewed heartbeat implementation" >&2
            exit 3
        fi
        ;;
    PROD-001-sync-send:compat_phase0_send|PROD-002-async-send:compat_phase0_send|PROD-003-oneway-send:compat_phase0_send)
        if [[ "$body" != "fixed" ]]; then
            echo "Scenario metadata differs from the reviewed producer-send implementation" >&2
            exit 3
        fi
        if [[ "$scenario_id" == "PROD-003-oneway-send" && "$key_prefix" != "compat_phase0_oneway" ]]; then
            echo "One-way send key prefix differs from the reviewed scenario input" >&2
            exit 3
        fi
        if [[ "$scenario_id" == "PROD-003-oneway-send" && \
            ("$java_expected_max_offset" != "1" || "$rust_expected_max_offset" != "2" || \
                "$java_expected_queue_offset" != "0" || "$rust_expected_queue_offset" != "1") ]]; then
            echo "One-way expected queue offsets differ from the isolated topic's empty start" >&2
            exit 3
        fi
        ;;
    PROD-004-batch-send:compat_phase0_batch)
        if [[ "$body" != "batch" || "$message_count" != "2" ]]; then
            echo "Batch send metadata differs from the reviewed two-message input" >&2
            exit 3
        fi
        ;;
    CONS-001-query-offset:compat_phase0_query_offset)
        if [[ "$producer_group" != "compat_phase0_query_group" || "$java_offset" != "17" || "$rust_offset" != "23" || "$queue_id" != "0" ]]; then
            echo "Consumer-offset metadata differs from the reviewed reciprocal update/query case" >&2
            exit 3
        fi
        ;;
    CONS-004-update-offset:compat_phase0_update_offset)
        if [[ "$producer_group" != "compat_phase0_update_group" || "$java_offset" != "31" || "$rust_offset" != "47" || "$queue_id" != "0" ]]; then
            echo "Offset-update metadata differs from the reviewed reciprocal update/query case" >&2
            exit 3
        fi
        ;;
    CONS-002-pull-found:compat_phase0_pull_found)
        if [[ "$producer_group" != "compat_phase0_pull_found_group" || "$body" != "pull-seed" || "$queue_id" != "0" || "$pull_offset" != "0" ]]; then
            echo "Found-pull metadata differs from the reviewed seeded queue case" >&2
            exit 3
        fi
        ;;
    CONS-003-pull-no-new-msg:compat_phase0_pull_empty)
        if [[ "$producer_group" != "compat_phase0_pull_empty_group" || "$queue_id" != "0" || "$pull_offset" != "0" ]]; then
            echo "Empty-pull metadata differs from the reviewed empty queue case" >&2
            exit 3
        fi
        ;;
    P5-V02-route:compat_p5_route)
        if [[ "$missing_topic" != "compat_p5_missing" ]]; then
            echo "P5 route metadata differs from the reviewed existing/missing topic pair" >&2
            exit 3
        fi
        ;;
    ORDER-001-lock-unlock:compat_phase0_order_topic)
        if [[ "$profile" != "two-broker" || "$producer_group" != "compat_phase0_order" ]]; then
            echo "Lock/unlock metadata differs from the reviewed two-broker queue-lock case" >&2
            exit 3
        fi
        ;;
    REBAL-001-average-10q-3c:compat_phase0_rebalance)
        if [[ "$profile" != "two-broker" || "$queue_count" != "10" || "$consumer_ids" != "consumer-a,consumer-b,consumer-c" ]]; then
            echo "Rebalance metadata differs from the reviewed 10-queue / 3-consumer case" >&2
            exit 3
        fi
        ;;
    TX-001-commit:compat_phase0_tx|TX-002-rollback:compat_phase0_tx)
        expected_transaction_key="compat_tx_commit"
        expected_consumer_group="compat_phase0_tx_consumer_group"
        if [[ "$scenario_id" == "TX-002-rollback" ]]; then
            expected_transaction_key="compat_tx_rollback"
        fi
        if [[ "$profile" != "transaction" || "$producer_group" != "compat_phase0_tx_group" || "$consumer_group" != "$expected_consumer_group" || "$key_prefix" != "$expected_transaction_key" || "$body" != "transaction-body" ]]; then
            echo "Transaction metadata differs from the reviewed local commit/rollback cases" >&2
            exit 3
        fi
        ;;
    P7-V02:compat_p7_send)
        if [[ "$profile" != "host-routed" ]]; then
            echo "P7-V02 requires the host-routed producer profile" >&2
            exit 3
        fi
        ;;
    P9-V03:compat_p9_pull)
        if [[ "$profile" != "host-routed" || "$consumer_group" != "compat_p9_pull" ]]; then
            echo "P9-V03 metadata differs from the reviewed explicit-pull case" >&2
            exit 3
        fi
        ;;
    P6-V04:compat_p6_shared)
        if [[ "$profile" != "host-routed" || "$consumer_group" != "compat_p6_consumer" ]]; then
            echo "P6-V04 metadata differs from the reviewed shared-runtime case" >&2
            exit 3
        fi
        ;;
    P0-V03:-)
        if [[ "$profile" != "basic" ]]; then
            echo "P0-V03 must use the reviewed basic-profile aggregate" >&2
            exit 3
        fi
        ;;
    *)
        echo "Scenario metadata differs from the reviewed case implementations" >&2
        exit 3
        ;;
esac
if [[ "$scenario_id" != "ROUTE-001-get-existing-topic" && "$scenario_id" != "ROUTE-002-topic-not-exist" && "$scenario_id" != "HEARTBEAT-001" && "$scenario_id" != "PROD-001-sync-send" && "$scenario_id" != "PROD-002-async-send" && "$scenario_id" != "PROD-003-oneway-send" && "$scenario_id" != "PROD-004-batch-send" && "$scenario_id" != "CONS-001-query-offset" && "$scenario_id" != "CONS-004-update-offset" && "$scenario_id" != "CONS-002-pull-found" && "$scenario_id" != "CONS-003-pull-no-new-msg" && "$scenario_id" != "ORDER-001-lock-unlock" && "$scenario_id" != "REBAL-001-average-10q-3c" && "$scenario_id" != "TX-001-commit" && "$scenario_id" != "TX-002-rollback" && "$scenario_id" != "P5-V02-route" && "$scenario_id" != "P0-V03" && "$scenario_id" != "P6-V04" && "$scenario_id" != "P7-V02" && "$scenario_id" != "P9-V03" ]]; then
    echo "No implementation is registered for scenario '$scenario_id'" >&2
    exit 3
fi
if [[ "${ROCKETMQ_CLASSIC_TEST_ENV:-}" != "local-disposable" ]]; then
    echo "Refusing to run live scenario: set ROCKETMQ_CLASSIC_TEST_ENV=local-disposable" >&2
    exit 2
fi
if [[ "${ROCKETMQ_PROFILE:-}" != "$profile" ]]; then
    echo "ROCKETMQ_PROFILE must explicitly equal the manifest profile '$profile'" >&2
    exit 2
fi
maven_java_version="$(mvn -version 2>&1 | sed -n 's/^Java version: \([^,]*\),.*/\1/p')"
if [[ "$maven_java_version" != "27" ]]; then
    echo "The live Java 5.5.1 runner requires the CI-pinned Maven JDK 27; found '${maven_java_version:-unknown}'" >&2
    exit 2
fi
rustc_version="$(rustc --version | awk '{print $2}')"
if [[ "$rustc_version" != "1.85.0" ]]; then
    echo "The live Rust runner requires the workspace-pinned Rust 1.85.0; found '${rustc_version:-unknown}'" >&2
    exit 2
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
started_epoch="$(date +%s)"
run_slug="$(printf '%s' "$run_id" | tr '[:upper:]' '[:lower:]')"
project_slug="$(printf '%s' "$scenario_id" | tr '[:upper:]_' '[:lower:]-')"
java_oneway_key=""
rust_oneway_key=""
if [[ "$scenario_id" == "PROD-003-oneway-send" ]]; then
    java_oneway_key="${key_prefix}_java_${run_slug}"
    rust_oneway_key="${key_prefix}_rust_${run_slug}"
fi
project_name="${COMPOSE_PROJECT_NAME:-rocketmq-classic-p19-${project_slug}-${run_slug}}"
if [[ ! "$project_name" =~ ^rocketmq-classic-p19-[a-z0-9-]+$ ]]; then
    echo "COMPOSE_PROJECT_NAME must start with rocketmq-classic-p19- and use lowercase letters, digits, and hyphens" >&2
    exit 2
fi
export COMPOSE_PROJECT_NAME="$project_name"
export ROCKETMQ_PROFILE="$profile"
export ROCKETMQ_NAMESRV_HOST_PORT="${ROCKETMQ_NAMESRV_HOST_PORT:-19876}"
default_broker_host_port=20911
if [[ "$profile" == "transaction" ]]; then
    default_broker_host_port=30911
elif [[ "$profile" == "host-routed" ]]; then
    default_broker_host_port=40911
fi
export ROCKETMQ_BROKER_HOST_PORT="${ROCKETMQ_BROKER_HOST_PORT:-$default_broker_host_port}"
if [[ "$profile" == "transaction" && "$ROCKETMQ_BROKER_HOST_PORT" != "30911" ]]; then
    echo "The transaction profile advertises broker-a on 127.0.0.1:30911; set ROCKETMQ_BROKER_HOST_PORT=30911" >&2
    exit 2
fi
if [[ "$profile" == "host-routed" && "$ROCKETMQ_BROKER_HOST_PORT" != "40911" ]]; then
    echo "The host-routed profile advertises broker-a on 127.0.0.1:40911; set ROCKETMQ_BROKER_HOST_PORT=40911" >&2
    exit 2
fi
compose=(docker compose --project-name "$project_name" --file "$repo_root/integration-tests/compose.yaml" --profile "$profile")
broker_service=broker
if [[ "$profile" == "transaction" ]]; then
    broker_service=broker-tx
elif [[ "$profile" == "host-routed" ]]; then
    broker_service=broker-host
fi

if ! command -v docker >/dev/null || ! docker compose version >/dev/null 2>&1; then
    echo "Docker Engine and Docker Compose are required for live differential scenarios" >&2
    exit 2
fi
if [[ -n "$("${compose[@]}" ps --all --quiet 2>/dev/null)" ]]; then
    echo "Refusing to reuse nonempty Compose project '$project_name'" >&2
    exit 2
fi

evidence_dir="$repo_root/docs/compatibility/evidence/phase-19/${scenario_id}/${run_id}"
namesrv_addr="127.0.0.1:${ROCKETMQ_NAMESRV_HOST_PORT}"
broker_addr="127.0.0.1:${ROCKETMQ_BROKER_HOST_PORT}"
code_dirty_paths=(Cargo.toml Cargo.lock scripts/check-workspace-metadata.py
    compat/java-reference/pom.xml
    compat/java-reference/src/main/java/compat/LiveRouteProbe.java
    compat/java-reference/src/main/java/compat/LiveHeartbeatProbe.java
    compat/java-reference/src/main/java/compat/LiveProducerProbe.java
    compat/java-reference/src/main/java/compat/LiveConsumerProbe.java
    compat/java-reference/src/main/java/compat/LiveRebalanceProbe.java
    compat/java-reference/src/main/java/compat/LiveTransactionProbe.java
    compat/java-reference/src/main/java/compat/LiveHighLevelProducerProbe.java
    compat/java-reference/src/main/java/compat/LiveSharedRuntimeProbe.java
    compat/java-reference/src/main/java/compat/LivePullConsumerProbe.java
    integration-tests/Cargo.toml integration-tests/src/bin/live_client.rs
    integration-tests/compose.yaml integration-tests/config/broker.conf integration-tests/config/broker-b.conf integration-tests/config/broker-tx.conf integration-tests/config/broker-host.conf
    integration-tests/setup.sh integration-tests/cleanup.sh
    tests/differential/compare.py tests/differential/run.sh)
if [[ -n "$(git -C "$repo_root" status --porcelain -- "${code_dirty_paths[@]}")" ]]; then
    source_code_dirty=true
else
    source_code_dirty=false
fi
if ! python3 - "$repo_root" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1])
try:
    committed = subprocess.check_output(
        ["git", "show", "HEAD:tests/differential/scenarios.yaml"], cwd=root, text=True
    )
    current = (root / "tests/differential/scenarios.yaml").read_text()
    before = json.loads(committed)
    after = json.loads(current)
    for manifest in (before, after):
        for scenario in manifest.get("scenarios", []):
            scenario.pop("live_status", None)
            scenario.pop("evidence", None)
    if before != after:
        raise SystemExit(1)
except (OSError, subprocess.CalledProcessError, json.JSONDecodeError):
    raise SystemExit(1)
PY
then
    source_code_dirty=true
fi
mkdir -p "$evidence_dir"
cp "$selected_file" "$evidence_dir/selected.tsv"
printf 'scenario=%s\nprofile=%s\nproject=%s\nrun_id=%s\njava_client=5.5.1\nserver=5.3.4\nserver_image=apache/rocketmq:5.3.4@sha256:d57ee4ebbd9d7bb6e185775bde41c128e98f185b25bf0b05c77d4e6c010572b8\nsource_revision=%s\nsource_code_dirty=%s\nproducer_group=%s\nbody=%s\nseed=not-applicable\n' \
    "$scenario_id" "$profile" "$project_name" "$run_id" \
    "$(git -C "$repo_root" rev-parse HEAD)" "$source_code_dirty" "$producer_group" "$body" \
    >"$evidence_dir/metadata.txt"
if [[ "$scenario_id" == "PROD-003-oneway-send" ]]; then
    printf 'java_message_key=%s\nrust_message_key=%s\n' "$java_oneway_key" "$rust_oneway_key" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "PROD-004-batch-send" ]]; then
    printf 'message_count=%s\nbody_prefix=%s\n' "$message_count" "$body" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "CONS-001-query-offset" || "$scenario_id" == "CONS-004-update-offset" ]]; then
    printf 'java_offset=%s\nrust_offset=%s\nqueue_id=%s\n' "$java_offset" "$rust_offset" "$queue_id" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "CONS-002-pull-found" || "$scenario_id" == "CONS-003-pull-no-new-msg" ]]; then
    printf 'queue_id=%s\npull_offset=%s\nbody=%s\n' "$queue_id" "$pull_offset" "$body" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "P5-V02-route" ]]; then
    printf 'existing_topic=%s\nmissing_topic=%s\n' "$topic" "$missing_topic" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "ORDER-001-lock-unlock" ]]; then
    printf 'topic=%s\nqueue=broker-a:0\njava_client_id=java-order-owner\nrust_client_id=rust-order-contender\n' "$topic" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "REBAL-001-average-10q-3c" ]]; then
    printf 'topic=%s\nqueue_count=%s\nconsumer_ids=%s\n' "$topic" "$queue_count" "$consumer_ids" \
        >>"$evidence_dir/metadata.txt"
fi
if [[ "$scenario_id" == "TX-001-commit" || "$scenario_id" == "TX-002-rollback" ]]; then
    printf 'consumer_group=%s\n' "$consumer_group" >>"$evidence_dir/metadata.txt"
fi
{
    uname -srm
    cargo --version
    rustc --version
    mvn -version
    java -version
    docker version --format 'Docker client {{.Client.Version}} / server {{.Server.Version}}'
    docker compose version
    docker info --format 'Docker platform {{.OSType}}/{{.Architecture}}'
    python3 - "$repo_root/integration-tests/compose.yaml" "$repo_root/integration-tests/config/broker.conf" "$repo_root/integration-tests/config/broker-b.conf" "$repo_root/integration-tests/config/broker-tx.conf" "$repo_root/integration-tests/config/broker-host.conf" <<'PY'
import hashlib
import sys
from pathlib import Path

for filename in sys.argv[1:]:
    path = Path(filename)
    print(f"sha256 {hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
PY
} >"$evidence_dir/toolchain.txt" 2>&1
if [[ "$scenario_id" == "ORDER-001-lock-unlock" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateSubGroup -c RocketMQLocalTest -g $producer_group -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="lock $broker_addr $producer_group java-initial $topic broker-a 0" exec:java@live-consumer-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="unlock $broker_addr $producer_group java-initial $topic broker-a 0" exec:java@live-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- lock $broker_addr $producer_group rust-initial $topic broker-a 0
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- unlock $broker_addr $producer_group rust-initial $topic broker-a 0
mvn -q -f compat/java-reference/pom.xml -Dexec.args="lock $broker_addr $producer_group java-order-owner $topic broker-a 0" exec:java@live-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- lock $broker_addr $producer_group rust-order-contender $topic broker-a 0
mvn -q -f compat/java-reference/pom.xml -Dexec.args="unlock $broker_addr $producer_group java-order-owner $topic broker-a 0" exec:java@live-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- lock $broker_addr $producer_group rust-order-contender $topic broker-a 0
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- unlock $broker_addr $producer_group rust-order-contender $topic broker-a 0
EOF
elif [[ "$scenario_id" == "REBAL-001-average-10q-3c" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 5 -w 5 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic $consumer_ids" exec:java@live-rebalance-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- rebalance $namesrv_addr $topic $consumer_ids
EOF
elif [[ "$scenario_id" == "TX-001-commit" || "$scenario_id" == "TX-002-rollback" ]]; then
    transaction_decision="commit"
    if [[ "$scenario_id" == "TX-002-rollback" ]]; then
        transaction_decision="rollback"
    fi
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec $broker_service ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec $broker_service ./mqadmin updateSubGroup -c RocketMQLocalTest -g $consumer_group -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic $producer_group $transaction_decision ${key_prefix}_java $body" exec:java@live-transaction-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- transaction $namesrv_addr $topic $producer_group $transaction_decision ${key_prefix}_rust $body
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- max-offset $broker_addr $topic 0
mvn -q -f compat/java-reference/pom.xml -Dexec.args="pull $broker_addr $consumer_group $topic 0 0" exec:java@live-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- pull $broker_addr $consumer_group $topic 0 0
EOF
elif [[ "$scenario_id" == "P7-V02" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic" exec:java@live-high-level-producer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- p7-producer $namesrv_addr $topic
EOF
elif [[ "$scenario_id" == "P6-V04" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateSubGroup -c RocketMQLocalTest -g $consumer_group -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic compat_p6_producer_a compat_p6_producer_b $consumer_group" exec:java@live-shared-runtime-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- p6-shared-runtime $namesrv_addr $topic compat_p6_producer_a compat_p6_producer_b $consumer_group
EOF
elif [[ "$scenario_id" == "P9-V03" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 1 -w 1 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateSubGroup -c RocketMQLocalTest -g ${consumer_group}_java -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile host-routed exec broker-host ./mqadmin updateSubGroup -c RocketMQLocalTest -g ${consumer_group}_rust -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="seed $namesrv_addr $topic" exec:java@live-pull-consumer-probe
mvn -q -f compat/java-reference/pom.xml -Dexec.args="probe $namesrv_addr $topic ${consumer_group}_java" exec:java@live-pull-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- p9-pull $namesrv_addr $topic ${consumer_group}_rust
EOF
elif [[ "$scenario_id" == "P0-V03" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario ROUTE-001-get-existing-topic
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario HEARTBEAT-001
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario PROD-001-sync-send
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario PROD-002-async-send
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario PROD-003-oneway-send
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario PROD-004-batch-send
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario CONS-001-query-offset
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario CONS-002-pull-found
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario CONS-003-pull-no-new-msg
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic bash tests/differential/run.sh --scenario CONS-004-update-offset
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=two-broker bash tests/differential/run.sh --scenario ORDER-001-lock-unlock
EOF
elif [[ "$scenario_id" == "HEARTBEAT-001" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$broker_addr $producer_group" exec:java@live-heartbeat-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- heartbeat $broker_addr $producer_group
EOF
elif [[ "$scenario_id" == "CONS-001-query-offset" || "$scenario_id" == "CONS-004-update-offset" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateSubGroup -c RocketMQLocalTest -g $producer_group -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="update $broker_addr $producer_group $topic $queue_id $java_offset" exec:java@live-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- offset-query $broker_addr $producer_group $topic $queue_id
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- offset-update $broker_addr $producer_group $topic $queue_id $rust_offset
mvn -q -f compat/java-reference/pom.xml -Dexec.args="query $broker_addr $producer_group $topic $queue_id" exec:java@live-consumer-probe
EOF
elif [[ "$scenario_id" == "CONS-002-pull-found" || "$scenario_id" == "CONS-003-pull-no-new-msg" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateSubGroup -c RocketMQLocalTest -g $producer_group -n namesrv:9876
EOF
    if [[ "$scenario_id" == "CONS-002-pull-found" ]]; then
        cat >>"$evidence_dir/commands.txt" <<EOF
mvn -q -f compat/java-reference/pom.xml -Dexec.args="send $broker_addr $topic $body" exec:java@live-producer-probe
EOF
    fi
    cat >>"$evidence_dir/commands.txt" <<EOF
mvn -q -f compat/java-reference/pom.xml -Dexec.args="pull $broker_addr $producer_group $topic $queue_id $pull_offset" exec:java@live-consumer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- pull $broker_addr $producer_group $topic $queue_id $pull_offset
EOF
elif [[ "$scenario_id" == "P5-V02-route" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic" exec:java@live-route-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- route $namesrv_addr $topic
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $missing_topic" exec:java@live-route-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- route $namesrv_addr $missing_topic
EOF
elif [[ "$scenario_id" == "PROD-001-sync-send" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="send $broker_addr $topic $body" exec:java@live-producer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- view $broker_addr $topic '<java-offset-message-id>'
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- send $broker_addr $topic $body
mvn -q -f compat/java-reference/pom.xml -Dexec.args="view $broker_addr $topic <rust-offset-message-id>" exec:java@live-producer-probe
EOF
elif [[ "$scenario_id" == "PROD-002-async-send" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="async-send $broker_addr $topic $body" exec:java@live-producer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- view $broker_addr $topic '<java-offset-message-id>'
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- async-send $broker_addr $topic $body
mvn -q -f compat/java-reference/pom.xml -Dexec.args="view $broker_addr $topic <rust-offset-message-id>" exec:java@live-producer-probe
EOF
elif [[ "$scenario_id" == "PROD-003-oneway-send" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="oneway-send $broker_addr $topic $body $java_oneway_key $java_expected_max_offset" exec:java@live-producer-probe
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin queryMsgByKey -t $topic -k $java_oneway_key -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin queryMsgById -t $topic -i <java-unique-key> -n namesrv:9876
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- view $broker_addr $topic <java-offset-id>
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- oneway-send $broker_addr $topic $body $rust_oneway_key $rust_expected_max_offset
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin queryMsgByKey -t $topic -k $rust_oneway_key -n namesrv:9876
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin queryMsgById -t $topic -i <rust-unique-key> -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="view $broker_addr $topic <rust-offset-id>" exec:java@live-producer-probe
EOF
elif [[ "$scenario_id" == "PROD-004-batch-send" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="batch-send $broker_addr $topic $body $message_count" exec:java@live-producer-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- view-batch $broker_addr $topic '<java-offset-message-ids>'
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- batch-send $broker_addr $topic $body $message_count
mvn -q -f compat/java-reference/pom.xml -Dexec.args="view-batch $broker_addr $topic <rust-offset-message-ids>" exec:java@live-producer-probe
EOF
else
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic" exec:java@live-route-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- route $namesrv_addr $topic
EOF
fi
topic_created=false
cleanup() {
    status=$?
    trap - EXIT
    set +e
    if [[ "$topic_created" == true ]]; then
        "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin deleteTopic -c RocketMQLocalTest -t "$topic" -n namesrv:9876 \
            >"$evidence_dir/topic-cleanup.log" 2>&1
        delete_status=$?
        if ((delete_status)); then
            status=1
            echo "Scenario topic cleanup failed; see $evidence_dir/topic-cleanup.log" >&2
        fi
    fi
    ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE="$profile" \
        COMPOSE_PROJECT_NAME="$project_name" bash "$repo_root/integration-tests/cleanup.sh" \
        >"$evidence_dir/compose-cleanup.log" 2>&1
    compose_cleanup_status=$?
    if ((compose_cleanup_status)); then
        status=1
        echo "Compose cleanup failed; see $evidence_dir/compose-cleanup.log" >&2
    fi
    if ! python3 - "$evidence_dir" "$repo_root" "${HOME:-}" <<'PY'
import sys
from pathlib import Path

evidence_dir = Path(sys.argv[1])
replacements = [
    (sys.argv[2].encode(), b"<repo>"),
    (sys.argv[3].encode(), b"<home>"),
]
for path in evidence_dir.iterdir():
    if not path.is_file():
        continue
    contents = path.read_bytes()
    for source, replacement in replacements:
        if source:
            contents = contents.replace(source, replacement)
    path.write_bytes(contents)
PY
    then
        status=1
        echo "Failed to redact local paths from scenario evidence" >&2
    fi
    ended_epoch="$(date +%s)"
    if ((status == 0)); then
        evidence_relative="${evidence_dir#"$repo_root"/}"
        if ! python3 - "$manifest" "$scenario_id" "$evidence_relative" <<'PY'
import json
import os
import re
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
scenario_id = sys.argv[2]
evidence = sys.argv[3]
lines = manifest_path.read_text().splitlines(keepends=True)
matches = 0
updated = []
for line in lines:
    body = line[:-1] if line.endswith("\n") else line
    match = re.fullmatch(r"(\s*)(\{.*\})(,?)(\s*)", body)
    if match is None:
        updated.append(line)
        continue
    scenario = json.loads(match.group(2))
    if scenario.get("id") != scenario_id:
        updated.append(line)
        continue
    matches += 1
    scenario["live_status"] = "PASS"
    scenario["evidence"] = evidence
    replacement = match.group(1) + json.dumps(scenario, separators=(",", ":")) + match.group(3) + match.group(4)
    updated.append(replacement + ("\n" if line.endswith("\n") else ""))

if matches != 1:
    raise SystemExit(f"expected one manifest row for {scenario_id}, found {matches}")
temporary = manifest_path.with_name(f".{manifest_path.name}.{os.getpid()}.tmp")
temporary.write_text("".join(updated))
temporary.replace(manifest_path)
PY
        then
            status=1
            echo "Failed to reconcile the successful scenario into the manifest" >&2
        fi
    fi
    printf 'exit_status=%s\n' "$status" >>"$evidence_dir/metadata.txt"
    printf 'duration_seconds=%s\n' "$((ended_epoch - started_epoch))" >>"$evidence_dir/metadata.txt"
    rm -f "$selected_file"
    exit "$status"
}
trap cleanup EXIT

echo "Provisioning isolated $profile profile in $project_name"
if [[ "$scenario_id" == "P0-V03" ]]; then
    printf 'Aggregate parent starts no Broker; each constituent run owns and removes its Compose project.\n' \
        >"$evidence_dir/setup.log"
else
    if ! ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE="$profile" \
        COMPOSE_PROJECT_NAME="$project_name" bash "$repo_root/integration-tests/setup.sh" \
        >"$evidence_dir/setup.log" 2>&1; then
        cat "$evidence_dir/setup.log" >&2
        exit 1
    fi
fi
cat "$evidence_dir/setup.log"

if [[ "$scenario_id" == "ROUTE-001-get-existing-topic" || "$scenario_id" == "PROD-001-sync-send" || "$scenario_id" == "PROD-002-async-send" || "$scenario_id" == "PROD-003-oneway-send" || "$scenario_id" == "PROD-004-batch-send" || "$scenario_id" == "CONS-001-query-offset" || "$scenario_id" == "CONS-002-pull-found" || "$scenario_id" == "CONS-003-pull-no-new-msg" || "$scenario_id" == "CONS-004-update-offset" || "$scenario_id" == "ORDER-001-lock-unlock" || "$scenario_id" == "REBAL-001-average-10q-3c" || "$scenario_id" == "TX-001-commit" || "$scenario_id" == "TX-002-rollback" || "$scenario_id" == "P5-V02-route" || "$scenario_id" == "P6-V04" || "$scenario_id" == "P7-V02" || "$scenario_id" == "P9-V03" ]]; then
    echo "Creating scenario-owned topic $topic"
    topic_created=true
    topic_queue_count=4
    if [[ "$scenario_id" == "P6-V04" || "$scenario_id" == "P9-V03" ]]; then
        topic_queue_count=1
    elif [[ "$scenario_id" == "REBAL-001-average-10q-3c" ]]; then
        topic_queue_count=5
    elif [[ "$scenario_id" == "TX-001-commit" || "$scenario_id" == "TX-002-rollback" ]]; then
        topic_queue_count=1
    fi
    if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateTopic -c RocketMQLocalTest -t "$topic" -r "$topic_queue_count" -w "$topic_queue_count" -n namesrv:9876 \
        >"$evidence_dir/topic-create.log" 2>&1; then
        cat "$evidence_dir/topic-create.log" >&2
        exit 1
    fi
    if [[ "$scenario_id" == "CONS-001-query-offset" || "$scenario_id" == "CONS-002-pull-found" || "$scenario_id" == "CONS-003-pull-no-new-msg" || "$scenario_id" == "CONS-004-update-offset" || "$scenario_id" == "ORDER-001-lock-unlock" ]]; then
        echo "Creating scenario-owned subscription group $producer_group"
        if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateSubGroup -c RocketMQLocalTest -g "$producer_group" -n namesrv:9876 \
            >"$evidence_dir/group-create.log" 2>&1; then
            cat "$evidence_dir/group-create.log" >&2
            exit 1
        fi
    elif [[ "$scenario_id" == "TX-001-commit" || "$scenario_id" == "TX-002-rollback" || "$scenario_id" == "P6-V04" ]]; then
        echo "Creating scenario-owned consumer group $consumer_group"
        if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateSubGroup -c RocketMQLocalTest -g "$consumer_group" -n namesrv:9876 \
            >"$evidence_dir/group-create.log" 2>&1; then
            cat "$evidence_dir/group-create.log" >&2
            exit 1
        fi
    elif [[ "$scenario_id" == "P9-V03" ]]; then
        for group_suffix in java rust; do
            pull_group="${consumer_group}_${group_suffix}"
            if ! "${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin updateSubGroup -c RocketMQLocalTest -g "$pull_group" -n namesrv:9876 \
                >>"$evidence_dir/group-create.log" 2>&1; then
                cat "$evidence_dir/group-create.log" >&2
                exit 1
            fi
        done
    fi
elif [[ "$scenario_id" == "ROUTE-002-topic-not-exist" ]]; then
    echo "Confirming that scenario topic $topic has no route"
elif [[ "$scenario_id" == "HEARTBEAT-001" ]]; then
    echo "Sending a Java/Rust heartbeat for producer group $producer_group"
else
    echo "Querying the same route with both clients"
fi

echo "Compiling Java 5.5.1 and Rust live probes"
if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" -DskipTests compile \
    >"$evidence_dir/java-compile.log" 2>&1; then
    cat "$evidence_dir/java-compile.log" >&2
    exit 1
fi
if ! cargo build --locked -p rocketmq-classic-integration --bin live_client \
    >"$evidence_dir/rust-build.log" 2>&1; then
    cat "$evidence_dir/rust-build.log" >&2
    exit 1
fi

if [[ "$scenario_id" == "PROD-001-sync-send" ]]; then
    echo "Sending one message from each language and reading each with the other client"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="send $broker_addr $topic $body" exec:java@live-producer-probe \
        >"$evidence_dir/java-send.tsv" 2>"$evidence_dir/java-send.stderr"; then
        cat "$evidence_dir/java-send.tsv" "$evidence_dir/java-send.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! awk -F '\t' -v topic="$topic" \
        'NF != 7 || $1 != "SEND" || $2 != "SEND_OK" || $3 != topic || $4 != 0 || $5 !~ /^[0-9]+$/ || $6 !~ /^[[:xdigit:]]+$/ || $7 !~ /^[0-9]+$/ { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/java-send.tsv"; then
        echo "Java synchronous send did not return a valid SEND_OK result" >&2
        exit 1
    fi
    IFS=$'\t' read -r _ java_send_status java_send_topic java_queue_id java_queue_offset java_offset_message_id java_physical_offset \
        <"$evidence_dir/java-send.tsv"
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        view "$broker_addr" "$topic" "$java_offset_message_id" \
        >"$evidence_dir/rust-read-java.tsv" 2>"$evidence_dir/rust-read-java.stderr"; then
        cat "$evidence_dir/rust-read-java.stderr" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        send "$broker_addr" "$topic" "$body" \
        >"$evidence_dir/rust-send.tsv" 2>"$evidence_dir/rust-send.stderr"; then
        cat "$evidence_dir/rust-send.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v topic="$topic" \
        'NF != 7 || $1 != "SEND" || $2 != "SEND_OK" || $3 != topic || $4 != 0 || $5 !~ /^[0-9]+$/ || $6 !~ /^[[:xdigit:]]+$/ || $7 !~ /^[0-9]+$/ { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/rust-send.tsv"; then
        echo "Rust synchronous send did not return a valid SEND_OK result" >&2
        exit 1
    fi
    IFS=$'\t' read -r _ rust_send_status rust_send_topic rust_queue_id rust_queue_offset rust_offset_message_id rust_physical_offset \
        <"$evidence_dir/rust-send.tsv"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="view $broker_addr $topic $rust_offset_message_id" exec:java@live-producer-probe \
        >"$evidence_dir/java-read-rust.tsv" 2>"$evidence_dir/java-read-rust.stderr"; then
        cat "$evidence_dir/java-read-rust.tsv" "$evidence_dir/java-read-rust.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if [[ "$java_send_status" != "$rust_send_status" || "$java_send_topic" != "$rust_send_topic" || "$java_queue_id" != "$rust_queue_id" ]]; then
        echo "Java and Rust synchronous send results differ in status, topic, or queue" >&2
        exit 1
    fi
    expected_body_hex="$(python3 -c 'import sys; print(sys.argv[1].encode().hex())' "$body")"
    for readback in "$evidence_dir/rust-read-java.tsv" "$evidence_dir/java-read-rust.tsv"; do
        if ! awk -F '\t' -v topic="$topic" -v body="$expected_body_hex" \
            'NF != 5 || $1 != "MESSAGE" || $2 != topic || $3 != 0 || $4 !~ /^[0-9]+$/ || $5 != body { exit 1 }
             END { if (NR != 1) exit 1 }' "$readback"; then
            echo "Cross-language readback did not match the scenario topic and body: $readback" >&2
            exit 1
        fi
    done
    printf 'PASS %s: matching SEND_OK results and bidirectional cross-language readback\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "PROD-002-async-send" ]]; then
    echo "Sending one asynchronous message from each language and reading each with the other client"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="async-send $broker_addr $topic $body" exec:java@live-producer-probe \
        >"$evidence_dir/java-async-send.tsv" 2>"$evidence_dir/java-async-send.stderr"; then
        cat "$evidence_dir/java-async-send.tsv" "$evidence_dir/java-async-send.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! awk -F '\t' -v topic="$topic" \
        'NF != 8 || $1 != "ASYNC_CALLBACK" || $2 != 1 || $3 != "SEND_OK" || $4 != topic || $5 != 0 || $6 !~ /^[0-9]+$/ || $7 !~ /^[[:xdigit:]]+$/ || $8 !~ /^[0-9]+$/ { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/java-async-send.tsv"; then
        echo "Java asynchronous send did not produce exactly one successful callback result" >&2
        exit 1
    fi
    IFS=$'\t' read -r _ _ java_send_status java_send_topic java_queue_id java_queue_offset java_offset_message_id java_physical_offset \
        <"$evidence_dir/java-async-send.tsv"
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        view "$broker_addr" "$topic" "$java_offset_message_id" \
        >"$evidence_dir/rust-read-java.tsv" 2>"$evidence_dir/rust-read-java.stderr"; then
        cat "$evidence_dir/rust-read-java.stderr" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        async-send "$broker_addr" "$topic" "$body" \
        >"$evidence_dir/rust-async-send.tsv" 2>"$evidence_dir/rust-async-send.stderr"; then
        cat "$evidence_dir/rust-async-send.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v topic="$topic" \
        'NF != 8 || $1 != "ASYNC_FUTURE" || $2 != 1 || $3 != "SEND_OK" || $4 != topic || $5 != 0 || $6 !~ /^[0-9]+$/ || $7 !~ /^[[:xdigit:]]+$/ || $8 !~ /^[0-9]+$/ { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/rust-async-send.tsv"; then
        echo "Rust asynchronous send did not resolve to exactly one successful future result" >&2
        exit 1
    fi
    IFS=$'\t' read -r _ _ rust_send_status rust_send_topic rust_queue_id rust_queue_offset rust_offset_message_id rust_physical_offset \
        <"$evidence_dir/rust-async-send.tsv"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="view $broker_addr $topic $rust_offset_message_id" exec:java@live-producer-probe \
        >"$evidence_dir/java-read-rust.tsv" 2>"$evidence_dir/java-read-rust.stderr"; then
        cat "$evidence_dir/java-read-rust.tsv" "$evidence_dir/java-read-rust.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if [[ "$java_send_status" != "$rust_send_status" || "$java_send_topic" != "$rust_send_topic" || "$java_queue_id" != "$rust_queue_id" ]]; then
        echo "Java and Rust asynchronous send results differ in status, topic, or queue" >&2
        exit 1
    fi
    expected_body_hex="$(python3 -c 'import sys; print(sys.argv[1].encode().hex())' "$body")"
    for readback in "$evidence_dir/rust-read-java.tsv" "$evidence_dir/java-read-rust.tsv"; do
        if ! awk -F '\t' -v topic="$topic" -v body="$expected_body_hex" \
            'NF != 5 || $1 != "MESSAGE" || $2 != topic || $3 != 0 || $4 !~ /^[0-9]+$/ || $5 != body { exit 1 }
             END { if (NR != 1) exit 1 }' "$readback"; then
            echo "Cross-language readback did not match the scenario topic and body: $readback" >&2
            exit 1
        fi
    done
    printf 'PASS %s: matching async results, one Java callback / Rust future completion, and bidirectional cross-language readback\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "PROD-003-oneway-send" ]]; then
    echo "Sending one oneway message from each language, then reading each with the other client"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="oneway-send $broker_addr $topic $body $java_oneway_key $java_expected_max_offset" exec:java@live-producer-probe \
        >"$evidence_dir/java-oneway.tsv" 2>"$evidence_dir/java-oneway.stderr"; then
        cat "$evidence_dir/java-oneway.tsv" "$evidence_dir/java-oneway.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    expected_body_hex="$(python3 -c 'import sys; print(sys.argv[1].encode().hex())' "$body")"
    if ! awk -F '\t' -v topic="$topic" -v key="$java_oneway_key" -v max_offset="$java_expected_max_offset" \
        'NR == 1 && (NF != 4 || $1 != "ONEWAY_RETURN" || $2 != "NO_RESPONSE" || $3 != topic || $4 != key) { exit 1 }
         NR == 2 && (NF != 2 || $1 != "QUEUE_MAX_OFFSET" || $2 != max_offset) { exit 1 }
         END { if (NR != 2) exit 1 }' "$evidence_dir/java-oneway.tsv"; then
        echo "Java oneway send must return without a response and advance the Broker queue" >&2
        exit 1
    fi

    query_message_id() {
        local message_key="$1"
        local language="$2"
        local raw_output="$evidence_dir/broker-query-$language.txt"
        local offset_output="$evidence_dir/broker-offset-query-$language.txt"
        local key_query="$evidence_dir/$language-key-query.tsv"
        local normalized_output="$evidence_dir/$language-message-id.tsv"
        if ! "${compose[@]}" exec --no-TTY broker ./mqadmin queryMsgByKey \
            -t "$topic" -k "$message_key" -n namesrv:9876 \
            >"$raw_output" 2>"$evidence_dir/broker-query-$language.stderr"; then
            cat "$raw_output" "$evidence_dir/broker-query-$language.stderr" 2>/dev/null >&2 || true
            return 1
        fi
        if ! awk -v key="$message_key" '
            NF == 3 && $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ {
                printf "KEY_QUERY\t%s\t%s\t%s\t%s\n", key, $1, $2, $3
                found++
            }
            END { if (found != 1) exit 1 }
        ' "$raw_output" >"$key_query"; then
            echo "Broker key query did not return exactly one indexed message for $language" >&2
            cat "$raw_output" >&2
            return 1
        fi
        IFS=$'\t' read -r _ _ unique_key _ _ <"$key_query"
        if ! "${compose[@]}" exec --no-TTY broker ./mqadmin queryMsgById \
            -t "$topic" -i "$unique_key" -n namesrv:9876 \
            >"$offset_output" 2>"$evidence_dir/broker-offset-query-$language.stderr"; then
            cat "$offset_output" "$evidence_dir/broker-offset-query-$language.stderr" 2>/dev/null >&2 || true
            return 1
        fi
        if ! awk -F ':' -v key="$message_key" '
            {
                value = $2
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
                if ($1 == "OffsetID") offset_id = value
                if ($1 == "Queue ID") queue_id = value
                if ($1 == "Queue Offset") queue_offset = value
            }
            END {
                if ((length(offset_id) != 32 && length(offset_id) != 56) ||
                    offset_id !~ /^[0-9A-Fa-f]+$/ || queue_id !~ /^[0-9]+$/ ||
                    queue_offset !~ /^[0-9]+$/) exit 1
                printf "MESSAGE_ID\t%s\t%s\t%s\t%s\n", key, offset_id, queue_id, queue_offset
            }
        ' "$offset_output" >"$normalized_output"; then
            echo "Broker message-ID query did not return a physical offset ID for $language" >&2
            cat "$offset_output" >&2
            return 1
        fi
    }

    query_message_id "$java_oneway_key" java
    IFS=$'\t' read -r _ _ java_message_id _ java_queue_offset <"$evidence_dir/java-message-id.tsv"
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        view "$broker_addr" "$topic" "$java_message_id" \
        >"$evidence_dir/rust-read-java.tsv" 2>"$evidence_dir/rust-read-java.stderr"; then
        cat "$evidence_dir/rust-read-java.stderr" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        oneway-send "$broker_addr" "$topic" "$body" "$rust_oneway_key" "$rust_expected_max_offset" \
        >"$evidence_dir/rust-oneway.tsv" 2>"$evidence_dir/rust-oneway.stderr"; then
        cat "$evidence_dir/rust-oneway.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v topic="$topic" -v key="$rust_oneway_key" -v max_offset="$rust_expected_max_offset" \
        'NR == 1 && (NF != 4 || $1 != "ONEWAY_RETURN" || $2 != "NO_RESPONSE" || $3 != topic || $4 != key) { exit 1 }
         NR == 2 && (NF != 2 || $1 != "QUEUE_MAX_OFFSET" || $2 != max_offset) { exit 1 }
         END { if (NR != 2) exit 1 }' "$evidence_dir/rust-oneway.tsv"; then
        echo "Rust oneway send must return without a response and advance the Broker queue" >&2
        exit 1
    fi

    query_message_id "$rust_oneway_key" rust
    IFS=$'\t' read -r _ _ rust_message_id _ rust_queue_offset <"$evidence_dir/rust-message-id.tsv"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="view $broker_addr $topic $rust_message_id" exec:java@live-producer-probe \
        >"$evidence_dir/java-read-rust.tsv" 2>"$evidence_dir/java-read-rust.stderr"; then
        cat "$evidence_dir/java-read-rust.tsv" "$evidence_dir/java-read-rust.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    for language_readback in "rust-read-java.tsv:$java_queue_offset" "java-read-rust.tsv:$rust_queue_offset"; do
        readback="${language_readback%%:*}"
        expected_queue_offset="${language_readback##*:}"
        if ! awk -F '\t' -v topic="$topic" -v queue_offset="$expected_queue_offset" -v body="$expected_body_hex" \
            'NF != 5 || $1 != "MESSAGE" || $2 != topic || $3 != 0 || $4 != queue_offset || $5 != body { exit 1 }
             END { if (NR != 1) exit 1 }' "$evidence_dir/$readback"; then
            echo "Cross-language oneway readback did not match the stored message: $readback" >&2
            exit 1
        fi
    done
    printf 'PASS %s: both oneway calls returned without a Broker result; each keyed message reached the queue and was read by the other client\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "PROD-004-batch-send" ]]; then
    echo "Sending one two-message batch from each language and decoding the other client's batch"
    validate_batch_send() {
        python3 - "$1" "$topic" "$message_count" <<'PY'
import re
import sys
from pathlib import Path

rows = [line.split("\t") for line in Path(sys.argv[1]).read_text().splitlines()]
topic = sys.argv[2]
count = int(sys.argv[3])
if len(rows) != 1 or len(rows[0]) != 9:
    raise SystemExit("batch send evidence must contain one nine-column result")
row = rows[0]
ids = row[5].split(",")
first_id = row[6]
if (
    row[0] != "BATCH_SEND"
    or row[1] != "SEND_OK"
    or row[2] != topic
    or row[3] != "0"
    or not row[4].isdecimal()
    or len(ids) != count
    or ids[0] != first_id
    or any(not re.fullmatch(r"[0-9a-fA-F]+", value) or len(value) not in {32, 56} for value in ids)
    or not re.fullmatch(r"[0-9a-fA-F]+", first_id)
    or len(first_id) not in {32, 56}
    or not row[7].isdecimal()
    or int(first_id[-16:], 16) != int(row[7])
    or row[8] != str(count)
):
    raise SystemExit("batch send result or physical-offset ID is invalid")
PY
    }
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="batch-send $broker_addr $topic $body $message_count" exec:java@live-producer-probe \
        >"$evidence_dir/java-batch-send.tsv" 2>"$evidence_dir/java-batch-send.stderr"; then
        cat "$evidence_dir/java-batch-send.tsv" "$evidence_dir/java-batch-send.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! validate_batch_send "$evidence_dir/java-batch-send.tsv"; then
        echo "Java batch send did not return one successful result for all messages" >&2
        exit 1
    fi
    IFS=$'\t' read -r _ java_send_status java_send_topic java_queue_id java_queue_offset java_offset_message_ids java_offset_message_id java_physical_offset java_message_count \
        <"$evidence_dir/java-batch-send.tsv"
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        view-batch "$broker_addr" "$topic" "$java_offset_message_ids" \
        >"$evidence_dir/rust-read-java.tsv" 2>"$evidence_dir/rust-read-java.stderr"; then
        cat "$evidence_dir/rust-read-java.stderr" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        batch-send "$broker_addr" "$topic" "$body" "$message_count" \
        >"$evidence_dir/rust-batch-send.tsv" 2>"$evidence_dir/rust-batch-send.stderr"; then
        cat "$evidence_dir/rust-batch-send.stderr" >&2
        exit 1
    fi
    if ! validate_batch_send "$evidence_dir/rust-batch-send.tsv"; then
        echo "Rust batch send did not return one successful result for all messages" >&2
        exit 1
    fi
    IFS=$'\t' read -r _ rust_send_status rust_send_topic rust_queue_id rust_queue_offset rust_offset_message_ids rust_offset_message_id rust_physical_offset rust_message_count \
        <"$evidence_dir/rust-batch-send.tsv"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="view-batch $broker_addr $topic $rust_offset_message_ids" exec:java@live-producer-probe \
        >"$evidence_dir/java-read-rust.tsv" 2>"$evidence_dir/java-read-rust.stderr"; then
        cat "$evidence_dir/java-read-rust.tsv" "$evidence_dir/java-read-rust.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if [[ "$java_send_status" != "$rust_send_status" || "$java_send_topic" != "$rust_send_topic" || \
        "$java_queue_id" != "$rust_queue_id" || "$java_message_count" != "$rust_message_count" || \
        "$rust_queue_offset" -ne $((java_queue_offset + message_count)) ]]; then
        echo "Java and Rust batch send result metadata or queue progression differs" >&2
        exit 1
    fi
    first_body_hex="$(python3 -c 'import sys; print((sys.argv[1] + "-0").encode().hex())' "$body")"
    second_body_hex="$(python3 -c 'import sys; print((sys.argv[1] + "-1").encode().hex())' "$body")"
    for language_readback in "rust-read-java.tsv:$java_queue_offset" "java-read-rust.tsv:$rust_queue_offset"; do
        readback="${language_readback%%:*}"
        expected_queue_offset="${language_readback##*:}"
        if ! awk -F '\t' -v topic="$topic" -v first_offset="$expected_queue_offset" \
            -v first_body="$first_body_hex" -v second_body="$second_body_hex" \
            'NR == 1 && (NF != 5 || $1 != "MESSAGE" || $2 != topic || $3 != 0 || $4 != first_offset || $5 != first_body) { exit 1 }
             NR == 2 && (NF != 5 || $1 != "MESSAGE" || $2 != topic || $3 != 0 || $4 != first_offset + 1 || $5 != second_body) { exit 1 }
             END { if (NR != 2) exit 1 }' "$evidence_dir/$readback"; then
            echo "Cross-language batch readback did not decode both expected messages: $readback" >&2
            exit 1
        fi
    done
    printf 'PASS %s: Java and Rust batch sends succeeded and each decoded both messages from the other client\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "HEARTBEAT-001" ]]; then
    echo "Sending the same heartbeat through both client APIs"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$broker_addr $producer_group" exec:java@live-heartbeat-probe \
        >"$evidence_dir/java-heartbeat.tsv" 2>"$evidence_dir/java-heartbeat.stderr"; then
        cat "$evidence_dir/java-heartbeat.tsv" "$evidence_dir/java-heartbeat.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- heartbeat "$broker_addr" "$producer_group" \
        >"$evidence_dir/rust-heartbeat.tsv" 2>"$evidence_dir/rust-heartbeat.stderr"; then
        cat "$evidence_dir/rust-heartbeat.stderr" >&2
        exit 1
    fi
    if [[ ! -s "$evidence_dir/java-heartbeat.tsv" || ! -s "$evidence_dir/rust-heartbeat.tsv" ]]; then
        echo "A client produced empty heartbeat evidence" >&2
        exit 1
    fi
    if ! cmp -s "$evidence_dir/java-heartbeat.tsv" "$evidence_dir/rust-heartbeat.tsv"; then
        diff -u "$evidence_dir/java-heartbeat.tsv" "$evidence_dir/rust-heartbeat.tsv" \
            >"$evidence_dir/heartbeat-diff.txt" || true
        cat "$evidence_dir/heartbeat-diff.txt" >&2
        exit 1
    fi
    if ! awk -F '\t' \
        'NF != 3 || $1 != "HEARTBEAT" || $2 !~ /^[0-9]+$/ || $3 !~ /^[[:xdigit:]]+$/ || length($3) % 2 { exit 1 }
         END { if (NR != 1) exit 1 }' \
        "$evidence_dir/java-heartbeat.tsv"; then
        echo "Heartbeat evidence must contain one response version and encoded request body" >&2
        exit 1
    fi
    printf 'PASS %s: matching response version and encoded request body\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "CONS-001-query-offset" || "$scenario_id" == "CONS-004-update-offset" ]]; then
    echo "Writing the Java offset, querying with Rust, then reversing the client direction"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="update $broker_addr $producer_group $topic $queue_id $java_offset" \
        exec:java@live-consumer-probe \
        >"$evidence_dir/java-offset-update.tsv" 2>"$evidence_dir/java-offset-update.stderr"; then
        cat "$evidence_dir/java-offset-update.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v offset="$java_offset" \
        'NF != 2 || $1 != "OFFSET_UPDATED" || $2 != offset { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/java-offset-update.tsv"; then
        echo "Java consumer-offset update returned an unexpected result" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        offset-query "$broker_addr" "$producer_group" "$topic" "$queue_id" \
        >"$evidence_dir/rust-offset-query.tsv" 2>"$evidence_dir/rust-offset-query.stderr"; then
        cat "$evidence_dir/rust-offset-query.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v offset="$java_offset" \
        'NF != 2 || $1 != "OFFSET" || $2 != offset { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/rust-offset-query.tsv"; then
        echo "Rust did not read the offset written by Java" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        offset-update "$broker_addr" "$producer_group" "$topic" "$queue_id" "$rust_offset" \
        >"$evidence_dir/rust-offset-update.tsv" 2>"$evidence_dir/rust-offset-update.stderr"; then
        cat "$evidence_dir/rust-offset-update.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v offset="$rust_offset" \
        'NF != 2 || $1 != "OFFSET_UPDATED" || $2 != offset { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/rust-offset-update.tsv"; then
        echo "Rust consumer-offset update returned an unexpected result" >&2
        exit 1
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="query $broker_addr $producer_group $topic $queue_id" \
        exec:java@live-consumer-probe \
        >"$evidence_dir/java-offset-query.tsv" 2>"$evidence_dir/java-offset-query.stderr"; then
        cat "$evidence_dir/java-offset-query.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v offset="$rust_offset" \
        'NF != 2 || $1 != "OFFSET" || $2 != offset { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/java-offset-query.tsv"; then
        echo "Java did not read the offset written by Rust" >&2
        exit 1
    fi
    printf 'PASS %s: Java and Rust read back each other’s consumer-offset updates\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "CONS-002-pull-found" || "$scenario_id" == "CONS-003-pull-no-new-msg" ]]; then
    expected_status="NO_NEW_MSG"
    if [[ "$scenario_id" == "CONS-002-pull-found" ]]; then
        expected_status="FOUND"
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="send $broker_addr $topic $body" exec:java@live-producer-probe \
            >"$evidence_dir/java-seed-send.tsv" 2>"$evidence_dir/java-seed-send.stderr"; then
            cat "$evidence_dir/java-seed-send.stderr" >&2
            exit 1
        fi
        if ! awk -F '\t' -v topic="$topic" \
            'NF != 7 || $1 != "SEND" || $2 != "SEND_OK" || $3 != topic || $4 != "0" || $5 != "0" || $6 !~ /^[[:xdigit:]]+$/ || $7 !~ /^[0-9]+$/ { exit 1 }
             END { if (NR != 1) exit 1 }' "$evidence_dir/java-seed-send.tsv"; then
            echo "Java pull seed did not land at queue 0 offset 0" >&2
            exit 1
        fi
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="pull $broker_addr $producer_group $topic $queue_id $pull_offset" \
        exec:java@live-consumer-probe \
        >"$evidence_dir/java-pull.tsv" 2>"$evidence_dir/java-pull.stderr"; then
        cat "$evidence_dir/java-pull.stderr" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        pull "$broker_addr" "$producer_group" "$topic" "$queue_id" "$pull_offset" \
        >"$evidence_dir/rust-pull.tsv" 2>"$evidence_dir/rust-pull.stderr"; then
        cat "$evidence_dir/rust-pull.stderr" >&2
        exit 1
    fi
    if ! cmp -s "$evidence_dir/java-pull.tsv" "$evidence_dir/rust-pull.tsv"; then
        diff -u "$evidence_dir/java-pull.tsv" "$evidence_dir/rust-pull.tsv" \
            >"$evidence_dir/pull-diff.txt" || true
        cat "$evidence_dir/pull-diff.txt" >&2
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-pull.tsv" "$expected_status" "$body" <<'PY'
import sys
from pathlib import Path

row = Path(sys.argv[1]).read_text().rstrip("\n").split("\t")
expected_status = sys.argv[2]
expected_body = sys.argv[3]
if len(row) != 7 or row[0] != "PULL" or row[1] != expected_status:
    raise SystemExit("pull result status/shape differs from the expected case")
if any(not value.isdecimal() for value in row[2:6]):
    raise SystemExit("pull result offsets/broker ID must be decimal values")
if row[6] and (len(row[6]) % 2 or any(ch not in "0123456789abcdef" for ch in row[6])):
    raise SystemExit("pull response body must be lowercase hexadecimal")
if expected_status == "FOUND":
    if row[2:6] != ["1", "0", "1", "0"]:
        raise SystemExit("found pull must report offsets 0..1 and broker ID 0")
    if not expected_body or expected_body.encode() not in bytes.fromhex(row[6]):
        raise SystemExit("found pull response does not contain the seeded message body")
elif row[6]:
    raise SystemExit("empty pull must not return a message body")
PY
    then
        echo "Java/Rust pull output does not satisfy the scenario result" >&2
        exit 1
    fi
    if [[ "$expected_status" == "FOUND" ]]; then
        printf 'PASS %s: matching FOUND status, offsets, and seeded message body\n' "$scenario_id" \
            | tee "$evidence_dir/result.txt"
    else
        printf 'PASS %s: matching NO_NEW_MSG status and empty response body\n' "$scenario_id" \
            | tee "$evidence_dir/result.txt"
    fi
elif [[ "$scenario_id" == "ORDER-001-lock-unlock" ]]; then
    run_java_lock() {
        local output_file="$1"
        local client_id="$2"
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="lock $broker_addr $producer_group $client_id $topic broker-a 0" exec:java@live-consumer-probe \
            >"$output_file" 2>"${output_file%.tsv}.stderr"; then
            cat "$output_file" "${output_file%.tsv}.stderr" 2>/dev/null >&2 || true
            return 1
        fi
    }
    run_java_unlock() {
        local output_file="$1"
        local client_id="$2"
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="unlock $broker_addr $producer_group $client_id $topic broker-a 0" exec:java@live-consumer-probe \
            >"$output_file" 2>"${output_file%.tsv}.stderr"; then
            cat "$output_file" "${output_file%.tsv}.stderr" 2>/dev/null >&2 || true
            return 1
        fi
    }
    run_rust_lock() {
        local output_file="$1"
        local client_id="$2"
        if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
            lock "$broker_addr" "$producer_group" "$client_id" "$topic" broker-a 0 \
            >"$output_file" 2>"${output_file%.tsv}.stderr"; then
            cat "${output_file%.tsv}.stderr" >&2
            return 1
        fi
    }
    run_rust_unlock() {
        local output_file="$1"
        local client_id="$2"
        if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
            unlock "$broker_addr" "$producer_group" "$client_id" "$topic" broker-a 0 \
            >"$output_file" 2>"${output_file%.tsv}.stderr"; then
            cat "${output_file%.tsv}.stderr" >&2
            return 1
        fi
    }
    validate_lock() {
        local output_file="$1"
        local expected="$2"
        awk -F '\t' -v acquired="$expected" -v topic="$topic" \
            'NF != 5 || $1 != "LOCK" || $2 != acquired || $3 != topic || $4 != "broker-a" || $5 != 0 { exit 1 }
             END { if (NR != 1) exit 1 }' "$output_file"
    }
    validate_unlock() {
        local output_file="$1"
        awk -F '\t' 'NF != 2 || $1 != "UNLOCK" || $2 != "OK" { exit 1 }
            END { if (NR != 1) exit 1 }' "$output_file"
    }

    echo "Comparing Java and Rust lock acquisition and cross-client contention"
    run_java_lock "$evidence_dir/java-initial-lock.tsv" java-initial
    run_java_unlock "$evidence_dir/java-initial-unlock.tsv" java-initial
    run_rust_lock "$evidence_dir/rust-initial-lock.tsv" rust-initial
    run_rust_unlock "$evidence_dir/rust-initial-unlock.tsv" rust-initial
    if ! cmp -s "$evidence_dir/java-initial-lock.tsv" "$evidence_dir/rust-initial-lock.tsv" \
        || ! validate_lock "$evidence_dir/java-initial-lock.tsv" true \
        || ! validate_unlock "$evidence_dir/java-initial-unlock.tsv" \
        || ! validate_unlock "$evidence_dir/rust-initial-unlock.tsv"; then
        echo "Java and Rust must both acquire and release the same unlocked queue" >&2
        exit 1
    fi
    run_java_lock "$evidence_dir/java-owner-lock.tsv" java-order-owner
    run_rust_lock "$evidence_dir/rust-contended-lock.tsv" rust-order-contender
    if ! validate_lock "$evidence_dir/java-owner-lock.tsv" true \
        || ! validate_lock "$evidence_dir/rust-contended-lock.tsv" false; then
        echo "A second client must not acquire a queue while Java owns its lock" >&2
        exit 1
    fi
    run_java_unlock "$evidence_dir/java-owner-unlock.tsv" java-order-owner
    if ! validate_unlock "$evidence_dir/java-owner-unlock.tsv"; then
        echo "Java queue unlock did not complete synchronously" >&2
        exit 1
    fi
    run_rust_lock "$evidence_dir/rust-after-unlock-lock.tsv" rust-order-contender
    if ! validate_lock "$evidence_dir/rust-after-unlock-lock.tsv" true; then
        echo "Rust client could not acquire the queue after Java released its lock" >&2
        exit 1
    fi
    run_rust_unlock "$evidence_dir/rust-owner-unlock.tsv" rust-order-contender
    if ! validate_unlock "$evidence_dir/rust-owner-unlock.tsv"; then
        echo "Rust queue unlock did not complete synchronously" >&2
        exit 1
    fi
    run_java_lock "$evidence_dir/java-after-unlock-lock.tsv" java-final
    if ! validate_lock "$evidence_dir/java-after-unlock-lock.tsv" true; then
        echo "Java client could not acquire the queue after Rust released its lock" >&2
        exit 1
    fi
    run_java_unlock "$evidence_dir/java-final-unlock.tsv" java-final
    if ! validate_unlock "$evidence_dir/java-final-unlock.tsv"; then
        echo "Final Java queue unlock did not complete synchronously" >&2
        exit 1
    fi
    printf 'PASS %s: Java/Rust both acquired the unlocked queue; Rust was denied while Java held it and acquired it after Java unlock; reverse release also passed\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "REBAL-001-average-10q-3c" ]]; then
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$namesrv_addr $topic $consumer_ids" exec:java@live-rebalance-probe \
        >"$evidence_dir/java-rebalance.tsv" 2>"$evidence_dir/java-rebalance.stderr"; then
        cat "$evidence_dir/java-rebalance.tsv" "$evidence_dir/java-rebalance.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        rebalance "$namesrv_addr" "$topic" "$consumer_ids" \
        >"$evidence_dir/rust-rebalance.tsv" 2>"$evidence_dir/rust-rebalance.stderr"; then
        cat "$evidence_dir/rust-rebalance.stderr" >&2
        exit 1
    fi
    if ! cmp -s "$evidence_dir/java-rebalance.tsv" "$evidence_dir/rust-rebalance.tsv"; then
        diff -u "$evidence_dir/java-rebalance.tsv" "$evidence_dir/rust-rebalance.tsv" \
            >"$evidence_dir/rebalance-diff.txt" || true
        cat "$evidence_dir/rebalance-diff.txt" >&2
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-rebalance.tsv" "$topic" "$consumer_ids" <<'PY'
import sys
from pathlib import Path

rows = [line.split("\t") for line in Path(sys.argv[1]).read_text().splitlines()]
topic = sys.argv[2]
consumers = sorted(sys.argv[3].split(","))
if len(rows) != len(consumers):
    raise SystemExit("assignment output must contain one row per consumer")
assignments = {}
for row in rows:
    if len(row) != 3 or row[0] != "ASSIGN" or row[1] in assignments:
        raise SystemExit("assignment output has an invalid or duplicate consumer row")
    assignments[row[1]] = row[2].split(",") if row[2] else []
if list(assignments) != consumers:
    raise SystemExit("consumer assignment rows must use Java's sorted consumer order")
if [len(assignments[consumer]) for consumer in consumers] != [4, 3, 3]:
    raise SystemExit("10 queues over three clients must assign 4, 3, and 3 queues")
all_queues = [queue for consumer in consumers for queue in assignments[consumer]]
if len(all_queues) != 10 or len(set(all_queues)) != 10:
    raise SystemExit("all 10 queue assignments must be unique and complete")
expected = {
    f"{topic}|{broker}|{queue_id}"
    for broker in ("broker-a", "broker-b")
    for queue_id in range(5)
}
if set(all_queues) != expected:
    raise SystemExit("assignments must cover queues 0-4 on both Brokers")
PY
    then
        echo "Live 10-queue route assignment does not match the expected three-consumer split" >&2
        exit 1
    fi
    printf 'PASS %s: Java and Rust matched all 10 live Broker queues with a 4/3/3 assignment split\n' "$scenario_id" \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "TX-001-commit" || "$scenario_id" == "TX-002-rollback" ]]; then
    transaction_decision="commit"
    expected_state="COMMIT_MESSAGE"
    expected_max_offset=2
    if [[ "$scenario_id" == "TX-002-rollback" ]]; then
        transaction_decision="rollback"
        expected_state="ROLLBACK_MESSAGE"
        expected_max_offset=0
    fi
    java_key="${key_prefix}_java"
    rust_key="${key_prefix}_rust"
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$namesrv_addr $topic $producer_group $transaction_decision $java_key $body" \
        exec:java@live-transaction-probe >"$evidence_dir/java-transaction.tsv" \
        2>"$evidence_dir/java-transaction.stderr"; then
        cat "$evidence_dir/java-transaction.tsv" "$evidence_dir/java-transaction.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        transaction "$namesrv_addr" "$topic" "$producer_group" "$transaction_decision" "$rust_key" "$body" \
        >"$evidence_dir/rust-transaction.tsv" 2>"$evidence_dir/rust-transaction.stderr"; then
        cat "$evidence_dir/rust-transaction.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v state="$expected_state" -v key="$java_key" \
        'NF != 5 || $1 != "TX" || $2 != "SEND_OK" || $3 != state || $4 != key || $5 == "" { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/java-transaction.tsv"; then
        echo "Java transaction send result does not match expected state $expected_state" >&2
        exit 1
    fi
    if ! awk -F '\t' -v state="$expected_state" -v key="$rust_key" \
        'NF != 5 || $1 != "TX" || $2 != "SEND_OK" || $3 != state || $4 != key || $5 == "" { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/rust-transaction.tsv"; then
        echo "Rust transaction send result does not match expected state $expected_state" >&2
        exit 1
    fi
    sleep 2
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        max-offset "$broker_addr" "$topic" 0 >"$evidence_dir/queue-max-offset.tsv" \
        2>"$evidence_dir/queue-max-offset.stderr"; then
        cat "$evidence_dir/queue-max-offset.stderr" >&2
        exit 1
    fi
    if ! awk -F '\t' -v expected="$expected_max_offset" \
        'NF != 2 || $1 != "MAX_OFFSET" || $2 != expected { exit 1 }
         END { if (NR != 1) exit 1 }' "$evidence_dir/queue-max-offset.tsv"; then
        echo "Broker queue max offset differs from expected $expected_max_offset" >&2
        exit 1
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="pull $broker_addr $consumer_group $topic 0 0" exec:java@live-consumer-probe \
        >"$evidence_dir/java-pull.tsv" 2>"$evidence_dir/java-pull.stderr"; then
        cat "$evidence_dir/java-pull.stderr" >&2
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        pull "$broker_addr" "$consumer_group" "$topic" 0 0 \
        >"$evidence_dir/rust-pull.tsv" 2>"$evidence_dir/rust-pull.stderr"; then
        cat "$evidence_dir/rust-pull.stderr" >&2
        exit 1
    fi
    if ! cmp -s "$evidence_dir/java-pull.tsv" "$evidence_dir/rust-pull.tsv"; then
        diff -u "$evidence_dir/java-pull.tsv" "$evidence_dir/rust-pull.tsv" \
            >"$evidence_dir/pull-diff.txt" || true
        cat "$evidence_dir/pull-diff.txt" >&2
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-pull.tsv" "$scenario_id" <<'PY'
import sys
from pathlib import Path

row = Path(sys.argv[1]).read_text().rstrip("\n").split("\t")
scenario_id = sys.argv[2]
if len(row) != 7 or row[0] != "PULL" or any(not value.isdecimal() for value in row[2:6]):
    raise SystemExit("transaction consumer pull has an invalid shape")
if row[6] and (len(row[6]) % 2 or any(ch not in "0123456789abcdef" for ch in row[6])):
    raise SystemExit("transaction pull body must be lowercase hexadecimal")
if scenario_id == "TX-001-commit":
    if row[1] != "FOUND" or row[2:6] != ["2", "0", "2", "0"]:
        raise SystemExit("commit pull must return both committed messages at offsets 0..2")
    if bytes.fromhex(row[6]).count(b"transaction-body") < 2:
        raise SystemExit("commit pull does not contain both transaction message bodies")
else:
    if row[1] != "NO_NEW_MSG" or row[2:6] != ["0", "0", "0", "0"] or row[6]:
        raise SystemExit("rollback pull must return no message and retain empty queue offsets")
PY
    then
        echo "Java/Rust transaction pull output does not satisfy the visibility assertion" >&2
        exit 1
    fi
    if [[ "$scenario_id" == "TX-001-commit" ]]; then
        printf 'PASS %s: Java/Rust commits matched; both committed messages were visible and max offset was %s\n' \
            "$scenario_id" "$expected_max_offset" | tee "$evidence_dir/result.txt"
    else
        printf 'PASS %s: Java/Rust rollbacks matched; no message was visible and max offset was %s\n' \
            "$scenario_id" "$expected_max_offset" | tee "$evidence_dir/result.txt"
    fi
elif [[ "$scenario_id" == "P9-V03" ]]; then
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="seed $namesrv_addr $topic" exec:java@live-pull-consumer-probe \
        >"$evidence_dir/seed.tsv" 2>"$evidence_dir/seed.stderr"; then
        cat "$evidence_dir/seed.tsv" "$evidence_dir/seed.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="probe $namesrv_addr $topic ${consumer_group}_java" \
        exec:java@live-pull-consumer-probe \
        >"$evidence_dir/java-pull-consumer.tsv" \
        2>"$evidence_dir/java-pull-consumer.stderr"; then
        cat "$evidence_dir/java-pull-consumer.tsv" "$evidence_dir/java-pull-consumer.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        p9-pull "$namesrv_addr" "$topic" "${consumer_group}_rust" \
        >"$evidence_dir/rust-pull-consumer.tsv" \
        2>"$evidence_dir/rust-pull-consumer.stderr"; then
        cat "$evidence_dir/rust-pull-consumer.tsv" "$evidence_dir/rust-pull-consumer.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-pull-consumer.tsv" \
        "$evidence_dir/rust-pull-consumer.tsv" <<'PY'
import sys
from pathlib import Path

expected_statuses = ["FOUND", "NO_MATCHED_MSG", "NO_NEW_MSG", "OFFSET_ILLEGAL"]

def parse(path):
    pulls = []
    bodies = []
    boundary = None
    resume = None
    for line in Path(path).read_text().splitlines():
        if not line.startswith("P9\t"):
            continue
        row = line.split("\t")
        if row[1] == "PULL":
            if len(row) != 7:
                raise SystemExit(f"invalid pull result row: {line}")
            pull = (row[2], *map(int, row[3:]))
            if pull[0] not in expected_statuses:
                raise SystemExit(f"unexpected pull status: {pull[0]}")
            if pull[2:4] != (0, 2) or pull[4] != 0:
                raise SystemExit(f"pull boundaries or suggested broker differ: {pull!r}")
            pulls.append(pull)
        elif row[1] == "BODY":
            bodies.append(row[2])
        elif row[1] == "BOUNDARY":
            boundary = tuple(map(int, row[2:]))
        elif row[1] == "RESUME":
            resume = int(row[2])
        else:
            raise SystemExit(f"unexpected P9 result row: {line}")
    if [pull[0] for pull in pulls] != expected_statuses:
        raise SystemExit(f"pull statuses differ from expected sequence: {pulls!r}")
    if bodies != ["p9-seed-match"]:
        raise SystemExit(f"manual pull returned unexpected body list: {bodies!r}")
    if boundary != (0, 2, 0):
        raise SystemExit(f"offset boundary RPCs returned {boundary!r}")
    if resume != 1:
        raise SystemExit(f"restarted pull consumer resumed at {resume!r}")
    return pulls, bodies, boundary, resume

java = parse(sys.argv[1])
rust = parse(sys.argv[2])
if java != rust:
    raise SystemExit(f"Java/Rust pull outcomes differ: {java!r} != {rust!r}")
PY
    then
        echo "Java/Rust pull-consumer behavior does not satisfy P9-V03" >&2
        exit 1
    fi
    printf 'PASS P9-V03: Java and Rust matched all four pull statuses, suggested broker, boundary RPCs, and persisted resume offset\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P6-V04" ]]; then
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$namesrv_addr $topic compat_p6_producer_a compat_p6_producer_b $consumer_group" \
        exec:java@live-shared-runtime-probe \
        >"$evidence_dir/java-shared-runtime.tsv" \
        2>"$evidence_dir/java-shared-runtime.stderr"; then
        cat "$evidence_dir/java-shared-runtime.tsv" "$evidence_dir/java-shared-runtime.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        p6-shared-runtime "$namesrv_addr" "$topic" \
        compat_p6_producer_a compat_p6_producer_b "$consumer_group" \
        >"$evidence_dir/rust-shared-runtime.tsv" \
        2>"$evidence_dir/rust-shared-runtime.stderr"; then
        cat "$evidence_dir/rust-shared-runtime.tsv" "$evidence_dir/rust-shared-runtime.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-shared-runtime.tsv" \
        "$evidence_dir/rust-shared-runtime.tsv" <<'PY'
import sys
from pathlib import Path

expected_received = [
    "p6-before-change",
    "p6-keep-after-change",
    "p6-survivor-after-unregister",
]

def parse(path):
    received = []
    markers = {}
    for line in Path(path).read_text().splitlines():
        if not line.startswith("P6\t"):
            continue
        row = line.split("\t")
        if len(row) != 3:
            raise SystemExit(f"invalid shared-runtime result row: {line}")
        if row[1] == "RECEIVED":
            received.append(row[2])
        elif row[1] in markers:
            raise SystemExit(f"duplicate shared-runtime marker: {line}")
        else:
            markers[row[1]] = row[2]
    if received != expected_received:
        raise SystemExit(f"unexpected consumed messages: {received!r}")
    expected_markers = {
        "SUBSCRIPTION": "*->KEEP",
        "UNREGISTER": "compat_p6_producer_a",
        "SURVIVOR": "SEND_OK",
    }
    if markers != expected_markers:
        raise SystemExit(f"shared-runtime lifecycle markers differ: {markers!r}")
    return received, markers

java = parse(sys.argv[1])
rust = parse(sys.argv[2])
if java != rust:
    raise SystemExit(f"Java/Rust shared-runtime results differ: {java!r} != {rust!r}")
PY
    then
        echo "Java/Rust shared-runtime behavior does not satisfy P6-V04" >&2
        exit 1
    fi
    printf 'PASS P6-V04: Java and Rust applied the new subscription, filtered the old tag, and retained the surviving producer\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P7-V02" ]]; then
    if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
        -Dexec.args="$namesrv_addr $topic" exec:java@live-high-level-producer-probe \
        >"$evidence_dir/java-high-level-producer.tsv" \
        2>"$evidence_dir/java-high-level-producer.stderr"; then
        cat "$evidence_dir/java-high-level-producer.tsv" "$evidence_dir/java-high-level-producer.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- \
        p7-producer "$namesrv_addr" "$topic" \
        >"$evidence_dir/rust-high-level-producer.tsv" \
        2>"$evidence_dir/rust-high-level-producer.stderr"; then
        cat "$evidence_dir/rust-high-level-producer.tsv" "$evidence_dir/rust-high-level-producer.stderr" 2>/dev/null >&2 || true
        exit 1
    fi
    if ! python3 - "$evidence_dir/java-high-level-producer.tsv" \
        "$evidence_dir/rust-high-level-producer.tsv" <<'PY'
import sys
from pathlib import Path

expected_methods = {"SYNC", "ASYNC", "ONEWAY", "EXPLICIT_QUEUE", "SELECTOR", "VALID_BATCH", "INVALID_BATCH"}

def parse(path):
    results = {}
    for line in Path(path).read_text().splitlines():
        if not line.startswith("P7\t"):
            continue
        row = line.split("\t")
        if len(row) < 3 or row[1] in results:
            raise SystemExit(f"invalid or duplicate producer result row: {line}")
        results[row[1]] = row[2:]
    if set(results) != expected_methods:
        raise SystemExit(f"producer methods differ from expected set: {sorted(results)}")
    return results

def normalize(results):
    sync = results["SYNC"]
    if len(sync) != 2 or sync[0] != "SEND_OK" or not sync[1].isdecimal():
        raise SystemExit("sync send must return SEND_OK and a queue ID")
    async_result = results["ASYNC"]
    if len(async_result) != 3 or async_result[0] != "1" or async_result[1] != "SEND_OK" or not async_result[2].isdecimal():
        raise SystemExit("async send must complete once with SEND_OK")
    if results["ONEWAY"] != ["NO_RESPONSE"]:
        raise SystemExit("oneway send must return without a Broker response")
    explicit = results["EXPLICIT_QUEUE"]
    if explicit != ["SEND_OK", "1"]:
        raise SystemExit("explicit-queue send must succeed on queue 1")
    selector = results["SELECTOR"]
    if selector != ["SEND_OK", "2"]:
        raise SystemExit("selector send must succeed on selected queue 2")
    batch = results["VALID_BATCH"]
    if len(batch) != 3 or batch[0] != "SEND_OK" or not batch[1].isdecimal() or batch[2] != "2":
        raise SystemExit("valid batch must send two messages successfully")
    if results["INVALID_BATCH"] != ["REJECTED"]:
        raise SystemExit("mixed-topic batch must be rejected")
    return {
        "SYNC": (sync[0],),
        "ASYNC": tuple(async_result[:2]),
        "ONEWAY": tuple(results["ONEWAY"]),
        "EXPLICIT_QUEUE": tuple(explicit),
        "SELECTOR": tuple(selector),
        "VALID_BATCH": (batch[0], batch[2]),
        "INVALID_BATCH": tuple(results["INVALID_BATCH"]),
    }

java = normalize(parse(sys.argv[1]))
rust = normalize(parse(sys.argv[2]))
if java != rust:
    raise SystemExit(f"Java/Rust high-level producer outcomes differ: {java!r} != {rust!r}")
PY
    then
        echo "Java/Rust high-level producer outcomes do not satisfy P7-V02" >&2
        exit 1
    fi
    printf 'PASS P7-V02: Java and Rust matched all seven high-level producer send modes\n' \
        | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "P0-V03" ]]; then
    constituents=(
        "ROUTE-001-get-existing-topic:basic"
        "HEARTBEAT-001:basic"
        "PROD-001-sync-send:basic"
        "PROD-002-async-send:basic"
        "PROD-003-oneway-send:basic"
        "PROD-004-batch-send:basic"
        "CONS-001-query-offset:basic"
        "CONS-002-pull-found:basic"
        "CONS-003-pull-no-new-msg:basic"
        "CONS-004-update-offset:basic"
        "ORDER-001-lock-unlock:two-broker"
    )
    printf 'scenario\tstatus\tevidence\n' >"$evidence_dir/constituent-results.tsv"
    expected_revision="$(git -C "$repo_root" rev-parse HEAD)"
    for item in "${constituents[@]}"; do
        child_id="${item%%:*}"
        child_profile="${item#*:}"
        child_log="$evidence_dir/child-${child_id}.log"
        echo "Replaying $child_id in a fresh $child_profile project"
        if ! env -u COMPOSE_PROJECT_NAME \
            ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE="$child_profile" \
            bash "$repo_root/tests/differential/run.sh" --scenario "$child_id" \
            >"$child_log" 2>&1; then
            cat "$child_log" >&2
            exit 1
        fi
        if ! python3 - "$manifest" "$repo_root" "$child_id" "$expected_revision" \
            "$evidence_dir/constituent-results.tsv" <<'PY'
import json
import sys
from pathlib import Path

manifest_path, root, scenario_id, expected_revision, output = sys.argv[1:]
rows = json.loads(Path(manifest_path).read_text()).get("scenarios", [])
matches = [row for row in rows if row.get("id") == scenario_id]
if len(matches) != 1 or matches[0].get("live_status") != "PASS":
    raise SystemExit(f"constituent {scenario_id} has no passing manifest result")
evidence = matches[0].get("evidence")
if not isinstance(evidence, str) or not evidence:
    raise SystemExit(f"constituent {scenario_id} has no evidence pointer")
run_dir = Path(root) / evidence
metadata = dict(
    line.split("=", 1)
    for line in (run_dir / "metadata.txt").read_text().splitlines()
    if "=" in line
)
if metadata.get("source_revision") != expected_revision:
    raise SystemExit(f"constituent {scenario_id} ran a different source revision")
if metadata.get("source_code_dirty") != "false" or metadata.get("exit_status") != "0":
    raise SystemExit(f"constituent {scenario_id} did not pass from clean source and cleanup")
if not (run_dir / "result.txt").read_text().startswith("PASS "):
    raise SystemExit(f"constituent {scenario_id} result file is not PASS")
with Path(output).open("a") as stream:
    stream.write(f"{scenario_id}\tPASS\t{evidence}\n")
PY
        then
            echo "Constituent $child_id does not have clean passing evidence" >&2
            exit 1
        fi
    done
    printf 'PASS P0-V03: all %s Phase 0 route, producer, consumer, heartbeat, offset, and lock checks passed on %s\n' \
        "${#constituents[@]}" "$expected_revision" | tee "$evidence_dir/result.txt"
elif [[ "$scenario_id" == "ROUTE-001-get-existing-topic" || "$scenario_id" == "ROUTE-002-topic-not-exist" || "$scenario_id" == "P5-V02-route" ]]; then
    route_topics=("$topic")
    if [[ "$scenario_id" == "P5-V02-route" ]]; then
        route_topics+=("$missing_topic")
    fi
    route_rows=0
    for route_index in "${!route_topics[@]}"; do
        route_topic="${route_topics[$route_index]}"
        suffix=""
        if ((route_index)); then
            suffix="-missing"
        fi
        java_route_file="$evidence_dir/java-route${suffix}.tsv"
        rust_route_file="$evidence_dir/rust-route${suffix}.tsv"
        if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
            -Dexec.args="$namesrv_addr $route_topic" exec:java@live-route-probe \
            >"$java_route_file" 2>"$evidence_dir/java-route${suffix}.stderr"; then
            cat "$java_route_file" "$evidence_dir/java-route${suffix}.stderr" 2>/dev/null >&2 || true
            exit 1
        fi
        if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- route "$namesrv_addr" "$route_topic" \
            >"$rust_route_file" 2>"$evidence_dir/rust-route${suffix}.stderr"; then
            cat "$evidence_dir/rust-route${suffix}.stderr" >&2
            exit 1
        fi
        if [[ ! -s "$java_route_file" || ! -s "$rust_route_file" ]]; then
            echo "A client produced empty route evidence for $route_topic" >&2
            exit 1
        fi
        if ! cmp -s "$java_route_file" "$rust_route_file"; then
            diff -u "$java_route_file" "$rust_route_file" \
                >"$evidence_dir/route${suffix}-diff.txt" || true
            cat "$evidence_dir/route${suffix}-diff.txt" >&2
            exit 1
        fi
        route_is_existing=true
        if [[ "$scenario_id" == "ROUTE-002-topic-not-exist" || "$suffix" == "-missing" ]]; then
            route_is_existing=false
        fi
        if [[ "$route_is_existing" == true ]]; then
            if ! awk -F '\t' -v topic="$route_topic" \
                'NF != 4 || $1 != "QUEUE" || $2 != topic || $3 != "broker-a" || $4 !~ /^[0-3]$/ || seen[$4]++ { exit 1 }
                 END { if (NR != 4) exit 1 }' "$java_route_file"; then
                echo "Route evidence must contain exactly queue IDs 0-3 for $route_topic on broker-a" >&2
                exit 1
            fi
            route_rows=$((route_rows + $(wc -l <"$java_route_file" | tr -d ' ')))
        else
            expected_route="$evidence_dir/expected-route${suffix}.tsv"
            printf 'NO_ROUTE\t%s\n' "$route_topic" >"$expected_route"
            if ! cmp -s "$expected_route" "$java_route_file"; then
                echo "Missing-topic scenario must produce exactly NO_ROUTE for $route_topic" >&2
                exit 1
            fi
            rm -f "$expected_route"
        fi
    done
    if [[ "$scenario_id" == "P5-V02-route" ]]; then
        printf 'PASS %s: both clients matched existing and missing topic routes (%s existing queue rows)\n' "$scenario_id" "$route_rows" \
            | tee "$evidence_dir/result.txt"
    else
        printf 'PASS %s: %s matching route rows\n' "$scenario_id" "$(wc -l <"$evidence_dir/java-route.tsv" | tr -d ' ')" \
            | tee "$evidence_dir/result.txt"
    fi
else
    echo "No executable assertion handler exists for $scenario_id" >&2
    exit 3
fi
echo "Evidence: $evidence_dir"
