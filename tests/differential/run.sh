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

if selection == "all":
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
    topic = scenario.get("inputs", {}).get("topic", "")
    if not isinstance(profile, str) or not profile:
        raise SystemExit(f"{scenario_id}: missing profile")
    print(f"{scenario_id}\t{profile}\t{topic}")
PY
then
    exit 2
fi

if [[ "$selection" == "list" ]]; then
    cat "$selected_file"
    exit 0
fi

implemented=("ROUTE-001-get-existing-topic")
unsupported=()
while IFS=$'\t' read -r scenario_id profile topic; do
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

if [[ $(wc -l <"$selected_file" | tr -d ' ') -ne 1 ]]; then
    echo "This runner currently supports one scenario per invocation; split the selection explicitly." >&2
    exit 3
fi

IFS=$'\t' read -r scenario_id profile topic <"$selected_file"
if [[ "$scenario_id" != "ROUTE-001-get-existing-topic" || "$profile" != "basic" || "$topic" != "compat_phase0_route" ]]; then
    echo "Scenario metadata differs from the reviewed ROUTE-001 implementation" >&2
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

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
started_epoch="$(date +%s)"
run_slug="$(printf '%s' "$run_id" | tr '[:upper:]' '[:lower:]')"
project_slug="$(printf '%s' "$scenario_id" | tr '[:upper:]_' '[:lower:]-')"
project_name="${COMPOSE_PROJECT_NAME:-rocketmq-classic-p19-${project_slug}-${run_slug}}"
if [[ ! "$project_name" =~ ^rocketmq-classic-p19-[a-z0-9-]+$ ]]; then
    echo "COMPOSE_PROJECT_NAME must start with rocketmq-classic-p19- and use lowercase letters, digits, and hyphens" >&2
    exit 2
fi
export COMPOSE_PROJECT_NAME="$project_name"
export ROCKETMQ_PROFILE="$profile"
export ROCKETMQ_NAMESRV_HOST_PORT="${ROCKETMQ_NAMESRV_HOST_PORT:-19876}"
compose=(docker compose --project-name "$project_name" --file "$repo_root/integration-tests/compose.yaml" --profile "$profile")

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
code_dirty_paths=(Cargo.toml Cargo.lock scripts/check-workspace-metadata.py
    compat/java-reference/pom.xml
    compat/java-reference/src/main/java/compat/LiveRouteProbe.java
    integration-tests/Cargo.toml integration-tests/src/bin/live_client.rs
    tests/differential/run.sh)
if [[ -n "$(git -C "$repo_root" status --porcelain -- "${code_dirty_paths[@]}")" ]]; then
    source_code_dirty=true
else
    source_code_dirty=false
fi
mkdir -p "$evidence_dir"
cp "$selected_file" "$evidence_dir/selected.tsv"
printf 'scenario=%s\nprofile=%s\nproject=%s\nrun_id=%s\njava_client=5.5.1\nserver=5.3.4\nserver_image=apache/rocketmq:5.3.4@sha256:d57ee4ebbd9d7bb6e185775bde41c128e98f185b25bf0b05c77d4e6c010572b8\nsource_revision=%s\nsource_code_dirty=%s\nseed=not-applicable\n' \
    "$scenario_id" "$profile" "$project_name" "$run_id" \
    "$(git -C "$repo_root" rev-parse HEAD)" "$source_code_dirty" \
    >"$evidence_dir/metadata.txt"
{
    uname -a
    cargo --version
    rustc --version
    mvn -version
    java -version
    docker version --format 'Docker client {{.Client.Version}} / server {{.Server.Version}}'
    docker compose version
    docker info --format 'Docker platform {{.OSType}}/{{.Architecture}}'
    python3 - "$repo_root/integration-tests/compose.yaml" "$repo_root/integration-tests/config/broker.conf" <<'PY'
import hashlib
import sys
from pathlib import Path

for filename in sys.argv[1:]:
    path = Path(filename)
    print(f"sha256 {hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
PY
} >"$evidence_dir/toolchain.txt" 2>&1
cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
docker compose --project-name $project_name --file integration-tests/compose.yaml --profile $profile exec broker ./mqadmin updateTopic -c RocketMQLocalTest -t $topic -r 4 -w 4 -n namesrv:9876
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$namesrv_addr $topic" exec:java@live-route-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- route $namesrv_addr $topic
EOF
topic_created=false
cleanup() {
    status=$?
    trap - EXIT
    set +e
    if [[ "$topic_created" == true ]]; then
        "${compose[@]}" exec --no-TTY broker ./mqadmin deleteTopic -c RocketMQLocalTest -t "$topic" -n namesrv:9876 \
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
    ended_epoch="$(date +%s)"
    printf 'exit_status=%s\n' "$status" >>"$evidence_dir/metadata.txt"
    printf 'duration_seconds=%s\n' "$((ended_epoch - started_epoch))" >>"$evidence_dir/metadata.txt"
    rm -f "$selected_file"
    exit "$status"
}
trap cleanup EXIT

echo "Provisioning isolated $profile profile in $project_name"
if ! ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE="$profile" \
    COMPOSE_PROJECT_NAME="$project_name" bash "$repo_root/integration-tests/setup.sh" \
    >"$evidence_dir/setup.log" 2>&1; then
    cat "$evidence_dir/setup.log" >&2
    exit 1
fi
cat "$evidence_dir/setup.log"

echo "Creating scenario-owned topic $topic"
topic_created=true
if ! "${compose[@]}" exec --no-TTY broker ./mqadmin updateTopic -c RocketMQLocalTest -t "$topic" -r 4 -w 4 -n namesrv:9876 \
    >"$evidence_dir/topic-create.log" 2>&1; then
    cat "$evidence_dir/topic-create.log" >&2
    exit 1
fi

echo "Compiling Java 5.5.1 and Rust route probes"
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

echo "Querying the same route with both clients"
if ! mvn -q -f "$repo_root/compat/java-reference/pom.xml" \
    -Dexec.args="$namesrv_addr $topic" exec:java@live-route-probe \
    >"$evidence_dir/java-route.tsv" 2>"$evidence_dir/java-route.stderr"; then
    cat "$evidence_dir/java-route.tsv" "$evidence_dir/java-route.stderr" 2>/dev/null >&2 || true
    exit 1
fi
if ! cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- route "$namesrv_addr" "$topic" \
    >"$evidence_dir/rust-route.tsv" 2>"$evidence_dir/rust-route.stderr"; then
    cat "$evidence_dir/rust-route.stderr" >&2
    exit 1
fi
if [[ ! -s "$evidence_dir/java-route.tsv" || ! -s "$evidence_dir/rust-route.tsv" ]]; then
    echo "A client produced empty route evidence" >&2
    exit 1
fi
if ! cmp -s "$evidence_dir/java-route.tsv" "$evidence_dir/rust-route.tsv"; then
    diff -u "$evidence_dir/java-route.tsv" "$evidence_dir/rust-route.tsv" \
        >"$evidence_dir/route-diff.txt" || true
    cat "$evidence_dir/route-diff.txt" >&2
    exit 1
fi
if ! awk -F '\t' -v topic="$topic" \
    'NF != 4 || $1 != "QUEUE" || $2 != topic || $3 != "broker-a" || $4 !~ /^[0-3]$/ || seen[$4]++ { exit 1 }
     END { if (NR != 4) exit 1 }' \
    "$evidence_dir/java-route.tsv"; then
    echo "Route evidence must contain exactly queue IDs 0-3 for $topic on broker-a" >&2
    exit 1
fi

printf 'PASS %s: %s matching queue rows\n' "$scenario_id" "$(wc -l <"$evidence_dir/java-route.tsv" | tr -d ' ')" \
    | tee "$evidence_dir/result.txt"
echo "Evidence: $evidence_dir"
