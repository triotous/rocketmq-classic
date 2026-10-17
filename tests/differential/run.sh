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
    inputs = scenario.get("inputs", {})
    topic = inputs.get("topic", "-")
    producer_group = inputs.get("producer_group", "-")
    body = inputs.get("body", "-")
    key_prefix = inputs.get("key_prefix", "-")
    java_expected_max_offset = inputs.get("java_expected_max_offset", "-")
    rust_expected_max_offset = inputs.get("rust_expected_max_offset", "-")
    java_expected_queue_offset = inputs.get("java_expected_queue_offset", "-")
    rust_expected_queue_offset = inputs.get("rust_expected_queue_offset", "-")
    message_count = inputs.get("messages", "-")
    if not isinstance(profile, str) or not profile:
        raise SystemExit(f"{scenario_id}: missing profile")
    if not isinstance(key_prefix, str) or not key_prefix:
        raise SystemExit(f"{scenario_id}: invalid key prefix")
    print(
        f"{scenario_id}\t{profile}\t{topic}\t{producer_group}\t{body}\t{key_prefix}\t"
        f"{java_expected_max_offset}\t{rust_expected_max_offset}\t"
        f"{java_expected_queue_offset}\t{rust_expected_queue_offset}\t{message_count}"
    )
PY
then
    exit 2
fi

if [[ "$selection" == "list" ]]; then
    cat "$selected_file"
    exit 0
fi

implemented=("ROUTE-001-get-existing-topic" "ROUTE-002-topic-not-exist" "HEARTBEAT-001" "PROD-001-sync-send" "PROD-002-async-send" "PROD-003-oneway-send" "PROD-004-batch-send")
unsupported=()
while IFS=$'\t' read -r scenario_id profile topic producer_group body key_prefix java_expected_max_offset rust_expected_max_offset java_expected_queue_offset rust_expected_queue_offset message_count; do
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

IFS=$'\t' read -r scenario_id profile topic producer_group body key_prefix java_expected_max_offset rust_expected_max_offset java_expected_queue_offset rust_expected_queue_offset message_count <"$selected_file"
if [[ "$profile" != "basic" ]]; then
    echo "Scenario metadata differs from the implemented basic profile" >&2
    exit 3
fi
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
    *)
        echo "Scenario metadata differs from the reviewed case implementations" >&2
        exit 3
        ;;
esac
if [[ "$scenario_id" != "ROUTE-001-get-existing-topic" && "$scenario_id" != "ROUTE-002-topic-not-exist" && "$scenario_id" != "HEARTBEAT-001" && "$scenario_id" != "PROD-001-sync-send" && "$scenario_id" != "PROD-002-async-send" && "$scenario_id" != "PROD-003-oneway-send" && "$scenario_id" != "PROD-004-batch-send" ]]; then
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
export ROCKETMQ_BROKER_HOST_PORT="${ROCKETMQ_BROKER_HOST_PORT:-20911}"
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
broker_addr="127.0.0.1:${ROCKETMQ_BROKER_HOST_PORT}"
code_dirty_paths=(Cargo.toml Cargo.lock scripts/check-workspace-metadata.py
    compat/java-reference/pom.xml
    compat/java-reference/src/main/java/compat/LiveRouteProbe.java
    compat/java-reference/src/main/java/compat/LiveHeartbeatProbe.java
    compat/java-reference/src/main/java/compat/LiveProducerProbe.java
    integration-tests/Cargo.toml integration-tests/src/bin/live_client.rs
    tests/differential/compare.py tests/differential/run.sh tests/differential/scenarios.yaml)
if [[ -n "$(git -C "$repo_root" status --porcelain -- "${code_dirty_paths[@]}")" ]]; then
    source_code_dirty=true
else
    source_code_dirty=false
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
{
    uname -srm
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
if [[ "$scenario_id" == "HEARTBEAT-001" ]]; then
    cat >"$evidence_dir/commands.txt" <<EOF
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=$profile COMPOSE_PROJECT_NAME=$project_name bash integration-tests/setup.sh
mvn -q -f compat/java-reference/pom.xml -Dexec.args="$broker_addr $producer_group" exec:java@live-heartbeat-probe
cargo run --locked --quiet -p rocketmq-classic-integration --bin live_client -- heartbeat $broker_addr $producer_group
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
if ! ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE="$profile" \
    COMPOSE_PROJECT_NAME="$project_name" bash "$repo_root/integration-tests/setup.sh" \
    >"$evidence_dir/setup.log" 2>&1; then
    cat "$evidence_dir/setup.log" >&2
    exit 1
fi
cat "$evidence_dir/setup.log"

if [[ "$scenario_id" == "ROUTE-001-get-existing-topic" || "$scenario_id" == "PROD-001-sync-send" || "$scenario_id" == "PROD-002-async-send" || "$scenario_id" == "PROD-003-oneway-send" || "$scenario_id" == "PROD-004-batch-send" ]]; then
    echo "Creating scenario-owned topic $topic"
    topic_created=true
    if ! "${compose[@]}" exec --no-TTY broker ./mqadmin updateTopic -c RocketMQLocalTest -t "$topic" -r 4 -w 4 -n namesrv:9876 \
        >"$evidence_dir/topic-create.log" 2>&1; then
        cat "$evidence_dir/topic-create.log" >&2
        exit 1
    fi
elif [[ "$scenario_id" == "ROUTE-002-topic-not-exist" ]]; then
    echo "Confirming that scenario topic $topic has no route"
elif [[ "$scenario_id" == "HEARTBEAT-001" ]]; then
    echo "Sending a Java/Rust heartbeat for producer group $producer_group"
elif [[ "$scenario_id" == "PROD-003-oneway-send" ]]; then
    echo "Sending Java/Rust oneway messages and querying each by its unique key"
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
elif [[ "$scenario_id" == "ROUTE-001-get-existing-topic" || "$scenario_id" == "ROUTE-002-topic-not-exist" ]]; then
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
    if [[ "$scenario_id" == "ROUTE-001-get-existing-topic" ]]; then
        if ! awk -F '\t' -v topic="$topic" \
            'NF != 4 || $1 != "QUEUE" || $2 != topic || $3 != "broker-a" || $4 !~ /^[0-3]$/ || seen[$4]++ { exit 1 }
             END { if (NR != 4) exit 1 }' \
            "$evidence_dir/java-route.tsv"; then
            echo "Route evidence must contain exactly queue IDs 0-3 for $topic on broker-a" >&2
            exit 1
        fi
    else
        expected_route="$evidence_dir/expected-route.tsv"
        printf 'NO_ROUTE\t%s\n' "$topic" >"$expected_route"
        if ! cmp -s "$expected_route" "$evidence_dir/java-route.tsv"; then
            echo "Missing-topic scenario must produce exactly NO_ROUTE for $topic" >&2
            exit 1
        fi
        rm -f "$expected_route"
    fi
    printf 'PASS %s: %s matching route rows\n' "$scenario_id" "$(wc -l <"$evidence_dir/java-route.tsv" | tr -d ' ')" \
        | tee "$evidence_dir/result.txt"
fi
echo "Evidence: $evidence_dir"
