#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_root/integration-tests/compose.yaml"

if [[ "${ROCKETMQ_CLASSIC_TEST_ENV:-}" != "local-disposable" ]]; then
    echo "Refusing to provision: set ROCKETMQ_CLASSIC_TEST_ENV=local-disposable" >&2
    exit 2
fi

if [[ "${ROCKETMQ_PROFILE:-}" != "basic" && "${ROCKETMQ_PROFILE:-}" != "two-broker" && "${ROCKETMQ_PROFILE:-}" != "transaction" ]]; then
    echo "ROCKETMQ_PROFILE must name an implemented profile (currently: basic, two-broker, or transaction)" >&2
    exit 2
fi

project_name="${COMPOSE_PROJECT_NAME:-rocketmq-classic-p19-local}"
if [[ ! "$project_name" =~ ^rocketmq-classic-p19-[a-z0-9-]+$ ]]; then
    echo "COMPOSE_PROJECT_NAME must start with rocketmq-classic-p19-" >&2
    exit 2
fi

export COMPOSE_PROJECT_NAME="$project_name"
compose=(docker compose --project-name "$project_name" --file "$compose_file" --profile "$ROCKETMQ_PROFILE")
broker_service=broker
if [[ "$ROCKETMQ_PROFILE" == "transaction" ]]; then
    broker_service=broker-tx
fi

"${compose[@]}" config --quiet
"${compose[@]}" up --detach --wait --wait-timeout 180

for attempt in {1..90}; do
    if output="$("${compose[@]}" exec --no-TTY "$broker_service" ./mqadmin clusterList -n namesrv:9876 2>&1)" \
        && [[ "$output" == *"broker-a"* ]]; then
        if [[ "$ROCKETMQ_PROFILE" == "basic" || "$ROCKETMQ_PROFILE" == "transaction" || "$output" == *"broker-b"* ]]; then
            printf '%s\n' "$output"
            printf 'RocketMQ 5.3.4 %s profile is ready in Compose project %s\n' "$ROCKETMQ_PROFILE" "$project_name"
            exit 0
        fi
    fi
    sleep 1
done

printf '%s\n' "${output:-Broker did not register with the NameServer}" >&2
"${compose[@]}" logs --no-color >&2
exit 1
