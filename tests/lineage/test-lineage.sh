#!/bin/bash

set -e

export STORAGE_TYPE=s3
export STORAGE_S3_BUCKET=testbucket
export STORAGE_S3_ROOT=admin
export STORAGE_S3_ENDPOINT_URL=http://127.0.0.1:9900
export STORAGE_S3_ACCESS_KEY_ID=minioadmin
export STORAGE_S3_SECRET_ACCESS_KEY=minioadmin
export STORAGE_ALLOW_INSECURE=true

BUILD_PROFILE="${BUILD_PROFILE:-debug}"
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "$SCRIPT_PATH/../.." && pwd)"
ICEBERG_COMPOSE_FILE="$REPO_ROOT/tests/sqllogictests/scripts/docker-compose-iceberg-tpch.yml"
iceberg_services_started=false

source "$SCRIPT_PATH/lineage_sqllogic.sh"

cleanup() {
    killall -9 databend-query || true
    killall -9 databend-meta || true
    if [ "$iceberg_services_started" = true ]; then
        docker compose -f "$ICEBERG_COMPOSE_FILE" down || true
    fi
}

start_iceberg_services() {
    echo "Starting Iceberg REST test services"
    iceberg_services_started=true
    docker compose -f "$ICEBERG_COMPOSE_FILE" up -d rustfs mc rest

    for _ in {1..60}; do
        if curl -fsS http://127.0.0.1:9002/health/ready >/dev/null \
            && curl -fsS http://127.0.0.1:8181/v1/config >/dev/null \
            && docker compose -f "$ICEBERG_COMPOSE_FILE" exec -T mc \
                /usr/bin/mc stat rustfs/iceberg-tpch >/dev/null 2>&1; then
            echo "Iceberg REST test services are ready"
            return 0
        fi
        sleep 1
    done

    echo "Iceberg REST test services did not become ready"
    docker compose -f "$ICEBERG_COMPOSE_FILE" logs rustfs mc rest || true
    return 1
}

execute_query() {
    local port="$1"
    local sql="$2"

    curl -fsS -u root: -XPOST "http://localhost:${port}/v1/query" \
        -H 'Content-Type: application/json' \
        -d "$(jq -nc --arg sql "$sql" '{sql: $sql, pagination: {wait_time_secs: 10}}')"
}

wait_for_lineage() {
    local sql="$1"
    local description="$2"
    local response=""

    for _ in {1..60}; do
        response=$(execute_query 8000 "$sql")
        if [ "$(echo "$response" | jq -r '.state')" = "Succeeded" ] \
            && [ "$(echo "$response" | jq -r '.data[0][0] // 0')" -eq 1 ] 2>/dev/null; then
            return 0
        fi
        sleep 1
    done

    echo "$description was not transformed within 60 seconds"
    echo "$response"
    return 1
}

trap cleanup EXIT
cd "$REPO_ROOT"

echo "Cleaning up previous lineage test runs"
cleanup
rm -rf ./.databend
mkdir -p ./.databend/config
start_iceberg_services

for node in 1 2 3; do
    cp "./scripts/ci/deploy/config/databend-meta-node-${node}.toml" \
        "./.databend/config/databend-meta-node-${node}.toml"
    cat ./tests/lineage/config/history_log_storage.toml \
        >> "./.databend/config/databend-meta-node-${node}.toml"
done

cp ./scripts/ci/deploy/config/databend-query-node-1.toml \
    ./.databend/config/databend-query-node-1.toml
cat ./tests/lineage/config/lineage.toml \
    >> ./.databend/config/databend-query-node-1.toml

# Node 2 intentionally has no lineage configuration. Views created through its HTTP port
# model objects that predate lineage capture while both nodes continue to share the same Meta.
cp ./scripts/ci/deploy/config/databend-query-node-2.toml \
    ./.databend/config/databend-query-node-2.toml

echo "Starting Meta HA cluster"
nohup ./target/${BUILD_PROFILE}/databend-meta \
    -c ./.databend/config/databend-meta-node-1.toml >./.databend/meta-1.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9191
sleep 1
nohup ./target/${BUILD_PROFILE}/databend-meta \
    -c ./.databend/config/databend-meta-node-2.toml >./.databend/meta-2.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 28202
sleep 1
nohup ./target/${BUILD_PROFILE}/databend-meta \
    -c ./.databend/config/databend-meta-node-3.toml >./.databend/meta-3.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 28302
sleep 1

echo "Starting lineage-enabled query node on port 8000"
nohup env RUST_BACKTRACE=1 ./target/${BUILD_PROFILE}/databend-query \
    -c ./.databend/config/databend-query-node-1.toml \
    --internal-enable-sandbox-tenant >./.databend/query-1.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 50 --port 8000

echo "Starting lineage-disabled query node on port 8002"
nohup env RUST_BACKTRACE=1 ./target/${BUILD_PROFILE}/databend-query \
    -c ./.databend/config/databend-query-node-2.toml \
    --internal-enable-sandbox-tenant >./.databend/query-2.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 50 --port 8002

run_lineage_suite legacy 8002
run_lineage_suite setup 8000

sentinel_response=$(execute_query 8000 \
    "INSERT INTO lineage_history_readiness.dst SELECT a FROM lineage_history_readiness.src")
if [ "$(echo "$sentinel_response" | jq -r '.state')" != "Succeeded" ]; then
    echo "Failed to write lineage readiness sentinel"
    echo "$sentinel_response"
    exit 1
fi
sentinel_query_id=$(echo "$sentinel_response" | jq -r '.id')
wait_for_lineage \
    "SELECT count(*) FROM system_history.lineage_history WHERE source_database = 'lineage_history_readiness' AND source_name = 'src' AND target_database = 'lineage_history_readiness' AND target_name = 'dst' AND lineage_kind = 'DML' AND query_info['query_id']::STRING = '$sentinel_query_id'" \
    "Lineage readiness sentinel"

run_lineage_suite refresh 8000
wait_for_lineage \
    "SELECT count(*) FROM system_history.lineage_history WHERE target_database = 'lineage_history_legacy' AND target_name = 'legacy_view' AND lineage_kind = 'CREATE_VIEW' AND query_info['backfilled_at'] IS NOT NULL" \
    "Refreshed legacy View lineage"

bash ./tests/lineage/test_external_catalog_fields.sh
run_lineage_suite check 8000

echo "All lineage tests completed successfully"
