#!/bin/bash

set -e

echo "*************************************"
echo "* Setting STORAGE_TYPE to S3.       *"
echo "*                                   *"
echo "* Please make sure that S3 backend  *"
echo "* is ready, and configured properly.*"
echo "*************************************"
export STORAGE_TYPE=s3
export STORAGE_S3_BUCKET=testbucket
export STORAGE_S3_ROOT=admin
export STORAGE_S3_ENDPOINT_URL=http://127.0.0.1:9900
export STORAGE_S3_ACCESS_KEY_ID=minioadmin
export STORAGE_S3_SECRET_ACCESS_KEY=minioadmin
export STORAGE_ALLOW_INSECURE=true

BUILD_PROFILE="${BUILD_PROFILE:-debug}"
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../" || exit

echo "Cleaning up previous runs"

killall -9 databend-query || true
killall -9 databend-meta || true
killall -9 vector || true
rm -rf ./.databend

echo "Starting Databend Query cluster with 2 nodes enable history tables"

for node in 1 2; do
    CONFIG_FILE="./scripts/ci/deploy/config/databend-query-node-${node}.toml"

    echo "Appending history table config to query node-${node}"
    cat ./tests/logging/history_table/history_table.toml >> "$CONFIG_FILE"
done

for node in 1 2 3; do
    CONFIG_FILE="./scripts/ci/deploy/config/databend-meta-node-${node}.toml"

    echo "Appending history table config to meta node-${node}"
    cat ./tests/logging/history_table/history_table_meta.toml >> "$CONFIG_FILE"
done

# Start meta cluster (3 nodes - needed for HA)
echo 'Start Meta service HA cluster(3 nodes)...'

mkdir -p ./.databend/

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-1.toml >./.databend/meta-1.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9191

sleep 1

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-2.toml >./.databend/meta-2.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 28202

sleep 1

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-3.toml >./.databend/meta-3.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 28302

sleep 1

# Start only 2 query nodes
echo 'Start databend-query node-1'
nohup env RUST_BACKTRACE=1 target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-1.toml --internal-enable-sandbox-tenant >./.databend/query-1.out 2>&1 &

echo "Waiting on node-1..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9091

echo 'Start databend-query node-2'
env "RUST_BACKTRACE=1" nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-2.toml --internal-enable-sandbox-tenant >./.databend/query-2.out 2>&1 &

echo "Waiting on node-2..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9092

echo "Started 2-node cluster with history tables enabled..."

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select 123"}')
drop_query_id=$(echo $response | jq -r '.id')
echo "Query ID: $drop_query_id"

echo "Running test queries to test inner history tables"
./tests/logging/history_table/run_all_tests.sh

response1=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"select * from system_history.log_history where query_id = '${drop_query_id}'\"}")

meta_count_response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"select count(*) from system_history.log_history where message like 'Databend Meta version%'\"}")

meta_count_response_data=$(echo "$meta_count_response" | jq -r '.data')
if [[ "$meta_count_response_data" != *"3"* ]]; then
    echo "ERROR: meta node logs is empty"
    echo "meta_count_response_data: $meta_count_response_data"
    exit 1
else
    echo "✓ meta node logs are collected as expected"
fi

startup_check_response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"select count(*) from system_history.log_history where message like 'Ready for connections after%'\"}")
startup_check_response_data=$(echo "$startup_check_response" | jq -r '.data')
if [[ "$startup_check_response_data" != *"2"* ]]; then
    echo "ERROR: startup check failed"
    debug_info=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"select * from system_history.log_history where message like 'Ready for connections after%'\"}")
    echo "$debug_info"
    echo "startup_check_response_data: $startup_check_response_data"
    exit 1
else
    echo "✓ node startup test passed"
fi

# **Internal -> External**: should reset

echo "Add a node with external history table enabled"

CONFIG_FILE="./scripts/ci/deploy/config/databend-query-node-3.toml"
echo "Appending external history table config to node-3"
cat ./tests/logging/history_table/history_table_external.toml >> "$CONFIG_FILE"

echo 'Start databend-query node-3'
env "RUST_BACKTRACE=1" nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-3.toml --internal-enable-sandbox-tenant >./.databend/query-3.out 2>&1 &

echo "Waiting on node-3..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9093

sleep 15


echo "Running test queries to test external history tables"
./tests/logging/history_table/run_all_tests.sh


response2=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"select * from system_history.log_history where query_id = '${drop_query_id}'\"}")

echo "Validating responses..."

# Check response1 data field is not empty
response1_data=$(echo "$response1" | jq -r '.data')
if [ "$response1_data" = "[]" ] || [ "$response1_data" = "null" ]; then
    echo "ERROR: response1 data field is empty but should contain data"
    echo "response1: $response1"
    exit 1
else
    echo "✓ response1 data field contains data as expected"
fi

# Check response2 data field is empty
response2_data=$(echo "$response2" | jq -r '.data')
if [ "$response2_data" != "[]" ] && [ "$response2_data" != "null" ]; then
    echo "ERROR: response2 data field contains data but should be empty"
    echo "response2: $response2"
    exit 1
else
    echo "✓ response2 data field is empty as expected"
fi
