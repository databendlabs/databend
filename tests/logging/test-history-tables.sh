#!/bin/bash

set -e

BUILD_PROFILE="${BUILD_PROFILE:-debug}"
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../" || exit

echo "Cleaning up previous runs"

killall -9 databend-query || true
killall -9 databend-meta || true
killall -9 vector || true
rm -rf .databend

echo "Starting Databend Query cluster with 2 nodes enable history tables"

for node in 1 2; do
    CONFIG_FILE="./scripts/ci/deploy/config/databend-query-node-${node}.toml"

    echo "Appending history table config to node-${node}"
    cat ./tests/logging/history_table.toml >> "$CONFIG_FILE"
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
./tests/logging/check_logs_table.sh

response1=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"select * from system_history.log_history where query_id = '${drop_query_id}'\"}")

# **Internal -> External**: should reset

echo "Add a node with external history table enabled"

CONFIG_FILE="./scripts/ci/deploy/config/databend-query-node-3.toml"
echo "Appending external history table config to node-3"
cat ./tests/logging/history_table_external.toml >> "$CONFIG_FILE"

echo 'Start databend-query node-3'
env "RUST_BACKTRACE=1" nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-3.toml --internal-enable-sandbox-tenant >./.databend/query-3.out 2>&1 &

echo "Waiting on node-3..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9093

sleep 15


echo "Running test queries to test external history tables"
./tests/logging/check_logs_table.sh


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

# **External -> Internal**: should not reset

echo "Kill databend-query-1"
PORT=9091

# Find the PID listening on the specified port
PID=$(lsof -t -i :$PORT)

# Check if a PID was found
if [ -z "$PID" ]; then
  echo "No process found listening on port $PORT."
else
  echo "Found process with PID $PID listening on port $PORT. Killing it..."
  kill -9 "$PID"
  echo "Process $PID killed."
fi

echo 'Restart databend-query node-1'
nohup env RUST_BACKTRACE=1 target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-1.toml --internal-enable-sandbox-tenant >./.databend/query-1.out 2>&1 &

echo "Waiting on node-1..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9091

response3=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"select count(*) from system_history.query_history;\"}")
response3_data=$(echo "$response3" | jq -r '.data')

if [ "$response3_data" = "[]" ] || [ "$response3_data" = "null" ] || [ "$response3_data" = "0" ]; then
    echo "ERROR: response3 data field is empty or 0 but should contain data"
    echo "response3: $response3"
    exit 1
else
    echo "✓ response3 data field contains data as expected"
fi

sleep 15

echo "Running test queries to test external history tables"
./tests/logging/check_logs_table.sh
