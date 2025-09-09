#!/bin/bash
# Copyright 2022 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../" || exit

killall -9 databend-query || true
killall -9 databend-meta || true
rm -rf .databend

# enable private task
CONFIG_FILE="./scripts/ci/deploy/config/databend-query-node-system-managed.toml"
cat ./tests/task/private_task.toml >> "$CONFIG_FILE"

echo "Starting Databend Query cluster enable private task"
./scripts/ci/deploy/databend-query-system-managed.sh 2

check_response_error() {
    local response="$1"
    local error_msg=$(echo "$response" | jq -r 'if .state == "Failed" then .error.message else empty end')

    if [ -n "$error_msg" ]; then
        echo "[Test Error] $error_msg" >&2
        exit 1
    fi
}

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE WAREHOUSE wh1 WITH WAREHOUSE_SIZE = '1'\"}")
check_response_error "$response"
create_warehouse_1_query_id=$(echo $response | jq -r '.id')
echo "Create WareHouse 1 Query ID: $create_warehouse_1_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE WAREHOUSE wh2 WITH WAREHOUSE_SIZE = '1'\"}")
check_response_error "$response"
create_warehouse_2_query_id=$(echo $response | jq -r '.id')
echo "Create WareHouse 2 Query ID: $create_warehouse_2_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TABLE t1 (c1 int)\"}")
check_response_error "$response"
create_table_query_id=$(echo $response | jq -r '.id')
echo "Create Table Query ID: $create_table_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_1 WAREHOUSE = 'wh1' SCHEDULE = 15 SECOND AS insert into t1 values(1)\"}")
check_response_error "$response"
create_task_1_query_id=$(echo $response | jq -r '.id')
echo "Create Task 1 Query ID: $create_task_1_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_2 WAREHOUSE = 'wh2' SCHEDULE = 15 SECOND AS insert into t1 values(2)\"}")
check_response_error "$response"
create_task_2_query_id=$(echo $response | jq -r '.id')
echo "Create Task 2 ID: $create_task_2_query_id"

sleep 10

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK my_task_1 RESUME\"}")
check_response_error "$response"
resume_task_1_query_id=$(echo $response | jq -r '.id')
echo "RESUME Task 1 ID: $resume_task_1_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK my_task_2 RESUME\"}")
check_response_error "$response"
resume_task_2_query_id=$(echo $response | jq -r '.id')
echo "RESUME Task 2 ID: $resume_task_2_query_id"

sleep 25

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t1 ORDER BY c1\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["1"],["2"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SUSPEND WAREHOUSE wh2\"}")
check_response_error "$response"
suspend_warehouse_2_query_id=$(echo $response | jq -r '.id')
echo "Suspend WareHouse 2 Query ID: $suspend_warehouse_2_query_id"

sleep 20

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'X-DATABEND-WAREHOUSE: wh1' -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t1 ORDER BY c1\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["1"],["1"],["2"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi
