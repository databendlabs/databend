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

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK task_missing_wh WAREHOUSE = 'missing_wh' SCHEDULE = 15 SECOND AS insert into t1 values(0)\"}")
state=$(echo "$response" | jq -r '.state')
error_msg=$(echo "$response" | jq -r '.error.message // ""')
if [ "$state" = "Failed" ] && [[ "$error_msg" == *"warehouse missing_wh not exists"* ]]; then
    echo "✅ CREATE TASK rejects unknown warehouse"
else
    echo "❌ Expected CREATE TASK with unknown warehouse to fail"
    echo "State: $state"
    echo "Error: $error_msg"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK IF EXISTS missing_task SET WAREHOUSE = 'missing_wh'\"}")
check_response_error "$response"
echo "✅ ALTER TASK IF EXISTS ignores unknown warehouse for missing task"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_1 WAREHOUSE = 'wh1' SCHEDULE = 15 SECOND AS insert into t1 values(1)\"}")
check_response_error "$response"
create_task_1_query_id=$(echo $response | jq -r '.id')
echo "Create Task 1 Query ID: $create_task_1_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_2 WAREHOUSE = 'wh2' SCHEDULE = 15 SECOND AS insert into t1 values(2)\"}")
check_response_error "$response"
create_task_2_query_id=$(echo $response | jq -r '.id')
echo "Create Task 2 ID: $create_task_2_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SHOW TASKS\"}")
check_response_error "$response"
task_ids=$(echo "$response" | jq -r '.data[] | select(.[1] == "my_task_1" or .[1] == "my_task_2") | .[2]')
task_id_count=$(echo "$task_ids" | sed '/^$/d' | wc -l)
zero_task_id_count=$(echo "$task_ids" | awk '$1 == "0" { count++ } END { print count + 0 }')
unique_task_id_count=$(echo "$task_ids" | sed '/^$/d' | sort -u | wc -l)
if [ "$task_id_count" -eq 2 ] && [ "$zero_task_id_count" -eq 0 ] && [ "$unique_task_id_count" -eq 2 ]; then
    echo "✅ Private task ids are assigned"
else
    echo "❌ Expected private task ids to be non-zero and unique"
    echo "Task IDs:"
    echo "$task_ids"
    exit 1
fi

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

sleep 25

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'X-DATABEND-WAREHOUSE: wh1' -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t1 ORDER BY c1\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["1"],["1"],["1"],["2"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"RESUME WAREHOUSE wh2\"}")
check_response_error "$response"
resume_warehouse_2_query_id=$(echo $response | jq -r '.id')
echo "Resume WareHouse 2 Query ID: $resume_warehouse_2_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TABLE t_move_wh (c1 int)\"}")
check_response_error "$response"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK move_wh_task WAREHOUSE = 'wh1' SCHEDULE = 2 SECOND AS insert into t_move_wh values(1)\"}")
check_response_error "$response"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK move_wh_task RESUME\"}")
check_response_error "$response"

sleep 5

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'X-DATABEND-WAREHOUSE: wh1' -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT count(*) FROM t_move_wh\"}")
check_response_error "$response"
count_before_move=$(echo "$response" | jq -r '.data[0][0]')
if [ "$count_before_move" -ge 1 ]; then
    echo "✅ Moving warehouse regression task runs on original warehouse"
else
    echo "❌ Expected moving warehouse regression task to run before ALTER"
    echo "Actual  : $count_before_move"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK move_wh_task SET WAREHOUSE = 'missing_wh'\"}")
state=$(echo "$response" | jq -r '.state')
error_msg=$(echo "$response" | jq -r '.error.message // ""')
if [ "$state" = "Failed" ] && [[ "$error_msg" == *"warehouse missing_wh not exists"* ]]; then
    echo "✅ ALTER TASK SET WAREHOUSE rejects unknown warehouse"
else
    echo "❌ Expected ALTER TASK SET WAREHOUSE with unknown warehouse to fail"
    echo "State: $state"
    echo "Error: $error_msg"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SHOW TASKS\"}")
check_response_error "$response"
move_wh_warehouse=$(echo "$response" | jq -r '.data[] | select(.[1] == "move_wh_task") | .[5]')
move_wh_schedule=$(echo "$response" | jq -r '.data[] | select(.[1] == "move_wh_task") | .[6]')
if [ "$move_wh_warehouse" = "wh1" ] && [ "$move_wh_schedule" = "INTERVAL 2 SECOND" ]; then
    echo "✅ Failed ALTER TASK SET WAREHOUSE preserves existing warehouse and schedule"
else
    echo "❌ Expected failed ALTER TASK SET WAREHOUSE to preserve existing warehouse and schedule"
    echo "Warehouse: $move_wh_warehouse"
    echo "Schedule : $move_wh_schedule"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK move_wh_task SET WAREHOUSE = 'wh2'\"}")
check_response_error "$response"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SHOW TASKS\"}")
check_response_error "$response"
move_wh_warehouse=$(echo "$response" | jq -r '.data[] | select(.[1] == "move_wh_task") | .[5]')
move_wh_schedule=$(echo "$response" | jq -r '.data[] | select(.[1] == "move_wh_task") | .[6]')
if [ "$move_wh_warehouse" = "wh2" ] && [ "$move_wh_schedule" = "INTERVAL 2 SECOND" ]; then
    echo "✅ ALTER TASK SET WAREHOUSE preserves schedule and updates warehouse"
else
    echo "❌ Expected ALTER TASK SET WAREHOUSE to preserve schedule and update warehouse"
    echo "Warehouse: $move_wh_warehouse"
    echo "Schedule : $move_wh_schedule"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SUSPEND WAREHOUSE wh2\"}")
check_response_error "$response"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'X-DATABEND-WAREHOUSE: wh1' -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK drop_wh2_cancel_task WAREHOUSE = 'wh2' AS SELECT 1\"}")
check_response_error "$response"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'X-DATABEND-WAREHOUSE: wh1' -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT id FROM system.tasks WHERE name = 'drop_wh2_cancel_task'\"}")
check_response_error "$response"
drop_wh2_cancel_task_id=$(echo "$response" | jq -r '.data[0][0]')

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'X-DATABEND-WAREHOUSE: wh1' -H 'Content-Type: application/json' -d "{\"sql\": \"INSERT INTO system_task.task_run (task_id, task_name, query_text, when_condition, after, comment, owner, owner_user, warehouse_name, using_warehouse_size, schedule_type, interval, interval_milliseconds, cron, time_zone, run_id, attempt_number, state, error_code, error_message, root_task_id, scheduled_at, completed_at, next_scheduled_at, error_integration, status, created_at, updated_at, session_params, last_suspended_at, suspend_task_after_num_failures) VALUES (${drop_wh2_cancel_task_id}, 'drop_wh2_cancel_task', 'SELECT 1', NULL, NULL, NULL, 'account_admin', 'root', 'wh2', NULL, NULL, NULL, NULL, NULL, NULL, 930000, 0, 'EXECUTING', 0, NULL, 0, now(), NULL, NULL, NULL, 'SUSPENDED', now(), now(), parse_json('{}'), NULL, NULL)\"}")
check_response_error "$response"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'X-DATABEND-WAREHOUSE: wh1' -H 'Content-Type: application/json' -d "{\"sql\": \"DROP TASK drop_wh2_cancel_task\"}")
check_response_error "$response"

actual=0
for _ in {1..10}; do
    response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'X-DATABEND-WAREHOUSE: wh1' -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT count(*) FROM system_task.task_run WHERE task_name = 'drop_wh2_cancel_task' AND task_id = ${drop_wh2_cancel_task_id} AND state = 'CANCELLED' AND completed_at IS NOT NULL\"}")
    check_response_error "$response"
    actual=$(echo "$response" | jq -r '.data[0][0]')
    if [ "$actual" = "1" ]; then
        break
    fi
    sleep 1
done

if [ "$actual" = "1" ]; then
    echo "✅ DROP TASK clears open runs even when the assigned warehouse is suspended"
else
    echo "❌ Expected DROP TASK to clear open runs from a suspended assigned warehouse"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'X-DATABEND-WAREHOUSE: wh1' -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT count(*) FROM t_move_wh\"}")
check_response_error "$response"
count_after_move=$(echo "$response" | jq -r '.data[0][0]')

sleep 5

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'X-DATABEND-WAREHOUSE: wh1' -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT count(*) FROM t_move_wh\"}")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" = "$count_after_move" ]; then
    echo "✅ ALTER TASK SET WAREHOUSE cancels old warehouse schedule"
else
    echo "❌ Expected old warehouse schedule to stop after ALTER TASK SET WAREHOUSE"
    echo "Before wait: $count_after_move"
    echo "After wait : $actual"
    exit 1
fi
