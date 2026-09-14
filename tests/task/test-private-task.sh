#!/bin/bash

set -e

BUILD_PROFILE="${BUILD_PROFILE:-debug}"
: "${QUERY_DATABEND_ENTERPRISE_LICENSE:?QUERY_DATABEND_ENTERPRISE_LICENSE must be set}"
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../" || exit

echo "Cleaning up previous runs"

killall -9 databend-query || true
killall -9 databend-meta || true
rm -rf .databend
mkdir -p ./.databend/

echo "Starting Databend Query cluster with 2 nodes enable private task"

for node in 1 2; do
    SOURCE_CONFIG_FILE="./scripts/ci/deploy/config/databend-query-node-${node}.toml"
    CONFIG_FILE="./.databend/databend-query-node-${node}-private-task.toml"

    echo "Creating private task config for node-${node}"
    cp "$SOURCE_CONFIG_FILE" "$CONFIG_FILE"
    sed -i '/^cloud_control_grpc_server_address/d' $CONFIG_FILE
    printf '\n' >> "$CONFIG_FILE"
    cat ./tests/task/private_task.toml >> "$CONFIG_FILE"
    printf '\n' >> "$CONFIG_FILE"
done

# Start meta cluster (3 nodes - needed for HA)
echo 'Start Meta service HA cluster(3 nodes)...'

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
nohup env RUST_BACKTRACE=1 target/${BUILD_PROFILE}/databend-query -c ./.databend/databend-query-node-1-private-task.toml --internal-enable-sandbox-tenant >./.databend/query-1.out 2>&1 &

echo "Waiting on node-1..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9091

echo 'Start databend-query node-2'
env "RUST_BACKTRACE=1" nohup target/${BUILD_PROFILE}/databend-query -c ./.databend/databend-query-node-2-private-task.toml --internal-enable-sandbox-tenant >./.databend/query-2.out 2>&1 &

echo "Waiting on node-2..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9092

echo "Started 2-node cluster with private task enabled..."

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TABLE t1 (c1 int)\"}")
create_table_query_id=$(echo $response | jq -r '.id')
echo "Create Table Query ID: $create_table_query_id"

check_response_error() {
    local response="$1"
    local error_msg=$(echo "$response" | jq -r 'if .state == "Failed" then .error.message else empty end')

    if [ -n "$error_msg" ]; then
        echo "[Test Error] $error_msg" >&2
        exit 1
    fi
}

query_sql_with_auth() {
    local auth="$1"
    local sql="$2"

    curl -s -u "$auth" -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "$(jq -nc --arg sql "$sql" '{sql: $sql}')"
}

response=$(query_sql_with_auth "root:" "SET GLOBAL enterprise_license = '${QUERY_DATABEND_ENTERPRISE_LICENSE}'")
check_response_error "$response"
echo "Set enterprise license for private task tests"

response=$(query_sql_with_auth "root:" "SELECT value FROM system.configs WHERE \"group\" = 'task' AND name = 'on'")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" = "true" ]; then
    echo "✅ system.configs exposes private task config"
else
    echo "❌ Expected system.configs to expose task.on"
    echo "Actual  : $actual"
    exit 1
fi

expected_task_history_schema='["name","id","owner","comment","schedule","warehouse","state","definition","condition_text","run_id","query_id","exception_code","exception_text","attempt_number","completed_time","scheduled_time","root_task_id","session_parameters"]'

check_task_history_schema() {
    local response="$1"
    local actual_schema=$(echo "$response" | jq -c '[.schema[].name]')

    if [ "$actual_schema" != "$expected_task_history_schema" ]; then
        echo "❌ TASK_HISTORY schema mismatch"
        echo "Expected: $expected_task_history_schema"
        echo "Actual  : $actual_schema"
        exit 1
    fi
}

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT * FROM TASK_HISTORY(SCHEDULED_TIME_RANGE_START => '2026-01-01')\"}")
state=$(echo "$response" | jq -r '.state')
error_msg=$(echo "$response" | jq -r '.error.message // ""')
if [ "$state" = "Failed" ] && [[ "$error_msg" == *"unsupported data type for SCHEDULED_TIME_RANGE_START"* ]]; then
    echo "✅ TASK_HISTORY rejects non-date scheduled-time arguments"
else
    echo "❌ Expected TASK_HISTORY with string scheduled-time argument to fail"
    echo "State: $state"
    echo "Error: $error_msg"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TABLE t_script (c1 int)\"}")
check_response_error "$response"
create_script_table_query_id=$(echo $response | jq -r '.id')
echo "Create Script Table Query ID: $create_script_table_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE DATABASE task_session_db\"}")
check_response_error "$response"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TABLE task_session_db.session_target (c1 int)\"}")
check_response_error "$response"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK task_missing_wh WAREHOUSE = 'missing_wh' SCHEDULE = 5 SECOND AS insert into t1 values(0)\"}")
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

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_script_task SCHEDULE = 3600 SECOND AS BEGIN insert into t_script values(10); insert into t_script values(20); END;\"}")
check_response_error "$response"
create_script_task_query_id=$(echo $response | jq -r '.id')
echo "Create Script Task Query ID: $create_script_task_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"EXECUTE TASK my_script_task\"}")
check_response_error "$response"
execute_script_task_query_id=$(echo $response | jq -r '.id')
echo "Execute Script Task Query ID: $execute_script_task_query_id"

sleep 5

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t_script ORDER BY c1\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["10"],["20"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Private task executes multiple statements"
else
    echo "❌ Expected private task script block to execute all statements"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK db_session_task DATABASE = 'task_session_db' AS insert into session_target values(42)\"}")
check_response_error "$response"
create_db_session_task_query_id=$(echo $response | jq -r '.id')
echo "Create DB Session Task Query ID: $create_db_session_task_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"EXECUTE TASK db_session_task\"}")
check_response_error "$response"
execute_db_session_task_query_id=$(echo $response | jq -r '.id')
echo "Execute DB Session Task Query ID: $execute_db_session_task_query_id"

sleep 5

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM task_session_db.session_target ORDER BY c1\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["42"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Private task applies DATABASE session parameter"
else
    echo "❌ Expected private task DATABASE session parameter to resolve unqualified table"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TABLE t_ms (c1 int)\"}")
check_response_error "$response"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK ms_schedule_task SCHEDULE = 500 MILLISECOND AS insert into t_ms values(1)\"}")
check_response_error "$response"
create_ms_schedule_task_query_id=$(echo $response | jq -r '.id')
echo "Create Millisecond Schedule Task Query ID: $create_ms_schedule_task_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"EXECUTE TASK ms_schedule_task\"}")
check_response_error "$response"
execute_ms_schedule_task_query_id=$(echo $response | jq -r '.id')
echo "Execute Millisecond Schedule Task Query ID: $execute_ms_schedule_task_query_id"

sleep 5

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t_ms ORDER BY c1\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["1"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Private task executes millisecond schedule task"
else
    echo "❌ Expected private task millisecond schedule task to execute"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "CREATE TASK overlap_schedule_task SCHEDULE = 500 MILLISECOND AS SELECT sleep(2)")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "ALTER TASK overlap_schedule_task RESUME")
check_response_error "$response"

sleep 1

response=$(query_sql_with_auth "root:" "SELECT next_schedule_time, last_committed_on FROM system.tasks WHERE name = 'overlap_schedule_task'")
check_response_error "$response"
initial_next_schedule_time=$(echo "$response" | jq -r '.data[0][0]')
initial_last_committed_on=$(echo "$response" | jq -r '.data[0][1]')

sleep 3

response=$(query_sql_with_auth "root:" "SELECT if(next_schedule_time > to_timestamp('${initial_next_schedule_time}'), 1, 0), if(last_committed_on > to_timestamp('${initial_last_committed_on}'), 1, 0) FROM system.tasks WHERE name = 'overlap_schedule_task'")
check_response_error "$response"
actual=$(echo "$response" | jq -c '.data')
expected='[["1","1"]]'
if [ "$actual" = "$expected" ]; then
    echo "✅ Interval scheduling advances next_schedule_time and last_committed_on"
else
    echo "❌ Expected interval scheduling metadata to advance"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SHOW TASKS")
check_response_error "$response"
show_next_schedule_time=$(echo "$response" | jq -r '.data[] | select(.[1] == "overlap_schedule_task") | .[13]')
show_last_committed_on=$(echo "$response" | jq -r '.data[] | select(.[1] == "overlap_schedule_task") | .[14]')
if [ -n "$show_next_schedule_time" ] &&
    [ "$show_next_schedule_time" != "$initial_next_schedule_time" ] &&
    [ -n "$show_last_committed_on" ] &&
    [ "$show_last_committed_on" != "$initial_last_committed_on" ]; then
    echo "✅ SHOW TASKS exposes advancing scheduling metadata"
else
    echo "❌ Expected SHOW TASKS scheduling metadata to advance"
    echo "Initial next schedule: $initial_next_schedule_time"
    echo "SHOW next schedule   : $show_next_schedule_time"
    echo "Initial commit       : $initial_last_committed_on"
    echo "SHOW commit          : $show_last_committed_on"
    exit 1
fi

sleep 5

response=$(query_sql_with_auth "root:" "ALTER TASK overlap_schedule_task SUSPEND")
check_response_error "$response"

sleep 3

response=$(query_sql_with_auth "root:" "SELECT count(*) FROM TASK_HISTORY(TASK_NAME => 'overlap_schedule_task', RESULT_LIMIT => 10) WHERE state = 'SUCCEEDED'")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" -ge 2 ]; then
    echo "✅ Slow interval task continues after previous run completes"
else
    echo "❌ Expected slow interval task to have at least 2 successful runs"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT count(*) FROM system_task.task_run a, system_task.task_run b WHERE a.task_name = 'overlap_schedule_task' AND b.task_name = 'overlap_schedule_task' AND a.run_id < b.run_id AND a.state = 'SUCCEEDED' AND b.state = 'SUCCEEDED' AND a.scheduled_at < b.completed_at AND b.scheduled_at < a.completed_at")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" = "0" ]; then
    echo "✅ Slow interval task runs do not overlap"
else
    echo "❌ Expected slow interval task runs not to overlap"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT count_if(state = 'SCHEDULED'), count(*) - count(DISTINCT run_id) FROM system_task.task_run WHERE task_name = 'overlap_schedule_task'")
check_response_error "$response"
actual=$(echo "$response" | jq -c '.data')
expected='[["0","0"]]'
if [ "$actual" = "$expected" ]; then
    echo "✅ Each trigger creates one history row with a unique run_id"
else
    echo "❌ Expected no duplicate scheduled/history rows"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT count_if(state = 'SKIPPED' AND error_message LIKE 'OVERLAPPING_EXECUTION:%') FROM system_task.task_run WHERE task_name = 'overlap_schedule_task'")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" -ge 1 ]; then
    echo "✅ Overlapping schedule windows are recorded as SKIPPED"
else
    echo "❌ Expected overlapping schedule windows to record a skip reason"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT * FROM system.task_history WHERE name = 'overlap_schedule_task' LIMIT 1")
check_response_error "$response"
check_task_history_schema "$response"

response=$(query_sql_with_auth "root:" "CREATE TASK when_false_task SCHEDULE = 500 MILLISECOND WHEN 1 = 0 AS INSERT INTO t1 VALUES (99)")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "ALTER TASK when_false_task RESUME")
check_response_error "$response"

sleep 2

response=$(query_sql_with_auth "root:" "ALTER TASK when_false_task SUSPEND")
check_response_error "$response"

sleep 1

response=$(query_sql_with_auth "root:" "SELECT count_if(state = 'SKIPPED' AND error_message LIKE 'WHEN_CONDITION_FALSE:%'), count_if(state IN ('EXECUTING', 'SUCCEEDED', 'FAILED')) FROM system_task.task_run WHERE task_name = 'when_false_task'")
check_response_error "$response"
skipped_count=$(echo "$response" | jq -r '.data[0][0]')
executed_count=$(echo "$response" | jq -r '.data[0][1]')
if [ "$skipped_count" -ge 2 ] && [ "$executed_count" = "0" ]; then
    echo "✅ False WHEN conditions are recorded without executing the task"
else
    echo "❌ Expected false WHEN conditions to produce only SKIPPED history"
    echo "Skipped : $skipped_count"
    echo "Executed: $executed_count"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT state, exception_code, exception_text FROM system.task_history WHERE name = 'when_false_task' ORDER BY scheduled_time DESC LIMIT 1")
check_response_error "$response"
history_state=$(echo "$response" | jq -r '.data[0][0]')
history_code=$(echo "$response" | jq -r '.data[0][1]')
history_reason=$(echo "$response" | jq -r '.data[0][2]')
if [ "$history_state" = "SKIPPED" ] &&
    [ "$history_code" = "0" ] &&
    [[ "$history_reason" == WHEN_CONDITION_FALSE:* ]]; then
    echo "✅ system.task_history explains why the task was skipped"
else
    echo "❌ Expected public task history to expose the skip reason"
    echo "State : $history_state"
    echo "Code  : $history_code"
    echo "Reason: $history_reason"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT count(*) FROM TASK_HISTORY(TASK_NAME => 'when_false_task', ERROR_ONLY => TRUE)")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" = "0" ]; then
    echo "✅ Skipped task runs are excluded from error-only history"
else
    echo "❌ Expected error-only history to exclude skipped task runs"
    echo "Actual: $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT count_if(c1 = 99) FROM t1")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" = "0" ]; then
    echo "✅ False WHEN condition did not execute task SQL"
else
    echo "❌ Task SQL executed despite a false WHEN condition"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "CREATE TASK when_error_task SCHEDULE = 500 MILLISECOND WHEN EXISTS (SELECT 1 FROM missing_when_source) AS SELECT 1")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "ALTER TASK when_error_task RESUME")
check_response_error "$response"

sleep 2

response=$(query_sql_with_auth "root:" "SELECT count_if(state = 'FAILED' AND error_code != 0 AND error_message LIKE 'WHEN_CONDITION_EVALUATION_ERROR:%') FROM system_task.task_run WHERE task_name = 'when_error_task'")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" -ge 2 ]; then
    echo "✅ WHEN evaluation errors are recorded and later schedules continue"
else
    echo "❌ Expected repeated WHEN evaluation failures in task history"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT state FROM system.tasks WHERE name = 'when_error_task'")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" = "Started" ]; then
    echo "✅ WHEN evaluation errors do not silently stop the scheduler"
else
    echo "❌ Expected task scheduler to remain started after WHEN errors"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "ALTER TASK when_error_task SUSPEND")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "CREATE TABLE t_failed_task_recovery (c1 int)")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "CREATE TASK failed_recovery_task SCHEDULE = 2 SECOND SUSPEND_TASK_AFTER_NUM_FAILURES = 1 AS SELECT CAST('bad' AS INT)")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "ALTER TASK failed_recovery_task RESUME")
check_response_error "$response"

sleep 5

response=$(query_sql_with_auth "root:" "SELECT state, exception_text FROM TASK_HISTORY(TASK_NAME => 'failed_recovery_task', RESULT_LIMIT => 1)")
check_response_error "$response"
state=$(echo "$response" | jq -r '.data[0][0]')
exception_text=$(echo "$response" | jq -r '.data[0][1]')
if [ "$state" = "FAILED" ] && [[ "$exception_text" == *"bad"* ]]; then
    echo "✅ Failed task records quoted error text"
else
    echo "❌ Expected failed task history to record quoted error text"
    echo "State: $state"
    echo "Exception: $exception_text"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT count(*) FROM system_task.task_run WHERE task_name = 'failed_recovery_task' AND state = 'EXECUTING' AND completed_at IS NULL")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" = "0" ]; then
    echo "✅ Failed task does not leave an executing run behind"
else
    echo "❌ Expected failed task not to block future runs"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT count(*) FROM TASK_HISTORY(TASK_NAME => 'failed_recovery_task', RESULT_LIMIT => 10) WHERE state = 'FAILED'")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
response=$(query_sql_with_auth "root:" "SHOW TASKS LIKE 'failed_recovery_task'")
check_response_error "$response"
status=$(echo "$response" | jq -r '.data[0][7]')
if [ "$actual" = "1" ] && [ "$status" = "Suspended" ]; then
    echo "✅ Failed task suspends after the configured failure count"
else
    echo "❌ Expected failed task to suspend after one configured failure"
    echo "Failed runs: $actual"
    echo "Status     : $status"
    exit 1
fi

response=$(query_sql_with_auth "root:" "ALTER TASK failed_recovery_task MODIFY AS INSERT INTO t_failed_task_recovery VALUES(1)")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "ALTER TASK failed_recovery_task RESUME")
check_response_error "$response"

actual=0
for _ in {1..20}; do
    response=$(query_sql_with_auth "root:" "SELECT count(*) FROM t_failed_task_recovery")
    check_response_error "$response"
    actual=$(echo "$response" | jq -r '.data[0][0]')
    if [ "$actual" -ge 1 ]; then
        break
    fi
    sleep 1
done

response=$(query_sql_with_auth "root:" "ALTER TASK failed_recovery_task SUSPEND")
check_response_error "$response"

if [ "$actual" -ge 1 ]; then
    echo "✅ Failed scheduled task can run again after resume"
else
    echo "❌ Expected failed scheduled task to run again after resume"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "CREATE TASK drop_cancel_task AS SELECT 1")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "SELECT id FROM system.tasks WHERE name = 'drop_cancel_task'")
check_response_error "$response"
drop_cancel_task_id=$(echo "$response" | jq -r '.data[0][0]')
other_drop_cancel_task_id=$((drop_cancel_task_id + 1000000))

response=$(query_sql_with_auth "root:" "INSERT INTO system_task.task_run (task_id, task_name, query_text, when_condition, after, comment, owner, owner_user, warehouse_name, using_warehouse_size, schedule_type, interval, interval_milliseconds, cron, time_zone, run_id, attempt_number, state, error_code, error_message, root_task_id, scheduled_at, completed_at, next_scheduled_at, error_integration, status, created_at, updated_at, session_params, last_suspended_at, suspend_task_after_num_failures) VALUES (${drop_cancel_task_id}, 'drop_cancel_task', 'SELECT 1', NULL, NULL, NULL, 'account_admin', 'root', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 910000, 0, 'EXECUTING', 0, NULL, 0, now(), NULL, NULL, NULL, 'SUSPENDED', now(), now(), parse_json('{}'), NULL, NULL)")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "INSERT INTO system_task.task_run (task_id, task_name, query_text, when_condition, after, comment, owner, owner_user, warehouse_name, using_warehouse_size, schedule_type, interval, interval_milliseconds, cron, time_zone, run_id, attempt_number, state, error_code, error_message, root_task_id, scheduled_at, completed_at, next_scheduled_at, error_integration, status, created_at, updated_at, session_params, last_suspended_at, suspend_task_after_num_failures) VALUES (${other_drop_cancel_task_id}, 'drop_cancel_task', 'SELECT 1', NULL, NULL, NULL, 'account_admin', 'root', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 910001, 0, 'EXECUTING', 0, NULL, 0, now(), NULL, NULL, NULL, 'SUSPENDED', now(), now(), parse_json('{}'), NULL, NULL)")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "DROP TASK drop_cancel_task")
check_response_error "$response"

actual=0
for _ in {1..10}; do
    response=$(query_sql_with_auth "root:" "SELECT count(*) FROM system_task.task_run WHERE task_name = 'drop_cancel_task' AND task_id = ${drop_cancel_task_id} AND state = 'CANCELLED' AND completed_at IS NOT NULL")
    check_response_error "$response"
    actual=$(echo "$response" | jq -r '.data[0][0]')
    if [ "$actual" = "1" ]; then
        break
    fi
    sleep 1
done

if [ "$actual" = "1" ]; then
    echo "✅ Dropping a task cancels open executing task runs"
else
    echo "❌ Expected DROP TASK to cancel open executing task runs"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT count(*) FROM system_task.task_run WHERE task_name = 'drop_cancel_task' AND task_id = ${other_drop_cancel_task_id} AND state = 'EXECUTING' AND completed_at IS NULL")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" = "1" ]; then
    echo "✅ Dropping a task does not cancel open runs from a different task generation"
else
    echo "❌ Expected DROP TASK to preserve open runs from a different task generation"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "UPDATE system_task.task_run SET state = 'CANCELLED', completed_at = now() WHERE task_name = 'drop_cancel_task' AND task_id = ${other_drop_cancel_task_id}")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "CREATE TASK drop_cancel_task AS SELECT 1")
check_response_error "$response"
response=$(query_sql_with_auth "root:" "EXECUTE TASK drop_cancel_task")
check_response_error "$response"

actual=0
for _ in {1..10}; do
    response=$(query_sql_with_auth "root:" "SELECT count(*) FROM system_task.task_run WHERE task_name = 'drop_cancel_task' AND state = 'SUCCEEDED'")
    check_response_error "$response"
    actual=$(echo "$response" | jq -r '.data[0][0]')
    if [ "$actual" = "1" ]; then
        break
    fi
    sleep 1
done

if [ "$actual" = "1" ]; then
    echo "✅ Recreated task can run after dropped task cancels its open run"
else
    echo "❌ Expected recreated task to run after dropping the stale open run"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "CREATE TASK manual_cancel_task AS SELECT 1")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "SELECT id FROM system.tasks WHERE name = 'manual_cancel_task'")
check_response_error "$response"
manual_cancel_task_id=$(echo "$response" | jq -r '.data[0][0]')
other_manual_cancel_task_id=$((manual_cancel_task_id + 1000000))

response=$(query_sql_with_auth "root:" "INSERT INTO system_task.task_run (task_id, task_name, query_text, when_condition, after, comment, owner, owner_user, warehouse_name, using_warehouse_size, schedule_type, interval, interval_milliseconds, cron, time_zone, run_id, attempt_number, state, error_code, error_message, root_task_id, scheduled_at, completed_at, next_scheduled_at, error_integration, status, created_at, updated_at, session_params, last_suspended_at, suspend_task_after_num_failures) VALUES (${manual_cancel_task_id}, 'manual_cancel_task', 'SELECT 1', NULL, NULL, NULL, 'account_admin', 'root', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 920000, 0, 'EXECUTING', 0, NULL, 0, now(), NULL, NULL, NULL, 'SUSPENDED', now(), now(), parse_json('{}'), NULL, NULL)")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "INSERT INTO system_task.task_run (task_id, task_name, query_text, when_condition, after, comment, owner, owner_user, warehouse_name, using_warehouse_size, schedule_type, interval, interval_milliseconds, cron, time_zone, run_id, attempt_number, state, error_code, error_message, root_task_id, scheduled_at, completed_at, next_scheduled_at, error_integration, status, created_at, updated_at, session_params, last_suspended_at, suspend_task_after_num_failures) VALUES (${other_manual_cancel_task_id}, 'manual_cancel_task', 'SELECT 1', NULL, NULL, NULL, 'account_admin', 'root', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 920001, 0, 'EXECUTING', 0, NULL, 0, now(), NULL, NULL, NULL, 'SUSPENDED', now(), now(), parse_json('{}'), NULL, NULL)")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "CALL SYSTEM\$USER_TASK_CANCEL_ONGOING_EXECUTIONS('manual_cancel_task')")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [[ "$actual" == "Cancelled 1 ongoing execution(s) for task manual_cancel_task." ]]; then
    echo "✅ SYSTEM\$USER_TASK_CANCEL_ONGOING_EXECUTIONS returns cancelled run count"
else
    echo "❌ Expected task cancel system function to report one cancelled run"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT count(*) FROM system_task.task_run WHERE task_name = 'manual_cancel_task' AND task_id = ${manual_cancel_task_id} AND state = 'CANCELLED' AND completed_at IS NOT NULL")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" = "1" ]; then
    echo "✅ SYSTEM\$USER_TASK_CANCEL_ONGOING_EXECUTIONS cancels open task runs"
else
    echo "❌ Expected task cancel system function to cancel open executing task runs"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT count(*) FROM system_task.task_run WHERE task_name = 'manual_cancel_task' AND task_id = ${other_manual_cancel_task_id} AND state = 'EXECUTING' AND completed_at IS NULL")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" = "1" ]; then
    echo "✅ SYSTEM\$USER_TASK_CANCEL_ONGOING_EXECUTIONS preserves newer task generations"
else
    echo "❌ Expected task cancel system function to preserve newer task generations"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "UPDATE system_task.task_run SET state = 'CANCELLED', completed_at = now() WHERE task_name = 'manual_cancel_task' AND task_id = ${other_manual_cancel_task_id}")
check_response_error "$response"
response=$(query_sql_with_auth "root:" "DROP TASK manual_cancel_task")
check_response_error "$response"

# Check Task Fan-out

response=$(query_sql_with_auth "root:" "CREATE OR REPLACE TABLE fanout_sink (child string)")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "CREATE TASK fanout_root AS SELECT 1")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "CREATE TASK fanout_child_a AFTER 'fanout_root' AS INSERT INTO fanout_sink VALUES ('a')")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "CREATE TASK fanout_child_b AFTER 'fanout_root' AS INSERT INTO fanout_sink VALUES ('b')")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "CREATE TASK fanout_child_c AFTER 'fanout_root' AS INSERT INTO fanout_sink VALUES ('c')")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "ALTER TASK fanout_child_a RESUME")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "ALTER TASK fanout_child_b RESUME")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "ALTER TASK fanout_child_c RESUME")
check_response_error "$response"

actual_after_count=0
for _ in {1..20}; do
    response=$(query_sql_with_auth "root:" "SELECT count(*) FROM system_task.task_after WHERE task_name = 'fanout_root' AND next_task IN ('fanout_child_a', 'fanout_child_b', 'fanout_child_c')")
    check_response_error "$response"
    actual_after_count=$(echo "$response" | jq -r '.data[0][0]')
    if [ "$actual_after_count" = "3" ]; then
        break
    fi
    sleep 1
done

if [ "$actual_after_count" = "3" ]; then
    echo "✅ Private task fan-out AFTER bindings are ready"
else
    echo "❌ Expected private task fan-out AFTER bindings to be ready"
    echo "Expected: 3"
    echo "Actual  : $actual_after_count"
    exit 1
fi

response=$(query_sql_with_auth "root:" "EXECUTE TASK fanout_root")
check_response_error "$response"

actual='[]'
for _ in {1..20}; do
    response=$(query_sql_with_auth "root:" "SELECT child FROM fanout_sink ORDER BY child")
    check_response_error "$response"
    actual=$(echo "$response" | jq -c '.data')
    if [ "$actual" = '[["a"],["b"],["c"]]' ]; then
        break
    fi
    sleep 1
done

expected='[["a"],["b"],["c"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Private task fan-out triggers all successors"
else
    echo "❌ Expected private task fan-out to trigger all successors"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "UPDATE system_task.task_run SET state = 'SKIPPED', error_code = 0, error_message = 'OVERLAPPING_EXECUTION: test', completed_at = to_timestamp(4102444800) WHERE task_name = 'fanout_root'")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "EXECUTE TASK fanout_root")
check_response_error "$response"

actual=0
for _ in {1..20}; do
    response=$(query_sql_with_auth "root:" "SELECT count(*) FROM fanout_sink")
    check_response_error "$response"
    actual=$(echo "$response" | jq -r '.data[0][0]')
    if [ "$actual" = "6" ]; then
        break
    fi
    sleep 1
done

if [ "$actual" = "6" ]; then
    echo "✅ Overlap skips do not mask successful parent runs"
else
    echo "❌ Expected successful parent run to release all successors after an overlap skip"
    echo "Expected: 6"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_1 SCHEDULE = 5 SECOND AS insert into t1 values(0)\"}")
check_response_error "$response"
create_task_1_query_id=$(echo $response | jq -r '.id')
echo "Create Task 1 Query ID: $create_task_1_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_2 SCHEDULE = 25 SECOND AS insert into t1 values(1)\"}")
check_response_error "$response"
create_task_2_query_id=$(echo $response | jq -r '.id')
echo "Create Task 2 ID: $create_task_2_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_3 AFTER 'my_task_1', 'my_task_2' AS insert into t1 values(2)\"}")
check_response_error "$response"
create_task_3_query_id=$(echo $response | jq -r '.id')
echo "Create Task 3 ID: $create_task_3_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SHOW TASKS\"}")
check_response_error "$response"
task_ids=$(echo "$response" | jq -r '.data[] | select(.[1] == "my_task_1" or .[1] == "my_task_2" or .[1] == "my_task_3") | .[2]')
task_id_count=$(echo "$task_ids" | sed '/^$/d' | wc -l)
zero_task_id_count=$(echo "$task_ids" | awk '$1 == "0" { count++ } END { print count + 0 }')
unique_task_id_count=$(echo "$task_ids" | sed '/^$/d' | sort -u | wc -l)
if [ "$task_id_count" -eq 3 ] && [ "$zero_task_id_count" -eq 0 ] && [ "$unique_task_id_count" -eq 3 ]; then
    echo "✅ Private task ids are assigned"
else
    echo "❌ Expected private task ids to be non-zero and unique"
    echo "Task IDs:"
    echo "$task_ids"
    exit 1
fi

sleep 5

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK my_task_3 RESUME\"}")
check_response_error "$response"
alter_task_3_query_id=$(echo $response | jq -r '.id')
echo "Resume Task 3 ID: $alter_task_3_query_id"

sleep 5

# Check Task Afters 1

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"EXECUTE TASK my_task_1\"}")
check_response_error "$response"
execute_task_1_query_id=$(echo $response | jq -r '.id')
echo "Execute Task 1 ID: $execute_task_1_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"EXECUTE TASK my_task_2\"}")
check_response_error "$response"
execute_task_2_query_id=$(echo $response | jq -r '.id')
echo "Execute Task 2 ID: $execute_task_2_query_id"

sleep 25

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t1 ORDER BY c1\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["0"],["1"],["2"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

# Check Task Afters With Schedule Root

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"EXECUTE TASK my_task_1\"}")
check_response_error "$response"
execute_task_1_query_id=$(echo $response | jq -r '.id')
echo "Execute Task 1 ID: $execute_task_1_query_id"

sleep 25

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t1 ORDER BY c1\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["0"],["0"],["1"],["2"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK my_task_2 RESUME\"}")
check_response_error "$response"
alter_task_2_query_id=$(echo $response | jq -r '.id')
echo "Resume Task 2 ID: $alter_task_2_query_id"

sleep 40

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t1 ORDER BY c1\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["0"],["0"],["1"],["1"],["2"],["2"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

# Test whether the schedule can be restored after restart

killall -9 databend-query || true

echo 'Start databend-query node-1'
nohup env RUST_BACKTRACE=1 target/${BUILD_PROFILE}/databend-query -c ./.databend/databend-query-node-1-private-task.toml --internal-enable-sandbox-tenant >./.databend/query-1.out 2>&1 &

echo "Waiting on node-1..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9091

echo 'Start databend-query node-2'
env "RUST_BACKTRACE=1" nohup target/${BUILD_PROFILE}/databend-query -c ./.databend/databend-query-node-2-private-task.toml --internal-enable-sandbox-tenant >./.databend/query-2.out 2>&1 &

echo "Waiting on node-2..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9092

echo "Started 2-node cluster with private task enabled..."

sleep 45

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t1 ORDER BY c1\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["0"],["0"],["1"],["1"],["1"],["2"],["2"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

# Show Task
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"Describe Task my_task_1\"}")
check_response_error "$response"
state=$(echo "$response" | jq -r '.state')
if [ "$state" != "Succeeded" ]; then
  echo "❌ Failed"
  exit 1
fi
actual=$(echo "$response" | jq -c '.data')
echo "\n\nDescribe Task my_task_1: $actual"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SHOW TASKS\"}")
check_response_error "$response"
state=$(echo "$response" | jq -r '.state')
if [ "$state" != "Succeeded" ]; then
  echo "❌ Failed"
  exit 1
fi
actual=$(echo "$response" | jq -c '.data')
echo "\n\nSHOW TASKS: $actual"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT * FROM system.task_history\"}")
check_response_error "$response"
state=$(echo "$response" | jq -r '.state')
if [ "$state" != "Succeeded" ]; then
  echo "❌ Failed"
  exit 1
fi
actual=$(echo "$response" | jq -c '.data')
echo "\n\nSELECT * FROM system.task_history: $actual"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT * FROM TASK_HISTORY(TASK_NAME => 'db_session_task', RESULT_LIMIT => 1)\"}")
check_response_error "$response"
state=$(echo "$response" | jq -r '.state')
if [ "$state" != "Succeeded" ]; then
    echo "❌ Failed"
    exit 1
fi
check_task_history_schema "$response"
actual=$(echo "$response" | jq -c '.data')
name=$(echo "$response" | jq -r '.data[0][0]')
state=$(echo "$response" | jq -r '.data[0][6]')
definition=$(echo "$response" | jq -r '.data[0][7]')
scheduled_time=$(echo "$response" | jq -r '.data[0][15]')
session_parameters=$(echo "$response" | jq -c '.data[0][17]')
if [ "$name" = "db_session_task" ] &&
    [ "$state" = "SUCCEEDED" ] &&
    [[ "$definition" == *"session_target"* ]] &&
    [ -n "$scheduled_time" ] &&
    [[ "$session_parameters" == *"task_session_db"* ]]; then
    echo "✅ TASK_HISTORY table function matches cloud-style schema and content"
else
    echo "❌ Expected TASK_HISTORY table function to return cloud-style db_session_task history"
    echo "Name: $name"
    echo "State: $state"
    echo "Definition: $definition"
    echo "Scheduled Time: $scheduled_time"
    echo "Session Parameters: $session_parameters"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT * FROM TASK_HISTORY(TASK_NAME => 'db_session_task', SCHEDULED_TIME_RANGE_START => to_date('2020-01-01'), SCHEDULED_TIME_RANGE_END => to_timestamp('2999-01-01'), RESULT_LIMIT => 1)\"}")
check_response_error "$response"
state=$(echo "$response" | jq -r '.state')
if [ "$state" != "Succeeded" ]; then
    echo "❌ Failed"
    exit 1
fi
check_task_history_schema "$response"
actual=$(echo "$response" | jq -c '.data')
name=$(echo "$response" | jq -r '.data[0][0]')
state=$(echo "$response" | jq -r '.data[0][6]')
scheduled_time=$(echo "$response" | jq -r '.data[0][15]')
if [ "$name" = "db_session_task" ] &&
    [ "$state" = "SUCCEEDED" ] &&
    [ -n "$scheduled_time" ]; then
    echo "✅ TASK_HISTORY accepts date/timestamp scheduled-time arguments"
else
    echo "❌ Expected TASK_HISTORY with date/timestamp scheduled-time arguments to return db_session_task history"
    echo "Name: $name"
    echo "State: $state"
    echo "Scheduled Time: $scheduled_time"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT * FROM TASK_HISTORY(TASK_NAME => 'ms_schedule_task', RESULT_LIMIT => 1)\"}")
check_response_error "$response"
state=$(echo "$response" | jq -r '.state')
if [ "$state" != "Succeeded" ]; then
    echo "❌ Failed"
    exit 1
fi
check_task_history_schema "$response"
actual=$(echo "$response" | jq -c '.data')
name=$(echo "$response" | jq -r '.data[0][0]')
schedule=$(echo "$response" | jq -r '.data[0][4]')
if [ "$name" = "ms_schedule_task" ] &&
    [ "$schedule" = "INTERVAL 0 SECOND 500 MILLISECOND" ]; then
    echo "✅ TASK_HISTORY preserves millisecond schedule"
else
    echo "❌ Expected TASK_HISTORY to preserve millisecond schedule"
    echo "Name: $name"
    echo "Schedule: $schedule"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "INSERT INTO system_task.task_run (task_id, task_name, query_text, when_condition, after, comment, owner, owner_user, warehouse_name, using_warehouse_size, schedule_type, interval, interval_milliseconds, cron, time_zone, run_id, attempt_number, state, error_code, error_message, root_task_id, scheduled_at, completed_at, next_scheduled_at, error_integration, status, created_at, updated_at, session_params, last_suspended_at, suspend_task_after_num_failures) SELECT 900000 + number, 'limit_history_task', 'SELECT 1', NULL, NULL, NULL, 'account_admin', 'root', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 900000 + number, 0, 'SUCCEEDED', 0, NULL, 0, to_timestamp(1700000000 + number), to_timestamp(1700000000 + number), NULL, NULL, 'SUSPENDED', to_timestamp(1700000000 + number), to_timestamp(1700000000 + number), parse_json('{}'), NULL, NULL FROM numbers(101)")
check_response_error "$response"

response=$(query_sql_with_auth "root:" "SELECT count(*) FROM TASK_HISTORY(TASK_NAME => 'limit_history_task')")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" = "100" ]; then
    echo "✅ TASK_HISTORY applies the default result limit"
else
    echo "❌ Expected TASK_HISTORY without RESULT_LIMIT to return 100 rows"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "SELECT count(*) FROM TASK_HISTORY(TASK_NAME => 'limit_history_task', RESULT_LIMIT => 100000)")
check_response_error "$response"
actual=$(echo "$response" | jq -r '.data[0][0]')
if [ "$actual" = "101" ]; then
    echo "✅ TASK_HISTORY clamps explicit result limits without dropping valid rows"
else
    echo "❌ Expected TASK_HISTORY with large RESULT_LIMIT to return all 101 matching rows"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "root:" "CREATE ROLE task_history_role_a")
check_response_error "$response"
response=$(query_sql_with_auth "root:" "CREATE ROLE task_history_role_b")
check_response_error "$response"
response=$(query_sql_with_auth "root:" "CREATE USER task_history_user_a IDENTIFIED BY 'task_history_pwd' WITH DEFAULT_ROLE='task_history_role_a'")
check_response_error "$response"
response=$(query_sql_with_auth "root:" "CREATE USER task_history_user_b IDENTIFIED BY 'task_history_pwd' WITH DEFAULT_ROLE='task_history_role_b'")
check_response_error "$response"
response=$(query_sql_with_auth "root:" "GRANT ROLE task_history_role_a TO task_history_user_a")
check_response_error "$response"
response=$(query_sql_with_auth "root:" "GRANT ROLE task_history_role_b TO task_history_user_b")
check_response_error "$response"
response=$(query_sql_with_auth "root:" "GRANT SUPER ON *.* TO ROLE task_history_role_a")
check_response_error "$response"
response=$(query_sql_with_auth "root:" "GRANT SUPER ON *.* TO ROLE task_history_role_b")
check_response_error "$response"

response=$(query_sql_with_auth "task_history_user_a:task_history_pwd" "CREATE TASK role_a_history_task AS SELECT 1")
check_response_error "$response"
response=$(query_sql_with_auth "task_history_user_a:task_history_pwd" "EXECUTE TASK role_a_history_task")
check_response_error "$response"
response=$(query_sql_with_auth "task_history_user_b:task_history_pwd" "CREATE TASK role_b_history_task AS SELECT 1")
check_response_error "$response"
response=$(query_sql_with_auth "task_history_user_b:task_history_pwd" "EXECUTE TASK role_b_history_task")
check_response_error "$response"

sleep 5

response=$(query_sql_with_auth "task_history_user_a:task_history_pwd" "SELECT * FROM TASK_HISTORY(TASK_NAME => 'role_a_history_task', RESULT_LIMIT => 1)")
check_response_error "$response"
state=$(echo "$response" | jq -r '.state')
if [ "$state" != "Succeeded" ]; then
    echo "❌ Failed"
    exit 1
fi
check_task_history_schema "$response"
actual=$(echo "$response" | jq -c '.data')
name=$(echo "$response" | jq -r '.data[0][0]')
owner=$(echo "$response" | jq -r '.data[0][2]')
state=$(echo "$response" | jq -r '.data[0][6]')
definition=$(echo "$response" | jq -r '.data[0][7]')
scheduled_time=$(echo "$response" | jq -r '.data[0][15]')
if [ "$name" = "role_a_history_task" ] &&
    [ "$owner" = "task_history_role_a" ] &&
    [ "$state" = "SUCCEEDED" ] &&
    [ "$definition" = "SELECT 1" ] &&
    [ -n "$scheduled_time" ]; then
    echo "✅ TASK_HISTORY returns history for an available owner role"
else
    echo "❌ Expected TASK_HISTORY to return role_a_history_task for task_history_role_a"
    echo "Name: $name"
    echo "Owner: $owner"
    echo "State: $state"
    echo "Definition: $definition"
    echo "Scheduled Time: $scheduled_time"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "task_history_user_b:task_history_pwd" "SELECT * FROM TASK_HISTORY(TASK_NAME => 'role_a_history_task', RESULT_LIMIT => 1)")
check_response_error "$response"
state=$(echo "$response" | jq -r '.state')
if [ "$state" != "Succeeded" ]; then
    echo "❌ Failed"
    exit 1
fi
check_task_history_schema "$response"
actual=$(echo "$response" | jq -c '.data')
if [ "$actual" = "[]" ]; then
    echo "✅ TASK_HISTORY hides explicitly named tasks owned by unavailable roles"
else
    echo "❌ Expected TASK_HISTORY to hide role_a_history_task from task_history_role_b"
    echo "Actual  : $actual"
    exit 1
fi

response=$(query_sql_with_auth "task_history_user_b:task_history_pwd" "SELECT name, owner FROM TASK_HISTORY(RESULT_LIMIT => 100) WHERE name IN ('role_a_history_task', 'role_b_history_task') ORDER BY name")
check_response_error "$response"
state=$(echo "$response" | jq -r '.state')
if [ "$state" != "Succeeded" ]; then
    echo "❌ Failed"
    exit 1
fi
actual=$(echo "$response" | jq -c '.data')
expected='[["role_b_history_task","task_history_role_b"]]'
if [ "$actual" = "$expected" ]; then
    echo "✅ TASK_HISTORY filters unqualified history by available owner roles"
else
    echo "❌ Expected TASK_HISTORY without TASK_NAME to only return available owner roles"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

# Drop Task
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"DROP TASK my_task_1\"}")
check_response_error "$response"
state=$(echo "$response" | jq -r '.state')
if [ "$state" != "Succeeded" ]; then
  echo "❌ Failed"
  exit 1
else
  echo "✅ Passed"
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"EXECUTE TASK my_task_1\"}")
state=$(echo "$response" | jq -r '.state')
if [ "$state" = "Succeeded" ]; then
  echo "❌ Failed"
  exit 1
else
  echo "✅ Passed"
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TABLE t2 (c1 int)\"}")
check_response_error "$response"
create_table_query_id_1=$(echo $response | jq -r '.id')
echo "Create Table Query ID: $create_table_query_id_1"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_4 SCHEDULE = USING CRON '*/25 * * * * *' 'UTC' AS insert into t2 values(0)\"}")
check_response_error "$response"
create_task_4_query_id=$(echo $response | jq -r '.id')
echo "Create Task 4 Query ID: $create_task_4_query_id"

sleep 5

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK my_task_4 RESUME\"}")
check_response_error "$response"
alter_task_4_query_id=$(echo $response | jq -r '.id')
echo "Resume Task 4 ID: $alter_task_4_query_id"

sleep 80

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT count(*) FROM t2\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -r '.data[0][0]')

if [ "$actual" -ge 3 ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected at least 3 cron task runs"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT * FROM TASK_HISTORY(TASK_NAME => 'my_task_4', RESULT_LIMIT => 1)\"}")
check_response_error "$response"
state=$(echo "$response" | jq -r '.state')
if [ "$state" != "Succeeded" ]; then
    echo "❌ Failed"
    exit 1
fi
check_task_history_schema "$response"
actual=$(echo "$response" | jq -c '.data')
name=$(echo "$response" | jq -r '.data[0][0]')
schedule=$(echo "$response" | jq -r '.data[0][4]')
if [ "$name" = "my_task_4" ] &&
    [ "$schedule" = "CRON */25 * * * * * TIMEZONE UTC" ]; then
    echo "✅ TASK_HISTORY preserves cron timezone schedule"
else
    echo "❌ Expected TASK_HISTORY to preserve cron timezone schedule"
    echo "Name: $name"
    echo "Schedule: $schedule"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT * FROM system.tasks\"}")
check_response_error "$response"
state=$(echo "$response" | jq -r '.state')
if [ "$state" != "Succeeded" ]; then
  echo "❌ Failed"
  exit 1
fi
actual=$(echo "$response" | jq -c '.data')
echo "\n\nSELECT * FROM system.tasks: $actual"

# Test Task When on After & Schedule & Execute
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE OR REPLACE TABLE t3 (c1 int, c2 int)\"}")
check_response_error "$response"
create_table_query_id_2=$(echo $response | jq -r '.id')
echo "Create Table Query ID: $create_table_query_id_2"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_5 SCHEDULE = 25 SECOND WHEN EXISTS (SELECT 1 FROM t3 WHERE c2 = 1) AS insert into t3 values(1, 0)\"}")
check_response_error "$response"
create_task_5_query_id=$(echo $response | jq -r '.id')
echo "Create Task 5 Query ID: $create_task_5_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_6 SCHEDULE = 25 SECOND WHEN EXISTS (SELECT 1 FROM t3 WHERE c2 = 1) AS insert into t3 values(2, 0)\"}")
check_response_error "$response"
create_task_6_query_id=$(echo $response | jq -r '.id')
echo "Create Task 6 Query ID: $create_task_6_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"CREATE TASK my_task_7 AFTER 'my_task_5', 'my_task_6' WHEN EXISTS (SELECT 1 FROM t3 WHERE c2 = 2)  AS insert into t3 values(3, 0)\"}")
check_response_error "$response"
create_task_7_query_id=$(echo $response | jq -r '.id')
echo "Create Task 7 Query ID: $create_task_7_query_id"

sleep 5

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK my_task_5 RESUME\"}")
check_response_error "$response"
alter_task_5_query_id=$(echo $response | jq -r '.id')
echo "Resume Task 5 ID: $alter_task_5_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK my_task_6 RESUME\"}")
check_response_error "$response"
alter_task_6_query_id=$(echo $response | jq -r '.id')
echo "Resume Task 6 ID: $alter_task_6_query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"ALTER TASK my_task_7 RESUME\"}")
check_response_error "$response"
alter_task_7_query_id=$(echo $response | jq -r '.id')
echo "Resume Task 7 ID: $alter_task_7_query_id"

sleep 30

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1 FROM t3 ORDER BY c1\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"INSERT INTO t3 VALUES (1, 1)\"}")
check_response_error "$response"
insert_t3_query_id=$(echo $response | jq -r '.id')
echo "INSERT T3 (1, 1) ID: $insert_t3_query_id"

sleep 25

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1, c2 FROM t3 ORDER BY c1, c2\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["1","0"],["1","1"],["2","0"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"EXECUTE TASK my_task_7\"}")
check_response_error "$response"
execute_task_7_query_id=$(echo $response | jq -r '.id')
echo "Execute Task 7 ID: $execute_task_7_query_id"

sleep 5

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1, c2 FROM t3 ORDER BY c1, c2\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["1","0"],["1","1"],["2","0"],["3","0"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"INSERT INTO t3 VALUES (2, 2)\"}")
check_response_error "$response"
insert_t3_query_id_1=$(echo $response | jq -r '.id')
echo "INSERT T3 (2, 2) ID: $insert_t3_query_id_1"

sleep 30

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"SELECT c1, c2 FROM t3 ORDER BY c1, c2\"}")
check_response_error "$response"

actual=$(echo "$response" | jq -c '.data')
expected='[["1","0"],["1","0"],["1","1"],["2","0"],["2","0"],["2","2"],["3","0"],["3","0"]]'

if [ "$actual" = "$expected" ]; then
    echo "✅ Query result matches expected"
else
    echo "❌ Mismatch"
    echo "Expected: $expected"
    echo "Actual  : $actual"
    exit 1
fi
