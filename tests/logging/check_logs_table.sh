#!/bin/bash

set -e

function check_query_log() {
  local count=$1
  local query_id=$2
  local check_query=$3
  local expected_result=$4

  local full_sql_query
  full_sql_query="$check_query"

  if [ -n "$query_id" ] && [ "$query_id" != "null" ]; then
    full_sql_query+=" query_id = '$query_id'"
  fi

  echo $full_sql_query
  response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
    -H 'Content-Type: application/json' \
    -d "{\"sql\": \"$full_sql_query\", \"pagination\": {\"wait_time_secs\": 10}}")

  result=$(echo $response | jq -r '.data[0][0]' | tr -d '"')
  if [ "$result" != "$expected_result" ]; then
    echo "Log table test #$count failed, Result: $result, Expected: $expected_result"
    exit 1
  else
    echo "Log table test #$count passed, Result: $result, Expected: $expected_result"
  fi
}

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"set global timezone='Asia/Shanghai'\"}")

# Execute the initial query and get the query_id
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select 123"}')
query_id=$(echo $response | jq -r '.id')
echo "Query ID: $query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "drop table if exists t"}')
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "create table t (a INT)"}')
create_query_id=$(echo $response | jq -r '.id')
echo "Create Query ID: $create_query_id"
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "create view v as select a from t"}')
create_view_query_id=$(echo $response | jq -r '.id')
echo "Create VIEW Query ID: $create_view_query_id"
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "insert into t values (1),(2),(3)"}')
insert_query_id=$(echo $response | jq -r '.id')
echo "Insert Query ID: $insert_query_id"
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json'  -H 'X-Databend-Client-Caps: session_cookie' -d '{"sql": "select * from t"}')
select_query_id=$(echo $response | jq -r '.id')
select_session_id=$(echo $response | jq -r '.session_id')
echo "Select Query ID: $select_query_id"
echo "Select Session ID: $select_session_id"

sleep 2
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select 123"}')
# Wait for the query to be logged
sleep 13


# Test 1
check_query_log "basic-1" "$query_id" "SELECT count(*) FROM system_history.log_history WHERE target = 'databend::log::profile' and" "1"

# Test 2
check_query_log "basic-2" "$query_id" "SELECT count(*) FROM system_history.profile_history WHERE" "1"

# Test 3
check_query_log "basic-3" "$query_id" "SELECT count(*) FROM system_history.log_history WHERE target = 'databend::log::query' and" "2"

# Test 4
check_query_log "basic-4" "$query_id" "SELECT count(*) FROM system_history.query_history WHERE" "1"


# Test 6
check_query_log "basic-6" "$select_query_id" "SELECT count(*) FROM system_history.access_history WHERE" "1"

# Test 7
check_query_log "basic-7" "$create_query_id" "SELECT object_modified_by_ddl[0]['object_name'] FROM system_history.access_history WHERE" "default.default.t"

# Test 8
check_query_log "basic-8" "$insert_query_id" "SELECT objects_modified[0]['object_name'] FROM system_history.access_history WHERE" "default.default.t"

# Test 9
check_query_log "basic-9" "$select_query_id" "SELECT base_objects_accessed[0]['object_name'] FROM system_history.access_history WHERE" "default.default.t"

# Test 10
check_query_log "basic-10" $create_view_query_id "select query_text from system_history.query_history where" "CREATE VIEW v AS SELECT a FROM t"

# Test 11
check_query_log "basic-11" null "SELECT count(*) FROM system_history.login_history WHERE session_id = '$select_session_id' " "1"

# Check timezone, regression test for https://github.com/databendlabs/databend/pull/18059
check_query_log "t-1" "$select_query_id" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, timestamp, now()) FROM system_history.log_history WHERE target = 'databend::log::profile' and" "0"

check_query_log "t-2" "$select_query_id" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, event_time, now()) FROM system_history.query_history WHERE" "0"

check_query_log "t-3" "$select_query_id" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, timestamp, now()) FROM system_history.profile_history WHERE" "0"

check_query_log "t-4" null "settings (timezone='Asia/Shanghai') SELECT sum(DATE_DIFF(hour, event_time, now())) FROM system_history.login_history" "0"

check_query_log "t-5" "$select_query_id" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, query_start, now()) FROM system_history.access_history WHERE" "0"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "drop user if exists a"}')
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "drop role if exists ra"}')
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"create user a identified by '123' with default_role='ra'\"}")
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "create role ra"}')
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "grant select , drop on system_history.* to role ra"}')
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "grant alter on system_history.* to role ra"}')
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "grant role ra to a"}')
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "grant write, read on stage log_1f93b76af0bd4b1d8e018667865fbc65 to a"}')

execute_and_verify() {
    local cmd_description="$1"
    local user_cred="$2"
    local json_payload="$3"
    local jq_expression="$4"
    local result

    echo "Executing: $cmd_description ... "

    result=$(curl -s -u "${user_cred}" -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "${json_payload}" | jq -r "${jq_expression}")

    if [[ "$result" != "true" ]]; then
        echo "Failed! Expected result: true, actual result: $result."
        echo "$cmd_description failed."
        exit 1 # Exit script immediately if it fails
    else
        echo "Description: $cmd_description completed successfully."
    fi
}

check_system_history_permissions() {
    # Command 1: User 'a:123' attempts to create a table in 'system_history' (expected to fail)
    execute_and_verify \
        "User 'a:123' attempts to create a table in 'system_history'" \
        "a:123" \
        '{"sql": "create table system_history.t (a INT)"}' \
        '.state == "Failed"'

    # Command 2: User 'root:' attempts to modify 'system_history.log_history' table (expected to fail)
    execute_and_verify \
        "User 'root:' attempts to modify 'system_history.log_history' table" \
        "root:" \
        '{"sql": "alter table system_history.log_history add column id int"}' \
        '.state == "Failed"'

    # Command 3: User 'a:123' drop 'system_history.access_history' table (expected to succeed)
    execute_and_verify \
        "User 'a:123' drop 'system_history.access_history' table" \
        "a:123" \
        '{"sql": "drop table system_history.access_history"}' \
        '.state == "Succeeded"'

    # Command 4: User 'root:' grant ownership on 'system_history.*' (expected to fail)
    execute_and_verify \
        "User 'root:' grant ownership on 'system_history.*'" \
        "root:" \
        '{"sql": "grant ownership on system_history.* to role ra"}' \
        '.state == "Failed"'

    # Command 5: User 'root:' grant ownership on 'system_history.query_history' (expected to fail)
    execute_and_verify \
        "User 'root:' grant ownership on 'system_history.query_history'" \
        "root:" \
        '{"sql": "grant ownership on system_history.query_history to role ra"}' \
        '.state == "Failed"'
    # Command 6: User 'a:123' select 'system_history.query_history' table (expected to succeed)
        execute_and_verify \
            "User 'a:123' query 'system_history.query_history' table" \
            "a:123" \
            '{"sql": "select count() from system_history.query_history"}' \
            '.state == "Succeeded"'

    # Command 7: User 'a:123' drop stage 'log_1f93b76af0bd4b1d8e018667865fbc65' table (expected to failed)
            execute_and_verify \
                "User 'a:123' drop stage log_1f93b76af0bd4b1d8e018667865fbc65" \
                "a:123" \
                '{"sql": "drop stage log_1f93b76af0bd4b1d8e018667865fbc65"}' \
                '.state == "Failed"'

    # Command 8: User 'a:123' copy into @log_1f93b76af0bd4b1d8e018667865fbc65 from (select * from system.one) (expected to failed)
                execute_and_verify \
                    "User 'a:123' copy into @log_1f93b76af0bd4b1d8e018667865fbc65 from (select * from system.one);" \
                    "a:123" \
                    '{"sql": "copy into @log_1f93b76af0bd4b1d8e018667865fbc65 from (select * from system.one);"}' \
                    '.state == "Failed"'

    # Command 9: User 'a:123' copy into t from (select * from @log_1f93b76af0bd4b1d8e018667865fbc65) (expected to failed)
                execute_and_verify \
                    "User 'a:123' copy into t from (select * from @log_1f93b76af0bd4b1d8e018667865fbc65);" \
                    "a:123" \
                    '{"sql": "copy into t from (select * from @log_1f93b76af0bd4b1d8e018667865fbc65);"}' \
                    '.state == "Failed"'

    # Command 10: User 'root' grant stage 'log_1f93b76af0bd4b1d8e018667865fbc65' ownership (expected to failed)
                execute_and_verify \
                    "User 'root' grant ownership on stage log_1f93b76af0bd4b1d8e018667865fbc65 to role ra" \
                    "root:" \
                    '{"sql": "grant ownership on stage log_1f93b76af0bd4b1d8e018667865fbc65 to role ra"}' \
                    '.state == "Failed"'
}

check_system_history_permissions
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "drop user if exists a"}')
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "drop role if exists ra"}')
