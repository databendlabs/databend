#!/bin/bash

set -e

function check_query_log() {
  local count=$1
  local query_id=$2
  local check_query=$3
  local expected_result=$4

  response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
    -H 'Content-Type: application/json' \
    -d "{\"sql\": \"$check_query query_id = '$query_id'\"}")

  result=$(echo $response | jq -r '.data[0][0]' | tr -d '"')
  if [ "$result" != "$expected_result" ]; then
    echo "Log table test #$count failed, Result: $result, Expected: $expected_result"
    exit 1
  else
    echo "Log table test #$count passed, Result: $result, Expected: $expected_result"
  fi
}

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "set global timezone='Asia/Shanghai'"}')

# Execute the initial query and get the query_id
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select 123"}')
query_id=$(echo $response | jq -r '.id')
echo "Query ID: $query_id"

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "drop table if exists t"}')
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "create table t (a INT)"}')
create_query_id=$(echo $response | jq -r '.id')
echo "Create Query ID: $create_query_id"
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "insert into t values (1),(2),(3)"}')
insert_query_id=$(echo $response | jq -r '.id')
echo "Insert Query ID: $insert_query_id"
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select * from t"}')
select_query_id=$(echo $response | jq -r '.id')
echo "Select Query ID: $select_query_id"

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

# Test 5
check_query_log "basic-5" "$select_query_id" "SELECT count(*) FROM system_history.log_history WHERE target = 'databend::log::access' and" "1"

# Test 6
check_query_log "basic-6" "$select_query_id" "SELECT count(*) FROM system_history.access_history WHERE" "1"

# Test 7
check_query_log "basic-7" "$create_query_id" "SELECT object_modified_by_ddl[0]['object_name'] FROM system_history.access_history WHERE" "default.default.t"

# Test 8
check_query_log "basic-8" "$insert_query_id" "SELECT objects_modified[0]['object_name'] FROM system_history.access_history WHERE" "default.default.t"

# Test 9
check_query_log "basic-9" "$select_query_id" "SELECT base_objects_accessed[0]['object_name'] FROM system_history.access_history WHERE" "default.default.t"



# Check timezone, regression test for https://github.com/databendlabs/databend/pull/18059
check_query_log "t-1" "$select_query_id" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, timestamp, now()) FROM system_history.log_history WHERE target = 'databend::log::profile' and" "0"

check_query_log "t-2" "$select_query_id" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, event_time, now()) FROM system_history.query_history WHERE" "0"

check_query_log "t-3" "$select_query_id" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, timestamp, now()) FROM system_history.profile_history WHERE" "0"

check_query_log "t-4" "$select_query_id" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, event_time, now()) FROM system_history.login_history WHERE" "0"

check_query_log "t-5" "$select_query_id" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, query_start, now()) FROM system_history.access_history WHERE" "0"
