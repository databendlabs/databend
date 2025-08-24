#!/bin/bash

set -e

# Source query IDs from setup script
if [ -z "$QUERY_ID" ]; then
    echo "Error: Query IDs not set. Run setup_test_data.sh first."
    echo "Usage: source setup_test_data.sh && ./check_logs_table.sh"
    exit 1
fi

execute_query() {
  local sql="$1"
  curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"$sql\", \"pagination\": {\"wait_time_secs\": 10}}"
}

check_query_log() {
  local test_name=$1
  local query_id=$2
  local check_query=$3
  local expected_result=$4

  local full_sql_query="$check_query"

  if [ -n "$query_id" ] && [ "$query_id" != "null" ]; then
    full_sql_query+=" query_id = '$query_id'"
  fi

  echo "$full_sql_query"
  response=$(execute_query "$full_sql_query")

  result=$(echo "$response" | jq -r '.data[0][0]' | tr -d '"')
  if [ "$result" != "$expected_result" ]; then
    echo "Log table test #$test_name failed, Result: $result, Expected: $expected_result"
    exit 1
  else
    echo "Log table test #$test_name passed, Result: $result, Expected: $expected_result"
  fi
}


# Basic log table tests
check_query_log "basic-1" "$QUERY_ID" "SELECT count(*) FROM system_history.log_history WHERE target = 'databend::log::profile' and" "1"

check_query_log "basic-2" "$QUERY_ID" "SELECT count(*) FROM system_history.profile_history WHERE" "1"

check_query_log "basic-3" "$QUERY_ID" "SELECT count(*) FROM system_history.log_history WHERE target = 'databend::log::query' and" "2"

check_query_log "basic-4" "$QUERY_ID" "SELECT count(*) FROM system_history.query_history WHERE" "1"

check_query_log "basic-6" "$SELECT_QUERY_ID" "SELECT count(*) FROM system_history.access_history WHERE" "1"

# Access history tests
check_query_log "basic-7" "$CREATE_QUERY_ID" "SELECT object_modified_by_ddl[0]['object_name'] FROM system_history.access_history WHERE" "default.default.t"

check_query_log "basic-8" "$INSERT_QUERY_ID" "SELECT objects_modified[0]['object_name'] FROM system_history.access_history WHERE" "default.default.t"

check_query_log "basic-9" "$SELECT_QUERY_ID" "SELECT base_objects_accessed[0]['object_name'] FROM system_history.access_history WHERE" "default.default.t"

check_query_log "basic-10" "$CREATE_VIEW_QUERY_ID" "select query_text from system_history.query_history where" "CREATE VIEW v AS SELECT a FROM t"

check_query_log "basic-11" null "SELECT count(*) FROM system_history.login_history WHERE session_id = '$SELECT_SESSION_ID' " "1"

# Timezone tests - regression test for https://github.com/databendlabs/databend/pull/18059
check_query_log "timezone-1" "$SELECT_QUERY_ID" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, timestamp, now()) FROM system_history.log_history WHERE target = 'databend::log::profile' and" "0"

check_query_log "timezone-2" "$SELECT_QUERY_ID" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, event_time, now()) FROM system_history.query_history WHERE" "0"

check_query_log "timezone-3" "$SELECT_QUERY_ID" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, timestamp, now()) FROM system_history.profile_history WHERE" "0"

check_query_log "timezone-4" null "settings (timezone='Asia/Shanghai') SELECT sum(DATE_DIFF(hour, event_time, now())) FROM system_history.login_history" "0"

check_query_log "timezone-5" "$SELECT_QUERY_ID" "settings (timezone='Asia/Shanghai') SELECT DATE_DIFF(hour, query_start, now()) FROM system_history.access_history WHERE" "0"

# Login history tests
check_query_log "login-1" null "SELECT CASE WHEN count(*) >= 1 THEN 1 ELSE 0 END FROM system_history.login_history WHERE user_name = 'root'" "1"

check_query_log "login-2" null "SELECT CASE WHEN count(*) >= 1 THEN 1 ELSE 0 END FROM system_history.login_history WHERE error_message LIKE 'AuthenticateFailure.%' AND user_name = 'wrong_pass_user'" "1"


echo "All log table tests passed successfully!"
