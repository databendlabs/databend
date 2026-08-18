#!/bin/bash

set -e

execute_query() {
  local sql="$1"
  local extra_headers="$2"

  if [ -n "$extra_headers" ]; then
    curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -H "$extra_headers" -d "{\"sql\": \"$sql\"}"
  else
    curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"$sql\"}"
  fi
}

execute_query_silent() {
  execute_query "$@" > /dev/null
}

# Setup timezone
execute_query_silent "set global timezone='Asia/Shanghai'"

# Execute the initial query and get the query_id
response=$(execute_query "select 123")
query_id=$(echo $response | jq -r '.id')
echo "Query ID: $query_id"

# Setup test table and view
execute_query_silent "drop view if exists v"
execute_query_silent "drop table if exists t"
response=$(execute_query "create table t (a INT)")
create_query_id=$(echo $response | jq -r '.id')
echo "Create Query ID: $create_query_id"

response=$(execute_query "create view v as select a from t")
if [ "$(echo "$response" | jq -r '.state')" != "Succeeded" ]; then
  echo "Failed to create test view v"
  echo "$response"
  exit 1
fi
create_view_query_id=$(echo $response | jq -r '.id')
echo "Create VIEW Query ID: $create_view_query_id"

response=$(execute_query "insert into t values (1),(2),(3)")
insert_query_id=$(echo $response | jq -r '.id')
echo "Insert Query ID: $insert_query_id"

response=$(execute_query "select * from t" "X-Databend-Client-Caps: session_cookie")
select_query_id=$(echo $response | jq -r '.id')
select_session_id=$(echo $response | jq -r '.session_id')
echo "Select Query ID: $select_query_id"
echo "Select Session ID: $select_session_id"

execute_query_silent "drop user if exists wrong_pass_user"

execute_query_silent "create user wrong_pass_user identified by 'secure_password'"

response=$(curl -s -u wrong_pass_user:wrong_password -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select 1"}')


# Ensure all executed queries are logged
for _ in {1..3}; do
  execute_query_silent "select 123"
  sleep 3
done

# History transforms are asynchronous, especially while switching from internal to external
# storage. Wait for this fixed set of generic history records instead of relying on the sleeps
# above to cover every runner.
history_ready=false
for _ in {1..30}; do
  history_response=$(execute_query "SELECT (SELECT count(*) FROM system_history.query_history WHERE query_id = '$query_id'), (SELECT count(*) FROM system_history.profile_history WHERE query_id = '$query_id'), (SELECT count(*) FROM system_history.access_history WHERE query_id = '$select_query_id'), (SELECT count(*) FROM system_history.login_history WHERE session_id = '$select_session_id')")
  if [ "$(echo "$history_response" | jq -r '.state')" = "Succeeded" ]; then
    query_count=$(echo "$history_response" | jq -r '.data[0][0] // 0')
    profile_count=$(echo "$history_response" | jq -r '.data[0][1] // 0')
    access_count=$(echo "$history_response" | jq -r '.data[0][2] // 0')
    login_count=$(echo "$history_response" | jq -r '.data[0][3] // 0')
    if [ "$query_count" -eq 1 ] 2>/dev/null \
        && [ "$profile_count" -eq 1 ] 2>/dev/null \
        && [ "$access_count" -eq 1 ] 2>/dev/null \
        && [ "$login_count" -ge 1 ] 2>/dev/null; then
      history_ready=true
      break
    fi
  fi
  sleep 1
done

if [ "$history_ready" = false ]; then
  echo "Generic history tables were not transformed within 30 seconds"
  echo "$history_response"
  exit 1
fi

# Export query IDs for use in other scripts
export QUERY_ID="$query_id"
export CREATE_QUERY_ID="$create_query_id"
export CREATE_VIEW_QUERY_ID="$create_view_query_id"
export INSERT_QUERY_ID="$insert_query_id"
export SELECT_QUERY_ID="$select_query_id"
export SELECT_SESSION_ID="$select_session_id"

echo "Exported environment variables:"
echo "QUERY_ID='$QUERY_ID'"
echo "CREATE_QUERY_ID='$CREATE_QUERY_ID'"
echo "CREATE_VIEW_QUERY_ID='$CREATE_VIEW_QUERY_ID'"
echo "INSERT_QUERY_ID='$INSERT_QUERY_ID'"
echo "SELECT_QUERY_ID='$SELECT_QUERY_ID'"
echo "SELECT_SESSION_ID='$SELECT_SESSION_ID'"
