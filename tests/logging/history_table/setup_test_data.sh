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
execute_query_silent "drop table if exists t"
response=$(execute_query "create table t (a INT)")
create_query_id=$(echo $response | jq -r '.id')
echo "Create Query ID: $create_query_id"

response=$(execute_query "create view v as select a from t")
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
