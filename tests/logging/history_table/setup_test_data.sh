#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lineage_sqllogic.sh"

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

# Lineage setup is grouped in a dedicated sqllogictest suite so scenarios remain reviewable and
# can grow without adding SQL assertions to this shell script.
run_lineage_suite setup

execute_query_silent "drop user if exists wrong_pass_user"

execute_query_silent "create user wrong_pass_user identified by 'secure_password'"

response=$(curl -s -u wrong_pass_user:wrong_password -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select 1"}')


# Ensure all executed queries are logged
for _ in {1..3}; do
  execute_query_silent "select 123"
  sleep 3
done

# History ingestion is asynchronous. Poll the transformed edge table instead of assuming the fixed
# delay above is sufficient on every CI runner.
lineage_ready=false
for _ in {1..30}; do
  lineage_response=$(execute_query "SELECT count_if(source_database IN ('lineage_history_multi_kind', 'lineage_history_columns', 'lineage_history_views', 'lineage_history_statements') OR target_database IN ('lineage_history_multi_kind', 'lineage_history_columns', 'lineage_history_views', 'lineage_history_statements')) AS captured_edges, count_if(target_database = 'lineage_history_lifecycle') AS lifecycle_edges, count_if(source_catalog = 'lineage_history_iceberg_catalog' AND source_database = 'lineage_db' AND target_database = 'lineage_history_iceberg') AS iceberg_edges, count_if(target_database = 'lineage_history_views' AND target_name IN ('src_view', 'view_dst')) AS view_edges FROM system_history.lineage_unresolved")
  if [ "$(echo "$lineage_response" | jq -r '.state')" = "Failed" ]; then
    # The history transform creates its destination table asynchronously. Treat a missing table
    # like any other not-ready state and report the last response if the poll eventually times out.
    sleep 1
    continue
  fi
  lineage_count=$(echo "$lineage_response" | jq -r '.data[0][0] // 0')
  lifecycle_count=$(echo "$lineage_response" | jq -r '.data[0][1] // 0')
  iceberg_count=$(echo "$lineage_response" | jq -r '.data[0][2] // 0')
  view_count=$(echo "$lineage_response" | jq -r '.data[0][3] // 0')
  if [ "$lineage_count" -ge 16 ] 2>/dev/null && [ "$lifecycle_count" -eq 3 ] 2>/dev/null && [ "$iceberg_count" -ge 1 ] 2>/dev/null && [ "$view_count" -ge 2 ] 2>/dev/null; then
    lineage_ready=true
    break
  fi
  sleep 1
done

if [ "$lineage_ready" = false ]; then
  echo "Lineage history was not transformed within 30 seconds"
  echo "$lineage_response"
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
