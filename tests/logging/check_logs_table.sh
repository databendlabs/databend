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

  result=$(echo $response | jq -r '.data[0][0]')
  if [ "$result" != "$expected_result" ]; then
    echo "Log table test #$count failed, Result: $result, Expected: $expected_result"
    exit 1
  else
    echo "Log table test #$count passed, Result: $result, Expected: $expected_result"
  fi
}

# Execute the initial query and get the query_id
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select 123"}')
query_id=$(echo $response | jq -r '.id')
echo "Query ID: $query_id"
# Wait for the query to be logged
sleep 15

# Test 1
check_query_log "1" "$query_id" "SELECT count(*) FROM persistent_system.query_log WHERE target = 'databend::log::profile' and" "1"

# Test 2
check_query_log "2" "$query_id" "SELECT count(*) FROM persistent_system.query_profile WHERE" "1"

# Test 3
check_query_log "3" "$query_id" "SELECT count(*) FROM persistent_system.query_log WHERE target = 'databend::log::query' and" "2"

# Test 4
check_query_log "4" "$query_id" "SELECT count(*) FROM persistent_system.query_details WHERE" "2"

# Test 5
check_query_log "5" "$query_id" "SELECT count(*) FROM persistent_system.query_log WHERE" "3"
