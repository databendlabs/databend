#!/bin/bash

set -e

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select 123"}')

query_id=$(echo $response | jq -r '.id')

sleep 10

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  -H 'Content-Type: application/json' \
  -d "{\"sql\": \"SELECT COUNT(*) FROM persistent_system.query_log WHERE query_id = '$query_id'\"}")

count=$(echo $response | jq -r '.data[0][0]')

if [ $count != "0" ]; then
  echo "Log table test1 passed"
else
  echo "Log table test1 failed"
  exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  -H 'Content-Type: application/json' \
  -d "{\"sql\": \"SELECT check_json(message) FROM persistent_system.query_log WHERE target = 'databend::log::profile' and query_id = '$query_id'\"}")

result=$(echo $response | jq -r '.data[0][0]')

if [ result != "NULL" ]; then
  echo "Log table test2 passed"
else
  echo "Log table test2 failed"
  exit 1
fi

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  -H 'Content-Type: application/json' \
  -d "{\"sql\": \"SELECT check_json(message) FROM persistent_system.query_log WHERE target = 'databend::log::query' and query_id = '$query_id'\"}")

result=$(echo $response | jq -r '.data[0][0]')

if [ result != "NULL" ]; then
  echo "Log table test3 passed"
else
  echo "Log table test3 failed"
  exit 1
fi


