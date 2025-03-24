#!/bin/bash

set -e

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select 123"}')

query_id=$(echo $response | jq -r '.id')

sleep 10

response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  -H 'Content-Type: application/json' \
  -d "{\"sql\": \"SELECT COUNT(*) FROM persistent_system.text_log WHERE query_id = '$query_id'\"}")

count=$(echo $response | jq -r '.data[0][0]')

echo $count

if [ $count != "0" ]; then
  echo "Log table test passed"
  exit 0
else
  echo "Log table test failed"
  exit 1
fi
