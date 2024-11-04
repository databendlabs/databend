#!/usr/bin/env bash
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select sleep(2)"}')

query_id=$(echo $response | jq -r '.id')

# test running status
http_code=$(curl -s -o /dev/null -w "%{http_code}" -u root: "http://localhost:8080/v1/queries/${query_id}/profiling")
if [ "$http_code" -eq 200 ]; then
    echo "true"
else
    echo "false"
fi

sleep 3

#test finished status
http_code=$(curl -s -o /dev/null -w "%{http_code}" -u root: "http://localhost:8080/v1/queries/${query_id}/profiling")
if [ "$http_code" -eq 200 ]; then
    echo "true"
else
    echo "false"
fi

