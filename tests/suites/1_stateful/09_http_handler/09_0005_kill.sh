#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

QID="my_query_for_kill_${RANDOM}"
echo "## query"
curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H "x-databend-query-id:${QID}"  -H 'Content-Type: application/json' -d '{"sql": "select sleep(0.5), number from numbers(15000000000);",  "pagination": { "wait_time_secs": 6}}' | jq ".state"
echo "## kill"
curl -s -u root: -XGET -w "%{http_code}\n"  "http://localhost:8000/v1/query/${QID}/kill"
echo "## page"
curl -s -u root: -XGET -w "\n%{http_code}\n" "http://localhost:8000/v1/query/${QID}/page/0" | sed "s/${QID}/QID/g"
echo "## final"
curl -s -u root: -XGET -w "\n" "http://localhost:8000/v1/query/${QID}/final" | jq ".error"
