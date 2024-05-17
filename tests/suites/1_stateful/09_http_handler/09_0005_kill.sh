#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

QID="my_query_for_kill_4"
curl -s -u root: -XPOST "http://localhost:8000/v1/query" -H "x-databend-query-id:${QID}"  -H 'Content-Type: application/json' -d '{"sql": "select sleep(0.5) from numbers(15000000);",  "pagination": { "wait_time_secs": 6}}' | jq ".state"
curl -s -u root: -XGET -w "%{http_code}\n"  "http://localhost:8000/v1/query/${QID}/kill"
curl -s -u root: -XGET -w "\n%{http_code}\n" "http://localhost:8000/v1/query/${QID}/page/0"

## todo: this is flaky
# query "select log_type, exception_code, exception_text from system.query_log where query_id='${QID}' order by log_type"
