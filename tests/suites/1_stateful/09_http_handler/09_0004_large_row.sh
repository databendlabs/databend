#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

qid="09_004"
curl -s --header 'Content-Type: application/json' --header "X-DATABEND-QUERY-ID: $qid" --request POST '127.0.0.1:8000/v1/query/' \
 --data-raw $'{"sql": "select json_array_agg(json_object(\'num\',number)), (number % 2) as s from numbers(2000000) group by s;", "pagination": { "wait_time_secs": 5}}' \
 -u root: | jq '.data | length'

curl -s --header 'Content-Type: application/json'  "127.0.0.1:8000/v1/query/$qid/page/1" \
 -u root: | jq '(.data | length), .next_uri'
