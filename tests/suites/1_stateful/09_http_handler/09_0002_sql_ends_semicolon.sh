#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

query "create or replace table t_09_0002 (a int)"

# insert_stmt
curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/query/'  --data-raw '{"sql": "insert into t_09_0002 from @~/not_exist file_format=(type=csv);", "pagination": { "wait_time_secs": 5}}' -u root: | jq -c ".state",".error"
curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/query/'  --data-raw '{"sql": "insert into t_09_0002 select 1;", "pagination": { "wait_time_secs": 5}}' -u root: | jq -c ".state",".error"
curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/query/'  --data-raw '{"sql": "insert into t_09_0002 values (1);", "pagination": { "wait_time_secs": 5}}' -u root: | jq -c ".state",".error"

# statement
curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/query/'  --data-raw '{"sql": "select 1;", "pagination": { "wait_time_secs": 2}}' -u root:  | jq -c ".state",".error"
