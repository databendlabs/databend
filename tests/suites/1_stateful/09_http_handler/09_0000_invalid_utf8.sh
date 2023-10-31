#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists t1;" | $BENDSQL_CLIENT_CONNECT
echo "create table t1(a varchar);" | $BENDSQL_CLIENT_CONNECT
echo "insert INTO t1 VALUES(FROM_BASE64('0Aw='));" | $BENDSQL_CLIENT_CONNECT

curl -s --header 'Content-Type: application/json'  --request POST '127.0.0.1:8000/v1/query/'  --data-raw '{"sql": "select * from t1", "pagination": { "wait_time_secs": 3}}' -u root: | jq ".data[0][0]"
