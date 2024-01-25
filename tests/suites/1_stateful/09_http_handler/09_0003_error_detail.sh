#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists products;" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists s1;" | $BENDSQL_CLIENT_CONNECT
echo "CREATE STAGE s1 FILE_FORMAT = (TYPE = CSV);" | $BENDSQL_CLIENT_CONNECT
echo "remove @s1;" | $BENDSQL_CLIENT_CONNECT
echo "create table products (id int, name string, description string);" | $BENDSQL_CLIENT_CONNECT

curl -s -u root: -H "x-databend-stage-name:s1" -F "upload=@${CURDIR}/../../../data/csv/select.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -XPOST "http://localhost:8000/v1/query" --header 'Content-Type: application/json' -d '{"sql": "copy /*+ set_var(enable_distributed_copy_into = 0) */ into products (id, name, description) from @s1/", "pagination": { "wait_time_secs": 6}}' -u root: | jq ".error"

echo "drop table if exists products;" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists s1;" | $BENDSQL_CLIENT_CONNECT
