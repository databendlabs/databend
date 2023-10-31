#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists products;" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists s1;" | $BENDSQL_CLIENT_CONNECT
echo "CREATE STAGE s1 FILE_FORMAT = (TYPE = CSV);" | $BENDSQL_CLIENT_CONNECT
echo "create table products (id int, name string, description string);" | $BENDSQL_CLIENT_CONNECT

curl -s -u root: -H "x-databend-stage-name:s1" -F "upload=@${CURDIR}/../../../data/csv/itt.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -XPOST "http://localhost:8000/v1/query" --header 'Content-Type: application/json' -d '{"sql": "insert into products (id, name, description) VALUES(?,?,?)", "stage_attachment": {"location": "@s1/ttt.csv", "copy_options": {"purge": "false"}}, "pagination": { "wait_time_secs": 6}}' -u root: | jq ".data"
echo "select query_kind from system.query_log where query_text =  'INSERT INTO products (id, name, description) VALUES (?,?,?)' limit 1;" | $BENDSQL_CLIENT_CONNECT
