#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh
echo $CURDIR

echo "drop stage if exists s1;" | $MYSQL_CLIENT_CONNECT
echo "CREATE STAGE s1 FILE_FORMAT = (TYPE = CSV);" | $MYSQL_CLIENT_CONNECT

curl -u root: -H "stage_name:s1" -F "upload=@${CURDIR}/../../../data/ttt.csv" -XPUT "http://localhost:8000/v1/upload_to_stage"

curl -u root: -H "stage_name:s1" -F "upload=@ttt.csv" -XPUT "http://localhost:8000/v1/upload_to_stage"

curl -s -u root: -XPOST "http://localhost:8000/v1/query" --header 'Content-Type: application/json' -d '{"sql": "insert into products (id, name, description) VALUES(?,?,?)", "stage_attachment": {"location": "@s1/ttt.csv", "copy_options": {"purge": "false"}}}'
echo ""
echo "select query_kind from system.query_log where query_text =  'INSERT INTO products (id, name, description) VALUES (?,?,?)' limit 1;" | $MYSQL_CLIENT_CONNECT