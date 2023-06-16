#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists sample" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists s1" | $MYSQL_CLIENT_CONNECT

## Create table
cat <<EOF | $MYSQL_CLIENT_CONNECT
CREATE TABLE sample
(
    Id      INT,
    City    VARCHAR,
    Score   INT,
    Country VARCHAR DEFAULT 'China'
);
EOF

aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/sample.csv s3://testbucket/admin/stage/internal/s1/sample.csv >/dev/null

## Copy from internal stage
echo "CREATE STAGE s1 FILE_FORMAT = (TYPE = CSV)" | $MYSQL_CLIENT_CONNECT
echo "list @s1" | $MYSQL_CLIENT_CONNECT | awk '{print $1}'

## Insert with stage use http API
curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" --header 'Content-Type: application/json' --header 'X-DATABEND-QUERY-ID:  insert1' -d '{"sql": "insert into sample (Id, City, Score) values", "stage_attachment": {"location": "@s1/sample.csv"}}' | jq -r '.id, .stats.scan_progress.bytes, .stats.write_progress.bytes, .error'

echo "select * from sample" | $MYSQL_CLIENT_CONNECT

## Insert again with the same query_id will have no effect
curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" --header 'Content-Type: application/json' --header 'X-DATABEND-QUERY-ID:  insert1' -d '{"sql": "insert into sample (Id, City, Score) values", "stage_attachment": {"location": "@s1/sample.csv"}}' | jq -r '.id, .stats.scan_progress.bytes, .stats.write_progress.bytes, .error'

echo "select * from sample" | $MYSQL_CLIENT_CONNECT

### Drop table.
echo "drop table sample" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists s1" | $MYSQL_CLIENT_CONNECT
