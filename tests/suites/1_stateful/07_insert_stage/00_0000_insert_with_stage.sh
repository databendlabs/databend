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

aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/sample.csv s3://testbucket/admin/stage/internal/s1/sample.csv >/dev/null 2>&1

## Copy from internal stage
echo "CREATE STAGE s1" | $MYSQL_CLIENT_CONNECT
echo "list @s1" | $MYSQL_CLIENT_CONNECT | awk '{print $1}'

## Insert with stage use http API
curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" --header 'Content-Type: application/json' -d '{"sql": "insert into sample (Id, City, Score) values", "stage_attachment": {"location": "@s1/sample.csv", "copy_options": {"purge": "true"}}}' | jq -r '.stats.scan_progress.bytes, .stats.write_progress.bytes'

## list stage has metacache, so we just we aws client to ensure the data are purged
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 ls s3://testbucket/admin/stage/internal/s1/ | grep -o ontime_200.csv | wc -l

echo "select * from sample" | $MYSQL_CLIENT_CONNECT

## Drop table.
echo "drop table sample" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists s1" | $MYSQL_CLIENT_CONNECT
