#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists sample_table" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists s1" | $BENDSQL_CLIENT_CONNECT

## Create table
cat <<EOF | $BENDSQL_CLIENT_CONNECT
CREATE TABLE sample_table
(
    Id      INT NOT NULL,
    City    VARCHAR NOT NULL,
    Score   INT NOT NULL,
    Country VARCHAR NOT NULL DEFAULT 'China'
);
EOF

aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/csv/sample.csv s3://testbucket/admin/stage/internal/s1/sample.csv >/dev/null

## Copy from internal stage
echo "CREATE STAGE s1 FILE_FORMAT = (TYPE = CSV)" | $BENDSQL_CLIENT_CONNECT
echo "list @s1" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}'

## Insert with stage use http API
curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" --header 'Content-Type: application/json' -d '{"sql": "replace into sample_table (Id, City, Score) ON(Id) VALUES", "stage_attachment": {"location": "@s1/sample.csv", "copy_options": {"purge": "true"}}, "pagination": { "wait_time_secs": 3}}' | jq -r '.stats.scan_progress.bytes, .stats.write_progress.bytes, .error'

## list stage has metacache, so we just we aws client to ensure the data are purged
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 ls s3://testbucket/admin/stage/internal/s1/sample.csv

echo "select * from sample_table order by id" | $BENDSQL_CLIENT_CONNECT


# use placeholder (?, ?, ?)
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/csv/sample.csv s3://testbucket/admin/stage/internal/s1/sample1.csv >/dev/null
curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" --header 'Content-Type: application/json' -d '{"sql": "replace into sample_table (Id, City, Score) ON(Id) values (?,?,?)", "stage_attachment": {"location": "@s1/sample1.csv", "copy_options": {"purge": "true"}}, "pagination": { "wait_time_secs": 3}}' | jq -r '.stats.scan_progress.bytes, .error'
echo "select * from sample_table order by id" | $BENDSQL_CLIENT_CONNECT

# use placeholder (?, ?, 1+1)
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/csv/sample_2_columns.csv s3://testbucket/admin/stage/internal/s1/sample2.csv  >/dev/null

curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" --header 'Content-Type: application/json' -d '{"sql": "replace into sample_table (Id, City, Score) ON(Id) values (?,?,1+1)", "stage_attachment": {"location": "@s1/sample2.csv", "copy_options": {"purge": "true"}}, "pagination": { "wait_time_secs": 3}}' | jq -r '.stats.scan_progress.bytes, .error'
echo "select * from sample_table order by id" | $BENDSQL_CLIENT_CONNECT

aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/csv/sample_3_replace.csv s3://testbucket/admin/stage/internal/s1/sample3.csv >/dev/null
curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" --header 'Content-Type: application/json' -d '{"sql": "replace into sample_table (Id, City, Score) ON(Id) values (?,?,?)", "stage_attachment": {"location": "@s1/sample3.csv", "copy_options": {"purge": "true"}}, "pagination": { "wait_time_secs": 3}}' | jq -r '.stats.scan_progress.bytes, .error'
echo "select * from sample_table order by id" | $BENDSQL_CLIENT_CONNECT

# duplicate value would show error and would not take effect
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/csv/sample_3_duplicate.csv s3://testbucket/admin/stage/internal/s1/sample4.csv >/dev/null
curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" --header 'Content-Type: application/json' -d '{"sql": "replace into sample_table (Id, City, Score) ON(Id) values (?,?,?)", "stage_attachment": {"location": "@s1/sample4.csv", "copy_options": {"purge": "true"}}, "pagination": { "wait_time_secs": 3}}' | jq -r '.error'
echo "select * from sample_table order by id" | $BENDSQL_CLIENT_CONNECT

### Drop table.
echo "drop table sample_table" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists s1" | $BENDSQL_CLIENT_CONNECT
