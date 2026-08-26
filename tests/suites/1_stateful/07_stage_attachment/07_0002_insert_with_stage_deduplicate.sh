#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists sample_table" | bendsql_connect_root
echo "drop stage if exists s1" | bendsql_connect_root

## Create table
cat <<EOF | bendsql_connect_root
CREATE TABLE sample_table
(
    Id      INT NOT NULL,
    City    VARCHAR NOT NULL,
    Score   INT NOT NULL,
    Country VARCHAR NOT NULL DEFAULT 'China'
);
EOF

aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/data/csv/sample.csv s3://testbucket/admin/stage/internal/s1/sample.csv >/dev/null

## Copy from internal stage
echo "CREATE STAGE s1 FILE_FORMAT = (TYPE = CSV)" | bendsql_connect_root
echo "list @s1" | bendsql_connect_root | awk '{print $1}'

## Insert with stage use http API
curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" \
  --header 'Content-Type: application/json' \
  --header 'X-DATABEND-DEDUPLICATE-LABEL: insert1' \
  -d '{
    "sql": "insert into sample_table (Id, City, Score) values",
    "stage_attachment": {
      "location": "@s1/sample.csv"
    },
    "pagination": {
      "wait_time_secs": 3
    }
  }' | jq -r '.stats.scan_progress.bytes, .error'

echo "select * from sample_table" | bendsql_connect_root

## Insert again with the same deduplicate_label will have no effect
curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" \
  --header 'Content-Type: application/json' \
  --header 'X-DATABEND-DEDUPLICATE-LABEL: insert1' \
  -d '{
    "sql": "insert into sample_table (Id, City, Score) values",
    "stage_attachment": {
      "location": "@s1/sample.csv"
    },
    "pagination": {
      "wait_time_secs": 3
    }
  }' | jq -r '.stats.scan_progress.bytes, .error'


echo "select * from sample_table" | bendsql_connect_root

### Drop table.
echo "drop table sample_table" | bendsql_connect_root
echo "drop stage if exists s1" | bendsql_connect_root
