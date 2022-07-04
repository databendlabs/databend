#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "set enable_planner_v2 = 1;" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists named_external_stage" | $MYSQL_CLIENT_CONNECT

## tempdate/
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/tempdata/ontime_200.csv >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/tempdata/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/tempdata/ontime_200.csv.zst >/dev/null 2>&1

## tempdate/dir/
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/tempdata/dir/ontime_200.csv >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/tempdata/dir/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/tempdata/dir/ontime_200.csv.zst >/dev/null 2>&1

## Copy from named external stage
echo "CREATE STAGE named_external_stage url = 's3://testbucket/admin/tempdata/' credentials=(aws_key_id='minioadmin' aws_secret_key='minioadmin');" | $MYSQL_CLIENT_CONNECT

## List files in internal stage
echo "=== List files in external stage ==="
echo "list @named_external_stage" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort

## Remove external stage file
echo "=== Test remove external stage file ==="
echo "remove @named_external_stage/ontime_200.csv.gz" | $MYSQL_CLIENT_CONNECT
echo "list @named_external_stage" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort
echo "remove @named_external_stage/dir/ontime_200.csv.gz" | $MYSQL_CLIENT_CONNECT
echo "list @named_external_stage/dir/" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort

## Remove external stage file with pattern
echo "=== Test remove external stage file with pattern ==="
echo "remove @named_external_stage/dir/ PATTERN = '.*zst'" | $MYSQL_CLIENT_CONNECT
echo "list @named_external_stage" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort
echo "remove @named_external_stage PATTERN = '.*zst'" | $MYSQL_CLIENT_CONNECT
echo "list @named_external_stage" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort

echo "drop stage named_external_stage" | $MYSQL_CLIENT_CONNECT
