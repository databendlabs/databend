#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop stage if exists s1" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists named_external_stage" | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/stage/s1/ontime_200.csv >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/stage/s1/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/stage/s1/ontime_200.csv.zst >/dev/null 2>&1

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/stage/s1/dir/ontime_200.csv >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/stage/s1/dir/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/stage/s1/dir/ontime_200.csv.zst >/dev/null 2>&1

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/tempdata/ontime_200.csv >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/tempdata/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/tempdata/ontime_200.csv.zst >/dev/null 2>&1

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/tempdata/dir/ontime_200.csv >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/tempdata/dir/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/tempdata/dir/ontime_200.csv.zst >/dev/null 2>&1


## Copy from internal stage
echo "CREATE STAGE s1;" | $MYSQL_CLIENT_CONNECT

## Remove internal stage file
echo "Test remove internal stage file"
echo "remove @s1/ontime_200.csv.gz" | $MYSQL_CLIENT_CONNECT
echo "remove @s1/dir/ontime_200.csv.gz" | $MYSQL_CLIENT_CONNECT
echo "list @s1 PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT | sort
echo "list @s1/dir/ PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT | sort

## Remove internal stage file with pattern
echo "Test remove internal stage file with pattern"
echo "remove @s1  PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT
echo "remove @s1/dir/  PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT
echo "list @s1 PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT | sort
echo "list @s1/dir/ PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT | sort

## Copy from named external stage
echo "CREATE STAGE named_external_stage url = 's3://testbucket/admin/tempdata/' credentials=(aws_key_id='minioadmin' aws_secret_key='minioadmin');" | $MYSQL_CLIENT_CONNECT

## Remove external stage file
echo "Test remove external stage file"
echo "remove @named_external_stage/ontime_200.csv.gz" | $MYSQL_CLIENT_CONNECT
echo "remove @named_external_stage/dir/ontime_200.csv.gz" | $MYSQL_CLIENT_CONNECT
echo "list @named_external_stage PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT | sort
echo "list @named_external_stage/dir/ PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT | sort

## Remove external stage file with pattern
echo "Test remove external stage file with pattern"
echo "remove @named_external_stage  PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT
echo "remove @named_external_stage/dir/  PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT
echo "list @named_external_stage PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT | sort
echo "list @named_external_stage/dir/ PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT | sort
