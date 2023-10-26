#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "drop stage if exists s1" | $BENDSQL_CLIENT_CONNECT

## Copy from internal stage
echo "CREATE STAGE s1;" | $BENDSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/stage/internal/s1/ontime_200.csv >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/stage/internal/s1/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/stage/internal/s1/ontime_200.csv.zst >/dev/null 2>&1

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/stage/internal/s1/dir/ontime_200.csv >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/stage/internal/s1/dir/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/stage/internal/s1/dir/ontime_200.csv.zst >/dev/null 2>&1

## List files in internal stage
echo "=== List files in internal stage ==="
echo "list @s1" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}' | sort

## Remove internal stage file
echo "=== Test remove internal stage file ==="
echo "remove @s1/ontime_200.csv.gz" | $BENDSQL_CLIENT_CONNECT
echo "remove @s1/dir/ontime_200.csv.gz" | $BENDSQL_CLIENT_CONNECT
echo "list @s1/dir/" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}'| sort
echo "list @s1" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}' | sort

## Remove internal stage file with pattern
echo "=== Test remove internal stage file with pattern ==="
echo "remove @s1/dir/ PATTERN = '.*zst'" | $BENDSQL_CLIENT_CONNECT
echo "list @s1" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}' | sort
echo "remove @s1 PATTERN = 'ontime.*'" | $BENDSQL_CLIENT_CONNECT
echo "list @s1" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}' | sort

echo "drop stage s1" | $BENDSQL_CLIENT_CONNECT
