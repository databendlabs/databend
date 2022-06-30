#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop stage if exists test_sync" | $MYSQL_CLIENT_CONNECT

echo "CREATE STAGE test_sync;" | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/stage/test_sync/ontime_200.csv >/dev/null 2>&1

## List files in internal stage
echo "=== List files in internal stage ==="
echo "list @test_sync" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/stage/test_sync/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/stage/test_sync/ontime_200.csv.zst >/dev/null 2>&1

echo "call system\$sync_stage_file('test_sync', 'ontime_200.csv.gz')" | $MYSQL_CLIENT_CONNECT

echo "=== List stage files after sync ==="
echo "list @test_sync" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/stage/test_sync/dir/ontime_200.csv >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/stage/test_sync/dir/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/stage/test_sync/dir/ontime_200.csv.zst >/dev/null 2>&1

echo "call system\$sync_stage_file('test_sync', 'dir')" | $MYSQL_CLIENT_CONNECT
echo "list @test_sync" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort

echo "drop stage test_sync" | $MYSQL_CLIENT_CONNECT
