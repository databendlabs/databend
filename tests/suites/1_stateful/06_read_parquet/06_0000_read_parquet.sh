#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.parquet /tmp/06_0000_read_parquet_ontime_200.parquet  > /dev/null 2>&1

echo "select count(*) from read_parquet('/tmp/06_0000_read_parquet_ontime_200.parquet')" | $MYSQL_CLIENT_CONNECT

echo "select count(*) from read_parquet('/tmp/06_0000_read_parquet_ontime_200.parquet', '/tmp/06_0000_read_parquet_ontime_200.parquet')" | $MYSQL_CLIENT_CONNECT

echo "select tail_number from read_parquet('/tmp/06_0000_read_parquet_ontime_200.parquet') where dayofmonth=1;" |  $MYSQL_CLIENT_CONNECT

