#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.parquet /tmp/06_0000_basic.parquet  > /dev/null 2>&1

echo "select count(*) from read_parquet('/tmp/06_0000_basic.parquet')" | $MYSQL_CLIENT_CONNECT

echo "select count(*) from read_parquet('/tmp/06_0000_basic.parquet', '/tmp/06_0000_basic.parquet')" | $MYSQL_CLIENT_CONNECT

echo "select tail_number from read_parquet('/tmp/06_0000_basic.parquet') where dayofmonth=1;" |  $MYSQL_CLIENT_CONNECT

echo "select tail_number from read_parquet('/tmp/06_0000_basic.parquet', do_prewhere=>false) where dayofmonth=1;" |  $MYSQL_CLIENT_CONNECT

echo "select tail_number from read_parquet('/tmp/06_0000_basic.parquet', push_down_bitmap=>true) where dayofmonth=1;" |  $MYSQL_CLIENT_CONNECT

echo "select tail_number from read_parquet('/tmp/06_0000_basic.parquet', prune_pages=>true) where dayofmonth=1;" |  $MYSQL_CLIENT_CONNECT

