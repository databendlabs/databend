#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop stage if exists s1;" | $MYSQL_CLIENT_CONNECT
echo "create stage s1 FILE_FORMAT = (type = PARQUET);" | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900 s3 cp s3://testbucket/admin/data/ontime_200.parquet s3://testbucket/admin/stage/internal/s1/ontime_200.parquet  >/dev/null 2>&1

echo "select count(*) from @s1" | $MYSQL_CLIENT_CONNECT

#
#echo "select tail_number from read_parquet('/tmp/06_0000_basic.parquet') where dayofmonth=1;" |  $MYSQL_CLIENT_CONNECT
#
#echo "select tail_number from read_parquet('/tmp/06_0000_basic.parquet', do_prewhere=>false) where dayofmonth=1;" |  $MYSQL_CLIENT_CONNECT
#
#echo "select tail_number from read_parquet('/tmp/06_0000_basic.parquet', push_down_bitmap=>true) where dayofmonth=1;" |  $MYSQL_CLIENT_CONNECT
#
#echo "select tail_number from read_parquet('/tmp/06_0000_basic.parquet', prune_pages=>true) where dayofmonth=1;" |  $MYSQL_CLIENT_CONNECT
#
