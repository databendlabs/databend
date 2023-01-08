#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.parquet /tmp/06_0000_read_parquet_ontime_200.parquet  > /dev/null 2>&1

echo "select count(*) from read_parquet('/tmp/06_0000_read_parquet_ontime_200.parquet')" | $MYSQL_CLIENT_CONNECT

echo "select count(*) from read_parquet('/tmp/06_0000_read_parquet_ontime_200.parquet', '/tmp/06_0000_read_parquet_ontime_200.parquet')" | $MYSQL_CLIENT_CONNECT

echo "select tail_number from read_parquet('/tmp/06_0000_read_parquet_ontime_200.parquet') where dayofmonth=1;" |  $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/tuple.parquet /tmp/06_0000_read_parquet_tuple.parquet  > /dev/null 2>&1

echo "select * from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet');" |  $MYSQL_CLIENT_CONNECT

echo "select * from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet') where t:a = 1;" |  $MYSQL_CLIENT_CONNECT

echo "select * from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet') where t:a = id;" |  $MYSQL_CLIENT_CONNECT

echo "select * from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet') where t:a >= 2;" |  $MYSQL_CLIENT_CONNECT

echo "select t:b from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet') where t:a >= 2;" |  $MYSQL_CLIENT_CONNECT

echo "select t:b from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet');" |  $MYSQL_CLIENT_CONNECT

echo "select t from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet');" |  $MYSQL_CLIENT_CONNECT

echo "select id, t:a, t:b, t from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet');" |  $MYSQL_CLIENT_CONNECT

echo "select id, t:a, t:b, t from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet') where id > 2;" |  $MYSQL_CLIENT_CONNECT

echo "select id, t:a, t:b, t from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet') where t:b < 'c';" |  $MYSQL_CLIENT_CONNECT

echo "select * from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet') where t:b < 'c';" |  $MYSQL_CLIENT_CONNECT

echo "select t:a from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet') where t:b < 'c';" |  $MYSQL_CLIENT_CONNECT

echo "select id from read_parquet('/tmp/06_0000_read_parquet_tuple.parquet') where t:b < 'c';" |  $MYSQL_CLIENT_CONNECT

