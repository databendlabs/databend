#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop stage if exists s1;" | $MYSQL_CLIENT_CONNECT
echo "create stage s1 FILE_FORMAT = (type = PARQUET);" | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900 s3 cp s3://testbucket/admin/data/ontime_200.parquet s3://testbucket/admin/stage/internal/s1/ontime_200.parquet  >/dev/null 2>&1

echo "select count(*) from @s1;" | $MYSQL_CLIENT_CONNECT

echo "select tail_number from @s1 where dayofmonth=1;" |  $MYSQL_CLIENT_CONNECT

echo "select tail_number from @s1 where dayofmonth > 15 order by tail_number limit 5;" |  $MYSQL_CLIENT_CONNECT

echo "select tail_number from @s1 where dayofmonth > 15 order by tail_number desc limit 5;" |  $MYSQL_CLIENT_CONNECT

echo "select month from @s1 where dayofmonth > 15 order by tail_number desc limit 5;" |  $MYSQL_CLIENT_CONNECT

echo "select month from @s1 where dayofmonth > 15 order by tail_number desc limit 5;" |  $MYSQL_CLIENT_CONNECT
