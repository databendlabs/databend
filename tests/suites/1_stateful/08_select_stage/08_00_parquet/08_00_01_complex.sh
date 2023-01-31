#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop stage if exists s1;" | $MYSQL_CLIENT_CONNECT
echo "create stage s1 FILE_FORMAT = (type = PARQUET);" | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/tuple.parquet s3://testbucket/admin/stage/internal/s1/tuple.parquet  >/dev/null 2>&1

echo "select * from @s1;" |  $MYSQL_CLIENT_CONNECT

echo "select * from @s1 where t:a = 1;" |  $MYSQL_CLIENT_CONNECT

echo "select * from @s1 where t:a = id;" |  $MYSQL_CLIENT_CONNECT

echo "select * from @s1 where t:a >= 2;" |  $MYSQL_CLIENT_CONNECT

echo "select t:b from @s1 where t:a >= 2;" |  $MYSQL_CLIENT_CONNECT

echo "select t:b from @s1;" |  $MYSQL_CLIENT_CONNECT

echo "select t from @s1;" |  $MYSQL_CLIENT_CONNECT

echo "select id, t:a, t:b, t from @s1;" |  $MYSQL_CLIENT_CONNECT

echo "select id, t:a, t:b, t from @s1 where id > 2;" |  $MYSQL_CLIENT_CONNECT

echo "select id, t:a, t:b, t from @s1 where t:b < 'c';" |  $MYSQL_CLIENT_CONNECT

echo "select * from @s1 where t:b < 'c';" |  $MYSQL_CLIENT_CONNECT

echo "select t:a from @s1 where t:b < 'c';" |  $MYSQL_CLIENT_CONNECT

echo "select id from @s1 where t:b < 'c';" |  $MYSQL_CLIENT_CONNECT


echo "drop stage if exists s2;" | $MYSQL_CLIENT_CONNECT
echo "create stage s2 FILE_FORMAT = (type = PARQUET);" | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/complex.parquet s3://testbucket/admin/stage/internal/s2/complex.parquet  >/dev/null 2>&1

echo "select meta from @s2 limit 3;" |  $MYSQL_CLIENT_CONNECT

echo "select meta.2, meta.6 from @s2 limit 3;" |  $MYSQL_CLIENT_CONNECT

echo "select name from @s2 limit 3;" |  $MYSQL_CLIENT_CONNECT

echo "select name[1] from @s2 limit 3;" |  $MYSQL_CLIENT_CONNECT

echo "select name[1].5 from @s2 limit 3;" |  $MYSQL_CLIENT_CONNECT

echo "select name[2] from @s2 limit 3;" |  $MYSQL_CLIENT_CONNECT

echo "select name[2].6 from @s2 limit 3;" |  $MYSQL_CLIENT_CONNECT
