#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop stage if exists s1;" | $MYSQL_CLIENT_CONNECT
echo "create stage s1;" | $MYSQL_CLIENT_CONNECT

echo "drop table if exists t1;" | $MYSQL_CLIENT_CONNECT
echo "CREATE TABLE t1 (id INT, name VARCHAR, age INT);" | $MYSQL_CLIENT_CONNECT

echo "insert into t1 (id,name,age) values(1,'2',3), (4, '5', 6);" | $MYSQL_CLIENT_CONNECT
echo " copy into @s1 from t1 FILE_FORMAT = (type = 'PARQUET');" | $MYSQL_CLIENT_CONNECT

echo "select * from @s1;" | $MYSQL_CLIENT_CONNECT
