#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

## Setup
echo "drop database if exists test_vacuum" | $MYSQL_CLIENT_CONNECT

echo "CREATE DATABASE test_vacuum" | $MYSQL_CLIENT_CONNECT
echo "create table test_vacuum.a(c int)" | $MYSQL_CLIENT_CONNECT

# insert some values
bash ./suites/5_ee/00_vacuum/insert_vacuum_table_values > /dev/null 2>&1 &

# kill query randomly
sleep 5
killall databend-query
killall insert_vacuum_table_values

# restart query
bash ../scripts/ci/deploy/databend-query-standalone.sh > /dev/null 2>&1

# check if before and after vacuum table the table count matched
old_count=`echo "select * from test_vacuum.a order by c" | $MYSQL_CLIENT_CONNECT`

echo "vacuum table test_vacuum.a retain 0 hours" |  | $MYSQL_CLIENT_CONNECT
#echo "optimize table test_vacuum.a all" | $MYSQL_CLIENT_CONNECT
count=`echo "select * from test_vacuum.a order by c" | $MYSQL_CLIENT_CONNECT`
if [ $old_count -ne $count ]; then
    echo "vacuum table, old count:$old_count,new count:$count"
    exit -1
fi

echo "vacuum table success"
echo "drop database if exists test_vacuum" | $MYSQL_CLIENT_CONNECT
