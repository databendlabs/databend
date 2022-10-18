#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "create table s_distinct (a String);" | $MYSQL_CLIENT_CONNECT
echo "insert into s_distinct select to_string(number+1) from numbers(100);" | $MYSQL_CLIENT_CONNECT
echo "insert into s_distinct select to_string(number+2) from numbers(100);" | $MYSQL_CLIENT_CONNECT
echo "insert into s_distinct select to_string(number+3) from numbers(100);" | $MYSQL_CLIENT_CONNECT

echo "select count(distinct a) from s_distinct" |  $MYSQL_CLIENT_CONNECT

echo "drop table s_distinct;" | $MYSQL_CLIENT_CONNECT
