#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP TABLE IF EXISTS t;" | $MYSQL_CLIENT_CONNECT
echo "DROP TABLE IF EXISTS t1;" | $MYSQL_CLIENT_CONNECT
echo "DROP TABLE IF EXISTS t2;" | $MYSQL_CLIENT_CONNECT
echo "DROP TABLE IF EXISTS t3;" | $MYSQL_CLIENT_CONNECT

echo "select *;" |  $MYSQL_CLIENT_CONNECT
echo "select * from t;" |  $MYSQL_CLIENT_CONNECT
echo "select base64(1);" |  $MYSQL_CLIENT_CONNECT
echo "select to_base64(1);" | $MYSQL_CLIENT_CONNECT
echo "select 1 + 'a';" | $MYSQL_CLIENT_CONNECT

echo "create table t1 (a tuple(b int, c int))" | $MYSQL_CLIENT_CONNECT
echo "select t1.a:z from t" | $MYSQL_CLIENT_CONNECT

echo "create table t2 (a int, b int)" | $MYSQL_CLIENT_CONNECT
echo "create table t3 (c int, d int)" | $MYSQL_CLIENT_CONNECT
echo "select * from t2 join t3 using (c)" | $MYSQL_CLIENT_CONNECT

echo "DROP TABLE t;" | $MYSQL_CLIENT_CONNECT
echo "DROP TABLE t1;" | $MYSQL_CLIENT_CONNECT
echo "DROP TABLE t2;" | $MYSQL_CLIENT_CONNECT
echo "DROP TABLE t3;" | $MYSQL_CLIENT_CONNECT
