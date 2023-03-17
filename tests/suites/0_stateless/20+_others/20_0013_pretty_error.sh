#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "DROP DATABASE IF EXISTS db20_13;" | $MYSQL_CLIENT_CONNECT
echo "CREATE DATABASE db20_13;" | $MYSQL_CLIENT_CONNECT
echo "USE db20_13;" | $MYSQL_CLIENT_CONNECT


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

echo "DROP DATABASE db20_13;" | $MYSQL_CLIENT_CONNECT
