#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP DATABASE IF EXISTS test_modify_column_type" | $MYSQL_CLIENT_CONNECT
echo "CREATE DATABASE test_modify_column_type" | $MYSQL_CLIENT_CONNECT

echo "CREATE table test_modify_column_type.a(a String, b int, c int)"  | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.a values('1', 2, 3)"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b,c from test_modify_column_type.a"  | $MYSQL_CLIENT_CONNECT
echo "DESC test_modify_column_type.a"  | $MYSQL_CLIENT_CONNECT

echo "alter table test_modify_column_type.a modify column set data type a float, b String"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.a"  | $MYSQL_CLIENT_CONNECT
echo "DESC test_modify_column_type.a"  | $MYSQL_CLIENT_CONNECT

echo "CREATE table test_modify_column_type.b(a String)"  | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.b values('a')"  | $MYSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.b modify column set data type a float"  | $MYSQL_CLIENT_CONNECT

echo "DROP DATABASE IF EXISTS test_modify_column_type" | $MYSQL_CLIENT_CONNECT
