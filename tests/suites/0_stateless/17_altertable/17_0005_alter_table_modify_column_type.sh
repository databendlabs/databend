#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP DATABASE IF EXISTS test_modify_column_type" | $MYSQL_CLIENT_CONNECT
echo "CREATE DATABASE test_modify_column_type" | $MYSQL_CLIENT_CONNECT

echo "CREATE table test_modify_column_type.a(a String not null, b int not null, c int not null)"  | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.a values('1', 2, 3)"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b,c from test_modify_column_type.a"  | $MYSQL_CLIENT_CONNECT
echo "DESC test_modify_column_type.a"  | $MYSQL_CLIENT_CONNECT

echo "alter table test_modify_column_type.a modify column a float, column b String"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.a"  | $MYSQL_CLIENT_CONNECT
echo "DESC test_modify_column_type.a"  | $MYSQL_CLIENT_CONNECT

echo "CREATE table test_modify_column_type.b(a String not null)"  | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.b values('a')"  | $MYSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.b modify column a float"  | $MYSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.b modify column b float"  | $MYSQL_CLIENT_CONNECT

echo "CREATE table test_modify_column_type.c(a int not null, b int not null)"  | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.c (b) values(1)"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.c"  | $MYSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.c modify column a float default 'a'"  | $MYSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.c modify column a float default 1.2"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.c"  | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.c (b) values(2)"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.c order by a"  | $MYSQL_CLIENT_CONNECT

echo "CREATE table test_modify_column_type.d(a int not null, b int not null default 10)"  | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.d (a) values(1)"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.d"  | $MYSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.d modify column b int default 2"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.d"  | $MYSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.d add column c float default 1.01" | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b,c from test_modify_column_type.d"  | $MYSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.d modify column c float default 2.2"  | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.d (a) values(10)"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b,c from test_modify_column_type.d order by a"  | $MYSQL_CLIENT_CONNECT

echo "begin test default column"
echo "CREATE table test_modify_column_type.e(a int not null, b int not null)"  | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.e values(1,1)"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.e order by b"  | $MYSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.e modify column a VARCHAR(10) DEFAULT 'not'"  | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.e (b) values(2)"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.e order by b"  | $MYSQL_CLIENT_CONNECT

echo "begin test not NULL column"
echo "CREATE table test_modify_column_type.f(a int not null, b int not null)"  | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.f values(1,1)"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.f order by b"  | $MYSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.f modify column a VARCHAR(10) NOT NULL COMMENT 'new column'"  | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.f (b) values(2)"  | $MYSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.f order by b"  | $MYSQL_CLIENT_CONNECT
echo "DESC test_modify_column_type.f"  | $MYSQL_CLIENT_CONNECT

echo "DROP DATABASE IF EXISTS test_modify_column_type" | $MYSQL_CLIENT_CONNECT
