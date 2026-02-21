#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP DATABASE IF EXISTS test_modify_column_type" | $BENDSQL_CLIENT_CONNECT
echo "CREATE DATABASE test_modify_column_type" | $BENDSQL_CLIENT_CONNECT

echo "CREATE table test_modify_column_type.a(a String not null, b int not null, c int not null)"  | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.a values('1', 2, 3)"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b,c from test_modify_column_type.a"  | $BENDSQL_CLIENT_CONNECT
echo "DESC test_modify_column_type.a"  | $BENDSQL_CLIENT_CONNECT

echo "alter table test_modify_column_type.a modify column a float not null, column b String not null"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.a"  | $BENDSQL_CLIENT_CONNECT
echo "DESC test_modify_column_type.a"  | $BENDSQL_CLIENT_CONNECT

echo "CREATE table test_modify_column_type.b(a String not null)"  | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.b values('a')"  | $BENDSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.b modify column a float not null"  | $BENDSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.b modify column b float not null"  | $BENDSQL_CLIENT_CONNECT

echo "CREATE table test_modify_column_type.c(a int not null, b int not null)"  | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.c (b) values(1)"  | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.c (a,b) values(0,1)"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.c"  | $BENDSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.c modify column a float not null default 'a'"  | $BENDSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.c modify column a float not null default 1.2"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.c"  | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.c (b) values(2)"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.c order by a"  | $BENDSQL_CLIENT_CONNECT

echo "CREATE table test_modify_column_type.d(a int not null, b int not null default 10)"  | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.d (a) values(1)"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.d"  | $BENDSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.d modify column b int not null default 2"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.d"  | $BENDSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.d add column c float not null default 1.01" | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b,c from test_modify_column_type.d"  | $BENDSQL_CLIENT_CONNECT
# This ALTER only updates table metadata; it does not rewrite existing rows.
# After changing c's default to 2.2, the old row also reads c as 2.2.
echo "alter table test_modify_column_type.d modify column c float not null default 2.2"  | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.d (a) values(10)"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b,c from test_modify_column_type.d where a = 1"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b,c from test_modify_column_type.d where a = 10"  | $BENDSQL_CLIENT_CONNECT

echo "begin test default column"
echo "CREATE table test_modify_column_type.e(a int not null, b int not null)"  | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.e values(1,1)"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.e order by b"  | $BENDSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.e modify column a VARCHAR(10) not null DEFAULT 'not'"  | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.e (b) values(2)"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.e order by b"  | $BENDSQL_CLIENT_CONNECT

echo "begin test not NULL column"
echo "CREATE table test_modify_column_type.f(a int not null, b int not null)"  | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.f values(1,1)"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.f order by b"  | $BENDSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.f modify column a VARCHAR(10) NOT NULL" | $BENDSQL_CLIENT_CONNECT
echo "alter table test_modify_column_type.f modify column a COMMENT 'new column'"  | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.f (b) values(2)"  | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type.f (a,b) values('',2)"  | $BENDSQL_CLIENT_CONNECT
echo "SELECT a,b from test_modify_column_type.f order by b"  | $BENDSQL_CLIENT_CONNECT
echo "DESC test_modify_column_type.f"  | $BENDSQL_CLIENT_CONNECT

echo "begin test modify column NULL to not NULL"
stmt "CREATE TABLE test_modify_column_type.g(a STRING NULL, b INT NULL, c string not null)"
stmt "INSERT INTO test_modify_column_type.g VALUES('a',1,'c1'),('b',NULL,'c2'),(NULL,3,'c3'),('d',4,'c4')"
stmt "SELECT a,b,c from test_modify_column_type.g"
stmt "ALTER TABLE test_modify_column_type.g MODIFY COLUMN a STRING NOT NULL"
stmt "ALTER TABLE test_modify_column_type.g MODIFY COLUMN b INT NOT NULL"
stmt "ALTER TABLE test_modify_column_type.g MODIFY COLUMN c STRING NOT NULL"
stmt "SELECT a,b,c from test_modify_column_type.g"
stmt "DESC test_modify_column_type.g"

echo "DROP DATABASE IF EXISTS test_modify_column_type" | $BENDSQL_CLIENT_CONNECT
