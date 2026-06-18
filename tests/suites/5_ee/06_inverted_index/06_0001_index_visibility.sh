#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop role if exists role1" | bendsql_connect_root
echo "drop role if exists role2" | bendsql_connect_root
echo "drop user if exists u1" | bendsql_connect_root
echo "drop user if exists u2" | bendsql_connect_root
echo "drop database if exists db1" | bendsql_connect_root
echo "create database db1" | bendsql_connect_root
echo "create table db1.t1(id int, title string, inverted index idx1(title))" | bendsql_connect_root
echo "insert into db1.t1 values(1, 'hello world')" | bendsql_connect_root_null
echo "create role role1;" | bendsql_connect_root
echo "create role role2;" | bendsql_connect_root
echo "grant select on db1.t1 to role role1;" | bendsql_connect_root
echo "create user u1 identified by '123' with DEFAULT_ROLE='role1';" | bendsql_connect_root
echo "create user u2 identified by '123' with DEFAULT_ROLE='role2';" | bendsql_connect_root
echo "grant role role1 to u1;" | bendsql_connect_root
echo "grant role role2 to u2;" | bendsql_connect_root

echo "=== test u1 with role1 ==="
export TEST_U1_CONNECT="bendsql_connect_user u1 123"
echo "select name, type, definition from system.indexes where database = 'db1' order by name" | $TEST_U1_CONNECT

echo "=== test u2 with role2 ==="
export TEST_U2_CONNECT="bendsql_connect_user u2 123"
echo "select name, type, definition from system.indexes where database = 'db1' order by name" | $TEST_U2_CONNECT
