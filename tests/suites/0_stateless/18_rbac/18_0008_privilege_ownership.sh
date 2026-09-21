#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_NAME="owner"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql_connect_user owner password -A"

run_root_sql "
drop table if exists d20_0014.table2;
drop database if exists d20_0014;
drop user if exists '${TEST_USER_NAME}';
drop role if exists 'd20_0014_owner';
create user '${TEST_USER_NAME}' IDENTIFIED BY '$TEST_USER_PASSWORD';
create role \`d20_0014_owner\`;
create database d20_0014;
GRANT OWNERSHIP ON d20_0014.* TO ROLE 'd20_0014_owner';
GRANT ROLE 'd20_0014_owner' TO '${TEST_USER_NAME}';
ALTER USER '${TEST_USER_NAME}' WITH DEFAULT_ROLE='d20_0014_owner';
"

## owner should have all privileges on the table
echo "create table d20_0014.table1(i int);" | $TEST_USER_CONNECT
echo "create table d20_0014.table2(i int);" | $TEST_USER_CONNECT
echo "insert into d20_0014.table1 values(1),(2),(3);" | $TEST_USER_CONNECT
echo "select * from d20_0014.table1;" | $TEST_USER_CONNECT

echo "=== test drop role ==="
echo "select name, owner from system.tables where name in ('table1', 'table2') and database='d20_0014' order by name" | bendsql_connect_root
echo "select name, owner from system.databases where name='d20_0014'" | bendsql_connect_root
echo "drop role 'd20_0014_owner'" | bendsql_connect_root
echo "select name, owner from system.tables where name in ('table1', 'table2') and database='d20_0014' order by name" | bendsql_connect_root
echo "select name, owner from system.databases where name='d20_0014'" | bendsql_connect_root
echo "create role 'd20_0014_owner'" | bendsql_connect_root
echo "select name, owner from system.tables where name in ('table1', 'table2') and database='d20_0014' order by name" | bendsql_connect_root
echo "select name, owner from system.databases where name='d20_0014'" | bendsql_connect_root

run_root_sql "
drop role 'd20_0014_owner';
drop database d20_0014;
drop user '${TEST_USER_NAME}';
"
