#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_NAME="owner"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=owner --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
echo "drop table if exists d20_0014.table2;" | $BENDSQL_CLIENT_CONNECT

## cleanup
echo "drop database if exists d20_0014;" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists '${TEST_USER_NAME}'" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists 'd20_0014_owner'" | $BENDSQL_CLIENT_CONNECT

## create user
echo "create user '${TEST_USER_NAME}' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT
## create role
echo 'create role `d20_0014_owner`' | $BENDSQL_CLIENT_CONNECT

## create database
echo "create database d20_0014" | $BENDSQL_CLIENT_CONNECT

## ownership transfer to role d20_0014_owner
echo "GRANT OWNERSHIP ON d20_0014.* TO ROLE 'd20_0014_owner'" | $BENDSQL_CLIENT_CONNECT

echo "GRANT ROLE 'd20_0014_owner' TO '${TEST_USER_NAME}'" | $BENDSQL_CLIENT_CONNECT
echo "ALTER USER '${TEST_USER_NAME}' WITH DEFAULT_ROLE='d20_0014_owner'" | $BENDSQL_CLIENT_CONNECT

## owner should have all privileges on the table
echo "create table d20_0014.table1(i int);" | $TEST_USER_CONNECT
echo "create table d20_0014.table2(i int);" | $TEST_USER_CONNECT
echo "insert into d20_0014.table1 values(1),(2),(3);" | $TEST_USER_CONNECT
echo "select * from d20_0014.table1;" | $TEST_USER_CONNECT

echo "=== test drop role ==="
echo "select name, owner from system.tables where name in ('table1', 'table2') and database='d20_0014' order by name" | $BENDSQL_CLIENT_CONNECT
echo "select name, owner from system.databases where name='d20_0014'" | $BENDSQL_CLIENT_CONNECT
echo "drop role 'd20_0014_owner'" | $BENDSQL_CLIENT_CONNECT
echo "select name, owner from system.tables where name in ('table1', 'table2') and database='d20_0014' order by name" | $BENDSQL_CLIENT_CONNECT
echo "select name, owner from system.databases where name='d20_0014'" | $BENDSQL_CLIENT_CONNECT
echo "create role 'd20_0014_owner'" | $BENDSQL_CLIENT_CONNECT
echo "select name, owner from system.tables where name in ('table1', 'table2') and database='d20_0014' order by name" | $BENDSQL_CLIENT_CONNECT
echo "select name, owner from system.databases where name='d20_0014'" | $BENDSQL_CLIENT_CONNECT

## cleanup
echo "drop role 'd20_0014_owner'" | $BENDSQL_CLIENT_CONNECT
echo "drop database d20_0014;" | $BENDSQL_CLIENT_CONNECT
echo "drop user '${TEST_USER_NAME}'" | $BENDSQL_CLIENT_CONNECT
