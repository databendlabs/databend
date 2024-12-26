#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop role if exists role1" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists role2" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists u1" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists u2" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists db1" | $BENDSQL_CLIENT_CONNECT
echo "create database db1" | $BENDSQL_CLIENT_CONNECT
echo "create table db1.t1(id int, title string, inverted index idx1(title))" | $BENDSQL_CLIENT_CONNECT
echo "insert into db1.t1 values(1, 'hello world')" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "create role role1;" | $BENDSQL_CLIENT_CONNECT
echo "create role role2;" | $BENDSQL_CLIENT_CONNECT
echo "grant select on db1.t1 to role role1;" | $BENDSQL_CLIENT_CONNECT
echo "create user u1 identified by '123' with DEFAULT_ROLE='role1';" | $BENDSQL_CLIENT_CONNECT
echo "create user u2 identified by '123' with DEFAULT_ROLE='role2';" | $BENDSQL_CLIENT_CONNECT
echo "grant role role1 to u1;" | $BENDSQL_CLIENT_CONNECT
echo "grant role role2 to u2;" | $BENDSQL_CLIENT_CONNECT

echo "=== test u1 with role1 ==="
export TEST_U1_CONNECT="bendsql --user=u1 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
echo "select name, type, definition from system.indexes order by name" | $TEST_U1_CONNECT

echo "=== test u2 with role2 ==="
export TEST_U2_CONNECT="bendsql --user=u2 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
echo "select name, type, definition from system.indexes order by name" | $TEST_U2_CONNECT

