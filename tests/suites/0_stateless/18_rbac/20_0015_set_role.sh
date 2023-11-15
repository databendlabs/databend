#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=testuser1 --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo '-- reset user, roles, and tables'
echo "DROP USER IF EXISTS 'testuser1'" | $BENDSQL_CLIENT_CONNECT
echo "DROP ROLE IF EXISTS 'testrole1'" | $BENDSQL_CLIENT_CONNECT
echo "DROP ROLE IF EXISTS 'testrole2'" | $BENDSQL_CLIENT_CONNECT

echo '-- prepare user, roles, and tables for tests'
echo "CREATE USER 'testuser1' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT
echo 'CREATE ROLE `testrole1`' | $BENDSQL_CLIENT_CONNECT
echo 'CREATE ROLE `testrole2`' | $BENDSQL_CLIENT_CONNECT
echo 'GRANT ROLE testrole1 to testuser1' | $BENDSQL_CLIENT_CONNECT
echo 'GRANT ROLE testrole2 to testuser1' | $BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE t20_0015_table1(c int not null)" | $BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE t20_0015_table2(c int not null)" | $BENDSQL_CLIENT_CONNECT

echo '-- grant privilege to roles'
echo 'GRANT SELECT, INSERT ON default.t20_0015_table1 TO ROLE testrole1' | $BENDSQL_CLIENT_CONNECT
echo 'GRANT SELECT, INSERT ON default.t20_0015_table2 TO ROLE testrole2' | $BENDSQL_CLIENT_CONNECT

echo '-- test 1: set role as testrole1, then SELECT current_role()'
echo "SET ROLE testrole1; SELECT current_role();" | $TEST_USER_CONNECT

echo '-- test 2: set role as testrole1, secondary roles as NONE, can access table1, can not access table2'
echo "SET ROLE testrole1; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT
echo "SET ROLE testrole1; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT || true

echo '-- test 3: set role as testrole2, secondary roles as NONE, can access table2, can not access table1'
echo "SET ROLE testrole2; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT || true
echo "SET ROLE testrole2; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT

echo '-- test 4: set role as testrole1, secondary roles as ALL, can access both table1 and table2'
echo "SET ROLE testrole1; SET SECONDARY ROLES ALL; INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT
echo "SET ROLE testrole1; SET SECONDARY ROLES ALL; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT

echo '-- test 5: set role as testrole1, secondary roles defaults as ALL, can both table1 and table2'
echo "SET ROLE testrole1; INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT
echo "SET ROLE testrole1; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT

echo '-- test 6: not change role, secondary roles defaults as ALL, can both table1 and table2'
echo "INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT
echo "INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT