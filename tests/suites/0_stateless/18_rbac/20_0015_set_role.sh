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
echo 'GRANT SELECT, INSERT ON testrole1 TO ROLE t20_0015_table1' | $BENDSQL_CLIENT_CONNECT
echo 'GRANT SELECT, INSERT ON testrole2 TO ROLE t20_0015_table2' | $BENDSQL_CLIENT_CONNECT

echo '-- test 1: set role as role1, secondary roles as NONE, can access table1, can not access table2'
echo "SET ROLE testrole1; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT
echo "SET ROLE testrole1; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT