#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=testuser1 --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo '-- reset user, roles, and tables'
echo "DROP USER IF EXISTS 'testuser1'" | $BENDSQL_CLIENT_CONNECT
echo "DROP ROLE IF EXISTS 'testrole1'" | $BENDSQL_CLIENT_CONNECT
echo "DROP ROLE IF EXISTS 'testrole2'" | $BENDSQL_CLIENT_CONNECT
echo "DROP ROLE IF EXISTS 'testrole3'" | $BENDSQL_CLIENT_CONNECT
echo "DROP ROLE IF EXISTS 'testrole4'" | $BENDSQL_CLIENT_CONNECT
echo "DROP TABLE IF EXISTS t20_0015_table1" | $BENDSQL_CLIENT_CONNECT   
echo "DROP TABLE IF EXISTS t20_0015_table2" | $BENDSQL_CLIENT_CONNECT

echo '-- prepare user, roles, and tables for tests'
echo "CREATE USER 'testuser1' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT
echo 'CREATE ROLE `testrole1`' | $BENDSQL_CLIENT_CONNECT
echo 'CREATE ROLE `testrole2`' | $BENDSQL_CLIENT_CONNECT
echo 'CREATE ROLE `testrole3`' | $BENDSQL_CLIENT_CONNECT
echo 'CREATE ROLE `testrole4`' | $BENDSQL_CLIENT_CONNECT # not granted to testuser1
echo 'GRANT ROLE testrole2 to ROLE testrole3' | $BENDSQL_CLIENT_CONNECT
echo 'GRANT ROLE testrole1 to testuser1' | $BENDSQL_CLIENT_CONNECT
echo 'GRANT ROLE testrole2 to testuser1' | $BENDSQL_CLIENT_CONNECT
echo 'GRANT ROLE testrole3 to testuser1' | $BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE t20_0015_table1(c int not null) ENGINE = MEMORY" | $BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE t20_0015_table2(c int not null) ENGINE = MEMORY" | $BENDSQL_CLIENT_CONNECT

echo '-- grant privilege to roles'
echo 'GRANT SELECT, INSERT ON default.t20_0015_table1 TO ROLE testrole1' | $BENDSQL_CLIENT_CONNECT
echo 'GRANT SELECT, INSERT ON default.t20_0015_table2 TO ROLE testrole2' | $BENDSQL_CLIENT_CONNECT

echo '-- test 1: set role as testrole1, then SELECT current_role()'
echo "SET ROLE testrole1; SELECT current_role();" | $TEST_USER_CONNECT

echo '-- test 2: set a nonexistent role, a existed but not granted role, will fail'
echo "SET ROLE nonexisting_role;" | $TEST_USER_CONNECT || true
echo "SET ROLE testrole4;" | $TEST_USER_CONNECT || true

echo '-- test 3: set role as testrole1, secondary roles as NONE, can access table1, can not access table2'
echo "SET ROLE testrole1; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT
echo "SET ROLE testrole1; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT || true

echo '-- test 4: set role as testrole2, secondary roles as NONE, can access table2, can not access table1'
echo "SET ROLE testrole2; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT || true
echo "SET ROLE testrole2; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT

echo '-- test 5: set role as testrole3, secondary roles as NONE, can access table2, can not access table1, because role3 inherited from role2'
echo "SET ROLE testrole3; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT || true
echo "SET ROLE testrole3; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT

echo '-- test 6: set role as testrole1, secondary roles as ALL, can access both table1 and table2'
echo "SET ROLE testrole1; SET SECONDARY ROLES ALL; INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT
echo "SET ROLE testrole1; SET SECONDARY ROLES ALL; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT

echo '-- test 7: set role as testrole1, testrole2, secondary roles defaults as ALL, can both table1 and table2'
echo "SET ROLE testrole1; INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT
echo "SET ROLE testrole1; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT
echo "SET ROLE testrole2; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT
echo "SET ROLE testrole3; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT

echo '-- test 8: not change role, secondary roles defaults as ALL, can both table1 and table2'
echo "INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT
echo "INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT

echo '-- test 9: set default role as testrole1, secondary roles as NONE, current role will still be testrole1 in another session'
echo "SET DEFAULT ROLE testrole1;" | $TEST_USER_CONNECT
echo "SELECT current_role();" | $TEST_USER_CONNECT

echo '-- test 10: set default role as nonexisting_role, will fail'
echo "SET DEFAULT ROLE nonexistedrole;" | $TEST_USER_CONNECT || true