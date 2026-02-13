#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql -A --user=testuser1 --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo '-- reset user, roles, and tables'
run_root_sql "
DROP USER IF EXISTS 'testuser1';
DROP ROLE IF EXISTS 'testrole1';
DROP ROLE IF EXISTS 'testrole2';
DROP ROLE IF EXISTS 'testrole3';
DROP ROLE IF EXISTS 'testrole4';
DROP TABLE IF EXISTS t20_0015_table1;
DROP TABLE IF EXISTS t20_0015_table2;
"

echo '-- prepare user, roles, and tables for tests'
run_root_sql "
CREATE USER 'testuser1' IDENTIFIED BY '$TEST_USER_PASSWORD';
CREATE ROLE \`testrole1\`;
CREATE ROLE \`testrole2\`;
CREATE ROLE \`testrole3\`;
CREATE ROLE \`testrole4\`;
GRANT ROLE testrole2 to ROLE testrole3;
GRANT ROLE testrole1 to testuser1;
GRANT ROLE testrole2 to testuser1;
GRANT ROLE testrole3 to testuser1;
CREATE TABLE t20_0015_table1(c int not null) ENGINE = MEMORY;
CREATE TABLE t20_0015_table2(c int not null) ENGINE = MEMORY;
"

echo '-- grant privilege to roles'
run_root_sql "
GRANT SELECT, INSERT ON default.t20_0015_table1 TO ROLE testrole1;
GRANT SELECT, INSERT ON default.t20_0015_table2 TO ROLE testrole2;
"

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
echo "SET SECONDARY ROLES NONE;select parse_json(current_secondary_roles())" | $TEST_USER_CONNECT
echo "select current_available_roles()" | $TEST_USER_CONNECT
echo "SET ROLE testrole3; SET SECONDARY ROLES NONE; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT

echo '-- test 6: set role as testrole1, secondary roles as ALL, can access both table1 and table2'
echo "SET ROLE testrole1; SET SECONDARY ROLES ALL; INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT
echo "SET SECONDARY ROLES ALL;select parse_json(current_secondary_roles())" | $TEST_USER_CONNECT
echo "select current_available_roles()" | $TEST_USER_CONNECT
echo "SET ROLE testrole1; SET SECONDARY ROLES ALL; INSERT INTO t20_0015_table2 VALUES (1);" | $TEST_USER_CONNECT

echo '-- test 6.1: set secondary roles as testrole3, can access table2(because testrole2 can access table2, testrole2 is child of testrole3), can not access table1'
echo "SET SECONDARY ROLES testrole3; INSERT INTO t20_0015_table1 VALUES (1);" | $TEST_USER_CONNECT
echo "SET SECONDARY ROLES testrole3; select parse_json(current_secondary_roles())" | $TEST_USER_CONNECT
echo "SET SECONDARY ROLES testrole3; select current_available_roles()" | $TEST_USER_CONNECT
echo "SET SECONDARY ROLES testrole4; select count(*) from t20_0015_table2;" | $TEST_USER_CONNECT

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

echo '-- test 11: set secondary All | None, create object only check current role'
run_root_sql "
DROP USER if exists 'test_c';
DROP role if exists 'role_c';
CREATE USER 'test_c' IDENTIFIED BY '123';
CREATE ROLE \`role_c\`;
GRANT ALL ON *.* TO ROLE \`role_c\`;
GRANT ROLE \`role_c\` to test_c;
"

export TEST_C_CONNECT="bendsql -A --user=test_c --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
run_root_sql "
drop database if exists db_c;
drop database if exists db_d;
drop database if exists db_e;
"
echo 'SET SECONDARY ROLES ALL;create database db_c' | $TEST_C_CONNECT
echo 'SET SECONDARY ROLES NONE;create database db_c' | $TEST_C_CONNECT
echo 'SET ROLE role_c;SET SECONDARY ROLES NONE;create database db_c' | $TEST_C_CONNECT
echo 'SET ROLE role_c;SET SECONDARY ROLES ALL;create database db_d' | $TEST_C_CONNECT
echo "show grants for role role_c where object_name in ('db_c', 'db_d')" | $TEST_C_CONNECT | awk -F ' ' '{$3=""; print $0}'

run_root_sql "
revoke ROLE \`role_c\` from test_c;
drop ROLE if exists \`role_test\`;
create ROLE \`role_test\`;
grant all on *.* to role \`role_test\`;
grant ROLE \`role_test\` to test_c;
alter user test_c with default_role='role_test';
"

echo 'create database db_e' | $TEST_C_CONNECT
echo "show grants for role public where object_name in ('db_e')" | $TEST_C_CONNECT | awk -F ' ' '{$3=""; print $0}'

run_root_sql "
drop database if exists db_c;
drop database if exists db_d;
drop database if exists db_e;
DROP USER if exists 'test_c';
DROP role if exists 'role_c';
drop ROLE if exists \`role_test\`;
"
