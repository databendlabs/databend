#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_NAME="owner"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=owner --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"


echo "drop role if exists role1" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists role2" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists u1" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists u2" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists u3" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists db1" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists db2" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists db_u3" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists db_root" | $BENDSQL_CLIENT_CONNECT
echo "create database db_root" | $BENDSQL_CLIENT_CONNECT
echo "create role role1;" | $BENDSQL_CLIENT_CONNECT
echo "create role role2;" | $BENDSQL_CLIENT_CONNECT
echo "grant create database on *.* to role role1;" | $BENDSQL_CLIENT_CONNECT
echo "grant create database on *.* to role role2;" | $BENDSQL_CLIENT_CONNECT
echo "create user u1 identified by '123' with DEFAULT_ROLE='role1';" | $BENDSQL_CLIENT_CONNECT
echo "create user u2 identified by '123' with DEFAULT_ROLE='role1';" | $BENDSQL_CLIENT_CONNECT
echo "create user u3 identified by '123' with DEFAULT_ROLE='role2';" | $BENDSQL_CLIENT_CONNECT

echo "=== test u1 with role1 ==="
echo "grant role role1 to u1;" | $BENDSQL_CLIENT_CONNECT
export TEST_U1_CONNECT="bendsql --user=u1 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
echo "show databases" | $TEST_U1_CONNECT
echo "create database db1;" | $TEST_U1_CONNECT
echo "show databases" | $TEST_U1_CONNECT

echo "=== test u2 with role1 ==="
echo "grant role role1 to u2;" | $BENDSQL_CLIENT_CONNECT
export TEST_U2_CONNECT="bendsql --user=u2 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
echo "show databases" | $TEST_U2_CONNECT
echo "create database db2" | $TEST_U2_CONNECT
echo "show databases" | $TEST_U2_CONNECT

echo "=== test u3 with role2 ==="
echo "grant role role2 to u3;" | $BENDSQL_CLIENT_CONNECT
## http handler set role, set secondary roles will invalid. same reason : https://github.com/datafuselabs/databend/issues/14204
## So in this test, use mysql client
export TEST_U3_CONNECT="mysql --defaults-extra-file=password.out --port ${QUERY_MYSQL_HANDLER_PORT} -s"
echo -e "[mysql]\nhost=${QUERY_MYSQL_HANDLER_HOST}\nuser=u3\npassword=123" >> password.out

echo "show databases" | $TEST_U3_CONNECT
echo "create database db_u3" | $TEST_U3_CONNECT
echo "show databases" | $TEST_U3_CONNECT

echo "=== test u3 with role2 and role1 secondary roles all ==="
echo "grant role role1 to u3" | $BENDSQL_CLIENT_CONNECT
echo "SET SECONDARY ROLES ALL; show databases" | $TEST_U3_CONNECT

echo "=== test u3(set role1) with role2 and role1 secondary roles none ==="
echo "set role role1; SET SECONDARY ROLES NONE; show databases;" | $TEST_U3_CONNECT

echo "=== test root user ==="
echo "show databases" | $BENDSQL_CLIENT_CONNECT
