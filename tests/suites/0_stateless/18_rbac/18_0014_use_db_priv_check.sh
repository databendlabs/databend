#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


export TEST_USER_PASSWORD="password"
export USER_TEST_CONNECT="bendsql -A --user=test --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"


echo "drop user if exists test" | $BENDSQL_CLIENT_CONNECT
echo "create user test identified by '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT

echo "create or replace database a" | $BENDSQL_CLIENT_CONNECT
echo "create or replace database b" | $BENDSQL_CLIENT_CONNECT
echo "create or replace database c" | $BENDSQL_CLIENT_CONNECT
echo "create or replace table a.t(ida int)" | $BENDSQL_CLIENT_CONNECT
echo "create or replace table a.t1(ida1 int)" | $BENDSQL_CLIENT_CONNECT
echo "create or replace table b.t(idb int)" | $BENDSQL_CLIENT_CONNECT
echo "create or replace table b.t1(idb1 int)" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists test_role1" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists test_role2" | $BENDSQL_CLIENT_CONNECT
echo "create role test_role1" | $BENDSQL_CLIENT_CONNECT
echo "create role test_role2" | $BENDSQL_CLIENT_CONNECT
echo "grant usage on a.* to role test_role1" | $BENDSQL_CLIENT_CONNECT
echo "grant usage on b.* to role test_role1" | $BENDSQL_CLIENT_CONNECT
echo "grant ownership on b.t to role test_role1" | $BENDSQL_CLIENT_CONNECT

echo "grant role test_role1 to test" | $BENDSQL_CLIENT_CONNECT
echo "grant select on a.t1 to test" | $BENDSQL_CLIENT_CONNECT

echo "=== show db ==="
echo "show databases" | $USER_TEST_CONNECT


echo "=== use db ==="
echo "use a" | $USER_TEST_CONNECT
echo "use b" | $USER_TEST_CONNECT

echo "=== show tables from a ==="
echo "show tables from a" | $USER_TEST_CONNECT # only display a.t1
echo "=== show tables from b ==="
echo "show tables from b" | $USER_TEST_CONNECT # only display b.t

echo "=== show columns ==="
echo "show columns from t from a" | $USER_TEST_CONNECT
echo "show columns from t1 from a" | $USER_TEST_CONNECT
echo "show columns from t from b" | $USER_TEST_CONNECT
echo "show columns from t1 from b" | $USER_TEST_CONNECT

echo "drop user if exists test" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists test_role1" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists test_role2" | $BENDSQL_CLIENT_CONNECT
echo "drop database a" | $BENDSQL_CLIENT_CONNECT
echo "drop database b" | $BENDSQL_CLIENT_CONNECT
echo "drop database c" | $BENDSQL_CLIENT_CONNECT
