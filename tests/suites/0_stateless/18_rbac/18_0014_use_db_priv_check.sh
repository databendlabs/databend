#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


export TEST_USER_PASSWORD="password"
export USER_TEST_CONNECT="bendsql -A --user=test --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

run_root_sql "
drop user if exists test;
create user test identified by '$TEST_USER_PASSWORD';
create or replace database a;
create or replace database b;
create or replace database c;
create or replace table a.t(ida int);
create or replace table a.t1(ida1 int);
create or replace table b.t(idb int);
create or replace table b.t1(idb1 int);
drop role if exists test_role1;
drop role if exists test_role2;
create role test_role1;
create role test_role2;
grant usage on a.* to role test_role1;
grant usage on b.* to role test_role1;
grant ownership on b.t to role test_role1;
grant role test_role1 to test;
grant select on a.t1 to test;
"

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

run_root_sql "
drop user if exists test;
drop role if exists test_role1;
drop role if exists test_role2;
drop database a;
drop database b;
drop database c;
"
