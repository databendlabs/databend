#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_NAME="owner"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql -A --user=owner --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

export TEST_TRANSFER_USER_CONNECT="bendsql -A --user=owner1 --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

run_root_sql "
drop database if exists d_0002;
drop user if exists '${TEST_USER_NAME}';
drop user if exists 'owner1';
drop role if exists 'r_0002';
drop role if exists 'r_0002_1';
drop stage if exists hello;
drop function if exists a;
create user '${TEST_USER_NAME}' IDENTIFIED BY '$TEST_USER_PASSWORD';
create role \`r_0002\`;
GRANT ROLE 'r_0002' TO '${TEST_USER_NAME}';
GRANT CREATE DATABASE, SUPER ON *.* TO ROLE 'r_0002';
create ROLE r_0002_1;
create user 'owner1' IDENTIFIED BY 'password';
grant ROLE r_0002_1 to user owner1;
"

echo "set default role 'r_0002'" | $TEST_USER_CONNECT

## database/table
echo "=== test db/table ==="
echo "create database d_0002" | $TEST_USER_CONNECT
echo "create or replace table d_0002.t(id int)" | $TEST_USER_CONNECT
echo "insert into d_0002.t values(200)" | $TEST_USER_CONNECT
echo "select * from d_0002.t" | $TEST_USER_CONNECT

## stage
echo "=== test stage ==="
echo 'create stage hello' | $TEST_USER_CONNECT
echo 'COPY INTO @hello from (select number from numbers(1)) FILE_FORMAT = (type = parquet)' | $TEST_USER_CONNECT | cut -d$'\t' -f1,2
echo 'select * from @hello' | $TEST_USER_CONNECT

## udf
echo "=== test udf ==="
echo "create function a as (a) -> (a+1);" | $TEST_USER_CONNECT
echo "select a(1);" | $TEST_USER_CONNECT

# ownership transfer
echo "=== test ownership r_0002 transfer to r_0002_1 ==="
echo "GRANT OWNERSHIP on d_0002.* to role 'r_0002_1'" | $BENDSQL_CLIENT_CONNECT
echo "GRANT OWNERSHIP on d_0002.t to role 'r_0002_1'" | $BENDSQL_CLIENT_CONNECT
echo "GRANT OWNERSHIP on stage hello to role 'r_0002_1'" | $BENDSQL_CLIENT_CONNECT
echo "GRANT OWNERSHIP on udf a to role 'r_0002_1'" | $BENDSQL_CLIENT_CONNECT
echo "set default role 'r_0002_1'" | $TEST_TRANSFER_USER_CONNECT
echo "=== test role r_0002_1 ==="
echo "create table d_0002.t1(id int)" | $TEST_TRANSFER_USER_CONNECT
echo "select a(1);" | $TEST_TRANSFER_USER_CONNECT
echo 'select * from @hello' | $TEST_TRANSFER_USER_CONNECT
echo "select * from d_0002.t" | $TEST_TRANSFER_USER_CONNECT

echo "=== test role r_0002 ==="
echo "create table d_0002.t2(id int)" | $TEST_TRANSFER_USER_CONNECT
echo "select a(1);" | $TEST_USER_CONNECT
echo 'select * from @hello' | $TEST_USER_CONNECT
echo "select * from d_0002.t" | $TEST_USER_CONNECT

run_root_sql "
drop database d_0002;
"
echo "drop stage hello;" | $TEST_TRANSFER_USER_CONNECT
echo "drop stage if exists noexistshello;" | $TEST_TRANSFER_USER_CONNECT
echo "drop function a;" | $TEST_TRANSFER_USER_CONNECT
echo "drop function if exists noexistsa;" | $TEST_TRANSFER_USER_CONNECT
run_root_sql "
drop user '${TEST_USER_NAME}';
drop user 'owner1';
drop role 'r_0002_1';
drop role 'r_0002';
"

echo "=== test ownership: show stmt ==="
run_root_sql "
drop user if exists a;
drop user if exists b;
drop database if exists db_a;
drop role if exists role1;
create role role1;
create user a identified by '123';
create database db_a;
grant ownership on db_a.* to role role1;
grant role role1 to a;
create user b identified by '123';
"

export USER_A_CONNECT="bendsql -A --user=a --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_B_CONNECT="bendsql -A --user=b --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo "set default role role1;" | $USER_A_CONNECT
echo "show roles;" | $USER_A_CONNECT
echo "create table db_a.t(id int);" | $USER_A_CONNECT
echo "create table db_a.t1(id int);" | $USER_A_CONNECT
echo "show tables from db_a" | $USER_A_CONNECT
echo "show databases" | $USER_A_CONNECT

echo "show grants for role role1;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'

echo "grant role role1 to b;" | $BENDSQL_CLIENT_CONNECT
echo "show grants for b;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "show grants for a;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'


echo "show tables from db_a;" | $USER_A_CONNECT
echo "show tables from db_a;" | $USER_B_CONNECT

echo "revoke role role1 from a;" | $BENDSQL_CLIENT_CONNECT
echo "show grants for a ;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'

echo "show tables from db_a;" | $USER_A_CONNECT
echo "show tables from db_a;" | $USER_B_CONNECT

echo "drop table db_a.t1;" | $USER_B_CONNECT
run_root_sql "
drop database db_a;
drop role if exists role1;
drop user if exists a;
drop user if exists b;
"

echo "=== fix_issue_14572: test drop role; grant ownership ==="
run_root_sql "
drop role if exists drop_role;
drop role if exists drop_role1;
drop user if exists u1;
drop database if exists a;
create role drop_role;
create role drop_role1;
create user u1 identified by '123' with DEFAULT_ROLE='drop_role';
grant role drop_role to u1;
grant create database on *.* to role drop_role;
"
export USER_U1_CONNECT="bendsql -A --user=u1 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo "create database a" | $USER_U1_CONNECT
echo "create table a.t(id int)" | $USER_U1_CONNECT
echo "select name, owner from system.databases where name='a'" | $USER_U1_CONNECT
echo "select name, owner from system.tables where database='a'" | $USER_U1_CONNECT
echo "drop role drop_role;" | $BENDSQL_CLIENT_CONNECT
echo "select name, owner from system.databases where name='a'" | $BENDSQL_CLIENT_CONNECT
echo "select name, owner from system.tables where database='a'" | $BENDSQL_CLIENT_CONNECT
echo "select name, owner from system.tables where database='a'" | $USER_U1_CONNECT
echo "grant ownership on a.* to role drop_role1;" | $BENDSQL_CLIENT_CONNECT
echo "grant ownership on a.t to role drop_role1;" | $BENDSQL_CLIENT_CONNECT
echo "select name, owner from system.databases where name='a'" | $BENDSQL_CLIENT_CONNECT
echo "select name, owner from system.tables where database='a'" | $BENDSQL_CLIENT_CONNECT
echo "select name, owner from system.tables where database='a'" | $USER_U1_CONNECT
echo "show grants for role drop_role1" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
run_root_sql "
drop role drop_role1;
drop user u1;
drop database a;
"


echo "== test create database privilege and drop object ==="
run_root_sql "
create role role1;
grant create database on *.* to role role1;
create user a identified by '123' with DEFAULT_ROLE='role1';
grant role role1 to a;
"

echo "drop database if exists c" | $BENDSQL_CLIENT_CONNECT
echo "create database c" | $USER_A_CONNECT
echo "drop database c" | $USER_A_CONNECT
echo "show tables from c" | $USER_A_CONNECT
run_root_sql "
drop role if exists role1;
drop user if exists a;
"

echo "=== test db owner can access all table under this db ==="
run_root_sql "
drop database if exists db1;
drop role if exists role1;
drop role if exists role2;
drop user if exists u1;
drop user if exists u2;
create database db1;
create role role1;
create role role2;
create table db1.t1(id int);
create table db1.t2(id int);
grant ownership on db1.* to role role1;
grant ownership on db1.t2 to role role2;
create user u1 identified by '123' with default_role ='role1';
create user u2 identified by '123' with default_role ='role2';
grant role role1 to u1;
grant role role2 to u2;
"

echo "set role role1;show tables from default;" | $USER_U1_CONNECT
echo "set role role1;show tables from db1;" | $USER_U1_CONNECT
echo "set role role1;insert into db1.t1 values(1);" | $USER_U1_CONNECT
echo "set role role1;insert into db1.t2 values(2);" | $USER_U1_CONNECT
echo "set role role1;select * from db1.t1;" | $USER_U1_CONNECT
echo "set role role1;select * from db1.t2;" | $USER_U1_CONNECT
export USER_U2_CONNECT="bendsql -A --user=u2 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
echo "set role role2;select * from db1.t1;" | $USER_U2_CONNECT
echo "set role role2;select * from db1.t2;" | $USER_U2_CONNECT

echo "show grants for role role2;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "set role role1;drop table db1.t2;" | $USER_U1_CONNECT
echo "show grants for role role2;" | $BENDSQL_CLIENT_CONNECT

run_root_sql "
drop database if exists db1;
drop role if exists role1;
drop role if exists role2;
drop user if exists u1;
drop user if exists u2;
"
