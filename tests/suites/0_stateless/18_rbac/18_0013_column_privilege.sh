#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


export TEST_USER_PASSWORD="password"
export USER_A_CONNECT="bendsql -A --user=a --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

run_root_sql "
drop user if exists a;
create user a identified by '$TEST_USER_PASSWORD';
create or replace database grant_db;
create table grant_db.t(c1 int not null);
create or replace database nogrant;
create table nogrant.t(id int not null);
grant select on default.* to a;
grant select on grant_db.t to a;
create or replace table default.test_t(id int not null);
"

echo "=== show grants for a ==="
echo "show grants for a" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "=== show databases ==="
echo "show databases" | $USER_A_CONNECT

echo "=== show tables ==="
echo "show tables from system" | $USER_A_CONNECT
echo "show tables from grant_db" | $USER_A_CONNECT
echo "=== use db ==="
echo "use system" | $USER_A_CONNECT
echo "use information_schema" | $USER_A_CONNECT
echo "use grant_db" | $USER_A_CONNECT
echo "=== show columns ==="
echo "show columns from one from system" | $USER_A_CONNECT
echo "show columns from t from grant_db" | $USER_A_CONNECT
echo "show columns from roles from system" | $USER_A_CONNECT
echo "show columns from keywords from information_schema" | $USER_A_CONNECT
echo "show tables from nogrant" | $USER_A_CONNECT
echo "show columns from t from nogrant" | $USER_A_CONNECT

echo "=== grant system to a ==="
echo "grant select on system.* to a" | $BENDSQL_CLIENT_CONNECT
echo "show tables from system" | $USER_A_CONNECT | echo $?
echo "use system" | $USER_A_CONNECT | echo $?

echo "select count(1) from information_schema.columns where table_schema in ('grant_db');" | $USER_A_CONNECT
echo "select count(1) from information_schema.columns where table_schema in ('nogrant');" | $USER_A_CONNECT

run_root_sql "
drop database nogrant;
drop database grant_db;
drop table default.test_t;
drop user a;
"

echo "=== FIX ISSUE 18056 ==="
run_root_sql "
create or replace database db1;
create or replace table db1.t(id1 int);
create or replace database db2;
create or replace table db2.t(id2 int);
drop user if exists a;
create user a identified by '$TEST_USER_PASSWORD';
grant select on db1.t to a;
"

echo "select database, table, name from system.columns where database in ('db1', 'db2') and table='t';" | $USER_A_CONNECT
run_root_sql "drop database if exists db1; drop database if exists db2; drop user if exists a;"

echo "=== FIX: ISSUE 18797 ==="
run_root_sql "
drop user if exists a;
drop role if exists role1;
drop database if exists sysdb;
create user a identified by '$TEST_USER_PASSWORD' with default_role='role1';
create role role1;
grant role role1 to a;
create database sysdb;
create table sysdb.sysdb_tt(id int);
grant ownership on sysdb.* to role role1;
grant ownership on sysdb.sysdb_tt to role role1;
"

echo "select database, table from system.columns where table='sysdb_tt';" | $USER_A_CONNECT
run_root_sql "drop table sysdb.sysdb_tt; drop database if exists sysdb; drop user if exists a; drop role if exists role1;"
echo "======"
