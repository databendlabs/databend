#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


run_root_sql "
drop role if exists role3;
drop database if exists c_r;
drop database if exists c_r1;
drop database if exists c_r2;
create role role3;
grant select on system.one to role role3;
grant update on information_schema.* to role role3;
create database c_r1;
create table c_r1.t1(id int);
create database c_r2;
create table c_r2.t2(id int);
create database c_r;
create table c_r.t(id int);
grant ownership on c_r.* to role role3;
grant ownership on c_r.t to role role3;
grant select on c_r.t to role role3;
grant insert on c_r1.* to role role3;
grant update, delete on c_r1.t1 to role role3;
grant select, insert on c_r2.* to role role3;
grant update, delete on c_r2.t2 to role role3;
"

echo "=== review init role3 grants ==="
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'

echo "=== drop table c_r2.t2 ==="
echo "drop table c_r2.t2;" | $BENDSQL_CLIENT_CONNECT
echo "=== drop c_r2.t2 once ==="
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'

echo "=== create same name table c_r2.t2 ==="
echo "create table c_r2.t2(id int);" | $BENDSQL_CLIENT_CONNECT
echo "grant select on c_r2.t2 to role role3;" | $BENDSQL_CLIENT_CONNECT
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "drop table c_r2.t2;" | $BENDSQL_CLIENT_CONNECT
echo "=== drop c_r2.t2 twice ==="
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "create table c_r2.t2(id int);" | $BENDSQL_CLIENT_CONNECT
echo "grant update, delete on c_r2.t2 to role role3;" | $BENDSQL_CLIENT_CONNECT

echo "=== test database drop ==="
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "drop database c_r;" | $BENDSQL_CLIENT_CONNECT
echo "drop database c_r2;" | $BENDSQL_CLIENT_CONNECT

echo "=== drop database c_r , c_r2 ==="
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'

echo "=== undrop database c_r2 ==="
echo "undrop database c_r2" | $BENDSQL_CLIENT_CONNECT
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'

echo "=== undrop database c_r, contain table c_r.t's ownership ==="
echo "undrop database c_r" | $BENDSQL_CLIENT_CONNECT
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'

echo "=== drop database c_r, c_r2, re-create c_r, c_r2 ==="
echo "drop database c_r" | $BENDSQL_CLIENT_CONNECT
echo "drop database c_r2" | $BENDSQL_CLIENT_CONNECT
echo "create database c_r" | $BENDSQL_CLIENT_CONNECT
echo "create database c_r2" | $BENDSQL_CLIENT_CONNECT
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'

echo "=== show grants test ==="
echo "drop user if exists u1" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists role1" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists role2" | $BENDSQL_CLIENT_CONNECT
echo "create user u1 identified by '123' with DEFAULT_ROLE='role1'" | $BENDSQL_CLIENT_CONNECT
echo "create role role1" | $BENDSQL_CLIENT_CONNECT
echo "create role role2" | $BENDSQL_CLIENT_CONNECT
echo "grant select on *.* to role role1;" | $BENDSQL_CLIENT_CONNECT
echo "grant insert on *.* to role role1;" | $BENDSQL_CLIENT_CONNECT
echo "grant role role1 to u1" | $BENDSQL_CLIENT_CONNECT
echo "grant role role2 to u1" | $BENDSQL_CLIENT_CONNECT

export USER_U1_CONNECT="bendsql -A --user=u1 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo "show grants for role role1" | $USER_U1_CONNECT
echo "show grants for role role2" | $USER_U1_CONNECT
echo "show grants for user u1" | $USER_U1_CONNECT
echo "show grants for user u1 where name='u1' limit 1;" | $USER_U1_CONNECT
echo "show grants for user u1 where name!='u1' limit 1;" | $USER_U1_CONNECT
echo "show grants on database c_r1" | $USER_U1_CONNECT  | awk -F ' ' '{$3=""; print $0}'
echo "show grants" | $USER_U1_CONNECT
echo "Need Err:"
echo "show grants for user root" | $USER_U1_CONNECT
echo "show grants for role account_admin" | $USER_U1_CONNECT

echo "grant grant on *.* to role role1;" | $BENDSQL_CLIENT_CONNECT
echo "show grants for role role3;" | $USER_U1_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "show grants for user 'role3@test';" | $USER_U1_CONNECT

echo "=== clean up ==="
run_root_sql "
drop user if exists u1;
drop role if exists role1;
drop role if exists role2;
drop role if exists role3;
drop database if exists c_r;
drop database if exists c_r1;
drop database if exists c_r2;
"
