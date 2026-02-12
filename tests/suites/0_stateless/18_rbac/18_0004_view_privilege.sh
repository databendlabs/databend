#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_NAME="owner"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql -A --user=owner --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

run_root_sql "
drop user if exists 'owner';
drop role if exists role1;
create user 'owner' IDENTIFIED BY '$TEST_USER_PASSWORD' with DEFAULT_ROLE='role1';
create role role1;
grant role role1 to owner;
grant create on default.* to role role1;
drop table if exists t;
drop view if exists v_t;
drop table if exists t_owner;
drop view if exists v_t_owner;
drop view if exists v_t_union;
drop view if exists v_t1;
create table t(id int);
insert into t values(1);
"
echo 'create table t_owner(c1 int)' | $TEST_USER_CONNECT
echo 'insert into t_owner values(2)' | $TEST_USER_CONNECT
run_root_sql "
revoke create on default.* from role role1;
drop role if exists role2;
create role role2;
grant create on default.* to role role2;
grant role role2 to owner;
alter user owner with default_role=role2;
"

echo 'need failed: with 1063'
echo 'create view v_t as select * from t' | $TEST_USER_CONNECT

echo 'need failed: with 1063'
echo 'create view v_t_union as select * from t union all select * from t_owner' | $TEST_USER_CONNECT
echo 'create view v_t_owner as select * from t_owner' | $TEST_USER_CONNECT
run_root_sql "
grant ownership on default.v_t_owner to role role1;
create view v_t as select * from t;
create view v_t_union as select * from t union all select * from t_owner;
"

echo "'select * from v_t order by id' failed."
echo "select * from v_t order by id" | $TEST_USER_CONNECT
run_root_sql "
grant select on default.v_t to owner;
grant select on default.v_t_union to owner;
"
echo "select * from v_t order by id" | $TEST_USER_CONNECT
echo "select * from v_t_owner order by c1" | $TEST_USER_CONNECT
echo "select * from v_t_union order by id" | $TEST_USER_CONNECT

query "select * from v_t order by id"
query "select * from v_t_owner order by c1"
query "select * from v_t_union order by id"

echo "=== create view as select view ==="

run_root_sql "
revoke select on default.v_t from owner;
grant select on default.t to owner;
"
echo 'create view v_t1 as select * from t union select * from v_t' | $TEST_USER_CONNECT
run_root_sql "
grant select on default.v_t to owner;
grant select on default.t to owner;
"
echo 'create view v_t1 as select * from t union select * from v_t' | $TEST_USER_CONNECT
run_root_sql "grant select on default.v_t1 to owner;"
echo "select * from v_t1 order by id" | $TEST_USER_CONNECT

run_root_sql "
drop table if exists t;
drop view if exists v_t;
drop table if exists t_owner;
drop view if exists v_t_owner;
drop view if exists v_t_union;
drop view if exists v_t1;
drop user if exists owner;
drop role if exists role1;
drop role if exists role2;
"
