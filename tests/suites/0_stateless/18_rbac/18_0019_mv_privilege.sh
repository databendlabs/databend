#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_NAME="mv_user"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql_connect_user mv_user password --database default"

run_root_sql "
drop user if exists 'mv_user';
drop role if exists mv_role;
drop materialized view if exists default.test_mv_priv;
drop table if exists default.mv_priv_src;
create table default.mv_priv_src(id int, val varchar);
insert into default.mv_priv_src values(1,'a'),(2,'b');
create materialized view default.test_mv_priv as select val, count(*) as cnt from default.mv_priv_src group by val;
create user 'mv_user' IDENTIFIED BY 'password' with DEFAULT_ROLE='mv_role';
create role mv_role;
grant role mv_role to mv_user;
" > /dev/null

echo "=== SELECT on MV requires privilege ==="
echo "select * from default.test_mv_priv order by val" | $TEST_USER_CONNECT

run_root_sql "grant select on default.test_mv_priv to role mv_role;"
echo "select * from default.test_mv_priv order by val" | $TEST_USER_CONNECT

echo "=== REFRESH MV requires INSERT+DELETE on MV and SELECT on source ==="
echo "refresh materialized view default.test_mv_priv" | $TEST_USER_CONNECT

run_root_sql "grant insert, delete on default.test_mv_priv to role mv_role;"
echo "refresh materialized view default.test_mv_priv" | $TEST_USER_CONNECT

run_root_sql "grant select on default.mv_priv_src to role mv_role;"
echo "refresh materialized view default.test_mv_priv" | $TEST_USER_CONNECT

echo "=== DROP MV requires DROP privilege ==="
echo "drop materialized view default.test_mv_priv" | $TEST_USER_CONNECT

run_root_sql "grant drop on default.test_mv_priv to role mv_role;"
echo "drop materialized view default.test_mv_priv" | $TEST_USER_CONNECT

echo "=== Cleanup ==="
run_root_sql "
drop table if exists default.mv_priv_src;
drop user if exists 'mv_user';
drop role if exists mv_role;
"
