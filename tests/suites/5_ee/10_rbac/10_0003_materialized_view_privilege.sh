#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export MV_PRIV_USER_CONNECT="bendsql_connect_user mv-priv-user password -A"

show_visible_mv_count() {
	echo "select count(*) from system.materialized_views where database = 'mv_rbac_db_0020' and name = 'mv'" | $MV_PRIV_USER_CONNECT
}

run_root_sql "
drop user if exists 'mv-priv-user';
drop role if exists mv_priv_role;
drop database if exists mv_rbac_db_0020;
create role mv_priv_role;
create user 'mv-priv-user' identified by 'password' with default_role = 'mv_priv_role';
grant role mv_priv_role to 'mv-priv-user';
create database mv_rbac_db_0020;
create table mv_rbac_db_0020.source(c int) change_tracking = true;
"

echo "=== CREATE MV requires CREATE on the database and SELECT on the source ==="
echo "create materialized view mv_rbac_db_0020.mv as select c from mv_rbac_db_0020.source where 1 = 1" | $MV_PRIV_USER_CONNECT
run_root_sql "grant create on mv_rbac_db_0020.* to role mv_priv_role;"
echo "create materialized view mv_rbac_db_0020.mv as select c from mv_rbac_db_0020.source where 1 = 1" | $MV_PRIV_USER_CONNECT
run_root_sql "grant select on mv_rbac_db_0020.source to role mv_priv_role;"
echo "create materialized view mv_rbac_db_0020.mv as select c from mv_rbac_db_0020.source where 1 = 1" | $MV_PRIV_USER_CONNECT

# Materialized-view access is authorized through the source table.
run_root_sql "revoke select on mv_rbac_db_0020.source from role mv_priv_role;"

echo "=== SELECT, REFRESH, and SHOW CREATE MATERIALIZED VIEW require only SELECT on the source ==="
echo "select * from mv_rbac_db_0020.mv" | $MV_PRIV_USER_CONNECT
echo "refresh materialized view mv_rbac_db_0020.mv" | $MV_PRIV_USER_CONNECT
echo "show create materialized view mv_rbac_db_0020.mv" | $MV_PRIV_USER_CONNECT

echo "=== system.materialized_views visibility follows the source privileges ==="
run_root_sql "
grant insert on mv_rbac_db_0020.source to role mv_priv_role;
grant select on mv_rbac_db_0020.mv to role mv_priv_role;
"
echo "--- source INSERT and MV SELECT are insufficient ---"
show_visible_mv_count

run_root_sql "grant select on mv_rbac_db_0020.source to role mv_priv_role;"
echo "--- source table SELECT makes the MV visible ---"
show_visible_mv_count

run_root_sql "
revoke select on mv_rbac_db_0020.source from role mv_priv_role;
grant select on mv_rbac_db_0020.* to role mv_priv_role;
"
echo "--- source database SELECT makes the MV visible ---"
show_visible_mv_count

run_root_sql "
revoke select on mv_rbac_db_0020.* from role mv_priv_role;
grant ownership on mv_rbac_db_0020.source to role mv_priv_role;
"
echo "--- source table ownership makes the MV visible ---"
show_visible_mv_count

run_root_sql "
grant ownership on mv_rbac_db_0020.source to role account_admin;
grant ownership on mv_rbac_db_0020.* to role mv_priv_role;
"
echo "--- source database ownership makes the MV visible ---"
show_visible_mv_count

run_root_sql "grant ownership on mv_rbac_db_0020.* to role account_admin;"
echo "--- revoking source ownership hides the MV again ---"
show_visible_mv_count

# Source SELECT alone authorizes MV reads and management; MV SELECT is not required.
run_root_sql "
revoke select on mv_rbac_db_0020.mv from role mv_priv_role;
grant select on mv_rbac_db_0020.source to role mv_priv_role;
"
echo "select * from mv_rbac_db_0020.mv" | $MV_PRIV_USER_CONNECT >/dev/null
echo "refresh materialized view mv_rbac_db_0020.mv" | $MV_PRIV_USER_CONNECT >/dev/null
echo "show create materialized view mv_rbac_db_0020.mv" | $MV_PRIV_USER_CONNECT >/dev/null

echo "=== DROP MV requires DROP on the database ==="
echo "drop materialized view mv_rbac_db_0020.mv" | $MV_PRIV_USER_CONNECT
echo "drop materialized view if exists mv_rbac_db_0020.missing" | $MV_PRIV_USER_CONNECT
run_root_sql "grant drop on mv_rbac_db_0020.* to role mv_priv_role;"
echo "drop materialized view mv_rbac_db_0020.mv" | $MV_PRIV_USER_CONNECT

run_root_sql "
drop database if exists mv_rbac_db_0020;
drop user 'mv-priv-user';
drop role mv_priv_role;
"
