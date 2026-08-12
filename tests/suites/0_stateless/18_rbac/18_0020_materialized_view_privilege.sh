#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export MV_PRIV_USER_CONNECT="bendsql_connect_user mv-priv-user password -A"

run_root_sql "
drop user if exists 'mv-priv-user';
drop role if exists mv_priv_role;
drop materialized view if exists mv_rbac_0020;
drop table if exists mv_rbac_source_0020;
create user 'mv-priv-user' identified by 'password' with default_role = 'mv_priv_role';
create role mv_priv_role;
grant role mv_priv_role to 'mv-priv-user';
create table mv_rbac_source_0020(c int) change_tracking = true;
"

echo "=== CREATE MV requires CREATE on the database and SELECT on the source ==="
echo "create materialized view mv_rbac_0020 as select c from mv_rbac_source_0020 where 1 = 1" | $MV_PRIV_USER_CONNECT
run_root_sql "grant create on default.* to role mv_priv_role;"
echo "create materialized view mv_rbac_0020 as select c from mv_rbac_source_0020 where 1 = 1" | $MV_PRIV_USER_CONNECT
run_root_sql "grant select on default.mv_rbac_source_0020 to role mv_priv_role;"
echo "create materialized view mv_rbac_0020 as select c from mv_rbac_source_0020 where 1 = 1" | $MV_PRIV_USER_CONNECT

# Materialized-view access is authorized through the source table.
run_root_sql "revoke select on default.mv_rbac_source_0020 from role mv_priv_role;"

echo "=== SELECT, REFRESH, and SHOW CREATE MATERIALIZED VIEW require only SELECT on the source ==="
echo "select * from mv_rbac_0020" | $MV_PRIV_USER_CONNECT
echo "refresh materialized view mv_rbac_0020" | $MV_PRIV_USER_CONNECT
echo "show create materialized view mv_rbac_0020" | $MV_PRIV_USER_CONNECT
run_root_sql "grant select on default.mv_rbac_source_0020 to role mv_priv_role;"
echo "select * from mv_rbac_0020" | $MV_PRIV_USER_CONNECT >/dev/null
echo "refresh materialized view mv_rbac_0020" | $MV_PRIV_USER_CONNECT >/dev/null
echo "show create materialized view mv_rbac_0020" | $MV_PRIV_USER_CONNECT >/dev/null

echo "=== DROP MV requires DROP on the database ==="
echo "drop materialized view mv_rbac_0020" | $MV_PRIV_USER_CONNECT
echo "drop materialized view if exists mv_rbac_0020_missing" | $MV_PRIV_USER_CONNECT
run_root_sql "grant drop on default.* to role mv_priv_role;"
echo "drop materialized view mv_rbac_0020" | $MV_PRIV_USER_CONNECT

run_root_sql "
drop materialized view if exists mv_rbac_0020;
drop table if exists mv_rbac_source_0020;
drop user 'mv-priv-user';
drop role mv_priv_role;
"
