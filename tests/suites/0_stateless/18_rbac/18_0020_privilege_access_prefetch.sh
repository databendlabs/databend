#!/usr/bin/env bash
# Regression coverage for the query-local privilege cache and the batched ownership
# prefetch in PrivilegeAccess::check. Each case drives a plan that references the same
# objects repeatedly, which is what makes the prefetch and the dedup paths run.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

bendsql_connect_root_null <<SQL
drop database if exists db_0020;
drop database if exists db_0020_other;
drop role if exists role_db_0020;
drop role if exists role_tbl_0020;
drop role if exists role_gone_0020;
drop user if exists u_db_0020;
drop user if exists u_tbl_0020;
drop user if exists u_grant_0020;
drop user if exists u_gone_0020;
create database db_0020;
create database db_0020_other;
create table db_0020.t1(id int);
create table db_0020.t2(id int);
create table db_0020_other.t3(id int);
insert into db_0020.t1 values(1);
insert into db_0020.t2 values(2);
insert into db_0020_other.t3 values(3);
create role role_db_0020;
create role role_tbl_0020;
create role role_gone_0020;
grant ownership on db_0020.* to role role_db_0020;
grant ownership on db_0020.t2 to role role_tbl_0020;
create user u_db_0020 identified by '123' with default_role = 'role_db_0020';
create user u_tbl_0020 identified by '123' with default_role = 'role_tbl_0020';
grant role role_db_0020 to u_db_0020;
grant role role_tbl_0020 to u_tbl_0020;
SQL

export USER_DB_CONNECT="bendsql_connect_user u_db_0020 123 -A"
export USER_TBL_CONNECT="bendsql_connect_user u_tbl_0020 123 -A"

# Database ownership must still cover every table under it, including when a table is
# bound many times in one plan. Each alias resolves to the same ownership object, so this
# is the case the prefetch deduplicates.
echo "=== db owner: repeated binds of the same tables ==="
echo "select count(*) from db_0020.t1 a, db_0020.t1 b, db_0020.t2 c;" | $USER_DB_CONNECT
echo "select id from db_0020.t1 union all select id from db_0020.t1 union all select id from db_0020.t2 order by id;" | $USER_DB_CONNECT
echo "select (select count(*) from db_0020.t1) + (select count(*) from db_0020.t2);" | $USER_DB_CONNECT

# Table ownership alone grants only that table; the sibling under the same database and a
# table in another database must still be denied.
echo "=== table owner: only the owned table ==="
echo "select * from db_0020.t2;" | $USER_TBL_CONNECT
echo "select * from db_0020.t1;" | $USER_TBL_CONNECT 2>&1 | grep -o "Permission denied" | head -1
echo "select * from db_0020_other.t3;" | $USER_TBL_CONNECT 2>&1 | grep -o "Permission denied" | head -1

# A plan mixing an owned table with a denied one must be denied as a whole, even though
# the owned table's ownership entry is prefetched successfully.
echo "=== table owner: owned joined with denied ==="
echo "select count(*) from db_0020.t2 a, db_0020.t1 b;" | $USER_TBL_CONNECT 2>&1 | grep -o "Permission denied" | head -1

# Objects spanning two databases exercise the per-catalog/database keying of the db_id cache.
echo "=== cross-database plan ==="
echo "grant ownership on db_0020_other.* to role role_db_0020;" | bendsql_connect_root_null
echo "select count(*) from db_0020.t1 a, db_0020_other.t3 b;" | $USER_DB_CONNECT
echo "grant ownership on db_0020_other.* to role role_tbl_0020;" | bendsql_connect_root_null
echo "select count(*) from db_0020.t1 a, db_0020_other.t3 b;" | $USER_DB_CONNECT 2>&1 | grep -o "Permission denied" | head -1

# System and information_schema tables are exempt from ownership; mixing them with a
# user table must not disturb the user table's check.
echo "=== system schema mixed with user table ==="
echo "select count(*) > 0 from system.tables where database = 'db_0020';" | $USER_DB_CONNECT
echo "select count(*) from db_0020.t1 a, system.one b;" | $USER_DB_CONNECT
echo "select count(*) > 0 from information_schema.tables where table_schema = 'db_0020';" | $USER_DB_CONNECT

# Table functions carry their own privilege rules and are skipped by the prefetch.
echo "=== table function alongside an owned table ==="
echo "select count(*) from db_0020.t1 a, numbers(3) b;" | $USER_DB_CONNECT

# Access granted by a name-based grant rather than ownership: the eager prefetch must not
# turn a grant-authorized query into a failure.
echo "=== name-based grant, no ownership ==="
bendsql_connect_root_null <<SQL
create user u_grant_0020 identified by '123';
grant select on db_0020.t1 to u_grant_0020;
SQL
export USER_GRANT_CONNECT="bendsql_connect_user u_grant_0020 123 -A"
echo "select count(*) from db_0020.t1 a, db_0020.t1 b;" | $USER_GRANT_CONNECT
echo "select * from db_0020.t2;" | $USER_GRANT_CONNECT 2>&1 | grep -o "Permission denied" | head -1

# An object owned by a dropped role falls back to account_admin: root still reads it, the
# former owner no longer does. This is the fallback the prefetch has to reproduce.
echo "=== owner role dropped, falls back to account_admin ==="
bendsql_connect_root_null <<SQL
create table db_0020.t_gone(id int);
grant ownership on db_0020.t_gone to role role_gone_0020;
create user u_gone_0020 identified by '123' with default_role = 'role_gone_0020';
grant role role_gone_0020 to u_gone_0020;
SQL
export USER_GONE_CONNECT="bendsql_connect_user u_gone_0020 123 -A"
echo "select count(*) from db_0020.t_gone;" | $USER_GONE_CONNECT
echo "drop role role_gone_0020;" | bendsql_connect_root_null
echo "select count(*) from db_0020.t_gone;" | bendsql_connect_root
echo "select count(*) from db_0020.t_gone;" | $USER_GONE_CONNECT 2>&1 | grep -o "Permission denied" | head -1

# Temp tables are skipped by the prefetch and by the ownership check; they must remain
# usable alongside an owned table in the same session.
echo "=== temp table alongside an owned table ==="
echo "create temp table tmp_0020(id int); insert into tmp_0020 values(9); select count(*) from tmp_0020 a, db_0020.t1 b;" | $USER_DB_CONNECT

# A view's source tables are validated through the view, not as directly bound tables.
echo "=== view over owned tables ==="
echo "create view db_0020.v as select id from db_0020.t1 union all select id from db_0020.t2; select count(*) from db_0020.v;" | $USER_DB_CONNECT

bendsql_connect_root_null <<SQL
drop database if exists db_0020;
drop database if exists db_0020_other;
drop role if exists role_db_0020;
drop role if exists role_tbl_0020;
drop user if exists u_db_0020;
drop user if exists u_tbl_0020;
drop user if exists u_grant_0020;
drop user if exists u_gone_0020;
SQL
