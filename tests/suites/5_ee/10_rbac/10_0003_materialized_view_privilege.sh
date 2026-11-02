#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

user_sql() {
	printf '%s\n' "$1" | bendsql_connect_user mv-priv-user mv-select-test -A -o tsv --quote-style=never
}

root_sql() {
	printf '%s\n' "$1" | bendsql_connect_root -A -o null
}

expect_result() {
	local label="$1" sql="$2" expected="$3" actual
	actual=$(user_sql "$sql")
	if [[ "$actual" != "$expected" ]]; then
		printf '%s: expected <%s>, got <%s>\n' "$label" "$expected" "$actual" >&2
		exit 1
	fi
	echo "$label: OK"
}

expect_error() {
	local executor="$1" label="$2" code="$3" sql="$4" actual
	if actual=$("$executor" "$sql" 2>&1); then
		echo "$label: unexpectedly succeeded" >&2
		exit 1
	fi
	if [[ "$actual" != *"[$code]"* ]]; then
		printf '%s: expected error %s, got <%s>\n' "$label" "$code" "$actual" >&2
		exit 1
	fi
	echo "$label: OK"
}

root_sql "
drop user if exists 'mv-priv-user';
drop role if exists mv_priv_role;
drop database if exists mv_rbac_db_0020;
create role mv_priv_role;
create user 'mv-priv-user' identified by 'mv-select-test' with default_role = 'mv_priv_role';
grant role mv_priv_role to 'mv-priv-user';
create database mv_rbac_db_0020;
create table mv_rbac_db_0020.source(c int, content string) change_tracking = true;
insert into mv_rbac_db_0020.source values (1, 'one'), (2, 'two'), (-100, 'hidden');
"

# CREATE and ownership behavior are unchanged: creating an MV grants no SELECT on it.
expect_error user_sql 'CREATE requires database CREATE' 1063 \
	'create materialized view mv_rbac_db_0020.mv as select c, content from mv_rbac_db_0020.source where c > 0'
root_sql 'grant create on mv_rbac_db_0020.* to role mv_priv_role;'
expect_error user_sql 'CREATE requires source SELECT' 1063 \
	'create materialized view mv_rbac_db_0020.mv as select c, content from mv_rbac_db_0020.source where c > 0'
root_sql 'grant select on mv_rbac_db_0020.source to role mv_priv_role;'
user_sql 'create materialized view mv_rbac_db_0020.mv as select c, content from mv_rbac_db_0020.source where c > 0'
expect_error user_sql 'Source SELECT does not authorize direct MV SELECT' 1063 \
	'select c from mv_rbac_db_0020.mv'
expect_result 'Source SELECT does not make the MV visible' \
	"select count(*) from system.materialized_views where database = 'mv_rbac_db_0020' and name = 'mv'" '0'
root_sql "
create materialized view mv_rbac_db_0020.mv_sum as select sum(c) as total from mv_rbac_db_0020.source where c > 0;
create view mv_rbac_db_0020.v as select c from mv_rbac_db_0020.mv;
revoke select on mv_rbac_db_0020.source from role mv_priv_role;
"

# Both GRANT and REVOKE must use the explicit MV object type.
expect_error root_sql 'Bare GRANT rejects an MV' 1302 \
	'grant select on mv_rbac_db_0020.mv to role mv_priv_role'
expect_error root_sql 'Bare REVOKE rejects an MV' 1302 \
	'revoke select on mv_rbac_db_0020.mv from role mv_priv_role'
expect_error root_sql 'MV GRANT rejects a base table' 1302 \
	'grant select on materialized view mv_rbac_db_0020.source to role mv_priv_role'
expect_error root_sql 'MV REVOKE rejects a regular view' 1302 \
	'revoke select on materialized view mv_rbac_db_0020.v from role mv_priv_role'

root_sql "
grant select on materialized view mv_rbac_db_0020.mv to role mv_priv_role;
grant select on materialized view mv_rbac_db_0020.mv_sum to role mv_priv_role;
"
# Exercise detail and aggregate storage paths together in each read mode.
read_mv='select m.c, s.total from mv_rbac_db_0020.mv m cross join mv_rbac_db_0020.mv_sum s order by m.c'
expect_result 'LiveFallback reads need only MV SELECT' "$read_mv" $'1\t3\n2\t3'
expect_error user_sql 'MV SELECT does not authorize source SELECT' 1063 \
	'select c from mv_rbac_db_0020.source'
expect_result 'MV SELECT makes the MV visible' \
	"select count(*) from system.materialized_views where database = 'mv_rbac_db_0020' and name = 'mv'" '1'
# Replaying SHOW GRANTS also checks it emits the required explicit MV type.
grant_sql=$(user_sql "select grants from show_grants('role', 'mv_priv_role') where object_name = 'default.mv_rbac_db_0020.mv'")
[[ "$grant_sql" == *'ON MATERIALIZED VIEW '* ]]
root_sql "$grant_sql"
echo 'SHOW GRANTS emits executable MV syntax: OK'

# SELECT on an MV does not authorize refresh, definition inspection, or index maintenance.
expect_error user_sql 'REFRESH still requires source SELECT' 1063 \
	'refresh materialized view mv_rbac_db_0020.mv'
expect_error user_sql 'SHOW CREATE still requires source SELECT' 1063 \
	'show create materialized view mv_rbac_db_0020.mv'
expect_error user_sql 'CREATE INDEX still requires source SELECT' 1063 \
	'create inverted index mv_rbac_idx_0020 on mv_rbac_db_0020.mv(content)'
root_sql 'create inverted index mv_rbac_idx_0020 on mv_rbac_db_0020.mv(content);'
expect_error user_sql 'DROP INDEX still requires source SELECT' 1063 \
	'drop inverted index mv_rbac_idx_0020 on mv_rbac_db_0020.mv'
root_sql 'drop inverted index mv_rbac_idx_0020 on mv_rbac_db_0020.mv;'

root_sql "
refresh materialized view mv_rbac_db_0020.mv;
refresh materialized view mv_rbac_db_0020.mv_sum;
"
expect_result 'Fresh reads need only MV SELECT' "$read_mv" $'1\t3\n2\t3'
root_sql 'revoke select on materialized view mv_rbac_db_0020.mv_sum from role mv_priv_role;'
expect_error user_sql 'Each joined MV requires its own SELECT' 1063 "$read_mv"
root_sql 'grant select on materialized view mv_rbac_db_0020.mv_sum to role mv_priv_role;'
root_sql "insert into mv_rbac_db_0020.source values (3, 'three');"
expect_result 'Hybrid reads need only MV SELECT' "$read_mv" $'1\t6\n2\t6\n3\t6'
root_sql 'update mv_rbac_db_0020.source set c = 4 where c = 2;'
expect_result 'UPDATE fallback needs only MV SELECT' "$read_mv" $'1\t8\n3\t8\n4\t8'
expect_result 'Caller CTE cannot capture the MV source' \
	'with source as (select 999 as c) select total from mv_rbac_db_0020.mv_sum' '8'

# Internal source scans must not exempt an independent caller reference to the source.
expect_error user_sql 'JOIN still checks an explicit source reference' 1063 \
	'select m.c from mv_rbac_db_0020.mv m join mv_rbac_db_0020.source s on m.c = s.c'
expect_error user_sql 'Subquery still checks an explicit source reference' 1063 \
	'select c from mv_rbac_db_0020.mv where c in (select c from mv_rbac_db_0020.source)'
expect_error user_sql 'UNION still checks an explicit source reference' 1063 \
	'select c from mv_rbac_db_0020.mv union all select c from mv_rbac_db_0020.source'
expect_error user_sql 'CTE still checks an explicit source reference' 1063 \
	'with s as (select c from mv_rbac_db_0020.source) select m.c from mv_rbac_db_0020.mv m join s on m.c = s.c'
root_sql 'grant select on mv_rbac_db_0020.source to role mv_priv_role;'
expect_result 'JOIN succeeds with both object grants' \
	'select m.c from mv_rbac_db_0020.mv m join mv_rbac_db_0020.source s on m.c = s.c order by m.c' $'1\n3\n4'

# An ordinary view remains an independent access boundary around an expanded MV.
root_sql "
grant select on mv_rbac_db_0020.v to role mv_priv_role;
revoke select on mv_rbac_db_0020.source from role mv_priv_role;
revoke select on materialized view mv_rbac_db_0020.mv from role mv_priv_role;
"
expect_error user_sql 'REVOKE denies direct MV SELECT even during fallback' 1063 \
	'select c from mv_rbac_db_0020.mv'
expect_result 'Regular view authorizes fallback MV reads' \
	'select c from mv_rbac_db_0020.v order by c' $'1\n3\n4'
expect_result 'Revoked MV SELECT hides the MV' \
	"select count(*) from system.materialized_views where database = 'mv_rbac_db_0020' and name = 'mv'" '0'

# A source query may use an MV without a grant on that MV. Other candidates must not
# introduce additional permission checks, even when the requested expression cannot rewrite.
root_sql "
create table mv_rbac_db_0020.rewrite_source(c int);
insert into mv_rbac_db_0020.rewrite_source select number % 2 from numbers(10000);
create materialized view mv_rbac_db_0020.rewrite_mv as select c, count(*) as n from mv_rbac_db_0020.rewrite_source group by c;
refresh materialized view mv_rbac_db_0020.rewrite_mv;
grant select on mv_rbac_db_0020.rewrite_source to role mv_priv_role;
"
plan=$(user_sql 'explain select c, count(*) from mv_rbac_db_0020.rewrite_source group by c')
if [[ "$plan" != *'table: default.mv_rbac_db_0020.rewrite_mv'* ]]; then
	printf 'Expected MV rewrite, got:\n%s\n' "$plan" >&2
	exit 1
fi
echo 'Source SELECT permits MV rewrite without MV SELECT: OK'
expect_result 'Rewritten source query returns correct results' \
	'select c, count(*) from mv_rbac_db_0020.rewrite_source group by c order by c' $'0\t5000\n1\t5000'
expect_result 'Unselected MV candidates require no MV SELECT' \
	'select sum(c) from mv_rbac_db_0020.rewrite_source' '5000'

# Existing management authorization is unchanged, even without MV SELECT.
root_sql 'grant select on mv_rbac_db_0020.source to role mv_priv_role;'
user_sql 'refresh materialized view mv_rbac_db_0020.mv' >/dev/null
user_sql 'show create materialized view mv_rbac_db_0020.mv' >/dev/null
user_sql 'create inverted index mv_rbac_idx_0020 on mv_rbac_db_0020.mv(content)' >/dev/null
user_sql 'drop inverted index mv_rbac_idx_0020 on mv_rbac_db_0020.mv' >/dev/null
echo 'Source SELECT still authorizes existing MV management operations: OK'

# Broader database grants apply to MV reads without creating MV ownership.
root_sql 'grant select on mv_rbac_db_0020.* to role mv_priv_role;'
expect_result 'Database SELECT authorizes MV SELECT' \
	'select c from mv_rbac_db_0020.mv order by c' $'1\n3\n4'
root_sql 'revoke select on mv_rbac_db_0020.* from role mv_priv_role;'
expect_error user_sql 'DROP still requires database DROP' 1063 \
	'drop materialized view mv_rbac_db_0020.mv'
root_sql 'grant drop on mv_rbac_db_0020.* to role mv_priv_role;'
user_sql 'drop materialized view mv_rbac_db_0020.mv'

root_sql "
drop database mv_rbac_db_0020;
drop user 'mv-priv-user';
drop role mv_priv_role;
"
