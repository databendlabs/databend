#!/usr/bin/env bash
# End-to-end checks of the batched ownership fallback in multi-table SELECT: tables on the grant
# fast path mixed with system tables and table functions, role scope (SET ROLE / SECONDARY ROLES),
# ownership changes between statements, and owner roles that no longer exist. Meta request
# counts and cache behaviour are asserted in src/query/service/tests/privilege_access.rs.

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

run_user() {
	local user="$1"
	local sql="$2"
	printf '%s\n' "$sql" | bendsql_connect_user "$user" password -A --output tsv --quote-style=never
}

expect_denied() {
	local output
	if output=$(run_user "$1" "$2" 2>&1); then
		echo "expected SELECT permission denial"
		return 1
	fi
	if ! grep -q 'privilege \[Select\]' <<<"$output"; then
		printf '%s\n' "$output"
		return 1
	fi
	echo denied
}

cleanup() {
	bendsql_connect_root_null --query="
DROP DATABASE IF EXISTS access_cache;
DROP DATABASE IF EXISTS access_cache_other;
DROP USER IF EXISTS cache_db_owner;
DROP USER IF EXISTS cache_roles;
DROP USER IF EXISTS cache_gone;
DROP ROLE IF EXISTS cache_db_owner_role;
DROP ROLE IF EXISTS cache_limited_role;
DROP ROLE IF EXISTS cache_gone_role;
"
}

cleanup
trap cleanup EXIT
bendsql_connect_root_null --query="
CREATE DATABASE access_cache;
CREATE DATABASE access_cache_other;
CREATE TABLE access_cache.t1(a INT);
CREATE TABLE access_cache.t2(a INT);
CREATE TABLE access_cache.t_gone(a INT);
CREATE TABLE access_cache_other.t3(a INT);
INSERT INTO access_cache.t1 VALUES (1);
INSERT INTO access_cache.t2 VALUES (2);
INSERT INTO access_cache_other.t3 VALUES (3);
CREATE USER cache_db_owner IDENTIFIED BY 'password';
CREATE USER cache_roles IDENTIFIED BY 'password';
CREATE USER cache_gone IDENTIFIED BY 'password';
CREATE ROLE cache_db_owner_role;
CREATE ROLE cache_limited_role;
CREATE ROLE cache_gone_role;
GRANT OWNERSHIP ON access_cache.* TO ROLE cache_db_owner_role;
GRANT OWNERSHIP ON access_cache.t_gone TO ROLE cache_gone_role;
GRANT ROLE cache_db_owner_role TO cache_db_owner;
GRANT ROLE cache_db_owner_role TO cache_roles;
GRANT ROLE cache_limited_role TO cache_roles;
GRANT ROLE cache_gone_role TO cache_gone;
"

echo '=== owned tables next to system tables, table functions and another database ==='
run_user cache_db_owner 'SELECT count(*) FROM access_cache.t1 a, access_cache.t2 b, system.one c, numbers(3) d'
expect_denied cache_db_owner 'SELECT count(*) FROM access_cache.t1 a, access_cache_other.t3 b, access_cache.t1 c'
bendsql_connect_root_null --query='GRANT OWNERSHIP ON access_cache_other.* TO ROLE cache_db_owner_role;'
run_user cache_db_owner 'SELECT count(*) FROM access_cache.t1 a, access_cache_other.t3 b, access_cache.t1 c'

echo '=== ownership follows the active roles ==='
expect_denied cache_roles 'SET ROLE cache_limited_role; SET SECONDARY ROLES NONE; SELECT count(*) FROM access_cache.t1 a, access_cache.t2 b'
run_user cache_roles 'SET ROLE cache_limited_role; SET SECONDARY ROLES ALL; SELECT count(*) FROM access_cache.t1 a, access_cache.t2 b'
run_user cache_roles 'SET ROLE cache_db_owner_role; SET SECONDARY ROLES NONE; SELECT count(*) FROM access_cache.t1 a, access_cache.t2 b'

echo '=== dropped owner role falls back to account_admin ==='
run_user cache_gone 'SELECT count(*) FROM access_cache.t_gone a, access_cache.t_gone b'
bendsql_connect_root_null --query='DROP ROLE cache_gone_role;'
expect_denied cache_gone 'SELECT count(*) FROM access_cache.t_gone a, access_cache.t_gone b'
bendsql_connect_root --output tsv --query='SELECT count(*) FROM access_cache.t_gone a, access_cache.t_gone b'

echo '=== ownership changes between statements are seen by the next one ==='
bendsql_connect_root_null --query='GRANT OWNERSHIP ON access_cache.* TO ROLE cache_limited_role;'
expect_denied cache_db_owner 'SELECT count(*) FROM access_cache.t1 a, access_cache.t2 b'
bendsql_connect_root_null --query="
DROP DATABASE access_cache_other;
CREATE DATABASE access_cache_other;
CREATE TABLE access_cache_other.t3(a INT);
"
expect_denied cache_db_owner 'SELECT count(*) FROM access_cache_other.t3 a, access_cache_other.t3 b'
