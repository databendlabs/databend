#!/usr/bin/env bash
# Multi-table SELECT authorization: tables bound several times, grants and ownership mixed in
# one statement, and the batched ownership fallback for tables without a matching grant.

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
DROP USER IF EXISTS cache_t2_owner;
DROP USER IF EXISTS cache_mixed;
DROP USER IF EXISTS cache_gone;
DROP ROLE IF EXISTS cache_db_owner_role;
DROP ROLE IF EXISTS cache_t2_owner_role;
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
CREATE USER cache_t2_owner IDENTIFIED BY 'password';
CREATE USER cache_mixed IDENTIFIED BY 'password';
CREATE USER cache_gone IDENTIFIED BY 'password';
CREATE ROLE cache_db_owner_role;
CREATE ROLE cache_t2_owner_role;
CREATE ROLE cache_gone_role;
GRANT OWNERSHIP ON access_cache.* TO ROLE cache_db_owner_role;
GRANT OWNERSHIP ON access_cache.t2 TO ROLE cache_t2_owner_role;
GRANT OWNERSHIP ON access_cache.t_gone TO ROLE cache_gone_role;
GRANT ROLE cache_db_owner_role TO cache_db_owner;
GRANT ROLE cache_t2_owner_role TO cache_t2_owner;
GRANT ROLE cache_t2_owner_role TO cache_mixed;
GRANT SELECT ON access_cache.t1 TO cache_mixed;
GRANT ROLE cache_gone_role TO cache_gone;
"

echo '=== database owner, tables bound several times ==='
run_user cache_db_owner 'SELECT count(*) FROM access_cache.t1 a, access_cache.t1 b, access_cache.t2 c'
run_user cache_db_owner 'SELECT a FROM access_cache.t1 UNION ALL SELECT a FROM access_cache.t1 UNION ALL SELECT a FROM access_cache.t2 ORDER BY a'
expect_denied cache_db_owner 'SELECT count(*) FROM access_cache.t1 a, access_cache_other.t3 b'

echo '=== table owner covers only its table ==='
run_user cache_t2_owner 'SELECT count(*) FROM access_cache.t2 a, access_cache.t2 b'
expect_denied cache_t2_owner 'SELECT count(*) FROM access_cache.t2 a, access_cache.t1 b'

echo '=== grant and ownership in one statement ==='
run_user cache_mixed 'SELECT count(*) FROM access_cache.t1 a, access_cache.t2 b, access_cache.t1 c'
expect_denied cache_mixed 'SELECT count(*) FROM access_cache.t1 a, access_cache.t2 b, access_cache_other.t3 c'

echo '=== ownership of a second database ==='
bendsql_connect_root_null --query='GRANT OWNERSHIP ON access_cache_other.* TO ROLE cache_db_owner_role;'
run_user cache_db_owner 'SELECT count(*) FROM access_cache.t1 a, access_cache_other.t3 b'

echo '=== system table and table function next to an owned table ==='
run_user cache_db_owner 'SELECT count(*) FROM access_cache.t1 a, system.one b, numbers(3) c'

echo '=== dropped owner role falls back to account_admin ==='
run_user cache_gone 'SELECT count(*) FROM access_cache.t_gone a, access_cache.t_gone b'
bendsql_connect_root_null --query='DROP ROLE cache_gone_role;'
expect_denied cache_gone 'SELECT count(*) FROM access_cache.t_gone'
bendsql_connect_root --output tsv --query='SELECT count(*) FROM access_cache.t_gone'
