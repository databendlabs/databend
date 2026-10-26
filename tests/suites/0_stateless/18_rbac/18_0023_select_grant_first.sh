#!/usr/bin/env bash

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
	bendsql_connect_root_null --query "
DROP DATABASE IF EXISTS select_grant_first;
DROP USER IF EXISTS select_grant_user;
DROP USER IF EXISTS select_role_user;
DROP USER IF EXISTS select_table_owner;
DROP USER IF EXISTS select_db_owner;
DROP USER IF EXISTS select_denied;
DROP ROLE IF EXISTS select_reader;
DROP ROLE IF EXISTS select_parent;
DROP ROLE IF EXISTS select_limited;
DROP ROLE IF EXISTS select_table_owner_role;
DROP ROLE IF EXISTS select_db_owner_role;
"
}

cleanup
trap cleanup EXIT
bendsql_connect_root_null --query "
CREATE DATABASE select_grant_first;
CREATE TABLE select_grant_first.t(a INT);
INSERT INTO select_grant_first.t VALUES (7);
CREATE TABLE select_grant_first.numbers(a INT);
INSERT INTO select_grant_first.numbers VALUES (9);
CREATE VIEW select_grant_first.v AS SELECT * FROM select_grant_first.t;
CREATE USER select_grant_user IDENTIFIED BY 'password';
CREATE USER select_role_user IDENTIFIED BY 'password';
CREATE USER select_table_owner IDENTIFIED BY 'password';
CREATE USER select_db_owner IDENTIFIED BY 'password';
CREATE USER select_denied IDENTIFIED BY 'password';
CREATE ROLE select_reader;
CREATE ROLE select_parent;
CREATE ROLE select_limited;
CREATE ROLE select_table_owner_role;
CREATE ROLE select_db_owner_role;
GRANT SELECT ON select_grant_first.t TO select_grant_user;
GRANT SELECT ON select_grant_first.t TO ROLE select_reader;
GRANT ROLE select_reader TO ROLE select_parent;
GRANT ROLE select_parent TO select_role_user;
GRANT ROLE select_limited TO select_role_user;
GRANT OWNERSHIP ON select_grant_first.t TO ROLE select_table_owner_role;
GRANT ROLE select_table_owner_role TO select_table_owner;
GRANT OWNERSHIP ON select_grant_first.* TO ROLE select_db_owner_role;
GRANT ROLE select_db_owner_role TO select_db_owner;
"

echo '=== explicit table grant and self join ==='
run_user select_grant_user 'SELECT * FROM select_grant_first.t'
run_user select_grant_user 'SELECT l.a FROM select_grant_first.t l JOIN select_grant_first.t r ON l.a = r.a'

echo '=== inherited grant and role selection ==='
run_user select_role_user 'SET ROLE select_parent; SET SECONDARY ROLES NONE; SELECT * FROM select_grant_first.t'
expect_denied select_role_user 'SET ROLE select_limited; SET SECONDARY ROLES NONE; SELECT * FROM select_grant_first.t'
run_user select_role_user 'SET ROLE select_limited; SET SECONDARY ROLES ALL; SELECT * FROM select_grant_first.t'

echo '=== ownership fallback ==='
run_user select_table_owner 'SELECT * FROM select_grant_first.t'
run_user select_db_owner 'SELECT * FROM select_grant_first.t'
expect_denied select_denied 'SELECT * FROM select_grant_first.t'

echo '=== grant on one table does not authorize another ==='
expect_denied select_grant_user 'SELECT t.a FROM select_grant_first.t t JOIN select_grant_first.numbers n ON t.a = n.a'

echo '=== revoke and database grant ==='
bendsql_connect_root_null --query 'REVOKE SELECT ON select_grant_first.t FROM select_grant_user;'
expect_denied select_grant_user 'SELECT * FROM select_grant_first.t'
bendsql_connect_root_null --query 'GRANT SELECT ON select_grant_first.* TO select_grant_user;'
run_user select_grant_user 'SELECT * FROM select_grant_first.t'
run_user select_grant_user 'SELECT * FROM select_grant_first.numbers'
bendsql_connect_root_null --query 'REVOKE SELECT ON select_grant_first.* FROM select_grant_user;'
expect_denied select_grant_user 'SELECT * FROM select_grant_first.numbers'

echo '=== table function name collision ==='
expect_denied select_denied 'SELECT * FROM select_grant_first.numbers'
run_user select_denied 'SELECT count(*) FROM numbers(3)'

echo '=== view grant does not expose source table ==='
bendsql_connect_root_null --query 'GRANT SELECT ON select_grant_first.v TO select_grant_user;'
run_user select_grant_user 'SELECT * FROM select_grant_first.v'
expect_denied select_grant_user 'SELECT * FROM select_grant_first.t'

echo '=== ID grant survives rename but not name reuse ==='
bendsql_connect_root_null --query '
ALTER TABLE select_grant_first.t RENAME TO renamed;
CREATE TABLE select_grant_first.t(a INT);
INSERT INTO select_grant_first.t VALUES (13);
'
run_user select_role_user 'SET ROLE select_parent; SET SECONDARY ROLES NONE; SELECT * FROM select_grant_first.renamed'
expect_denied select_role_user 'SET ROLE select_parent; SET SECONDARY ROLES NONE; SELECT * FROM select_grant_first.t'

echo '=== temporary and system tables ==='
run_user select_denied 'CREATE TEMP TABLE select_grant_first.tmp(a INT); INSERT INTO select_grant_first.tmp VALUES (11); SELECT * FROM select_grant_first.tmp'
run_user select_denied 'SELECT dummy FROM system.one'
