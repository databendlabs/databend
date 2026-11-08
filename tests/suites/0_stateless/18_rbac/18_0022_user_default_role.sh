#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

root_sql() {
	bendsql_connect_root_null --query="$1"
}

user_sql() {
	bendsql_connect_user "$1" password --output null --query="$2"
}

expect_error() {
	local user="$1" pattern="$2" sql="$3" output
	if output=$(user_sql "$user" "$sql" 2>&1); then
		echo "Expected failure: $sql" >&2
		exit 1
	fi
	if ! grep -Eq "$pattern" <<<"$output"; then
		printf 'Unexpected error for %s:\n%s\n' "$sql" "$output" >&2
		exit 1
	fi
}

assert_root() {
	local output
	output=$(bendsql_connect_root --output tsv --query="$1")
	if [[ "$output" != "$2" ]]; then
		printf 'Expected %s, got %s for %s\n' "$2" "$output" "$1" >&2
		exit 1
	fi
}

cleanup() {
	root_sql "
DROP USER IF EXISTS default_role_reader;
DROP USER IF EXISTS default_role_manager;
DROP USER IF EXISTS default_role_created;
DROP USER IF EXISTS default_role_rejected;
DROP USER IF EXISTS default_role_public;
DROP USER IF EXISTS default_role_inherited;
DROP DATABASE IF EXISTS default_role_db;
DROP ROLE IF EXISTS default_role_grandparent;
DROP ROLE IF EXISTS default_role_parent;
DROP ROLE IF EXISTS default_role_viewer;
DROP ROLE IF EXISTS default_role_other;
DROP ROLE IF EXISTS default_role_missing;
"
}

cleanup
trap cleanup EXIT
root_sql "
CREATE DATABASE default_role_db;
CREATE TABLE default_role_db.t (a INT);
INSERT INTO default_role_db.t VALUES (1);
CREATE ROLE default_role_viewer;
CREATE ROLE default_role_other;
CREATE ROLE default_role_parent;
CREATE ROLE default_role_grandparent;
GRANT ROLE default_role_viewer TO ROLE default_role_parent;
GRANT ROLE default_role_parent TO ROLE default_role_grandparent;
CREATE USER default_role_inherited IDENTIFIED BY 'password';
GRANT ROLE default_role_grandparent TO USER default_role_inherited;
GRANT SELECT ON default_role_db.* TO ROLE default_role_viewer;
CREATE USER default_role_reader IDENTIFIED BY 'password' WITH DEFAULT_ROLE = default_role_viewer;
CREATE USER default_role_manager IDENTIFIED BY 'password';
GRANT CREATE USER ON *.* TO USER default_role_manager;
GRANT ALTER ON *.* TO USER default_role_manager;
"

echo "DEFAULT_ROLE alone enables SELECT and selects the role on login"
user_sql default_role_reader "SELECT * FROM default_role_db.t"
output=$(bendsql_connect_user default_role_reader password --output tsv --query="SELECT current_role()")
[[ "$output" == "default_role_viewer" ]]

echo "CREATE USER without GRANT cannot implicitly grant account_admin"
user_sql default_role_manager "CREATE USER default_role_created IDENTIFIED BY 'password'"
expect_error default_role_manager '\[1063\].*\[Grant\]' "CREATE OR REPLACE USER default_role_created IDENTIFIED BY 'password' WITH DEFAULT_ROLE = account_admin"
expect_error default_role_manager '\[1063\].*\[Grant\]' "CREATE USER default_role_rejected IDENTIFIED BY 'password' WITH DEFAULT_ROLE = account_admin"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_rejected'" "0"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_created' AND roles = ''" "1"

echo "CREATE with PUBLIC needs no GRANT and writes no explicit grant"
expect_error default_role_reader '\[1063\].*\[CreateUser\]' "CREATE USER default_role_rejected IDENTIFIED BY 'password' WITH DEFAULT_ROLE = public"
user_sql default_role_manager "CREATE USER default_role_public IDENTIFIED BY 'password' WITH DEFAULT_ROLE = public"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_public' AND default_role = 'public' AND roles = ''" "1"
output=$(bendsql_connect_user default_role_public password --output tsv --query="SELECT current_role()")
[[ "$output" == "public" ]]
user_sql default_role_manager "CREATE OR REPLACE USER default_role_public IDENTIFIED BY 'password' WITH DEFAULT_ROLE = public"
user_sql default_role_manager "CREATE USER IF NOT EXISTS default_role_public IDENTIFIED BY 'password' WITH DEFAULT_ROLE = public"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_public' AND default_role = 'public' AND roles = ''" "1"

echo "ALTER accepts inherited defaults without adding a direct grant"
user_sql default_role_manager "ALTER USER default_role_inherited WITH DEFAULT_ROLE = default_role_viewer"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_inherited' AND default_role = 'default_role_viewer' AND roles = 'default_role_grandparent'" "1"
output=$(bendsql_connect_user default_role_inherited password --output tsv --query="SELECT current_role()")
[[ "$output" == "default_role_viewer" ]]
user_sql default_role_inherited "SET ROLE public; SET ROLE default_role_viewer; SELECT * FROM default_role_db.t"

echo "ALTER validates the target user's closure rather than the executor's"
root_sql "GRANT ROLE default_role_parent TO USER default_role_manager"
expect_error default_role_manager '\[2206\]' "ALTER USER default_role_created WITH DEFAULT_ROLE = default_role_viewer"
root_sql "REVOKE ROLE default_role_parent FROM USER default_role_manager"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_created' AND default_role = '' AND roles = ''" "1"

echo "Revoked inheritance cannot be restored by repeating DEFAULT_ROLE"
root_sql "REVOKE ROLE default_role_viewer FROM ROLE default_role_parent"
expect_error default_role_manager '\[2206\]' "ALTER USER default_role_inherited WITH DEFAULT_ROLE = default_role_viewer"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_inherited' AND default_role = 'default_role_viewer' AND roles = 'default_role_grandparent'" "1"
output=$(bendsql_connect_user default_role_inherited password --output tsv --query="SELECT current_role()")
[[ "$output" == "public" ]]
expect_error default_role_inherited '\[1063\]' "SELECT * FROM default_role_db.t"
root_sql "GRANT ROLE default_role_viewer TO ROLE default_role_parent"
user_sql default_role_manager "ALTER USER default_role_inherited WITH DEFAULT_ROLE = default_role_viewer"
user_sql default_role_inherited "SELECT * FROM default_role_db.t"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_inherited' AND roles = 'default_role_grandparent'" "1"

echo "ALTER cannot select an ungranted role"
expect_error default_role_manager '\[2206\]' "ALTER USER default_role_created WITH DEFAULT_ROLE = account_admin"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_created' AND default_role = '' AND roles = ''" "1"

# A default-role assignment must never use the self-password-change shortcut,
# even if the stored option already has exactly the requested value.
echo "An unchanged default still requires ALTER and membership"
root_sql "REVOKE ROLE default_role_viewer FROM USER default_role_reader"
expect_error default_role_reader '\[1063\].*\[Alter\]' "ALTER USER default_role_reader WITH DEFAULT_ROLE = default_role_viewer"
expect_error default_role_manager '\[2206\]' "ALTER USER default_role_reader WITH DEFAULT_ROLE = default_role_viewer"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_reader' AND roles = ''" "1"

echo "Switching to an already granted role does not require GRANT"
root_sql "GRANT ROLE default_role_other TO USER default_role_created"
user_sql default_role_manager "ALTER USER default_role_created WITH DEFAULT_ROLE = default_role_other"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_created' AND default_role = roles" "1"

echo "Explicit regrant is required before selecting the revoked default"
root_sql "GRANT ROLE default_role_viewer TO USER default_role_reader"
root_sql "ALTER USER default_role_reader WITH DEFAULT_ROLE = default_role_viewer"
user_sql default_role_reader "SELECT * FROM default_role_db.t"

echo "Missing roles fail without changing the user"
root_sql "GRANT GRANT ON *.* TO USER default_role_manager"
expect_error default_role_manager '\[2204\]' "ALTER USER default_role_reader IDENTIFIED BY 'changed-password' WITH DEFAULT_ROLE = default_role_missing"
user_sql default_role_reader "SELECT * FROM default_role_db.t"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_reader' AND default_role = roles" "1"

echo "Even ALTER plus GRANT cannot implicitly grant a default role"
expect_error default_role_manager '\[2206\]' "ALTER USER default_role_reader IDENTIFIED BY 'changed-password' WITH DEFAULT_ROLE = default_role_other"
user_sql default_role_reader "SELECT * FROM default_role_db.t"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_reader' AND default_role = 'default_role_viewer' AND roles = 'default_role_viewer'" "1"
expect_error default_role_manager '\[2206\]' "ALTER USER default_role_manager WITH DEFAULT_ROLE = account_admin"

echo "Switching defaults preserves old grants without granting the executor"
root_sql "GRANT ROLE default_role_other TO USER default_role_reader"
user_sql default_role_manager "ALTER USER default_role_reader WITH DEFAULT_ROLE = default_role_other"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_reader' AND default_role = 'default_role_other' AND roles = 'default_role_other, default_role_viewer'" "1"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_manager' AND roles = ''" "1"
user_sql default_role_manager "ALTER USER default_role_reader WITH DEFAULT_ROLE = default_role_viewer"
root_sql "REVOKE ROLE default_role_other FROM USER default_role_reader"

echo "CREATE USER plus GRANT can assign a default role"
user_sql default_role_manager "CREATE OR REPLACE USER default_role_created IDENTIFIED BY 'password' WITH DEFAULT_ROLE = default_role_viewer"
user_sql default_role_created "SELECT * FROM default_role_db.t"

# Revoking a default role remains effective after an ordinary ALTER, including
# the self-password-change path, which must not restore the role implicitly.
echo "An ordinary password update does not restore a revoked default role"
root_sql "REVOKE ROLE default_role_viewer FROM USER default_role_reader"
user_sql default_role_reader "ALTER USER default_role_reader IDENTIFIED WITH sha256_password BY 'password'"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_reader' AND roles = ''" "1"
expect_error default_role_reader '\[1063\]' "SELECT * FROM default_role_db.t"

echo "Selecting PUBLIC requires no explicit grant and changes no grants"
user_sql default_role_manager "ALTER USER default_role_reader WITH DEFAULT_ROLE = public"
assert_root "SELECT count(*) FROM system.users WHERE name = 'default_role_reader' AND default_role = 'public' AND roles = ''" "1"
