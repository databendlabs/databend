#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

root_sql() {
	bendsql_connect_root_null --query="$1"
}

user_sql() {
	local user="$1"
	bendsql_connect_user "$user" password --output null --query="$2"
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

cleanup() {
	root_sql "
DROP USER IF EXISTS attach_priv_creator;
DROP USER IF EXISTS attach_priv_operator;
DROP USER IF EXISTS attach_priv_owner;
DROP DATABASE IF EXISTS attach_priv_db;
DROP ROLE IF EXISTS attach_priv_creator;
DROP ROLE IF EXISTS attach_priv_operator;
DROP ROLE IF EXISTS attach_priv_owner;
DROP ROLE IF EXISTS attach_priv_all;
"
}

cleanup
trap cleanup EXIT
root_sql "
CREATE DATABASE attach_priv_db;
CREATE ROLE attach_priv_creator;
CREATE ROLE attach_priv_operator;
CREATE ROLE attach_priv_owner;
CREATE ROLE attach_priv_all;
CREATE USER attach_priv_creator IDENTIFIED BY 'password' WITH DEFAULT_ROLE = 'attach_priv_creator';
CREATE USER attach_priv_operator IDENTIFIED BY 'password' WITH DEFAULT_ROLE = 'attach_priv_operator';
CREATE USER attach_priv_owner IDENTIFIED BY 'password' WITH DEFAULT_ROLE = 'attach_priv_owner';
GRANT ROLE attach_priv_creator TO USER attach_priv_creator;
GRANT ROLE attach_priv_operator TO USER attach_priv_operator;
GRANT ROLE attach_priv_owner TO USER attach_priv_owner;
GRANT CREATE ON attach_priv_db.* TO ROLE attach_priv_creator;
GRANT SUPER ON *.* TO ROLE attach_priv_creator;
GRANT ATTACH TABLE ON attach_priv_db.* TO ROLE attach_priv_operator;
GRANT OWNERSHIP ON attach_priv_db.* TO ROLE attach_priv_owner;
"

echo "CREATE-only role can create ordinary tables"
user_sql attach_priv_creator "CREATE TABLE attach_priv_db.ordinary (a INT)"

# Use a valid local source so binding can finish before the execution access check.
attach_sql="ATTACH TABLE attach_priv_db.attached 'fs:///tmp/1/2'"
echo "CREATE and SUPER do not imply ATTACH TABLE"
expect_error attach_priv_creator '\[1063\].*\[AttachTable\]' "$attach_sql"
echo "Database ownership does not imply ATTACH TABLE"
expect_error attach_priv_owner '\[1063\].*\[AttachTable\]' "$attach_sql"
echo "ATTACH TABLE does not replace CREATE"
expect_error attach_priv_operator '\[1063\].*\[Create\]' "$attach_sql"
echo "CREATE with STORAGE_PREFIX cannot bypass ATTACH TABLE"
expect_error attach_priv_creator '\[1063\].*\[AttachTable\]' "CREATE TABLE attach_priv_db.bypass (a INT) STORAGE_PREFIX = '1/2'"

echo "ATTACH TABLE cannot be granted on a table"
if output=$(bendsql_connect_root --query="GRANT ATTACH TABLE ON attach_priv_db.ordinary TO ROLE attach_priv_operator" 2>&1); then
	echo "Expected table-level GRANT to fail" >&2
	exit 1
fi
grep -q 'Illegal GRANT/REVOKE' <<<"$output"

echo "Administrator can grant and revoke ATTACH TABLE directly to USER"
root_sql "GRANT ATTACH TABLE ON attach_priv_db.* TO USER attach_priv_creator"
output=$(bendsql_connect_root --query="SHOW GRANTS FOR USER attach_priv_creator")
grep -q 'ATTACH TABLE' <<<"$output"
root_sql "REVOKE ATTACH TABLE ON attach_priv_db.* FROM USER attach_priv_creator"
expect_error attach_priv_creator '\[1063\].*\[AttachTable\]' "$attach_sql"

echo "Database ALL includes ATTACH TABLE"
# Remove SELECT so SHOW GRANTS expands names instead of displaying the ALL shorthand.
root_sql "GRANT ALL ON attach_priv_db.* TO ROLE attach_priv_all; REVOKE SELECT ON attach_priv_db.* FROM ROLE attach_priv_all"
output=$(bendsql_connect_root --query="SHOW GRANTS FOR ROLE attach_priv_all")
grep -q 'ATTACH TABLE' <<<"$output"
echo "Global ALL includes ATTACH TABLE"
root_sql "REVOKE ALL ON attach_priv_db.* FROM ROLE attach_priv_all; GRANT ALL ON *.* TO ROLE attach_priv_all; REVOKE SELECT ON *.* FROM ROLE attach_priv_all"
output=$(bendsql_connect_root --query="SHOW GRANTS FOR ROLE attach_priv_all")
grep -q 'ATTACH TABLE' <<<"$output"
