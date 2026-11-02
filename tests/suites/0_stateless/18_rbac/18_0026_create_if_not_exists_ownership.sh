#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

run_user() {
	printf '%s\n' "$2" | bendsql_connect_user "$1" password -A --output tsv --quote-style=never
}

expect_denied() {
	local output
	if output=$(run_user "$1" "$2" 2>&1); then
		echo "expected permission denial: $2"
		return 1
	fi
	if ! grep -q 'Permission denied' <<<"$output"; then
		printf '%s\n' "$output"
		return 1
	fi
	echo denied
}

# Prints the ownership grant of the object, e.g. `OWNERSHIP	if_not_exists_owner_role`.
owner_of() {
	bendsql_connect_root --output tsv --query="SHOW GRANTS ON $1" | awk -F '\t' '$1 == "OWNERSHIP" { print $1 "\t" $5 }'
}

table_id() {
	bendsql_connect_root --output tsv --query="SELECT table_id FROM system.tables WHERE database = 'if_not_exists_db' AND name = '$1'"
}

cleanup() {
	bendsql_connect_root_null --query="
DROP DATABASE IF EXISTS if_not_exists_db;
DROP CONNECTION IF EXISTS if_not_exists_conn;
DROP CONNECTION IF EXISTS if_not_exists_new_conn;
DROP USER IF EXISTS if_not_exists_owner;
DROP USER IF EXISTS if_not_exists_creator;
DROP ROLE IF EXISTS if_not_exists_owner_role;
DROP ROLE IF EXISTS if_not_exists_creator_role;
"
}

cleanup
trap cleanup EXIT
bendsql_connect_root_null --query="
CREATE DATABASE if_not_exists_db;
CREATE ROLE if_not_exists_owner_role;
CREATE ROLE if_not_exists_creator_role;
CREATE USER if_not_exists_owner IDENTIFIED BY 'password' WITH DEFAULT_ROLE = 'if_not_exists_owner_role';
CREATE USER if_not_exists_creator IDENTIFIED BY 'password' WITH DEFAULT_ROLE = 'if_not_exists_creator_role';
GRANT ROLE if_not_exists_owner_role TO if_not_exists_owner;
GRANT ROLE if_not_exists_creator_role TO if_not_exists_creator;
GRANT CREATE ON if_not_exists_db.* TO ROLE if_not_exists_owner_role;
GRANT CREATE ON if_not_exists_db.* TO ROLE if_not_exists_creator_role;
GRANT CREATE CONNECTION ON *.* TO ROLE if_not_exists_owner_role;
GRANT CREATE CONNECTION ON *.* TO ROLE if_not_exists_creator_role;
"

echo "== CREATE TABLE IF NOT EXISTS on an existing table keeps its owner"
run_user if_not_exists_owner "CREATE TABLE if_not_exists_db.t1(a INT); INSERT INTO if_not_exists_db.t1 VALUES (42);" >/dev/null
before=$(table_id t1)
owner_of "TABLE if_not_exists_db.t1"
run_user if_not_exists_creator "CREATE TABLE IF NOT EXISTS if_not_exists_db.t1(a INT)"
owner_of "TABLE if_not_exists_db.t1"
[[ "$(table_id t1)" == "$before" ]] && echo "same table"
expect_denied if_not_exists_creator "SELECT * FROM if_not_exists_db.t1"
run_user if_not_exists_owner "SELECT * FROM if_not_exists_db.t1"

echo "== CREATE TABLE IF NOT EXISTS on a new table grants ownership"
run_user if_not_exists_creator "CREATE TABLE IF NOT EXISTS if_not_exists_db.t2(a INT)"
owner_of "TABLE if_not_exists_db.t2"

echo "== CREATE OR REPLACE TABLE creates a table owned by the creator"
run_user if_not_exists_creator "CREATE OR REPLACE TABLE if_not_exists_db.t1(a INT)"
owner_of "TABLE if_not_exists_db.t1"
[[ "$(table_id t1)" != "$before" ]] && echo "new table"

echo "== CREATE CONNECTION IF NOT EXISTS on an existing connection keeps its owner"
run_user if_not_exists_owner "CREATE CONNECTION if_not_exists_conn STORAGE_TYPE = 's3' ENDPOINT_URL = 'http://127.0.0.1:9900'"
owner_of "CONNECTION if_not_exists_conn"
run_user if_not_exists_creator "CREATE CONNECTION IF NOT EXISTS if_not_exists_conn STORAGE_TYPE = 's3' ENDPOINT_URL = 'http://127.0.0.1:9901'"
owner_of "CONNECTION if_not_exists_conn"
expect_denied if_not_exists_creator "DESC CONNECTION if_not_exists_conn"
expect_denied if_not_exists_creator "DROP CONNECTION if_not_exists_conn"
run_user if_not_exists_owner "DESC CONNECTION if_not_exists_conn" | cut -f1,2

echo "== CREATE CONNECTION IF NOT EXISTS on a new connection grants ownership"
run_user if_not_exists_creator "CREATE CONNECTION IF NOT EXISTS if_not_exists_new_conn STORAGE_TYPE = 's3' ENDPOINT_URL = 'http://127.0.0.1:9900'"
owner_of "CONNECTION if_not_exists_new_conn"
