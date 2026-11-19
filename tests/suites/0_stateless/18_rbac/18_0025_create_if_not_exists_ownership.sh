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

# Counts the owner role's OWNERSHIP grants on objects named $1. SHOW GRANTS FOR ROLE lists
# ownership records directly, so a record left behind by DROP still shows up here.
owned_count() {
	bendsql_connect_root --output tsv --query="SHOW GRANTS FOR ROLE if_not_exists_owner_role" | awk -F '\t' -v name="$1" '$1 == "OWNERSHIP" && $2 == name { n++ } END { print n + 0 }'
}

table_id() {
	bendsql_connect_root --output tsv --query="SELECT table_id FROM system.tables WHERE database = 'if_not_exists_db' AND name = '$1'"
}

cleanup() {
	bendsql_connect_root_null --query="
UNSET GLOBAL enable_experimental_connection_privilege_check;
UNSET GLOBAL enable_experimental_sequence_privilege_check;
DROP DATABASE IF EXISTS if_not_exists_db;
DROP CONNECTION IF EXISTS if_not_exists_conn;
DROP STAGE IF EXISTS if_not_exists_stage;
DROP SEQUENCE IF EXISTS if_not_exists_seq;
DROP FUNCTION IF EXISTS if_not_exists_udf;
DROP USER IF EXISTS if_not_exists_owner;
DROP USER IF EXISTS if_not_exists_creator;
DROP ROLE IF EXISTS if_not_exists_owner_role;
DROP ROLE IF EXISTS if_not_exists_creator_role;
"
}

cleanup
trap cleanup EXIT
bendsql_connect_root_null --query="
SET GLOBAL enable_experimental_connection_privilege_check = 1;
SET GLOBAL enable_experimental_sequence_privilege_check = 1;
CREATE DATABASE if_not_exists_db;
CREATE ROLE if_not_exists_owner_role;
CREATE ROLE if_not_exists_creator_role;
CREATE USER if_not_exists_owner IDENTIFIED BY 'password' WITH DEFAULT_ROLE = 'if_not_exists_owner_role';
CREATE USER if_not_exists_creator IDENTIFIED BY 'password' WITH DEFAULT_ROLE = 'if_not_exists_creator_role';
GRANT ROLE if_not_exists_owner_role TO if_not_exists_owner;
GRANT ROLE if_not_exists_creator_role TO if_not_exists_creator;
GRANT CREATE ON if_not_exists_db.* TO ROLE if_not_exists_owner_role;
GRANT CREATE ON if_not_exists_db.* TO ROLE if_not_exists_creator_role;
GRANT CREATE CONNECTION, CREATE SEQUENCE ON *.* TO ROLE if_not_exists_owner_role;
GRANT CREATE CONNECTION, CREATE SEQUENCE ON *.* TO ROLE if_not_exists_creator_role;
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

echo "== CREATE CONNECTION IF NOT EXISTS on an existing connection keeps its owner"
run_user if_not_exists_owner "CREATE CONNECTION if_not_exists_conn STORAGE_TYPE = 's3' ENDPOINT_URL = 'http://127.0.0.1:9900'"
owner_of "CONNECTION if_not_exists_conn"
run_user if_not_exists_creator "CREATE CONNECTION IF NOT EXISTS if_not_exists_conn STORAGE_TYPE = 's3' ENDPOINT_URL = 'http://127.0.0.1:9901'"
owner_of "CONNECTION if_not_exists_conn"
expect_denied if_not_exists_creator "DESC CONNECTION if_not_exists_conn"
expect_denied if_not_exists_creator "DROP CONNECTION if_not_exists_conn"
run_user if_not_exists_owner "DESC CONNECTION if_not_exists_conn" | cut -f1,2
echo "owned before DROP: $(owned_count if_not_exists_conn)"
run_user if_not_exists_owner "DROP CONNECTION if_not_exists_conn"
echo "owned after DROP: $(owned_count if_not_exists_conn)"

# Checked before SUPER is granted: with sequence object checks, SUPER can manage any sequence.
echo "== CREATE SEQUENCE IF NOT EXISTS on an existing sequence keeps its owner"
run_user if_not_exists_owner "CREATE SEQUENCE if_not_exists_seq START = 42" >/dev/null
owner_of "SEQUENCE if_not_exists_seq"
run_user if_not_exists_creator "CREATE SEQUENCE IF NOT EXISTS if_not_exists_seq START = 42" >/dev/null
owner_of "SEQUENCE if_not_exists_seq"
expect_denied if_not_exists_creator "DROP SEQUENCE if_not_exists_seq"
echo "owned before DROP: $(owned_count if_not_exists_seq)"
run_user if_not_exists_owner "DROP SEQUENCE if_not_exists_seq" >/dev/null
echo "owned after DROP: $(owned_count if_not_exists_seq)"

# SUPER authorizes Stage/UDF creation, but does not grant access to another
# role's objects.
bendsql_connect_root_null --query="
GRANT SUPER ON *.* TO ROLE if_not_exists_owner_role;
GRANT SUPER ON *.* TO ROLE if_not_exists_creator_role;
"
for kind in STAGE UDF; do
	case "$kind" in
	STAGE)
		name=if_not_exists_stage
		ddl=STAGE
		definition='FILE_FORMAT = (TYPE = CSV)'
		;;
	UDF)
		name=if_not_exists_udf
		ddl=FUNCTION
		definition='AS (x) -> x + 1'
		;;
	esac

	echo "== CREATE $ddl IF NOT EXISTS on an existing object keeps its owner"
	run_user if_not_exists_owner "CREATE $ddl $name $definition" >/dev/null
	owner_of "$kind $name"
	run_user if_not_exists_creator "CREATE $ddl IF NOT EXISTS $name $definition" >/dev/null
	owner_of "$kind $name"
	expect_denied if_not_exists_creator "DROP $ddl $name"
	echo "owned before DROP: $(owned_count "$name")"
	run_user if_not_exists_owner "DROP $ddl $name" >/dev/null
	echo "owned after DROP: $(owned_count "$name")"
done
