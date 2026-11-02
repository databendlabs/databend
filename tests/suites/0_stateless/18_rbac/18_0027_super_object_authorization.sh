#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cleanup() {
	bendsql_connect_root_null --query="
DROP STAGE IF EXISTS super_forbidden_stage;
DROP CONNECTION IF EXISTS super_private_conn;
DROP CONNECTION IF EXISTS super_created_conn;
DROP SEQUENCE IF EXISTS super_private_seq;
DROP SEQUENCE IF EXISTS super_created_seq;
DROP USER IF EXISTS super_direct_user;
DROP USER IF EXISTS super_role_user;
DROP USER IF EXISTS super_low_user;
DROP USER IF EXISTS super_public_creator;
DROP ROLE IF EXISTS super_object_role;
DROP ROLE IF EXISTS super_creator_role;
DROP ROLE IF EXISTS super_access_role;
"
}

request() {
	local payload
	payload=$(jq -nc --arg sql "$2" '{sql: $sql, pagination: {wait_time_secs: 30}}')
	curl --fail --silent --show-error -u "$1:super_object_test" \
		"http://$QUERY_MYSQL_HANDLER_HOST:$QUERY_HTTP_HANDLER_PORT/v1/query" \
		-H 'Content-Type: application/json' --data-binary "$payload"
}

expect_denied() {
	local output
	output=$(request "$1" "$2")
	jq -e --arg privilege "$3" '
		(.error.code == 1063 or (.error.code == 1006 and (.error.message | contains("PermissionDenied. Code: 1063")))) and
		(.error.message | contains($privilege))
	' <<<"$output" >/dev/null || {
		printf '%s\n' "$output"
		return 1
	}
	echo denied
}

expect_allowed() {
	local output
	output=$(request "$1" "$2")
	jq -e '.state == "Succeeded" and .error == null' <<<"$output" >/dev/null || {
		printf '%s\n' "$output"
		return 1
	}
	echo allowed
}

expect_visible() {
	local output
	output=$(request "$1" "$2")
	jq -e --arg name "$3" --argjson count "$4" '
		.state == "Succeeded" and .error == null and
		([.data[] | select(.[0] == $name)] | length) == $count
	' <<<"$output" >/dev/null || {
		printf '%s\n' "$output"
		return 1
	}
	echo "visible=$4"
}

cleanup
trap cleanup EXIT
bendsql_connect_root_null --query="
CREATE CONNECTION super_private_conn STORAGE_TYPE = 's3' ENDPOINT_URL = 'http://127.0.0.1:9900';
CREATE SEQUENCE super_private_seq;
GRANT OWNERSHIP ON CONNECTION super_private_conn TO ROLE account_admin;
GRANT OWNERSHIP ON SEQUENCE super_private_seq TO ROLE account_admin;
CREATE USER super_direct_user IDENTIFIED BY 'super_object_test';
GRANT SUPER ON *.* TO super_direct_user;
CREATE ROLE super_object_role;
GRANT SUPER ON *.* TO ROLE super_object_role;
CREATE USER super_role_user IDENTIFIED BY 'super_object_test' WITH DEFAULT_ROLE = 'super_object_role';
GRANT ROLE super_object_role TO super_role_user;
CREATE USER super_low_user IDENTIFIED BY 'super_object_test';
CREATE USER super_public_creator IDENTIFIED BY 'super_object_test';
"

# SUPER alone does not grant creation or access privileges, including for
# IF NOT EXISTS and OR REPLACE. The fixtures remain owned by account_admin.
for user in super_direct_user super_role_user; do
	echo "=== SUPER alone: $user ==="
	expect_denied "$user" "CREATE CONNECTION super_created_conn STORAGE_TYPE = 's3'" CreateConnection
	expect_denied "$user" 'CREATE SEQUENCE super_created_seq' CreateSequence
	expect_denied "$user" "CREATE CONNECTION IF NOT EXISTS super_private_conn STORAGE_TYPE = 's3'" CreateConnection
	expect_denied "$user" 'CREATE SEQUENCE IF NOT EXISTS super_private_seq' CreateSequence
	expect_denied "$user" "CREATE OR REPLACE CONNECTION super_private_conn STORAGE_TYPE = 's3'" CreateConnection
	expect_denied "$user" 'CREATE OR REPLACE SEQUENCE super_private_seq' CreateSequence
	expect_denied "$user" 'DESC CONNECTION super_private_conn' AccessConnection
	expect_denied "$user" 'DROP CONNECTION super_private_conn' AccessConnection
	expect_denied "$user" 'DESC SEQUENCE super_private_seq' AccessSequence
	expect_denied "$user" 'DROP SEQUENCE super_private_seq' AccessSequence
	expect_denied "$user" 'SELECT nextval(super_private_seq)' 'ACCESS SEQUENCE'
	expect_denied "$user" "CREATE STAGE super_forbidden_stage URL = 's3://bucket' CONNECTION = (CONNECTION_NAME = 'super_private_conn')" AccessConnection
	expect_visible "$user" 'SHOW CONNECTIONS' super_private_conn 0
	expect_visible "$user" 'SHOW SEQUENCES' super_private_seq 0
done

echo '=== Effective CREATE privileges work with public as the current role ==='
bendsql_connect_root_null --query="
CREATE ROLE super_creator_role;
GRANT CREATE CONNECTION, CREATE SEQUENCE ON *.* TO ROLE super_creator_role;
GRANT ROLE super_creator_role TO super_direct_user;
GRANT ROLE super_creator_role TO super_public_creator;
"
expect_allowed super_direct_user "CREATE CONNECTION super_created_conn STORAGE_TYPE = 's3'"
expect_allowed super_public_creator 'CREATE SEQUENCE super_created_seq'
for object in 'CONNECTION super_created_conn' 'SEQUENCE super_created_seq'; do
	bendsql_connect_root --output tsv --query="SHOW GRANTS ON $object" |
		awk -F '\t' '$1 == "OWNERSHIP" { print $1 "\t" $5 }'
done
# Public ownership is shared with other users; this is not an authorization
# error. SET ROLE or DEFAULT_ROLE selects a more restricted owner instead.
expect_allowed super_role_user 'DROP CONNECTION super_created_conn'
expect_allowed super_role_user 'DROP SEQUENCE super_created_seq'

echo '=== SET ROLE selects the owner, not whether creation is allowed ==='
printf '%s\n' "SET ROLE super_creator_role; CREATE CONNECTION super_created_conn STORAGE_TYPE = 's3'; CREATE SEQUENCE super_created_seq;" |
	bendsql_connect_user super_direct_user super_object_test -A >/dev/null
for object in 'CONNECTION super_created_conn' 'SEQUENCE super_created_seq'; do
	bendsql_connect_root --output tsv --query="SHOW GRANTS ON $object" |
		awk -F '\t' '$1 == "OWNERSHIP" { print $1 "\t" $5 }'
done
expect_allowed super_direct_user 'DESC CONNECTION super_created_conn'
expect_allowed super_direct_user 'SELECT nextval(super_created_seq)'
expect_denied super_low_user 'DROP CONNECTION super_created_conn' AccessConnection
expect_denied super_low_user 'DROP SEQUENCE super_created_seq' AccessSequence

echo '=== Explicit ACCESS grants work for SUPER users ==='
bendsql_connect_root_null --query="
CREATE ROLE super_access_role;
GRANT ACCESS CONNECTION ON CONNECTION super_private_conn TO ROLE super_access_role;
GRANT ACCESS SEQUENCE ON SEQUENCE super_private_seq TO ROLE super_access_role;
GRANT ROLE super_access_role TO super_direct_user;
GRANT ROLE super_access_role TO super_role_user;
"
for user in super_direct_user super_role_user; do
	expect_allowed "$user" 'DESC CONNECTION super_private_conn'
	expect_allowed "$user" 'DESC SEQUENCE super_private_seq'
	expect_allowed "$user" 'SELECT nextval(super_private_seq)'
	expect_allowed "$user" "CREATE OR REPLACE STAGE super_forbidden_stage URL = 's3://bucket' CONNECTION = (CONNECTION_NAME = 'super_private_conn')"
	expect_visible "$user" 'SHOW CONNECTIONS' super_private_conn 1
	expect_visible "$user" 'SHOW SEQUENCES' super_private_seq 1
done
expect_allowed super_direct_user 'DROP CONNECTION super_private_conn'
expect_allowed super_role_user 'DROP SEQUENCE super_private_seq'
