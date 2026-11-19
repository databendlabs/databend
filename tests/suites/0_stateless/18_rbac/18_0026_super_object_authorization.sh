#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

sequence_check=enable_experimental_sequence_privilege_check

cleanup() {
	bendsql_connect_root_null --query="
UNSET GLOBAL $sequence_check;
DROP STAGE IF EXISTS super_forbidden_stage;
DROP TABLE IF EXISTS super_seq_defaults;
DROP CONNECTION IF EXISTS super_legacy_conn;
DROP CONNECTION IF EXISTS super_private_conn;
DROP CONNECTION IF EXISTS super_created_conn;
DROP SEQUENCE IF EXISTS super_private_seq;
DROP SEQUENCE IF EXISTS super_created_seq;
DROP USER IF EXISTS super_direct_user;
DROP USER IF EXISTS super_public_creator;
DROP ROLE IF EXISTS super_creator_role;
DROP ROLE IF EXISTS super_switch_role;
"
}

request() {
	local payload
	payload=$(jq -nc --arg sql "$2" --argjson session "${3:-null}" \
		'{sql: $sql, session: $session, pagination: {wait_time_secs: 30}}')
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
	output=$(request "$@")
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
CREATE USER super_public_creator IDENTIFIED BY 'super_object_test';
"

echo '=== SUPER can manage connections, but cannot use named credentials ==='
expect_allowed super_direct_user "CREATE CONNECTION super_legacy_conn STORAGE_TYPE = 's3'"
expect_denied super_public_creator 'DESC CONNECTION super_legacy_conn' AccessConnection
expect_allowed super_direct_user 'DROP CONNECTION super_legacy_conn'
expect_allowed super_direct_user 'DESC CONNECTION super_private_conn'
expect_visible super_direct_user 'SHOW CONNECTIONS' super_private_conn 1
expect_denied super_direct_user "CREATE STAGE super_forbidden_stage URL = 's3://bucket' CONNECTION = (CONNECTION_NAME = 'super_private_conn')" AccessConnection

echo '=== SUPER alone cannot create or list private sequences ==='
expect_denied super_direct_user 'CREATE SEQUENCE super_created_seq' CreateSequence
expect_visible super_direct_user 'SHOW SEQUENCES' super_private_seq 0

echo '=== CREATE privileges must be granted to the current role ==='
bendsql_connect_root_null --query="
CREATE ROLE super_creator_role;
GRANT CREATE CONNECTION, CREATE SEQUENCE ON *.* TO ROLE super_creator_role;
GRANT ROLE super_creator_role TO super_direct_user;
GRANT ROLE super_creator_role TO super_public_creator;
"
# A secondary role must not create objects owned by public, which every user
# shares. The explicit CREATE privileges are checked against the current role.
expect_denied super_public_creator "CREATE CONNECTION super_created_conn STORAGE_TYPE = 's3'" CreateConnection
expect_denied super_public_creator 'CREATE SEQUENCE super_created_seq' CreateSequence

echo '=== SET ROLE grants creation and selects the owner ==='
printf '%s\n' "SET ROLE super_creator_role; CREATE CONNECTION super_created_conn STORAGE_TYPE = 's3'; CREATE SEQUENCE super_created_seq;" |
	bendsql_connect_user super_direct_user super_object_test -A >/dev/null
for object in 'CONNECTION super_created_conn' 'SEQUENCE super_created_seq'; do
	bendsql_connect_root --output tsv --query="SHOW GRANTS ON $object" |
		awk -F '\t' '$1 == "OWNERSHIP" { print $1 "\t" $5 }'
done
expect_allowed super_direct_user 'DESC CONNECTION super_created_conn'
expect_allowed super_direct_user 'SELECT nextval(super_created_seq)'

echo '=== By default, compatibility covers sequence use, not management ==='
bendsql_connect_root_null --query="
CREATE TABLE super_seq_defaults (id BIGINT DEFAULT nextval(super_private_seq), payload INT);
GRANT INSERT ON default.super_seq_defaults TO super_public_creator;
"
expect_denied super_public_creator "SET GLOBAL $sequence_check = 1" Super
expect_allowed super_public_creator 'INSERT INTO super_seq_defaults (payload) VALUES (1)'
expect_allowed super_public_creator 'INSERT INTO super_seq_defaults VALUES (nextval(super_private_seq), 2)'
expect_allowed super_public_creator 'SELECT nextval(super_private_seq) FROM numbers(2)'
expect_denied super_public_creator 'DESC SEQUENCE super_private_seq' AccessSequence
expect_visible super_public_creator 'SHOW SEQUENCES' super_private_seq 0

echo '=== Only direct SUPER or SUPER on the current role can change the global switch ==='
bendsql_connect_root_null --query="
CREATE ROLE super_switch_role;
GRANT SUPER ON *.* TO ROLE super_switch_role;
GRANT ROLE super_switch_role TO super_public_creator;
"
# Changing the tenant setting affects new sessions.
expect_allowed super_direct_user "SET GLOBAL $sequence_check = 1"
expect_denied super_public_creator "SET GLOBAL $sequence_check = 0" Super
expect_denied super_public_creator "UNSET GLOBAL $sequence_check" Super
expect_denied super_public_creator 'INSERT INTO super_seq_defaults (payload) VALUES (3)' 'ACCESS SEQUENCE'
output=$(request super_public_creator 'SELECT nextval(super_private_seq)' "{\"settings\":{\"$sequence_check\":\"0\"}}")
jq -e '.error.message | contains("Variable scope mismatch")' <<<"$output" >/dev/null || {
	printf '%s\n' "$output"
	exit 1
}
echo 'session override rejected'
expect_allowed super_public_creator "UNSET GLOBAL $sequence_check" '{"role":"super_switch_role"}'
expect_allowed super_public_creator 'INSERT INTO super_seq_defaults (payload) VALUES (4)'
