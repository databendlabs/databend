#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

rbac_check=enable_experimental_rbac_check
connection_check=enable_experimental_connection_privilege_check
sequence_check=enable_experimental_sequence_privilege_check

cleanup() {
	bendsql_connect_root_null --query="
UNSET GLOBAL $rbac_check;
UNSET GLOBAL $connection_check;
UNSET GLOBAL $sequence_check;
DROP STAGE IF EXISTS super_forbidden_stage;
DROP STAGE IF EXISTS super_private_stage;
DROP TABLE IF EXISTS super_seq_defaults;
DROP FUNCTION IF EXISTS super_private_udf;
DROP PROCEDURE IF EXISTS super_private_proc();
DROP CONNECTION IF EXISTS super_legacy_conn;
DROP CONNECTION IF EXISTS super_private_conn;
DROP CONNECTION IF EXISTS super_created_conn;
DROP CONNECTION IF EXISTS super_ops_conn;
DROP SEQUENCE IF EXISTS super_private_seq;
DROP SEQUENCE IF EXISTS super_created_seq;
DROP SEQUENCE IF EXISTS super_legacy_seq;
DROP USER IF EXISTS super_direct_user;
DROP USER IF EXISTS super_public_creator;
DROP USER IF EXISTS super_ops_user;
DROP ROLE IF EXISTS super_creator_role;
DROP ROLE IF EXISTS super_switch_role;
DROP ROLE IF EXISTS super_ops_role;
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
		(.error.message | ascii_downcase | contains($privilege | ascii_downcase))
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
CREATE FUNCTION super_private_udf AS (x) -> (x + 1);
CREATE OR REPLACE PROCEDURE super_private_proc() RETURNS INT LANGUAGE SQL AS \$\$ BEGIN RETURN 42; END; \$\$;
CREATE STAGE super_private_stage;
GRANT OWNERSHIP ON CONNECTION super_private_conn TO ROLE account_admin;
GRANT OWNERSHIP ON SEQUENCE super_private_seq TO ROLE account_admin;
CREATE USER super_direct_user IDENTIFIED BY 'super_object_test';
GRANT SUPER ON *.* TO super_direct_user;
CREATE USER super_public_creator IDENTIFIED BY 'super_object_test';
"

echo '=== Legacy Connection mode allows SUPER management and named usage ==='
expect_allowed super_direct_user "CREATE CONNECTION super_legacy_conn STORAGE_TYPE = 's3'"
expect_denied super_public_creator 'DESC CONNECTION super_legacy_conn' Super
expect_allowed super_direct_user 'DROP CONNECTION super_legacy_conn'
expect_allowed super_direct_user 'DESC CONNECTION super_private_conn'
expect_visible super_public_creator 'SHOW CONNECTIONS' super_private_conn 1
create_stage="CREATE STAGE super_forbidden_stage URL = 's3://bucket' CONNECTION = (CONNECTION_NAME = 'super_private_conn')"
expect_allowed super_direct_user "$create_stage"
bendsql_connect_root_null --query='DROP STAGE super_forbidden_stage'

echo '=== Only SUPER can enable Connection object checks ==='
expect_denied super_public_creator "SET GLOBAL $connection_check = 1" Super
expect_allowed super_direct_user "SET GLOBAL $connection_check = 1"
expect_allowed super_direct_user 'DESC CONNECTION super_private_conn'
expect_visible super_direct_user 'SHOW CONNECTIONS' super_private_conn 0
expect_denied super_direct_user "$create_stage" AccessConnection

echo '=== Legacy sequence mode: SUPER manages sequences, and all sequences are listed ==='
expect_allowed super_direct_user 'CREATE SEQUENCE super_legacy_seq'
echo "owners: $(bendsql_connect_root --output tsv --query='SHOW GRANTS ON SEQUENCE super_legacy_seq' | grep -c OWNERSHIP)"
expect_allowed super_direct_user 'DESC SEQUENCE super_private_seq'
expect_visible super_public_creator 'SHOW SEQUENCES' super_private_seq 1
expect_denied super_public_creator 'DESC SEQUENCE super_private_seq' Super
expect_denied super_public_creator 'CREATE SEQUENCE super_created_seq' Super
expect_allowed super_direct_user 'DROP SEQUENCE super_legacy_seq'

echo '=== Sequence object checks keep the SUPER bypass for management ==='
expect_allowed super_direct_user "SET GLOBAL $sequence_check = 1"
expect_allowed super_direct_user 'DESC SEQUENCE super_private_seq'
expect_visible super_direct_user 'SHOW SEQUENCES' super_private_seq 0
expect_denied super_public_creator 'DESC SEQUENCE super_private_seq' AccessSequence

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

echo '=== With object checks, a SUPER role owns the connections it creates or replaces ==='
bendsql_connect_root_null --query="
CREATE ROLE super_ops_role;
GRANT SUPER ON *.* TO ROLE super_ops_role;
CREATE USER super_ops_user IDENTIFIED BY 'super_object_test' WITH DEFAULT_ROLE = 'super_ops_role';
GRANT ROLE super_ops_role TO super_ops_user;
"
# super_ops_role has no CREATE CONNECTION, and super_created_conn is owned by super_creator_role.
expect_allowed super_ops_user "CREATE CONNECTION super_ops_conn STORAGE_TYPE = 's3'"
expect_allowed super_ops_user "CREATE OR REPLACE CONNECTION super_created_conn STORAGE_TYPE = 's3'"
for connection in super_ops_conn super_created_conn; do
	bendsql_connect_root --output tsv --query="SHOW GRANTS ON CONNECTION $connection" |
		awk -F '\t' '$1 == "OWNERSHIP" { print $1 "\t" $5 }'
	expect_visible super_ops_user 'SHOW CONNECTIONS' "$connection" 1
done

echo '=== In legacy sequence mode, nextval skips object checks ==='
expect_allowed super_direct_user "UNSET GLOBAL $sequence_check"
bendsql_connect_root_null --query="
CREATE TABLE super_seq_defaults (id BIGINT DEFAULT nextval(super_private_seq), payload INT);
GRANT INSERT ON default.super_seq_defaults TO super_public_creator;
"
expect_denied super_public_creator "SET GLOBAL $sequence_check = 1" Super
expect_allowed super_public_creator 'INSERT INTO super_seq_defaults (payload) VALUES (1)'
expect_allowed super_public_creator 'INSERT INTO super_seq_defaults VALUES (nextval(super_private_seq), 2)'
expect_allowed super_public_creator 'SELECT nextval(super_private_seq) FROM numbers(2)'
expect_denied super_public_creator 'DESC SEQUENCE super_private_seq' Super
expect_visible super_public_creator 'SHOW SEQUENCES' super_private_seq 1

echo '=== Procedure rows and bodies follow the RBAC switch ==='
procedure_body_sql="SELECT body FROM system.procedures WHERE name = 'super_private_proc'"
request super_public_creator "$procedure_body_sql" | jq -e '
	.state == "Succeeded" and .error == null and .data == []
' >/dev/null
echo 'procedure row hidden when RBAC is enabled'

echo '=== Stage and UDF checks are on by default; SUPER can restore legacy behavior ==='
expect_denied super_public_creator 'SELECT super_private_udf(1)' Usage
expect_denied super_public_creator 'LIST @super_private_stage' Read
expect_denied super_public_creator "SET GLOBAL $rbac_check = 0" Super
expect_allowed super_direct_user "SET GLOBAL $rbac_check = 0"
expect_allowed super_public_creator 'SELECT super_private_udf(1)'
expect_allowed super_public_creator 'LIST @super_private_stage'

request super_public_creator "$procedure_body_sql" | jq -e '
	.state == "Succeeded" and .error == null and .data == [["BEGIN RETURN 42; END;"]]
' >/dev/null
echo 'procedure body visible when RBAC is disabled'
request super_public_creator "SHOW PROCEDURES WHERE name = 'super_private_proc'" | jq -e '
	.state == "Succeeded" and .error == null and
	(.schema | map(.name)) == ["name", "procedure_id", "arguments", "comment", "description", "created_on"] and
	.data[0][0] == "super_private_proc"
' >/dev/null
echo 'SHOW PROCEDURES omits the body column'

echo '=== Only direct SUPER or SUPER on the current role can change the global switches ==='
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
expect_denied super_public_creator "SET GLOBAL $connection_check = 0" Super
expect_denied super_public_creator "UNSET GLOBAL $connection_check" Super
expect_denied super_public_creator "SET GLOBAL $rbac_check = 1" Super
expect_denied super_public_creator "UNSET GLOBAL $rbac_check" Super
for setting in "$sequence_check" "$connection_check" "$rbac_check"; do
	output=$(request super_public_creator 'SELECT 1' "{\"settings\":{\"$setting\":\"0\"}}")
	jq -e '.error.message | contains("Variable scope mismatch")' <<<"$output" >/dev/null || {
		printf '%s\n' "$output"
		exit 1
	}
	echo 'session override rejected'
	expect_allowed super_public_creator "UNSET GLOBAL $setting" '{"role":"super_switch_role"}'
done
expect_denied super_public_creator 'SELECT super_private_udf(1)' Usage
expect_denied super_public_creator 'LIST @super_private_stage' Read
expect_allowed super_direct_user "$create_stage"
expect_allowed super_public_creator 'INSERT INTO super_seq_defaults (payload) VALUES (4)'
