#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cleanup() {
	bendsql_connect_root_null --query="
DROP CONNECTION IF EXISTS always_checked_conn;
DROP SEQUENCE IF EXISTS always_checked_seq;
DROP USER IF EXISTS authorization_no_grants;
DROP USER IF EXISTS authorization_granted;
DROP ROLE IF EXISTS authorization_access_role;
"
}

cleanup
trap cleanup EXIT
bendsql_connect_root_null --query="
CREATE CONNECTION always_checked_conn STORAGE_TYPE = 's3' ENDPOINT_URL = 'http://127.0.0.1:9900';
CREATE SEQUENCE always_checked_seq;
CREATE USER authorization_no_grants IDENTIFIED BY 'authorization_test';
CREATE USER authorization_granted IDENTIFIED BY 'authorization_test';
CREATE ROLE authorization_access_role;
GRANT ACCESS CONNECTION ON CONNECTION always_checked_conn TO ROLE authorization_access_role;
GRANT ACCESS SEQUENCE ON SEQUENCE always_checked_seq TO ROLE authorization_access_role;
GRANT ROLE authorization_access_role TO authorization_granted;
"

request() {
	local payload
	payload=$(jq -nc --arg sql "$2" --argjson settings "$settings" '{sql: $sql, session: {settings: $settings}, pagination: {wait_time_secs: 30}}')
	curl --fail --silent --show-error -u "$1:authorization_test" \
		"http://$QUERY_MYSQL_HANDLER_HOST:$QUERY_HTTP_HANDLER_PORT/v1/query" \
		-H 'Content-Type: application/json' --data-binary "$payload"
}

expect_visibility() {
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

expect_denied() {
	local output
	output=$(request authorization_no_grants "$1")
	# Connection resolution wraps PermissionDenied in BadArguments; require the
	# nested permission error as well as the requested privilege in that case.
	jq -e --arg privilege "${2:-}" '
		(.error.code == 1063 or (.error.code == 1006 and (.error.message | contains("PermissionDenied. Code: 1063")))) and
		(.error.message | contains($privilege))
	' <<<"$output" >/dev/null || {
		printf '%s\n' "$output"
		return 1
	}
	echo denied
}

# HTTP session settings ignore unknown names for compatibility. Supplying either
# removed switch must not disable visibility filtering or object access checks.
for value in baseline 0 1; do
	settings='{}'
	if [[ "$value" != baseline ]]; then
		settings=$(jq -nc --arg value "$value" '{enable_experimental_connection_privilege_check: $value, enable_experimental_sequence_privilege_check: $value}')
	fi
	echo "=== HTTP authorization settings: $value ==="
	expect_visibility authorization_no_grants 'SHOW CONNECTIONS' always_checked_conn 0
	expect_visibility authorization_no_grants 'SHOW SEQUENCES' always_checked_seq 0
	expect_visibility authorization_no_grants 'SELECT name FROM show_sequences()' always_checked_seq 0
	expect_denied 'DESC CONNECTION always_checked_conn'
	expect_denied 'DROP CONNECTION always_checked_conn'
	expect_denied "CREATE STAGE forbidden_stage URL = 's3://bucket' CONNECTION = (CONNECTION_NAME = 'always_checked_conn')" AccessConnection
	expect_denied 'DESC SEQUENCE always_checked_seq'
	expect_denied 'DROP SEQUENCE always_checked_seq'
	expect_denied 'SELECT nextval(always_checked_seq)'
	expect_visibility authorization_granted 'SHOW CONNECTIONS' always_checked_conn 1
	expect_visibility authorization_granted 'SHOW SEQUENCES' always_checked_seq 1
done
