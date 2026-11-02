#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cleanup() {
	bendsql_connect_root_null --query="
DROP STAGE IF EXISTS advisory_private_stage;
DROP FUNCTION IF EXISTS advisory_private_udf;
DROP USER IF EXISTS advisory_no_grants;
DROP USER IF EXISTS advisory_granted;
DROP ROLE IF EXISTS advisory_access_role;
"
}

cleanup
trap cleanup EXIT
bendsql_connect_root_null --query="
CREATE STAGE advisory_private_stage FILE_FORMAT = (TYPE = CSV);
CREATE FUNCTION advisory_private_udf AS (x) -> x + 1;
GRANT OWNERSHIP ON STAGE advisory_private_stage TO ROLE account_admin;
GRANT OWNERSHIP ON UDF advisory_private_udf TO ROLE account_admin;
COPY INTO @advisory_private_stage/input/ FROM (SELECT 1);
CREATE USER advisory_no_grants IDENTIFIED BY 'advisory_test';
CREATE USER advisory_granted IDENTIFIED BY 'advisory_test';
CREATE ROLE advisory_access_role;
GRANT READ, WRITE ON STAGE advisory_private_stage TO ROLE advisory_access_role;
GRANT USAGE ON UDF advisory_private_udf TO ROLE advisory_access_role;
GRANT ROLE advisory_access_role TO advisory_granted;
"

request() {
	local payload
	payload=$(jq -nc --arg sql "$2" --argjson settings "$settings" '{sql: $sql, session: {settings: $settings}, pagination: {wait_time_secs: 30}}')
	curl --fail --silent --show-error -u "$1:advisory_test" \
		"http://$QUERY_MYSQL_HANDLER_HOST:$QUERY_HTTP_HANDLER_PORT/v1/query" \
		-H 'Content-Type: application/json' --data-binary "$payload"
}

check_access() {
	local output object
	object=advisory_private_stage
	if [[ "$1" == udf ]]; then
		object=advisory_private_udf
	fi
	output=$(request advisory_no_grants "$2")
	# Require an actual authorization error about the target, not a parse error,
	# missing file, or unrelated storage failure.
	jq -e --arg object "$object" '
		.error.code == 1063 and
		(.error.message | contains("Permission denied") and contains($object))
	' <<<"$output" >/dev/null || {
		printf '%s\n' "$output"
		return 1
	}
	echo "denied $1"

	# The same statement must succeed with the corresponding grant. This also
	# checks that each injection syntax and the internal-stage fixture are valid.
	output=$(request advisory_granted "$2")
	jq -e '.state == "Succeeded" and .error == null' <<<"$output" >/dev/null || {
		printf '%s\n' "$output"
		return 1
	}
	echo "allowed $1"
}

for mode in baseline http settings hint; do
	settings='{}'
	prefix=''
	hint=''
	case "$mode" in
	http) settings='{"enable_experimental_rbac_check":"0"}' ;;
	settings) prefix='SETTINGS (enable_experimental_rbac_check = 0) ' ;;
	hint) hint='/*+ SET_VAR(enable_experimental_rbac_check = 0) */' ;;
	esac

	echo "=== Stage/UDF authorization: $mode ==="
	check_access stage-read "${prefix}SELECT $hint \$1 FROM @advisory_private_stage"
	check_access stage-write "${prefix}COPY $hint INTO @advisory_private_stage/output/ FROM (SELECT 1)"
	if [[ "$mode" == hint ]]; then
		# LIST is rewritten to this table function; use SELECT to carry its hint.
		check_access stage-list "SELECT $hint * FROM LIST_STAGE(location => '@advisory_private_stage')"
	else
		check_access stage-list "${prefix}LIST @advisory_private_stage"
	fi
	check_access udf "${prefix}SELECT $hint advisory_private_udf(1)"
done
