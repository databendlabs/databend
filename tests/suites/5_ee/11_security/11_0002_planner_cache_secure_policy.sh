#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

TABLE_NAME="pc_current_user_mask_test"
POLICY_NAME="pc_current_user_mask_policy"
ROLE_NAME="pc_same_role"
USER_A="pc_user_a"
USER_B="pc_user_b"
PASSWORD="123"

quiet_root_sql() {
	echo "$1" | bendsql_connect_root_null
}

cleanup() {
	quiet_root_sql "DROP TABLE IF EXISTS ${TABLE_NAME}" || true
	quiet_root_sql "DROP MASKING POLICY IF EXISTS ${POLICY_NAME}" || true
	quiet_root_sql "DROP USER IF EXISTS ${USER_A}" || true
	quiet_root_sql "DROP USER IF EXISTS ${USER_B}" || true
	quiet_root_sql "DROP ROLE IF EXISTS ${ROLE_NAME}" || true
}

run_user_query() {
	local user="$1"
	local sql="$2"
	bendsql_connect_user "${user}" "${PASSWORD}" -A --quote-style=never <<<"${sql}"
}

expect_user_query() {
	local user="$1"
	local sql="$2"
	local expected="$3"
	local actual

	actual=$(run_user_query "${user}" "${sql}")
	if [[ "${actual}" != "${expected}" ]]; then
		echo "unexpected result for ${user}"
		echo "expected: ${expected}"
		echo "actual: ${actual}"
		exit 1
	fi
	echo "ok"
}

comment "Test: planner cache secure policy key includes current user"
comment "setup"
cleanup

quiet_root_sql "CREATE ROLE ${ROLE_NAME}"
quiet_root_sql "CREATE USER ${USER_A} IDENTIFIED BY '${PASSWORD}' WITH DEFAULT_ROLE='${ROLE_NAME}'"
quiet_root_sql "CREATE USER ${USER_B} IDENTIFIED BY '${PASSWORD}' WITH DEFAULT_ROLE='${ROLE_NAME}'"
quiet_root_sql "GRANT ROLE ${ROLE_NAME} TO ${USER_A}"
quiet_root_sql "GRANT ROLE ${ROLE_NAME} TO ${USER_B}"
quiet_root_sql "CREATE TABLE ${TABLE_NAME}(id INT, secret STRING)"
quiet_root_sql "INSERT INTO ${TABLE_NAME} VALUES (1, 'visible_to_a')"
quiet_root_sql "GRANT SELECT ON default.${TABLE_NAME} TO ROLE ${ROLE_NAME}"
quiet_root_sql "CREATE MASKING POLICY ${POLICY_NAME} AS (val STRING) RETURNS STRING -> CASE WHEN current_user() = '''${USER_A}''@''%''' THEN val ELSE 'MASKED' END"
quiet_root_sql "ALTER TABLE ${TABLE_NAME} MODIFY COLUMN secret SET MASKING POLICY ${POLICY_NAME}"

SQL="SET enable_planner_cache = 1; SET enable_query_result_cache = 0; SELECT secret FROM ${TABLE_NAME} ORDER BY id;"

comment "user a populates a plan with unmasked policy body"
expect_user_query "${USER_A}" "${SQL}" "visible_to_a"

comment "user a can reuse its own secure plan"
expect_user_query "${USER_A}" "${SQL}" "visible_to_a"

comment "same role user b must not reuse user a's secure plan"
expect_user_query "${USER_B}" "${SQL}" "MASKED"

comment "user b can reuse its own secure plan"
expect_user_query "${USER_B}" "${SQL}" "MASKED"

comment "cleanup"
cleanup
