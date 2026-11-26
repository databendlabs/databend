#!/usr/bin/env bash

# IS_ROLE_IN_SESSION, CURRENT_SECONDARY_ROLES and CURRENT_AVAILABLE_ROLES resolve to the roles
# enabled for the session, CURRENT_USER and CURRENT_ROLE to the user and the role. The plan cache
# key does not hold the roles, so these plans are not cached. The result cache key holds each
# value: a cached result is reused by the same user and role, but never served after a revoke or
# to another user or role, including when a plan cache hit skips binding.

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

PASSWORD="rc_role_fn_pw"
ADMIN_ROLE="rc_admin_role"
READER_ROLE="rc_reader_role"
USER_A="rc_user_a"
USER_B="rc_user_b"

root_sql() {
	echo "$1" | bendsql_connect_root_null
}

user_query() {
	bendsql_connect_user "$1" "${PASSWORD}" -A --quote-style=never <<<"$2"
}

cleanup() {
	root_sql "DROP VIEW IF EXISTS rc_owner_v" || true
	root_sql "DROP TABLE IF EXISTS rc_owner_t" || true
	root_sql "DROP TABLE IF EXISTS rc_rap_t" || true
	root_sql "DROP TABLE IF EXISTS rc_mask_t" || true
	root_sql "DROP TABLE IF EXISTS rc_user_mask_t" || true
	root_sql "DROP TABLE IF EXISTS rc_role_mask_t" || true
	root_sql "DROP ROW ACCESS POLICY IF EXISTS rc_rap_policy" || true
	root_sql "DROP MASKING POLICY IF EXISTS rc_mask_policy" || true
	root_sql "DROP MASKING POLICY IF EXISTS rc_user_mask_policy" || true
	root_sql "DROP MASKING POLICY IF EXISTS rc_role_mask_policy" || true
	root_sql "DROP USER IF EXISTS ${USER_A}" || true
	root_sql "DROP USER IF EXISTS ${USER_B}" || true
	root_sql "DROP ROLE IF EXISTS ${ADMIN_ROLE}" || true
	root_sql "DROP ROLE IF EXISTS ${READER_ROLE}" || true
}

PLAN_CACHE="SET enable_planner_cache = 1; SET enable_query_result_cache = 0;"
RESULT_CACHE="SET enable_planner_cache = 0; SET enable_query_result_cache = 1; SET query_result_cache_min_execute_secs = 0;"
BOTH_CACHES="SET enable_planner_cache = 1; SET enable_query_result_cache = 1; SET query_result_cache_min_execute_secs = 0;"

# Prints hit if the statement would read a cached result. $3 runs first, such as SET ROLE.
cache_status() {
	local plan
	plan=$(user_query "$1" "${RESULT_CACHE} ${3:-} EXPLAIN $2")
	if grep -q '^ReadQueryResultCache' <<<"${plan}"; then
		echo "hit"
	else
		echo "miss"
	fi
}

comment "setup"
root_sql "SET GLOBAL enable_experimental_row_access_policy = 1"
cleanup
root_sql "CREATE ROLE ${ADMIN_ROLE}"
root_sql "CREATE ROLE ${READER_ROLE}"
root_sql "CREATE USER ${USER_A} IDENTIFIED BY '${PASSWORD}' WITH DEFAULT_ROLE = '${READER_ROLE}'"
root_sql "CREATE USER ${USER_B} IDENTIFIED BY '${PASSWORD}' WITH DEFAULT_ROLE = '${READER_ROLE}'"
root_sql "GRANT ROLE ${READER_ROLE} TO ${USER_A}"
root_sql "GRANT ROLE ${READER_ROLE} TO ${USER_B}"
root_sql "GRANT ROLE ${ADMIN_ROLE} TO ${USER_A}"

root_sql "CREATE TABLE rc_rap_t(id INT, region STRING)"
root_sql "INSERT INTO rc_rap_t VALUES (1, 'public'), (2, 'private')"
root_sql "CREATE ROW ACCESS POLICY rc_rap_policy AS (region STRING) RETURNS BOOLEAN -> IS_ROLE_IN_SESSION('${ADMIN_ROLE}') OR region = 'public'"
root_sql "ALTER TABLE rc_rap_t ADD ROW ACCESS POLICY rc_rap_policy ON (region)"

root_sql "CREATE TABLE rc_mask_t(id INT, secret STRING)"
root_sql "INSERT INTO rc_mask_t VALUES (1, 'plain')"
# The masked value reads the column: a constant would let the planner drop the table scan.
root_sql "CREATE MASKING POLICY rc_mask_policy AS (val STRING) RETURNS STRING -> CASE WHEN IS_ROLE_IN_SESSION('${ADMIN_ROLE}') THEN val ELSE CONCAT(LEFT(val, 1), '***') END"
root_sql "ALTER TABLE rc_mask_t MODIFY COLUMN secret SET MASKING POLICY rc_mask_policy"

root_sql "GRANT SELECT ON default.rc_rap_t TO ROLE ${READER_ROLE}"
root_sql "GRANT SELECT ON default.rc_mask_t TO ROLE ${READER_ROLE}"

root_sql "CREATE TABLE rc_user_mask_t(id INT, secret STRING)"
root_sql "INSERT INTO rc_user_mask_t VALUES (1, 'plain')"
root_sql "CREATE MASKING POLICY rc_user_mask_policy AS (val STRING) RETURNS STRING -> CASE WHEN current_user() = '''${USER_A}''@''%''' THEN val ELSE CONCAT(LEFT(val, 1), '***') END"
root_sql "ALTER TABLE rc_user_mask_t MODIFY COLUMN secret SET MASKING POLICY rc_user_mask_policy"

root_sql "CREATE TABLE rc_role_mask_t(id INT, secret STRING)"
root_sql "INSERT INTO rc_role_mask_t VALUES (1, 'plain')"
root_sql "CREATE MASKING POLICY rc_role_mask_policy AS (val STRING) RETURNS STRING -> CASE WHEN current_role() = '${ADMIN_ROLE}' THEN val ELSE CONCAT(LEFT(val, 1), '***') END"
root_sql "ALTER TABLE rc_role_mask_t MODIFY COLUMN secret SET MASKING POLICY rc_role_mask_policy"

root_sql "CREATE TABLE rc_owner_t(id INT, owner STRING)"
root_sql "INSERT INTO rc_owner_t VALUES (1, '''${USER_A}''@''%'''), (2, '''${USER_B}''@''%''')"
root_sql "CREATE VIEW rc_owner_v AS SELECT id FROM rc_owner_t WHERE owner = current_user()"

for table in rc_user_mask_t rc_role_mask_t rc_owner_t rc_owner_v; do
	root_sql "GRANT SELECT ON default.${table} TO ROLE ${READER_ROLE}"
done

comment "planner cache: user a holds the admin role"
user_query "${USER_A}" "${PLAN_CACHE} SELECT id FROM rc_rap_t ORDER BY id;"
user_query "${USER_A}" "${PLAN_CACHE} SELECT secret FROM rc_mask_t ORDER BY id;"

root_sql "REVOKE ROLE ${ADMIN_ROLE} FROM ${USER_A}"

comment "planner cache: the revoked role no longer applies to the same statements"
user_query "${USER_A}" "${PLAN_CACHE} SELECT id FROM rc_rap_t ORDER BY id;"
user_query "${USER_A}" "${PLAN_CACHE} SELECT secret FROM rc_mask_t ORDER BY id;"

root_sql "GRANT ROLE ${ADMIN_ROLE} TO ${USER_A}"

comment "result cache: user a caches results with the admin role"
user_query "${USER_A}" "${RESULT_CACHE} SELECT id AS rid FROM rc_rap_t ORDER BY id;"
user_query "${USER_A}" "${RESULT_CACHE} SELECT secret AS s FROM rc_mask_t ORDER BY id;"
user_query "${USER_A}" "${RESULT_CACHE} SELECT CURRENT_AVAILABLE_ROLES() AS r, count(*) AS c FROM rc_mask_t;"
user_query "${USER_A}" "${RESULT_CACHE} SELECT CURRENT_SECONDARY_ROLES() AS r, count(*) AS c FROM rc_mask_t;"

comment "result cache: user a reads its own results"
cache_status "${USER_A}" "SELECT id AS rid FROM rc_rap_t ORDER BY id;"
cache_status "${USER_A}" "SELECT secret AS s FROM rc_mask_t ORDER BY id;"
cache_status "${USER_A}" "SELECT CURRENT_AVAILABLE_ROLES() AS r, count(*) AS c FROM rc_mask_t;"

comment "result cache: user b does not read the results of user a"
cache_status "${USER_B}" "SELECT id AS rid FROM rc_rap_t ORDER BY id;"
cache_status "${USER_B}" "SELECT secret AS s FROM rc_mask_t ORDER BY id;"
user_query "${USER_B}" "${RESULT_CACHE} SELECT id AS rid FROM rc_rap_t ORDER BY id;"
user_query "${USER_B}" "${RESULT_CACHE} SELECT secret AS s FROM rc_mask_t ORDER BY id;"
user_query "${USER_B}" "${RESULT_CACHE} SELECT CURRENT_AVAILABLE_ROLES() AS r, count(*) AS c FROM rc_mask_t;"
user_query "${USER_B}" "${RESULT_CACHE} SELECT CURRENT_SECONDARY_ROLES() AS r, count(*) AS c FROM rc_mask_t;"

root_sql "REVOKE ROLE ${ADMIN_ROLE} FROM ${USER_A}"

comment "result cache: after the revoke, user a reads the results cached for the roles it keeps"
cache_status "${USER_A}" "SELECT id AS rid FROM rc_rap_t ORDER BY id;"
cache_status "${USER_A}" "SELECT secret AS s FROM rc_mask_t ORDER BY id;"
user_query "${USER_A}" "${RESULT_CACHE} SELECT secret AS s FROM rc_mask_t ORDER BY id;"

root_sql "GRANT ROLE ${ADMIN_ROLE} TO ${USER_A}"

comment "result cache: user a caches results that depend on its user and role"
user_query "${USER_A}" "${RESULT_CACHE} SELECT secret FROM rc_user_mask_t ORDER BY id;"
user_query "${USER_A}" "${RESULT_CACHE} SET ROLE ${ADMIN_ROLE}; SELECT secret FROM rc_role_mask_t ORDER BY id;"
user_query "${USER_A}" "${RESULT_CACHE} SELECT id FROM rc_owner_v ORDER BY id;"
user_query "${USER_A}" "${RESULT_CACHE} SELECT current_user() AS u, count(*) AS c FROM rc_owner_t;"
cache_status "${USER_A}" "SELECT secret FROM rc_user_mask_t ORDER BY id;"
cache_status "${USER_A}" "SELECT secret FROM rc_role_mask_t ORDER BY id;" "SET ROLE ${ADMIN_ROLE};"

comment "result cache: user a with another role does not read them"
cache_status "${USER_A}" "SELECT secret FROM rc_role_mask_t ORDER BY id;" "SET ROLE ${READER_ROLE};"
user_query "${USER_A}" "${RESULT_CACHE} SET ROLE ${READER_ROLE}; SELECT secret FROM rc_role_mask_t ORDER BY id;"

comment "result cache: user b does not read them"
user_query "${USER_B}" "${RESULT_CACHE} SELECT secret FROM rc_user_mask_t ORDER BY id;"
user_query "${USER_B}" "${RESULT_CACHE} SELECT secret FROM rc_role_mask_t ORDER BY id;"
user_query "${USER_B}" "${RESULT_CACHE} SELECT id FROM rc_owner_v ORDER BY id;"
user_query "${USER_B}" "${RESULT_CACHE} SELECT current_user() AS u, count(*) AS c FROM rc_owner_t;"

# A plan cache hit skips binding: the user and role must still reach the result cache key.
# Otherwise the second run of each user stores and reads its result under a shared key.
comment "planner and result cache: user a runs the same statement twice"
user_query "${USER_A}" "${BOTH_CACHES} SELECT secret AS both_s FROM rc_user_mask_t ORDER BY id;"
user_query "${USER_A}" "${BOTH_CACHES} SELECT secret AS both_s FROM rc_user_mask_t ORDER BY id;"
user_query "${USER_A}" "${BOTH_CACHES} SET ROLE ${ADMIN_ROLE}; SELECT secret AS both_s FROM rc_role_mask_t ORDER BY id;"
user_query "${USER_A}" "${BOTH_CACHES} SET ROLE ${ADMIN_ROLE}; SELECT secret AS both_s FROM rc_role_mask_t ORDER BY id;"

comment "planner and result cache: user b runs the same statement twice"
user_query "${USER_B}" "${BOTH_CACHES} SELECT secret AS both_s FROM rc_user_mask_t ORDER BY id;"
user_query "${USER_B}" "${BOTH_CACHES} SELECT secret AS both_s FROM rc_user_mask_t ORDER BY id;"
user_query "${USER_B}" "${BOTH_CACHES} SELECT secret AS both_s FROM rc_role_mask_t ORDER BY id;"
user_query "${USER_B}" "${BOTH_CACHES} SELECT secret AS both_s FROM rc_role_mask_t ORDER BY id;"

comment "cleanup"
cleanup
root_sql "SET GLOBAL enable_experimental_row_access_policy = 0"
