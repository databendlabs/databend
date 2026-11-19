#!/usr/bin/env bash

set -euo pipefail

endpoint=${DATABEND_TEST_HTTP_ENDPOINT:-http://localhost:8000}
administrator=root:
writer=history_access_writer:history_access_test_password

request() {
  local auth=$1 sql=$2
  curl --fail --silent --show-error -u "$auth" "$endpoint/v1/query" \
    -H 'Content-Type: application/json' \
    --data-binary "$(jq -nc --arg sql "$sql" '{sql: $sql, pagination: {wait_time_secs: 10}}')"
}

run_sql() {
  local response
  response=$(request "$@")
  if ! jq -e '.state == "Succeeded" and .error == null' <<<"$response" >/dev/null; then
    printf 'Failed SQL: %s\n%s\n' "$2" "$response" >&2
    return 1
  fi
  printf '%s\n' "$response"
}

cleanup() {
  for sql in \
    'DROP STAGE IF EXISTS history_access_stage' \
    'DROP TABLE IF EXISTS default.history_access_table' \
    'DROP SEQUENCE IF EXISTS history_access_seq' \
    'DROP CONNECTION IF EXISTS history_access_conn' \
    'DROP TAG IF EXISTS history_access_tag' \
    'DROP USER IF EXISTS history_access_writer' \
    'DROP ROLE IF EXISTS history_access_role'; do
    run_sql "$administrator" "$sql" >/dev/null
  done
}

cleanup
trap cleanup EXIT
for sql in \
  "CREATE CONNECTION history_access_conn STORAGE_TYPE = 's3' ENDPOINT_URL = 'http://127.0.0.1:9900'" \
  'CREATE SEQUENCE history_access_seq' \
  'CREATE TABLE default.history_access_table (id BIGINT DEFAULT nextval(history_access_seq), payload INT)' \
  'CREATE ROLE history_access_role' \
  'CREATE TAG history_access_tag' \
  "CREATE USER history_access_writer IDENTIFIED BY '${writer#*:}' WITH DEFAULT_ROLE = 'history_access_role'" \
  'GRANT ROLE history_access_role TO history_access_writer' \
  'GRANT INSERT ON default.history_access_table TO ROLE history_access_role'; do
  run_sql "$administrator" "$sql" >/dev/null
done

create_stage="CREATE STAGE history_access_stage URL = 's3://history-access-test' CONNECTION = (CONNECTION_NAME = 'history_access_conn')"
denied=$(request "$writer" "$create_stage")
denied_id=$(jq -er 'if .state == "Failed" and (.error.message | contains("AccessConnection")) then .id else error(tostring) end' <<<"$denied")
grant_id=$(run_sql "$administrator" 'GRANT ACCESS CONNECTION ON CONNECTION history_access_conn TO ROLE history_access_role' | jq -er '.id')
connection_id=$(run_sql "$administrator" "$create_stage" | jq -er '.id')
conn_desc_id=$(run_sql "$writer" 'DESC CONNECTION history_access_conn' | jq -er '.id')
tag_id=$(run_sql "$administrator" "ALTER CONNECTION history_access_conn SET TAG history_access_tag = 'test'" | jq -er '.id')
tag_read_id=$(run_sql "$writer" "SELECT * FROM tag_references('history_access_conn', 'CONNECTION')" | jq -er '.id')
desc_id=$(run_sql "$administrator" 'DESC SEQUENCE history_access_seq' | jq -er '.id')
default_id=$(run_sql "$writer" 'INSERT INTO default.history_access_table (payload) VALUES (1)' | jq -er '.id')
values_id=$(run_sql "$writer" \
  'INSERT INTO default.history_access_table VALUES (nextval(history_access_seq), 2), (nextval(history_access_seq), 3)' | jq -er '.id')
select_id=$(run_sql "$writer" 'SELECT nextval(history_access_seq) FROM numbers(4)' | jq -er '.id')
explicit_id=$(run_sql "$writer" 'INSERT INTO default.history_access_table VALUES (1234, 4)' | jq -er '.id')
explain_id=$(run_sql "$writer" 'EXPLAIN SELECT nextval(history_access_seq)' | jq -er '.id')
# Exercise the no-op path before the last positive event, so polling does not
# finish before those earlier statements' access events have been ingested.
noop_id=$(run_sql "$administrator" 'DROP CONNECTION IF EXISTS history_access_missing_conn' | jq -er '.id')
missing_db_id=$(run_sql "$administrator" "ALTER DATABASE IF EXISTS history_access_missing_db SET OPTIONS (DEFAULT_STORAGE_CONNECTION = 'history_access_conn', DEFAULT_STORAGE_PATH = 's3://history-access-test')" | jq -er '.id')
drop_seq_id=$(run_sql "$administrator" 'DROP SEQUENCE history_access_seq' | jq -er '.id')
drop_conn_id=$(run_sql "$administrator" 'DROP CONNECTION history_access_conn' | jq -er '.id')

# Named accesses are independent of the plan-based table/stage records with the same query_id.
sql="SELECT h.query_id, h.user_name, a.value['object_domain']::STRING,
       a.value['object_name']::STRING, a.value['role_name']::STRING
FROM system_history.access_history AS h, LATERAL FLATTEN(input => h.direct_objects_accessed) AS a
WHERE h.query_id IN ('$connection_id', '$conn_desc_id', '$tag_id', '$tag_read_id', '$desc_id', '$default_id', '$values_id',
  '$select_id', '$explicit_id', '$explain_id', '$denied_id', '$grant_id', '$noop_id', '$missing_db_id', '$drop_seq_id', '$drop_conn_id')"
for _ in {1..30}; do
  response=$(request "$administrator" "$sql")
  if jq -e --arg conn "$connection_id" --arg conn_desc "$conn_desc_id" --arg tag "$tag_id" --arg tag_read "$tag_read_id" \
    --arg desc "$desc_id" --arg default "$default_id" --arg values "$values_id" \
    --arg select "$select_id" --arg drop_seq "$drop_seq_id" --arg drop_conn "$drop_conn_id" '
      def conn_as_writer($id): [$id, "history_access_writer", "Connection", "history_access_conn", "history_access_role"];
      def conn_as_admin($id): [$id, "root", "Connection", "history_access_conn", "account_admin"];
      def seq_as_writer($id): [$id, "history_access_writer", "Sequence", "history_access_seq", "history_access_role"];
      def seq_as_admin($id): [$id, "root", "Sequence", "history_access_seq", "account_admin"];
      .state == "Succeeded" and .error == null and
      ([.data[] | select(.[0] == $values)] == [seq_as_writer($values)]) and
      ((.data | unique) == ([conn_as_admin($conn), conn_as_writer($conn_desc), conn_as_admin($tag), conn_as_writer($tag_read),
        seq_as_admin($desc), seq_as_writer($default), seq_as_writer($values), seq_as_writer($select),
        seq_as_admin($drop_seq), conn_as_admin($drop_conn)] | sort))' <<<"$response" >/dev/null; then
    echo 'Named object access history checks passed'
    exit 0
  fi
  sleep 1
done
printf 'Unexpected named object access history: %s\n' "$response" >&2
exit 1
