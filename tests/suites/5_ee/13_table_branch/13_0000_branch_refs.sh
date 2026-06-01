#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

DB="test_table_branch_refs"

run_sql() {
    echo "$1" | $BENDSQL_CLIENT_CONNECT > /dev/null || exit 1
}

run_ref_sql() {
    run_sql "set enable_experimental_table_ref=1; $1"
}

query_sql() {
    echo "$1" | $BENDSQL_CLIENT_CONNECT || exit 1
}

query_ref_sql() {
    query_sql "set enable_experimental_table_ref=1; $1"
}

scalar_sql() {
    local result
    result=$(echo "$1" | $BENDSQL_CLIENT_CONNECT) || return 1
    [ -n "$result" ] || return 1
    echo "$result"
}

scalar_ref_sql() {
    scalar_sql "set enable_experimental_table_ref=1; $1"
}

run_sql "DROP DATABASE IF EXISTS ${DB}"
run_sql "CREATE DATABASE ${DB}"

run_sql "CREATE OR REPLACE TABLE ${DB}.t_undrop_by_id(id INT, val STRING)"
run_sql "INSERT INTO ${DB}.t_undrop_by_id VALUES (1, 'base'), (2, 'base')"
run_ref_sql "ALTER TABLE ${DB}.t_undrop_by_id CREATE BRANCH dev"
run_ref_sql "INSERT INTO ${DB}.t_undrop_by_id/dev VALUES (3, 'branch')"
run_ref_sql "ALTER TABLE ${DB}.t_undrop_by_id DROP BRANCH dev"

BRANCH_ID=$(scalar_sql "SELECT branch_id FROM system.branches_with_history WHERE database = '${DB}' AND table = 't_undrop_by_id' AND name = 'dev' AND dropped_on IS NOT NULL ORDER BY dropped_on DESC LIMIT 1") || exit 1
run_ref_sql "ALTER TABLE ${DB}.t_undrop_by_id UNDROP BRANCH IDENTIFIER(${BRANCH_ID}) RETAIN 7 DAYS"

echo "undrop branch by id"
query_ref_sql "SELECT * FROM ${DB}.t_undrop_by_id/dev ORDER BY id"
query_sql "SELECT name, expire_at IS NOT NULL FROM system.branches WHERE database = '${DB}' AND table = 't_undrop_by_id' AND name = 'dev'"

run_sql "CREATE OR REPLACE TABLE ${DB}.t_time_travel(id INT, val STRING)"
run_sql "INSERT INTO ${DB}.t_time_travel VALUES (1, 'base')"
run_ref_sql "ALTER TABLE ${DB}.t_time_travel CREATE BRANCH dev"
run_ref_sql "INSERT INTO ${DB}.t_time_travel/dev VALUES (2, 'branch_first')"

SNAPSHOT_ID=$(scalar_ref_sql "SELECT snapshot_id FROM fuse_snapshot('${DB}', 't_time_travel/dev') WHERE row_count = 2 ORDER BY timestamp LIMIT 1") || exit 1
TIMEPOINT=$(scalar_ref_sql "SELECT timestamp FROM fuse_snapshot('${DB}', 't_time_travel/dev') WHERE row_count = 2 ORDER BY timestamp LIMIT 1") || exit 1

run_ref_sql "INSERT INTO ${DB}.t_time_travel/dev VALUES (3, 'branch_second')"

echo "branch at snapshot"
query_ref_sql "SELECT * FROM ${DB}.t_time_travel/dev AT (SNAPSHOT => '${SNAPSHOT_ID}') ORDER BY id"

echo "branch at timestamp"
query_ref_sql "SELECT * FROM ${DB}.t_time_travel/dev AT (TIMESTAMP => '${TIMEPOINT}'::TIMESTAMP) ORDER BY id"

run_sql "DROP DATABASE IF EXISTS ${DB}"
