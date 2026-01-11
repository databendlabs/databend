#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

DB="test_ee_11_table_ref"
TBL="t11_0000"

export BENDSQL_CLIENT_CONNECT_DB="${BENDSQL_CLIENT_CONNECT} -D ${DB}"
export BENDSQL_CLIENT_OUTPUT_NULL_DB="${BENDSQL_CLIENT_OUTPUT_NULL} -D ${DB}"

echo "drop database if exists ${DB}"
echo "DROP DATABASE IF EXISTS ${DB}" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "create database ${DB}"
echo "CREATE DATABASE ${DB}" | $BENDSQL_CLIENT_OUTPUT_NULL

echo "create table ${TBL}(a int)"
echo "CREATE TABLE ${TBL}(a INT)" | $BENDSQL_CLIENT_OUTPUT_NULL_DB

echo "two insertions"
echo "INSERT INTO ${TBL} VALUES (1),(2)" | $BENDSQL_CLIENT_OUTPUT_NULL_DB

echo "INSERT INTO ${TBL} VALUES (3)" | $BENDSQL_CLIENT_OUTPUT_NULL_DB
echo "latest snapshot should contain 3 rows"
echo "SELECT COUNT(*) FROM ${TBL}" | $BENDSQL_CLIENT_CONNECT_DB

SNAPSHOT_ID=$(
    echo "SELECT previous_snapshot_id FROM fuse_snapshot('${DB}','${TBL}') WHERE row_count=3" \
        | $BENDSQL_CLIENT_CONNECT
)
TIMEPOINT=$(
    echo "SELECT timestamp FROM fuse_snapshot('${DB}','${TBL}') WHERE row_count=2" \
        | $BENDSQL_CLIENT_CONNECT
)

echo "create branch at snapshot (expect 2 rows)"
echo "ALTER TABLE ${TBL} CREATE BRANCH b_snapshot AT (SNAPSHOT => '${SNAPSHOT_ID}')" \
    | $BENDSQL_CLIENT_OUTPUT_NULL_DB
echo "SELECT COUNT(*) FROM ${TBL}/b_snapshot" | $BENDSQL_CLIENT_CONNECT_DB

echo "create branch at timestamp (expect 2 rows)"
echo "ALTER TABLE ${TBL} CREATE BRANCH b_ts AT (TIMESTAMP => '${TIMEPOINT}'::TIMESTAMP)" \
    | $BENDSQL_CLIENT_OUTPUT_NULL_DB
echo "SELECT COUNT(*) FROM ${TBL}/b_ts" | $BENDSQL_CLIENT_CONNECT_DB

echo "alter table schema"
echo "ALTER TABLE ${TBL} ADD COLUMN b INT" | $BENDSQL_CLIENT_OUTPUT_NULL_DB

echo "create branch from old snapshot after schema change should fail"
output=$(
    echo "ALTER TABLE ${TBL} CREATE BRANCH b_err AT (SNAPSHOT => '${SNAPSHOT_ID}')" \
        | $BENDSQL_CLIENT_CONNECT_DB 2>&1 || true
)
cnt=$(echo "$output" | grep -c "\\[2747\\]")
echo "$cnt"
if [ "$cnt" -ne 1 ]; then
    exit 1
fi
cnt=$(echo "$output" | grep -c "different schema")
echo "$cnt"
if [ "$cnt" -ne 1 ]; then
    exit 1
fi

echo "DROP DATABASE IF EXISTS ${DB}" | $BENDSQL_CLIENT_OUTPUT_NULL
