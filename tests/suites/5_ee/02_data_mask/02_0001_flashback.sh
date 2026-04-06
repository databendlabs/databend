#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

DB="ee_flashback_metadata"
RUN_ID=$$
ROW_POLICY="row_filter_${RUN_ID}"
MASK_POLICY="mask_c_${RUN_ID}"
AGG_INDEX="testi_${RUN_ID}"

snapshot_id() {
    echo "select snapshot_id from fuse_snapshot('$1', '$2') where row_count=$3 limit 1" | $BENDSQL_CLIENT_CONNECT
}

quiet_stmt() {
    echo "$1" | $BENDSQL_CLIENT_CONNECT > /dev/null || exit 1
}

comment "prepare ee flashback metadata regression environment"
stmt "SET GLOBAL enable_experimental_row_access_policy = 1"
stmt "CREATE DATABASE IF NOT EXISTS ${DB}"
stmt "DROP TABLE IF EXISTS ${DB}.t_idx"
stmt "DROP TABLE IF EXISTS ${DB}.t_mask"
stmt "DROP TABLE IF EXISTS ${DB}.t_row"
stmt "DROP DATABASE IF EXISTS ${DB}"
stmt "CREATE DATABASE ${DB}"

comment "time travel should handle row access policy metadata but flashback should reject it"
stmt "CREATE OR REPLACE TABLE ${DB}.t_row(a INT, b STRING)"
stmt "INSERT INTO ${DB}.t_row VALUES (1, 'alice'), (2, 'bob')"
ROW_SNAPSHOT_ID=$(snapshot_id "${DB}" "t_row" 2)
stmt "ALTER TABLE ${DB}.t_row ADD COLUMN c INT"
stmt "INSERT INTO ${DB}.t_row VALUES (3, 'charlie', 100), (4, 'dave', 200)"
comment "create and attach row access policy"
quiet_stmt "CREATE ROW ACCESS POLICY ${ROW_POLICY} AS (val INT) RETURNS BOOLEAN -> val > 100"
quiet_stmt "ALTER TABLE ${DB}.t_row ADD ROW ACCESS POLICY ${ROW_POLICY} ON (c)"
comment "time travel row policy table at saved snapshot"
echo "SELECT count(*)=2 FROM ${DB}.t_row AT (SNAPSHOT => '$ROW_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT || exit 1
comment "flashback row policy table to saved snapshot should fail"
echo ">>>> ALTER TABLE ${DB}.t_row FLASHBACK TO (SNAPSHOT => '<saved_snapshot>')"
echo "ALTER TABLE ${DB}.t_row FLASHBACK TO (SNAPSHOT => '$ROW_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT > /dev/null 2>&1
if [ $? -eq 0 ]; then
    exit 1
fi
echo "<<<< expected failure happened"

comment "flashback should reject incompatible masking policy metadata"
stmt "CREATE OR REPLACE TABLE ${DB}.t_mask(a INT, b STRING)"
stmt "INSERT INTO ${DB}.t_mask VALUES (1, 'hello')"
MASK_SNAPSHOT_ID=$(snapshot_id "${DB}" "t_mask" 1)
stmt "ALTER TABLE ${DB}.t_mask ADD COLUMN c STRING"
comment "create and attach masking policy"
quiet_stmt "CREATE MASKING POLICY ${MASK_POLICY} AS (val STRING) RETURNS STRING -> '***'"
quiet_stmt "ALTER TABLE ${DB}.t_mask MODIFY COLUMN c SET MASKING POLICY ${MASK_POLICY}"
comment "time travel masking table at saved snapshot"
echo "SELECT count(*)=1 FROM ${DB}.t_mask AT (SNAPSHOT => '$MASK_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT || exit 1
comment "flashback masking table to saved snapshot should fail"
echo ">>>> ALTER TABLE ${DB}.t_mask FLASHBACK TO (SNAPSHOT => '<saved_snapshot>')"
echo "ALTER TABLE ${DB}.t_mask FLASHBACK TO (SNAPSHOT => '$MASK_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT > /dev/null 2>&1
if [ $? -eq 0 ]; then
    exit 1
fi
echo "<<<< expected failure happened"

stmt "CREATE TABLE ${DB}.t_idx(a INT, c INT) STORAGE_FORMAT = 'parquet'"
stmt "INSERT INTO ${DB}.t_idx VALUES (1, 1), (2, 2)"
IDX_SNAPSHOT_ID=$(snapshot_id "${DB}" "t_idx" 2)
stmt "ALTER TABLE ${DB}.t_idx ADD COLUMN b INT"
stmt "INSERT INTO ${DB}.t_idx VALUES (1, 1, 4), (1, 2, 1), (1, 2, 4), (2, 2, 5)"
comment "create and refresh aggregating index"
quiet_stmt "CREATE AGGREGATING INDEX ${AGG_INDEX} AS SELECT a, b FROM ${DB}.t_idx WHERE b > 1"
quiet_stmt "REFRESH AGGREGATING INDEX ${AGG_INDEX}"
comment "flashback should reject aggregating indexes bound to dropped columns"
comment "flashback indexed table to saved snapshot should fail"
echo ">>>> ALTER TABLE ${DB}.t_idx FLASHBACK TO (SNAPSHOT => '<saved_snapshot>')"
echo "ALTER TABLE ${DB}.t_idx FLASHBACK TO (SNAPSHOT => '$IDX_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT > /dev/null 2>&1
if [ $? -eq 0 ]; then
    exit 1
fi
echo "<<<< expected failure happened"

comment "cleanup ee flashback metadata regression environment"
stmt "DROP TABLE IF EXISTS ${DB}.t_idx"
stmt "DROP TABLE IF EXISTS ${DB}.t_mask"
stmt "DROP TABLE IF EXISTS ${DB}.t_row"
stmt "DROP DATABASE IF EXISTS ${DB}"
quiet_stmt "DROP MASKING POLICY IF EXISTS ${MASK_POLICY}"
quiet_stmt "DROP ROW ACCESS POLICY IF EXISTS ${ROW_POLICY}"
quiet_stmt "DROP AGGREGATING INDEX ${AGG_INDEX}"
stmt "SET GLOBAL enable_experimental_row_access_policy = 0"
