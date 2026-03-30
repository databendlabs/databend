#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

DB="db_16_0002"

snapshot_id() {
    echo "select snapshot_id from fuse_snapshot('$1', '$2') where row_count=$3 limit 1" | $BENDSQL_CLIENT_CONNECT
}

insert_stmt() {
    echo ">>>> $1"
    echo "$1" | $BENDSQL_CLIENT_OUTPUT_NULL
}

comment "prepare flashback metadata regression environment"
stmt "DROP DATABASE IF EXISTS ${DB}"
stmt "CREATE DATABASE ${DB}"

comment "flashback should clear bloom index options tied to dropped columns"
stmt "CREATE OR REPLACE TABLE ${DB}.t_bloom(a INT, b INT)"
insert_stmt "INSERT INTO ${DB}.t_bloom VALUES (1, 1)"
BLOOM_SNAPSHOT_ID=$(snapshot_id "${DB}" "t_bloom" 1)
stmt "ALTER TABLE ${DB}.t_bloom ADD COLUMN c INT"
stmt "ALTER TABLE ${DB}.t_bloom SET OPTIONS(bloom_index_columns = 'a, c')"
insert_stmt "INSERT INTO ${DB}.t_bloom VALUES (2, 2, 2)"
comment "flashback bloom table to saved snapshot"
echo "ALTER TABLE ${DB}.t_bloom FLASHBACK TO (SNAPSHOT => '$BLOOM_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT > /dev/null || exit 1
query "SELECT table_option NOT LIKE '%BLOOM_INDEX_COLUMNS=%' FROM system.tables WHERE database = '${DB}' AND name = 't_bloom'"
insert_stmt "INSERT INTO ${DB}.t_bloom VALUES (3, 3)"
query "SELECT count(*)=2 FROM ${DB}.t_bloom"

comment "flashback should preserve still-compatible bloom index options"
stmt "CREATE OR REPLACE TABLE ${DB}.t_bloom_keep(a INT, b INT)"
insert_stmt "INSERT INTO ${DB}.t_bloom_keep VALUES (1, 1)"
BLOOM_KEEP_SNAPSHOT_ID=$(snapshot_id "${DB}" "t_bloom_keep" 1)
stmt "ALTER TABLE ${DB}.t_bloom_keep ADD COLUMN c INT"
stmt "ALTER TABLE ${DB}.t_bloom_keep SET OPTIONS(bloom_index_columns = 'a')"
insert_stmt "INSERT INTO ${DB}.t_bloom_keep VALUES (2, 2, 2)"
comment "flashback compatible bloom table to saved snapshot"
echo "ALTER TABLE ${DB}.t_bloom_keep FLASHBACK TO (SNAPSHOT => '$BLOOM_KEEP_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT > /dev/null || exit 1
query "SELECT table_option LIKE '%BLOOM_INDEX_COLUMNS=%' FROM system.tables WHERE database = '${DB}' AND name = 't_bloom_keep'"
insert_stmt "INSERT INTO ${DB}.t_bloom_keep VALUES (3, 3)"
query "SELECT count(*)=2 FROM ${DB}.t_bloom_keep"

comment "flashback should preserve still-compatible approx distinct options"
stmt "CREATE OR REPLACE TABLE ${DB}.t_hll_keep(a INT, b INT)"
insert_stmt "INSERT INTO ${DB}.t_hll_keep VALUES (1, 1)"
HLL_KEEP_SNAPSHOT_ID=$(snapshot_id "${DB}" "t_hll_keep" 1)
stmt "ALTER TABLE ${DB}.t_hll_keep ADD COLUMN c INT"
stmt "ALTER TABLE ${DB}.t_hll_keep SET OPTIONS(approx_distinct_columns = 'a')"
insert_stmt "INSERT INTO ${DB}.t_hll_keep VALUES (2, 2, 2)"
comment "flashback compatible approx distinct table to saved snapshot"
echo "ALTER TABLE ${DB}.t_hll_keep FLASHBACK TO (SNAPSHOT => '$HLL_KEEP_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT > /dev/null || exit 1
query "SELECT table_option LIKE '%APPROX_DISTINCT_COLUMNS=%' FROM system.tables WHERE database = '${DB}' AND name = 't_hll_keep'"
insert_stmt "INSERT INTO ${DB}.t_hll_keep VALUES (3, 3)"
query "SELECT count(*)=2 FROM ${DB}.t_hll_keep"

comment "flashback should preserve explicit bloom none option"
stmt "CREATE OR REPLACE TABLE ${DB}.t_bloom_none(a INT, b INT)"
insert_stmt "INSERT INTO ${DB}.t_bloom_none VALUES (1, 1)"
BLOOM_NONE_SNAPSHOT_ID=$(snapshot_id "${DB}" "t_bloom_none" 1)
stmt "ALTER TABLE ${DB}.t_bloom_none ADD COLUMN c INT"
stmt "ALTER TABLE ${DB}.t_bloom_none SET OPTIONS(bloom_index_columns = '')"
insert_stmt "INSERT INTO ${DB}.t_bloom_none VALUES (2, 2, 2)"
comment "flashback bloom none table to saved snapshot"
echo "ALTER TABLE ${DB}.t_bloom_none FLASHBACK TO (SNAPSHOT => '$BLOOM_NONE_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT > /dev/null || exit 1
query "SELECT table_option LIKE '%BLOOM_INDEX_COLUMNS=%' FROM system.tables WHERE database = '${DB}' AND name = 't_bloom_none'"
insert_stmt "INSERT INTO ${DB}.t_bloom_none VALUES (3, 3)"
query "SELECT count(*)=2 FROM ${DB}.t_bloom_none"

comment "flashback should preserve explicit approx distinct none option"
stmt "CREATE OR REPLACE TABLE ${DB}.t_hll_none(a INT, b INT)"
insert_stmt "INSERT INTO ${DB}.t_hll_none VALUES (1, 1)"
HLL_NONE_SNAPSHOT_ID=$(snapshot_id "${DB}" "t_hll_none" 1)
stmt "ALTER TABLE ${DB}.t_hll_none ADD COLUMN c INT"
stmt "ALTER TABLE ${DB}.t_hll_none SET OPTIONS(approx_distinct_columns = '')"
insert_stmt "INSERT INTO ${DB}.t_hll_none VALUES (2, 2, 2)"
comment "flashback approx distinct none table to saved snapshot"
echo "ALTER TABLE ${DB}.t_hll_none FLASHBACK TO (SNAPSHOT => '$HLL_NONE_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT > /dev/null || exit 1
query "SELECT table_option LIKE '%APPROX_DISTINCT_COLUMNS=%' FROM system.tables WHERE database = '${DB}' AND name = 't_hll_none'"
insert_stmt "INSERT INTO ${DB}.t_hll_none VALUES (3, 3)"
query "SELECT count(*)=2 FROM ${DB}.t_hll_none"

comment "flashback should clear approx distinct options tied to dropped columns"
stmt "CREATE OR REPLACE TABLE ${DB}.t_hll(a INT, b INT)"
insert_stmt "INSERT INTO ${DB}.t_hll VALUES (1, 1)"
HLL_SNAPSHOT_ID=$(snapshot_id "${DB}" "t_hll" 1)
stmt "ALTER TABLE ${DB}.t_hll ADD COLUMN c INT"
stmt "ALTER TABLE ${DB}.t_hll SET OPTIONS(approx_distinct_columns = 'a, c')"
insert_stmt "INSERT INTO ${DB}.t_hll VALUES (2, 2, 2)"
comment "flashback approx distinct table to saved snapshot"
echo "ALTER TABLE ${DB}.t_hll FLASHBACK TO (SNAPSHOT => '$HLL_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT > /dev/null || exit 1
query "SELECT table_option NOT LIKE '%APPROX_DISTINCT_COLUMNS=%' FROM system.tables WHERE database = '${DB}' AND name = 't_hll'"
insert_stmt "INSERT INTO ${DB}.t_hll VALUES (3, 3)"
query "SELECT count(*)=2 FROM ${DB}.t_hll"

comment "flashback should clear cluster metadata tied to dropped columns"
stmt "CREATE OR REPLACE TABLE ${DB}.t_ck(a INT, b INT)"
insert_stmt "INSERT INTO ${DB}.t_ck VALUES (1, 1)"
CK_SNAPSHOT_ID=$(snapshot_id "${DB}" "t_ck" 1)
stmt "ALTER TABLE ${DB}.t_ck ADD COLUMN c INT"
insert_stmt "INSERT INTO ${DB}.t_ck VALUES (2, 2, 2)"
stmt "ALTER TABLE ${DB}.t_ck CLUSTER BY (c, a)"
comment "flashback cluster table to saved snapshot"
echo "ALTER TABLE ${DB}.t_ck FLASHBACK TO (SNAPSHOT => '$CK_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT > /dev/null || exit 1
query "SELECT table_option NOT LIKE '%CLUSTER_TYPE=%' FROM system.tables WHERE database = '${DB}' AND name = 't_ck'"

comment "flashback should reject constraints tied to dropped columns"
stmt "CREATE OR REPLACE TABLE ${DB}.t_constraint(a INT, b INT)"
insert_stmt "INSERT INTO ${DB}.t_constraint VALUES (1, 1)"
CONSTRAINT_SNAPSHOT_ID=$(snapshot_id "${DB}" "t_constraint" 1)
stmt "ALTER TABLE ${DB}.t_constraint ADD COLUMN c INT"
stmt "ALTER TABLE ${DB}.t_constraint ADD CONSTRAINT c_positive CHECK (c > 0)"
insert_stmt "INSERT INTO ${DB}.t_constraint VALUES (2, 2, 2)"
stmt_fail "INSERT INTO ${DB}.t_constraint VALUES (3, 3, -1)"
comment "flashback constrained table to saved snapshot should fail"
echo ">>>> ALTER TABLE ${DB}.t_constraint FLASHBACK TO (SNAPSHOT => '<saved_snapshot>')"
echo "ALTER TABLE ${DB}.t_constraint FLASHBACK TO (SNAPSHOT => '$CONSTRAINT_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT > /dev/null 2>&1
if [ $? -eq 0 ]; then
    exit 1
fi
echo "<<<< expected failure happened"

comment "cleanup flashback metadata regression environment"
stmt "DROP DATABASE ${DB}"
