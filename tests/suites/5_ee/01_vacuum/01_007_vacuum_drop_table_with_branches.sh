#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

mkdir -p /tmp/test_vacuum_drop_table_with_branches/

stmt "create or replace database test_vacuum_drop_table_with_branches"

stmt "create or replace table test_vacuum_drop_table_with_branches.a(c int) 'fs:///tmp/test_vacuum_drop_table_with_branches/'"
stmt "insert into test_vacuum_drop_table_with_branches.a values (1),(2)"
SNAPSHOT_LOCATION=$(echo "select snapshot_location from fuse_snapshot('test_vacuum_drop_table_with_branches','a') limit 1" | $BENDSQL_CLIENT_CONNECT)
PREFIX=$(echo "$SNAPSHOT_LOCATION" | cut -d'/' -f1)

stmt "set enable_experimental_table_ref=1; alter table test_vacuum_drop_table_with_branches.a create branch b_live"
stmt "set enable_experimental_table_ref=1; alter table test_vacuum_drop_table_with_branches.a create branch b_mixed"
stmt "set enable_experimental_table_ref=1; alter table test_vacuum_drop_table_with_branches.a drop branch b_mixed"
stmt "set enable_experimental_table_ref=1; alter table test_vacuum_drop_table_with_branches.a create branch b_mixed"
stmt "set enable_experimental_table_ref=1; alter table test_vacuum_drop_table_with_branches.a create branch b_drop"
stmt "set enable_experimental_table_ref=1; alter table test_vacuum_drop_table_with_branches.a drop branch b_drop"

echo "before vacuum drop table"
ls -l /tmp/test_vacuum_drop_table_with_branches/"$PREFIX"/ | wc -l
stmt "select c from test_vacuum_drop_table_with_branches.a/b_live order by c"
stmt "select c from test_vacuum_drop_table_with_branches.a/b_mixed order by c"
stmt_fail "select c from test_vacuum_drop_table_with_branches.a/b_drop order by c"

stmt "drop table test_vacuum_drop_table_with_branches.a"
stmt "set data_retention_time_in_days=0; vacuum drop table from test_vacuum_drop_table_with_branches" >/dev/null

echo "after vacuum drop table"
ls -l /tmp/test_vacuum_drop_table_with_branches/"$PREFIX"/ | wc -l
stmt_fail "undrop table test_vacuum_drop_table_with_branches.a"
