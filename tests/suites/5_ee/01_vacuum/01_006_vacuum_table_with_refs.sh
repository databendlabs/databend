#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "create or replace database test_vacuum_orphan_refs"

mkdir -p /tmp/test_vacuum_orphan_refs/

stmt "create or replace table test_vacuum_orphan_refs.a(c int) 'fs:///tmp/test_vacuum_orphan_refs/'"

stmt "insert into test_vacuum_orphan_refs.a values (1),(2)"
stmt "set enable_experimental_table_ref=1; alter table test_vacuum_orphan_refs.a create tag t_live"
stmt "insert into test_vacuum_orphan_refs.a values (2),(3)"
stmt "optimize table test_vacuum_orphan_refs.a compact"

stmt "set enable_experimental_table_ref=1; alter table test_vacuum_orphan_refs.a create branch b_live"

stmt "set enable_experimental_table_ref=1; alter table test_vacuum_orphan_refs.a create branch b_exp retain 0 seconds"
stmt "set enable_experimental_table_ref=1; alter table test_vacuum_orphan_refs.a create tag t_exp retain 0 seconds"

SNAPSHOT_LOCATION=$(echo "select snapshot_location from fuse_snapshot('test_vacuum_orphan_refs','a') limit 1" | $BENDSQL_CLIENT_CONNECT)
PREFIX=$(echo "$SNAPSHOT_LOCATION" | cut -d'/' -f1)

echo "before vacuum"
ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/ | wc -l
stmt "set enable_experimental_table_ref=1; select c from test_vacuum_orphan_refs.a/b_live order by c"
stmt "set enable_experimental_table_ref=1; select c from test_vacuum_orphan_refs.a at(tag => t_live) order by c"
stmt_fail "set enable_experimental_table_ref=1; alter table test_vacuum_orphan_refs.a create branch b_exp"
stmt_fail "set enable_experimental_table_ref=1; alter table test_vacuum_orphan_refs.a create tag t_exp"

echo "first vacuum"
stmt "set data_retention_time_in_days=0; select * from fuse_vacuum2('test_vacuum_orphan_refs','a');" > /dev/null

echo "after vacuum"
ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/ | wc -l
stmt "set enable_experimental_table_ref=1; select c from test_vacuum_orphan_refs.a/b_live order by c"
stmt "set enable_experimental_table_ref=1; select c from test_vacuum_orphan_refs.a at(tag => t_live) order by c"

echo "second vacuum"
stmt "set data_retention_time_in_days=0; select * from fuse_vacuum2('test_vacuum_orphan_refs','a');" > /dev/null
ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/ | wc -l
