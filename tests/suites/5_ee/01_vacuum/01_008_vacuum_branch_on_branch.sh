#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "create or replace database test_vacuum_branch_on_branch"
mkdir -p /tmp/test_vacuum_branch_on_branch/
stmt "create or replace table test_vacuum_branch_on_branch.a(c int) 'fs:///tmp/test_vacuum_branch_on_branch/'"
stmt "insert into test_vacuum_branch_on_branch.a values (1),(2)"
stmt "set enable_experimental_table_ref=1; alter table test_vacuum_branch_on_branch.a create branch b1"
stmt "insert into test_vacuum_branch_on_branch.a values (3)"
stmt "set enable_experimental_table_ref=1; alter table test_vacuum_branch_on_branch.a/b1 create branch b2"
stmt_fail "set enable_experimental_table_ref=1; alter table test_vacuum_branch_on_branch.a/b1 create tag t1"
stmt "set data_retention_time_in_days=0; select * from fuse_vacuum2('test_vacuum_branch_on_branch','a');" > /dev/null
stmt "set enable_experimental_table_ref=1; select c from test_vacuum_branch_on_branch.a/b1 order by c"
stmt "set enable_experimental_table_ref=1; select c from test_vacuum_branch_on_branch.a/b2 order by c"
