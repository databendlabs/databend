#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "create or replace database test_vacuum_table_only_orphans"

mkdir -p /tmp/test_vacuum_table_only_orphans/

stmt "create or replace table test_vacuum_table_only_orphans.a(c int) 'fs:///tmp/test_vacuum_table_only_orphans/'"


stmt "insert into test_vacuum_table_only_orphans.a values (1)"
stmt "insert into test_vacuum_table_only_orphans.a values (2)"
stmt "insert into test_vacuum_table_only_orphans.a values (3)"

SNAPSHOT_LOCATION=$(echo "select snapshot_location from fuse_snapshot('test_vacuum_table_only_orphans','a') limit 1" | $BENDSQL_CLIENT_CONNECT)
PREFIX=$(echo "$SNAPSHOT_LOCATION" | cut -d'/' -f1-2)

echo "before purge"

ls -l /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_b/ | wc -l
ls -l /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_sg/ | wc -l
ls -l /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_i_b_v2/ | wc -l


stmt "truncate table test_vacuum_table_only_orphans.a"
stmt "truncate table test_vacuum_table_only_orphans.a"
stmt "truncate table test_vacuum_table_only_orphans.a"

stmt "set data_retention_time_in_days=0; optimize table test_vacuum_table_only_orphans.a purge"

echo "after purge"

ls -l /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_b/ | wc -l
ls -l /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_sg/ | wc -l
ls -l /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_i_b_v2/ | wc -l


# simulates orphans
touch /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_b/o1
touch /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_b/o2
touch /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_b/o3

touch /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_sg/sg1
touch /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_sg/sg2
touch /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_sg/sg3

touch /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_i_b_v2/bf1
touch /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_i_b_v2/bf2
touch /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_i_b_v2/bf3

echo "after add pure orphan files"

ls -l /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_b/ | wc -l
ls -l /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_sg/ | wc -l
ls -l /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_i_b_v2/ | wc -l


stmt "set data_retention_time_in_days=0; vacuum table test_vacuum_table_only_orphans.a" > /dev/null

echo "after vacuum"

ls -l /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_b/ | wc -l
ls -l /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_sg/ | wc -l
ls -l /tmp/test_vacuum_table_only_orphans/"$PREFIX"/_i_b_v2/ | wc -l


