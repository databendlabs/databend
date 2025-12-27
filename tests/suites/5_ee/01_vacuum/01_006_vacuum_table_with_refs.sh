#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "create or replace database test_vacuum_orphan_refs"

mkdir -p /tmp/test_vacuum_orphan_refs/

stmt "create or replace table test_vacuum_orphan_refs.a(c int) 'fs:///tmp/test_vacuum_orphan_refs/'"

stmt "insert into test_vacuum_orphan_refs.a values (1),(2)"
stmt "insert into test_vacuum_orphan_refs.a values (2),(3)"
stmt "optimize table test_vacuum_orphan_refs.a compact"

FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('test_vacuum_orphan_refs','a') where row_count=2" | $BENDSQL_CLIENT_CONNECT)
echo "alter table test_vacuum_orphan_refs.a create branch b at(snapshot => '$FST_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT

SNAPSHOT_LOCATION=$(echo "select snapshot_location from fuse_snapshot('test_vacuum_orphan_refs','a') limit 1" | $BENDSQL_CLIENT_CONNECT)
PREFIX=$(echo "$SNAPSHOT_LOCATION" | cut -d'/' -f1-2)

echo "before vacuum"

ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/_b/ | wc -l
ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/_sg/ | wc -l
ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/_i_b_v2/ | wc -l
ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/_ref/ | wc -l

stmt "set data_retention_time_in_days=0; select * from fuse_vacuum2('test_vacuum_orphan_refs','a');" > /dev/null

echo "after vacuum"

ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/_b/ | wc -l
ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/_sg/ | wc -l
ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/_i_b_v2/ | wc -l
ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/_ref/ | wc -l

stmt "select c from test_vacuum_orphan_refs.a/b order by c"

# simulates orphans
mkdir -p /tmp/test_vacuum_orphan_refs/"$PREFIX"/_ref/1/
mkdir -p /tmp/test_vacuum_orphan_refs/"$PREFIX"/_ref/2/

echo "after add pure orphan files"
ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/_ref/ | wc -l

stmt "set data_retention_time_in_days=0; select * from fuse_vacuum2('test_vacuum_orphan_refs','a');" > /dev/null

echo "after vacuum"
ls -l /tmp/test_vacuum_orphan_refs/"$PREFIX"/_ref/ | wc -l
