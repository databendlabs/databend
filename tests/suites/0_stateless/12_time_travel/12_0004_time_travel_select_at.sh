#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


## Create table t12_0004
echo "create table t12_0004(c int)" | bendsql_connect_root
echo "two insertions"
echo "insert into t12_0004 values(1),(2)" | bendsql_connect_root_null

# for at offset.
sleep 2

echo "insert into t12_0004 values(3)" | bendsql_connect_root_null
echo "latest snapshot should contain 3 rows"
echo "select count(*)  from t12_0004" | bendsql_connect_root

## Get the previous snapshot id of the latest snapshot
SNAPSHOT_ID=$(echo "select previous_snapshot_id from fuse_snapshot('default','t12_0004') where row_count=3 " | bendsql_connect_root)

echo "counting the data set of first insertion, which should contain 2 rows"
echo "select count(*) from t12_0004 at (snapshot => '$SNAPSHOT_ID')" | bendsql_connect_root

echo "counting the data set of first insertion, which should contain 2 rows"
echo "select count(t.c) from t12_0004 at (snapshot => '$SNAPSHOT_ID') as t" | bendsql_connect_root

# Get a time point at/after the first insertion.
TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't12_0004') where row_count=2" | bendsql_connect_root)

echo "counting the data set of first insertion by timestamp, which should contains 2 rows"
echo "select count(t.c) from t12_0004 at (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP) as t" | bendsql_connect_root

offset=$(( $(date -u +%s) - $(date -u -d "$TIMEPOINT" +%s) - 1 ))
echo "counting the data set of first insertion by offset, which should contains 2 rows"
echo "select count(t.c) from t12_0004 at (OFFSET => -$offset) as t" | bendsql_connect_root

## Drop table.
echo "drop table t12_0004" | bendsql_connect_root
