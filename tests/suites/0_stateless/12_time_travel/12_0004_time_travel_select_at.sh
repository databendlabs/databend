#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


## Create table t12_0004
echo "create table t12_0004(c int)" | $BENDSQL_CLIENT_CONNECT
echo "two insertions"
echo "insert into t12_0004 values(1),(2)" | $BENDSQL_CLIENT_OUTPUT_NULL

# for at offset.
sleep 2

echo "insert into t12_0004 values(3)" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "latest snapshot should contain 3 rows"
echo "select count(*)  from t12_0004" | $BENDSQL_CLIENT_CONNECT

## Get the previous snapshot id of the latest snapshot
SNAPSHOT_ID=$(echo "select previous_snapshot_id from fuse_snapshot('default','t12_0004') where row_count=3 " | $BENDSQL_CLIENT_CONNECT)

echo "counting the data set of first insertion, which should contain 2 rows"
echo "select count(*) from t12_0004 at (snapshot => '$SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT

echo "counting the data set of first insertion, which should contain 2 rows"
echo "select count(t.c) from t12_0004 at (snapshot => '$SNAPSHOT_ID') as t" | $BENDSQL_CLIENT_CONNECT

# Get a time point at/after the first insertion.
TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't12_0004') where row_count=2" | $BENDSQL_CLIENT_CONNECT)

echo "counting the data set of first insertion by timestamp, which should contains 2 rows"
echo "select count(t.c) from t12_0004 at (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP) as t" | $BENDSQL_CLIENT_CONNECT

offset=$(( $(date -u +%s) - $(date -u -d "$TIMEPOINT" +%s) - 1 ))
echo "counting the data set of first insertion by offset, which should contains 2 rows"
echo "select count(t.c) from t12_0004 at (OFFSET => -$offset) as t" | $BENDSQL_CLIENT_CONNECT

## Drop table.
echo "drop table t12_0004" | $BENDSQL_CLIENT_CONNECT
