#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


## Create table t12_0005
echo "drop table if exists t12_0005" | $BENDSQL_CLIENT_CONNECT
echo "create table t12_0005(c int not null)" | $BENDSQL_CLIENT_CONNECT
echo "two insertions"
# the first snapshot contains 2 rows, 1 block
echo "insert into t12_0005 values(1),(2)" | $BENDSQL_CLIENT_CONNECT

# the second snapshot contains 3 rows, 2 blocks
echo "insert into t12_0005 values(3)" | $BENDSQL_CLIENT_CONNECT
echo "latest snapshot should contain 3 rows"
echo "select count(*)  from t12_0005" | $BENDSQL_CLIENT_CONNECT

# alter table add a column
echo "alter table add a column"
# alter table add a column will create a new snapshot (the latest snapshot)
echo "alter table t12_0005 add column a float default 1.01" | $BENDSQL_CLIENT_CONNECT

## Get the 3rd latest snapshot id of the latest snapshot
SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t12_0005') limit 1 offset 2" | $BENDSQL_CLIENT_CONNECT)

echo "counting the data set of first insertion, which should contain 2 rows"
echo "select count(*) from t12_0005 at (snapshot => '$SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT

echo "select the data set of first insertion, which should contain 2 rows"
echo "select * from t12_0005 at (snapshot => '$SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT

echo "counting the data set of first insertion, which should contain 2 rows"
echo "select count(t.c) from t12_0005 at (snapshot => '$SNAPSHOT_ID') as t" | $BENDSQL_CLIENT_CONNECT


# Get a time point at/after the first insertion.
TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't12_0005') where row_count=2" | $BENDSQL_CLIENT_CONNECT)

echo "counting the data set of first insertion by timestamp, which should contains 2 rows"
echo "select count(t.c) from t12_0005 at (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP) as t" | $BENDSQL_CLIENT_CONNECT

echo "select the data set of first insertion by timestamp, which should contains 2 rows"
echo "select * from t12_0005 at (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP) as t" | $BENDSQL_CLIENT_CONNECT

# alter table drop a column
echo "alter table drop a column"
echo "alter table t12_0005 drop column c" | $BENDSQL_CLIENT_CONNECT

## Get the previous snapshot id of the latest snapshot
S1=$(echo "select previous_snapshot_id from fuse_snapshot('default','t12_0005') where row_count=3 limit 1 offset 1" | $BENDSQL_CLIENT_CONNECT)
SNAPSHOT_ID=$(echo "select previous_snapshot_id from fuse_snapshot('default','t12_0005') where snapshot_id='${S1}'" | $BENDSQL_CLIENT_CONNECT)

echo "counting the data set of first insertion, which should contain 2 rows"
echo "select count(*) from t12_0005 at (snapshot => '$SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT

echo "select the data set of first insertion, which should contain 2 rows"
echo "select * from t12_0005 at (snapshot => '$SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT

echo "counting the data set of first insertion, which should contain 2 rows"
echo "select count(t.c) from t12_0005 at (snapshot => '$SNAPSHOT_ID') as t" | $BENDSQL_CLIENT_CONNECT


# Get a time point at/after the first insertion.
TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't12_0005') where row_count=2 limit 1" | $BENDSQL_CLIENT_CONNECT)

echo "counting the data set of first insertion by timestamp, which should contains 2 rows"
echo "select count(t.c) from t12_0005 at (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP) as t" | $BENDSQL_CLIENT_CONNECT

echo "select the data set of first insertion by timestamp, which should contains 2 rows"
echo "select * from t12_0005 at (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP) as t" | $BENDSQL_CLIENT_CONNECT

## Drop table.
#echo "drop table t12_0005" | $BENDSQL_CLIENT_CONNECT
