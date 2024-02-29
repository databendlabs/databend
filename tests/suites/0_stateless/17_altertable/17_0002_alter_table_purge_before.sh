#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh



# PURGE BEFORE SNAPSHOT

## Setup
echo "create table t17_0002(c int not null)" | $BENDSQL_CLIENT_CONNECT
## - 1st snapshot contains 2 rows, 1 block, 1 segment
echo "insert into t17_0002 values(1),(2)" | $BENDSQL_CLIENT_CONNECT
## - 2nd snapshot contains 3 rows, 2 blocks, 2 segments
echo "insert into t17_0002 values(3)" | $BENDSQL_CLIENT_CONNECT
## - 3rd snapshot contains 4 rows, 3 blocks, 3 segments
echo "insert into t17_0002 values(4)" | $BENDSQL_CLIENT_CONNECT

echo "checking that there should are 3 snapshots before purge"
echo "select count(*)=3  from fuse_snapshot('default', 't17_0002')" | $BENDSQL_CLIENT_CONNECT

## location the id of 2nd snapshot
SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t17_0002') where row_count=3 limit 1" | $BENDSQL_CLIENT_CONNECT)
#TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't17_0002') where row_count=3" | $BENDSQL_CLIENT_CONNECT)

# alter table add a column
echo "alter table add a column"
# alter table will generate a new snapshot
echo "alter table t17_0002 add column a float default 1.01" | $BENDSQL_CLIENT_CONNECT

## verify
echo "set data_retention_time_in_days=0; optimize table t17_0002 purge before (snapshot => '$SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT
echo "checking that after purge (by snapshot id) there should be 3 snapshots left"
echo "select count(*)=3  from fuse_snapshot('default', 't17_0002')" | $BENDSQL_CLIENT_CONNECT
echo "checking that after purge (by snapshot id) there should be 4 rows left"
echo "select count(*)=4  from t17_0002" | $BENDSQL_CLIENT_CONNECT

# alter table drop a column
echo "alter table drop a column"
# alter table will generate a new snapshot
echo "alter table t17_0002 drop column c" | $BENDSQL_CLIENT_CONNECT

## verify
echo "set data_retention_time_in_days=0; optimize table t17_0002 purge before (snapshot => '$SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT
echo "checking that after purge (by snapshot id) there should be 4 snapshots left"
echo "select count(*)=4  from fuse_snapshot('default', 't17_0002')" | $BENDSQL_CLIENT_CONNECT
echo "checking that after purge (by snapshot id) there should be 4 rows left"
echo "select count(*)=4  from t17_0002" | $BENDSQL_CLIENT_CONNECT

## Drop table.
echo "drop table t17_0002 all" | $BENDSQL_CLIENT_CONNECT

# PURGE BEFORE TIMESTAMP

## Setup
echo "create table t17_0002(c int not null)" | $BENDSQL_CLIENT_CONNECT
## - 1st snapshot contains 2 rows, 1 block, 1 segment
echo "insert into t17_0002 values(1),(2)" | $BENDSQL_CLIENT_CONNECT
## - 2nd snapshot contains 3 rows, 2 blocks, 2 segments
echo "insert into t17_0002 values(3)" | $BENDSQL_CLIENT_CONNECT
## - 3rd snapshot contains 4 rows, 3 blocks, 3 segments
echo "insert into t17_0002 values(4)" | $BENDSQL_CLIENT_CONNECT
## - 4rd snapshot contains 5 rows, 4 blocks, 4 segments
echo "insert into t17_0002 values(5)" | $BENDSQL_CLIENT_CONNECT

echo "checking that there should are 4 snapshots before purge"
echo "select count(*)=4  from fuse_snapshot('default', 't17_0002')" | $BENDSQL_CLIENT_CONNECT

## location the timestamp of latest snapshot
TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't17_0002') where row_count=5 limit 1" | $BENDSQL_CLIENT_CONNECT)

# alter table add a column
echo "alter table add a column"
echo "alter table t17_0002 add column a float default 1.01" | $BENDSQL_CLIENT_CONNECT

## verify
echo "set data_retention_time_in_days=0; optimize table t17_0002 purge before (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP)" | $BENDSQL_CLIENT_CONNECT
echo "checking that after purge (by timestamp) there should be at least 2 snapshots left"
echo "select count(*)>=2  from fuse_snapshot('default', 't17_0002')" | $BENDSQL_CLIENT_CONNECT
echo "checking that after purge (by timestamp) there should be 5 rows left"
echo "select count(*)=5  from t17_0002" | $BENDSQL_CLIENT_CONNECT

# alter table drop a column
echo "alter table drop a column"
echo "alter table t17_0002 drop column a" | $BENDSQL_CLIENT_CONNECT

## verify
echo "set data_retention_time_in_days=0; optimize table t17_0002 purge before (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP)" | $BENDSQL_CLIENT_CONNECT
echo "checking that after purge (by timestamp) there should be at least 2 snapshots left"
echo "select count(*)>=2  from fuse_snapshot('default', 't17_0002')" | $BENDSQL_CLIENT_CONNECT
echo "checking that after purge (by timestamp) there should be 5 rows left"
echo "select count(*)=5  from t17_0002" | $BENDSQL_CLIENT_CONNECT

## Drop table.
echo "drop table t17_0002 all" | $BENDSQL_CLIENT_CONNECT
