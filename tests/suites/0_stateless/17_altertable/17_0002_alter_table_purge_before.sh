#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh



# PURGE BEFORE SNAPSHOT

## Setup
echo "create table t17_0002(c int not null)" | bendsql_connect_root
## - 1st snapshot contains 2 rows, 1 block, 1 segment
echo "insert into t17_0002 values(1),(2)" | bendsql_connect_root
## - 2nd snapshot contains 3 rows, 2 blocks, 2 segments
echo "insert into t17_0002 values(3)" | bendsql_connect_root
## - 3rd snapshot contains 4 rows, 3 blocks, 3 segments
echo "insert into t17_0002 values(4)" | bendsql_connect_root

echo "checking that there should are 3 snapshots before purge"
echo "select count(*)=3  from fuse_snapshot('default', 't17_0002')" | bendsql_connect_root

## location the id of 2nd snapshot
SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t17_0002') where row_count=3 limit 1" | bendsql_connect_root)
#TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't17_0002') where row_count=3" | bendsql_connect_root)

# alter table add a column
echo "alter table add a column"
# alter table will generate a new snapshot
echo "alter table t17_0002 add column a float default 1.01" | bendsql_connect_root

## verify
echo "set data_retention_time_in_days=0; optimize table t17_0002 purge before (snapshot => '$SNAPSHOT_ID')" | bendsql_connect_root
echo "checking that after purge (by snapshot id) there should be 3 snapshots left"
echo "select count(*)=3  from fuse_snapshot('default', 't17_0002')" | bendsql_connect_root
echo "checking that after purge (by snapshot id) there should be 4 rows left"
echo "select count(*)=4  from t17_0002" | bendsql_connect_root

# alter table drop a column
echo "alter table drop a column"
# alter table will generate a new snapshot
echo "alter table t17_0002 drop column c" | bendsql_connect_root

## verify
echo "set data_retention_time_in_days=0; optimize table t17_0002 purge before (snapshot => '$SNAPSHOT_ID')" | bendsql_connect_root
echo "checking that after purge (by snapshot id) there should be 4 snapshots left"
echo "select count(*)=4  from fuse_snapshot('default', 't17_0002')" | bendsql_connect_root
echo "checking that after purge (by snapshot id) there should be 4 rows left"
echo "select count(*)=4  from t17_0002" | bendsql_connect_root

## Drop table.
echo "drop table t17_0002 all" | bendsql_connect_root

# PURGE BEFORE TIMESTAMP

## Setup
echo "create table t17_0002(c int not null)" | bendsql_connect_root
## - 1st snapshot contains 2 rows, 1 block, 1 segment
echo "insert into t17_0002 values(1),(2)" | bendsql_connect_root
## - 2nd snapshot contains 3 rows, 2 blocks, 2 segments
echo "insert into t17_0002 values(3)" | bendsql_connect_root
## - 3rd snapshot contains 4 rows, 3 blocks, 3 segments
echo "insert into t17_0002 values(4)" | bendsql_connect_root
## - 4rd snapshot contains 5 rows, 4 blocks, 4 segments
echo "insert into t17_0002 values(5)" | bendsql_connect_root

echo "checking that there should are 4 snapshots before purge"
echo "select count(*)=4  from fuse_snapshot('default', 't17_0002')" | bendsql_connect_root

## location the timestamp of latest snapshot
TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't17_0002') where row_count=5 limit 1" | bendsql_connect_root)

# alter table add a column
echo "alter table add a column"
echo "alter table t17_0002 add column a float default 1.01" | bendsql_connect_root

## verify
echo "set data_retention_time_in_days=0; optimize table t17_0002 purge before (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP)" | bendsql_connect_root
echo "checking that after purge (by timestamp) there should be at least 2 snapshots left"
echo "select count(*)>=2  from fuse_snapshot('default', 't17_0002')" | bendsql_connect_root
echo "checking that after purge (by timestamp) there should be 5 rows left"
echo "select count(*)=5  from t17_0002" | bendsql_connect_root

# alter table drop a column
echo "alter table drop a column"
echo "alter table t17_0002 drop column a" | bendsql_connect_root

## verify
echo "set data_retention_time_in_days=0; optimize table t17_0002 purge before (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP)" | bendsql_connect_root
echo "checking that after purge (by timestamp) there should be at least 2 snapshots left"
echo "select count(*)>=2  from fuse_snapshot('default', 't17_0002')" | bendsql_connect_root
echo "checking that after purge (by timestamp) there should be 5 rows left"
echo "select count(*)=5  from t17_0002" | bendsql_connect_root

## Drop table.
echo "drop table t17_0002 all" | bendsql_connect_root
