#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh



# PURGE BEFORE SNAPSHOT

## Setup
echo "create table t20_0011(c int not null)" | bendsql_connect_root
## - 1st snapshot contains 2 rows, 1 block, 1 segment
echo "insert into t20_0011 values(1),(2)" | bendsql_connect_root
## - 2nd snapshot contains 3 rows, 2 blocks, 2 segments
echo "insert into t20_0011 values(3)" | bendsql_connect_root
## - 3rd snapshot contains 4 rows, 3 blocks, 3 segments
echo "insert into t20_0011 values(4)" | bendsql_connect_root

echo "checking that there should are 3 snapshots before purge"
echo "select count(*)=3  from fuse_snapshot('default', 't20_0011')" | bendsql_connect_root

## location the id of 2nd snapshot
SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t20_0011') where row_count=3" | bendsql_connect_root)
#TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't20_0011') where row_count=3" | bendsql_connect_root)

## verify
echo "set data_retention_time_in_days=0; optimize table t20_0011 purge before (snapshot => '$SNAPSHOT_ID')" | bendsql_connect_root
echo "checking that after purge (by snapshot id) there should be 2 snapshots left"
echo "select count(*)=2  from fuse_snapshot('default', 't20_0011')" | bendsql_connect_root
echo "checking that after purge (by snapshot id) there should be 4 rows left"
echo "select count(*)=4  from t20_0011" | bendsql_connect_root

## Drop table.
echo "drop table t20_0011 all" | bendsql_connect_root

# PURGE BEFORE TIMESTAMP

## Setup
echo "create table t20_0011(c int not null)" | bendsql_connect_root
## - 1st snapshot contains 2 rows, 1 block, 1 segment
echo "insert into t20_0011 values(1),(2)" | bendsql_connect_root
## - 2nd snapshot contains 3 rows, 2 blocks, 2 segments
echo "insert into t20_0011 values(3)" | bendsql_connect_root
## - 3rd snapshot contains 4 rows, 3 blocks, 3 segments
echo "insert into t20_0011 values(4)" | bendsql_connect_root

echo "checking that there should are 3 snapshots before purge"
echo "select count(*)=3  from fuse_snapshot('default', 't20_0011')" | bendsql_connect_root

## location the timestamp of latest snapshot
TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't20_0011') where row_count=4" | bendsql_connect_root)

## verify
echo "set data_retention_time_in_days=0; optimize table t20_0011 purge before (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP)" | bendsql_connect_root
echo "checking that after purge (by timestamp) there should be 1 snapshot left"
echo "select count(*)=1  from fuse_snapshot('default', 't20_0011')" | bendsql_connect_root
echo "checking that after purge (by timestamp) there should be 4 rows left"
echo "select count(*)=4  from t20_0011" | bendsql_connect_root

## Drop table.
echo "drop table t20_0011 all" | bendsql_connect_root
