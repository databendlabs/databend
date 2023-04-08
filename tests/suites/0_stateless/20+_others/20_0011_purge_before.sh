#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh



# PURGE BEFORE SNAPSHOT

## Setup
echo "create table t20_0011(c int)" | $MYSQL_CLIENT_CONNECT
## - 1st snapshot contains 2 rows, 1 block, 1 segment
echo "insert into t20_0011 values(1),(2)" | $MYSQL_CLIENT_CONNECT
## - 2nd snapshot contains 3 rows, 2 blocks, 2 segments
echo "insert into t20_0011 values(3)" | $MYSQL_CLIENT_CONNECT
## - 3rd snapshot contains 4 rows, 3 blocks, 3 segments
echo "insert into t20_0011 values(4)" | $MYSQL_CLIENT_CONNECT

echo "checking that there should are 3 snapshots before purge"
echo "select count(*)=3  from fuse_snapshot('default', 't20_0011')" | $MYSQL_CLIENT_CONNECT

## location the id of 2nd snapshot
SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t20_0011') where row_count=3" | $MYSQL_CLIENT_CONNECT)
#TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't20_0011') where row_count=3" | $MYSQL_CLIENT_CONNECT)

## verify
echo "set retention_period=0; optimize table t20_0011 purge before (snapshot => '$SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT
echo "checking that after purge (by snapshot id) there should be 2 snapshots left"
echo "select count(*)=2  from fuse_snapshot('default', 't20_0011')" | $MYSQL_CLIENT_CONNECT
echo "checking that after purge (by snapshot id) there should be 4 rows left"
echo "select count(*)=4  from t20_0011" | $MYSQL_CLIENT_CONNECT

## Drop table.
echo "drop table t20_0011 all" | $MYSQL_CLIENT_CONNECT

# PURGE BEFORE TIMESTAMP

## Setup
echo "create table t20_0011(c int)" | $MYSQL_CLIENT_CONNECT
## - 1st snapshot contains 2 rows, 1 block, 1 segment
echo "insert into t20_0011 values(1),(2)" | $MYSQL_CLIENT_CONNECT
## - 2nd snapshot contains 3 rows, 2 blocks, 2 segments
echo "insert into t20_0011 values(3)" | $MYSQL_CLIENT_CONNECT
## - 3rd snapshot contains 4 rows, 3 blocks, 3 segments
echo "insert into t20_0011 values(4)" | $MYSQL_CLIENT_CONNECT

echo "checking that there should are 3 snapshots before purge"
echo "select count(*)=3  from fuse_snapshot('default', 't20_0011')" | $MYSQL_CLIENT_CONNECT

## location the timestamp of 2nd snapshot
TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't20_0011') where row_count=3" | $MYSQL_CLIENT_CONNECT)

## verify
echo "set retention_period=0; optimize table t20_0011 purge before (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP)" | $MYSQL_CLIENT_CONNECT
echo "checking that after purge (by timestamp) there should be 2 snapshots left"
echo "select count(*)=2  from fuse_snapshot('default', 't20_0011')" | $MYSQL_CLIENT_CONNECT
echo "checking that after purge (by timestamp) there should be 4 rows left"
echo "select count(*)=4  from t20_0011" | $MYSQL_CLIENT_CONNECT

## Drop table.
echo "drop table t20_0011 all" | $MYSQL_CLIENT_CONNECT
