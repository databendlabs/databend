#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


## Create table t12_0004
echo "create table t12_0004(c int)" | $MYSQL_CLIENT_CONNECT
echo "two insertions"
echo "insert into t12_0004 values(1),(2)" | $MYSQL_CLIENT_CONNECT

# Ensure there is a small interval between two insertions, for time travel
sleep 0.001

echo "insert into t12_0004 values(3)" | $MYSQL_CLIENT_CONNECT
echo "latest snapshot should contain 3 rows"
echo "select count(*)  from t12_0004" | $MYSQL_CLIENT_CONNECT

## Get the previous snapshot id of the latest snapshot
#SNAPSHOT_ID=$(echo "select previous_snapshot_id fuse_snapshot('default','t12_0004') where row_count=3 " | mysql -h127.0.0.1 -P3307 -uroot -s)
SNAPSHOT_ID=$(echo "select previous_snapshot_id from fuse_snapshot('default','t12_0004') where row_count=3 " | $MYSQL_CLIENT_CONNECT)

echo "counting the data set of first insertion, which should contains 2 rows"
echo "select count(*) from t12_0004 at (snapshot => '$SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "planner_v2: counting the data set of first insertion, which should contains 2 rows"
echo "set enable_planner_v2 = 1;select count(t.c) from t12_0004 at (snapshot => '$SNAPSHOT_ID') as t" | $MYSQL_CLIENT_CONNECT


# Get a time point at/after the first insertion.
# Cannot directly use the timestamp of first insertion, because the time precision of snapshot is higher than timestamp(6). So +1μs.

TIMEPOINT=$(echo "select cast(timestamp as BIGINT) + 1 from fuse_snapshot('default', 't12_0004') where row_count=2" | $MYSQL_CLIENT_CONNECT)

echo "planner_v2: counting the data set of fisrt insertion by timestamp, which should contains 2 rows"
echo "set enable_planner_v2 = 1;select count(t.c) from t12_0004 at (TIMESTAMP => $TIMEPOINT::TIMESTAMP) as t" | $MYSQL_CLIENT_CONNECT

## Drop table.
echo "drop table t12_0004" | $MYSQL_CLIENT_CONNECT
