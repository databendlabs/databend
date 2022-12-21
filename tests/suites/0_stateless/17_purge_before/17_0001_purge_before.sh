#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "create table t17_001(c int)" | $MYSQL_CLIENT_CONNECT

# setup
# - 1st snapshot contains 2 rows, 1 block, 1 segment
echo "insert into t17_001 values(1),(2)" | $MYSQL_CLIENT_CONNECT
# - 2nd snapshot contains 3 rows, 2 blocks, 2 segments
echo "insert into t17_001 values(3)" | $MYSQL_CLIENT_CONNECT
# - 3rd snapshot contains 4 rows, 3 blocks, 3 segments
echo "insert into t17_001 values(4)" | $MYSQL_CLIENT_CONNECT

# flash back to the second(last) snapshot should be ok, and have no effects
SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t17_001') where row_count=3" | $MYSQL_CLIENT_CONNECT)
TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't17_001') where row_count=3" | $MYSQL_CLIENT_CONNECT)

echo "optimize table t17_001 purge before (snapshot => '$SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT
echo "checking that after flashback to snapshot, there should be 3 rows"
echo "select count(*)=3  from t17_001" | $MYSQL_CLIENT_CONNECT
echo "checking that after flashback to the same snapshot, no new snapshots shall be generated"
echo "select count(*)=2  from fuse_snapshot('default', 't17_001')" | $MYSQL_CLIENT_CONNECT

echo "alter table t17_001 flashback to (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP)" | $MYSQL_CLIENT_CONNECT
echo "checking that after flashback to timestamp, there should be 3 rows"
echo "select count(*)=3  from t17_001" | $MYSQL_CLIENT_CONNECT

echo "checking that after flashback to the same timestamp, number of snapshots shall be the same"
echo "select count(*)=2  from fuse_snapshot('default', 't17_001')" | $MYSQL_CLIENT_CONNECT

# flash back to the first snapshot
FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t17_001') where row_count=2" | $MYSQL_CLIENT_CONNECT)
echo "alter table t17_001 flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "checking that after flashback to the first snapshot, there should be 2 rows"
echo "select count(*)=2  from t17_001" | $MYSQL_CLIENT_CONNECT

echo "checking that after flashback to the first snapshot, there should be only 1 snapshot visible"
echo "select count(*)=1  from fuse_snapshot('default', 't17_001')" | $MYSQL_CLIENT_CONNECT

# flash back to point that not exist should fail
echo "flash back to snapshot id that not exist should report error 1105"
echo "alter table t17_001 flashback to (snapshot => 'NOTE_EXIST')" | $MYSQL_CLIENT_CONNECT

echo "flash back to timestamp that not exist should report error 1105"
echo "alter table t17_001 flashback to (TIMESTAMP => '2000-12-06 04:35:17.856848'::TIMESTAMP)" | $MYSQL_CLIENT_CONNECT;

# flash back to point that does not visible to the current snapshot will also fail
#  although $SNAPSHOT_ID has been in the history of table `t17_001`, but
#  after reverted to the $FST_SNAPSHOT_ID, it no longer visible to the table `t17_001`
echo "flash back to point that does not visible to the current snapshot should report error 1105"
echo "alter table t17_001 flashback to (snapshot => '$SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT


## Drop table.
echo "drop table t17_001" | $MYSQL_CLIENT_CONNECT
