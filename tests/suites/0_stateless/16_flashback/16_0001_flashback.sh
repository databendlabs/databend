#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "create table t16(c int not null)" | $BENDSQL_CLIENT_CONNECT
# the first snapshot contains 2 rows
echo "insert into t16 values(1),(2)" | $BENDSQL_CLIENT_OUTPUT_NULL

# the second(last) snapshot should contain 3 rows
echo "insert into t16 values(3)" | $BENDSQL_CLIENT_OUTPUT_NULL

# flash back to the second(last) snapshot should be ok, and have no effects
SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t16') where row_count=3" | $BENDSQL_CLIENT_CONNECT)
TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default', 't16') where row_count=3" | $BENDSQL_CLIENT_CONNECT)

echo "alter table t16 flashback to (snapshot => '$SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT
echo "checking that after flashback to snapshot, there should be 3 rows"
echo "select count(*)=3  from t16" | $BENDSQL_CLIENT_CONNECT
echo "checking that after flashback to the same snapshot, no new snapshots shall be generated"
echo "select count(*)=2  from fuse_snapshot('default', 't16')" | $BENDSQL_CLIENT_CONNECT

echo "alter table t16 flashback to (TIMESTAMP => '$TIMEPOINT'::TIMESTAMP)" | $BENDSQL_CLIENT_CONNECT
echo "checking that after flashback to timestamp, there should be 3 rows"
echo "select count(*)=3  from t16" | $BENDSQL_CLIENT_CONNECT

echo "checking that after flashback to the same timestamp, number of snapshots shall be the same"
echo "select count(*)=2  from fuse_snapshot('default', 't16')" | $BENDSQL_CLIENT_CONNECT

# flash back to the first snapshot
FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t16') where row_count=2" | $BENDSQL_CLIENT_CONNECT)
echo "alter table t16 flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT

echo "checking that after flashback to the first snapshot, there should be 2 rows"
echo "select count(*)=2  from t16" | $BENDSQL_CLIENT_CONNECT

echo "checking that after flashback to the first snapshot, there should be only 1 snapshot visible"
echo "select count(*)=1  from fuse_snapshot('default', 't16')" | $BENDSQL_CLIENT_CONNECT

# flash back to point that not exist should fail
echo "flash back to snapshot id that not exist should report error 1105"
echo "alter table t16 flashback to (snapshot => 'NOTE_EXIST')" | $BENDSQL_CLIENT_CONNECT

echo "flash back to timestamp that not exist should report error 1105"
echo "alter table t16 flashback to (TIMESTAMP => '2000-12-06 04:35:17.856848'::TIMESTAMP)" | $BENDSQL_CLIENT_CONNECT;

# flash back to point that does not visible to the current snapshot will also fail
#  although $SNAPSHOT_ID has been in the history of table `t16`, but
#  after reverted to the $FST_SNAPSHOT_ID, it no longer visible to the table `t16`
echo "flash back to point that does not visible to the current snapshot should report error 1105"
echo "alter table t16 flashback to (snapshot => '$SNAPSHOT_ID')" | $BENDSQL_CLIENT_CONNECT


## Drop table.
echo "drop table t16" | $BENDSQL_CLIENT_CONNECT
