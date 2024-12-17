#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


## Create or replace table t12_0005
echo "create or replace table t12_0005(a int, b int) change_tracking=true" | $BENDSQL_CLIENT_CONNECT
echo "insert into t12_0005 values(1, 1),(2, 1)" | $BENDSQL_CLIENT_OUTPUT_NULL

echo "update t12_0005 set b = 2 where a = 2" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "delete from t12_0005 where a = 1" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "insert into t12_0005 values(3, 3)" | $BENDSQL_CLIENT_OUTPUT_NULL

echo "latest snapshot should contain 2 rows"
echo "select count(*) from t12_0005" | $BENDSQL_CLIENT_CONNECT

## Get the snapshot id of the oldest snapshot.
AT_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t12_0005') where previous_snapshot_id is null" | $BENDSQL_CLIENT_CONNECT)

## Get end the snapshot id after delete.
END_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t12_0005') where row_count=1" | $BENDSQL_CLIENT_CONNECT)

echo "changes default snapshot at the first insertion"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => default) at(snapshot => '$AT_SNAPSHOT_ID') order by a, b" | $BENDSQL_CLIENT_CONNECT

echo "changes default snapshot at the first insertion end the deletion"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => default) at(snapshot => '$AT_SNAPSHOT_ID') end(snapshot => '$END_SNAPSHOT_ID') order by a, b" | $BENDSQL_CLIENT_CONNECT

echo "changes append_only snapshot at the first insertion"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => append_only) at(snapshot => '$AT_SNAPSHOT_ID') order by a, b" | $BENDSQL_CLIENT_CONNECT

# Not find end point.
echo "not find end point"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => default) at(snapshot => '$END_SNAPSHOT_ID') end(snapshot => '$AT_SNAPSHOT_ID') order by a, b" | $BENDSQL_CLIENT_CONNECT

# Get a time point at/after the first insertion.
AT_TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default','t12_0005') where previous_snapshot_id is null" | $BENDSQL_CLIENT_CONNECT)

## Get a time point after delete.
END_TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default','t12_0005') where row_count=1" | $BENDSQL_CLIENT_CONNECT)

echo "changes default timestamp at the first insertion end the deletion"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => default) at(timestamp => '$AT_TIMEPOINT'::TIMESTAMP) end(timestamp => '$END_TIMEPOINT'::TIMESTAMP) order by a, b" | $BENDSQL_CLIENT_CONNECT

echo "changes append_only timestamp at the first insertion"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => append_only) at(timestamp => '$AT_TIMEPOINT'::TIMESTAMP) order by a, b" | $BENDSQL_CLIENT_CONNECT

# Change tracking is disabled.
echo "alter table t12_0005 set options(change_tracking=false)" | $BENDSQL_CLIENT_CONNECT
echo "change tracking is disabled"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => append_only) at(snapshot => '$AT_SNAPSHOT_ID') order by a, b" | $BENDSQL_CLIENT_CONNECT

# Change tracking has been missing for the time range requested.
echo "alter table t12_0005 set options(change_tracking=true)" | $BENDSQL_CLIENT_CONNECT
echo "change tracking has been missing for the time range requested"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => append_only) at(snapshot => '$AT_SNAPSHOT_ID') order by a, b" | $BENDSQL_CLIENT_CONNECT

## Drop table.
echo "drop table t12_0005 all" | $BENDSQL_CLIENT_CONNECT
