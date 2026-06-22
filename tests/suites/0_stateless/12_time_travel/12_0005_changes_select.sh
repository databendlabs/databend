#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


## Create or replace table t12_0005
echo "create or replace table t12_0005(a int, b int) change_tracking=true enable_auto_analyze=0" | bendsql_connect_root
echo "insert into t12_0005 values(1, 1),(2, 1)" | bendsql_connect_root_null

echo "update t12_0005 set b = 2 where a = 2" | bendsql_connect_root_null
echo "delete from t12_0005 where a = 1" | bendsql_connect_root_null
echo "insert into t12_0005 values(3, 3)" | bendsql_connect_root_null

echo "latest snapshot should contain 2 rows"
echo "select count(*) from t12_0005" | bendsql_connect_root

## Get the snapshot id of the oldest snapshot.
AT_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t12_0005') where previous_snapshot_id is null" | bendsql_connect_root)

## Get end the snapshot id after delete.
END_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t12_0005') where row_count=1" | bendsql_connect_root)

echo "changes default snapshot at the first insertion"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => default) at(snapshot => '$AT_SNAPSHOT_ID') order by a, b" | bendsql_connect_root

echo "changes default snapshot at the first insertion end the deletion"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => default) at(snapshot => '$AT_SNAPSHOT_ID') end(snapshot => '$END_SNAPSHOT_ID') order by a, b" | bendsql_connect_root

echo "changes append_only snapshot at the first insertion"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => append_only) at(snapshot => '$AT_SNAPSHOT_ID') order by a, b" | bendsql_connect_root

# Not find end point.
echo "not find end point"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => default) at(snapshot => '$END_SNAPSHOT_ID') end(snapshot => '$AT_SNAPSHOT_ID') order by a, b" | bendsql_connect_root

# Get a time point at/after the first insertion.
AT_TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default','t12_0005') where previous_snapshot_id is null" | bendsql_connect_root)

## Get a time point after delete.
END_TIMEPOINT=$(echo "select timestamp from fuse_snapshot('default','t12_0005') where row_count=1" | bendsql_connect_root)

echo "changes default timestamp at the first insertion end the deletion"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => default) at(timestamp => '$AT_TIMEPOINT'::TIMESTAMP) end(timestamp => '$END_TIMEPOINT'::TIMESTAMP) order by a, b" | bendsql_connect_root

echo "changes append_only timestamp at the first insertion"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => append_only) at(timestamp => '$AT_TIMEPOINT'::TIMESTAMP) order by a, b" | bendsql_connect_root

# Change tracking is disabled.
echo "alter table t12_0005 set options(change_tracking=false)" | bendsql_connect_root
echo "change tracking is disabled"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => append_only) at(snapshot => '$AT_SNAPSHOT_ID') order by a, b" | bendsql_connect_root

# Change tracking has been missing for the time range requested.
echo "alter table t12_0005 set options(change_tracking=true)" | bendsql_connect_root
echo "change tracking has been missing for the time range requested"
echo "select a, b, change\$action, change\$is_update from t12_0005 changes(information => append_only) at(snapshot => '$AT_SNAPSHOT_ID') order by a, b" | bendsql_connect_root

## Drop table.
echo "drop table t12_0005 all" | bendsql_connect_root
