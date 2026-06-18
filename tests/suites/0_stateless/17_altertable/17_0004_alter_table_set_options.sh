#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace table t(a int not null)" | bendsql_connect_root
echo "insert into t values(1)" | bendsql_connect_root

# get snapshot location
SNAPSHOT_LOCATION=$(echo "select _snapshot_name from t;" | bendsql_connect_root)

echo "create or replace table t2(a int not null)" | bendsql_connect_root
echo "alter table t2 set options(snapshot_location = '$SNAPSHOT_LOCATION',block_per_segment = 500)" | bendsql_connect_root
echo "alter table t2 set options(snapshot_loc = '$SNAPSHOT_LOCATION',block_per_segment = 500)" | bendsql_connect_root
echo "alter table t2 set options(extrenal_location = '$SNAPSHOT_LOCATION',block_per_segment = 500)" | bendsql_connect_root
echo "alter table t2 set options(block_per_segment = 500)" | bendsql_connect_root
echo "select * from t2;" | bendsql_connect_root
# valid key check
echo "alter table t2 set options(abc = '1')" | bendsql_connect_root
echo "alter table t2 set options(block_per_segment = 2000)" | bendsql_connect_root
echo "alter table t2 set options(storage_format = 'memory')" | bendsql_connect_root
echo "alter table t2 set options(bloom_index_columns = 'b')" | bendsql_connect_root
echo "alter table t2 set options(bloom_index_type = 'binary_fuse32')" | bendsql_connect_root
echo "alter table t2 set options(bloom_index_type = 'nope')" | bendsql_connect_root

# valid bloom index column data type.
echo "create or replace table t3(a decimal(4,2) not null)" | bendsql_connect_root
echo "alter table t3 set options(bloom_index_columns = 'a')" | bendsql_connect_root
echo "create or replace table t4(a int not null) bloom_index_type = 'binary_fuse32'" | bendsql_connect_root

#drop table
echo "drop table if exists t" | bendsql_connect_root
echo "drop table if exists t2" | bendsql_connect_root
echo "drop table if exists t3" | bendsql_connect_root
echo "drop table if exists t4" | bendsql_connect_root
