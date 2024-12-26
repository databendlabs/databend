#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace table t(a int not null)" | $BENDSQL_CLIENT_CONNECT
echo "insert into t values(1)" | $BENDSQL_CLIENT_CONNECT

# get snapshot location
SNAPSHOT_LOCATION=$(echo "select _snapshot_name from t;" | $BENDSQL_CLIENT_CONNECT)

echo "create or replace table t2(a int not null)" | $BENDSQL_CLIENT_CONNECT
echo "alter table t2 set options(snapshot_location = '$SNAPSHOT_LOCATION',block_per_segment = 500)" | $BENDSQL_CLIENT_CONNECT
echo "alter table t2 set options(snapshot_loc = '$SNAPSHOT_LOCATION',block_per_segment = 500)" | $BENDSQL_CLIENT_CONNECT
echo "alter table t2 set options(extrenal_location = '$SNAPSHOT_LOCATION',block_per_segment = 500)" | $BENDSQL_CLIENT_CONNECT
echo "alter table t2 set options(block_per_segment = 500)" | $BENDSQL_CLIENT_CONNECT
echo "select * from t2;" | $BENDSQL_CLIENT_CONNECT
# valid key check
echo "alter table t2 set options(abc = '1')" | $BENDSQL_CLIENT_CONNECT
echo "alter table t2 set options(block_per_segment = 2000)" | $BENDSQL_CLIENT_CONNECT
echo "alter table t2 set options(storage_format = 'memory')" | $BENDSQL_CLIENT_CONNECT
echo "alter table t2 set options(bloom_index_columns = 'b')" | $BENDSQL_CLIENT_CONNECT

# valid bloom index column data type.
echo "create or replace table t3(a decimal(4,2) not null)" | $BENDSQL_CLIENT_CONNECT
echo "alter table t3 set options(bloom_index_columns = 'a')" | $BENDSQL_CLIENT_CONNECT

#drop table
echo "drop table if exists t" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists t2" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists t3" | $BENDSQL_CLIENT_CONNECT