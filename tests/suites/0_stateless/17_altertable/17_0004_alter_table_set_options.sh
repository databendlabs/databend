#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create table t(a int)" | $MYSQL_CLIENT_CONNECT
echo "insert into t values(1)" | $MYSQL_CLIENT_CONNECT

# get snapshot location
SNAPSHOT_LOCATION=$(echo "select _snapshot_name from t;" | $MYSQL_CLIENT_CONNECT)

echo "create table t2(a int)" | $MYSQL_CLIENT_CONNECT
echo "alter table t2 set options(snapshot_location = '$SNAPSHOT_LOCATION',block_per_segment = 500)" | $MYSQL_CLIENT_CONNECT
echo "select * from t2;" | $MYSQL_CLIENT_CONNECT
# valid key check
echo "alter table t2 set options(abc = '1')" | $MYSQL_CLIENT_CONNECT

#drop table
echo "drop table if exists t" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists t2" | $MYSQL_CLIENT_CONNECT