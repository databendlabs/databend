#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop database if exists db_stream" | bendsql_connect_root
echo "create database db_stream" | bendsql_connect_root
echo "create table db_stream.base(a int)" | bendsql_connect_root
echo "insert into db_stream.base values(1)" | bendsql_connect_root_null
echo "alter table db_stream.base set options(change_tracking = true)" | bendsql_connect_root
echo "insert into db_stream.base values(2)" | bendsql_connect_root_null
echo "insert into db_stream.base values(3)" | bendsql_connect_root_null

BASE_ROW_ID=$(echo "select _base_row_id from db_stream.base where a = 3" | bendsql_connect_root)

SNAPSHOT_ID_1=$(echo "select snapshot_id from fuse_snapshot('db_stream','base') where row_count=1 limit 1" | bendsql_connect_root)
echo "create stream db_stream.s1 on table db_stream.base at (snapshot => '$SNAPSHOT_ID_1')" | bendsql_connect_root

SNAPSHOT_ID_2=$(echo "select snapshot_id from fuse_snapshot('db_stream','base') where row_count=2 limit 1" | bendsql_connect_root)
echo "create stream db_stream.s2 on table db_stream.base at (snapshot => '$SNAPSHOT_ID_2')" | bendsql_connect_root
echo "select a, change\$action, change\$is_update, change\$row_id='$BASE_ROW_ID' from db_stream.s2" | bendsql_connect_root
echo "select a from db_stream.base at (stream => db_stream.s2) order by a" | bendsql_connect_root

TIMEPOINT_1=$(echo "select timestamp from fuse_snapshot('db_stream','base') where row_count=1 limit 1" | bendsql_connect_root)
echo "create stream db_stream.t1 on table db_stream.base at (timestamp => '$TIMEPOINT_1'::TIMESTAMP)" | bendsql_connect_root

echo "alter table db_stream.base set options(change_tracking = true)" | bendsql_connect_root
TIMEPOINT_2=$(echo "select timestamp from fuse_snapshot('db_stream','base') where row_count=2 limit 1" | bendsql_connect_root)
echo "create stream db_stream.t2 on table db_stream.base at (timestamp => '$TIMEPOINT_2'::TIMESTAMP)" | bendsql_connect_root
echo "select a, change\$action, change\$is_update, change\$row_id='$BASE_ROW_ID' from db_stream.t2" | bendsql_connect_root

echo "alter table db_stream.base set options(change_tracking = false)" | bendsql_connect_root
echo "create stream db_stream.s3 on table db_stream.base at (snapshot => '$SNAPSHOT_ID_2')" | bendsql_connect_root
echo "alter table db_stream.base set options(change_tracking = true)" | bendsql_connect_root
echo "create stream db_stream.s4 on table db_stream.base at (stream => db_stream.s2)" | bendsql_connect_root
echo "select a from db_stream.s2" | bendsql_connect_root

echo "select count(*) from fuse_snapshot('db_stream', 'base')" | bendsql_connect_root
echo "set data_retention_time_in_days=0; optimize table db_stream.base purge before (stream => db_stream.s2)" | bendsql_connect_root
echo "select count(*) from fuse_snapshot('db_stream', 'base')" | bendsql_connect_root

echo "drop stream if exists db_stream.s2" | bendsql_connect_root
echo "drop stream if exists db_stream.t2" | bendsql_connect_root
echo "drop table if exists db_stream.base all" | bendsql_connect_root
echo "drop database if exists db_stream" | bendsql_connect_root
