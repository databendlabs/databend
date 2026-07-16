#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

DATABASE=db_stream_recovery

echo "drop database if exists $DATABASE" | bendsql_connect_root_null
echo "create database $DATABASE" | bendsql_connect_root_null
echo "create table $DATABASE.base(id int, value int) change_tracking=true enable_auto_analyze=0" | bendsql_connect_root_null
echo "insert into $DATABASE.base values(1, 0)" | bendsql_connect_root_null
echo "update $DATABASE.base set value = 1 where id = 1" | bendsql_connect_root_null
BLOCK=$(echo "select _block_name from $DATABASE.base" | bendsql_connect_root)

echo "truncate table $DATABASE.base" | bendsql_connect_root_null
echo "create stream $DATABASE.recovered on table $DATABASE.base append_only=true" | bendsql_connect_root_null
echo "create table $DATABASE.sink(id int)" | bendsql_connect_root_null

# Origin fields in the UPDATE block must not leak into the rewritten INSERT.
echo "copy into $DATABASE.base from FUSE_RECOVERY_BLOCKS(FILES => ('$BLOCK')) force = false" | bendsql_connect_root_null
echo "recovery is a new stream insert"
echo "select id, change\$action, change\$is_update from $DATABASE.recovered" | bendsql_connect_root

# Consume the stream, then verify an aborted recovery batch does not advance it.
echo "insert into $DATABASE.sink select id from $DATABASE.recovered" | bendsql_connect_root_null
MISSING="${BLOCK%/*}/missing_v2.parquet"
if echo "copy into $DATABASE.base from FUSE_RECOVERY_BLOCKS(FILES => ('$BLOCK', '$MISSING')) force = false" | bendsql_connect_root_null >/dev/null 2>&1; then
	echo "missing file unexpectedly succeeded"
else
	echo "missing file aborted stream batch"
fi
echo "select count() from $DATABASE.recovered" | bendsql_connect_root

echo "drop stream $DATABASE.recovered" | bendsql_connect_root_null
echo "drop table $DATABASE.sink all" | bendsql_connect_root_null
echo "drop table $DATABASE.base all" | bendsql_connect_root_null
echo "drop database $DATABASE" | bendsql_connect_root_null
