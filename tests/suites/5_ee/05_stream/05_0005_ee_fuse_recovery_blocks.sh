#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

DATABASE=db_stream_recovery

echo "drop database if exists $DATABASE" | bendsql_connect_root_null
echo "create database $DATABASE" | bendsql_connect_root_null
echo "create table $DATABASE.base(id int) change_tracking=true enable_auto_analyze=0" | bendsql_connect_root_null
echo "insert into $DATABASE.base values(1)" | bendsql_connect_root_null
BLOCK=$(echo "select _block_name from $DATABASE.base" | bendsql_connect_root)

echo "truncate table $DATABASE.base" | bendsql_connect_root_null
echo "create stream $DATABASE.recovered on table $DATABASE.base append_only=true" | bendsql_connect_root_null

# Keep a MinIO/S3 smoke test for reading a managed block directly and exposing
# the recovery as a new append-only stream INSERT. Detailed behavior is covered
# by the EE sqllogictest suite.
echo "copy into $DATABASE.base from FUSE_RECOVERY_BLOCKS(FILES => ('$BLOCK')) force = false" | bendsql_connect_root_null
echo "select id, change\$action, change\$is_update from $DATABASE.recovered" | bendsql_connect_root

echo "drop stream $DATABASE.recovered" | bendsql_connect_root_null
echo "drop table $DATABASE.base all" | bendsql_connect_root_null
echo "drop database $DATABASE" | bendsql_connect_root_null
