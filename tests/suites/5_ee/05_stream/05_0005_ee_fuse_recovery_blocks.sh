#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

DATABASE=db_stream_recovery
CONNECTION=conn_stream_recovery

echo "drop database if exists $DATABASE" | bendsql_connect_root_null
echo "drop connection if exists $CONNECTION" | bendsql_connect_root_null
echo "create connection $CONNECTION storage_type='s3' access_key_id='minioadmin' secret_access_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}'" | bendsql_connect_root_null
echo "create database $DATABASE options(DEFAULT_STORAGE_CONNECTION='$CONNECTION', DEFAULT_STORAGE_PATH='s3://testbucket/fuse-recovery-external/')" | bendsql_connect_root_null
echo "create table $DATABASE.base(id int) change_tracking=true enable_auto_analyze=0" | bendsql_connect_root_null
echo "insert into $DATABASE.base values(1)" | bendsql_connect_root_null
BLOCK_FILE=$(echo "select split_part(_block_name, '/', -1) from $DATABASE.base" | bendsql_connect_root)

echo "truncate table $DATABASE.base" | bendsql_connect_root_null
echo "create stream $DATABASE.stream_recovered_data on table $DATABASE.base append_only=true" | bendsql_connect_root_null

# Recover from an external S3 table that inherits database storage settings.
# The second statement must be skipped by COPY history.
echo "copy into $DATABASE.base from FUSE_RECOVERY_BLOCKS(FILES => ('$BLOCK_FILE'))" | bendsql_connect_root_null
echo "copy into $DATABASE.base from FUSE_RECOVERY_BLOCKS(FILES => ('$BLOCK_FILE'))" | bendsql_connect_root_null
echo "select id, change\$action, change\$is_update from $DATABASE.stream_recovered_data" | bendsql_connect_root

echo "drop stream $DATABASE.stream_recovered_data" | bendsql_connect_root_null
echo "drop table $DATABASE.base all" | bendsql_connect_root_null
echo "drop database $DATABASE" | bendsql_connect_root_null
echo "drop connection $CONNECTION" | bendsql_connect_root_null
