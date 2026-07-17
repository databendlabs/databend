#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

DATABASE=db_stream_recovery
CONNECTION=conn_stream_recovery
RECOVERY_USER=user_stream_recovery
RECOVERY_PASSWORD=password

echo "drop database if exists $DATABASE" | bendsql_connect_root_null
echo "drop connection if exists $CONNECTION" | bendsql_connect_root_null
echo "drop user if exists '$RECOVERY_USER'" | bendsql_connect_root_null
echo "create connection $CONNECTION storage_type='s3' access_key_id='minioadmin' secret_access_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}'" | bendsql_connect_root_null
echo "create database $DATABASE options(DEFAULT_STORAGE_CONNECTION='$CONNECTION', DEFAULT_STORAGE_PATH='s3://testbucket/fuse-recovery-external/')" | bendsql_connect_root_null
echo "create table $DATABASE.base(id int) change_tracking=true enable_auto_analyze=0" | bendsql_connect_root_null
echo "insert into $DATABASE.base values(1)" | bendsql_connect_root_null
BLOCK_FILE=$(echo "select split_part(_block_name, '/', -1) from $DATABASE.base" | bendsql_connect_root)

echo "truncate table $DATABASE.base" | bendsql_connect_root_null
echo "create stream $DATABASE.stream_recovered_data on table $DATABASE.base append_only=true" | bendsql_connect_root_null

# Recover from an external S3 table that inherits database storage settings.
# The second statement must be skipped by COPY history.
COPY_SQL="copy into $DATABASE.base from FUSE_RECOVERY_BLOCKS(SOURCE_TABLE => default.$DATABASE.base, FILES => ('$BLOCK_FILE'))"
echo "create user '$RECOVERY_USER' identified by '$RECOVERY_PASSWORD'" | bendsql_connect_root_null
echo "grant super on *.* to '$RECOVERY_USER'" | bendsql_connect_root_null
echo "grant insert on $DATABASE.base to '$RECOVERY_USER'" | bendsql_connect_root_null

# Source SELECT is required even when the user has SUPER and target INSERT.
if echo "$COPY_SQL" | bendsql_connect_user "$RECOVERY_USER" "$RECOVERY_PASSWORD" --output null >/dev/null 2>&1; then
	echo "FUSE recovery unexpectedly succeeded without source SELECT"
	exit 1
fi

echo "grant select on $DATABASE.base to '$RECOVERY_USER'" | bendsql_connect_root_null
echo "$COPY_SQL" | bendsql_connect_user "$RECOVERY_USER" "$RECOVERY_PASSWORD" --output null
echo "$COPY_SQL" | bendsql_connect_user "$RECOVERY_USER" "$RECOVERY_PASSWORD" --output null
echo "select id, change\$action, change\$is_update from $DATABASE.stream_recovered_data" | bendsql_connect_root

echo "drop user '$RECOVERY_USER'" | bendsql_connect_root_null
echo "drop stream $DATABASE.stream_recovered_data" | bendsql_connect_root_null
echo "drop table $DATABASE.base all" | bendsql_connect_root_null
echo "drop database $DATABASE" | bendsql_connect_root_null
echo "drop connection $CONNECTION" | bendsql_connect_root_null
