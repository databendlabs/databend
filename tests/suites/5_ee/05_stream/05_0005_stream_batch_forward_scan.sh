#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# The EE sqllogic job uses local FS, which does not advertise
# list_with_start_after and would only exercise the fallback path. This EE
# functional suite runs on MinIO/S3 so the forward-scan path is covered.

echo "drop database if exists db_stream_forward_batch" | bendsql_connect_root_null
echo "create database db_stream_forward_batch" | bendsql_connect_root_null
echo "create table db_stream_forward_batch.t(c int)" | bendsql_connect_root_null
echo "create stream db_stream_forward_batch.s on table db_stream_forward_batch.t append_only = true" | bendsql_connect_root_null

for i in {1..5}; do
  echo "insert into db_stream_forward_batch.t values ($i)" | bendsql_connect_root_null
done

echo "batch 1"
echo "set enable_stream_batch_snapshot_forward_scan = 1; select c from db_stream_forward_batch.s with (consume = true, max_batch_size = 2) order by c" | bendsql_connect_root

echo "batch 2"
echo "set enable_stream_batch_snapshot_forward_scan = 1; select c from db_stream_forward_batch.s with (consume = true, max_batch_size = 2) order by c" | bendsql_connect_root

echo "remaining"
echo "select c from db_stream_forward_batch.s order by c" | bendsql_connect_root

echo "drop database if exists db_stream_forward_batch" | bendsql_connect_root_null
