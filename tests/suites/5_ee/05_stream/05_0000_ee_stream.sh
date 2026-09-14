#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop database if exists db_stream" | bendsql_connect_root

echo "CREATE DATABASE db_stream" | bendsql_connect_root
echo "create table db_stream.t(a int) change_tracking = true" | bendsql_connect_root
echo "insert into db_stream.t values(1)" | bendsql_connect_root_null
echo "create stream default.test_s on table db_stream.t comment = 'test'" | bendsql_connect_root
echo "insert into db_stream.t values(2)" | bendsql_connect_root_null

BASE_ROW_ID=$(echo "select _base_row_id from default.test_s" | bendsql_connect_root)
echo "select change\$row_id='$BASE_ROW_ID' from default.test_s" | bendsql_connect_root
echo "optimize table db_stream.t compact" | bendsql_connect_root
echo "select a, change\$action, change\$is_update, change\$row_id='$BASE_ROW_ID' from default.test_s" | bendsql_connect_root

echo "create stream test_s1 on table db_stream.t at(stream => default.test_s) append_only=false comment = 'standard'" | bendsql_connect_root
echo "show streams like 'test_s%'" | bendsql_connect_root
echo "show full streams like 'test_s%'" | bendsql_connect_root | awk '{print $(NF-7), $(NF-6), $(NF-5), $(NF-4), $(NF-3), $(NF-2), $(NF-1), $NF}'
echo "desc stream default.test_s" | bendsql_connect_root | awk '{print $(NF-7), $(NF-6), $(NF-5), $(NF-4), $(NF-3), $(NF-2), $(NF-1), $NF}'

echo "drop stream if exists default.test_s" | bendsql_connect_root
echo "drop stream if exists default.test_s1" | bendsql_connect_root
echo "drop table if exists db_stream.t" | bendsql_connect_root
echo "drop database if exists db_stream" | bendsql_connect_root
