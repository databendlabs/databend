#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop stream if exists default.s1" | bendsql_connect_root
echo "drop database if exists db_stream" | bendsql_connect_root

echo "CREATE DATABASE db_stream" | bendsql_connect_root
echo "create table db_stream.t(a int)" | bendsql_connect_root
echo "create stream default.s1 on table db_stream.t comment = 'test'" | bendsql_connect_root
echo "create stream db_stream.s2 on table db_stream.t at(stream => default.s1)" | bendsql_connect_root

curl -X GET -s http://localhost:8081/v1/tenants/system/stream_status\?stream_name=s1 | jq .has_data
curl -X GET -s http://localhost:8081/v1/tenants/system/stream_status\?stream_name\=s2\&database\=db_stream | jq .has_data

curl -X GET -s http://localhost:8081/v1/tenants/system/stream_backlog\?stream_name=s1 | jq -c '[.rows_added,.rows_removed,.estimated_rows]'
curl -X GET -s http://localhost:8081/v1/tenants/system/stream_backlog\?stream_name\=s2\&database\=db_stream | jq -c '[.rows_added,.rows_removed,.estimated_rows]'

echo "drop stream if exists default.s1" | bendsql_connect_root
echo "drop stream if exists db_stream.s2" | bendsql_connect_root
echo "drop table if exists db_stream.t all" | bendsql_connect_root
echo "drop database if exists db_stream" | bendsql_connect_root
