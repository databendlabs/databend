#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop database if exists db_stream" | $BENDSQL_CLIENT_CONNECT

echo "CREATE DATABASE db_stream" | $BENDSQL_CLIENT_CONNECT
echo "create table db_stream.t(a int)" | $BENDSQL_CLIENT_CONNECT
echo "create stream default.s1 on table db_stream.t comment = 'test'" | $BENDSQL_CLIENT_CONNECT
echo "create stream db_stream.s2 on table db_stream.t at(stream => default.s1)" | $BENDSQL_CLIENT_CONNECT

curl -X GET -s http://localhost:8081/v1/tenants/system/stream_status\?stream_name=s1 | jq .has_data
curl -X GET -s http://localhost:8081/v1/tenants/system/stream_status\?stream_name\=s2\&database\=db_stream | jq .has_data

echo "drop stream if exists default.s1" | $BENDSQL_CLIENT_CONNECT
echo "drop stream if exists db_stream.s2" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists db_stream.t all" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists db_stream" | $BENDSQL_CLIENT_CONNECT
