#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists t1" | $BENDSQL_CLIENT_CONNECT
echo "drop stream if exists s1" | $BENDSQL_CLIENT_CONNECT

echo "create table t1(a int)" | $BENDSQL_CLIENT_CONNECT
echo "create stream s1 on table t1" | $BENDSQL_CLIENT_CONNECT

echo "# Test catalog streams API - existing database"
curl -s -u root: -XGET "http://localhost:8000/v1/catalog/databases/default/streams" | jq 'del(.streams[].created_on, .streams[].updated_on, .streams[].stream_id, .streams[].table_id, .streams[].table_version)'

echo "# Test catalog streams API - non-existent database"
curl -s -u root: -XGET "http://localhost:8000/v1/catalog/databases/nonexistent/streams" -w "%{http_code}\n"

echo "drop stream if exists s1" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists t1" | $BENDSQL_CLIENT_CONNECT
