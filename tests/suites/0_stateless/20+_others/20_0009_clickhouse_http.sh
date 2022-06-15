#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d 'select 1'

curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/" -d 'select 1'

curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}//" -d 'select 1'

curl -s -u root: "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/ping"

curl -s -u root: "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/replicas_status"
