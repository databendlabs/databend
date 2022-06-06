#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/clickhouse" -d 'drop user if exists user1'

curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/clickhouse" -d 'create user user1 identified by abc123'

curl -s -u user1:abc123 -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/clickhouse" -d 'select 1 FORMAT CSV'
