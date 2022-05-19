#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/clickhouse" -d 'select number, number::varchar, number + 1 from numbers(3) FORMAT CSV'


curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/clickhouse" -d 'select number, number::varchar, number + 1 from numbers(3) FORMAT TSV'


curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/clickhouse" -d 'select number, number::varchar, number + 1 from numbers(3) FORMAT NDJSON'
