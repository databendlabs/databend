#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d 'select 1'

curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/" -d 'select 1'

curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}//" -d 'select 1'

curl -s -u root: "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/ping"

curl -s -u root: "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/replicas_status"

curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_clickhouse_handler=1" -d 'select version()'

curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_clickhouse_handler=1" -d 'drop database if exists db2'
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_clickhouse_handler=1" -d 'create database db2'
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_clickhouse_handler=1" -d "show databases like 'db%' format TabSeparatedWithNamesAndTypes"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?database=db2&enable_clickhouse_handler=1" -d 'create table t2(a int not null)'
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?database=db2&enable_clickhouse_handler=1" -d "show TABLES LIKE 't%' format TabSeparatedWithNamesAndTypes"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?database=db2&enable_clickhouse_handler=1" -d 'show create table t2 format TabSeparatedWithNamesAndTypes'
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?database=db2&enable_clickhouse_handler=1" -d 'desc t2 format TabSeparatedWithNamesAndTypes'
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?database=db2&enable_clickhouse_handler=1" -d 'insert into table t2 values(1)'
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_clickhouse_handler=1" -d 'select * from db2.t2'
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_clickhouse_handler=1" -d 'drop database if exists db2'

echo "--- default_format"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?default_format=TabSeparatedWithNamesAndTypes&enable_clickhouse_handler=1" -d 'select number as a from numbers(1)'

curl -s -u root: -H "X-CLICKHOUSE-FORMAT: TabSeparatedWithNamesAndTypes" -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_clickhouse_handler=1" -d 'select number as a from numbers(1)'
