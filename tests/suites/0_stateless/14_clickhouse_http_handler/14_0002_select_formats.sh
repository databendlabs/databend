#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "----csv"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d 'select number, number::varchar, number + 1 from numbers(3) FORMAT CSV'

echo "----tsv"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d 'select number, number::varchar, number + 1 from numbers(3) FORMAT TSV'

echo "----tsv header escape"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "select regexp_like('fo\nfo', '^fo$') FORMAT TabSeparatedWithNamesAndTypes"

echo "----NDJSON"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d 'select number, number::varchar from numbers(2) FORMAT NDJSON'

echo "----JSONEachRow"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "select number, number::varchar from numbers(2) FORMAT JSONEachRow"

echo "----JSONStringsEachRow"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "select number, number::varchar from numbers(2) FORMAT JSONStringsEachRow"

echo "----JSONCompactEachRow"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "select number, number::varchar from numbers(2) FORMAT JSONCompactEachRow"

echo "----JSONCompactEachRowWithNames"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "select number, number::varchar from numbers(2) FORMAT JSONCompactEachRowWithNames"

echo "----JSONCompactEachRowWithNamesAndTypes"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "select number, number::varchar from numbers(2) FORMAT JSONCompactEachRowWithNamesAndTypes"

echo "----JSONCompactStringsEachRow"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "select number, number::varchar from numbers(2) FORMAT JSONCompactStringsEachRow"

echo "----JSONCompactStringsEachRowWithNames"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "select number, number::varchar from numbers(2) FORMAT JSONCompactStringsEachRowWithNames"

echo "----JSONCompactStringsEachRowWithNamesAndTypes"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "select number, number::varchar from numbers(2) FORMAT JSONCompactStringsEachRowWithNamesAndTypes"

echo "----JSON"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "select number, number::varchar from numbers(2) FORMAT JSON"
