#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists test_load_unload" | $BENDSQL_CLIENT_CONNECT

# todo(youngsofun): add more types
echo "CREATE TABLE test_load_unload
(
    a VARCHAR NULL,
    b float,
    c array(string),
    d Variant,
    e timestamp,
    f decimal(4, 2),
    g map(string,int),
    h tuple(string,int)
);" | $BENDSQL_CLIENT_CONNECT

insert_data() {
	echo "insert into test_load_unload values
	('a\"b', 1, ['a\"b'], parse_json('{\"k\":\"v\"}'), '2044-05-06T03:25:02.868894-07:00', 010.011, {'k1':10,'k2':20}, ('a', 5)),
	(null, 2, ['a\'b'], parse_json('[1]'), '2044-05-06T03:25:02.868894-07:00', -010.011, {}, ('b',10))
	" | $BENDSQL_CLIENT_CONNECT
}

test_format() {
	echo "---${1}"
	echo "truncate table test_load_unload" | $BENDSQL_CLIENT_CONNECT
	insert_data
	rm -f /tmp/test_load_unload2.txt /tmp/test_load_unload.txt

	curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" \
	-d "select * from test_load_unload FORMAT ${1}" > /tmp/test_load_unload.txt

	cat /tmp/test_load_unload.txt

	echo "truncate table test_load_unload" | $BENDSQL_CLIENT_CONNECT

	curl -sH "insert_sql:insert into test_load_unload file_format = (type = ${1})" \
	-F "upload=@/tmp/test_load_unload.txt" \
	-u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | jq -r '.state, .error'

	curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" \
	-d "select * from test_load_unload FORMAT ${1}" > /tmp/test_load_unload2.txt

	diff /tmp/test_load_unload2.txt /tmp/test_load_unload.txt
}

test_format "CSV"

test_format "TSV"

test_format "NDJSON"

echo "drop table if exists test_load_unload" | $BENDSQL_CLIENT_CONNECT
