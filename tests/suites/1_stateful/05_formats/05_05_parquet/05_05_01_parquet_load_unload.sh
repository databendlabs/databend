#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop table if exists test_load_unload" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE test_load_unload
(
    a VARCHAR NULL,
    b float,
    e timestamp
);" | $MYSQL_CLIENT_CONNECT

insert_data() {
	echo "insert into test_load_unload values
	('a\"b', 1, '2044-05-06T03:25:02.868894-07:00')
	" | $MYSQL_CLIENT_CONNECT
}

test_format() {
	# insert data
	insert_data

	# unload clickhouse
	curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" \
	-d "select * from test_load_unload FORMAT ${1}" > /tmp/test_load_unload.txt

	echo "truncate table test_load_unload" | $MYSQL_CLIENT_CONNECT

	curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" \
	-d "select * from test_load_unload FORMAT ${1}" > /tmp/test_load_unload.txt

	# load streaming
	curl -sH "insert_sql:insert into test_load_unload file_format = (type = '${1}')" \
	-F "upload=@/tmp/test_load_unload.txt" \
	-u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"

	# unload clickhouse again
	curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" \
	-d "select * from test_load_unload FORMAT ${1}" > /tmp/test_load_unload2.txt

	diff /tmp/test_load_unload2.txt /tmp/test_load_unload.txt
	rm  /tmp/test_load_unload2.txt /tmp/test_load_unload.txt
	echo "truncate table test_load_unload" | $MYSQL_CLIENT_CONNECT
}

test_format "PARQUET"
