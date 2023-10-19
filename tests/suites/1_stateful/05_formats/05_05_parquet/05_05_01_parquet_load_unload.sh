#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop table if exists test_load_unload" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE test_load_unload
(
    a VARCHAR NULL,
    b float,
    c array(string),
    d Variant,
    e timestamp,
    f decimal(4, 2),
    h tuple(string,int)
);" | $MYSQL_CLIENT_CONNECT

insert_data() {
	echo "insert into test_load_unload values
	('a\"b', 1, ['a\"b'], parse_json('{\"k\":\"v\"}'), '2044-05-06T03:25:02.868894-07:00', 010.011, ('a', 5)),
	(null, 2, ['a\'b'], parse_json('[1]'), '2044-05-06T03:25:02.868894-07:00', -010.011, ('b',10))
	" | $MYSQL_CLIENT_CONNECT
}

test_format() {
	# insert data
	insert_data

	# unload clickhouse
	curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" \
	-d "select * from test_load_unload FORMAT ${1}" > /tmp/test_load_unload.parquet

	echo "truncate table test_load_unload" | $MYSQL_CLIENT_CONNECT

	# load streaming
	curl -sH "insert_sql:insert into test_load_unload file_format = (type = ${1})" \
	-F "upload=@/tmp/test_load_unload.parquet" \
	-u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"

	# unload clickhouse again
	curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" \
	-d "select * from test_load_unload FORMAT ${1}" > /tmp/test_load_unload2.parquet

	echo "truncate table test_load_unload" | $MYSQL_CLIENT_CONNECT

	# copy into table
	echo "copy into test_load_unload from 'fs:///tmp/test_load_unload.parquet' file_format = (type = ${1});" | $MYSQL_CLIENT_CONNECT

	# unload clickhouse again
	curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" \
	-d "select * from test_load_unload FORMAT ${1}" > /tmp/test_load_unload3.parquet

	# copy into stage
	rm -rf /tmp/test_load_unload_fs
	echo "drop stage if exists data_fs;" | $MYSQL_CLIENT_CONNECT
	echo "create stage data_fs url = 'fs:///tmp/test_load_unload_fs/' FILE_FORMAT = (type = ${1});"  | $MYSQL_CLIENT_CONNECT
	echo "copy into @data_fs from test_load_unload file_format = (type = ${1});" | $MYSQL_CLIENT_CONNECT

	# unload clickhouse again from stage
	curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" \
	-d "select * from @data_fs FORMAT ${1}" > /tmp/test_load_unload4.parquet

	diff /tmp/test_load_unload2.parquet /tmp/test_load_unload.parquet
	diff /tmp/test_load_unload3.parquet /tmp/test_load_unload.parquet
	diff /tmp/test_load_unload4.parquet /tmp/test_load_unload.parquet
	echo "truncate table test_load_unload" | $MYSQL_CLIENT_CONNECT
}

test_format "PARQUET"
