#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists test_xml" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE test_xml (
  id          INT,
  name        VARCHAR,
  data        VARCHAR,
  create_time TIMESTAMP,
  empty       VARCHAR NULL
) ENGINE=FUSE;" | $BENDSQL_CLIENT_CONNECT


for VER in 'v1'  'v2' 'v3'
do
	echo "${VER}_default.xml"
	curl -sH "insert_sql:insert into test_xml file_format = (type = XML)" \
		-F "upload=@$TESTS_DATA_DIR/xml/${VER}_default.xml" \
		 -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | jq -r '.state, .error'
	echo "select * from test_xml" | $BENDSQL_CLIENT_CONNECT
	echo "truncate table test_xml" | $BENDSQL_CLIENT_CONNECT

	echo "${VER}_custom_row_tag.xml"
	curl -sH "insert_sql:insert into test_xml file_format = (type = XML  row_tag = 'databend')" \
		 -F "upload=@$TESTS_DATA_DIR/xml/${VER}_custom_row_tag.xml" \
		 -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | jq -r '.state, .error'
	echo "select * from test_xml" | $BENDSQL_CLIENT_CONNECT
	echo "truncate table test_xml" | $BENDSQL_CLIENT_CONNECT
done

echo "drop table if exists test_xml" | $BENDSQL_CLIENT_CONNECT


