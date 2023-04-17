#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists variant_test;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists variant_test2;" | $MYSQL_CLIENT_CONNECT
## create variant_test and variant_test2 table
cat $CURDIR/../ddl/variant_test.sql | $MYSQL_CLIENT_CONNECT

DATADIR=$CURDIR/../../../data

# load csv
curl -H "insert_sql:insert into variant_test file_format = (type = CSV field_delimiter = ',' quote = '\'')" -F "upload=@/${DATADIR}/json_sample1.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
curl -H "insert_sql:insert into variant_test file_format = (type = CSV field_delimiter = '|' quote = '\'')" -F "upload=@/${DATADIR}/json_sample2.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1

echo "select * from variant_test order by Id asc;" | $MYSQL_CLIENT_CONNECT

# load ndjson
curl -H "insert_sql:insert into variant_test2 file_format = (type = NdJson)" -F "upload=@/${DATADIR}/json_sample.ndjson" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select * from variant_test2 order by b asc;" | $MYSQL_CLIENT_CONNECT

echo "drop table variant_test;" | $MYSQL_CLIENT_CONNECT
echo "drop table variant_test2;" | $MYSQL_CLIENT_CONNECT
