#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

 echo "drop table if exists test_x01_csv" | $MYSQL_CLIENT_CONNECT

 echo "CREATE TABLE test_x01_csv
 (
     a VARCHAR,
     b Int,
     c VARCHAR
 );" | $MYSQL_CLIENT_CONNECT

 curl -sH "insert_sql:insert into test_x01_csv format CSV" -H "format_field_delimiter:\x01" -H "format_record_delimiter:\x02" -H "format_skip_header:0" -F "upload=@${CURDIR}/testdata/x01_field_x02_record.csv" \
 -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"
 echo "select count() from test_x01_csv" | $MYSQL_CLIENT_CONNECT
