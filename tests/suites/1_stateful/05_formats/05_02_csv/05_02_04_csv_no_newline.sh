#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

 echo "drop table if exists test_csv" | $MYSQL_CLIENT_CONNECT

 echo "CREATE TABLE test_csv
 (
     a Int NULL,
     b Int NULL
 );" | $MYSQL_CLIENT_CONNECT

 curl -sH "insert_sql:insert into test_csv file_format = (type = 'CSV')" -F "upload=@${CURDIR}/testdata/no_newline.csv" \
 -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"
 echo "select * from test_csv" | $MYSQL_CLIENT_CONNECT
