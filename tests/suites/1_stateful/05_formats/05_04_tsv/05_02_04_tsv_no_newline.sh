#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

 echo "drop table if exists test_tsv" | $MYSQL_CLIENT_CONNECT

 echo "CREATE TABLE test_tsv
 (
     a Int NULL,
     b Int NULL
 );" | $MYSQL_CLIENT_CONNECT

 curl -sH "insert_sql:insert into test_tsv file_format = (type = 'TSV')" -F "upload=@${CURDIR}/no_newline.tsv" -H "max_threads: 1" \
 -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"
 echo "select * from test_tsv" | $MYSQL_CLIENT_CONNECT
