#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

 echo "drop table if exists test_csv_header_only" | $MYSQL_CLIENT_CONNECT

cat << EOF > /tmp/test_csv_header_only.csv
c1,c2
EOF

 echo "CREATE TABLE test_csv_header_only
 (
     a Int,
     b Int
 );" | $MYSQL_CLIENT_CONNECT

 curl -sH "insert_sql:insert into test_csv_header_only file_format = (type = 'CSV' skip_header = 1 record_delimiter = '-')" -F "upload=@/tmp/test_csv_header_only.csv" \
 -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c '\"rows\":0'
