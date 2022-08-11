#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cat << EOF > /tmp/databend_test_csv1.txt
'2023-04-08 01:01:01', "Hello", 123
'2023-02-03 02:03:02', "World", 123

EOF

cat << EOF > /tmp/databend_test_csv2.txt
'2023-04-08 01:01:01', "Hello", 123
'2023-02-032 02:03:02', "World", 123

EOF


echo "create table a ( a datetime, b string, c int);" | $MYSQL_CLIENT_CONNECT

curl -sH "insert_sql:insert into a format Csv" -H "skip_header:0" -F "upload=@/tmp/databend_test_csv1.txt" -F "upload=@/tmp/databend_test_csv2.txt" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep "Error"

echo "drop table a;" | $MYSQL_CLIENT_CONNECT
