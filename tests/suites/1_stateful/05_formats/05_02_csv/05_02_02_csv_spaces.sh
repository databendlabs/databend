#!/usr/bin/env bash

# https://www.rfc-editor.org/rfc/rfc4180
# Spaces are considered part of a field and should not be ignored

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop table if exists test_csv" | $MYSQL_CLIENT_CONNECT
echo "CREATE TABLE test_csv
(
    a VARCHAR NULL,
    b VARCHAR NULL
);" | $MYSQL_CLIENT_CONNECT

echo "drop table if exists test_csv_number" | $MYSQL_CLIENT_CONNECT
echo "CREATE TABLE test_csv_number
(
  	a INT NULL,
  	b INT NULL
);" | $MYSQL_CLIENT_CONNECT

# ok
cat << EOF > /tmp/whitespace.csv
" abc" ,"xyz"
 "abc" ,"xyz"
EOF

# error
cat << EOF > /tmp/whitespace_number1.csv
 123,123
EOF

# error
cat << EOF > /tmp/whitespace_number2.csv
123 ,123
EOF


curl -H "insert_sql:insert into test_csv file_format = (type = 'CSV')" -F "upload=@/tmp/whitespace.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select * from test_csv" | $MYSQL_CLIENT_CONNECT

curl -s -H "insert_sql:insert into test_csv_number file_format = (type = 'CSV')" -F "upload=@/tmp/whitespace_number1.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "bad field"
curl -s -H "insert_sql:insert into test_csv_number file_format = (type = 'CSV')" -F "upload=@/tmp/whitespace_number2.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "bad field"

echo "drop table if exists test_csv" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists test_csv_number" | $MYSQL_CLIENT_CONNECT
