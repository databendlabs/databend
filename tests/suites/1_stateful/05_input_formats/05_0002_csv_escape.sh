#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists test_csv" | $MYSQL_CLIENT_CONNECT
echo "CREATE TABLE test_csv
(
    a VARCHAR NULL
);" | $MYSQL_CLIENT_CONNECT

cat << EOF > /tmp/escape_normal.csv
"a""b"
EOF

cat << EOF > /tmp/escape_slash.csv
"c\"d"
EOF

cat << EOF > /tmp/escape_slash2.csv
"e\"f"
EOF

curl -sH "insert_sql:insert into test_csv format CSV" -F "upload=@/tmp/escape_normal.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"
echo "select * from test_csv" | $MYSQL_CLIENT_CONNECT
echo "truncate table test_csv" | $MYSQL_CLIENT_CONNECT

curl -sH "insert_sql:insert into test_csv format CSV" -H "format_escape:'\\'" -F "upload=@/tmp/escape_slash.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"
echo "select * from test_csv" | $MYSQL_CLIENT_CONNECT
echo "truncate table test_csv" | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp /tmp/escape_slash2.csv s3://testbucket/admin/data/csv/escape_slash2.csv > /dev/null 2>&1
echo "copy into test_csv from 's3://testbucket/admin/data/csv/escape_slash2.csv' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='http://127.0.0.1:9900/') FILE_FORMAT = (type = 'CSV' escape='\\\')" | $MYSQL_CLIENT_CONNECT
echo "select * from test_csv" | $MYSQL_CLIENT_CONNECT

echo "drop table if exists test_csv" | $MYSQL_CLIENT_CONNECT
