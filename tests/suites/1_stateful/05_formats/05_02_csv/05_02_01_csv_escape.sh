#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh


# Should be <root>/tests/data/
DATADIR=$(realpath $CURDIR/../../../data/)

echo "drop table if exists test_csv" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists test_csv2" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE test_csv
(
    a VARCHAR NULL
);" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE test_csv2 (
  id INT,
  c1 VARCHAR,
  c2 TIMESTAMP,
  c3 VARCHAR
) ENGINE=FUSE;" | $MYSQL_CLIENT_CONNECT

cat << EOF > /tmp/escape_normal.csv
"a""b"
EOF

cat << EOF > /tmp/escape_slash.csv
"c\"d"
EOF

cat << EOF > /tmp/escape_slash2.csv
"e\"f"
EOF

cat << EOF > /tmp/escape_slash3.csv
"id","name","upadte_at","content"
1,"a","2022-10-25 10:51:14","{\"hello\":\"world\"}"
2,"b","2022-10-25 10:51:28","{\"金角大王\":\"沙河\"}"
3,"c","2022-10-25 10:51:50","{\"银角大王\":\"沙河\"}"
EOF

curl -sH "insert_sql:insert into test_csv file_format = (type = 'CSV')" -F "upload=@/tmp/escape_normal.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"
echo "select * from test_csv" | $MYSQL_CLIENT_CONNECT
echo "truncate table test_csv" | $MYSQL_CLIENT_CONNECT

curl -sH "insert_sql:insert into test_csv file_format = (type = 'CSV' escape = '\\\')" -F "upload=@/tmp/escape_slash.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"
echo "select * from test_csv" | $MYSQL_CLIENT_CONNECT
echo "truncate table test_csv" | $MYSQL_CLIENT_CONNECT

## just need `\\` in sql client, `\\\\` is for shell
aws --endpoint-url http://127.0.0.1:9900/ s3 cp /tmp/escape_slash2.csv s3://testbucket/admin/data/csv/escape_slash2.csv > /dev/null 2>&1
echo "copy into test_csv from 'fs:///tmp/escape_slash2.csv' FILE_FORMAT = (type = 'CSV' escape='\\\\')" | $MYSQL_CLIENT_CONNECT
echo "select * from test_csv" | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp /tmp/escape_slash3.csv s3://testbucket/admin/data/csv/escape_slash3.csv > /dev/null 2>&1
echo "copy into test_csv2 from 'fs:///tmp/escape_slash3.csv' FILE_FORMAT = (type = 'CSV' escape='\\\\' skip_header=1)" | $MYSQL_CLIENT_CONNECT
echo "select * from test_csv2" | $MYSQL_CLIENT_CONNECT

echo "drop table if exists test_csv" | $MYSQL_CLIENT_CONNECT
#echo "drop table if exists test_csv2" | $MYSQL_CLIENT_CONNECT
