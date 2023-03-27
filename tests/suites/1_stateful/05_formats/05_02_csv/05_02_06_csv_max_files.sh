#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh


# Should be <root>/tests/data/
DATADIR=$(realpath $CURDIR/../../../data/)

echo "drop table if exists test_max_files" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE test_max_files (
  id INT,
  c1 VARCHAR,
  c2 TIMESTAMP,
  c3 VARCHAR
) ENGINE=FUSE;" | $MYSQL_CLIENT_CONNECT

cat << EOF > /tmp/force_true.csv
"id","name","upadte_at","content"
1,"a","2022-10-25 10:51:14","{\"hello\":\"world\"}"
2,"c","2022-10-25 10:51:50","{\"银角大王\":\"沙河\"}"
EOF

cat << EOF > /tmp/force_false.csv
"id","name","upadte_at","content"
3,"a","2022-10-25 10:51:14","{\"hello\":\"world\"}"
4,"b","2022-10-25 10:51:28","{\"金角大王\":\"沙河\"}"
EOF


aws --endpoint-url http://127.0.0.1:9900/ s3 cp /tmp/force_true.csv s3://testbucket/admin/data/csv/force_true.csv > /dev/null 2>&1
echo "copy into test_max_files from 'fs:///tmp/force_true.csv' FILE_FORMAT = (type = CSV escape='\\\\' skip_header=1) max_files=1 force=true" | $MYSQL_CLIENT_CONNECT
echo "select * from test_max_files order by id" | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp /tmp/force_false.csv s3://testbucket/admin/data/csv/force_false.csv > /dev/null 2>&1
echo "copy into test_max_files from 'fs:///tmp/force_false.csv' FILE_FORMAT = (type = CSV escape='\\\\' skip_header=1) max_files=1 force=false" | $MYSQL_CLIENT_CONNECT
echo "select * from test_max_files order by id" | $MYSQL_CLIENT_CONNECT

echo "drop table if exists test_max_files" | $MYSQL_CLIENT_CONNECT
