#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop table if exists test_xml" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE test_xml (
  id          INT,
  name        VARCHAR,
  data        VARCHAR,
  create_time TIMESTAMP,
  empty       VARCHAR NULL
) ENGINE=FUSE;" | $MYSQL_CLIENT_CONNECT

cat << EOF > /tmp/simple_v2.xml
<?xml version="1.0"?>
<data>
    <row id="1" name='shuai"ge' data='{"我是":"帅哥"}' create_time="2022-11-01 10:51:14"/>
    <row id="2" name='"mengnan"' data='"猛"男' create_time="2022-11-01 10:51:14"/>
    <row ID="3" NAME='"mengnan"' DATA='"猛"男' CREATE_TIME="2022-11-01 10:51:14" EMPTY="123"/>
</data>
EOF

# custom row_tag
cat << EOF > /tmp/simple_v3.xml
<?xml version="1.0"?>
<data>
    <databend id="1" name='shuai"ge' data='{"我是":"帅哥"}' create_time="2022-11-01 10:51:14"/>
    <databend id="2" name='"mengnan"' data='"猛"男' create_time="2022-11-01 10:51:14"/>
    <databend ID="3" NAME='"mengnan"' DATA='"猛"男' CREATE_TIME="2022-11-01 10:51:14" EMPTY="123"/>
</data>
EOF

curl -sH "insert_sql:insert into test_xml file_format = (type = 'XML')" -F "upload=@/tmp/simple_v2.xml" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"
echo "select * from test_xml" | $MYSQL_CLIENT_CONNECT
echo "truncate table test_xml" | $MYSQL_CLIENT_CONNECT

curl -sH "insert_sql:insert into test_xml file_format = (type = 'XML' row_tag = 'databend')" -F "upload=@/tmp/simple_v3.xml" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"
echo "select * from test_xml" | $MYSQL_CLIENT_CONNECT
echo "truncate table test_xml" | $MYSQL_CLIENT_CONNECT
