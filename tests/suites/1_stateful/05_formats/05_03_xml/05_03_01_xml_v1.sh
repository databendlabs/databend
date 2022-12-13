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

cat << EOF > /tmp/simple_v1.xml
<?xml version="1.0"?>
<data>
    <row>
        <id>1</id>
        <name>shuai"ge</name>
        <data>{"我是":"帅哥"}</data>
        <create_time>2022-11-01 10:51:14</create_time>
    </row>
    <row>
        <id>2</id>
        <name>"mengnan"</name>
        <data>"猛"男</data>
        <create_time>2022-11-01 10:51:14</create_time>
    </row>
    <row>
        <ID>3</ID>
        <NAME>"mengnan"</NAME>
        <DATA>"猛"男</DATA>
        <CREATE_TIME>2022-11-01 10:51:14</CREATE_TIME>
        <EMPTY>123</EMPTY>
    </row>
</data>
EOF

# custom row_tag
cat << EOF > /tmp/simple_v2.xml
<?xml version="1.0"?>
<data>
    <databend>
        <id>1</id>
        <name>shuai"ge</name>
        <data>{"我是":"帅哥"}</data>
        <create_time>2022-11-01 10:51:14</create_time>
    </databend>
    <databend>
        <id>2</id>
        <name>"mengnan"</name>
        <data>"猛"男</data>
        <create_time>2022-11-01 10:51:14</create_time>
    </databend>
    <databend>
        <ID>3</ID>
        <NAME>"mengnan"</NAME>
        <DATA>"猛"男</DATA>
        <CREATE_TIME>2022-11-01 10:51:14</CREATE_TIME>
        <EMPTY>123</EMPTY>
    </databend>
</data>
EOF

curl -sH "insert_sql:insert into test_xml file_format = (type = 'XML')" -F "upload=@/tmp/simple_v1.xml" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load"  | grep -c "SUCCESS"
echo "select * from test_xml" | $MYSQL_CLIENT_CONNECT
echo "truncate table test_xml" | $MYSQL_CLIENT_CONNECT

curl -sH "insert_sql:insert into test_xml file_format = (type = 'XML' row_tag = 'databend')" -F "upload=@/tmp/simple_v2.xml" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"
echo "select * from test_xml" | $MYSQL_CLIENT_CONNECT
echo "truncate table test_xml" | $MYSQL_CLIENT_CONNECT
