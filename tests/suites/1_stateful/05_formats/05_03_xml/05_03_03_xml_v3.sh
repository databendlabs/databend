#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

cat << EOF > /tmp/simple_v3.xml
<?xml version="1.0"?>
<data>
    <row>
        <field name="id">1</field>
        <field name="name">shuai"ge</field>
        <field name="data">{"我是":"帅哥"}</field>
        <field name="create_time">2022-11-01 10:51:14</field>
    </row>
    <row>
        <field name="id">2</field>
        <field name="name">"mengnan"</field>
        <field name="data">"猛"男</field>
        <field name="create_time">2022-11-01 10:51:14</field>
    </row>
    <row>
        <field name="ID">3</field>
        <field name="NAME">"mengnan"</field>
        <field name="DATA">"猛"男</field>
        <field name="CREATE_TIME">2022-11-01 10:51:14</field>
        <field name="EMPTY">123</field>
    </row>
</data>
EOF

# custom row_tag
cat << EOF > /tmp/simple_v4.xml
<?xml version="1.0"?>
<data>
    <databend>
        <field name="id">1</field>
        <field name="name">shuai"ge</field>
        <field name="data">{"我是":"帅哥"}</field>
        <field name="create_time">2022-11-01 10:51:14</field>
    </databend>
    <databend>
        <field name="id">2</field>
        <field name="name">"mengnan"</field>
        <field name="data">"猛"男</field>
        <field name="create_time">2022-11-01 10:51:14</field>
    </databend>
    <databend>
        <field name="ID">3</field>
        <field name="NAME">"mengnan"</field>
        <field name="DATA">"猛"男</field>
        <field name="CREATE_TIME">2022-11-01 10:51:14</field>
        <field name="EMPTY">123</field>
    </databend>
</data>
EOF

curl -sH "insert_sql:insert into test_xml file_format = (type = 'XML')" -F "upload=@/tmp/simple_v3.xml" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"
echo "select * from test_xml" | $MYSQL_CLIENT_CONNECT
echo "truncate table test_xml" | $MYSQL_CLIENT_CONNECT

curl -sH "insert_sql:insert into test_xml file_format = (type = 'XML' row_tag = 'databend')" -F "upload=@/tmp/simple_v4.xml" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"
echo "select * from test_xml" | $MYSQL_CLIENT_CONNECT
echo "truncate table test_xml" | $MYSQL_CLIENT_CONNECT
