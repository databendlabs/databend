#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

 echo "drop table if exists test_json" | $MYSQL_CLIENT_CONNECT

 echo "CREATE TABLE test_json
 (
     c1 Int NULL,
     c2 String NULL
 );" | $MYSQL_CLIENT_CONNECT

echo "---empty"
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d 'select * from test_json format Json'

echo "---1 row"
echo "insert into test_json values (1, 'a');" | $MYSQL_CLIENT_CONNECT
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d 'select * from test_json format Json'

echo "---2 rows"
echo "insert into test_json values (2, 'b');" | $MYSQL_CLIENT_CONNECT
curl -s -u root: -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d 'select * from test_json order by c1 format Json'
