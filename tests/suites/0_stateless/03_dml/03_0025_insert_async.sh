#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# create table
echo "drop table if exists async_insert_test;" | $MYSQL_CLIENT_CONNECT
echo "create table async_insert_test(a tinyint, b bigint, c double);" | $MYSQL_CLIENT_CONNECT

# parallel insert
count=0
while(($count < 100))
do curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" \
    --header 'Content-Type: application/json' \
    --data-raw '{"sql": "insert into async_insert_test values(1, 2, 3.4);", "session": {"settings": {"enable_async_insert": "1"}}}' > /dev/null 2>&1 &
    let "count++"
done

sleep 1
echo "select count() from async_insert_test;" | $MYSQL_CLIENT_CONNECT

# drop table
echo "drop table if exists async_insert_test;" | $MYSQL_CLIENT_CONNECT
