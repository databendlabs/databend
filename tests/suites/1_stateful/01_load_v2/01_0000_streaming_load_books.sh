#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists books;" | $MYSQL_CLIENT_CONNECT
## create book table
echo "CREATE TABLE books
(
    title VARCHAR NULL,
    author VARCHAR NULL,
    date VARCHAR NULL,
    publish_time TIMESTAMP NULL
);" | $MYSQL_CLIENT_CONNECT

# load csv
curl -H "insert_sql:insert into books format CSV" -F "upload=@${CURDIR}/books.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select count(), count_if(title is null), count_if(author is null), count_if(date is null), count_if(publish_time is null) from books " |  $MYSQL_CLIENT_CONNECT

# load tsv
curl -H "insert_sql:insert into books format TSV" -F "upload=@${CURDIR}/books.tsv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select count(), count_if(title is null), count_if(author is null), count_if(date is null), count_if(publish_time is null) from books " |  $MYSQL_CLIENT_CONNECT


echo "drop table books;" | $MYSQL_CLIENT_CONNECT

