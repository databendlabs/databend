#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists mytime;" | $MYSQL_CLIENT_CONNECT
## create mytime table
echo "create table mytime(id string null, t_bool boolean null,
t_float float null, t_double double null,
t_timestamp timestamp null, t_data date null, t_array array(int null));" | $MYSQL_CLIENT_CONNECT

DATADIR=$CURDIR/../../../data

# load parquet
curl -H "insert_sql:insert into mytime file_format = (type = 'Parquet')" -F "upload=@/${DATADIR}/mytime.parquet" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select * from mytime" | $MYSQL_CLIENT_CONNECT
echo "drop table mytime;" | $MYSQL_CLIENT_CONNECT

