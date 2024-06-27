#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists wrong_csv;" | $BENDSQL_CLIENT_CONNECT
echo "create table wrong_csv(a int, b string, c int);" | $BENDSQL_CLIENT_CONNECT

DATADIR=$CURDIR/../../../data

# load parquet
curl -H "enable_streaming_load:1" -H "insert_sql:insert into wrong_csv file_format = (type = CSV) on_error=abort_5" -F "upload=@/${DATADIR}/csv/wrong_sample.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select count(*) from wrong_csv" | $BENDSQL_CLIENT_CONNECT
echo "drop table wrong_csv;" | $BENDSQL_CLIENT_CONNECT

