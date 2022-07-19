#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists mytime;" | $MYSQL_CLIENT_CONNECT
## create mytime table
echo "create table mytime(id string null, t_bool boolean null,
t_float float null, t_double double null,
t_timestamp timestamp null, t_data date null, t_array array(int) null);" | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/mytime.parquet /tmp/mytime.parquet > /dev/null 2>&1

# do the Data integrity check
echo "a75efd2d255779b8ef557dce1e829902730c399427447539886c0a6a3aa64b46 /tmp/mytime.parquet" | sha256sum --check > /dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "The downloaded dataset has been corrupted, please remove and fetch it again."
	exit 1
fi

# load parquet
curl -H "insert_sql:insert into mytime format Parquet" -F "upload=@/tmp/mytime.parquet" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select * from mytime" | $MYSQL_CLIENT_CONNECT
echo "drop table mytime;" | $MYSQL_CLIENT_CONNECT

