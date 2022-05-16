#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "drop table if exists ontime_streaming_load;" | $MYSQL_CLIENT_CONNECT
## create ontime table
cat $CURDIR/../ontime/create_table.sql | sed 's/ontime/ontime_streaming_load/g' | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv /tmp/ontime_200.csv > /dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.parquet /tmp/ontime_200.parquet  > /dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.ndjson /tmp/ontime_200.ndjson  > /dev/null 2>&1


# do the Data integrity check
echo "33b1243ecd881e701a1c33cc8d621ecbf9817be006dce8722cfc6dd7ef0637f9 /tmp/ontime_200.csv" | sha256sum --check > /dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "The downloaded dataset has been corrupted, please remove and fetch it again."
	exit 1
fi

echo "e90086f5a25ef8bdb2469030a90e5ae30e967e41ed38b71f386f2a1bdc24efc8 /tmp/ontime_200.parquet" | sha256sum --check > /dev/null 2>&1

if [ $? -ne 0 ]; then
	echo "The downloaded dataset has been corrupted, please remove and fetch it again."
	exit 1
fi

echo "8e6e663cf6fdaedf99516f8969512e40954bfa863822e8ef2d61e50c182c8d91 /tmp/ontime_200.ndjson" | sha256sum --check > /dev/null 2>&1

if [ $? -ne 0 ]; then
	echo "The downloaded dataset has been corrupted, please remove and fetch it again."
	exit 1
fi



curl -H "insert_sql:insert into ontime_streaming_load format Csv" -H "skip_header:1" -F  "upload=@/tmp/ontime_200.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select count(1) ,avg(Year), sum(DayOfWeek)  from ontime_streaming_load;" | $MYSQL_CLIENT_CONNECT


curl -H "insert_sql:insert into ontime_streaming_load format Parquet" -H "skip_header:1" -F "upload=@/tmp/ontime_200.parquet" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select count(1) ,avg(Year), sum(DayOfWeek)  from ontime_streaming_load;" | $MYSQL_CLIENT_CONNECT


curl -H "insert_sql:insert into ontime_streaming_load format NdJson" -H "skip_header:1" -F "upload=@/tmp/ontime_200.ndjson" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select count(1) ,avg(Year), sum(DayOfWeek)  from ontime_streaming_load;" | $MYSQL_CLIENT_CONNECT


echo "drop table ontime_streaming_load;" | $MYSQL_CLIENT_CONNECT
