#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists variant_test_streaming_load;" | $MYSQL_CLIENT_CONNECT
## create variant_test_streaming_load table
cat $CURDIR/../ddl/variant_test.sql | sed 's/variant_test/variant_test_streaming_load/g' | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/json_sample1.csv /tmp/json_sample1.csv > /dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/json_sample2.csv /tmp/json_sample2.csv > /dev/null 2>&1

# do the Data integrity check
echo "c52505462ec69689af22b855987ae84ffcfacdc484cdb4de7938c2e65bd3aa09 /tmp/json_sample1.csv" | sha256sum --check > /dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "The downloaded dataset has been corrupted, please remove and fetch it again."
	exit 1
fi

echo "6f592b994e31049df1ea4339ab761201567418a2bd43abfcc5154abce846234b /tmp/json_sample2.csv" | sha256sum --check > /dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "The downloaded dataset has been corrupted, please remove and fetch it again."
	exit 1
fi

# load csv
curl -H "insert_sql:insert into variant_test_streaming_load format Csv" -H "skip_header:0" -H 'field_delimiter: ,' -H 'record_delimiter: \n' -F "upload=@/tmp/json_sample1.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
curl -H "insert_sql:insert into variant_test_streaming_load format Csv" -H "skip_header:0" -H 'field_delimiter: |' -H 'record_delimiter: \n' -F "upload=@/tmp/json_sample2.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" > /dev/null 2>&1
echo "select * from variant_test_streaming_load order by Id asc;" | $MYSQL_CLIENT_CONNECT
echo "truncate table variant_test_streaming_load" | $MYSQL_CLIENT_CONNECT

echo "drop table variant_test_streaming_load;" | $MYSQL_CLIENT_CONNECT
