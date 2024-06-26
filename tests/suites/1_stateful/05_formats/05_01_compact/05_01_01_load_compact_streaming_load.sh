#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

# prepare data
DATA="/tmp/load_compact.csv"
rm -rf $DATA
for j in $(seq 1 1000);do
	printf "0123456789\n" >> "$DATA"
done

echo "drop table if exists t1 all" | $BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE t1
(
    c0 string
);" | $BENDSQL_CLIENT_CONNECT

echo "---load"
curl -sH "insert_sql:insert into t1 file_format = (type = CSV)" \
-F "upload=@${DATA}" \
-H "input_read_buffer_size: 100" \
-H "enable_streaming_load: 1" \
-u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | grep -c "SUCCESS"

echo "---row_count"
echo "select count(*) from t1" | $BENDSQL_CLIENT_CONNECT

echo "---block_count"
echo "select block_count from fuse_snapshot('default','t1')" | $BENDSQL_CLIENT_CONNECT

echo "drop table if exists t1" | $BENDSQL_CLIENT_CONNECT
