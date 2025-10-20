#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# prepare data
DATA="/tmp/load_compact.csv"
rm -rf $DATA
for j in $(seq 1 1000);do
	printf "0123456789\n" >> "$DATA"
done

stmt "drop table if exists t1 all"
stmt "CREATE TABLE t1 ( c0 string );"

curl -sS \
-H "X-Databend-SQL:insert /*+ set_var(input_read_buffer_size=100)  */into t1 values  from @_databend_load file_format = (type = CSV)" \
-H "X-databend-query-id: test" \
-F "upload=@${DATA}" \
-u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | jq -r '.id, .stats.rows'
echo

query "select count(*) from t1"
query "select block_count from fuse_snapshot('default','t1')"
stmt "drop table if exists t1"
