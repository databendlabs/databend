#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

DATA="/tmp/streaming_load_08"
rm -rf $DATA
mkdir $DATA
stmt "create or replace stage streaming_load_08 url='fs://$DATA/';"
stmt "CREATE or replace TABLE streaming_load_08 (c1 string, c2 int);"
stmt "copy into @streaming_load_08/data.csv from (select '2020-01-02', 110) file_format=(type=csv)  single=true include_query_id=false use_raw_path=true detailed_output=true overwrite=true;"

curl -i -sS \
 	-H "x-databend-query-id:load-csv" \
 	-H "X-Databend-Query-Context:{}" \
 	-H "X-Databend-SQL:insert into streaming_load_08 from @_databend_load file_format = (type=csv)" \
 	-F "upload=@$DATA/data.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" \
 	| grep "x-databend-query-context"  | cut -d ':' -f 2- |  sed 's/-/+/g; s/_/\//g; s/^[[:space:]]*//g; s/[[:space:]]*$//g' | base64 -d

echo

curl  -sS \
 	-H "x-databend-query-id:load-csv" \
 	-H "X-Databend-Query-Context:{}" \
 	-H "X-Databend-SQL:insert into streaming_load_08 from @_databend_load file_format = (type=csv)" \
 	-F "upload=@$DATA/data.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load"

# for compatibility, still work if client do not provide the header,

curl -i -sS \
 	-H "x-databend-query-id:load-csv" \
 	-H "X-Databend-SQL:insert into streaming_load_08 from @_databend_load file_format = (type=csv)" \
 	-F "upload=@$DATA/data.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" \
 	| grep "x-databend-query-context"

echo

curl  -sS \
 	-H "x-databend-query-id:load-csv" \
 	-H "X-Databend-Query-Context:{}" \
 	-H "X-Databend-SQL:insert into streaming_load_08 from @_databend_load file_format = (type=csv)" \
 	-F "upload=@$DATA/data.csv" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load"