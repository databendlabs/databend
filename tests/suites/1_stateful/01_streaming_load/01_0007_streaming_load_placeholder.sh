#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


DATA="/tmp/streaming_load_07"
rm -rf $DATA
mkdir $DATA
stmt "create or replace stage streaming_load_07 url='fs://$DATA/';"
stmt "CREATE or replace TABLE streaming_load_07 (c1 string default 'ok', c2 int, c3 string, c4 date);"

function run() {
  echo "--$1"
  stmt "copy into @streaming_load_07/data.$1 from (select '2020-01-02' as c4, 110 as c2) file_format=(type='$1')  single=true include_query_id=false use_raw_path=true detailed_output=true overwrite=true;"
  (set -x; curl -sS -H "x-databend-query-id:load-$1" -H "sql:insert into streaming_load_07(c3, c4, c2) values ('a', ?, ?) file_format = (type=$1)" -F "upload=@$DATA/data.$1" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load")
  echo
  echo "<<<<"
  query "select * from streaming_load_07;"
  stmt "truncate table streaming_load_07"
}

run "csv"
run "tsv"
run "ndjson"
run "parquet"

stmt "drop table if exists streaming_load_07"
