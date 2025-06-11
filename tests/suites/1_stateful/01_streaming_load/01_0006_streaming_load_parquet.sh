#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


DATA="/tmp/streaming_load_parquet"
rm -rf $DATA
mkdir $DATA
stmt "create or replace stage streaming_load_parquet url='fs://$DATA/';"
stmt "CREATE or replace TABLE streaming_load_parquet (c1 string default 'ok', c2 int, c3 date);"

function run() {
  echo "--$2"
  stmt "copy into @streaming_load_parquet/$1.parquet from (select $2)  single=true include_query_id=false use_raw_path=true detailed_output=true overwrite=true;"
  echo ">>>> streaming load: $1.parquet $4 $5:"
  (set -x; curl -sS -H "x-databend-query-id:load-$1" -H "sql:insert into streaming_load_parquet($3) values file_format = (type='parquet', missing_field_as=$4, null_if=($5))" -F "upload=@$DATA/$1.parquet" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load")
  echo
  echo "<<<<"
  query "select * from streaming_load_parquet;"
  stmt "truncate table streaming_load_parquet"
}

run "q1" "'2021-01-01' as c3, '1' as c2"  "c2,c3" "error" ""
run "q2" "'2021-01-01' as c3"  "c2,c3" "error" ""
run "q3" "'2021-01-01' as c3"  "c2,c3" "field_default" ""
run "q4" "'2021-01-01' as c3, 'my_null' as c1"  "c1,c3" "error" ""
run "q5" "'2021-01-01' as c3, 'my_null' as c1"  "c1,c3" "error" "'my_null'"

stmt "drop table if exists streaming_load_parquet"
