#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "create or replace table t1 (a int, b string);"

# run format path table
function run() {
	echo ">>>> load $2 with format ($1)"
  curl -sS \
  -H "x-databend-query-id:$2" \
  -H "X-Databend-SQL:insert into t1 values from @_databend_load  file_format = ($1)" \
  -F "upload=@/${TESTS_DATA_DIR}/$2" \
  -u root: -XPUT \
  "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | jq .error.code
	echo "<<<<"
	query "select * from t1"
}


# parse error
run "typ=csv " "csv/it.csv"
# bind error
run "format_name='not_exist'" "csv/it.csv"
# file parse error
run "type=csv" "parquet/tuple.parquet"
run "type=parquet" "ontime_200.csv"

# file decode error
run "type=csv" "csv/header_only.csv"


stmt "drop table t1;"