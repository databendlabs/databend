#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "create or replace table t1 (a int, b string);"

# run format path label
function run() {
	echo ">>>> load $2 with format ($1), with label $3"
  curl -sS \
  -H "x-databend-query-id:$2" \
  -H "X-Databend-SQL:insert /*+ set_var(deduplicate_label='$3') */ into t1  from @_databend_load file_format = ($1)" \
  -F "upload=@/${TESTS_DATA_DIR}/$2" \
  -u root: -XPUT \
  "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | jq -r '.id, .stats.rows'
  echo
  echo "<<<<"
}

# load csv
run "type = CSV" "csv/it.csv" "L1"
query "select * from t1 order by a"
run "type = CSV" "csv/it.csv" "L1"
query "select * from t1 order by a"
run "type = CSV" "csv/it.csv" "L2"
query "select * from t1 order by a"

stmt "drop table t1;"
