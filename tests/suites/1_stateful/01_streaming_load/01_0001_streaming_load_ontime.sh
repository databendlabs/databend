#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "drop table if exists ontime_streaming_load;" | $BENDSQL_CLIENT_CONNECT
## create ontime table
cat $TESTS_DATA_DIR/ddl/ontime.sql | sed 's/ontime/ontime_streaming_load/g' | $BENDSQL_CLIENT_CONNECT

function run() {
  echo "--$2"
  curl -sS -H "x-databend-query-id:$2" -H "X-Databend-SQL:insert into ontime_streaming_load  from @_databend_load  file_format = ($1)" -F "upload=@/${TESTS_DATA_DIR}/$2" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | jq -r '.id, .stats.rows'
  echo
  echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime_streaming_load;" | $BENDSQL_CLIENT_CONNECT
  echo "truncate table ontime_streaming_load" | $BENDSQL_CLIENT_CONNECT
}

run "type = CSV skip_header = 1" "ontime_200.csv"
run "type = CSV skip_header = 1 compression = 'gzip'" "ontime_200.csv.gz"
run "type = CSV skip_header = 1 compression = 'zstd'" "ontime_200.csv.zst"
run "type = CSV skip_header = 1 compression = 'bz2'" "ontime_200.csv.bz2"
run "type = ndjson" "ontime_200.ndjson"
run "type = parquet" "ontime_200.parquet"
