#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists variant_test;" | bendsql_connect_root
echo "drop table if exists variant_test2;" | bendsql_connect_root
## create variant_test and variant_test2 table
cat ${TESTS_DATA_DIR}/ddl/variant_test.sql | bendsql_connect_root

# run format path table
function run() {
  curl -sS -H "x-databend-query-id:$2" -H "X-Databend-SQL:insert into $3 values  from @_databend_load file_format = ($1)" -F "upload=@/${TESTS_DATA_DIR}/$2" -u root: -XPUT "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/streaming_load" | jq -r '.id, .stats.rows'
  echo
}

# load csv
run "type = CSV field_delimiter = ',' quote = '\''" "csv/json_sample1.csv" "variant_test"
run "type = CSV field_delimiter = '|' quote = '\''" "csv/json_sample2.csv" "variant_test"

echo "select * from variant_test order by Id asc;" | bendsql_connect_root

# load ndjson
run "type = ndjson" "ndjson/json_sample.ndjson" "variant_test2"
echo "select * from variant_test2 order by b asc;" | bendsql_connect_root

echo "drop table variant_test;" | bendsql_connect_root
echo "drop table variant_test2;" | bendsql_connect_root
