#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cat << EOF > /tmp/databend_test_tsv_bigint.txt
insert into t1(a) format TSV  5632622125792883430
EOF


curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "create table t1 (a bigint)"

echo "---bigint"
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_tsv_bigint.txt
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "select * from t1"

curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "drop table t1"

rm /tmp/databend_test*.txt
