#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cat << EOF > /tmp/databend_test_tsv_bigint.txt
insert into t1(a) format TSV  5632622125792883430
EOF

cat << EOF > /tmp/databend_test_tsv_escape.txt
insert into t2(a, b) format TSV  1a\nb	c\td
2a\x20b	c\Nd
EOF


echo "---bigint"
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "drop table if exists t1"
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "create table t1 (a bigint)"
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1&default_format=TSV" --data-binary @/tmp/databend_test_tsv_bigint.txt
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "select * from t1"
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "drop table t1"

echo "---escape"
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "drop table if exists t2"
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "create table t2 (a string, b string)"
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1&default_format=TSV" --data-binary @/tmp/databend_test_tsv_escape.txt
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "select * from t2"
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "drop table t2"
rm /tmp/databend_test*.txt
