#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cat << EOF > /tmp/databend_test_csv_error1.txt
insert into a(a,b,c) format CSV
'2023-04-08 01:01:01', "Hello", 12345678
'19892-02-03 15:23:23', "World", 123456

EOF

cat << EOF > /tmp/databend_test_csv_error2.txt
insert into a(a,b,c) format CSV '2023-04-08 01:01:01', "Hello", 12345678,1

EOF

cat << EOF > /tmp/databend_test_csv_error3.txt
insert into a(a,b,c) format CSV '2023-04-08 01:01:01',,123Hello

EOF

cat << EOF > /tmp/databend_test_tsv_error1.txt
insert into a(a,b,c) format TSV
'2023-04-08 01:01:01'	"Hello"	12345678
1989-023-03 15:23:23	"World"	123456

EOF

cat << EOF > /tmp/databend_test_tsv_error2.txt
insert into a(a,b,c) format TSV
2023-04-08 01:01:01	"Hello"	12345678
1989-02-03 15:23:23	"World"	123456 1

EOF

cat << EOF > /tmp/databend_test_tsv_error3.txt
insert into a(a,b,c) format TSV
2023-04-08 01:01:01		123Hello

EOF

curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "create table a ( a datetime, b string, c int)"

curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_csv_error1.txt
echo -e '\n'
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_csv_error2.txt
echo -e '\n'
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_csv_error3.txt
echo -e '\n'

curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_tsv_error1.txt
echo -e '\n'
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_tsv_error2.txt
echo -e '\n'
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_tsv_error3.txt

curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "drop table a"

rm /tmp/databend_test*.txt
