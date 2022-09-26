#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/" -d "drop table if exists a"
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "create table a ( a datetime, b string, c int)"

# 1 bad date
echo -e 'csv 1'
cat << EOF > /tmp/databend_test_csv_error1.txt
insert into a(a,b,c) format CSV
"2023-04-08 01:01:01","Hello",12345678
"19892-02-03 15:23:23","World",123456
EOF
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_csv_error1.txt | grep -c "Date"

# 2 one more column
echo -e '\ncsv 2'
cat << EOF > /tmp/databend_test_csv_error2.txt
insert into a(a,b,c) format CSV "2023-04-08 01:01:01", "Hello",12345678,1

EOF
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_csv_error2.txt | grep -c "allow ending"

# 3 bad number
echo -e '\ncsv 3'
cat << EOF > /tmp/databend_test_csv_error3.txt
insert into a(a,b,c) format CSV "2023-04-08 01:01:01",,123Hello

EOF
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_csv_error3.txt | grep -c "column 2: bad field end"

# 1 bad date
echo -e '\ntsv 1'
cat << EOF > /tmp/databend_test_tsv_error1.txt
insert into a(a,b,c) format TSV
"2023-04-08 01:01:01"	"Hello"	12345678
1989-023-03 15:23:23	"World"	123456
EOF
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_tsv_error1.txt | grep -c "Date"

# 2 one more column
echo -e '\ntsv 2'
cat << EOF > /tmp/databend_test_tsv_error2.txt
insert into a(a,b,c) format TSV
2023-04-08 01:01:01	"Hello"	12345678
1989-02-03 15:23:23	"World"	123456 1
EOF
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_tsv_error2.txt | grep -c "column 2: bad field end"

echo -e '\ntsv 3'
# 3 bad number
cat << EOF > /tmp/databend_test_tsv_error3.txt
insert into a(a,b,c) format TSV
2023-04-08 01:01:01		123Hello
EOF
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/databend_test_tsv_error3.txt | grep -c "column 2: bad field end"

# cleanup
curl -s -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "drop table a"
rm /tmp/databend_test*.txt
