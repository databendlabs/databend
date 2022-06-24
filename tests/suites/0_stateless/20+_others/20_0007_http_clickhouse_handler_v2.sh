#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


cat << EOF > /tmp/csv.txt
insert into a(a,b,c) format CSV 100,'2',100.3
200,'3',200.4
300,'2',300

EOF


cat << EOF > /tmp/csv_names.txt
insert into a(a,b,c) format CSVWithNames a,b,c
100,'2',100.3
200,'3',200.4
300,'2',300

EOF

cat << EOF > /tmp/csv_names_and_types.txt
insert into a(a,b,c) format CSVWithNamesAndTypes a,b,c
'int','varchar','double'
100,'2',100.3
200,'3',200.4
300,'2',300

EOF

cat << EOF > /tmp/tsv_names_and_types.txt
insert into a(a,b,c) format TabSeparatedWithNamesAndTypes a	b	c
'int'	'varchar'	'double'
100	'2'	100.3
200	'3'	200.4
300	'2'	300

EOF


cat << EOF > /tmp/values.txt
insert into a(a,b,c) format VALUES (100,'2',100.3),
(200,'3',200.4),
(300,'2',300)

EOF

curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "create table a ( a int, b varchar, c double)"


curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/csv.txt
curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/csv_names.txt
curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/csv_names_and_types.txt
curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/tsv_names_and_types.txt

curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" --data-binary @/tmp/values.txt
curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "SELECT sum(a), min(b), sum(c) from a"

curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}/?enable_planner_v2=1" -d "drop table a"


# rm /tmp/*.txt
