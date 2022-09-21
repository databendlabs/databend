#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


cat << EOF > /tmp/databend_test_csv.txt
insert into a(a,b,c) format CSV 100,'2',100.3
200,'3',200.4
300,'2',300

EOF


cat << EOF > /tmp/databend_test_csv_names.txt
insert into a(a,b,c) format CSVWithNames a,b,c
100,'2',100.3
200,'3',200.4
300,'2',300

EOF

cat << EOF > /tmp/databend_test_csv_names_and_types.txt
insert into a(a,b,c) format CSVWithNamesAndTypes a,b,c
'int','varchar','double'
100,'2',100.3
200,'3',200.4
300,'2',300

EOF

cat << EOF > /tmp/databend_test_tsv_names_and_types.txt
insert into a(a,b,c) format TabSeparatedWithNamesAndTypes a	b	c
'int'	'varchar'	'double'
100	2	100.3
200	3	200.4
300	2	300

EOF


cat << EOF > /tmp/databend_test_values.txt
insert into a(a,b,c) format VALUES (100,'2',100.3),
(200,'3',200.4),
(300,'2',300)

EOF

cat << EOF > /tmp/databend_test_ndjson.txt
insert into a(a,b,c) format NDJSON {"a": 100, "b": "2", "c": 100.3}
{"a": 200, "b": "3", "c": 200.4}
EOF


for i in `seq 1 10000`;do
	echo '{"a": 0, "b": "3", "c": 0}' >>  /tmp/databend_test_ndjson.txt
done

curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "create table a ( a int, b varchar, c double)"


curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" --data-binary @/tmp/databend_test_csv.txt
curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" --data-binary @/tmp/databend_test_csv_names.txt
curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" --data-binary @/tmp/databend_test_csv_names_and_types.txt
curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" --data-binary @/tmp/databend_test_tsv_names_and_types.txt

curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" --data-binary @/tmp/databend_test_values.txt
curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" --data-binary @/tmp/databend_test_ndjson.txt

# Flaky test: wait for https://github.com/datafuselabs/databend/issues/7657
#curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "SELECT count(), sum(a), min(b), sum(c) from a"
curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "drop table a"

rm /tmp/databend_test*.txt
