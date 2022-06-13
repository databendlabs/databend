#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


cat << EOF > /tmp/csv_with_sql.txt
insert into a(a,b,c) format CSV 100,'2',100.3
200,'3',200.4
300,'2',300

EOF



cat << EOF > /tmp/values_with_sql.txt
insert into a(a,b,c) format VALUES (100,'2',100.3),
(200,'3',200.4),
(300,'2',300)

EOF

curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "create table a ( a int, b varchar, c double)"


curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" --data-binary @/tmp/csv_with_sql.txt
curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" --data-binary @/tmp/values_with_sql.txt
curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "SELECT sum(a), min(b), sum(c) from a"

curl -s  -u 'root:' -XPOST "http://localhost:${QUERY_CLICKHOUSE_HTTP_HANDLER_PORT}" -d "drop table a"


# rm /tmp/*.txt
