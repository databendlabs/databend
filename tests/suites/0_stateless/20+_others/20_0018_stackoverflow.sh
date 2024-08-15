#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP TABLE IF EXISTS t1"|$BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE t1(id VARCHAR NULL, timestamp TIMESTAMP NULL, type VARCHAR NULL)" |$BENDSQL_CLIENT_CONNECT

SQL="SELECT * FROM t1 WHERE 1 = 1 AND((timestamp = '2024-05-05 18:05:20' AND type = '1' AND id = 'xx')"

for i in `seq 1 300`;do
  SQL="$SQL OR (timestamp = '2024-05-05 18:05:20' AND type = '1' AND id = 'xx')"
done

SQL="$SQL)"

echo "$SQL"|$BENDSQL_CLIENT_CONNECT


SQL="a > 0 "

for i in `seq 1 100`;do
  SQL="$SQL OR (a > ${i})"
done 

SQL="$SQL OR (a / 0 > 0)"

echo "SELECT $SQL from numbers(1) t(a)" | $BENDSQL_CLIENT_CONNECT

echo "DROP TABLE IF EXISTS t1"|$BENDSQL_CLIENT_CONNECT
