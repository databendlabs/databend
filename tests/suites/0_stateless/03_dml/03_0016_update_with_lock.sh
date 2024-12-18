#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop database if exists test_update" | $BENDSQL_CLIENT_CONNECT

echo "CREATE DATABASE test_update" | $BENDSQL_CLIENT_CONNECT
echo "create table test_update.t(a int, b int)" | $BENDSQL_CLIENT_CONNECT
echo "set global enable_table_lock = 1" | $BENDSQL_CLIENT_CONNECT

for i in $(seq 1 10);do
	(
		j=$(($i+1))
		echo "insert into test_update.t values($i, $j)" | $BENDSQL_CLIENT_OUTPUT_NULL
	)&
done
wait

echo "optimize table test_update.t compact" | $BENDSQL_CLIENT_CONNECT
echo "select count() from test_update.t where a + 1 = b" | $BENDSQL_CLIENT_CONNECT

echo "Test table lock for update"
for i in $(seq 1 10);do
	(
		echo "update test_update.t set b = $i where a = $i" | $BENDSQL_CLIENT_OUTPUT_NULL
	)&
done
wait

echo "select count() from test_update.t where a = b" | $BENDSQL_CLIENT_CONNECT

echo "drop table test_update.t all" | $BENDSQL_CLIENT_CONNECT
echo "drop database test_update" | $BENDSQL_CLIENT_CONNECT
