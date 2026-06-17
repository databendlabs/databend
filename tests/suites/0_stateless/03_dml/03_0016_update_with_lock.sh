#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop database if exists test_update" | bendsql_connect_root

echo "CREATE DATABASE test_update" | bendsql_connect_root
echo "create table test_update.t(a int, b int)" | bendsql_connect_root
echo "set global enable_table_lock = 1" | bendsql_connect_root

for i in $(seq 1 10);do
	(
		j=$(($i+1))
		echo "insert into test_update.t values($i, $j)" | bendsql_connect_root_null
	)&
done
wait

echo "optimize table test_update.t compact" | bendsql_connect_root
echo "select count() from test_update.t where a + 1 = b" | bendsql_connect_root

echo "Test table lock for update"
for i in $(seq 1 10);do
	(
		echo "update test_update.t set b = $i where a = $i" | bendsql_connect_root_null
	)&
done
wait

echo "select count() from test_update.t where a = b" | bendsql_connect_root

echo "drop table test_update.t all" | bendsql_connect_root
echo "drop database test_update" | bendsql_connect_root
