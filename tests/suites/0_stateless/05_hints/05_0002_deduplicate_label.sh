#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# mariadb mysql client has some bug, please use mysql official client
# mysql --version
# mysql  Ver 8.0.32-0ubuntu0.20.04.2 for Linux on x86_64 ((Ubuntu))
echo "drop table if exists t5;" | bendsql_connect_root
echo "drop stage if exists s5;" | bendsql_connect_root
echo "drop stage if exists s5_1;" | bendsql_connect_root

echo "CREATE TABLE t5(a Int, b bool) Engine = Fuse;" | bendsql_connect_root

echo "INSERT /*+ SET_VAR(deduplicate_label='insert-test') */ INTO t5 (a, b) VALUES(1, false)" | bendsql_connect_root_null
echo "INSERT /*+ SET_VAR(deduplicate_label='insert-test') */ INTO t5 (a, b) VALUES(1, false)" | bendsql_connect_root_null
echo "select * from t5" | bendsql_connect_root

echo "CREATE STAGE s5_1;" | bendsql_connect_root
echo "copy /*+SET_VAR(deduplicate_label='copy-test')*/ into @s5_1 from (select * from t5);" | $MYSQL_CLINEENRT_CONNECT
echo "select * from @s5_1;" | $MYSQL_CLINEENRT_CONNECT
echo "CREATE STAGE s5;" | $MYSQL_CLINEENRT_CONNECT
echo "copy /*+SET_VAR(deduplicate_label='copy-test')*/ into @s5 from (select * from t5);" | $MYSQL_CLINEENRT_CONNECT
echo "select * from @s5;" | $MYSQL_CLINEENRT_CONNECT

echo "UPDATE /*+ SET_VAR(deduplicate_label='update-test') */ t5 SET a = 20 WHERE b = false;" | bendsql_connect_root_null
echo "UPDATE /*+ SET_VAR(deduplicate_label='update-test') */ t5 SET a = 30 WHERE b = false;" | bendsql_connect_root_null
echo "select * from t5" | bendsql_connect_root

echo "replace /*+ SET_VAR(deduplicate_label='replace-test') */ into t5 on(a,b) values(40,false);" | bendsql_connect_root_null
echo "replace /*+ SET_VAR(deduplicate_label='replace-test') */ into t5 on(a,b) values(50,false);" | bendsql_connect_root_null
echo "select * from t5 order by a" | bendsql_connect_root

echo "drop table if exists t5;" | bendsql_connect_root
echo "drop stage if exists s5;" | bendsql_connect_root
echo "drop stage if exists s5_1;" | bendsql_connect_root
