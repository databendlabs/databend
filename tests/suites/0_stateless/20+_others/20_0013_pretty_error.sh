#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP TABLE IF EXISTS t;" | bendsql_connect_root
echo "DROP TABLE IF EXISTS t1;" | bendsql_connect_root
echo "DROP TABLE IF EXISTS t2;" | bendsql_connect_root
echo "DROP TABLE IF EXISTS t3;" | bendsql_connect_root

echo "select *;" |  bendsql_connect_root
echo "select * from t;" |  bendsql_connect_root
echo "select base64(1);" |  bendsql_connect_root
echo "select to_base64(1);" | bendsql_connect_root
echo "select truncate('xx', -1);" | bendsql_connect_root
echo "select 1 + 'a';" | bendsql_connect_root
echo "select cast((1::int null,'a') as tuple(string,int not null)).1 +3;" | bendsql_connect_root

echo "create table t1 (a tuple(b int, c int) not null)" | bendsql_connect_root
echo "select t1.a:z from t" | bendsql_connect_root

echo "create table t2 (a int not null, b int not null)" | bendsql_connect_root
echo "create table t3 (c int not null, d int not null)" | bendsql_connect_root
echo "select * from t2 join t3 using (c)" | bendsql_connect_root

echo "DROP TABLE t;" | bendsql_connect_root
echo "DROP TABLE t1;" | bendsql_connect_root
echo "DROP TABLE t2;" | bendsql_connect_root
echo "DROP TABLE t3;" | bendsql_connect_root
