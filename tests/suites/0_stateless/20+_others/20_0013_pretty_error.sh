#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP TABLE IF EXISTS t;" | $BENDSQL_CLIENT_CONNECT
echo "DROP TABLE IF EXISTS t1;" | $BENDSQL_CLIENT_CONNECT
echo "DROP TABLE IF EXISTS t2;" | $BENDSQL_CLIENT_CONNECT
echo "DROP TABLE IF EXISTS t3;" | $BENDSQL_CLIENT_CONNECT

echo "select *;" |  $BENDSQL_CLIENT_CONNECT
echo "select * from t;" |  $BENDSQL_CLIENT_CONNECT
echo "select base64(1);" |  $BENDSQL_CLIENT_CONNECT
echo "select to_base64(1);" | $BENDSQL_CLIENT_CONNECT
echo "select 1 + 'a';" | $BENDSQL_CLIENT_CONNECT

echo "create table t1 (a tuple(b int, c int) not null)" | $BENDSQL_CLIENT_CONNECT
echo "select t1.a:z from t" | $BENDSQL_CLIENT_CONNECT

echo "create table t2 (a int not null, b int not null)" | $BENDSQL_CLIENT_CONNECT
echo "create table t3 (c int not null, d int not null)" | $BENDSQL_CLIENT_CONNECT
echo "select * from t2 join t3 using (c)" | $BENDSQL_CLIENT_CONNECT

echo "DROP TABLE t;" | $BENDSQL_CLIENT_CONNECT
echo "DROP TABLE t1;" | $BENDSQL_CLIENT_CONNECT
echo "DROP TABLE t2;" | $BENDSQL_CLIENT_CONNECT
echo "DROP TABLE t3;" | $BENDSQL_CLIENT_CONNECT
