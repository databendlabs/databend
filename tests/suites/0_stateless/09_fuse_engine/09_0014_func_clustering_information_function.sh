#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


## Create table t09_0014
echo "create table t09_0014(a int, b int) cluster by(b,a)" | $MYSQL_CLIENT_CONNECT
echo "insert into t09_0014 values(0,3),(1,1);" | $MYSQL_CLIENT_CONNECT
echo "insert into t09_0014 values(1,3),(2,1);" | $MYSQL_CLIENT_CONNECT
echo "insert into t09_0014 values(4,4);" | $MYSQL_CLIENT_CONNECT
echo "show value of table being cloned"
echo "select *  from t09_0014 order by b, a" | $MYSQL_CLIENT_CONNECT

## Get the clustering information
echo "select * from clustering_information('default','t09_0014')" | $MYSQL_CLIENT_CONNECT

## Drop table.
echo "drop table  t09_0014" | $MYSQL_CLIENT_CONNECT
