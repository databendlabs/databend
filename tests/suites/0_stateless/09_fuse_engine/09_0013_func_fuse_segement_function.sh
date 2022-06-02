#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


## Create table t09_0011
echo "create table t09_0011(c int)" | $MYSQL_CLIENT_CONNECT
echo "insert into t09_0011 values(1)" | $MYSQL_CLIENT_CONNECT
echo "show value of table being cloned"
echo "select *  from t09_0011" | $MYSQL_CLIENT_CONNECT

## Get the snapshot id
SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','t09_0011')" | mysql -h127.0.0.1 -P3307 -uroot -s)

## Get segments
echo "select count(*)>0 from fuse_segment('default', 't09_0011', '$SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

## Drop table.
echo "drop table  t09_0011" | $MYSQL_CLIENT_CONNECT
