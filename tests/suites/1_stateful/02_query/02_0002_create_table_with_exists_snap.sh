#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists tt;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists tt_v2;" | $MYSQL_CLIENT_CONNECT

## Create table
echo "create table tt(a Timestamp) engine=Fuse" | $MYSQL_CLIENT_CONNECT
echo  "insert into tt(a) select now() from numbers(10);" | $MYSQL_CLIENT_CONNECT
sleep 1
echo "select count(1) from tt" | $MYSQL_CLIENT_CONNECT
snap_loc=$(echo "set hide_options_in_show_create_table = 0;show create table default.tt" | mysql -uroot -h127.0.0.1 -P3307 -s|awk -F 'SNAPSHOT_LOCATION=' '{print $2}'|awk -F 'STORAGE_FORMAT=' '{print $1}')
echo "create table tt_v2(a Timestamp) SNAPSHOT_LOCATION=${snap_loc}" | $MYSQL_CLIENT_CONNECT

## Select table
echo "select count(1) from tt_v2" | $MYSQL_CLIENT_CONNECT

## Drop table
echo "drop table if exists tt;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists tt_v2;" | $MYSQL_CLIENT_CONNECT

