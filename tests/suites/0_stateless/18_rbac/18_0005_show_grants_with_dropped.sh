#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "drop role if exists role3;" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists c_r;" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists c_r1;" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists c_r2;" | $BENDSQL_CLIENT_CONNECT

echo "create role role3;" | $BENDSQL_CLIENT_CONNECT
echo "create database c_r1" | $BENDSQL_CLIENT_CONNECT
echo "create table c_r1.t1(id int)" | $BENDSQL_CLIENT_CONNECT
echo "create database c_r2" | $BENDSQL_CLIENT_CONNECT
echo "create table c_r2.t2(id int)" | $BENDSQL_CLIENT_CONNECT
echo "create database c_r;" | $BENDSQL_CLIENT_CONNECT
echo "create table c_r.t(id int);" | $BENDSQL_CLIENT_CONNECT
echo "grant ownership on c_r.* to role role3;" | $BENDSQL_CLIENT_CONNECT
echo "grant ownership on c_r.t to role role3;" | $BENDSQL_CLIENT_CONNECT

echo "grant select on c_r.t to role role3;" | $BENDSQL_CLIENT_CONNECT
echo "grant insert on c_r1.* to role role3;" | $BENDSQL_CLIENT_CONNECT
echo "grant update, delete on c_r1.t1 to role role3;" | $BENDSQL_CLIENT_CONNECT
echo "grant select, insert on c_r2.* to role role3;" | $BENDSQL_CLIENT_CONNECT
echo "grant update, delete on c_r2.t2 to role role3;" | $BENDSQL_CLIENT_CONNECT

echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "drop database c_r;" | $BENDSQL_CLIENT_CONNECT
echo "drop database c_r2;" | $BENDSQL_CLIENT_CONNECT

echo "=== drop database c_r , c_r2 ==="
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'

echo "=== undrop database c_r2 ==="
echo "undrop database c_r2" | $BENDSQL_CLIENT_CONNECT
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'

echo "=== undrop database c_r, contain table c_r.t's ownership ==="
echo "undrop database c_r" | $BENDSQL_CLIENT_CONNECT
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'

echo "=== drop database c_r, c_r2, re-create c_r, c_r2 ==="
echo "drop database c_r" | $BENDSQL_CLIENT_CONNECT
echo "drop database c_r2" | $BENDSQL_CLIENT_CONNECT
echo "create database c_r" | $BENDSQL_CLIENT_CONNECT
echo "create database c_r2" | $BENDSQL_CLIENT_CONNECT
echo "show grants for role role3;" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'

echo "drop role if exists role3;" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists c_r;" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists c_r1;" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists c_r2;" | $BENDSQL_CLIENT_CONNECT
