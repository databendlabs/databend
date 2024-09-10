#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


export TEST_USER_PASSWORD="password"
export USER_A_CONNECT="bendsql --user=a --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"


echo "drop user if exists a" | $BENDSQL_CLIENT_CONNECT
echo "create user a identified by '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT
echo "create or replace database grant_db" | $BENDSQL_CLIENT_CONNECT
echo "create table grant_db.t(c1 int not null)" | $BENDSQL_CLIENT_CONNECT
echo "create or replace database nogrant" | $BENDSQL_CLIENT_CONNECT
echo "create table nogrant.t(id int not null)" | $BENDSQL_CLIENT_CONNECT
echo "grant select on default.* to a" | $BENDSQL_CLIENT_CONNECT
echo "grant select on grant_db.t to a" | $BENDSQL_CLIENT_CONNECT
echo "create or replace table default.test_t(id int not null)" | $BENDSQL_CLIENT_CONNECT

echo "=== show grants for a ==="
echo "show grants for a" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "=== show databases ==="
echo "show databases" | $USER_A_CONNECT

echo "=== show tables ==="
echo "show tables from system" | $USER_A_CONNECT
echo "show tables from grant_db" | $USER_A_CONNECT
echo "=== use db ==="
echo "use system" | $USER_A_CONNECT
echo "use information_schema" | $USER_A_CONNECT
echo "use grant_db" | $USER_A_CONNECT
echo "=== show columns ==="
echo "show columns from one from system" | $USER_A_CONNECT
echo "show columns from t from grant_db" | $USER_A_CONNECT
echo "show columns from roles from system" | $USER_A_CONNECT
echo "show columns from keywords from information_schema" | $USER_A_CONNECT
echo "show tables from nogrant" | $USER_A_CONNECT
echo "show columns from t from nogrant" | $USER_A_CONNECT

echo "=== grant system to a ==="
echo "grant select on system.* to a" | $BENDSQL_CLIENT_CONNECT
echo "show tables from system" | $USER_A_CONNECT | echo $?
echo "use system" | $USER_A_CONNECT | echo $?

echo "select count(1) from information_schema.columns where table_schema in ('grant_db');" | $USER_A_CONNECT
echo "select count(1) from information_schema.columns where table_schema in ('nogrant');" | $USER_A_CONNECT

echo "drop database nogrant" | $BENDSQL_CLIENT_CONNECT
echo "drop database grant_db" | $BENDSQL_CLIENT_CONNECT
echo "drop table default.test_t" | $BENDSQL_CLIENT_CONNECT
echo "drop user a" | $BENDSQL_CLIENT_CONNECT
