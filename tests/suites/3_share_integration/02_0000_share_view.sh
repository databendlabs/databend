#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../shell_env.sh

echo "drop test share and database"
echo "drop share if exists test_share" | $MYSQL_CLIENT_PROVIDER_CONNECT
echo "drop database if exists provider_db" | $MYSQL_CLIENT_PROVIDER_CONNECT
echo "drop database if exists provider_ref_db" | $MYSQL_CLIENT_PROVIDER_CONNECT
echo "drop share endpoint if exists ed" | $MYSQL_CLIENT_CONSUMER_CONNECT
echo "drop catalog if exists share_catalog" | $MYSQL_CLIENT_CONSUMER_CONNECT

echo "create provider share and database\table"
echo "create database provider_db" | $MYSQL_CLIENT_PROVIDER_CONNECT

echo "create provider view and reference database\table"
echo "create database provider_ref_db;" | $MYSQL_CLIENT_PROVIDER_CONNECT
echo "create table provider_ref_db.t2 (a int);" | $MYSQL_CLIENT_PROVIDER_CONNECT
echo "insert into provider_ref_db.t2 values(3),(4);"  | $MYSQL_CLIENT_PROVIDER_CONNECT
echo "create view provider_db.v as select * from provider_ref_db.t2;" | $MYSQL_CLIENT_PROVIDER_CONNECT

echo "create provider share and grant access"
echo "CREATE SHARE test_share;" | $MYSQL_CLIENT_PROVIDER_CONNECT
echo "GRANT USAGE ON DATABASE provider_db TO SHARE test_share;" | $MYSQL_CLIENT_PROVIDER_CONNECT
echo "GRANT REFERENCE_USAGE on database provider_ref_db to share test_share;" | $MYSQL_CLIENT_PROVIDER_CONNECT
echo "GRANT SELECT on view provider_db.v to share test_share;" | $MYSQL_CLIENT_PROVIDER_CONNECT
echo "ALTER SHARE test_share ADD TENANTS = consumer;" | $MYSQL_CLIENT_PROVIDER_CONNECT

echo "create consumer catalog from share"
echo "CREATE SHARE ENDPOINT IF NOT EXISTS ed URL='http://127.0.0.1:22322' CREDENTIAL=(TYPE='HMAC' KEY='hello');" | $MYSQL_CLIENT_CONSUMER_CONNECT
echo "CREATE CATALOG share_catalog type=share connection=(name='provider.test_share' endpoint='ed')" | $MYSQL_CLIENT_CONSUMER_CONNECT
echo "select * from share_catalog.provider_db.v order by a;" | $MYSQL_CLIENT_CONSUMER_CONNECT
