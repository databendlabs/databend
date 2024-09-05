#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../shell_env.sh

export TEST_USER_NAME="owner"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=owner --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_MYSQL_HANDLER_SHARE_CONSUMER_PORT}"

echo "drop test share and database"
echo "drop share if exists test_share" | $BENDSQL_CLIENT_PROVIDER_CONNECT
echo "drop database if exists provider_db" | $BENDSQL_CLIENT_PROVIDER_CONNECT
echo "drop database if exists provider_ref_db" | $BENDSQL_CLIENT_PROVIDER_CONNECT
echo "drop share endpoint if exists ed" | $BENDSQL_CLIENT_CONSUMER_CONNECT
echo "drop catalog if exists share_catalog" | $BENDSQL_CLIENT_CONSUMER_CONNECT
echo "drop user if exists '${TEST_USER_NAME}'" | $BENDSQL_CLIENT_CONSUMER_CONNECT
echo "drop role if exists 'r_0002'" | $BENDSQL_CLIENT_CONSUMER_CONNECT

echo "create provider share and database\table"
echo "create database provider_db" | $BENDSQL_CLIENT_PROVIDER_CONNECT

echo "create provider view and reference database\table"
echo "create database provider_ref_db;" | $BENDSQL_CLIENT_PROVIDER_CONNECT
echo "create table provider_ref_db.t2 (a int);" | $BENDSQL_CLIENT_PROVIDER_CONNECT
echo "insert into provider_ref_db.t2 values(3),(4);"  | $BENDSQL_CLIENT_PROVIDER_CONNECT
echo "create view provider_db.v as select * from provider_ref_db.t2;" | $BENDSQL_CLIENT_PROVIDER_CONNECT

echo "create provider share and grant access"
echo "CREATE SHARE test_share;" | $BENDSQL_CLIENT_PROVIDER_CONNECT
echo "GRANT USAGE ON DATABASE provider_db TO SHARE test_share;" | $BENDSQL_CLIENT_PROVIDER_CONNECT
echo "GRANT REFERENCE_USAGE on database provider_ref_db to share test_share;" | $BENDSQL_CLIENT_PROVIDER_CONNECT
echo "GRANT SELECT on view provider_db.v to share test_share;" | $BENDSQL_CLIENT_PROVIDER_CONNECT
echo "ALTER SHARE test_share ADD TENANTS = consumer;" | $BENDSQL_CLIENT_PROVIDER_CONNECT

## create user
echo "create user and role"
echo "create user '${TEST_USER_NAME}' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONSUMER_CONNECT
## create role
echo 'create role `r_0002`' | $BENDSQL_CLIENT_CONSUMER_CONNECT
echo "GRANT ROLE 'r_0002' TO '${TEST_USER_NAME}'" | $BENDSQL_CLIENT_CONSUMER_CONNECT

echo "create consumer catalog from share"
echo "CREATE SHARE ENDPOINT IF NOT EXISTS ed URL='http://127.0.0.1:22322' CREDENTIAL=(TYPE='HMAC' KEY='hello');" | $BENDSQL_CLIENT_CONSUMER_CONNECT
echo "CREATE CATALOG share_catalog type=share connection=(name='provider.test_share' endpoint='ed')" | $BENDSQL_CLIENT_CONSUMER_CONNECT
echo "select * from share_catalog.provider_db.v order by a;" | $BENDSQL_CLIENT_CONSUMER_CONNECT

echo "select * from share_catalog.provider_db.v order by a;" | $TEST_USER_CONNECT
echo "GRANT SELECT ON *.* TO ROLE 'r_0002';" | $BENDSQL_CLIENT_CONSUMER_CONNECT
echo "select * from share_catalog.provider_db.v order by a;" | $TEST_USER_CONNECT
