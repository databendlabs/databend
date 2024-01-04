#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_NAME="owner"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=owner --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

export TEST_TRANSFER_USER_CONNECT="bendsql --user=owner1 --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo "set global enable_experimental_rbac_check=1" | $BENDSQL_CLIENT_CONNECT

## cleanup
echo "drop database if exists d_0002;" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists '${TEST_USER_NAME}'" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists 'owner1'" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists 'r_0002'" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists 'r_0002_1'" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists hello" | $BENDSQL_CLIENT_CONNECT
echo "drop function if exists a" | $BENDSQL_CLIENT_CONNECT

## create user
echo "create user '${TEST_USER_NAME}' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT
## create role
echo 'create role `r_0002`' | $BENDSQL_CLIENT_CONNECT
echo "GRANT ROLE 'r_0002' TO '${TEST_USER_NAME}'" | $BENDSQL_CLIENT_CONNECT
echo "GRANT CREATE, SUPER ON *.* TO ROLE 'r_0002'" | $BENDSQL_CLIENT_CONNECT

echo "create ROLE r_0002_1" | $BENDSQL_CLIENT_CONNECT
echo "create user 'owner1' IDENTIFIED BY 'password'" | $BENDSQL_CLIENT_CONNECT
echo "grant ROLE r_0002_1 to user owner1" | $BENDSQL_CLIENT_CONNECT

echo "set default role 'r_0002'" | $TEST_USER_CONNECT

## database/table
echo "=== test db/table ==="
echo "create database d_0002" | $TEST_USER_CONNECT
echo "create table d_0002.t(id int)" | $TEST_USER_CONNECT
echo "insert into d_0002.t values(200)" | $TEST_USER_CONNECT
echo "select * from d_0002.t" | $TEST_USER_CONNECT

## stage
echo "=== test stage ==="
echo 'create stage hello' | $TEST_USER_CONNECT
echo 'COPY INTO @hello from (select number from numbers(1)) FILE_FORMAT = (type = parquet)' | $TEST_USER_CONNECT
echo 'select * from @hello' | $TEST_USER_CONNECT

## udf
echo "=== test udf ==="
echo "create function a as (a) -> (a+1);" | $TEST_USER_CONNECT
echo "select a(1);" | $TEST_USER_CONNECT

# ownership transfer
echo "=== test ownership r_0002 transfer to r_0002_1 ==="
echo "GRANT OWNERSHIP on d_0002.* to role 'r_0002_1'" | $BENDSQL_CLIENT_CONNECT
echo "GRANT OWNERSHIP on d_0002.t to role 'r_0002_1'" | $BENDSQL_CLIENT_CONNECT
echo "GRANT OWNERSHIP on stage hello to role 'r_0002_1'" | $BENDSQL_CLIENT_CONNECT
echo "GRANT OWNERSHIP on udf a to role 'r_0002_1'" | $BENDSQL_CLIENT_CONNECT
echo "set default role 'r_0002_1'" | $TEST_TRANSFER_USER_CONNECT
echo "=== test role r_0002_1 ==="
echo "create table d_0002.t1(id int)" | $TEST_TRANSFER_USER_CONNECT
echo "select a(1);" | $TEST_TRANSFER_USER_CONNECT
echo 'select * from @hello' | $TEST_TRANSFER_USER_CONNECT
echo "select * from d_0002.t" | $TEST_TRANSFER_USER_CONNECT

echo "=== test role r_0002 ==="
echo "create table d_0002.t2(id int)" | $TEST_TRANSFER_USER_CONNECT
echo "select a(1);" | $TEST_USER_CONNECT
echo 'select * from @hello' | $TEST_USER_CONNECT
echo "select * from d_0002.t" | $TEST_USER_CONNECT

## cleanup
echo "drop database d_0002;" | $BENDSQL_CLIENT_CONNECT
echo "drop stage hello;" | $BENDSQL_CLIENT_CONNECT
echo "drop function a;" | $BENDSQL_CLIENT_CONNECT
echo "drop user '${TEST_USER_NAME}'" | $BENDSQL_CLIENT_CONNECT
echo "drop user 'owner1'" | $BENDSQL_CLIENT_CONNECT
echo "drop role 'r_0002_1'" | $BENDSQL_CLIENT_CONNECT
echo "drop role 'r_0002'" | $BENDSQL_CLIENT_CONNECT
echo "unset enable_experimental_rbac_check" | $BENDSQL_CLIENT_CONNECT
