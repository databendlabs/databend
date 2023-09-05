#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_NAME="owner"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="mysql --defaults-extra-file=password1.out --port ${QUERY_MYSQL_HANDLER_PORT} -s"
echo -e "[mysql]\nhost=${QUERY_MYSQL_HANDLER_HOST}\nuser=${TEST_USER_NAME}\npassword=${TEST_USER_PASSWORD}" >> password1.out
echo "drop table if exists d20_0014.table2;" | $MYSQL_CLIENT_CONNECT

## cleanup
echo "drop database if exists d20_0014;" | $MYSQL_CLIENT_CONNECT
echo "drop user if exists '${TEST_USER_NAME}'" | $MYSQL_CLIENT_CONNECT
echo "drop role if exists 'd20_0014_owner'" | $MYSQL_CLIENT_CONNECT

## create user
echo "create user '${TEST_USER_NAME}' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $MYSQL_CLIENT_CONNECT
## create role
echo 'create role `d20_0014_owner`' | $MYSQL_CLIENT_CONNECT

## create database
echo "create database d20_0014" | $MYSQL_CLIENT_CONNECT

## ownership transfer to role d20_0014_owner
echo "GRANT OWNERSHIP ON d20_0014.* TO ROLE 'd20_0014_owner'" | $MYSQL_CLIENT_CONNECT

echo "GRANT ROLE 'd20_0014_owner' TO '${TEST_USER_NAME}'" | $MYSQL_CLIENT_CONNECT
echo "ALTER USER '${TEST_USER_NAME}' WITH DEFAULT_ROLE='20_0014_owner'" | $MYSQL_CLIENT_CONNECT

## owner should have all privileges on the table
echo "create table d20_0014.table1(i int);" | $TEST_USER_CONNECT
echo "insert into d20_0014.table1 values(1),(2),(3);" | $TEST_USER_CONNECT
echo "select * from d20_0014.table1;" | $TEST_USER_CONNECT
echo "rename"
echo "ALTER TABLE d20_0014.table1 RENAME TO \`table2\`" | $TEST_USER_CONNECT
echo "TRUNCATE TABLE d20_0014.table2 PURGE;" | $TEST_USER_CONNECT
echo "insert into d20_0014.table2 values(4),(5);" | $TEST_USER_CONNECT
echo "select * from d20_0014.table2;" | $TEST_USER_CONNECT
echo "drop table d20_0014.table2;" | $TEST_USER_CONNECT

## cleanup
echo "drop database d20_0014;" | $MYSQL_CLIENT_CONNECT
echo "drop user '${TEST_USER_NAME}'" | $MYSQL_CLIENT_CONNECT
echo "drop role 'd20_0014_owner'" | $MYSQL_CLIENT_CONNECT