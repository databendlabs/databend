#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=testuser1 --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export TEST_USER2_CONNECT="bendsql --user=testuser2 --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo '-- reset users'
echo "DROP USER IF EXISTS 'testuser1'" | $BENDSQL_CLIENT_CONNECT
echo "DROP USER IF EXISTS 'testuser2'" | $BENDSQL_CLIENT_CONNECT

echo '-- prepare user and tables for tests'
echo "CREATE USER 'testuser1' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT
echo "CREATE USER 'testuser2' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT
echo "alter user 'testuser2' identified by '123'" | $TEST_USER_CONNECT 2>&1 | grep 'Permission denied: privilege \[Alter\]' |wc -l
echo "alter user 'testuser1' identified by '123' with default_role='role1'" | $TEST_USER_CONNECT  2>&1 | grep 'Permission denied: privilege \[Alter\]' |wc -l
echo "alter user 'testuser1' identified by '123' with disabled=true" | $TEST_USER_CONNECT  2>&1 | grep 'Permission denied: privilege \[Alter\]' |wc -l

# Note: this query in bendsql will return err, because bendsql will call auth in poll, after password modified, in next poll the auth failed, it will return err.
# testuser1@localhost:8000/default> alter user 'testuser1' identified by '123';
# error: APIError: RequestError: Query Page failed with status 401 Unauthorized: {"error":{"code":"401","message":"wrong password"}}
echo "alter user 'testuser1' identified by '123'" | $TEST_USER_CONNECT 2>&1 | grep 'wrong password' | wc -l

export TEST_USER_MODIFY_CONNECT="bendsql --user=testuser1 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo "select 1" | $TEST_USER_CONNECT  2>&1 | grep 'wrong password' | wc -l
echo "select 'testuser1 password is 123'" | $TEST_USER_MODIFY_CONNECT
echo "select 'testuser2 password not modify'" | $TEST_USER2_CONNECT

echo '-- reset users'
echo "DROP USER IF EXISTS 'testuser1'" | $BENDSQL_CLIENT_CONNECT
echo "DROP USER IF EXISTS 'testuser2'" | $BENDSQL_CLIENT_CONNECT
