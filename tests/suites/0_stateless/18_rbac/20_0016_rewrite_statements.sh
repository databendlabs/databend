#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=testuser1 --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo '-- reset users'
echo "DROP USER IF EXISTS 'testuser1'" | $BENDSQL_CLIENT_CONNECT

echo '-- prepare user and tables for tests'
echo "CREATE USER 'testuser1' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE IF NOT EXISTS t20_0016_table1(c int not null)" | $BENDSQL_CLIENT_CONNECT
echo "GRANT SELECT ON default.t20_0016_table1 TO testuser1" | $BENDSQL_CLIENT_CONNECT

echo '-- ensure the statements not break with PUBLIC role'
echo "SHOW TABLES;" | $TEST_USER_CONNECT > /dev/null
echo "SHOW DATABASES;" | $TEST_USER_CONNECT > /dev/null
echo "SHOW USERS;" | $TEST_USER_CONNECT > /dev/null
echo "SHOW ROLES;" | $TEST_USER_CONNECT > /dev/null
echo "SHOW STAGES;" | $TEST_USER_CONNECT > /dev/null
echo "SHOW PROCESSLIST;" | $TEST_USER_CONNECT > /dev/null