#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql_connect_user testuser1 password -A"

echo '-- reset users'
echo "DROP USER IF EXISTS 'testuser1'" | bendsql_connect_root

echo '-- prepare user and tables for tests'
echo "CREATE USER 'testuser1' IDENTIFIED BY '$TEST_USER_PASSWORD'" | bendsql_connect_root
echo "CREATE TABLE IF NOT EXISTS t20_0016_table1(c int not null)" | bendsql_connect_root
echo "GRANT SELECT ON default.t20_0016_table1 TO testuser1" | bendsql_connect_root

echo '-- ensure the statements not break with PUBLIC role'
echo "SHOW TABLES;" | $TEST_USER_CONNECT > /dev/null
echo "SHOW DATABASES;" | $TEST_USER_CONNECT > /dev/null
echo "SHOW USERS;" | $TEST_USER_CONNECT > /dev/null
echo "SHOW ROLES;" | $TEST_USER_CONNECT > /dev/null
echo "SHOW STAGES;" | $TEST_USER_CONNECT > /dev/null
echo "SHOW PROCESSLIST;" | $TEST_USER_CONNECT > /dev/null