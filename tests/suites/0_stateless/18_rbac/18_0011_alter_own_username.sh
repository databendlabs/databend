#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql_connect_user testuser1 password -A"
export TEST_USER2_CONNECT="bendsql_connect_user testuser2 password -A"

echo '-- reset users'
echo "DROP USER IF EXISTS 'testuser1'" | bendsql_connect_root
echo "DROP USER IF EXISTS 'testuser2'" | bendsql_connect_root

echo '-- prepare user and tables for tests'
echo "CREATE USER 'testuser1' IDENTIFIED BY '$TEST_USER_PASSWORD'" | bendsql_connect_root
echo "CREATE USER 'testuser2' IDENTIFIED BY '$TEST_USER_PASSWORD'" | bendsql_connect_root
echo "alter user 'testuser2' identified by '123'" | $TEST_USER_CONNECT 2>&1 | grep 'Permission denied: privilege \[Alter\]' |wc -l
echo "alter user 'testuser1' identified by '123' with default_role='role1'" | $TEST_USER_CONNECT  2>&1 | grep 'Permission denied: privilege \[Alter\]' |wc -l
echo "alter user 'testuser1' identified by '123' with disabled=true" | $TEST_USER_CONNECT  2>&1 | grep 'Permission denied: privilege \[Alter\]' |wc -l

# bendsql keeps polling this query with the old password after it changes the
# password. The submission credential stays bound to its own query, so polling
# succeeds instead of failing with "incorrect password".
echo "alter user 'testuser1' identified by '123'" | $TEST_USER_CONNECT 2>&1 | grep 'incorrect password' | wc -l

export TEST_USER_MODIFY_CONNECT="bendsql_connect_user testuser1 123 -A"

echo "select 1" | $TEST_USER_CONNECT  2>&1 | grep 'incorrect password' | wc -l
echo "select 'testuser1 password is 123'" | $TEST_USER_MODIFY_CONNECT
echo "select 'testuser2 password not modify'" | $TEST_USER2_CONNECT

echo '-- reset users'
echo "DROP USER IF EXISTS 'testuser1'" | bendsql_connect_root
echo "DROP USER IF EXISTS 'testuser2'" | bendsql_connect_root
