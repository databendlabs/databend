#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="mysql --defaults-extra-file=password.out --port ${QUERY_MYSQL_HANDLER_PORT} ${MYSQL_DATABASE} -s"
echo -e "[mysql]\nhost=${QUERY_MYSQL_HANDLER_HOST}\nuser=test-user\npassword=${TEST_USER_PASSWORD}" >> password.out

## create user
echo "create user 'test-user'@'$QUERY_MYSQL_HANDLER_HOST' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $MYSQL_CLIENT_CONNECT
## create table
echo "create table t20_0012(c int)" | $MYSQL_CLIENT_CONNECT

## insert data
echo "insert into t20_0012 values(1),(2)" | $TEST_USER_CONNECT
## grant user privilege
echo "GRANT SELECT, INSERT ON * TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
## insert data
echo "insert into t20_0012 values(1),(2)" | $TEST_USER_CONNECT
## verify
echo "select * from t20_0012 order by c" | $TEST_USER_CONNECT

## update data
echo "update t20_0012 set c=3 where c=1" | $TEST_USER_CONNECT
## grant user privilege
echo "GRANT UPDATE ON * TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
## update data
echo "update t20_0012 set c=3 where c=1" | $TEST_USER_CONNECT
## verify
echo "select * from t20_0012 order by c" | $TEST_USER_CONNECT

## delete data
echo "delete from t20_0012 where c=2" | $TEST_USER_CONNECT
## grant user privilege
echo "GRANT DELETE ON * TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
## delete data
echo "delete from t20_0012 where c=2" | $TEST_USER_CONNECT
## verify
echo "select count(*) = 0 from t20_0012 where c=2" | $TEST_USER_CONNECT

## optimize table
echo "optimize table t20_0012 all" | $TEST_USER_CONNECT
## grant user privilege
echo "GRANT Super ON *.* TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
## optimize table
echo "optimize table t20_0012 all" | $TEST_USER_CONNECT
## verify
echo "select count(*)=1  from fuse_snapshot('default', 't20_0012')" | $TEST_USER_CONNECT

## Drop table.
echo "drop table t20_0012 all" | $MYSQL_CLIENT_CONNECT
## Drop user
echo "drop user 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
rm -rf password.out
