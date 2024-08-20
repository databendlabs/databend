#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_NAME="owner"
export TEST_USER_PASSWORD="password"
export MYSQL_CLIENT_CONNECT="mysql --ssl-mode=DISABLED -u${TEST_USER_NAME} -p${TEST_USER_PASSWORD} --host ${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_MYSQL_HANDLER_PORT} -s"


stmt "drop user if exists 'owner'"
stmt "drop role if exists role1"
stmt "create user 'owner' IDENTIFIED BY '$TEST_USER_PASSWORD' with DEFAULT_ROLE='role1'"
stmt 'create role role1'
stmt "create or replace database test"

stmt 'grant role role1 to owner'

# no privilege to create a normal table
echo "create table test.t1(a int);" | $MYSQL_CLIENT_CONNECT || true

echo "create temp table test.t1(a int);
            show grants for role role1;
            insert into table test.t1 values(1),(2);
            select * from test.t1;
            delete from test.t1 where a=1;
            select * from test.t1;
            update test.t1 set a=3 where a=2;
            select * from test.t1;
            rename table test.t1 to test.t2;
            select * from test.t2;
            alter table test.t2 add column b int;
            select * from test.t2;
            insert into test.t2 values(3,4);
            select block_count from fuse_snapshot('test','t2') limit 1;
            optimize table  test.t2 compact;
            select block_count from fuse_snapshot('test','t2') limit 1;
            " | $MYSQL_CLIENT_CONNECT

# unknown table
echo "select * from test.t2;" | $MYSQL_CLIENT_CONNECT || true   