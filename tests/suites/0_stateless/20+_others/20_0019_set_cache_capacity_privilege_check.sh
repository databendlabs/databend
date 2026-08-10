#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create user if not exists test identified by 'test'"|bendsql_connect_root

export TEST_NON_PRIVILEGED_USER_CONNECT="bendsql_connect_user test test -A"


echo "call system\$fuse_amend('db', 't')" | $TEST_NON_PRIVILEGED_USER_CONNECT

echo "select * from fuse_amend('db', 't')" | $TEST_NON_PRIVILEGED_USER_CONNECT

# CI will check $? of this script, and think it is [FAIL] if $? is non-zero,
exit 0
