#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create user if not exists test identified by 'test'"|bendsql_connect_root

export TEST_NON_PRIVILEGED_USER_CONNECT="bendsql_connect_user test test -A"

echo "call system\$set_cache_capacity('memory_cache_block_meta', 100)" | $TEST_NON_PRIVILEGED_USER_CONNECT

echo "select * from set_cache_capacity('memory_cache_block_meta', '100')" | $TEST_NON_PRIVILEGED_USER_CONNECT

# CI will check $? of this script, and think it is [FAIL] if $? is non-zero,
exit 0