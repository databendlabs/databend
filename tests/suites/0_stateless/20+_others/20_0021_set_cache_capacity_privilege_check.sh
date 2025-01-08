#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create user if not exists test identified by 'test'"|$BENDSQL_CLIENT_CONNECT

export TEST_NON_PRIVILEGED_USER_CONNECT="bendsql --user=test --password=test --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo "call system\$set_cache_capacity('memory_cache_block_meta', 100)" | $TEST_NON_PRIVILEGED_USER_CONNECT

echo "select * from set_cache_capacity('memory_cache_block_meta', '100')" | $TEST_NON_PRIVILEGED_USER_CONNECT

# CI will check $? of this script, and think it is [FAIL] if $? is non-zero,
exit 0