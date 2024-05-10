#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "=== test TASK priv"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=test-user --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
