#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace DATABASE workloadgroup" |  $BENDSQL_CLIENT_CONNECT
echo "create table workloadgroup.t(a int)" | $BENDSQL_CLIENT_CONNECT
echo "insert into workloadgroup.t values(1)" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "DROP USER IF EXISTS test_user_workload_group2;" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "DROP WORKLOAD GROUP IF EXISTS valid_mem;" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "CREATE WORKLOAD GROUP valid_mem WITH memory_quota = '4GB';" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "create user test_user_workload_group2 identified by '123' with SET WORKLOAD GROUP='valid_mem'" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "grant select on workloadgroup.t to test_user_workload_group2" | $BENDSQL_CLIENT_OUTPUT_NULL

export TEST_USER_CONNECT="bendsql --user=test_user_workload_group2 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo "select * from workloadgroup.t" | $TEST_USER_CONNECT
echo "drop workload group valid_mem;" | $BENDSQL_CLIENT_OUTPUT_NULL

output=$(echo "select * from workloadgroup.t" | $TEST_USER_CONNECT 2>&1)

if [[ "$output" != *"[3142]Unknown workload id"* ]]; then
    exit 1
fi

echo "DROP DATABASE IF EXISTS workloadgroup" | $BENDSQL_CLIENT_CONNECT
echo "DROP USER IF EXISTS test_user_workload_group2" | $BENDSQL_CLIENT_CONNECT
