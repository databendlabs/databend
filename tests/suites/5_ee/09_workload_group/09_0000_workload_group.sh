#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace DATABASE workloadgroup" |  bendsql_connect_root
echo "create table workloadgroup.t(a int)" | bendsql_connect_root
echo "insert into workloadgroup.t values(1)" | bendsql_connect_root_null
echo "DROP USER IF EXISTS test_user_workload_group2;" | bendsql_connect_root_null
echo "DROP WORKLOAD GROUP IF EXISTS valid_mem;" | bendsql_connect_root_null
echo "CREATE WORKLOAD GROUP valid_mem WITH memory_quota = '4GB';" | bendsql_connect_root_null
echo "create user test_user_workload_group2 identified by '123' with SET WORKLOAD GROUP='valid_mem'" | bendsql_connect_root_null
echo "grant select on workloadgroup.t to test_user_workload_group2" | bendsql_connect_root_null

export TEST_USER_CONNECT="bendsql_connect_user test_user_workload_group2 123"

echo "select * from workloadgroup.t" | $TEST_USER_CONNECT
echo "drop workload group valid_mem;" | bendsql_connect_root_null

output=$(echo "select * from workloadgroup.t" | $TEST_USER_CONNECT 2>&1)

if [[ "$output" != *"[3142]Unknown workload id"* ]]; then
    exit 1
fi

echo "DROP DATABASE IF EXISTS workloadgroup" | bendsql_connect_root
echo "DROP USER IF EXISTS test_user_workload_group2" | bendsql_connect_root
