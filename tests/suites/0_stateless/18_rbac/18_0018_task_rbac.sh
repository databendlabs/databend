#!/usr/bin/env bash

# Please start the cloud control server first before running this test:
#   python3 tests/cloud_control_server/simple_server.py

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export USER_A_CONNECT="bendsql --user=a --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_B_CONNECT="bendsql --user=b --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_C_CONNECT="bendsql --user=c --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

## cleanup
echo "drop task if exists task1;" | $BENDSQL_CLIENT_CONNECT
echo "drop task if exists task2;" | $BENDSQL_CLIENT_CONNECT
echo "drop task if exists task3;" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists role1;" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists role2;" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists role3;" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists a;" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists b;" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists c;" | $BENDSQL_CLIENT_CONNECT

## create users and roles
echo "create user a identified by '123';" | $BENDSQL_CLIENT_CONNECT
echo "create user b identified by '123';" | $BENDSQL_CLIENT_CONNECT
echo "create user c identified by '123';" | $BENDSQL_CLIENT_CONNECT
echo "create role role1;" | $BENDSQL_CLIENT_CONNECT
echo "create role role2;" | $BENDSQL_CLIENT_CONNECT
echo "create role role3;" | $BENDSQL_CLIENT_CONNECT
echo "grant role role1 to a;" | $BENDSQL_CLIENT_CONNECT
echo "grant role role2 to b;" | $BENDSQL_CLIENT_CONNECT
echo "grant role role3 to c;" | $BENDSQL_CLIENT_CONNECT
echo "alter user a with default_role='role1';" | $BENDSQL_CLIENT_CONNECT
echo "alter user b with default_role='role2';" | $BENDSQL_CLIENT_CONNECT
echo "alter user c with default_role='role3';" | $BENDSQL_CLIENT_CONNECT

echo "=== TEST 1: CreateTask privilege ==="
echo "--- user a without CreateTask privilege cannot create task ---"
echo "CREATE TASK task1 WAREHOUSE = 'mywh' SCHEDULE = 1 MINUTE AS SELECT 1;" | $USER_A_CONNECT

echo "--- grant CreateTask privilege to role1 ---"
echo "grant create task on *.* to role role1;" | $BENDSQL_CLIENT_CONNECT

echo "--- user a can create task now ---"
echo "CREATE TASK task1 WAREHOUSE = 'mywh' SCHEDULE = 1 MINUTE AS SELECT 1;" | $USER_A_CONNECT
echo "select name from system.tasks where name = 'task1';" | $USER_A_CONNECT

echo "=== TEST 2: Task ownership access ==="
echo "--- user a (owner) can alter/describe/execute task1 ---"
echo "ALTER TASK task1 SUSPEND;" | $USER_A_CONNECT
echo "DESC TASK task1;" | $USER_A_CONNECT | grep task1 | wc -l
echo "EXECUTE TASK task1;" | $USER_A_CONNECT

echo "--- user b (non-owner) cannot alter/describe/execute task1 ---"
echo "ALTER TASK task1 RESUME;" | $USER_B_CONNECT
echo "DESC TASK task1;" | $USER_B_CONNECT
echo "EXECUTE TASK task1;" | $USER_B_CONNECT

echo "=== TEST 3: Super privilege can access any task ==="
echo "--- grant super privilege to role2 ---"
echo "grant super on *.* to role role2;" | $BENDSQL_CLIENT_CONNECT

echo "--- user b with super can alter/describe/execute task1 ---"
echo "ALTER TASK task1 RESUME;" | $USER_B_CONNECT
echo "DESC TASK task1;" | $USER_B_CONNECT | grep task1 | wc -l
echo "EXECUTE TASK task1;" | $USER_B_CONNECT

echo "=== TEST 4: system.tasks visibility ==="
echo "--- grant CreateTask to role2 so user b can create task2 ---"
echo "grant create task on *.* to role role2;" | $BENDSQL_CLIENT_CONNECT
echo "CREATE TASK task2 WAREHOUSE = 'mywh' SCHEDULE = 2 MINUTE AS SELECT 2;" | $USER_B_CONNECT

echo "--- user a can only see task1 (owned by role1) ---"
echo "select name from system.tasks order by name;" | $USER_A_CONNECT

echo "--- user b with super can see all tasks ---"
echo "select name from system.tasks order by name;" | $USER_B_CONNECT

echo "--- admin can see all tasks ---"
echo "select name from system.tasks order by name;" | $BENDSQL_CLIENT_CONNECT

echo "=== TEST 5: GRANT OWNERSHIP transfer ==="
echo "--- transfer task1 ownership from role1 to role3 ---"
echo "grant ownership on task task1 to role role3;" | $BENDSQL_CLIENT_CONNECT

echo "--- user a (old owner) cannot access task1 anymore ---"
echo "ALTER TASK task1 SUSPEND;" | $USER_A_CONNECT
echo "DESC TASK task1;" | $USER_A_CONNECT

echo "--- user c (new owner) can access task1 ---"
echo "ALTER TASK task1 SUSPEND;" | $USER_C_CONNECT
echo "DESC TASK task1;" | $USER_C_CONNECT | grep task1 | wc -l

echo "--- user c can see task1 in system.tasks ---"
echo "select name from system.tasks order by name;" | $USER_C_CONNECT

echo "=== TEST 6: Drop task permission ==="
echo "--- user a cannot drop task1 (not owner anymore) ---"
echo "DROP TASK task1;" | $USER_A_CONNECT

echo "--- user c (owner) can drop task1 ---"
echo "DROP TASK task1;" | $USER_C_CONNECT

echo "--- user a (owner) can drop task owned by role1 ---"
echo "select name from system.tasks where name = 'task1';" | $USER_A_CONNECT

echo "=== cleanup ==="
echo "drop task if exists task1;" | $BENDSQL_CLIENT_CONNECT
echo "drop task if exists task2;" | $BENDSQL_CLIENT_CONNECT
echo "drop task if exists task3;" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists role1;" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists role2;" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists role3;" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists a;" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists b;" | $BENDSQL_CLIENT_CONNECT
echo "drop user if exists c;" | $BENDSQL_CLIENT_CONNECT
