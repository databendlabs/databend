#!/usr/bin/env bash

# Get the current directory of the script
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR/../../../shell_env.sh"

# Define connection strings for each user
echo "=== Setting up user connection strings ==="
export USER_ADMIN_USER_CONNECT="bendsql -A --user=admin_user --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_DEV_USER_CONNECT="bendsql -A --user=dev_user --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_TEST_USER_CONNECT="bendsql -A --user=test_user --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

# Create roles
echo "=== Creating roles ==="
echo "DROP ROLE IF EXISTS admin_role;" | $BENDSQL_CLIENT_CONNECT
echo "DROP ROLE IF EXISTS dev_role;" | $BENDSQL_CLIENT_CONNECT
echo "DROP ROLE IF EXISTS test_role;" | $BENDSQL_CLIENT_CONNECT
echo "CREATE ROLE admin_role;" | $BENDSQL_CLIENT_CONNECT
echo "CREATE ROLE dev_role;" | $BENDSQL_CLIENT_CONNECT
echo "CREATE ROLE test_role;" | $BENDSQL_CLIENT_CONNECT

# Create users
echo "=== Creating users ==="
echo "CREATE OR REPLACE USER admin_user IDENTIFIED BY '123' with default_role='admin_role';" | $BENDSQL_CLIENT_CONNECT
echo "CREATE OR REPLACE USER dev_user IDENTIFIED BY '123' with default_role='dev_role';" | $BENDSQL_CLIENT_CONNECT
echo "CREATE OR REPLACE USER test_user IDENTIFIED BY '123' with default_role='test_role';" | $BENDSQL_CLIENT_CONNECT

# Grant roles to users
echo "=== Granting roles to users ==="
echo "GRANT role admin_role TO USER admin_user;" | $BENDSQL_CLIENT_CONNECT
echo "GRANT role dev_role TO USER dev_user;" | $BENDSQL_CLIENT_CONNECT
echo "GRANT role test_role TO USER test_user;" | $BENDSQL_CLIENT_CONNECT

# Test global CREATE PROCEDURE permission
echo "=== Testing global CREATE PROCEDURE permission ==="
echo "GRANT CREATE PROCEDURE ON *.* TO ROLE admin_role;" | $BENDSQL_CLIENT_CONNECT

# admin_user creates procedures
echo "=== admin_user: Creating procedures ==="
echo "1. Creating add_numbers procedure..."
echo "CREATE PROCEDURE add_numbers(a INT, b INT) RETURNS INT NOT NULL LANGUAGE SQL AS \$\$
BEGIN
    RETURN a + b;
END;
\$\$;" | $USER_ADMIN_USER_CONNECT

echo "2. Creating get_current_time procedure..."
echo "CREATE PROCEDURE get_current_time() RETURNS STRING NOT NULL LANGUAGE SQL AS \$\$
BEGIN
    RETURN CURRENT_TIMESTAMP();
END;
\$\$;" | $USER_ADMIN_USER_CONNECT

# dev_user and test_user should fail to create procedures
echo "=== Testing dev_user and test_user cannot create procedures ==="
echo "3. dev_user tries to create test_proc (should fail)..."
echo "CREATE PROCEDURE test_proc() RETURNS INT NOT NULL LANGUAGE SQL AS \$\$
BEGIN
    RETURN 1;
END;
\$\$;" | $USER_DEV_USER_CONNECT

echo "4. test_user tries to create test_proc (should fail)..."
echo "CREATE PROCEDURE test_proc() RETURNS INT NOT NULL LANGUAGE SQL AS \$\$
BEGIN
    RETURN 1;
END;
\$\$;" | $USER_TEST_USER_CONNECT

# Test ACCESS PROCEDURE permission
echo "=== Testing ACCESS PROCEDURE permission ==="
echo "5. Granting dev_role access to add_numbers..."
echo "GRANT ACCESS PROCEDURE ON PROCEDURE add_numbers(int, int) TO ROLE dev_role;" | $BENDSQL_CLIENT_CONNECT

echo "6. dev_user calls add_numbers (should succeed)..."
echo "CALL procedure add_numbers(3::int32, 4::int32);" | $USER_DEV_USER_CONNECT

echo "7. test_user calls add_numbers (should fail)..."
echo "CALL procedure add_numbers(3::int32, 4::int32);" | $USER_TEST_USER_CONNECT

echo "8. Granting test_role access to get_current_time..."
echo "GRANT ACCESS PROCEDURE ON PROCEDURE get_current_time() TO ROLE test_role;" | $BENDSQL_CLIENT_CONNECT

echo "9. test_user calls get_current_time (should succeed)..."
echo "CALL procedure get_current_time();" | $USER_TEST_USER_CONNECT | echo $?

# Test ownership transfer
echo "=== Testing ownership transfer ==="
echo "10. admin_user creates multiply_numbers..."
echo "CREATE PROCEDURE multiply_numbers(a INT, b INT) RETURNS INT NOT NULL LANGUAGE SQL AS \$\$
BEGIN
    RETURN a * b;
END;
\$\$;" | $BENDSQL_CLIENT_CONNECT

echo "11. Transferring ownership of multiply_numbers to dev_role..."
echo "GRANT OWNERSHIP ON PROCEDURE multiply_numbers(INT, INT) TO ROLE dev_role;" | $BENDSQL_CLIENT_CONNECT

echo "12. dev_user describes multiply_numbers (should succeed)..."
echo "DESC PROCEDURE multiply_numbers(INT32, INT32);" | $USER_DEV_USER_CONNECT

echo "13. test_user describes multiply_numbers (should fail)..."
echo "DESC PROCEDURE multiply_numbers(INT32, INT32);" | $USER_TEST_USER_CONNECT

# Test ownership chaining
echo "=== Testing ownership chaining ==="
echo "14. Transferring ownership of multiply_numbers to test_role..."
echo "GRANT OWNERSHIP ON PROCEDURE multiply_numbers(INT, INT) TO ROLE test_role;" | $BENDSQL_CLIENT_CONNECT

echo "15. dev_user describes multiply_numbers (should fail)..."
echo "DESC PROCEDURE multiply_numbers(INT32, INT32);" | $USER_DEV_USER_CONNECT

echo "16. dev_user calls multiply_numbers (should fail)..."
echo "CALL procedure multiply_numbers(1::int32, 2::int32);" | $USER_DEV_USER_CONNECT

echo "17. test_user describes multiply_numbers (should succeed)..."
echo "DESC PROCEDURE multiply_numbers(INT32, INT32);" | $USER_TEST_USER_CONNECT

echo "18. test_user calls multiply_numbers (should succeed)..."
echo "CALL procedure multiply_numbers(1::int32, 2::int32);" | $USER_TEST_USER_CONNECT

# Test permission inheritance
echo "=== Testing permission inheritance ==="
echo "19. Creating test_with_access role..."
echo "CREATE ROLE test_with_access;" | $BENDSQL_CLIENT_CONNECT

echo "20. Granting test_with_access access to add_numbers..."
echo "GRANT ACCESS PROCEDURE ON PROCEDURE add_numbers(int, int) TO ROLE test_with_access;" | $BENDSQL_CLIENT_CONNECT

echo "21. test_user calls add_numbers (should fail, not yet granted role)..."
echo "CALL procedure add_numbers(5::int32, 6::int32);" | $USER_TEST_USER_CONNECT

echo "22. Granting test_with_access to test_user..."
echo "GRANT role test_with_access TO USER test_user;" | $BENDSQL_CLIENT_CONNECT

echo "23. test_user calls add_numbers (should succeed)..."
echo "CALL procedure add_numbers(5::int32, 6::int32);" | $USER_TEST_USER_CONNECT

# Test permission revocation
echo "=== Testing permission revocation ==="
echo "24. Revoking test_with_access's access to add_numbers..."
echo "REVOKE ACCESS PROCEDURE ON PROCEDURE add_numbers(int, int) FROM ROLE test_with_access;" | $BENDSQL_CLIENT_CONNECT

echo "25. test_user calls add_numbers (should fail)..."
echo "CALL procedure add_numbers(5::int32, 6::int32);" | $USER_TEST_USER_CONNECT

echo "26. test_user describes add_numbers (should fail)..."
echo "DESC PROCEDURE add_numbers(INT32, INT32);" | $USER_TEST_USER_CONNECT

# Test procedure without parameters
echo "=== Testing procedure without parameters ==="
echo "27. Creating greet procedure..."
echo "CREATE PROCEDURE greet() RETURNS STRING NOT NULL LANGUAGE SQL AS \$\$
BEGIN
    RETURN 'Hello, Databend!';
END;
\$\$;" | $BENDSQL_CLIENT_CONNECT

echo "28. Granting dev_role access to greet..."
echo "GRANT ACCESS PROCEDURE ON PROCEDURE greet() TO ROLE dev_role;" | $BENDSQL_CLIENT_CONNECT

echo "29. dev_user calls greet (should succeed)..."
echo "CALL procedure greet();" | $USER_DEV_USER_CONNECT

echo "30. test_user calls greet (should fail)..."
echo "CALL procedure greet();" | $USER_TEST_USER_CONNECT

# Test system.procedures visibility
echo "=== Testing system.procedures visibility ==="
echo "SET GLOBAL enable_experimental_rbac_check = 1;" | $BENDSQL_CLIENT_CONNECT

echo "31. admin_user queries system.procedures (should see add_numbers, get_current_time via ownership)..."
echo "SELECT name FROM system.procedures WHERE name IN ('add_numbers', 'get_current_time', 'multiply_numbers', 'greet') ORDER BY name;" | $USER_ADMIN_USER_CONNECT
echo "SELECT name FROM system.procedures WHERE name IN ('add_numbers', 'get_current_time', 'multiply_numbers', 'greet') ORDER BY name;" | $BENDSQL_CLIENT_CONNECT

echo "32. dev_user queries system.procedures (should see add_numbers and greet via grant)..."
echo "SELECT name FROM system.procedures WHERE name IN ('add_numbers', 'get_current_time', 'multiply_numbers', 'greet') ORDER BY name;" | $USER_DEV_USER_CONNECT

echo "33. test_user queries system.procedures (should see get_current_time via grant, multiply_numbers via ownership)..."
echo "SELECT name FROM system.procedures WHERE name IN ('add_numbers', 'get_current_time', 'multiply_numbers', 'greet') ORDER BY name;" | $USER_TEST_USER_CONNECT

echo "SET GLOBAL enable_experimental_rbac_check = 0;" | $BENDSQL_CLIENT_CONNECT

# Cleanup
echo "=== Cleaning up test environment ==="
echo "34. Dropping users..."
echo "DROP USER IF EXISTS admin_user;" | $BENDSQL_CLIENT_CONNECT
echo "DROP USER IF EXISTS dev_user;" | $BENDSQL_CLIENT_CONNECT
echo "DROP USER IF EXISTS test_user;" | $BENDSQL_CLIENT_CONNECT

echo "35. Dropping roles..."
echo "DROP ROLE IF EXISTS admin_role;" | $BENDSQL_CLIENT_CONNECT
echo "DROP ROLE IF EXISTS dev_role;" | $BENDSQL_CLIENT_CONNECT
echo "DROP ROLE IF EXISTS test_role;" | $BENDSQL_CLIENT_CONNECT
echo "DROP ROLE IF EXISTS test_with_access;" | $BENDSQL_CLIENT_CONNECT

echo "36. Dropping procedures..."
echo "DROP PROCEDURE IF EXISTS add_numbers(int, int);" | $BENDSQL_CLIENT_CONNECT
echo "DROP PROCEDURE IF EXISTS get_current_time();" | $BENDSQL_CLIENT_CONNECT
echo "DROP PROCEDURE IF EXISTS multiply_numbers(int, int);" | $BENDSQL_CLIENT_CONNECT
echo "DROP PROCEDURE IF EXISTS greet();" | $BENDSQL_CLIENT_CONNECT

echo "unset global enable_experimental_procedure;" | $BENDSQL_CLIENT_CONNECT
echo "=== Test script completed ==="
