CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "=== test UDF priv"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=test-user --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo "drop user if exists 'test-user'" | $BENDSQL_CLIENT_CONNECT

## create user
echo "create user 'test-user' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT


echo "DROP FUNCTION IF EXISTS test_alter_udf;" |  $BENDSQL_CLIENT_CONNECT
echo "CREATE FUNCTION test_alter_udf AS (p) -> not(is_null(p))" | $BENDSQL_CLIENT_CONNECT
#error test
echo "select test_alter_udf(1)" | $TEST_USER_CONNECT

echo "grant usage on udf test_alter_udf to 'test-user';" |  $BENDSQL_CLIENT_CONNECT
echo "select test_alter_udf(1)" | $TEST_USER_CONNECT

echo "drop user 'test-user'" | $BENDSQL_CLIENT_CONNECT
