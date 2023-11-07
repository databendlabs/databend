#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "drop stage if exists s2;" | $BENDSQL_CLIENT_CONNECT
echo "CREATE STAGE if not exists s2;" | $BENDSQL_CLIENT_CONNECT
echo "list @s2" | $BENDSQL_CLIENT_CONNECT

# both stage_name and x-databend-stage-name is allowed
curl -u root: -XPUT -H "stage_name:s2" -F "upload=@${TESTS_DATA_DIR}/csv/books.csv" "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/upload_to_stage" > /dev/null 2>&1
curl -u root: -XPUT -H "x-databend-stage-name:s2" -H "relative_path:test" -F "upload=@${TESTS_DATA_DIR}/csv/books.csv" "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/upload_to_stage" > /dev/null 2>&1

echo "list @s2" | $BENDSQL_CLIENT_CONNECT | awk '{print $1,$2,$3}'
echo "drop stage s2;" | $BENDSQL_CLIENT_CONNECT

# test drop stage
echo "CREATE STAGE if not exists s2;" | $BENDSQL_CLIENT_CONNECT
echo "list @s2" | $BENDSQL_CLIENT_CONNECT
echo "drop stage s2;" | $BENDSQL_CLIENT_CONNECT
