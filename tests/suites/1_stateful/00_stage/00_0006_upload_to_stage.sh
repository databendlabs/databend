#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "drop stage if exists s2;" | bendsql_connect_root
echo "CREATE STAGE if not exists s2;" | bendsql_connect_root
echo "list @s2" | bendsql_connect_root

# both stage_name and x-databend-stage-name is allowed
curl -u root: -XPUT -H "stage_name:s2" -F "upload=@${TESTS_DATA_DIR}/csv/books.csv" "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/upload_to_stage" > /dev/null 2>&1
curl -u root: -XPUT -H "x-databend-stage-name:s2" -H "relative_path:test" -F "upload=@${TESTS_DATA_DIR}/csv/books.csv" "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/upload_to_stage" > /dev/null 2>&1

echo "list @s2" | bendsql_connect_root | awk '{print $1,$2,$3}'
echo "drop stage s2;" | bendsql_connect_root

# test drop stage
echo "CREATE STAGE if not exists s2;" | bendsql_connect_root
echo "list @s2" | bendsql_connect_root
echo "drop stage s2;" | bendsql_connect_root
