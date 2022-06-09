#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop stage if exists s1" | $MYSQL_CLIENT_CONNECT

## Copy from internal stage
echo "CREATE STAGE s1;" | $MYSQL_CLIENT_CONNECT
curl -u root: -XPUT -H "stage_name:s1" -F "upload=@${CURDIR}/../../../data/ontime_200.csv" "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/stage/upload_to_stage" > /dev/null 2>&1
curl -u root: -XPUT -H "stage_name:s1" -F "upload=@${CURDIR}/../../../data/ontime_200.csv.gz" "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/stage/upload_to_stage" > /dev/null 2>&1
curl -u root: -XPUT -H "stage_name:s1" -F "upload=@${CURDIR}/../../../data/ontime_200.csv.zst" "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/stage/upload_to_stage" > /dev/null 2>&1

curl -u root: -XPUT -H "stage_name:s1" -H "relative_path:dir" -F "upload=@${CURDIR}/../../../data/ontime_200.csv" "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/stage/upload_to_stage" > /dev/null 2>&1
curl -u root: -XPUT -H "stage_name:s1" -H "relative_path:dir" -F "upload=@${CURDIR}/../../../data/ontime_200.csv.gz" "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/stage/upload_to_stage" > /dev/null 2>&1
curl -u root: -XPUT -H "stage_name:s1" -H "relative_path:dir" -F "upload=@${CURDIR}/../../../data/ontime_200.csv.zst" "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/stage/upload_to_stage" > /dev/null 2>&1

## List files in internal stage
echo "=== List files in internal stage ==="
echo "list @s1" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort

## Remove internal stage file
echo "=== Test remove internal stage file ==="
echo "remove @s1/ontime_200.csv.gz" | $MYSQL_CLIENT_CONNECT
echo "remove @s1/dir/ontime_200.csv.gz" | $MYSQL_CLIENT_CONNECT
echo "list @s1/dir/" | $MYSQL_CLIENT_CONNECT | awk '{print $1}'| sort
echo "list @s1" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort

## Remove internal stage file with pattern
echo "=== Test remove internal stage file with pattern ==="
echo "remove @s1/dir/ PATTERN = '.*zst'" | $MYSQL_CLIENT_CONNECT
echo "list @s1" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort
echo "remove @s1 PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT
echo "list @s1" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort

echo "drop stage s1" | $MYSQL_CLIENT_CONNECT
