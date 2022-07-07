#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop stage if exists presign_stage" | $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/stage/presign_stage/ontime_200.csv >/dev/null 2>&1

echo "CREATE STAGE presign_stage;" | $MYSQL_CLIENT_CONNECT

# Here is the a magic of curl.
# -s: make curl silent to avoid pollute our result.
# -w "%{http_code}": only print http code so that we can use this for test.
# -o /dev/null: redirect content to /dev/null to avoid pollute our result.
# cut -f 3: get the third value of output - the url.
curl -s -w "%{http_code}\n" -o /dev/null "`echo "set enable_planner_v2 = 1; PRESIGN @presign_stage/ontime_200.csv" | $MYSQL_CLIENT_CONNECT | cut -f 3`"

# Most arguements is the same with previous, except:
# -X PUT: Specify the http method
curl -s -w "%{http_code}\n" -X PUT -o /dev/null "`echo "set enable_planner_v2 = 1; PRESIGN UPLOAD @presign_stage/hello_world.txt" | $MYSQL_CLIENT_CONNECT | cut -f 3`" "Hello, World!"

echo "set enable_planner_v2 = 1; LIST @presign_stage/" | $MYSQL_CLIENT_CONNECT | awk '{print $1,$2,$3}';

## Drop table.
echo "drop stage if exists presign_stage" | $MYSQL_CLIENT_CONNECT
