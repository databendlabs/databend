#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop stage if exists presign_stage" | $MYSQL_CLIENT_CONNECT

echo "CREATE STAGE presign_stage;" | $MYSQL_CLIENT_CONNECT

# Most arguments is the same with previous, except:
# -X PUT: Specify the http method
curl -s -w "%{http_code}\n" -X PUT -o /dev/null "`echo "set enable_planner_v2 = 1; PRESIGN UPLOAD @presign_stage/hello_world.txt" | $MYSQL_CLIENT_CONNECT | cut -f 3`" -d "Hello, World!"

# LIST will output file's updated time, so we only take first three of output:
# file_name, file_size, file_md5
echo "set enable_planner_v2 = 1; LIST @presign_stage/" | $MYSQL_CLIENT_CONNECT | awk '{print $1,$2,$3}';

## Drop table.
echo "drop stage if exists presign_stage" | $MYSQL_CLIENT_CONNECT
