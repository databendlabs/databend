#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "CREATE STAGE if not exists s2;" | $MYSQL_CLIENT_CONNECT
echo "list @s2" | $MYSQL_CLIENT_CONNECT

## Debug
ls -aril "${CURDIR}/00_0001_upload_to_stage.sh"

curl  -H "stage_name:s2" -F "upload=@${CURDIR}/00_0001_upload_to_stage.sh" -XPUT http://localhost:8000/v1/upload_to_stage
echo "list @s2" | $MYSQL_CLIENT_CONNECT
echo "drop stage s2;" | $MYSQL_CLIENT_CONNECT

