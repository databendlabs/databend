#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "SELECT 1;select 2;select 3;" | bendsql_connect_root

echo "SELECT 1" | bendsql_connect_root
echo "SELECT 2" | bendsql_connect_root
echo "SELECT 3" | bendsql_connect_root
