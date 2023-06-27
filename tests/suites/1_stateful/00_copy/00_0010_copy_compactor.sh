#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh
DATADIR_PATH="$CURDIR/../../../data/"

echo "drop stage if exists data_csv;" | $MYSQL_CLIENT_CONNECT
echo "create stage data_csv url = 'fs://$DATADIR_PATH' FILE_FORMAT = (type = CSV)" | $MYSQL_CLIENT_CONNECT

echo "drop table if exists copy_compactor;" | $MYSQL_CLIENT_CONNECT
echo "create table copy_compactor (a int) row_per_block=10;" | $MYSQL_CLIENT_CONNECT


echo "set enable_distributed_copy_into = 1;copy into copy_compactor from @data_csv files=('numbers.csv');" | $MYSQL_CLIENT_CONNECT
echo "SELECT row_count FROM FUSE_BLOCK('default', 'copy_compactor') order by row_count;" | $MYSQL_CLIENT_CONNECT
