#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh
DATADIR_PATH="$CURDIR/../../../../data/"

echo "drop stage if exists data_fs;" | $MYSQL_CLIENT_CONNECT
echo "create stage data_fs url = 'fs://$DATADIR_PATH' FILE_FORMAT = (type = NDJSON)" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists t;" | $MYSQL_CLIENT_CONNECT
echo "create table t (a variant)" | $MYSQL_CLIENT_CONNECT
echo "insert into table t values (1.1)" | $MYSQL_CLIENT_CONNECT

echo "select \$1 from @data_fs (files=>('json_sample.ndjson')) order by \$1:b;" | $MYSQL_CLIENT_CONNECT

echo "select \$1:a as a from @data_fs (files=>('json_sample.ndjson')) order by a;" | $MYSQL_CLIENT_CONNECT

echo "select t2.\$1:a, a from @data_fs (files=>('json_sample.ndjson')) as t2, t order by t2.\$1:a;" | $MYSQL_CLIENT_CONNECT

echo "copy into t from (select \$1:b from @data_fs t2) files=('json_sample.ndjson');" | $MYSQL_CLIENT_CONNECT
echo "select \$1 from t order by \$1" | $MYSQL_CLIENT_CONNECT
