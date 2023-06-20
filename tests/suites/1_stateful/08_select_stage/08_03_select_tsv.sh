#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh
DATADIR_PATH="$CURDIR/../../../data/"

echo "drop stage if exists data_tsv;" | $MYSQL_CLIENT_CONNECT
echo "create stage data_tsv url = 'fs://$DATADIR_PATH' FILE_FORMAT = (type = TSV)" | $MYSQL_CLIENT_CONNECT

echo "drop table if exists t;" | $MYSQL_CLIENT_CONNECT
echo "create table t (a int, b string)" | $MYSQL_CLIENT_CONNECT

echo "---simple"
echo "select \$2 from @data_tsv (files=>('select.tsv')) order by \$1;" | $MYSQL_CLIENT_CONNECT

echo "---table alias"
echo "select a.\$1 from @data_tsv (files=>('select.tsv')) a order by \$1;" | $MYSQL_CLIENT_CONNECT

echo "---select *"
echo "select * from @data_tsv (files=>('select.tsv'));" | $MYSQL_CLIENT_CONNECT

echo "---select *,\$1"
echo "select *, \$1 from @data_tsv (files=>('select.tsv'));" | $MYSQL_CLIENT_CONNECT

echo "---copy"
echo "copy into t from (select \$1, \$2 from @data_tsv t2) files=('select.tsv');" | $MYSQL_CLIENT_CONNECT
echo "select * from t order by a" | $MYSQL_CLIENT_CONNECT
