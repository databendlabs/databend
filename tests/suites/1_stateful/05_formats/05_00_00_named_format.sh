#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# prepare table
echo "drop table if exists table_csv;" | $MYSQL_CLIENT_CONNECT
echo "create table table_csv (a int, b string, c int);" | $MYSQL_CLIENT_CONNECT

echo "drop table if exists table_parquet;" | $MYSQL_CLIENT_CONNECT
echo "create table table_parquet (id int, t tuple(a int, b string));" | $MYSQL_CLIENT_CONNECT

# prepare format
echo "drop file format if exists my_csv;"  | $MYSQL_CLIENT_CONNECT
echo "create file format my_csv type = CSV;"  | $MYSQL_CLIENT_CONNECT
echo "drop file format if exists my_parquet;"  | $MYSQL_CLIENT_CONNECT
echo "create file format my_parquet type = PARQUET;"  | $MYSQL_CLIENT_CONNECT

# prepare stage
DATADIR_PATH="/tmp/data_05_00_00"
rm -rf ${DATADIR_PATH}
mkdir ${DATADIR_PATH}
DATADIR="fs://$DATADIR_PATH/"
cp "$CURDIR"/../../../data/sample.csv ${DATADIR_PATH}/
cp "$CURDIR"/../../../data/tuple.parquet ${DATADIR_PATH}/

# test create stage
## default format is csv, so we can check args in select_stage and infer_schema really work
echo "drop stage if exists stage_05_00_00;" | $MYSQL_CLIENT_CONNECT
echo "create stage stage_05_00_00 url = '${DATADIR}' FILE_FORMAT = (FORMAT_NAME = 'my_csv');"  | $MYSQL_CLIENT_CONNECT

# test copy into table
echo "---copy into table, use format csv from stage"
echo "copy into table_csv from @stage_05_00_00  pattern = '.*csv' ;" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from table_csv" | $MYSQL_CLIENT_CONNECT

echo "---copy into table, use format csv from copy stmt"
echo "copy into table_csv from @stage_05_00_00 FILE_FORMAT = ( FORMAT_NAME = 'my_csv') pattern = '.*csv' ;" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from table_csv" | $MYSQL_CLIENT_CONNECT

echo "---copy into table, use format my_parquet from copy stmt"
echo "copy into table_parquet from @stage_05_00_00 FILE_FORMAT = ( FORMAT_NAME = 'my_parquet') pattern = '.*parquet' ;" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from table_parquet" | $MYSQL_CLIENT_CONNECT
echo "---copy into table, use format parquet from copy stmt"
echo "copy into table_parquet from @stage_05_00_00 FILE_FORMAT = ( FORMAT_NAME = 'parquet') pattern = '.*parquet' ;" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from table_parquet" | $MYSQL_CLIENT_CONNECT

# test copy from table
echo "---copy from table"
echo "copy into @stage_05_00_00 from table_csv FILE_FORMAT = ( FORMAT_NAME = 'my_csv')  pattern = '.*csv' ;" | $MYSQL_CLIENT_CONNECT
cat $DATADIR_PATH/*.csv | wc -l | sed 's/ //g'

# test select stage
echo "---select_stage use format my_parquet"
echo "select count(*) from '${DATADIR}' (pattern => '.*parquet' , FILE_FORMAT => 'my_parquet')" | $MYSQL_CLIENT_CONNECT
echo "---select_stage use format parquet"
echo "select count(*) from '${DATADIR}' (pattern => '.*parquet' , FILE_FORMAT => 'parquet')" | $MYSQL_CLIENT_CONNECT

# test infer_schema
echo "---infer_schema use format my_parquet"
echo "select * from infer_schema(location => '@stage_05_00_00', pattern => '.*parquet' , FILE_FORMAT => 'my_parquet')" | $MYSQL_CLIENT_CONNECT
echo "---infer_schema use format parquet"
echo "select * from infer_schema(location => '@stage_05_00_00', pattern => '.*parquet' , FILE_FORMAT => 'parquet')" | $MYSQL_CLIENT_CONNECT
