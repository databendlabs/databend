#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

DATADIR_PATH="/tmp/08_00_04/"
DATADIR="fs://$DATADIR_PATH/"

for USE_PARQEUT2 in 0 1
do
	echo "==="
	echo "set use_parquet2 = ${USE_PARQEUT2} ; show settings like 'use_parquet2';" | $MYSQL_CLIENT_CONNECT

	rm -rf ${DATADIR_PATH}
  mkdir ${DATADIR_PATH}
  mkdir ${DATADIR_PATH}/data

	cp "$CURDIR"/../../../../data/tuple.parquet ${DATADIR_PATH}/data

	# should omit this file
	cp "$CURDIR"/../../../../data/sample.csv ${DATADIR_PATH}/00.csv

	echo "drop stage if exists s2;" | $MYSQL_CLIENT_CONNECT
	echo "create stage s2 url = '${DATADIR}' FILE_FORMAT = (type = PARQUET);"  | $MYSQL_CLIENT_CONNECT

	echo "--- dir:"
	echo "set use_parquet2 = ${USE_PARQEUT2} ; select * from infer_schema(location => '@s2/data/');" | $MYSQL_CLIENT_CONNECT

	echo "--- file:"
	echo "set use_parquet2 = ${USE_PARQEUT2} ; select * from infer_schema(location => '@s2/data/tuple.parquet');" | $MYSQL_CLIENT_CONNECT

	echo '--- file_format:'
	echo "drop stage if exists s3;" | $MYSQL_CLIENT_CONNECT
	echo "create stage s3 url = '${DATADIR}' FILE_FORMAT = (type = CSV);"  | $MYSQL_CLIENT_CONNECT
	echo "set use_parquet2 = ${USE_PARQEUT2} ; select * from infer_schema(location => '@s3', FILE_FORMAT => 'PARQUET',  pattern => '.*parquet');" | $MYSQL_CLIENT_CONNECT

	echo "--- pattern:"
	# should omit this file
	cp "$CURDIR"/../../../../data/sample.csv ${DATADIR_PATH}/data/00.csv
	echo "set use_parquet2 = ${USE_PARQEUT2} ; select * from infer_schema(location => '@s2', pattern => '.*parquet');" | $MYSQL_CLIENT_CONNECT

	echo "--- complex:"
	cp "$CURDIR"/../../../../data/complex.parquet ${DATADIR_PATH}/data/complex.parquet
	echo "set use_parquet2 = ${USE_PARQEUT2} ; select * from infer_schema(location => '@s2/data/', pattern => 'complex.*');" | $MYSQL_CLIENT_CONNECT

	cp "$CURDIR"/../../../../data/variant.parquet ${DATADIR_PATH}/data

	echo "--- variant file:"
	echo "set use_parquet2 = ${USE_PARQEUT2} ; select * from infer_schema(location => '@s2/data/variant.parquet');" | $MYSQL_CLIENT_CONNECT
done

