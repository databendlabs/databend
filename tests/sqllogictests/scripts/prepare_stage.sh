#!/usr/bin/env bash

. tests/shell_env.sh

# create stage "data" which is readonly and map to tests/data.
# Use "fs" default to make development easier.
# most of the time, the tests will succeed with with "s3" too.
# todo: add storage "http"?
echo "drop stage if exists data" | $MYSQL_CLIENT_CONNECT
if [ -z "$TEST_STAGE_STORAGE" ] || [ "$TEST_STAGE_STORAGE" == "fs" ];
then
	DATADIR="fs://${PWD}/tests/data/"
	echo "create stage data url = '${DATADIR}' FILE_FORMAT = (type = PARQUET);"  | $MYSQL_CLIENT_CONNECT
elif  [ "$TEST_STAGE_STORAGE" == "s3" ];
then
	echo "create stage data url='s3://testbucket/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='http://127.0.0.1:9900') FILE_FORMAT = (type = PARQUET);" | $MYSQL_CLIENT_CONNECT
else
	echo "unknown TEST_STAGE_STORAGE value: ${TEST_STAGE_STORAGE}"
	exit 1
fi

echo "drop table if exists ontime" | $MYSQL_CLIENT_CONNECT
# todo: move ontime.sql to upper dir
cat tests/suites/1_stateful/ddl/ontime.sql | $MYSQL_CLIENT_CONNECT

if [ "$TEST_STAGE_PARQUET_LIB" == "parquet2" ] 
then 
	echo "set global use_parquet2=1;" | $MYSQL_CLIENT_CONNECT
else 
	echo "set global use_parquet2=0;" | $MYSQL_CLIENT_CONNECT
fi

