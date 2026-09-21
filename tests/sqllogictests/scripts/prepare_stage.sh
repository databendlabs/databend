#!/usr/bin/env bash

. tests/shell_env.sh

# create stage "data" which is readonly and map to tests/data.
# Use "fs" default to make development easier.
# most of the time, the tests will succeed with with "s3" too.
# todo: add storage "http"?
DATADIR="fs://${PWD}/tests/data/"
echo "drop stage if exists data" | bendsql_connect_root
echo "create or replace connection my_conn_s3 storage_type = 's3' access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='http://127.0.0.1:9900'" | bendsql_connect_root
echo "create or replace stage data_s3 url='s3://testbucket/data/' connection=(connection_name='my_conn_s3') FILE_FORMAT = (type = PARQUET);" | bendsql_connect_root
echo "create or replace stage data_fs url = '${DATADIR}' FILE_FORMAT = (type = PARQUET);" | bendsql_connect_root
if [ -z "$TEST_STAGE_STORAGE" ] || [ "$TEST_STAGE_STORAGE" == "fs" ]; then
	echo "create or replace stage data url = '${DATADIR}' FILE_FORMAT = (type = PARQUET);" | bendsql_connect_root
elif [ "$TEST_STAGE_STORAGE" == "s3" ]; then
	echo "create or replace stage data url='s3://testbucket/data/' connection=(connection_name='my_conn_s3') FILE_FORMAT = (type = PARQUET);" | bendsql_connect_root
else
	echo "unknown TEST_STAGE_STORAGE value: ${TEST_STAGE_STORAGE}"
	exit 1
fi

echo "drop table if exists ontime" | bendsql_connect_root
cat tests/data/ddl/ontime.sql | bendsql_connect_root

if [ -z "$TEST_STAGE_DEDUP" ] || [ "$TEST_STAGE_DEDUP" = "full_path" ]; then
    FULL_PATH=1
elif [ "$TEST_STAGE_DEDUP" = "sub_path" ]; then
    FULL_PATH=0
else
    echo "TEST_STAGE_DEDUP must be 'full_path' or 'sub_path'" >&2
    exit 1
fi

echo "set global copy_dedup_full_path_by_default = ${FULL_PATH}" | bendsql_connect_root

if [ -z "$TEST_STAGE_SIZE" ] || [ "$TEST_STAGE_SIZE" = "small" ]; then
		echo "set global parquet_fast_read_bytes = 1048576" | bendsql_connect_root
elif [ "$TEST_STAGE_SIZE" = "large" ]; then
		echo "set global parquet_fast_read_bytes = 0" | bendsql_connect_root
else
    echo "TEST_STAGE_DEDUP must be 'small' or 'large'" >&2
    exit 1
fi
