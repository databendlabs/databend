#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

DATADIR="fs://${CURDIR}/../../../data/multi_page/"

echo "drop stage if exists data_fs;" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists data_s3;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists small_parquets;" | $MYSQL_CLIENT_CONNECT

echo "create stage data_s3 url='s3://testbucket/admin/data/multi_page/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='http://127.0.0.1:9900') FILE_FORMAT = (type = PARQUET);" | $MYSQL_CLIENT_CONNECT
echo "create stage data_fs url = '${DATADIR}' FILE_FORMAT = (type = PARQUET);"  | $MYSQL_CLIENT_CONNECT
echo "create table small_parquets(col_arr array(int), col_int int);" | $MYSQL_CLIENT_CONNECT

## all_large, mixed, all_small
for threshold in "0" "3000" "10000"; do
	for stage in "data_fs" "data_s3"; do
		echo "--- copy, threshold=${threshold} stage=${stage}"
		echo "set parquet_fast_read_bytes=${threshold}; copy into small_parquets from @${stage} PATTERN='.*parquet' force=true;" | $MYSQL_CLIENT_CONNECT
		echo "select count(*) from small_parquets" | $MYSQL_CLIENT_CONNECT
		echo "truncate table small_parquets" | $MYSQL_CLIENT_CONNECT

		echo "--- copy from select, threshold=${threshold} stage=${stage}"
		echo "set parquet_fast_read_bytes=${threshold}; copy into small_parquets from (select * from @${stage} t) PATTERN='.*parquet' force = true;" | $MYSQL_CLIENT_CONNECT
		echo "select count(*) from small_parquets" | $MYSQL_CLIENT_CONNECT
		echo "truncate table small_parquets" | $MYSQL_CLIENT_CONNECT
	done
done
