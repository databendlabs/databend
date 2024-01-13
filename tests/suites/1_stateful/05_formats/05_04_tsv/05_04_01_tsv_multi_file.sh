#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

# prepare data
DATA="/tmp/multi_tsv/"
rm -rf $DATA
mkdir $DATA
for i  in $(seq 1 10);do
	for j in $(seq 1 1000);do
		printf "0\t1\t2\t3\t4\t5\t6\t7\t8\t9\n" >> "$DATA/${i}"
	done
done
aws --endpoint-url http://127.0.0.1:9900/ s3 cp /tmp/multi_tsv s3://testbucket/tmp/multi_tsv --recursive > /dev/null 2>&1

## prepare table
echo "drop table if exists test_tsv" | $BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE test_tsv
(
    c0 int,
    c1 int,
    c2 int,
    c3 int,
    c4 int,
    c5 int,
    c6 int,
    c7 int,
    c8 int,
    c9 int
);" | $BENDSQL_CLIENT_CONNECT

# do copy
echo "SET GLOBAL input_read_buffer_size = 111;" | $BENDSQL_CLIENT_CONNECT
COPY_SQL="copy into test_tsv from 's3://testbucket/tmp/multi_tsv/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='http://127.0.0.1:9900/') PATTERN = '.*' FILE_FORMAT = (type = TSV) force=true"
echo "${COPY_SQL}" |  $BENDSQL_CLIENT_CONNECT
echo "SET GLOBAL input_read_buffer_size = 1048576;" | $BENDSQL_CLIENT_CONNECT

# check
echo "select count(*) from test_tsv" |  $BENDSQL_CLIENT_CONNECT
echo "drop table if exists test_tsv" | $BENDSQL_CLIENT_CONNECT
