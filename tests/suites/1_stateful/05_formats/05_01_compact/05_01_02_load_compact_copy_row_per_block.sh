#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

# prepare data
# each row is about 10 + 8 = 18 bytes
DATA="/tmp/load_compact.csv"
rm -rf $DATA
for j in $(seq 1 1000);do
	printf "0123456789\n" >> "$DATA"
done

echo "drop table if exists t1 all" | $BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE t1
(
    c0 string
) engine=fuse row_per_block=800;
" | $BENDSQL_CLIENT_CONNECT


echo "---s3 cp"
aws --endpoint-url http://127.0.0.1:9900/ s3 cp $DATA s3://testbucket/$DATA > /dev/null 2>&1

echo "---copy into"
# let input data dispatch to multi threads
echo "copy  /*+ set_var(input_read_buffer_size=100) set_var(max_threads=1) */ into t1 from 's3://testbucket/${DATA}' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='http://127.0.0.1:9900/') FILE_FORMAT = (type = CSV) force=true" | $BENDSQL_CLIENT_CONNECT

echo "---row_count"
echo "select count(*) from t1" | $BENDSQL_CLIENT_CONNECT

echo "---block_count"
echo "select block_count from fuse_snapshot('default','t1') limit 1" | $BENDSQL_CLIENT_CONNECT

echo "drop table if exists t1" | $BENDSQL_CLIENT_CONNECT
