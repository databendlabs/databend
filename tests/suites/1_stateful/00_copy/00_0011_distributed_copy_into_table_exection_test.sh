#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "set global max_threads = 1" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists products;" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists s0011;" | $MYSQL_CLIENT_CONNECT
echo "CREATE STAGE s0011 FILE_FORMAT = (TYPE = CSV);" | $MYSQL_CLIENT_CONNECT
echo "create table products (id int, name string, description string);" | $MYSQL_CLIENT_CONNECT

#multi files to trigger distributed test
curl -s -u root: -H "stage_name:s0011" -F "upload=@${CURDIR}/../../../data/ttt.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -H "stage_name:s0011" -F "upload=@${CURDIR}/../../../data/sample.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -H "stage_name:s0011" -F "upload=@${CURDIR}/../../../data/sample_3_replace.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -H "stage_name:s0011" -F "upload=@${CURDIR}/../../../data/sample_3_duplicate.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"

echo "set enable_distributed_copy_into = 1;copy into products from @s0011 pattern = '.*[.]csv';" | $MYSQL_CLIENT_CONNECT
echo "select * from products order by id,name,description;" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from products;" | $MYSQL_CLIENT_CONNECT
## error test!!!
curl -s -u root: -H "stage_name:s0011" -F "upload=@${CURDIR}/../../../data/sample_2_columns.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
echo "set enable_distributed_copy_into = 1;copy into products from @s0011 pattern = '.*[.]csv' purge = true;" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from products;" | $MYSQL_CLIENT_CONNECT
echo "list @s0011;" | $MYSQL_CLIENT_CONNECT | grep -o 'csv' | wc -l

