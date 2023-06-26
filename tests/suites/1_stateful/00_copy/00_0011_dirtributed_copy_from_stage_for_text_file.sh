#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "set global max_threads = 1" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists products;" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists s1;" | $MYSQL_CLIENT_CONNECT
echo "CREATE STAGE s1 FILE_FORMAT = (TYPE = CSV);" | $MYSQL_CLIENT_CONNECT
echo "create table products (id int, name string, description string);" | $MYSQL_CLIENT_CONNECT

#multi files to trigger distributed test
curl -s -u root: -H "stage_name:s1" -F "upload=@${CURDIR}/../../../data/ttt.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -H "stage_name:s1" -F "upload=@${CURDIR}/../../../data/sample.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -H "stage_name:s1" -F "upload=@${CURDIR}/../../../data/sample_3_replace.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -H "stage_name:s1" -F "upload=@${CURDIR}/../../../data/sample_3_duplicate.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"

echo "copy into products from @s1 pattern = '.*[.]csv';" | $MYSQL_CLIENT_CONNECT
echo "select * from products order by id,name,description;" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from products;" | $MYSQL_CLIENT_CONNECT
echo "select block_count from fuse_snapshot('default','products');" | $MYSQL_CLIENT_CONNECT
