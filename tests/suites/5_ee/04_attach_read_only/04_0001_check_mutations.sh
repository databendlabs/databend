#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace database test_attach_only;" | bendsql_connect_root

# mutation related enterprise features

echo "create or replace table test_attach_only.test_json(id int, val json) 's3://testbucket/data/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | bendsql_connect_root
echo "insert into test_attach_only.test_json values(1, '{\"a\":33,\"b\":44}'),(2, '{\"a\":55,\"b\":66}')" | bendsql_connect_root_null
storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table test_attach_only.test_json" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')
echo "attach table test_attach_only.test_json_read_only 's3://testbucket/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | bendsql_connect_root


echo "create index should fail"
echo "CREATE INVERTED INDEX IF NOT EXISTS idx1 ON test_attach_only.test_json_read_only(val)" | bendsql_connect_root

# vacuum
echo "vacuum table"

echo "vacuum table should fail"
echo "VACUUM TABLE test_attach_only.test_json_read_only;" | bendsql_connect_root

echo "vacuum drop table from db should not include the read_only attach table"
# drop & vacuum
echo "drop table test_attach_only.test_json_read_only" | bendsql_connect_root
echo "vacuum drop table from test_attach_only" | bendsql_connect_root > /dev/null
# attach it back
echo "attach table test_attach_only.test_json_read_only 's3://testbucket/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}')" | bendsql_connect_root
echo "expect table data still there"
echo "select * from test_attach_only.test_json_read_only order by id" | bendsql_connect_root


