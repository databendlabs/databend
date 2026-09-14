#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace DATABASE test_failsafe" |  bendsql_connect_root

echo "create table test_failsafe.t(a int) 's3://testbucket/test_amend/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='http://127.0.0.1:9900')" | bendsql_connect_root

echo "CREATE or replace stage test_amend_stage URL='s3://testbucket/test_amend/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='http://127.0.0.1:9900')" | bendsql_connect_root


# generate multiple blocks & segments

echo "insert into test_failsafe.t select * from numbers(11)" | bendsql_connect_root_null
echo "insert into test_failsafe.t select * from numbers(11)" | bendsql_connect_root_null
echo "insert into test_failsafe.t select * from numbers(11)" | bendsql_connect_root_null

echo "remove @test_amend_stage" | bendsql_connect_root


echo "check table is corrupted"
echo "select * from test_failsafe.t" | bendsql_connect_root 2>&1 | grep "NotFound" | wc -l

echo "call system\$fuse_amend('test_failsafe', 't')" | bendsql_connect_root


echo "verify that table restored"
echo "select sum(a) from  test_failsafe.t" | bendsql_connect_root

echo "DROP DATABASE IF EXISTS test_failsafe" | bendsql_connect_root
