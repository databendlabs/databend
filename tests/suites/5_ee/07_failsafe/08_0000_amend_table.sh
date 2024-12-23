#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace DATABASE test_failsafe" |  $BENDSQL_CLIENT_CONNECT

echo "create table test_failsafe.t(a int) 's3://testbucket/test_amend/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='http://127.0.0.1:9900')" | $BENDSQL_CLIENT_CONNECT

echo "CREATE or replace stage test_amend_stage URL='s3://testbucket/test_amend/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='http://127.0.0.1:9900')" | $BENDSQL_CLIENT_CONNECT


# generate multiple blocks & segments

echo "insert into test_failsafe.t select * from numbers(11)" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "insert into test_failsafe.t select * from numbers(11)" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "insert into test_failsafe.t select * from numbers(11)" | $BENDSQL_CLIENT_OUTPUT_NULL

echo "remove @test_amend_stage" | $BENDSQL_CLIENT_CONNECT


echo "check table is corrupted"
echo "select * from test_failsafe.t" | $BENDSQL_CLIENT_CONNECT 2>&1 | grep "NotFound" | wc -l

echo "call system\$fuse_amend('test_failsafe', 't')" | $BENDSQL_CLIENT_CONNECT


echo "verify that table restored"
echo "select sum(a) from  test_failsafe.t" | $BENDSQL_CLIENT_CONNECT

echo "DROP DATABASE IF EXISTS test_failsafe" | $BENDSQL_CLIENT_CONNECT
