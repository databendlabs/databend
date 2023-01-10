#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/tuple.parquet /tmp/06_0001_complex.parquet  > /dev/null 2>&1

echo "select * from read_parquet('/tmp/06_0001_complex.parquet');" |  $MYSQL_CLIENT_CONNECT

echo "select * from read_parquet('/tmp/06_0001_complex.parquet') where t:a = 1;" |  $MYSQL_CLIENT_CONNECT

echo "select * from read_parquet('/tmp/06_0001_complex.parquet') where t:a = id;" |  $MYSQL_CLIENT_CONNECT

echo "select * from read_parquet('/tmp/06_0001_complex.parquet') where t:a >= 2;" |  $MYSQL_CLIENT_CONNECT

echo "select t:b from read_parquet('/tmp/06_0001_complex.parquet') where t:a >= 2;" |  $MYSQL_CLIENT_CONNECT

echo "select t:b from read_parquet('/tmp/06_0001_complex.parquet');" |  $MYSQL_CLIENT_CONNECT

echo "select t from read_parquet('/tmp/06_0001_complex.parquet');" |  $MYSQL_CLIENT_CONNECT

echo "select id, t:a, t:b, t from read_parquet('/tmp/06_0001_complex.parquet');" |  $MYSQL_CLIENT_CONNECT

echo "select id, t:a, t:b, t from read_parquet('/tmp/06_0001_complex.parquet') where id > 2;" |  $MYSQL_CLIENT_CONNECT

echo "select id, t:a, t:b, t from read_parquet('/tmp/06_0001_complex.parquet') where t:b < 'c';" |  $MYSQL_CLIENT_CONNECT

echo "select * from read_parquet('/tmp/06_0001_complex.parquet') where t:b < 'c';" |  $MYSQL_CLIENT_CONNECT

echo "select t:a from read_parquet('/tmp/06_0001_complex.parquet') where t:b < 'c';" |  $MYSQL_CLIENT_CONNECT

echo "select id from read_parquet('/tmp/06_0001_complex.parquet') where t:b < 'c';" |  $MYSQL_CLIENT_CONNECT

aws --endpoint-url http://127.0.0.1:9900/ s3 cp s3://testbucket/admin/data/complex.parquet /tmp/06_0001_complex.parquet  > /dev/null 2>&1

echo "select meta from read_parquet('/tmp/06_0001_complex.parquet') limit 3;" |  $MYSQL_CLIENT_CONNECT

echo "select meta.2, meta.6 from read_parquet('/tmp/06_0001_complex.parquet') limit 3;" |  $MYSQL_CLIENT_CONNECT

echo "select name from read_parquet('/tmp/06_0001_complex.parquet') limit 3;" |  $MYSQL_CLIENT_CONNECT

echo "select name[1] from read_parquet('/tmp/06_0001_complex.parquet') limit 3;" |  $MYSQL_CLIENT_CONNECT

echo "select name[1].5 from read_parquet('/tmp/06_0001_complex.parquet') limit 3;" |  $MYSQL_CLIENT_CONNECT

echo "select name[2] from read_parquet('/tmp/06_0001_complex.parquet') limit 3;" |  $MYSQL_CLIENT_CONNECT

echo "select name[2].6 from read_parquet('/tmp/06_0001_complex.parquet') limit 3;" |  $MYSQL_CLIENT_CONNECT
