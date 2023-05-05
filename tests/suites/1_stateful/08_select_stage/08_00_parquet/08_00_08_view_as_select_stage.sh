#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop view if exists view_08_00_08;" |  $MYSQL_CLIENT_CONNECT

echo "create view view_08_00_08 as select * from 's3://testbucket/admin/data/ontime_200.parquet' (aws_key_id => 'minioadmin', aws_secret_key => 'minioadmin', endpoint_url => 'http://127.0.0.1:9900/', file_format => 'parquet');" | $MYSQL_CLIENT_CONNECT

echo "select count(*) from view_08_00_08;" | $MYSQL_CLIENT_CONNECT

echo "select tail_number from view_08_00_08 where dayofmonth=1;" |  $MYSQL_CLIENT_CONNECT

echo "select tail_number from view_08_00_08 where dayofmonth > 15 order by tail_number limit 5;" |  $MYSQL_CLIENT_CONNECT

echo "select tail_number from view_08_00_08 where dayofmonth > 15 order by tail_number desc limit 5;" |  $MYSQL_CLIENT_CONNECT

echo "select month from view_08_00_08 where dayofmonth > 15 order by tail_number limit 5;" |  $MYSQL_CLIENT_CONNECT

echo "select month from view_08_00_08 where dayofmonth > 15 order by tail_number desc limit 5;" |  $MYSQL_CLIENT_CONNECT

echo "drop view view_08_00_08;" |  $MYSQL_CLIENT_CONNECT