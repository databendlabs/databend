#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

expected="0"

for i in {1..100}
do
    stage="@s""$i"
    name="s""$i"
    echo "drop table if exists products;" | $MYSQL_CLIENT_CONNECT
    echo "drop table if exists table_random;" | $MYSQL_CLIENT_CONNECT
    echo "drop table if exists table_random2;" | $MYSQL_CLIENT_CONNECT
    echo "drop stage if exists $name;" | $MYSQL_CLIENT_CONNECT
    echo "create stage $name FILE_FORMAT = (TYPE = CSV);" | $MYSQL_CLIENT_CONNECT
    echo "create table products (id int, name string, description string);" | $MYSQL_CLIENT_CONNECT
    echo "create table table_random(a int,b string,c string) ENGINE = Random;" | $MYSQL_CLIENT_CONNECT
    echo "create table table_random2(a int,b string) ENGINE = Random;" | $MYSQL_CLIENT_CONNECT
    echo "copy into $stage from (select a,b,c from table_random limit 1000000);" | $MYSQL_CLIENT_CONNECT
    echo "copy into $stage from (select a,b,c from table_random limit 1000000);" | $MYSQL_CLIENT_CONNECT
    echo "copy into $stage from (select a,b,c from table_random limit 1000000);" | $MYSQL_CLIENT_CONNECT
    echo "copy into $stage from (select a,b,c from table_random limit 1000000);" | $MYSQL_CLIENT_CONNECT
    echo "copy into $stage from (select a,b,c from table_random limit 1000000);" | $MYSQL_CLIENT_CONNECT
    echo "copy into $stage from (select a,b,c from table_random limit 1000000);" | $MYSQL_CLIENT_CONNECT
    echo "copy into $stage from (select a,b,c from table_random limit 1000000);" | $MYSQL_CLIENT_CONNECT
    echo "copy into $stage from (select a,b from table_random2 limit 1000000);" | $MYSQL_CLIENT_CONNECT
    echo "set enable_distributed_copy_into = 1;copy into products from $stage pattern = '.*[.]csv' purge = true;" | $MYSQL_CLIENT_CONNECT
    res=$(echo "select count(*) from products;" | $MYSQL_CLIENT_CONNECT)
    echo "$res"
    if [ $res = $expected ]
    then
        echo "drop table products all;" | $MYSQL_CLIENT_CONNECT
    else
        echo "终于复现了: $i"
        break
    fi
done