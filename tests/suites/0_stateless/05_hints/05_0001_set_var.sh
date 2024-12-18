#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# mariadb mysql client has some bug, please use mysql official client
# mysql --version
# mysql  Ver 8.0.32-0ubuntu0.20.04.2 for Linux on x86_64 ((Ubuntu))
echo "select * from (select /*+SET_VAR(timezone='Asia/Shanghai')*/ name, value value from system.settings where name in ('timezone') union all (select /*+SET_VAR(timezone='America/Los_Angeles')*/ 'x','x')) order by name desc;" |  $BENDSQL_CLIENT_CONNECT
echo "select * from (select /*+SET_VAR(timezone='America/Los_Angeles')*/ name, value value from system.settings where name in ('timezone') union all (select /*+SET_VAR(timezone='Asia/Shanghai')*/ 'x','x')) order by name desc;" |  $BENDSQL_CLIENT_CONNECT
echo "select /*+SET_VAR(timezone='Asia/Shanghai') */ * from system.settings where name = 'timezone';" |  $BENDSQL_CLIENT_CONNECT
echo "select /*+SET_VAR(timezone='Asia') SET_VAR(storage_read_buffer_size=200)*/ name, value from system.settings where name in ('timezone', 'storage_read_buffer_size')" |  $BENDSQL_CLIENT_CONNECT
echo "select /*+SET_VAR(timezone='Asia/Shanghai') SET_VAR(storage_read_buffer_size=200)*/ /*+SET_VAR(storage_read_buffer_size=100)*/name, /*+xx*/ value from system.settings where name in ('timezone', 'storage_read_buffer_size')" |  $BENDSQL_CLIENT_CONNECT
echo "select /*+SET_VA(timezone='Asia/Shanghai') storage_read_buffer_size=200 SET_VAR(storage_read_buffer_size=100)*/name, /*+xx*/ value from system.settings where name in ('timezone', 'storage_read_buffer_size')" |  $BENDSQL_CLIENT_CONNECT
echo "select /*+ SET_VAR(timezone=x) SET_VAR(timezone=select 1) */ name, value from system.settings where name='timezone';" |  $BENDSQL_CLIENT_CONNECT
echo "select /*+ SET_VAR(storage_read_buffer_size=200) SET_VAR(timezone=x) */ name, value from system.settings where name='timezone' or name = 'storage_read_buffer_size';" |  $BENDSQL_CLIENT_CONNECT

echo "drop database if exists set_var;" | $BENDSQL_CLIENT_CONNECT
echo "create database set_var;" | $BENDSQL_CLIENT_CONNECT
echo "create table set_var.test(id int);" | $BENDSQL_CLIENT_CONNECT
echo "insert /*+SET_VAR(timezone='Asia/Shanghai') SET_VAR(storage_read_buffer_size=200)*/ into set_var.test values(1)" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "insert /*+SET_VAR(timezone='Asia/Shanghai') (storage_read_buffer_size=200)*/ into set_var.test values(3)" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "select /*+SET_VAR(timezone='Asia/Shanghai') SET_VAR(storage_read_buffer_size=200)*/ * from set_var.test order by id" | $BENDSQL_CLIENT_CONNECT
echo "select /*+SET_VAR(timezone='Asia/Shanghai') (storage_read_buffer_size=200)*/ id from set_var.test order by id" | $BENDSQL_CLIENT_CONNECT
echo "update /*+SET_VAR(timezone='Asia/Shanghai') SET_VAR(storage_read_buffer_size=200)*/ set_var.test set id=2 where id=1" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "update /*+SET_VAR(timezone='Asia/Shanghai') (storage_read_buffer_size=200)*/ set_var.test set id=4 where id=3" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "select * from set_var.test order by id" | $BENDSQL_CLIENT_CONNECT
echo "delete /*+SET_VAR(timezone='Asia/Shanghai') SET_VAR(storage_read_buffer_size=200)*/ from set_var.test where id=2" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "delete /*+SET_VAR(timezone='Asia/Shanghai') (storage_read_buffer_size=200)*/ from set_var.test where id=4" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "select * from set_var.test" | $BENDSQL_CLIENT_CONNECT
echo "set timezone='America/Toronto'; select /*+SET_VAR(timezone='Asia/Shanghai') */ timezone(); select timezone();" | $BENDSQL_CLIENT_CONNECT
echo "create table set_var.t(c1 timestamp)" | $BENDSQL_CLIENT_CONNECT
# Toronto and Shanghai time diff is 13 hours.
echo "set timezone='America/Toronto'; insert /*+SET_VAR(timezone='Asia/Shanghai') */ into set_var.t values('2022-02-02 03:00:00'); select /*+SET_VAR(timezone='Asia/Shanghai') */ * from set_var.t; select * from set_var.t;" | $BENDSQL_CLIENT_CONNECT
echo "drop database set_var;" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists s2" | $BENDSQL_CLIENT_CONNECT
echo "create stage s2" | $BENDSQL_CLIENT_CONNECT
echo "copy  /*+SET_VAR(timezone='Asia/Shanghai') */ into @s2 from (select timezone()); " | $BENDSQL_CLIENT_CONNECT
echo "select * from @s2 " | $BENDSQL_CLIENT_CONNECT
echo "drop stage s2" | $BENDSQL_CLIENT_CONNECT
