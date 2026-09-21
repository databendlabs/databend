#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# mariadb mysql client has some bug, please use mysql official client
# mysql --version
# mysql  Ver 8.0.32-0ubuntu0.20.04.2 for Linux on x86_64 ((Ubuntu))
echo "select * from (select /*+SET_VAR(timezone='Asia/Shanghai')*/ name, value value from system.settings where name in ('timezone') union all (select /*+SET_VAR(timezone='America/Los_Angeles')*/ 'x','x')) order by name desc;" |  bendsql_connect_root
echo "select * from (select /*+SET_VAR(timezone='America/Los_Angeles')*/ name, value value from system.settings where name in ('timezone') union all (select /*+SET_VAR(timezone='Asia/Shanghai')*/ 'x','x')) order by name desc;" |  bendsql_connect_root
echo "select /*+SET_VAR(timezone='Asia/Shanghai') */ * from system.settings where name = 'timezone';" |  bendsql_connect_root
echo "select /*+SET_VAR(timezone='Asia') SET_VAR(storage_read_buffer_size=200)*/ name, value from system.settings where name in ('timezone', 'storage_read_buffer_size')" |  bendsql_connect_root
echo "select /*+SET_VAR(timezone='Asia/Shanghai') SET_VAR(storage_read_buffer_size=200)*/ /*+SET_VAR(storage_read_buffer_size=100)*/name, /*+xx*/ value from system.settings where name in ('timezone', 'storage_read_buffer_size')" |  bendsql_connect_root
echo "select /*+SET_VA(timezone='Asia/Shanghai') storage_read_buffer_size=200 SET_VAR(storage_read_buffer_size=100)*/name, /*+xx*/ value from system.settings where name in ('timezone', 'storage_read_buffer_size')" |  bendsql_connect_root
echo "select /*+ SET_VAR(timezone=x) SET_VAR(timezone=select 1) */ name, value from system.settings where name='timezone';" |  bendsql_connect_root
echo "select /*+ SET_VAR(storage_read_buffer_size=200) SET_VAR(timezone=x) */ name, value from system.settings where name='timezone' or name = 'storage_read_buffer_size';" |  bendsql_connect_root

echo "drop database if exists set_var;" | bendsql_connect_root
echo "create database set_var;" | bendsql_connect_root
echo "create table set_var.test(id int);" | bendsql_connect_root
echo "insert /*+SET_VAR(timezone='Asia/Shanghai') SET_VAR(storage_read_buffer_size=200)*/ into set_var.test values(1)" | bendsql_connect_root_null
echo "insert /*+SET_VAR(timezone='Asia/Shanghai') (storage_read_buffer_size=200)*/ into set_var.test values(3)" | bendsql_connect_root_null
echo "select /*+SET_VAR(timezone='Asia/Shanghai') SET_VAR(storage_read_buffer_size=200)*/ * from set_var.test order by id" | bendsql_connect_root
echo "select /*+SET_VAR(timezone='Asia/Shanghai') (storage_read_buffer_size=200)*/ id from set_var.test order by id" | bendsql_connect_root
echo "update /*+SET_VAR(timezone='Asia/Shanghai') SET_VAR(storage_read_buffer_size=200)*/ set_var.test set id=2 where id=1" | bendsql_connect_root_null
echo "update /*+SET_VAR(timezone='Asia/Shanghai') (storage_read_buffer_size=200)*/ set_var.test set id=4 where id=3" | bendsql_connect_root_null
echo "select * from set_var.test order by id" | bendsql_connect_root
echo "delete /*+SET_VAR(timezone='Asia/Shanghai') SET_VAR(storage_read_buffer_size=200)*/ from set_var.test where id=2" | bendsql_connect_root_null
echo "delete /*+SET_VAR(timezone='Asia/Shanghai') (storage_read_buffer_size=200)*/ from set_var.test where id=4" | bendsql_connect_root_null
echo "select * from set_var.test" | bendsql_connect_root
echo "set timezone='America/Toronto'; select /*+SET_VAR(timezone='Asia/Shanghai') */ timezone(); select timezone();" | bendsql_connect_root
echo "create table set_var.t(c1 timestamp)" | bendsql_connect_root
# Toronto and Shanghai time diff is 13 hours.
echo "set timezone='America/Toronto'; insert /*+SET_VAR(timezone='Asia/Shanghai') */ into set_var.t values('2022-02-02 03:00:00'); select /*+SET_VAR(timezone='Asia/Shanghai') */ * from set_var.t; select * from set_var.t;" | bendsql_connect_root
echo "drop database set_var;" | bendsql_connect_root
echo "drop stage if exists s2" | bendsql_connect_root
echo "create stage s2" | bendsql_connect_root
echo "copy  /*+SET_VAR(timezone='Asia/Shanghai') */ into @s2 from (select timezone()); " | bendsql_connect_root | cut -d$'\t' -f1,2
echo "select * from @s2 " | bendsql_connect_root
echo "drop stage s2" | bendsql_connect_root
