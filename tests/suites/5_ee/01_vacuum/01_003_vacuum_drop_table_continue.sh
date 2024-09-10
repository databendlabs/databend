#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "create or replace database test_vacuum_drop_table_continue" 

mkdir -p /tmp/test_vacuum_drop_table_continue/

stmt "create table test_vacuum_drop_table_continue.a(c int) 'fs:///tmp/test_vacuum_drop_table_continue/'" 

stmt "create table test_vacuum_drop_table_continue.b(c int)" 

stmt "create table test_vacuum_drop_table_continue.c(c int)" 

stmt "create table test_vacuum_drop_table_continue.d(c int)" 


stmt "insert into test_vacuum_drop_table_continue.a values (1)" 

stmt "insert into test_vacuum_drop_table_continue.b values (1)" 

stmt "insert into test_vacuum_drop_table_continue.c values (1)" 

stmt "insert into test_vacuum_drop_table_continue.d values (1)" 

chmod 444 /tmp/test_vacuum_drop_table_continue/

stmt "drop database test_vacuum_drop_table_continue" 

# can't vacuum files of table a, but can go on vacuum other tables
stmt "set data_retention_time_in_days=0; vacuum drop table" 

chmod 755 /tmp/test_vacuum_drop_table_continue/
find /tmp/test_vacuum_drop_table_continue/ -type d -exec chmod 755 {} +
find /tmp/test_vacuum_drop_table_continue/ -type f -exec chmod 644 {} +

stmt "undrop database test_vacuum_drop_table_continue" 

query "use test_vacuum_drop_table_continue;show tables" 

query "select * from test_vacuum_drop_table_continue.a" 
