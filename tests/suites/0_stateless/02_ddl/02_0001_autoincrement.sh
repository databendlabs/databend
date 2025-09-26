#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace database test" | $BENDSQL_CLIENT_CONNECT


stmt """
create or replace table test_autoincrement (c1 int, c2 string autoincrement);
"""
stmt """
create or replace table test_autoincrement (c1 int, c2 int default 1 autoincrement);
"""
stmt """
create or replace table test_autoincrement (c1 int, c2 int autoincrement, c3 float autoincrement, c4 int identity (1,2), c5 int autoincrement start 1 increment 2, c6 decimal(20, 2) autoincrement);
"""

stmt """
insert into test_autoincrement (c1) values(0);
"""

stmt """
insert into test_autoincrement (c1) values(0);
"""

stmt """
insert into test_autoincrement (c1) values(0);
"""

query "select * from test_autoincrement order by c2;"

stmt """
alter table test_autoincrement add column c4 string autoincrement;
"""
stmt """
alter table test_autoincrement add column c4 int default autoincrement;
"""
stmt """
alter table test_autoincrement add column c7 int autoincrement;
"""

stmt """
alter table test_autoincrement drop column c3;
"""

query "select * from test_autoincrement order by c2;"

# test add column success on empty table
stmt """
create or replace table test_empty_autoincrement (c1 int);
"""

stmt """
alter table test_empty_autoincrement add column c2 int autoincrement;
"""

stmt """
insert into test_empty_autoincrement (c1) values(0);
"""
stmt """
insert into test_empty_autoincrement (c1) values(0);
"""

query "select * from test_empty_autoincrement order by c2;"

stmt """
drop table test_empty_autoincrement;
"""

stmt "drop database test"