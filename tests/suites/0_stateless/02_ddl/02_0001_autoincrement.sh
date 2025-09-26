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
insert into test_autoincrement (c1) values(0);
"""

query "select * from test_autoincrement order by c2;"

stmt """
alter table test_autoincrement drop column c2;
"""

query "select * from test_autoincrement order by c2;"

stmt """
drop table test_autoincrement;
"""

stmt "drop database test"