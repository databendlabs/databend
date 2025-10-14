#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace database test" | $BENDSQL_CLIENT_CONNECT


stmt """
CREATE TABLE test.a (    a bigint not null, b int not null default 3, c varchar(255) not null default 'x', d smallint null, e Date not null) Engine = Null
"""


query "SHOW CREATE TABLE test.a"

query "SHOW CREATE TABLE test.a WITH QUOTED_IDENTIFIERS"


stmt """
CREATE TABLE test.b ( a bigint not null comment 'abc '' vs \' cde', b int null default null, c varchar(255) not null, d smallint unsigned null) Engine = Null COMMENT = 'abc '' vs \' cde'
"""

query """
select
t.comment as table_comment,
c.comment as column_comment
from
system.tables t
join system.columns c on t.database = c.database
and t.name = c.table and c.name = 'a'
where
t.database = 'test'
and t.name = 'b'
"""


echo """
set hide_options_in_show_create_table=0;
SHOW CREATE TABLE test.b;
""" | $BENDSQL_CLIENT_CONNECT

stmt "create or replace view test.v_b as select * from test.b"

query "show create table test.v_b"
stmt "CREATE TABLE test.c (a int not null) CLUSTER BY (a, a % 3) COMPRESSION='lz4' STORAGE_FORMAT='parquet'"
query "SHOW CREATE TABLE test.c"


stmt """
CREATE OR REPLACE TABLE test.d (a int not null) ENGINE=FUSE 'fs:///tmp/load/files/' CONNECTION=(access_key_id='1a2b3c' aws_secret_key='4x5y6z') CLUSTER BY (a, a % 3) COMPRESSION='lz4' STORAGE_FORMAT='parquet'
"""


query "SHOW CREATE TABLE test.d"
query "SHOW CREATE TABLE test.c"




echo """
create or replace table test.t(c1 int, c2 int);
create or replace table test.t1(c1 int, c2 string);
""" | $BENDSQL_CLIENT_CONNECT

stmt "create or replace view test.v_union as select c1 from test.t union all select c1 from test.t1;"

stmt "create or replace view test.v_subquery as select * from (select c1 from test.t union all select c1 from test.t1) as res;"

stmt "create or replace view test.v_join as select * from (select c1 from test.t union all select c1 from test.t1) as res join t1 on t1.c1=res.c1;"

query "show create table test.v_t"
query "show create table test.v_union"
query "show create table test.v_subquery;"
query "show create table test.v_join;"

stmt """
create or replace table test.tc(id int, Id1_ int, \"id2\)\" int, "ID4" int, "ID""5" int);
"""

echo  """
set hide_options_in_show_create_table=1;
set quoted_ident_case_sensitive=0;
show create table test.tc;
set quoted_ident_case_sensitive=1;
show create table test.tc;

set quoted_ident_case_sensitive=1;
set sql_dialect='MySQL';
show create table test.tc;
""" | $BENDSQL_CLIENT_CONNECT

stmt """
create or replace table test.auto_increment(c1 int, c2 int autoincrement, c3 int identity (2, 3) order);
"""

query "show create table test.auto_increment;"


stmt "drop database test"
