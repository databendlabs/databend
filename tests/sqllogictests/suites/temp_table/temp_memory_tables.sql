statement ok
CREATE TEMPORARY TABLE t(c1 int) 

statement ok
CREATE TEMP TABLE IF NOT EXISTS t(c1 int) engine=MEMORY;

statement error 2302
CREATE TEMP TABLE t(c1 int)  engine=MEMORY;

statement ok
create temp table t2(a int,b int)  engine=MEMORY;

statement ok
insert into t2 values(1,1),(2,2)

query I
select a+b from t2 order by a+b
----
2
4

statement error 2302
create temp table t2(a int,b int)  engine=MEMORY;

statement error 2302
create temp table t2(a int,b int)  engine=MEMORY;

statement error 1005
create temp table t2(a INT auto_increment) engine=MEMORY;

statement error 1006
create temp table t3(a int,b int) engine=null engine=MEMORY;

statement ok
create temp table t3(`a` int)  engine=MEMORY;

statement ok
create temp table t4(a int)  engine=MEMORY;

statement ok
DROP TABLE IF EXISTS t

statement ok
DROP TABLE IF EXISTS t2

statement ok
DROP TABLE IF EXISTS t3

statement ok
DROP TABLE IF EXISTS t4

statement ok
DROP DATABASE IF EXISTS db1

statement ok
DROP DATABASE IF EXISTS db2

statement ok
CREATE DATABASE db1

statement ok
CREATE DATABASE db2

statement ok
CREATE TEMP TABLE db1.test1(a INT not null, b INT null)  engine=MEMORY;

statement ok
INSERT INTO db1.test1 VALUES (1, 2), (2, 3), (3, 4)

query I
select * from db1.test1
----
1 2
2 3
3 4

statement ok
CREATE TEMP TABLE db2.test2 LIKE db1.test1  engine=MEMORY;

statement ok
INSERT INTO db2.test2 VALUES (3, 5)

query I
SELECT a+b FROM db2.test2
----
8

query TTTTT
DESCRIBE db2.test2
----
a INT NO 0 (empty)
b INT YES NULL (empty)

statement ok
CREATE TEMP TABLE db2.test3(a Varchar null, y Varchar null) engine=MEMORY AS SELECT * FROM db1.test1;

query TTTTT
DESCRIBE db2.test3
----
a VARCHAR YES NULL (empty)
y VARCHAR YES NULL (empty)

query T
SELECT a FROM db2.test3
----
1
2
3

statement ok
CREATE TEMP TABLE db2.test4(a Varchar null, y Varchar null) engine=MEMORY AS SELECT b, a FROM db1.test1;

statement ok
CREATE TEMP TABLE if not exists db2.test4 engine=MEMORY AS SELECT b, a FROM db1.test1;

statement ok
CREATE TEMP TABLE if not exists db2.test4 engine=MEMORY AS SELECT b, a FROM db1.test1;

query TTTTT
DESCRIBE db2.test4
----
a VARCHAR YES NULL (empty)
y VARCHAR YES NULL (empty)

query T
SELECT a FROM db2.test4
----
2
3
4

statement error 1
CREATE TEMP TABLE db2.test5(a Varchar null, y Varchar null) engine=MEMORY AS SELECT b FROM db1.test1;

statement error 1006
CREATE TEMP TABLE db2.test5(a Varchar null, y Varchar null) engine=MEMORY AS SELECT a, b, a FROM db1.test1;

statement ok
create temp table db2.test55(id Int8, created timestamp  not null DEFAULT CURRENT_TIMESTAMP) engine=MEMORY;

statement ok
insert into db2.test55(id) select number % 3 from numbers(1000)

statement ok
insert into db2.test55(id) select number % 3 from numbers(1000)

query I
select count(distinct created) > 1 from db2.test55;
----
1

statement error 1065
create temp table db2.test6(id Int8, created timestamp  DEFAULT today() + a) engine=MEMORY;

statement ok
create temp table db2.test6(id Int8 not null, a Int8 not null DEFAULT 1 + 2, created timestamp not null DEFAULT now()) engine=MEMORY;

query TTTTT
desc db2.test6
----
id TINYINT NO 0 (empty)
a TINYINT NO 3 (empty)
created TIMESTAMP NO now() (empty)

statement ok
INSERT INTO db2.test6 (id) VALUES (1)

query IIT
SELECT id, a, now() >= created FROM db2.test6;
----
1 3 1

statement ok
create temp table db2.test7(tiny TINYINT(10) not null, tiny_unsigned TINYINT(10) UNSIGNED not null, smallint SMALLINT not null, smallint_unsigned SMALLINT UNSIGNED not null, int INT32(15) not null, int_unsigned UINT32(15) not null, bigint BIGINT not null, bigint_unsigned BIGINT UNSIGNED not null,float FLOAT not null, double DOUBLE not null, date DATE not null, datetime DATETIME not null, ts TIMESTAMP not null, str VARCHAR not null default '3', bool BOOLEAN not null, arr ARRAY(INT) not null, tup TUPLE(INT, BOOL) not null, map MAP(INT, STRING) not null, bitmap BITMAP not null, variant VARIANT not null) engine=MEMORY;

query TTTTT
desc db2.test7
----
tiny TINYINT NO 0 (empty)
tiny_unsigned TINYINT UNSIGNED NO 0 (empty)
smallint SMALLINT NO 0 (empty)
smallint_unsigned SMALLINT UNSIGNED NO 0 (empty)
int INT NO 0 (empty)
int_unsigned INT UNSIGNED NO 0 (empty)
bigint BIGINT NO 0 (empty)
bigint_unsigned BIGINT UNSIGNED NO 0 (empty)
float FLOAT NO 0 (empty)
double DOUBLE NO 0 (empty)
date DATE NO '1970-01-01' (empty)
datetime TIMESTAMP NO '1970-01-01 00:00:00.000000' (empty)
ts TIMESTAMP NO '1970-01-01 00:00:00.000000' (empty)
str VARCHAR NO '3' (empty)
bool BOOLEAN NO false (empty)
arr ARRAY(INT32) NO [] (empty)
tup TUPLE(1 INT32, 2 BOOLEAN) NO (NULL, NULL) (empty)
map MAP(INT32, STRING) NO {} (empty)
bitmap BITMAP NO '' (empty)
variant VARIANT NO 'null' (empty)

statement ok
create transient table db2.test8(tiny TINYINT not null, tiny_unsigned TINYINT UNSIGNED not null, smallint SMALLINT not null, smallint_unsigned SMALLINT UNSIGNED not null, int INT not null, int_unsigned INT UNSIGNED not null, bigint BIGINT not null, bigint_unsigned BIGINT UNSIGNED not null,float FLOAT not null, double DOUBLE not null, date DATE not null, datetime DATETIME not null, ts TIMESTAMP not null, str VARCHAR not null default '3', bool BOOLEAN not null, arr ARRAY(VARCHAR) not null, tup TUPLE(DOUBLE, INT) not null, map MAP(STRING, Date) not null, bitmap BITMAP not null, variant VARIANT not null)

query TTTTT
desc db2.test8
----
tiny TINYINT NO 0 (empty)
tiny_unsigned TINYINT UNSIGNED NO 0 (empty)
smallint SMALLINT NO 0 (empty)
smallint_unsigned SMALLINT UNSIGNED NO 0 (empty)
int INT NO 0 (empty)
int_unsigned INT UNSIGNED NO 0 (empty)
bigint BIGINT NO 0 (empty)
bigint_unsigned BIGINT UNSIGNED NO 0 (empty)
float FLOAT NO 0 (empty)
double DOUBLE NO 0 (empty)
date DATE NO '1970-01-01' (empty)
datetime TIMESTAMP NO '1970-01-01 00:00:00.000000' (empty)
ts TIMESTAMP NO '1970-01-01 00:00:00.000000' (empty)
str VARCHAR NO '3' (empty)
bool BOOLEAN NO false (empty)
arr ARRAY(STRING) NO [] (empty)
tup TUPLE(1 FLOAT64, 2 INT32) NO (NULL, NULL) (empty)
map MAP(STRING, DATE) NO {} (empty)
bitmap BITMAP NO '' (empty)
variant VARIANT NO 'null' (empty)


statement ok
use db2

statement ok
create temp table test9 like test8 engine=MEMORY;

statement ok
create temp table test10(a int, b tuple(int, int) default (1, 2), c array(int) default [10,20]) engine=MEMORY;

query TTTTT
desc test10
----
a INT YES NULL (empty)
b TUPLE(1 INT32, 2 INT32) YES (1, 2) (empty)
c ARRAY(INT32) YES [10, 20] (empty)

statement ok
insert into test10 (a) values (100),(200)

query ITT
select * from test10
----
100 (1,2) [10,20]
200 (1,2) [10,20]

statement ok
use default

statement ok
DROP DATABASE db1

statement ok
DROP DATABASE db2

statement error 1002
CREATE TEMP TABLE system.test(a INT) engine=MEMORY;

statement ok
drop table if exists t

statement error Duplicated column name
create temp table t(a int, a int) engine=MEMORY;

statement error Duplicated column name
create temp table t(a int, A int) engine=MEMORY;

statement error Duplicated column name
create temp table t engine=MEMORY as select number, number from numbers(1);

statement error 1006
create temp table t engine=MEMORY as select 'a' as c1, null as c2;