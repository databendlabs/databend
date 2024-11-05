statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(1,2),(3,4),(5,6);

# The number of columns in the target and the source must be the same
statement error 1006
INSERT FIRST
  WHEN c3 > 4 THEN
    INTO t1 (c1) VALUES(c3,c4)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

statement error 1006
INSERT FIRST
  WHEN c3 > 4 THEN
    INTO t1 (c1,c2) VALUES(c3)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

# The condition in WHEN clause must be a boolean expression
statement error 1007
INSERT FIRST
  WHEN 'a' THEN
    INTO t1 (c1,c2) VALUES(c3,c4)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

# The target column in into clause must be a column ref of the target table
statement error 1005
INSERT FIRST
  WHEN c3 > 4 THEN
    INTO t1 (c1 + 1,c2) VALUES(c3,c4)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

# The target column in into clause must be a column ref of the target table
statement error 1006
INSERT FIRST
  WHEN c3 > 4 THEN
    INTO t1 (c3,c2) VALUES(c3,c4)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

# The source column in into clause must be a column ref of the source table
statement error 1065
INSERT FIRST
  WHEN c3 > 4 THEN
    INTO t1 (c1,c2) VALUES(c1,c4)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

# test alias
statement error 1065
INSERT FIRST
  WHEN c3 > 4 THEN
    INTO t1 (c1,c2) VALUES(c3,c4)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT c3,c4 as c5 from s;

# column ref in when clause must be a column ref of the source table
statement error 1065
INSERT FIRST
  WHEN c1 > 4 THEN
    INTO t1 (c1,c2) VALUES(c3,c4)
  WHEN c1 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

# unbranched insert all
statement ok
INSERT ALL
    INTO t1
    INTO t2
SELECT * from s;

query II
select * from t1 order by c1;
----
1 2
3 4
5 6

query II
select * from t2 order by c1;
----
1 2
3 4
5 6

# branched insert all
statement ok
INSERT ALL
    WHEN c3 = 1 THEN
      INTO t1
    WHEN c3 = 3 THEN
      INTO t2
SELECT * from s;

query II
select * from t1 order by c1;
----
1 2
1 2
3 4
5 6

query II
select * from t2 order by c1;
----
1 2
3 4
3 4
5 6

# branched insert first
statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(1,2),(3,4),(5,6);

query II
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1
    WHEN c3 > 0 THEN
      INTO t2
SELECT * from s;
----
1 2

query II
select * from t1 order by c1;
----
5 6

query II
select * from t2 order by c1;
----
1 2
3 4

# projected by source columns
statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(1,2),(3,4),(5,6);

query II
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1 values(c4,c3)
    WHEN c3 > 0 THEN
      INTO t2 values(c4,c3)
SELECT * from s;
----
1 2

query II
select * from t1 order by c1;
----
6 5

query II
select * from t2 order by c1;
----
2 1
4 3

# cast schema to target table
statement ok 
create or replace table t1(c1 bigint,c2 int);

statement ok
create or replace table t2(c1 int,c2 bigint);

query II
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1 values(c4,c3)
    WHEN c3 > 0 THEN
      INTO t2 values(c4,c3)
SELECT * from s;
----
1 2

query II
select * from t1 order by c1;
----
6 5

query II
select * from t2 order by c1;
----
2 1
4 3

# reorder and fill default value
statement ok 
create or replace table t1(c1 bigint,c2 int);

statement ok
create or replace table t2(c1 int,c2 bigint);

query II
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1 (c2) values(c4)
    WHEN c3 > 0 THEN
      INTO t2 (c2,c1) values(c4,c3)
SELECT * from s;
----
1 2

query II
select * from t1 order by c1;
----
NULL 6

query II
select * from t2 order by c1;
----
1 2
3 4

# test cluster key
statement ok 
create or replace table t1(c1 bigint,c2 int) cluster by (c1 + 1);

statement ok
create or replace table t2(c1 int,c2 bigint) cluster by (c1 + 1);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(3,4),(1,2),(5,6);

query II
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1 (c2) values(c4)
    WHEN c3 > 0 THEN
      INTO t2 (c2,c1) values(c4,c3)
SELECT * from s;
----
1 2

query II
select * from t1 order by c1;
----
NULL 6

query II
select * from t2;
----
1 2
3 4

# test multi-statement transaction
statement ok 
create or replace table t1(c1 bigint,c2 int) cluster by (c1 + 1);

statement ok
create or replace table t2(c1 int,c2 bigint) cluster by (c1 + 1);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
begin transaction;

statement ok
insert into s values(3,4),(1,2),(5,6);

query I
select count(*) from fuse_snapshot('default', 's');
----
1

query II
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1 (c2) values(c4)
    WHEN c3 > 0 THEN
      INTO t2 (c2,c1) values(c4,c3)
SELECT * from s;
----
1 2

query I
select count(*) from fuse_snapshot('default', 't1');
----
1

query I
select count(*) from fuse_snapshot('default', 't2');
----
1

query II
select * from t1 order by c1;
----
NULL 6

query II
select * from t2;
----
1 2
3 4

statement ok
rollback;

query I
select count(*) from fuse_snapshot('default', 's');
----
0

query I
select count(*) from fuse_snapshot('default', 't1');
----
0

query I
select count(*) from fuse_snapshot('default', 't2');
----
0


query II
select * from t1 order by c1;
----


query II
select * from t2;
----


# render subquery results
statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(1,2),(3,4),(5,6);

query II
INSERT FIRST
    WHEN c3 = 6 THEN
      INTO t1
    WHEN c3 > 0 THEN
      INTO t2
SELECT (c3 + 1) as c3, (c4 + 2) as c4 from s;
----
1 2

query II
select * from t1 order by c1;
----
6 8

query II
select * from t2 order by c1;
----
2 4
4 6


# same table in different branches
statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(1,2),(3,4),(5,6);

query II
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1
    WHEN c3 > 0 THEN
      INTO t2
      INTO t1
SELECT * from s;
----
3 2

query II
select * from t1 order by c1;
----
1 2
3 4
5 6

query II
select * from t2 order by c1;
----
1 2
3 4


statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(1,2),(3,4),(5,6);

query II
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1 values(123,c3)
    WHEN c3 > 0 THEN
      INTO t2 values(c4,456)
SELECT * from s;
----
1 2

query II
select * from t1 order by c1;
----
123 5

query II
select * from t2 order by c1;
----
2 456
4 456


statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(1,2),(3,4),(5,6);

query II
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1 values(123,DEFAULT)
    WHEN c3 > 0 THEN
      INTO t2 values(c4 + 1,456)
SELECT * from s;
----
1 2

query II
select * from t1 order by c1;
----
123 NULL

query II
select * from t2 order by c1;
----
3 456
5 456

statement ok
CREATE OR REPLACE TABLE orders_placed (order_id INT, customer_id INT, order_amount FLOAT, order_date DATE);

statement ok
CREATE OR REPLACE TABLE processing_updates (order_id INT, update_note VARCHAR);

statement ok
INSERT INTO orders_placed (order_id, customer_id, order_amount, order_date)
VALUES    (101, 1, 250.00, '2023-01-01'),
          (102, 2, 450.00, '2023-01-02'),
          (103, 1, 1250.00, '2023-01-03'),
          (104, 3, 750.00, '2023-01-04'),
          (105, 2, 50.00, '2023-01-05');

query I
INSERT FIRST
WHEN order_amount > 1000 THEN INTO processing_updates VALUES (order_id, 'PriorityHandling')
WHEN order_amount > 500 THEN INTO processing_updates VALUES (order_id, 'ExpressHandling') 
WHEN order_amount > 100 THEN INTO processing_updates VALUES (order_id, 'StandardHandling')
ELSE INTO processing_updates VALUES (order_id, 'ReviewNeeded')
SELECT    order_id,
          order_amount
FROM      orders_placed;
----
5

query IT
select * from processing_updates order by order_id;
----
101 StandardHandling
102 StandardHandling
103 PriorityHandling
104 ExpressHandling
105 ReviewNeeded

# althogh target table is inserted in multi branches, the number of segment should be 1
query I
select count() from fuse_segment('default', 'processing_updates');
----
1

# test default ambiguity
statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,default int);

statement ok
insert into s values(1,2),(3,4),(5,6);

# The column name in source of multi-table insert cant be default
statement error 1006
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1
    WHEN c3 > 0 THEN
      INTO t2 values(c3, default)
SELECT * from s;


query II
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1
    WHEN c3 > 0 THEN
      INTO t2 values(c3, c4)
SELECT c3,default as c4 from s;
----
1 2

query II
select * from t1 order by c1;
----
5 6

query II
select * from t2 order by c1;
----
1 2
3 4