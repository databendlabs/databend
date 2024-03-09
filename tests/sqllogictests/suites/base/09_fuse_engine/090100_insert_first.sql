statement ok 
create or replace table t1(c int);

statement ok
create or replace table t2(c int);

statement ok
create or replace table s(c int);

statement ok
insert into s select * from numbers(5);

statement ok
INSERT FIRST
  WHEN c > 4 THEN
    INTO t1
  WHEN c > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT c from s;