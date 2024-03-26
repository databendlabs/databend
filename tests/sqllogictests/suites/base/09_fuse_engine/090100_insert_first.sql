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
statement error 1006
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
statement error 1006
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