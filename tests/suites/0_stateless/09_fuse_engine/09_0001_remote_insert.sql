DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a UInt32, b UInt64, c String) Engine = fuse;
INSERT INTO t1 (a,b,c) values ( 1.00, 1, '1' ), (2, 2.000, '"2"-"2"');

SELECT * FROM t1;
SELECT sum(a) from t1;
SELECT sum(b) from t1;

CREATE TABLE IF NOT EXISTS t2(a Boolean, b TimeStamp, c Date) Engine = fuse;
INSERT INTO t2 (a,b,c) values(true, '2021-09-07 21:38:35.000000', '2021-09-07'), (false, 1631050715000000, 18877);
SELECT * FROM t2;

DROP TABLE t1;
DROP TABLE t2;

set max_threads = 1;
select '==sort1==';
CREATE TABLE IF NOT EXISTS t_sort(a INT) Engine = fuse CLUSTER BY(a % 3, a);
INSERT INTO t_sort (a) select number from numbers(10);
SELECT * FROM t_sort;
drop table t_sort;


select '==sort2==';
CREATE TABLE IF NOT EXISTS t_sort2(a INT) Engine = fuse CLUSTER BY(a);
INSERT INTO t_sort2 (a) select number from numbers(5) order by number % 3;
SELECT * FROM t_sort2;
drop table t_sort2;


DROP DATABASE db1;
