DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a UInt32, b UInt64, c String) Engine = fuse;
INSERT INTO t1 (a,b,c) values ( 1.00, 1, '1' ), (2, 2.000, '"2"-"2"');
SELECT * FROM t1;

SELECT sum(a) from t1;

CREATE TABLE IF NOT EXISTS t2(a Boolean, b Timestamp, c Date) Engine = fuse;
INSERT INTO t2 (a,b,c) values(true, '2021-09-07 21:38:35', '2021-09-07'), (false, 1631050715, 18877);
SELECT * FROM t2;

DROP TABLE t1;
DROP TABLE t2;
DROP DATABASE db1;
