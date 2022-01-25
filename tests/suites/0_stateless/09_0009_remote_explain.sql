DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a UInt32, b UInt64, c String) Engine = fuse;
INSERT INTO t1 (a,b,c) values ( 1.00, 1, '1' ), (2, 2.000, '"2"-"2"');
INSERT INTO t1 (a,b,c) values ( 2.00, 2, '2' ), (3, 3.000, '"3"-"3"');
INSERT INTO t1 (a,b,c) values ( 4.00, 4, '4' ), (6, 6.000, '"6"-"6"');

set max_threads = 16;
EXPLAIN SELECT * FROM t1 WHERE a > 3;

DROP TABLE t1;
DROP DATABASE db1;
