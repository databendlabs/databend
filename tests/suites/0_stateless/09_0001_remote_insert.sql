DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a UInt32, b UInt64, c String);
insert into t1 (a,b,c) values ( 1, 1, '1' ), (2, 2, '"2"-"2"');
SELECT * FROM t1;

SELECT sum(a) from t1;

DROP TABLE t1;
DROP DATABASE db1;
