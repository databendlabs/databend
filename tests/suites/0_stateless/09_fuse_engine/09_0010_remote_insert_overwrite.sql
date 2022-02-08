DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t1(a Int8, b UInt32, c UInt64, d String) Engine = FUSE;
CREATE TABLE IF NOT EXISTS t2(a Int8, b UInt32, c UInt64, d String) Engine = FUSE;

INSERT INTO t1 (a,b,c,d) VALUES(1, 1, 1, 'origin'), (2, 2, 2, 'origin');
INSERT INTO t2 (a,b,c,d) VALUES(3, 3, 3, 'change'), (4, 4, 4, 'change');

select * from t1;
INSERT OVERWRITE t1 select * from t2;
select * from t1;
INSERT OVERWRITE t1 VALUES (5, 5, 5, 'change2'), (6, 6, 6, 'change2');
select * from t1;

DROP DATABASE db1;
