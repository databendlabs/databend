DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t(a varchar, b varchar) Engine = fuse;
INSERT INTO t(a,b) VALUES('1', 'v1'),('2','v2');
SELECT * FROM t;

CREATE TABLE IF NOT EXISTS t2(a varchar, b varchar) Engine = fuse;
INSERT INTO t2(a,b) VALUES('t2_1', 't2_v1'),('t2_2','t2_v2');
SELECT * FROM t2;

DROP TABLE t;
CREATE TABLE IF NOT EXISTS t(a varchar, b varchar) Engine = fuse;
SELECT * FROM t;

SELECT * FROM t2;

DROP DATABASE db1;
CREATE DATABASE db1;
USE db1;

CREATE TABLE IF NOT EXISTS t2(a varchar, b varchar) Engine = fuse;
SELECT * FROM t2;

DROP DATABASE IF EXISTS db1;
