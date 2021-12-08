DROP TABLE IF EXISTS t;

CREATE TABLE t(c1 int) ENGINE = Null;
SELECT COUNT(1) from system.tables where name = 't' and database = 'default';

CREATE TABLE IF NOT EXISTS t(c1 int) ENGINE = Null;
CREATE TABLE t(c1 int) ENGINE = Null; -- {ErrorCode 4003}


create table t2(a int,b int) engine=Memory;
insert into t2 values(1,1),(2,2);
select a+b from t2;

create table t2(a int,b int) engine=NotExists; -- {ErrorCode 4003}

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t2;

CREATE DATABASE db1;
CREATE DATABASE db2;
CREATE TABLE db1.test1(a INT, b INT);
CREATE TABLE db2.test2 LIKE db1.test1;
INSERT INTO db2.test2 VALUES (3, 5);
SELECT a+b FROM db2.test2;
DROP DATABASE db1;
DROP DATABASE db2;
