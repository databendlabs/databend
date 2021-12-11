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

-- prepare test databases for testing 'create table like' and 'as select' statements.
CREATE DATABASE db1;
CREATE DATABASE db2;
CREATE TABLE db1.test1(a INT, b INT);
INSERT INTO db1.test1 VALUES (1, 1), (2, 2), (3, 3);
-- test 'create table like' statement, expect db2.test2 has the same schema with db1.test1
CREATE TABLE db2.test2 LIKE db1.test1;
INSERT INTO db2.test2 VALUES (3, 5);
SELECT a+b FROM db2.test2;
-- test 'create table as select' statement, expect db2.test3 has the data from db1.test1 with casting
CREATE TABLE db2.test3(x Varchar, y Varchar) AS SELECT * FROM db1.test1;
SELECT x FROM db2.test3;
-- clean up test databases
DROP DATABASE db1;
DROP DATABASE db2;