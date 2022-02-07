DROP TABLE IF EXISTS t;

CREATE TABLE t(c1 int) ENGINE = Null;
SELECT COUNT(1) from system.tables where name = 't' and database = 'default';

CREATE TABLE IF NOT EXISTS t(c1 int) ENGINE = Null;
CREATE TABLE t(c1 int) ENGINE = Null; -- {ErrorCode 2302}


create table t2(a int,b int) engine=Memory;
insert into t2 values(1,1),(2,2);
select a+b from t2;

create table t2(a int,b int) engine=NotExists; -- {ErrorCode 2302}

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t2;

-- prepare test databases for testing 'create table like' and 'as select' statements.
CREATE DATABASE db1;
CREATE DATABASE db2;
CREATE TABLE db1.test1(a INT NOT NULL, b INT) ENGINE=memory;
INSERT INTO db1.test1 VALUES (1, 2), (2, 3), (3, 4);

SELECT '====BEGIN TEST CREATE TABLE LIKE STATEMENT====';
-- test 'create table like' statement, expect db2.test2 has the same schema with db1.test1.
CREATE TABLE db2.test2 LIKE db1.test1 ENGINE=fuse;
INSERT INTO db2.test2 VALUES (3, 5);
SELECT a+b FROM db2.test2;
-- check the schema of db2.test2, it should be the same as db1.test1, column 'a' is not nullable.
DESCRIBE db2.test2;
SELECT '====END TEST CREATE TABLE LIKE STATEMENT====';

SELECT '====BEGIN TEST CREATE TABLE AS SELECT STATEMENT====';
-- test 'create table as select' statement, expect db2.test3 has the data from db1.test1 with casting
CREATE TABLE db2.test3(a Varchar, y Varchar) ENGINE=fuse AS SELECT * FROM db1.test1;
DESCRIBE db2.test3;
SELECT a FROM db2.test3;
CREATE TABLE db2.test4(a Varchar, y Varchar) ENGINE=fuse AS SELECT b, a FROM db1.test1;
DESCRIBE db2.test4;
SELECT a FROM db2.test4;
CREATE TABLE db2.test5(a Varchar, y Varchar) ENGINE=fuse AS SELECT b FROM db1.test1;
SELECT a FROM db2.test5;
SELECT '====END TEST CREATE TABLE AS SELECT STATEMENT====';

-- clean up test databases
DROP DATABASE db1;
DROP DATABASE db2;

CREATE TABLE system.test; -- {ErrorCode 1002}
