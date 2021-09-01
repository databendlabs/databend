DROP TABLE IF EXISTS t;

CREATE TABLE t(c1 int) ENGINE = Null;
SELECT COUNT(1) from system.tables where name = 't' and database = 'default';

CREATE TABLE IF NOT EXISTS t(c1 int) ENGINE = Null;
CREATE TABLE t(c1 int) ENGINE = Null; -- {ErrorCode 2}


create table t2(a int,b int) engine=Memory;
insert into t2 values(1,1),(2,2);
select a+b from t2;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t2;
