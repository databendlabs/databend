DROP TABLE IF EXISTS t1;
CREATE TABLE t1(c1 int) ENGINE = Null;
INSERT INTO TABLE t1 values(1);
SELECT * FROM t1;
TRUNCATE TABLE t1;
DROP TABLE IF EXISTS t1;

DROP TABLE IF EXISTS t2;
CREATE TABLE t2(c1 int) ENGINE = Memory;
INSERT INTO TABLE t2 values(1);
SELECT * FROM t2;
TRUNCATE TABLE t2;
SELECT * FROM t2;
DROP TABLE IF EXISTS t2;

DROP TABLE IF EXISTS default.t3;
create table default.t3 (id int,name varchar(255),rank int) Engine = CSV location = 'tests/data/sample.csv';
TRUNCATE TABLE t3; -- {ErrorCode 2}
DROP TABLE IF EXISTS default.t3;
