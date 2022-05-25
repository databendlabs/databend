DROP DATABASE IF EXISTS db_20_0006;
CREATE DATABASE db_20_0006;
USE db_20_0006;

CREATE TABLE t(c1 int);
DROP TABLE t;

select "show tables should give empty list, if all the tables droppped";
SHOW TABLES;

select "after undrop, table should appear";
UNDROP TABLE t;
SHOW TABLES;


select "after undrop, data should appear";
insert into t values(1), (2);
DROP TABLE t;
UNDROP TABLE t;
select * from t;

select "Tables of same name";
DROP TABLE t;
CREATE TABLE t(c1 int, c2 int);
INSERT INTO t VALUES(1, 2);
DROP TABLE t;
UNDROP TABLE t;

select "-- the latest t should be recovered, which has 1 row";
SELECT count(1) FROM t;
ALTER TABLE t RENAME TO t1;
UNDROP TABLE t;
select "-- the first t should be recovered, which has 2 row";
SELECT count(1) FROM t;


DROP TABLE t;
DROP database db_20_0006;
