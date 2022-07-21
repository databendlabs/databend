DROP DATABASE IF EXISTS db_09_18;

CREATE DATABASE db_09_18;

USE db_09_18;

CREATE TABLE t(c1 int, c2 string);

select '-- insert 3 blocks';
INSERT INTO t VALUES(1, "a");
INSERT INTO t VALUES(2, "b");
INSERT INTO t VALUES(3, "c");

select '-- check pruning, by col c1 ';
explain select * from t where c1 = 1;

select '-- check pruning, by col c2 ';
explain select * from t where c2 > 'b';

DROP TABLE t;
DROP DATABASE db_09_18;
