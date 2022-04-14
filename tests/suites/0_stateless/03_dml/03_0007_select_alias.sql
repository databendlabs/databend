set max_threads=1;
SELECT (number+1) as c1, max(number) as c2 FROM numbers_mt(10) group by number+1 having c2>1 order by c1 desc, c2 asc;
DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;
CREATE TABLE test.t (id int);
INSERT INTO test.t VALUES(1);
SELECT t.`id` AS C, `t`.id AS C1, `id` AS C2, `t`.`id` AS C3  FROM test.t;
DROP DATABASE test;
SELECT `T`.`ID` AS C -- {ErrorCode 1058}
