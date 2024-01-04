statement ok
DROP DATABASE IF EXISTS db1

statement ok
CREATE DATABASE db1

statement ok
USE db1

statement ok
CREATE TABLE IF NOT EXISTS t1(a Int, b Date)

statement ok
INSERT INTO t1 VALUES(1, '2022-12-30')

statement ok
INSERT INTO t1 VALUES(2, '2023-01-01')

query IT
SELECT * FROM t1 ORDER BY b
----
1 2022-12-30
2 2023-01-01

statement ok
explain fragments UPDATE t1 SET a = 3 WHERE b > '2022-12-31'
----


query IT
SELECT * FROM t1 ORDER BY b
----
1 2022-12-30
2 2023-01-01

statement ok
explain fragments UPDATE t1 SET a = 3 WHERE false
----
-[ EXPLAIN ]-----------------------------------
Nothing to update


